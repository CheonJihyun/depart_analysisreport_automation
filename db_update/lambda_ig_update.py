import os
import json
import time
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests
import pg8000.native

# =========================
# Config
# =========================
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v24.0")
BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = int(os.getenv("DB_PORT", "5432"))
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
DAYS_BACK = int(os.getenv("DAYS_BACK", "14"))          # 0 => no time filter (not recommended)
MAX_ROWS = int(os.getenv("MAX_ROWS", "5000"))          # limit work per run
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))        # Graph batch supports up to 50
API_SLEEP_BASE = float(os.getenv("API_SLEEP_BASE", "0.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))

# =========================
# DB helpers
# =========================
def db_connect() -> pg8000.native.Connection:
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_NAME,
        port=DB_PORT,
        ssl_context=True,
    )

def fetch_missing_ad_rows(con: pg8000.native.Connection) -> List[str]:
    """
    Fetch fb_ad_id where any of the new columns are NULL.
    Optionally restrict by created_time (DAYS_BACK).
    """
    where = [
        "fb_ad_id IS NOT NULL",
        "("
        "creative_id IS NULL OR "
        "source_instagram_media_id IS NULL OR "
        "effective_object_story_id IS NULL OR "
        "ig_timestamp IS NULL OR "
        "ig_permalink IS NULL OR "
        "ig_media_type IS NULL"
        ")"
    ]
    params: Dict[str, Any] = {"limit": MAX_ROWS}
    if DAYS_BACK > 0:
        where.append("created_time >= (NOW() - (:days_back || ' days')::interval)")
        params["days_back"] = DAYS_BACK
    sql = f"""
        SELECT fb_ad_id
        FROM ad
        WHERE {" AND ".join(where)}
        ORDER BY created_time DESC NULLS LAST, ad_id DESC
        LIMIT :limit
    """
    rows = con.run(sql, **params)
    return [str(r[0]) for r in rows]

def update_ad_rows_bulk(con: pg8000.native.Connection, updates: List[Dict[str, Any]]) -> int:
    if not updates:
        return 0
    sql = """
    UPDATE ad
    SET
      creative_id = :creative_id,
      source_instagram_media_id = :source_instagram_media_id,
      effective_object_story_id = :effective_object_story_id,
      ig_timestamp = :ig_timestamp,
      ig_permalink = :ig_permalink,
      ig_media_type = :ig_media_type
    WHERE fb_ad_id = :fb_ad_id
    """
    updated = 0
    for u in updates:
        con.run(
            sql,
            fb_ad_id=u["fb_ad_id"],
            creative_id=u.get("creative_id"),
            source_instagram_media_id=u.get("source_instagram_media_id"),
            effective_object_story_id=u.get("effective_object_story_id"),
            ig_timestamp=u.get("ig_timestamp"),
            ig_permalink=u.get("ig_permalink"),
            ig_media_type=u.get("ig_media_type"),
        )
        updated += 1
    return updated

# =========================
# Meta Graph helpers (Batch)
# =========================
def _is_rate_limit_error(err: Dict[str, Any]) -> bool:
    code = err.get("code")
    subcode = err.get("error_subcode")
    return code in (4, 17, 32) or subcode in (2446079,)

def graph_batch(batch_items: List[Dict[str, Any]], max_retries: int = MAX_RETRIES) -> List[Dict[str, Any]]:
    """
    POST /{version} with 'batch' param.
    Retries on batch-level rate limit errors.
    """
    url = f"{BASE_URL}/"
    payload = {
        "access_token": ACCESS_TOKEN,
        "batch": json.dumps(batch_items, ensure_ascii=False),
    }
    for attempt in range(1, max_retries + 1):
        if API_SLEEP_BASE > 0:
            time.sleep(API_SLEEP_BASE)
        r = requests.post(url, data=payload, timeout=60)
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"Batch response not json: status={r.status_code}, text={r.text[:200]}")

        if isinstance(data, dict) and "error" in data:
            err = data["error"]
            if _is_rate_limit_error(err):
                sleep_s = min(60, (2 ** attempt) + random.random() * 2)
                print(f"⚠️ Rate limited (batch-level) code={err.get('code')} subcode={err.get('error_subcode')} sleep {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"Graph batch error: {err}")
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected batch response shape: {data}")
        return data
    raise RuntimeError("Graph batch failed after retries")

def _batch_group_with_item_retry(
    keys: List[str],
    batch_items: List[Dict[str, Any]],
) -> Dict[str, Dict]:
    """
    Execute a batch group and retry individual items that hit rate limits.
    Returns {key: resp} for all keys.
    """
    results: Dict[str, Dict] = {}
    pending_keys = list(keys)
    pending_items = list(batch_items)

    for attempt in range(1, MAX_RETRIES + 1):
        if not pending_keys:
            break

        responses = graph_batch(pending_items)

        retry_keys = []
        retry_items = []
        for key, item, resp in zip(pending_keys, pending_items, responses):
            code = resp.get("code")
            body = resp.get("body") or ""
            if code != 200:
                try:
                    err = json.loads(body).get("error", {})
                except Exception:
                    err = {}
                if _is_rate_limit_error(err):
                    retry_keys.append(key)
                    retry_items.append(item)
                    continue
            results[key] = resp

        pending_keys = retry_keys
        pending_items = retry_items
        if pending_keys:
            sleep_s = min(60, (2 ** attempt) + random.random() * 2)
            print(f"⚠️ {len(pending_keys)} items rate-limited, retry attempt {attempt} in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    # 재시도 모두 소진된 아이템은 빈 응답으로 처리
    for key in pending_keys:
        print(f"⚠️ Gave up on key={key} after {MAX_RETRIES} retries")
        results[key] = {"code": 429, "body": "{}"}

    return results

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def batch_fetch_creative_fields(ad_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for group in chunked(ad_ids, BATCH_SIZE):
        batch_items = []
        for ad_id in group:
            rel = f"{ad_id}?fields=creative{{id,source_instagram_media_id,effective_object_story_id}}"
            batch_items.append({"method": "GET", "relative_url": rel})

        resp_map = _batch_group_with_item_retry(group, batch_items)

        for ad_id in group:
            resp = resp_map.get(ad_id, {"code": -1, "body": "{}"})
            code = resp.get("code")
            body = resp.get("body") or ""
            if code != 200:
                print(f"⚠️ creative fetch failed ad_id={ad_id} code={code} body={body[:120]}")
                out[ad_id] = {
                    "creative_id": None,
                    "source_instagram_media_id": None,
                    "effective_object_story_id": None,
                }
                continue
            try:
                payload = json.loads(body)
            except Exception:
                print(f"⚠️ creative body not json ad_id={ad_id} body={body[:120]}")
                payload = {}
            creative = payload.get("creative") or {}
            out[ad_id] = {
                "creative_id": str(creative.get("id")) if creative.get("id") is not None else None,
                "source_instagram_media_id": str(creative.get("source_instagram_media_id")) if creative.get("source_instagram_media_id") is not None else None,
                "effective_object_story_id": str(creative.get("effective_object_story_id")) if creative.get("effective_object_story_id") is not None else None,
            }
    return out

def batch_fetch_ig_media_meta(ig_media_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    uniq = list(dict.fromkeys([x for x in ig_media_ids if x]))
    for group in chunked(uniq, BATCH_SIZE):
        batch_items = []
        for ig_id in group:
            rel = f"{ig_id}?fields=permalink,timestamp,media_type"
            batch_items.append({"method": "GET", "relative_url": rel})

        resp_map = _batch_group_with_item_retry(group, batch_items)

        for ig_id in group:
            resp = resp_map.get(ig_id, {"code": -1, "body": "{}"})
            code = resp.get("code")
            body = resp.get("body") or ""
            if code != 200:
                print(f"⚠️ ig media fetch failed ig_id={ig_id} code={code} body={body[:120]}")
                out[ig_id] = {"ig_permalink": None, "ig_timestamp": None, "ig_media_type": None}
                continue
            try:
                payload = json.loads(body)
            except Exception:
                payload = {}
            out[ig_id] = {
                "ig_permalink": payload.get("permalink"),
                "ig_timestamp": payload.get("timestamp"),
                "ig_media_type": payload.get("media_type"),
            }
    return out

# =========================
# Lambda handler
# =========================
def lambda_handler(event, context):
    start = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    con = db_connect()
    try:
        ad_ids = fetch_missing_ad_rows(con)
        print(f"✅ [{now_iso}] targets={len(ad_ids)} DAYS_BACK={DAYS_BACK} MAX_ROWS={MAX_ROWS}")
        if not ad_ids:
            return {"statusCode": 200, "body": json.dumps({"message": "No missing rows", "updated": 0})}

        creative_map = batch_fetch_creative_fields(ad_ids)

        ig_ids = []
        for ad_id in ad_ids:
            ig_id = creative_map.get(ad_id, {}).get("source_instagram_media_id")
            if ig_id:
                ig_ids.append(ig_id)
        ig_meta_map = batch_fetch_ig_media_meta(ig_ids) if ig_ids else {}

        updates = []
        no_ig = 0
        for ad_id in ad_ids:
            c = creative_map.get(ad_id, {})
            source_ig = c.get("source_instagram_media_id")
            meta = ig_meta_map.get(source_ig, {}) if source_ig else {}
            if not source_ig:
                no_ig += 1
            updates.append({
                "fb_ad_id": ad_id,
                "creative_id": c.get("creative_id"),
                "source_instagram_media_id": source_ig,
                "effective_object_story_id": c.get("effective_object_story_id"),
                "ig_timestamp": meta.get("ig_timestamp"),
                "ig_permalink": meta.get("ig_permalink"),
                "ig_media_type": meta.get("ig_media_type"),
            })

        updated = update_ad_rows_bulk(con, updates)
        elapsed = round(time.time() - start, 2)
        print(f"✅ Updated rows={updated} no_ig={no_ig} elapsed={elapsed}s")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Ad table creative/IG fields filled",
                    "targets": len(ad_ids),
                    "updated": updated,
                    "no_ig": no_ig,
                    "elapsed_s": elapsed,
                },
                ensure_ascii=False,
            ),
        }
    finally:
        try:
            con.close()
        except Exception:
            pass
