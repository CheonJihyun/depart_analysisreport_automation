
# ==== CELL 0 ====
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
# ==== CELL 2 ====
import os
import json
import time
from collections import defaultdict
from urllib.parse import urlencode

import urllib3

# =========================
# Config
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]

http = urllib3.PoolManager()

# =========================
# Meta GET
# =========================
def graph_get(path: str, params: dict):
    url = f"{BASE_URL}/{path.lstrip('/')}"
    params = dict(params)
    params["access_token"] = ACCESS_TOKEN

    full_url = f"{url}?{urlencode(params)}"
    r = http.request("GET", full_url)
    body = r.data.decode("utf-8")

    if r.status >= 400:
        raise RuntimeError(body)

    return json.loads(body)

# =========================
# 핵심 함수
# =========================
def collect_action_types(
    fb_ad_account_id: str,
    fb_ad_ids: list[str],
    since: str,
    until: str,
):
    """
    특정 광고계정 + ad_id 들에 대해
    Meta가 실제로 내려주는 모든 action_type 수집
    """

    action_counter = defaultdict(int)

    path = f"act_{fb_ad_account_id}/insights"

    filtering = [{
        "field": "ad.id",
        "operator": "IN",
        "value": fb_ad_ids,
    }]

    params = {
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "breakdowns": "age,gender",
        "fields": "ad_id,date_start,actions",
        "filtering": json.dumps(filtering),
        "limit": 5000,
    }

    after = None
    total_rows = 0

    while True:
        if after:
            params["after"] = after

        payload = graph_get(path, params)

        data = payload.get("data") or []
        total_rows += len(data)

        for row in data:
            actions = row.get("actions") or []
            for a in actions:
                at = a.get("action_type")
                if at:
                    action_counter[at] += 1

        paging = payload.get("paging") or {}
        cursors = paging.get("cursors") or {}
        after = cursors.get("after")

        if not after:
            break

        time.sleep(0.3)

    return total_rows, dict(sorted(action_counter.items(), key=lambda x: -x[1]))

# =========================
# 실행 예시
# =========================
if __name__ == "__main__":
    FB_AD_ACCOUNT_ID = "4204029286499182"
    FB_AD_IDS = [
        "120233891052040586",
    ]

    SINCE = "2025-01-01"
    UNTIL = "2026-02-01"

    rows, actions = collect_action_types(
        fb_ad_account_id=FB_AD_ACCOUNT_ID,
        fb_ad_ids=FB_AD_IDS,
        since=SINCE,
        until=UNTIL,
    )

    print(f"\n✅ scanned rows: {rows}")
    print("📊 action_type frequency:")
    for k, v in actions.items():
        print(f"{k:40s} {v}")
# ==== CELL 4 ====
import os
import json
import time
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import urllib3
import pg8000.native

# =========================
# Config
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

DAYS_BACK = int(os.getenv("DAYS_BACK", "14"))
CUMULATIVE_SINCE = os.getenv("CUMULATIVE_SINCE", "2025-01-01")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
API_SLEEP_BASE = float(os.getenv("API_SLEEP_BASE", "0.4"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))

KST = timezone(timedelta(hours=9))

ACTION_KEYS = {
    "link_clicks": [
        "link_click",
        "inline_link_click",
        "outbound_click",
        "outbound_clicks",
    ],
    "add_to_cart": [
        "add_to_cart",
        "offsite_conversion.fb_pixel_add_to_cart",
        "omni_add_to_cart",
    ],
    "website_landing_page_views": [
        "landing_page_view",
        "omni_landing_page_view",
    ],
    "purchases": [
        "purchase",
        "offsite_conversion.fb_pixel_purchase",
        "omni_purchase",
    ],
    "initiate_checkout": [
        "initiate_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
        "omni_initiated_checkout",
    ],
    "view_content": [
        "view_content",
        "offsite_conversion.fb_pixel_view_content",
        "omni_view_content",
    ],
    "complete_registration": [
        "complete_registration",
        "offsite_conversion.fb_pixel_complete_registration",
    ],
    "video_views": [
        "video_view",
    ],
    "post_engagements": [
        "post_engagement",
    ],
    "post_reactions": [
        "post_reaction",
    ],
    "comments": [
        "comment",
    ],
    "post_saves": [
        "onsite_conversion.post_save",
    ],
    "instagram_profile_visits": [
        "instagram_profile_visit",
    ],
    "follows": [
        "follow",
    ],
}

http = urllib3.PoolManager(
    num_pools=10,
    headers={"User-Agent": "meta-ads-insights-actions-sync/final"},
)

# =========================
# Helpers
# =========================
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def normalize_act_id(x):
    if not x:
        return ""
    s = str(x).strip()
    while s.startswith("act_"):
        s = s.replace("act_", "", 1)
    return s


def sleep_backoff(attempt):
    s = API_SLEEP_BASE * (2 ** attempt) + random.uniform(0, API_SLEEP_BASE)
    time.sleep(min(s, 30))


def graph_get(path, params):
    url = f"{BASE_URL}/{path.lstrip('/')}"
    params = dict(params or {})
    params["access_token"] = ACCESS_TOKEN

    for attempt in range(MAX_RETRIES):
        try:
            full_url = f"{url}?{urlencode(params)}"
            r = http.request(
                "GET",
                full_url,
                timeout=urllib3.Timeout(connect=10.0, read=90.0),
                retries=False,
            )
            body = r.data.decode("utf-8", errors="replace")

            if r.status >= 400:
                j = json.loads(body)
                err = j.get("error", {})
                code = err.get("code")
                subcode = err.get("error_subcode")
                msg = err.get("message", "")[:200]

                if code == 100 and subcode == 33:
                    raise RuntimeError(f"non-retryable: {msg}")

                sleep_backoff(attempt)
                continue

            return json.loads(body)

        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            sleep_backoff(attempt)

    raise RuntimeError("graph_get failed")


# =========================
# Metrics parsing
# =========================
def parse_actions_list(actions):
    out = {}
    if not actions:
        return out
    for a in actions:
        at = a.get("action_type")
        v = a.get("value")
        if at and v is not None:
            try:
                out[at] = float(v)
            except Exception:
                pass
    return out


def parse_roas(roas):
    if roas is None:
        return None
    if isinstance(roas, list):
        vals = []
        for x in roas:
            try:
                vals.append(float(x.get("value")))
            except Exception:
                pass
        return max(vals) if vals else None
    try:
        return float(roas)
    except Exception:
        return None


def max_from_actions(actions_map, keys):
    vals = []
    for k in keys:
        if k in actions_map:
            try:
                vals.append(float(actions_map[k]))
            except Exception:
                pass
    return max(vals) if vals else None


def sum_action_list(field_data):
    """video_p25_watched_actions 등 list 형태의 필드 합산"""
    if not field_data:
        return None
    total = 0.0
    for x in field_data:
        try:
            total += float(x.get("value", 0))
        except Exception:
            pass
    return total if total > 0 else None


def normalize_metrics(item):
    actions_map = parse_actions_list(item.get("actions"))
    roas = parse_roas(item.get("purchase_roas"))

    def _int_action(key):
        return int(max_from_actions(actions_map, ACTION_KEYS[key]) or 0)

    def _float_field(key):
        v = item.get(key)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def _int_field(key):
        v = item.get(key)
        try:
            return int(float(v)) if v is not None else None
        except Exception:
            return None

    return {
        "spend": float(item.get("spend") or 0.0),
        "frequency": _float_field("frequency"),
        "cpc": _float_field("cpc"),
        "cpm": _float_field("cpm"),
        "purchase_roas": roas,
        "link_clicks": _int_action("link_clicks"),
        "website_landing_page_views": _int_action("website_landing_page_views"),
        "add_to_cart": _int_action("add_to_cart"),
        "purchases": _int_action("purchases"),
        "initiate_checkout": _int_action("initiate_checkout"),
        "view_content": _int_action("view_content"),
        "complete_registration": _int_action("complete_registration"),
        "video_views": _int_action("video_views"),
        "post_engagements": _int_action("post_engagements"),
        "post_reactions": _int_action("post_reactions"),
        "comments": _int_action("comments"),
        "post_saves": _int_action("post_saves"),
        "instagram_profile_visits": _int_action("instagram_profile_visits"),
        "follows": _int_action("follows"),
        "video_p25_watched": int(sum_action_list(item.get("video_p25_watched_actions")) or 0),
        "video_p50_watched": int(sum_action_list(item.get("video_p50_watched_actions")) or 0),
        "video_p75_watched": int(sum_action_list(item.get("video_p75_watched_actions")) or 0),
        "video_p100_watched": int(sum_action_list(item.get("video_p100_watched_actions")) or 0),
        "video_thruplay_watched": int(sum_action_list(item.get("video_thruplay_watched_actions")) or 0),
    }


# =========================
# DB
# =========================
def db_connect():
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_NAME,
        port=5432,
        ssl_context=True,
    )


def fetch_accounts_and_ads(conn):
    sql = """
    SELECT aa.fb_ad_account_id, a.ad_id, a.fb_ad_id
    FROM ad a
    JOIN ad_account aa ON a.account_id = aa.account_id
    WHERE a.fb_ad_id IS NOT NULL
    """
    rows = conn.run(sql)

    by_act = {}
    adid_map = {}

    for act, ad_id, fb_ad_id in rows:
        act_num = normalize_act_id(act)
        by_act.setdefault(act_num, []).append(str(fb_ad_id))
        adid_map[str(fb_ad_id)] = ad_id

    return by_act, adid_map


def upsert_daily(conn, row):
    sql = """
    INSERT INTO ad_performance_daily (
      ad_id, date, age, gender,
      spend, frequency, cpc, cpm,
      purchase_roas, purchases,
      link_clicks, website_landing_page_views, add_to_cart,
      initiate_checkout, view_content, complete_registration,
      video_views, post_engagements, post_reactions, comments, post_saves,
      instagram_profile_visits, follows,
      video_p25_watched, video_p50_watched, video_p75_watched,
      video_p100_watched, video_thruplay_watched,
      updated_at
    )
    VALUES (
      :ad_id, :date, :age, :gender,
      :spend, :frequency, :cpc, :cpm,
      :purchase_roas, :purchases,
      :link_clicks, :website_landing_page_views, :add_to_cart,
      :initiate_checkout, :view_content, :complete_registration,
      :video_views, :post_engagements, :post_reactions, :comments, :post_saves,
      :instagram_profile_visits, :follows,
      :video_p25_watched, :video_p50_watched, :video_p75_watched,
      :video_p100_watched, :video_thruplay_watched,
      NOW()
    )
    ON CONFLICT (ad_id, date, age, gender)
    DO UPDATE SET
      spend = EXCLUDED.spend,
      frequency = EXCLUDED.frequency,
      cpc = EXCLUDED.cpc,
      cpm = EXCLUDED.cpm,
      purchase_roas = EXCLUDED.purchase_roas,
      purchases = EXCLUDED.purchases,
      link_clicks = EXCLUDED.link_clicks,
      website_landing_page_views = EXCLUDED.website_landing_page_views,
      add_to_cart = EXCLUDED.add_to_cart,
      initiate_checkout = EXCLUDED.initiate_checkout,
      view_content = EXCLUDED.view_content,
      complete_registration = EXCLUDED.complete_registration,
      video_views = EXCLUDED.video_views,
      post_engagements = EXCLUDED.post_engagements,
      post_reactions = EXCLUDED.post_reactions,
      comments = EXCLUDED.comments,
      post_saves = EXCLUDED.post_saves,
      instagram_profile_visits = EXCLUDED.instagram_profile_visits,
      follows = EXCLUDED.follows,
      video_p25_watched = EXCLUDED.video_p25_watched,
      video_p50_watched = EXCLUDED.video_p50_watched,
      video_p75_watched = EXCLUDED.video_p75_watched,
      video_p100_watched = EXCLUDED.video_p100_watched,
      video_thruplay_watched = EXCLUDED.video_thruplay_watched,
      updated_at = NOW()
    """
    conn.run(sql, **row)


def upsert_cumulative(conn, row):
    sql = """
    INSERT INTO ad_performance_cumulative (
      ad_id, age, gender,
      spend, frequency, cpc, cpm,
      purchase_roas, purchases,
      link_clicks, website_landing_page_views, add_to_cart,
      initiate_checkout, view_content, complete_registration,
      video_views, post_engagements, post_reactions, comments, post_saves,
      instagram_profile_visits, follows,
      video_p25_watched, video_p50_watched, video_p75_watched,
      video_p100_watched, video_thruplay_watched,
      updated_at
    )
    VALUES (
      :ad_id, :age, :gender,
      :spend, :frequency, :cpc, :cpm,
      :purchase_roas, :purchases,
      :link_clicks, :website_landing_page_views, :add_to_cart,
      :initiate_checkout, :view_content, :complete_registration,
      :video_views, :post_engagements, :post_reactions, :comments, :post_saves,
      :instagram_profile_visits, :follows,
      :video_p25_watched, :video_p50_watched, :video_p75_watched,
      :video_p100_watched, :video_thruplay_watched,
      NOW()
    )
    ON CONFLICT (ad_id, age, gender)
    DO UPDATE SET
      spend = EXCLUDED.spend,
      frequency = EXCLUDED.frequency,
      cpc = EXCLUDED.cpc,
      cpm = EXCLUDED.cpm,
      purchase_roas = EXCLUDED.purchase_roas,
      purchases = EXCLUDED.purchases,
      link_clicks = EXCLUDED.link_clicks,
      website_landing_page_views = EXCLUDED.website_landing_page_views,
      add_to_cart = EXCLUDED.add_to_cart,
      initiate_checkout = EXCLUDED.initiate_checkout,
      view_content = EXCLUDED.view_content,
      complete_registration = EXCLUDED.complete_registration,
      video_views = EXCLUDED.video_views,
      post_engagements = EXCLUDED.post_engagements,
      post_reactions = EXCLUDED.post_reactions,
      comments = EXCLUDED.comments,
      post_saves = EXCLUDED.post_saves,
      instagram_profile_visits = EXCLUDED.instagram_profile_visits,
      follows = EXCLUDED.follows,
      video_p25_watched = EXCLUDED.video_p25_watched,
      video_p50_watched = EXCLUDED.video_p50_watched,
      video_p75_watched = EXCLUDED.video_p75_watched,
      video_p100_watched = EXCLUDED.video_p100_watched,
      video_thruplay_watched = EXCLUDED.video_thruplay_watched,
      updated_at = NOW()
    """
    conn.run(sql, **row)


# =========================
# Insights
# =========================
INSIGHTS_FIELDS = ",".join([
    "ad_id", "date_start",
    "spend", "frequency", "cpc", "cpm",
    "actions", "purchase_roas",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
])


def fetch_account_insights(act_num, time_range, fb_ad_ids, time_increment=None):
    path = f"act_{act_num}/insights"
    params = {
        "level": "ad",
        "time_range": json.dumps(time_range),
        "breakdowns": "age,gender",
        "fields": INSIGHTS_FIELDS,
        "filtering": json.dumps([
            {"field": "ad.id", "operator": "IN", "value": fb_ad_ids}
        ]),
        "limit": 5000,
    }
    if time_increment is not None:
        params["time_increment"] = str(time_increment)

    data, after = [], None
    while True:
        if after:
            params["after"] = after
        payload = graph_get(path, params)
        data.extend(payload.get("data") or [])
        after = payload.get("paging", {}).get("cursors", {}).get("after")
        if not after:
            break
        time.sleep(API_SLEEP_BASE)

    return data


def load_cumulative(conn, by_act, adid_map, since, until):
    print(f"📥 CUMULATIVE since={since} until={until}")
    for act_num, ads in by_act.items():
        for chunk in chunked(ads, BATCH_SIZE):
            ins = fetch_account_insights(act_num, {"since": since, "until": until}, chunk)
            for item in ins:
                ad_id = adid_map.get(str(item.get("ad_id")))
                if not ad_id:
                    continue
                m = normalize_metrics(item)
                upsert_cumulative(conn, {
                    "ad_id": ad_id,
                    "age": item.get("age") or "unknown",
                    "gender": item.get("gender") or "unknown",
                    **m,
                })


def load_daily(conn, by_act, adid_map, since, until):
    print(f"📥 DAILY since={since} until={until}")
    start = datetime.fromisoformat(since).date()
    end = datetime.fromisoformat(until).date() - timedelta(days=1)

    d = start
    while d <= end:
        s = d.strftime("%Y-%m-%d")
        u = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"  - day {s}")

        for act_num, ads in by_act.items():
            for chunk in chunked(ads, BATCH_SIZE):
                ins = fetch_account_insights(
                    act_num,
                    {"since": s, "until": u},
                    chunk,
                    time_increment=1,
                )
                for item in ins:
                    ad_id = adid_map.get(str(item.get("ad_id")))
                    if not ad_id:
                        continue
                    m = normalize_metrics(item)
                    upsert_daily(conn, {
                        "ad_id": ad_id,
                        "date": item.get("date_start") or s,
                        "age": item.get("age") or "unknown",
                        "gender": item.get("gender") or "unknown",
                        **m,
                    })
        d += timedelta(days=1)


# =========================
# Main
# =========================
def main():
    now = datetime.now(KST)
    until = now.strftime("%Y-%m-%d")
    daily_since = (now - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    conn = db_connect()
    try:
        by_act, adid_map = fetch_accounts_and_ads(conn)
        print(f"✅ acts={len(by_act)} ads={sum(len(v) for v in by_act.values())}")

        load_cumulative(conn, by_act, adid_map, CUMULATIVE_SINCE, until)
        load_daily(conn, by_act, adid_map, daily_since, until)

        print("🎉 Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
# ==== CELL 6 ====
import os
import json
import time
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import urllib3
import pg8000.native

# =========================
# Config
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

# 최근 N일만 daily 업데이트
DAYS_BACK = int(os.getenv("DAYS_BACK", "325"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))  # ad.id filtering chunk size
API_SLEEP_BASE = float(os.getenv("API_SLEEP_BASE", "0.4"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))

KST = timezone(timedelta(hours=9))

ACTION_KEYS = {
    "link_clicks": ["link_click", "inline_link_click", "outbound_click", "outbound_clicks"],
    "add_to_cart": ["add_to_cart", "offsite_conversion.fb_pixel_add_to_cart", "omni_add_to_cart"],
    "website_landing_page_views": ["landing_page_view", "omni_landing_page_view"],
    "purchases": ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"],
    "initiate_checkout": [
        "initiate_checkout",
        "offsite_conversion.fb_pixel_initiate_checkout",
        "omni_initiated_checkout",
    ],
    "view_content": [
        "view_content",
        "offsite_conversion.fb_pixel_view_content",
        "omni_view_content",
    ],
    "complete_registration": [
        "complete_registration",
        "offsite_conversion.fb_pixel_complete_registration",
    ],
    "video_views": ["video_view"],
    "post_engagements": ["post_engagement"],
    "post_reactions": ["post_reaction"],
    "comments": ["comment"],
    "post_saves": ["onsite_conversion.post_save"],
    "instagram_profile_visits": ["instagram_profile_visit"],
    "follows": ["follow"],
}

http = urllib3.PoolManager(
    num_pools=10,
    headers={"User-Agent": "meta-ads-daily-actions-sync/1.0"},
)

# =========================
# Helpers
# =========================
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def normalize_act_id(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    while s.startswith("act_"):
        s = s.replace("act_", "", 1)
    return s.strip()


def _sleep_backoff(attempt: int):
    s = (API_SLEEP_BASE * (2 ** attempt)) + random.uniform(0, API_SLEEP_BASE)
    time.sleep(min(s, 30.0))


def is_retryable_meta_error(code, subcode, msg: str) -> bool:
    m = (msg or "").lower()
    if code in (4, 17, 32, 613):
        return True
    if "rate" in m and "limit" in m:
        return True
    if "too many calls" in m or "user request limit" in m:
        return True
    if "temporarily unavailable" in m or "try again later" in m:
        return True
    return False


def graph_get(path: str, params: dict) -> dict:
    """
    - 네트워크/타임아웃: 재시도
    - rate limit: backoff 재시도
    - code=100 subcode=33: 재시도 X (즉시 실패)
    """
    url = f"{BASE_URL}/{path.lstrip('/')}"
    params = dict(params or {})
    params["access_token"] = ACCESS_TOKEN

    last_err = None

    for attempt in range(MAX_RETRIES):
        try:
            full_url = f"{url}?{urlencode(params)}"
            r = http.request(
                "GET",
                full_url,
                timeout=urllib3.Timeout(connect=10.0, read=90.0),
                retries=False,
            )
            body = r.data.decode("utf-8", errors="replace")

            if r.status >= 400:
                try:
                    j = json.loads(body)
                except Exception:
                    last_err = RuntimeError(f"HTTP {r.status}: {body[:300]}")
                    _sleep_backoff(attempt)
                    continue

                err = j.get("error") or {}
                code = err.get("code")
                subcode = err.get("error_subcode")
                msg = (err.get("message", "") or "")[:300]

                if code == 100 and subcode == 33:
                    raise RuntimeError(
                        f"Meta API non-retryable error: code={code}, subcode={subcode}, msg={msg}"
                    )

                if is_retryable_meta_error(code, subcode, msg):
                    print(
                        f"⚠️ Retryable Meta error (code={code}, subcode={subcode}) "
                        f"attempt={attempt+1}/{MAX_RETRIES}: {msg[:160]}"
                    )
                    _sleep_backoff(attempt)
                    continue

                raise RuntimeError(f"Meta API error: code={code}, subcode={subcode}, msg={msg}")

            return json.loads(body)

        except RuntimeError as e:
            last_err = e
            if "non-retryable" in str(e).lower():
                raise
            if attempt == MAX_RETRIES - 1:
                raise
            _sleep_backoff(attempt)

        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES - 1:
                raise
            print(
                f"⚠️ Network error: {type(e).__name__}: {str(e)[:160]} | retry {attempt+1}/{MAX_RETRIES}"
            )
            _sleep_backoff(attempt)

    raise RuntimeError(f"graph_get failed: {last_err}")


# =========================
# Actions / ROAS parsing
# =========================
def parse_actions_list(actions):
    out = {}
    if not actions:
        return out
    for a in actions:
        at = a.get("action_type")
        v = a.get("value")
        if at is None or v is None:
            continue
        try:
            out[at] = float(v)
        except Exception:
            continue
    return out


def parse_roas(roas_field):
    if roas_field is None:
        return None
    if isinstance(roas_field, (int, float)):
        return float(roas_field)
    if isinstance(roas_field, str):
        try:
            return float(roas_field)
        except Exception:
            return None
    if isinstance(roas_field, list):
        vals = []
        for x in roas_field:
            if not isinstance(x, dict):
                continue
            v = x.get("value")
            if v is None:
                continue
            try:
                vals.append(float(v))
            except Exception:
                pass
        return max(vals) if vals else None
    return None


def max_from_actions(actions_map: dict, candidates: list[str]):
    vals = []
    for k in candidates:
        if k in actions_map:
            try:
                vals.append(float(actions_map[k]))
            except Exception:
                pass
    return max(vals) if vals else None


def sum_action_list(field_data):
    """video_p25_watched_actions 등 list 형태의 필드 합산"""
    if not field_data:
        return None
    total = 0.0
    for x in field_data:
        try:
            total += float(x.get("value", 0))
        except Exception:
            pass
    return total if total > 0 else None


def normalize_metrics(item: dict):
    """
    - count 계열(클릭/LPV/ATC/구매 등)은 actions가 없으면 0으로 저장
    - purchase_roas / 소수점 필드는 NULL 유지
    """
    actions_map = parse_actions_list(item.get("actions"))
    roas = parse_roas(item.get("purchase_roas"))

    def _int_action(key):
        return int(max_from_actions(actions_map, ACTION_KEYS[key]) or 0)

    def _float_field(key):
        v = item.get(key)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def _int_field(key):
        v = item.get(key)
        try:
            return int(float(v)) if v is not None else None
        except Exception:
            return None

    return {
        "spend": float(item.get("spend") or 0.0),
        "frequency": _float_field("frequency"),
        "cpc": _float_field("cpc"),
        "cpm": _float_field("cpm"),
        "purchase_roas": roas,
        "link_clicks": _int_action("link_clicks"),
        "website_landing_page_views": _int_action("website_landing_page_views"),
        "add_to_cart": _int_action("add_to_cart"),
        "purchases": _int_action("purchases"),
        "initiate_checkout": _int_action("initiate_checkout"),
        "view_content": _int_action("view_content"),
        "complete_registration": _int_action("complete_registration"),
        "video_views": _int_action("video_views"),
        "post_engagements": _int_action("post_engagements"),
        "post_reactions": _int_action("post_reactions"),
        "comments": _int_action("comments"),
        "post_saves": _int_action("post_saves"),
        "instagram_profile_visits": _int_action("instagram_profile_visits"),
        "follows": _int_action("follows"),
        "video_p25_watched": int(sum_action_list(item.get("video_p25_watched_actions")) or 0),
        "video_p50_watched": int(sum_action_list(item.get("video_p50_watched_actions")) or 0),
        "video_p75_watched": int(sum_action_list(item.get("video_p75_watched_actions")) or 0),
        "video_p100_watched": int(sum_action_list(item.get("video_p100_watched_actions")) or 0),
        "video_thruplay_watched": int(sum_action_list(item.get("video_thruplay_watched_actions")) or 0),
    }


# =========================
# DB
# =========================
def db_connect():
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_NAME,
        port=5432,
        ssl_context=True,
    )


def fetch_accounts_and_ads(conn):
    """
    returns:
      by_act: { "<numeric_act_id>": [fb_ad_id,...] }
      adid_map: { "<fb_ad_id>": ad_id }
    """
    sql = """
    SELECT
      aa.fb_ad_account_id,
      a.ad_id,
      a.fb_ad_id
    FROM ad a
    JOIN ad_account aa ON a.account_id = aa.account_id
    WHERE a.fb_ad_id IS NOT NULL
      AND aa.fb_ad_account_id IS NOT NULL
    """
    rows = conn.run(sql)

    by_act = {}
    adid_map = {}
    bad_act = 0

    for fb_ad_account_id, ad_id, fb_ad_id in rows:
        act_num = normalize_act_id(fb_ad_account_id)
        if not act_num:
            bad_act += 1
            continue
        fb_ad_id = str(fb_ad_id)
        by_act.setdefault(act_num, []).append(fb_ad_id)
        adid_map[fb_ad_id] = ad_id

    if bad_act:
        print(f"⚠️ skipped rows with empty/invalid fb_ad_account_id: {bad_act}")

    return by_act, adid_map


def upsert_daily(conn, row):
    sql = """
    INSERT INTO ad_performance_daily (
      ad_id, date, age, gender,
      spend, frequency, cpc, cpm,
      purchase_roas, purchases,
      link_clicks, website_landing_page_views, add_to_cart,
      initiate_checkout, view_content, complete_registration,
      video_views, post_engagements, post_reactions, comments, post_saves,
      instagram_profile_visits, follows,
      video_p25_watched, video_p50_watched, video_p75_watched,
      video_p100_watched, video_thruplay_watched,
      updated_at
    )
    VALUES (
      :ad_id, :date, :age, :gender,
      :spend, :frequency, :cpc, :cpm,
      :purchase_roas, :purchases,
      :link_clicks, :website_landing_page_views, :add_to_cart,
      :initiate_checkout, :view_content, :complete_registration,
      :video_views, :post_engagements, :post_reactions, :comments, :post_saves,
      :instagram_profile_visits, :follows,
      :video_p25_watched, :video_p50_watched, :video_p75_watched,
      :video_p100_watched, :video_thruplay_watched,
      NOW()
    )
    ON CONFLICT (ad_id, date, age, gender)
    DO UPDATE SET
      spend = EXCLUDED.spend,
      frequency = EXCLUDED.frequency,
      cpc = EXCLUDED.cpc,
      cpm = EXCLUDED.cpm,
      purchase_roas = EXCLUDED.purchase_roas,
      purchases = EXCLUDED.purchases,
      link_clicks = EXCLUDED.link_clicks,
      website_landing_page_views = EXCLUDED.website_landing_page_views,
      add_to_cart = EXCLUDED.add_to_cart,
      initiate_checkout = EXCLUDED.initiate_checkout,
      view_content = EXCLUDED.view_content,
      complete_registration = EXCLUDED.complete_registration,
      video_views = EXCLUDED.video_views,
      post_engagements = EXCLUDED.post_engagements,
      post_reactions = EXCLUDED.post_reactions,
      comments = EXCLUDED.comments,
      post_saves = EXCLUDED.post_saves,
      instagram_profile_visits = EXCLUDED.instagram_profile_visits,
      follows = EXCLUDED.follows,
      video_p25_watched = EXCLUDED.video_p25_watched,
      video_p50_watched = EXCLUDED.video_p50_watched,
      video_p75_watched = EXCLUDED.video_p75_watched,
      video_p100_watched = EXCLUDED.video_p100_watched,
      video_thruplay_watched = EXCLUDED.video_thruplay_watched,
      updated_at = NOW()
    """
    conn.run(sql, **row)


# =========================
# Insights
# =========================
INSIGHTS_FIELDS = ",".join([
    "ad_id", "date_start",
    "spend", "frequency", "cpc", "cpm",
    "actions", "purchase_roas",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
])


def fetch_account_insights_daily(act_num: str, time_range: dict, fb_ad_ids: list[str]):
    """
    ✅ daily 안정화를 위해 time_increment=1을 강제
    """
    path = f"act_{act_num}/insights"
    filtering = [{"field": "ad.id", "operator": "IN", "value": fb_ad_ids}]

    params = {
        "level": "ad",
        "time_range": json.dumps(time_range),
        "time_increment": "1",
        "breakdowns": "age,gender",
        "fields": INSIGHTS_FIELDS,
        "filtering": json.dumps(filtering),
        "limit": 5000,
    }

    data = []
    after = None

    while True:
        if after:
            params["after"] = after

        payload = graph_get(path, params)
        data.extend(payload.get("data") or [])

        paging = payload.get("paging") or {}
        cursors = paging.get("cursors") or {}
        after = cursors.get("after")
        if not after:
            break

        time.sleep(API_SLEEP_BASE)

    return data


def load_daily(conn, by_act, adid_map, since, until):
    print(f"📥 DAILY (actions-only) day-by-day from {since} to {until}")

    start_date = datetime.fromisoformat(since).date()
    end_date = (datetime.fromisoformat(until).date() - timedelta(days=1))

    total_rows = 0
    skipped_accounts = 0

    d = start_date
    while d <= end_date:
        s = d.strftime("%Y-%m-%d")
        u = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"  - day {s}")

        for act_num, fb_ads in by_act.items():
            for fb_chunk in chunked(fb_ads, BATCH_SIZE):
                try:
                    ins = fetch_account_insights_daily(act_num, {"since": s, "until": u}, fb_chunk)
                except RuntimeError as e:
                    msg = str(e)
                    if "non-retryable" in msg.lower():
                        skipped_accounts += 1
                        print(f"⏭️ skip non-retryable act={act_num} day={s}: {msg[:120]}")
                        break
                    print(f"⚠️ insights failed act={act_num} day={s}: {msg[:160]}")
                    continue

                for item in ins:
                    fb_ad_id = str(item.get("ad_id"))
                    ad_id = adid_map.get(fb_ad_id)
                    if not ad_id:
                        continue

                    m = normalize_metrics(item)

                    # ✅ date_start 누락 대응: 요청 day로 fallback
                    date_val = item.get("date_start") or s

                    row = {
                        "ad_id": ad_id,
                        "date": date_val,
                        "age": item.get("age") or "unknown",
                        "gender": item.get("gender") or "unknown",
                        **m,
                    }
                    upsert_daily(conn, row)
                    total_rows += 1

            time.sleep(API_SLEEP_BASE)

        d += timedelta(days=1)

    print(f"✅ upserted daily rows={total_rows} | skipped_accounts={skipped_accounts}")


# =========================
# Main
# =========================
def main():
    now_kst = datetime.now(KST)
    until = now_kst.strftime("%Y-%m-%d")
    daily_since = (now_kst - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    conn = db_connect()
    try:
        by_act, adid_map = fetch_accounts_and_ads(conn)
        total_ads = sum(len(v) for v in by_act.values())
        print(f"✅ loaded acts={len(by_act)} | ads={total_ads}")

        if by_act:
            sample_act = next(iter(by_act.keys()))
            print(f"🔎 DEBUG sample insights path => act_{sample_act}/insights")

        load_daily(conn, by_act, adid_map, since=daily_since, until=until)

        print("🎉 Done.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
# ==== CELL 8 ====
import os
import json
import time
import random
from urllib.parse import urlencode

import urllib3
import pg8000.native

# =========================
# Config
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
API_SLEEP_BASE = float(os.getenv("API_SLEEP_BASE", "0.4"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

http = urllib3.PoolManager(
    num_pools=10,
    headers={"User-Agent": "meta-ad-creative-url-sync/1.0"},
)

# =========================
# Helpers
# =========================
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def normalize_act_id(x):
    if not x:
        return ""
    s = str(x).strip()
    while s.startswith("act_"):
        s = s.replace("act_", "", 1)
    return s


def sleep_backoff(attempt):
    s = (API_SLEEP_BASE * (2 ** attempt)) + random.uniform(0, API_SLEEP_BASE)
    time.sleep(min(s, 20.0))


def graph_get(path, params):
    url = f"{BASE_URL}/{path.lstrip('/')}"
    params = dict(params or {})
    params["access_token"] = ACCESS_TOKEN

    last_err = None

    for attempt in range(MAX_RETRIES):
        try:
            full_url = f"{url}?{urlencode(params)}"
            r = http.request(
                "GET",
                full_url,
                timeout=urllib3.Timeout(connect=10.0, read=60.0),
                retries=False,
            )
            body = r.data.decode("utf-8", errors="replace")

            if r.status >= 400:
                j = json.loads(body)
                err = j.get("error", {})
                code = err.get("code")
                subcode = err.get("error_subcode")
                msg = err.get("message", "")[:200]

                # 권한/존재 문제 → skip
                if code == 100 and subcode == 33:
                    raise RuntimeError(f"non-retryable: {msg}")

                sleep_backoff(attempt)
                continue

            return json.loads(body)

        except RuntimeError:
            raise
        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES - 1:
                raise
            sleep_backoff(attempt)

    raise RuntimeError(f"graph_get failed: {last_err}")


# =========================
# URL extraction
# =========================
def extract_urls(ad_payload):
    """
    Meta creative 구조가 광고 타입별로 다르기 때문에
    가능한 모든 위치를 안전하게 탐색
    """
    link_url = None
    landing_page_url = None

    creative = ad_payload.get("creative") or {}

    # 1️⃣ 가장 단순한 케이스
    if creative.get("object_url"):
        link_url = creative.get("object_url")

    oss = creative.get("object_story_spec") or {}

    # 2️⃣ 링크 광고
    link_data = oss.get("link_data") or {}
    if link_data.get("link"):
        link_url = link_url or link_data.get("link")

    # 3️⃣ CTA 버튼
    cta = link_data.get("call_to_action") or {}
    cta_val = cta.get("value") or {}
    if cta_val.get("link"):
        link_url = link_url or cta_val.get("link")

    # 4️⃣ 비디오 광고 CTA
    video_data = oss.get("video_data") or {}
    vcta = video_data.get("call_to_action") or {}
    vcta_val = vcta.get("value") or {}
    if vcta_val.get("link"):
        link_url = link_url or vcta_val.get("link")

    # landing_page_url은 Meta가 명확히 주지 않으므로
    # 현실적으로는 "광고가 의도한 최종 목적지"로 간주
    landing_page_url = link_url

    return link_url, landing_page_url


# =========================
# DB
# =========================
def db_connect():
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_NAME,
        port=5432,
        ssl_context=True,
    )


def fetch_ads_to_update(conn):
    """
    아직 link_url이 비어있는 광고만 대상으로 함
    """
    sql = """
    SELECT ad_id, fb_ad_id
    FROM ad
    WHERE fb_ad_id IS NOT NULL
      AND link_url IS NULL
    """
    return conn.run(sql)


def update_ad_urls(conn, ad_id, link_url, landing_page_url):
    sql = """
    UPDATE ad
    SET
      link_url = :link_url,
      landing_page_url = :landing_page_url,
      updated_at = NOW()
    WHERE ad_id = :ad_id
    """
    conn.run(
        sql,
        ad_id=ad_id,
        link_url=link_url,
        landing_page_url=landing_page_url,
    )


# =========================
# Main
# =========================
def main():
    conn = db_connect()
    try:
        ads = fetch_ads_to_update(conn)
        print(f"🔎 ads to sync: {len(ads)}")

        for ad_id, fb_ad_id in ads:
            try:
                payload = graph_get(
                    path=str(fb_ad_id),
                    params={
                        "fields": (
                            "creative{"
                            "object_url,"
                            "object_story_spec{"
                            "link_data{link,call_to_action},"
                            "video_data{call_to_action}"
                            "}"
                            "}"
                        )
                    },
                )

                link_url, landing_page_url = extract_urls(payload)

                if not link_url:
                    print(f"⚠️ no link_url ad_id={ad_id}")
                    continue

                update_ad_urls(conn, ad_id, link_url, landing_page_url)
                print(f"✅ updated ad_id={ad_id}")

                time.sleep(API_SLEEP_BASE)

            except RuntimeError as e:
                if "non-retryable" in str(e):
                    print(f"⏭️ skip ad_id={ad_id}: {e}")
                    continue
                print(f"⚠️ failed ad_id={ad_id}: {e}")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
# ==== CELL 9 ====
