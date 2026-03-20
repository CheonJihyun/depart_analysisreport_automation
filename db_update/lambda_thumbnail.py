"""
AWS Lambda handler: Fetch Meta ad thumbnails for recently added ads,
upload them to S3, and update ad.thumb_link.

Required env vars:
- META_ACCESS_TOKEN
- S3_BUCKET
- DB_URL  (e.g. postgresql://user:pass@host:5432/dbname)

Optional env vars:
- LOOKBACK_DAYS     (default: 14)  — only process ads added within this many days
- META_GRAPH_VERSION (default: v22.0)
- AWS_REGION        (default: ap-northeast-2)
- S3_PREFIX         (default: thumbnails)
- S3_BASE_URL       (e.g. CloudFront/custom domain)
- FORCE             (default: false) — rebuild even if thumb_link already exists
- SLEEP_SECONDS     (default: 0.0)  — sleep between API calls
- MIN_SHORT_SIDE    (default: 300)
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pg8000.native
import requests
from requests import Response


GRAPH_BASE = "https://graph.facebook.com"


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _get_conn():
    return pg8000.native.Connection(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=int(os.environ.get("DB_PORT", "5432")),
    )


def _fetch_recent_rows(lookback_days: int, force: bool) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        if force:
            rows = conn.run(
                """
                SELECT ad_id, fb_ad_id, thumb_link
                FROM ad
                WHERE fb_ad_id IS NOT NULL
                  AND created_time >= NOW() - :days * INTERVAL '1 day'
                ORDER BY ad_id DESC
                """,
                days=lookback_days,
            )
        else:
            rows = conn.run(
                """
                SELECT ad_id, fb_ad_id, thumb_link
                FROM ad
                WHERE fb_ad_id IS NOT NULL
                  AND created_time >= NOW() - :days * INTERVAL '1 day'
                  AND (thumb_link IS NULL OR thumb_link = '')
                ORDER BY ad_id DESC
                """,
                days=lookback_days,
            )
        columns = [col["name"] for col in conn.columns]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def _update_thumb_link(fb_ad_id: str, thumb_link: str) -> int:
    conn = _get_conn()
    try:
        conn.run(
            "UPDATE ad SET thumb_link = :thumb_link WHERE fb_ad_id = :fb_ad_id",
            thumb_link=thumb_link,
            fb_ad_id=fb_ad_id,
        )
        return conn.row_count
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers (ported from thumbnail.py without modification)
# ---------------------------------------------------------------------------

def _safe_fb_ad_id(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    return re.sub(r"[^0-9A-Za-z_.-]", "_", token)


def _normalize_prefix(prefix: str) -> str:
    token = (prefix or "").strip().strip("/")
    if not token:
        return "thumbnails"
    token = token.replace("\\", "/").lstrip("./")
    if not token:
        return "thumbnails"
    if token == "thumbnails" or token.startswith("thumbnails/"):
        return token
    return f"thumbnails/{token}"


def _guess_extension(content_type: str, source_url: str) -> str:
    allowed_exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
    ctype = (content_type or "").split(";")[0].strip().lower()
    if ctype.startswith("image/"):
        ext = mimetypes.guess_extension(ctype)
        if ext:
            if ext == ".jpe":
                return ".jpg"
            if ext in allowed_exts:
                return ext
            return ".jpg"
    parsed = urlparse(source_url or "")
    path_ext = Path(parsed.path).suffix.lower()
    if path_ext in allowed_exts:
        return ".jpg" if path_ext == ".jpeg" else path_ext
    return ".jpg"


def _content_type_from_ext(ext: str) -> str:
    token = (ext or "").lower()
    if token in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if token == ".png":
        return "image/png"
    if token == ".webp":
        return "image/webp"
    if token == ".gif":
        return "image/gif"
    if token == ".bmp":
        return "image/bmp"
    return "image/jpeg"


def _join_public_url(base_url: str, bucket: str, region: str, key: str) -> str:
    if base_url:
        return f"{base_url.rstrip('/')}/{key}"
    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def _graph_get(
    node_id: str,
    fields: str,
    access_token: str,
    graph_version: str,
    extra_params: Optional[Dict[str, Any]] = None,
    timeout: int = 25,
) -> Dict[str, Any]:
    url = f"{GRAPH_BASE}/{graph_version}/{node_id}"
    params: Dict[str, Any] = {"fields": fields, "access_token": access_token}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(url, params=params, timeout=timeout)
    _raise_graph_error(resp, node_id)
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected graph response for node={node_id}")
    return data


def _raise_graph_error(resp: Response, node_id: str) -> None:
    if resp.ok:
        return
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    raise RuntimeError(f"Graph API error for node={node_id}: {body}")


def _extract_story_urls(story_spec: Any) -> List[str]:
    if not isinstance(story_spec, dict):
        return []
    urls: List[str] = []
    link_data = story_spec.get("link_data")
    if isinstance(link_data, dict):
        if isinstance(link_data.get("picture"), str):
            urls.append(link_data["picture"])
        child_attachments = link_data.get("child_attachments")
        if isinstance(child_attachments, list):
            for item in child_attachments:
                if isinstance(item, dict) and isinstance(item.get("picture"), str):
                    urls.append(item["picture"])
    video_data = story_spec.get("video_data")
    if isinstance(video_data, dict) and isinstance(video_data.get("image_url"), str):
        urls.append(video_data["image_url"])
    photo_data = story_spec.get("photo_data")
    if isinstance(photo_data, dict) and isinstance(photo_data.get("url"), str):
        urls.append(photo_data["url"])
    return urls


def _extract_asset_feed_urls(asset_feed_spec: Any) -> List[str]:
    if not isinstance(asset_feed_spec, dict):
        return []
    urls: List[str] = []
    for image_item in asset_feed_spec.get("images") or []:
        if isinstance(image_item, dict) and isinstance(image_item.get("url"), str):
            urls.append(image_item["url"])
    for video_item in asset_feed_spec.get("videos") or []:
        if isinstance(video_item, dict) and isinstance(video_item.get("thumbnail_url"), str):
            urls.append(video_item["thumbnail_url"])
    return urls


def _extract_story_attachment_urls(attachments_obj: Any) -> List[str]:
    urls: List[str] = []
    if not isinstance(attachments_obj, dict):
        return urls
    rows = attachments_obj.get("data") or []
    if not isinstance(rows, list):
        return urls
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("url", "unshimmed_url", "media_url", "source", "picture", "full_picture"):
            value = row.get(key)
            if isinstance(value, str):
                urls.append(value)
        media = row.get("media")
        if isinstance(media, dict):
            image_obj = media.get("image")
            if isinstance(image_obj, dict):
                src = image_obj.get("src")
                if isinstance(src, str):
                    urls.append(src)
            for key in ("source", "image_src"):
                value = media.get(key)
                if isinstance(value, str):
                    urls.append(value)
        urls.extend(_extract_story_attachment_urls(row.get("subattachments")))
    return urls


def _fetch_story_original_urls(story_id: str, access_token: str, graph_version: str) -> List[str]:
    field_candidates = [
        (
            "full_picture,picture,"
            "attachments{url,unshimmed_url,media_url,source,picture,full_picture,"
            "media{image,source},"
            "subattachments{url,unshimmed_url,media_url,source,picture,full_picture,media{image,source}}}"
        ),
        "full_picture,picture,attachments{url,picture,media{image},subattachments{url,picture,media{image}}}",
        "full_picture,picture",
    ]
    data: Optional[Dict[str, Any]] = None
    for fields in field_candidates:
        try:
            data = _graph_get(story_id, fields, access_token=access_token, graph_version=graph_version)
            break
        except Exception:
            continue
    if not isinstance(data, dict):
        return []
    urls: List[str] = []
    for key in ("full_picture", "picture"):
        value = data.get(key)
        if isinstance(value, str):
            urls.append(value)
    urls.extend(_extract_story_attachment_urls(data.get("attachments")))
    return urls


def _extract_image_hashes(creative_data: Dict[str, Any]) -> List[str]:
    hashes: List[str] = []
    for key in ("image_hash",):
        value = creative_data.get(key)
        if isinstance(value, str) and value.strip():
            hashes.append(value.strip())
    story_spec = creative_data.get("object_story_spec")
    if isinstance(story_spec, dict):
        link_data = story_spec.get("link_data")
        if isinstance(link_data, dict):
            img_hash = link_data.get("image_hash")
            if isinstance(img_hash, str) and img_hash.strip():
                hashes.append(img_hash.strip())
            child_attachments = link_data.get("child_attachments")
            if isinstance(child_attachments, list):
                for item in child_attachments:
                    if not isinstance(item, dict):
                        continue
                    child_hash = item.get("image_hash")
                    if isinstance(child_hash, str) and child_hash.strip():
                        hashes.append(child_hash.strip())
        photo_data = story_spec.get("photo_data")
        if isinstance(photo_data, dict):
            img_hash = photo_data.get("image_hash")
            if isinstance(img_hash, str) and img_hash.strip():
                hashes.append(img_hash.strip())
    asset_feed_spec = creative_data.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        for image_item in asset_feed_spec.get("images") or []:
            if isinstance(image_item, dict):
                img_hash = image_item.get("hash")
                if isinstance(img_hash, str) and img_hash.strip():
                    hashes.append(img_hash.strip())
    seen: set = set()
    deduped: List[str] = []
    for item in hashes:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _extract_video_ids(creative_data: Dict[str, Any]) -> List[str]:
    video_ids: List[str] = []
    story_spec = creative_data.get("object_story_spec")
    if isinstance(story_spec, dict):
        video_data = story_spec.get("video_data")
        if isinstance(video_data, dict):
            video_id = video_data.get("video_id")
            if isinstance(video_id, str) and video_id.strip():
                video_ids.append(video_id.strip())
    asset_feed_spec = creative_data.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        for video_item in asset_feed_spec.get("videos") or []:
            if isinstance(video_item, dict):
                video_id = video_item.get("video_id")
                if isinstance(video_id, str) and video_id.strip():
                    video_ids.append(video_id.strip())
    seen: set = set()
    deduped: List[str] = []
    for item in video_ids:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _fetch_video_thumbnail_urls(video_ids: List[str], access_token: str, graph_version: str) -> List[str]:
    urls: List[str] = []
    for video_id in video_ids:
        try:
            data = _graph_get(
                video_id,
                "thumbnails{uri,width,height,is_preferred},picture",
                access_token=access_token,
                graph_version=graph_version,
            )
        except Exception:
            continue
        thumbs_obj = data.get("thumbnails")
        if isinstance(thumbs_obj, dict):
            thumb_rows = thumbs_obj.get("data") or []
            if isinstance(thumb_rows, list):
                sorted_rows = sorted(
                    [row for row in thumb_rows if isinstance(row, dict)],
                    key=lambda r: float((r.get("width") or 0)) * float((r.get("height") or 0)),
                    reverse=True,
                )
                for row in sorted_rows:
                    uri = row.get("uri")
                    if isinstance(uri, str) and uri.strip():
                        urls.append(uri.strip())
        picture = data.get("picture")
        if isinstance(picture, str) and picture.strip():
            urls.append(picture.strip())
    return urls


def _fetch_hash_image_urls(image_hashes: List[str], access_token: str, graph_version: str) -> List[str]:
    urls: List[str] = []
    for image_hash in image_hashes:
        try:
            data = _graph_get(
                image_hash,
                "url,original_width,original_height",
                access_token=access_token,
                graph_version=graph_version,
            )
        except Exception:
            continue
        url = data.get("url")
        if isinstance(url, str) and url.strip():
            urls.append(url.strip())
    return urls


def _candidate_image_urls(creative_data: Dict[str, Any], extra_urls: Optional[List[str]] = None) -> List[str]:
    raw_candidates: List[str] = []
    for key in ("image_url", "thumbnail_url"):
        value = creative_data.get(key)
        if isinstance(value, str):
            raw_candidates.append(value)
    raw_candidates.extend(_extract_story_urls(creative_data.get("object_story_spec")))
    raw_candidates.extend(_extract_asset_feed_urls(creative_data.get("asset_feed_spec")))
    if extra_urls:
        raw_candidates.extend(extra_urls)
    deduped: List[str] = []
    seen: set = set()
    for item in raw_candidates:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url.startswith("http") or url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _image_dimensions(data: bytes) -> Optional[Tuple[int, int]]:
    if len(data) < 10:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        w = int.from_bytes(data[16:20], "big")
        h = int.from_bytes(data[20:24], "big")
        if w > 0 and h > 0:
            return (w, h)
    if data[:6] in (b"GIF87a", b"GIF89a") and len(data) >= 10:
        w = int.from_bytes(data[6:8], "little")
        h = int.from_bytes(data[8:10], "little")
        if w > 0 and h > 0:
            return (w, h)
    if data[:2] == b"BM" and len(data) >= 26:
        w = int.from_bytes(data[18:22], "little", signed=True)
        h = abs(int.from_bytes(data[22:26], "little", signed=True))
        if w > 0 and h > 0:
            return (w, h)
    if data[:2] == b"\xff\xd8":
        i = 2
        sof_markers = {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}
        while i + 9 < len(data):
            if data[i] != 0xFF:
                i += 1
                continue
            marker = data[i + 1]
            if marker in (0xD8, 0xD9):
                i += 2
                continue
            if i + 4 > len(data):
                break
            seg_len = int.from_bytes(data[i + 2:i + 4], "big")
            if seg_len < 2:
                break
            if marker in sof_markers and i + 9 < len(data):
                h = int.from_bytes(data[i + 5:i + 7], "big")
                w = int.from_bytes(data[i + 7:i + 9], "big")
                if w > 0 and h > 0:
                    return (w, h)
            i += 2 + seg_len
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP" and len(data) >= 30:
        chunk = data[12:16]
        if chunk == b"VP8X":
            w = int.from_bytes(data[24:27], "little") + 1
            h = int.from_bytes(data[27:30], "little") + 1
            if w > 0 and h > 0:
                return (w, h)
        if chunk == b"VP8L" and len(data) >= 25:
            bits = data[21:25]
            b0, b1, b2, b3 = bits[0], bits[1], bits[2], bits[3]
            w = (b0 | ((b1 & 0x3F) << 8)) + 1
            h = (((b1 >> 6) | (b2 << 2) | ((b3 & 0x0F) << 10))) + 1
            if w > 0 and h > 0:
                return (w, h)
        if chunk == b"VP8 " and len(data) >= 30 and data[23:26] == b"\x9d\x01\x2a":
            w = int.from_bytes(data[26:28], "little") & 0x3FFF
            h = int.from_bytes(data[28:30], "little") & 0x3FFF
            if w > 0 and h > 0:
                return (w, h)
    return None


def _download_best_image(
    urls: List[str],
    timeout: int = 30,
    min_short_side: int = 300,
) -> Tuple[bytes, str, str, int, int]:
    if not urls:
        raise RuntimeError("No candidate image URLs")

    best_bytes: Optional[bytes] = None
    best_type = best_url = ""
    best_width = best_height = 0
    best_score = (-1, -1, -1)

    fallback_bytes: Optional[bytes] = None
    fallback_type = fallback_url = ""
    fallback_width = fallback_height = 0
    fallback_score = (-1, -1, -1)

    for url in urls:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            body = resp.content
            if not body:
                continue
            dims = _image_dimensions(body)
            if not dims:
                continue
            width, height = dims
            short_side = min(width, height)
            area = width * height

            fb_score = (short_side, area, len(body))
            if fb_score > fallback_score:
                fallback_score = fb_score
                fallback_bytes, fallback_type, fallback_url = body, resp.headers.get("Content-Type", ""), url
                fallback_width, fallback_height = width, height

            if short_side < min_short_side:
                continue

            score = (short_side, area, len(body))
            if score > best_score:
                best_score = score
                best_bytes, best_type, best_url = body, resp.headers.get("Content-Type", ""), url
                best_width, best_height = width, height
        except Exception:
            continue

    if best_bytes is None:
        if fallback_bytes is None:
            raise RuntimeError("Failed to download any thumbnail candidate")
        print(
            f"[WARN] No image met min_short_side={min_short_side}px, "
            f"using best available ({fallback_width}x{fallback_height})"
        )
        return fallback_bytes, fallback_type, fallback_url, fallback_width, fallback_height

    return best_bytes, best_type, best_url, best_width, best_height


def _find_largest_existing_object(s3_client, bucket: str, key_prefix: str) -> Tuple[Optional[str], Optional[int]]:
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=100)
    except Exception:
        return None, None
    contents = resp.get("Contents") or []
    largest_key: Optional[str] = None
    largest_size: Optional[int] = None
    for item in contents:
        if not isinstance(item, dict):
            continue
        key = item.get("Key")
        size = item.get("Size")
        if not isinstance(key, str) or not isinstance(size, (int, float)):
            continue
        if key != key_prefix and not key.startswith(f"{key_prefix}."):
            continue
        size_i = int(size)
        if largest_size is None or size_i > largest_size:
            largest_key, largest_size = key, size_i
    return largest_key, largest_size


def _fetch_creative_data(fb_ad_id: str, access_token: str, graph_version: str) -> Dict[str, Any]:
    ad_data = _graph_get(fb_ad_id, "id,creative{id}", access_token=access_token, graph_version=graph_version)
    creative = ad_data.get("creative")
    if not isinstance(creative, dict) or not creative.get("id"):
        raise RuntimeError(f"No creative id for fb_ad_id={fb_ad_id}")
    creative_id = str(creative["id"])
    return _graph_get(
        creative_id,
        (
            "id,effective_object_story_id,image_hash,image_url,thumbnail_url,"
            "object_story_spec{link_data{picture,image_hash,child_attachments{picture,image_hash}},"
            "video_data{image_url,video_id},photo_data{url,image_hash}},"
            "asset_feed_spec{images{hash,url},videos{video_id,thumbnail_url}}"
        ),
        access_token=access_token,
        graph_version=graph_version,
        extra_params={"thumbnail_width": 2048, "thumbnail_height": 2048},
    )


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # --- Config from env vars ---
    access_token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    graph_version = os.environ.get("META_GRAPH_VERSION", "v22.0").strip() or "v22.0"
    region = os.environ.get("S3_REGION", os.environ.get("AWS_REGION", "ap-northeast-2")).strip() or "ap-northeast-2"
    bucket = os.environ.get("S3_BUCKET", "").strip()
    prefix = _normalize_prefix(os.environ.get("S3_PREFIX", "thumbnails"))
    base_url = os.environ.get("S3_BASE_URL", "").strip()
    lookback_days = int(os.environ.get("LOOKBACK_DAYS") or "14")
    force = os.environ.get("FORCE", "false").lower() in ("1", "true", "yes")
    sleep_seconds = float(os.environ.get("SLEEP_SECONDS") or "0")
    min_short_side = int(os.environ.get("MIN_SHORT_SIDE") or "300")

    if not access_token:
        return _error_response("META_ACCESS_TOKEN is required")
    if not bucket:
        return _error_response("S3_BUCKET is required")
    for required in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        if not os.environ.get(required):
            return _error_response(f"{required} is required")

    import boto3

    rows = _fetch_recent_rows(lookback_days=lookback_days, force=force)
    if not rows:
        msg = f"No target rows found (lookback_days={lookback_days}, force={force})"
        print(msg)
        return {"statusCode": 200, "body": json.dumps({"message": msg, "processed": 0})}

    print(f"Found {len(rows)} rows to process (lookback_days={lookback_days})")

    s3 = boto3.client("s3", region_name=region)
    processed = success = failed = updated_rows = 0

    for row in rows:
        processed += 1
        ad_id = row.get("ad_id")
        fb_ad_id = _safe_fb_ad_id(row.get("fb_ad_id"))
        if not fb_ad_id:
            failed += 1
            print(f"[SKIP] ad_id={ad_id} invalid fb_ad_id={row.get('fb_ad_id')}")
            continue

        try:
            creative_data = _fetch_creative_data(
                fb_ad_id=fb_ad_id,
                access_token=access_token,
                graph_version=graph_version,
            )
            image_hashes = _extract_image_hashes(creative_data)
            video_ids = _extract_video_ids(creative_data)
            extra_urls: List[str] = []
            if image_hashes:
                extra_urls.extend(_fetch_hash_image_urls(image_hashes, access_token, graph_version))
            if video_ids:
                extra_urls.extend(_fetch_video_thumbnail_urls(video_ids, access_token, graph_version))
            story_id = str(creative_data.get("effective_object_story_id") or "").strip()
            if story_id:
                extra_urls.extend(_fetch_story_original_urls(story_id, access_token, graph_version))

            candidates = _candidate_image_urls(creative_data, extra_urls=extra_urls)
            image_bytes, content_type, source_url, width, height = _download_best_image(
                candidates, min_short_side=max(1, min_short_side)
            )

            ext = _guess_extension(content_type, source_url)
            object_key_base = f"{prefix}/{fb_ad_id}"
            object_key = f"{object_key_base}{ext}"

            largest_key, largest_size = _find_largest_existing_object(s3, bucket, object_key_base)
            should_upload = largest_size is None or len(image_bytes) > largest_size
            final_key = object_key if should_upload else (largest_key or object_key)
            object_url = _join_public_url(base_url, bucket, region, final_key)

            if should_upload:
                s3.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=image_bytes,
                    ContentType=_content_type_from_ext(ext),
                )
            else:
                print(
                    f"[SKIP_NOT_LARGER] ad_id={ad_id} fb_ad_id={fb_ad_id} "
                    f"new_bytes={len(image_bytes)} existing_bytes={largest_size} key={largest_key}"
                )

            rowcount = _update_thumb_link(fb_ad_id=fb_ad_id, thumb_link=object_url)
            updated_rows += rowcount
            success += 1
            print(
                f"[OK] ad_id={ad_id} fb_ad_id={fb_ad_id} "
                f"bytes={len(image_bytes)} dims={width}x{height} updated={rowcount} key={final_key}"
            )
        except Exception as exc:
            failed += 1
            print(f"[FAIL] ad_id={ad_id} fb_ad_id={fb_ad_id}: {exc}")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = {
        "lookback_days": lookback_days,
        "processed": processed,
        "success": success,
        "failed": failed,
        "updated_rows": updated_rows,
    }
    print(f"Done. {summary}")
    return {
        "statusCode": 200 if failed == 0 else 207,
        "body": json.dumps(summary),
    }


def _error_response(msg: str) -> Dict[str, Any]:
    print(f"ERROR: {msg}")
    return {"statusCode": 500, "body": json.dumps({"error": msg})}
