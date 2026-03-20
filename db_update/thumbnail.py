#!/usr/bin/env python3
"""
Fetch highest-quality ad thumbnails from Meta Graph API, upload them to S3,
and write the S3 URL into ad.thumb_link matched by fb_ad_id.

Required env vars:
- META_ACCESS_TOKEN
- S3_BUCKET

Optional env vars:
- META_GRAPH_VERSION (default: v22.0)
- AWS_REGION (default: ap-northeast-2)
- S3_PREFIX (default: thumbnails)
- S3_BASE_URL (e.g. CloudFront/custom domain)
"""

from __future__ import annotations

import argparse
import mimetypes
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests import Response
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.db_connector import get_engine


GRAPH_BASE = "https://graph.facebook.com"


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists() or not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _safe_fb_ad_id(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    return re.sub(r"[^0-9A-Za-z_.-]", "_", token)


def _normalize_prefix(prefix: str) -> str:
    token = (prefix or "").strip().strip("/")
    if not token:
        return "thumbnails"
    token = token.replace("\\", "/")
    token = token.lstrip("./")
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
    resp = requests.get(
        url,
        params=params,
        timeout=timeout,
    )
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


def _fetch_story_original_urls(
    story_id: str,
    access_token: str,
    graph_version: str,
) -> List[str]:
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
            data = _graph_get(
                story_id,
                fields,
                access_token=access_token,
                graph_version=graph_version,
            )
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

    seen = set()
    deduped: List[str] = []
    for item in hashes:
        if item in seen:
            continue
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

    seen = set()
    deduped: List[str] = []
    for item in video_ids:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _fetch_video_thumbnail_urls(
    video_ids: List[str],
    access_token: str,
    graph_version: str,
) -> List[str]:
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


def _fetch_hash_image_urls(
    image_hashes: List[str],
    access_token: str,
    graph_version: str,
) -> List[str]:
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
    seen = set()
    for item in raw_candidates:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url.startswith("http"):
            continue
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _image_dimensions(data: bytes) -> Optional[Tuple[int, int]]:
    if len(data) < 10:
        return None

    # PNG
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        w = int.from_bytes(data[16:20], "big")
        h = int.from_bytes(data[20:24], "big")
        if w > 0 and h > 0:
            return (w, h)

    # GIF
    if data[:6] in (b"GIF87a", b"GIF89a") and len(data) >= 10:
        w = int.from_bytes(data[6:8], "little")
        h = int.from_bytes(data[8:10], "little")
        if w > 0 and h > 0:
            return (w, h)

    # BMP
    if data[:2] == b"BM" and len(data) >= 26:
        w = int.from_bytes(data[18:22], "little", signed=True)
        h = abs(int.from_bytes(data[22:26], "little", signed=True))
        if w > 0 and h > 0:
            return (w, h)

    # JPEG
    if data[:2] == b"\xff\xd8":
        i = 2
        sof_markers = {
            0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
            0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
        }
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

    # WEBP
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP" and len(data) >= 30:
        chunk = data[12:16]
        if chunk == b"VP8X" and len(data) >= 30:
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
    best_type = ""
    best_url = ""
    best_width = 0
    best_height = 0
    best_score = (-1, -1, -1)

    fallback_bytes: Optional[bytes] = None
    fallback_type = ""
    fallback_url = ""
    fallback_width = 0
    fallback_height = 0
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

            # 크기 무관하게 가장 큰 후보를 fallback으로 보관
            fallback_score_candidate = (short_side, area, len(body))
            if fallback_score_candidate > fallback_score:
                fallback_score = fallback_score_candidate
                fallback_bytes = body
                fallback_type = resp.headers.get("Content-Type", "")
                fallback_url = url
                fallback_width = width
                fallback_height = height

            if short_side < min_short_side:
                continue

            score = (short_side, area, len(body))
            if score > best_score:
                best_score = score
                best_bytes = body
                best_type = resp.headers.get("Content-Type", "")
                best_url = url
                best_width = width
                best_height = height
        except Exception:
            continue

    # min_short_side 기준을 통과한 이미지가 없으면 가장 큰 후보로 fallback
    if best_bytes is None:
        if fallback_bytes is None:
            raise RuntimeError("Failed to download any thumbnail candidate")
        print(
            f"[WARN] No image met min_short_side={min_short_side}px, "
            f"using best available ({fallback_width}x{fallback_height})"
        )
        return fallback_bytes, fallback_type, fallback_url, fallback_width, fallback_height

    return best_bytes, best_type, best_url, best_width, best_height


def _find_largest_existing_object(
    s3_client,
    bucket: str,
    key_prefix: str,
) -> Tuple[Optional[str], Optional[int]]:
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=100)
    except Exception:
        return None, None
    contents = resp.get("Contents") or []
    if not isinstance(contents, list):
        return None, None

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
            largest_key = key
            largest_size = size_i
    return largest_key, largest_size


def _fetch_rows(
    engine,
    account_id: Optional[int],
    limit: int,
    force: bool,
) -> List[Dict[str, Any]]:
    clauses = ["fb_ad_id IS NOT NULL"]
    params: Dict[str, Any] = {}

    if account_id is not None:
        clauses.append("account_id = :account_id")
        params["account_id"] = account_id
    if not force:
        clauses.append("(thumb_link IS NULL OR thumb_link = '')")

    where_sql = " AND ".join(clauses)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = " LIMIT :limit"
        params["limit"] = limit

    query = text(
        f"""
        SELECT ad_id, fb_ad_id, thumb_link
        FROM ad
        WHERE {where_sql}
        ORDER BY ad_id DESC
        {limit_sql}
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    return [dict(r) for r in rows]


def _update_thumb_link(engine, fb_ad_id: str, thumb_link: str, dry_run: bool) -> int:
    if dry_run:
        return 0
    query = text("UPDATE ad SET thumb_link = :thumb_link WHERE fb_ad_id = :fb_ad_id")
    with engine.begin() as conn:
        result = conn.execute(query, {"thumb_link": thumb_link, "fb_ad_id": fb_ad_id})
    return int(result.rowcount or 0)


def _fetch_creative_data(
    fb_ad_id: str,
    access_token: str,
    graph_version: str,
) -> Dict[str, Any]:
    ad_data = _graph_get(
        fb_ad_id,
        "id,creative{id}",
        access_token=access_token,
        graph_version=graph_version,
    )
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Meta thumbnails, upload to S3, update ad.thumb_link",
    )
    parser.add_argument("--account-id", type=int, default=None, help="Filter by ad.account_id")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0 means no limit)")
    parser.add_argument("--force", action="store_true", help="Rebuild even if thumb_link exists")
    parser.add_argument("--dry-run", action="store_true", help="Do not write S3/DB")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds per request loop")
    parser.add_argument(
        "--min-short-side",
        type=int,
        default=300,
        help="Prefer images whose shorter side is at or above this size; falls back to largest available",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_env_file(Path(__file__).resolve().with_name(".env"))

    access_token = os.getenv("META_ACCESS_TOKEN", "").strip()
    graph_version = os.getenv("META_GRAPH_VERSION", "v22.0").strip() or "v22.0"
    region = os.getenv("AWS_REGION", "ap-northeast-2").strip() or "ap-northeast-2"
    bucket = os.getenv("S3_BUCKET", "").strip()
    prefix = _normalize_prefix(os.getenv("S3_PREFIX", "thumbnails"))
    base_url = os.getenv("S3_BASE_URL", "").strip()

    if not access_token:
        print("ERROR: META_ACCESS_TOKEN is required")
        return 1
    if not bucket:
        print("ERROR: S3_BUCKET is required")
        return 1

    try:
        import boto3
    except Exception:
        print("ERROR: boto3 is not installed. Install with `pip install boto3`.")
        return 1

    engine = get_engine()
    rows = _fetch_rows(engine=engine, account_id=args.account_id, limit=args.limit, force=args.force)
    if not rows:
        print("No target rows found.")
        return 0

    s3 = boto3.client("s3", region_name=region)

    processed = 0
    success = 0
    failed = 0
    updated_rows = 0

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
                extra_urls.extend(
                    _fetch_hash_image_urls(
                        image_hashes=image_hashes,
                        access_token=access_token,
                        graph_version=graph_version,
                    )
                )
            if video_ids:
                extra_urls.extend(
                    _fetch_video_thumbnail_urls(
                        video_ids=video_ids,
                        access_token=access_token,
                        graph_version=graph_version,
                    )
                )
            story_id = str(creative_data.get("effective_object_story_id") or "").strip()
            if story_id:
                extra_urls.extend(
                    _fetch_story_original_urls(
                        story_id=story_id,
                        access_token=access_token,
                        graph_version=graph_version,
                    )
                )

            candidates = _candidate_image_urls(creative_data, extra_urls=extra_urls)
            image_bytes, content_type, source_url, width, height = _download_best_image(
                candidates,
                min_short_side=max(1, int(args.min_short_side)),
            )

            ext = _guess_extension(content_type, source_url)
            object_key_base = f"{prefix}/{fb_ad_id}"
            object_key = f"{object_key_base}{ext}"
            largest_key, largest_size = _find_largest_existing_object(
                s3_client=s3,
                bucket=bucket,
                key_prefix=object_key_base,
            )
            should_upload = largest_size is None or len(image_bytes) > largest_size
            final_key = object_key if should_upload else (largest_key or object_key)
            object_url = _join_public_url(base_url, bucket, region, final_key)

            if not args.dry_run:
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

            rowcount = _update_thumb_link(engine=engine, fb_ad_id=fb_ad_id, thumb_link=object_url, dry_run=args.dry_run)
            updated_rows += rowcount
            success += 1
            print(
                f"[OK] ad_id={ad_id} fb_ad_id={fb_ad_id} "
                f"bytes={len(image_bytes)} dims={width}x{height} updated={rowcount} key={final_key}"
            )
        except Exception as exc:
            failed += 1
            print(f"[FAIL] ad_id={ad_id} fb_ad_id={fb_ad_id}: {exc}")

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(
        "Done. "
        f"processed={processed} success={success} failed={failed} updated_rows={updated_rows} "
        f"dry_run={args.dry_run}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
