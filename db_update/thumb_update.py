#!/usr/bin/env python3
"""
Refresh thumbnails that already have thumb_link but whose linked files are too small.

Flow:
1) Read rows from ad where fb_ad_id and thumb_link are present.
2) Check linked object size on S3.
3) If object is missing / unresolvable / smaller than threshold, re-fetch from Meta.
4) Upload to S3 (with overwrite guard) and update ad.thumb_link.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db_update.thumbnail as thumb
from scripts.db_connector import get_engine


def _parse_s3_location(url: str, bucket_hint: str, base_url: str) -> Optional[Tuple[str, str]]:
    text_url = str(url or "").strip()
    if not text_url:
        return None

    base = (base_url or "").strip().rstrip("/")
    if base and text_url.startswith(f"{base}/"):
        key = unquote(text_url[len(base) + 1 :].lstrip("/"))
        if bucket_hint and key:
            return bucket_hint, key

    if text_url.startswith("s3://"):
        rest = text_url[5:]
        if "/" not in rest:
            return None
        bucket, key = rest.split("/", 1)
        bucket, key = bucket.strip(), key.strip()
        return (bucket, key) if bucket and key else None

    parsed = urlparse(text_url)
    host = parsed.netloc.lower()
    path = unquote(parsed.path.lstrip("/"))
    if not host or not path:
        return None

    if not host.endswith("amazonaws.com"):
        return None

    parts = host.split(".")
    if "s3" in parts and parts[0] != "s3":
        s3_idx = parts.index("s3")
        bucket = ".".join(parts[:s3_idx]).strip()
        key = path.strip()
        return (bucket, key) if bucket and key else None

    if parts[0] == "s3":
        if "/" not in path:
            return None
        bucket, key = path.split("/", 1)
        bucket, key = bucket.strip(), key.strip()
        return (bucket, key) if bucket and key else None

    return None


def _head_object_size(s3_client, bucket: str, key: str) -> Optional[int]:
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None
    size = head.get("ContentLength")
    return int(size) if isinstance(size, (int, float)) else None


def _fetch_target_rows(
    engine,
    account_id: Optional[int],
    limit: int,
) -> List[Dict[str, Any]]:
    clauses = [
        "fb_ad_id IS NOT NULL",
        "thumb_link IS NOT NULL",
        "thumb_link <> ''",
    ]
    params: Dict[str, Any] = {}
    if account_id is not None:
        clauses.append("account_id = :account_id")
        params["account_id"] = account_id

    where_sql = " AND ".join(clauses)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = " LIMIT :limit"
        params["limit"] = limit

    query = text(
        f"""
        SELECT ad_id, account_id, fb_ad_id, thumb_link
        FROM ad
        WHERE {where_sql}
        ORDER BY ad_id DESC
        {limit_sql}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    return [dict(r) for r in rows]


def _refresh_one(
    row: Dict[str, Any],
    s3_client,
    engine,
    access_token: str,
    graph_version: str,
    bucket: str,
    region: str,
    base_url: str,
    prefix: str,
    min_short_side: int,
    dry_run: bool,
) -> Tuple[bool, str]:
    ad_id = row.get("ad_id")
    fb_ad_id = thumb._safe_fb_ad_id(row.get("fb_ad_id"))
    if not fb_ad_id:
        return False, f"[SKIP] ad_id={ad_id} invalid fb_ad_id={row.get('fb_ad_id')}"

    creative_data = thumb._fetch_creative_data(
        fb_ad_id=fb_ad_id,
        access_token=access_token,
        graph_version=graph_version,
    )

    image_hashes = thumb._extract_image_hashes(creative_data)
    video_ids = thumb._extract_video_ids(creative_data)
    extra_urls: List[str] = []

    if image_hashes:
        extra_urls.extend(
            thumb._fetch_hash_image_urls(
                image_hashes=image_hashes,
                access_token=access_token,
                graph_version=graph_version,
            )
        )
    if video_ids:
        extra_urls.extend(
            thumb._fetch_video_thumbnail_urls(
                video_ids=video_ids,
                access_token=access_token,
                graph_version=graph_version,
            )
        )

    story_id = str(creative_data.get("effective_object_story_id") or "").strip()
    if story_id:
        extra_urls.extend(
            thumb._fetch_story_original_urls(
                story_id=story_id,
                access_token=access_token,
                graph_version=graph_version,
            )
        )

    candidates = thumb._candidate_image_urls(creative_data, extra_urls=extra_urls)
    image_bytes, content_type, source_url, width, height = thumb._download_best_image(
        candidates,
        min_short_side=max(1, int(min_short_side)),
    )

    ext = thumb._guess_extension(content_type, source_url)
    object_key_base = f"{prefix}/{fb_ad_id}"
    object_key = f"{object_key_base}{ext}"

    largest_key, largest_size = thumb._find_largest_existing_object(
        s3_client=s3_client,
        bucket=bucket,
        key_prefix=object_key_base,
    )
    should_upload = largest_size is None or len(image_bytes) > largest_size
    final_key = object_key if should_upload else (largest_key or object_key)
    object_url = thumb._join_public_url(base_url, bucket, region, final_key)

    if not dry_run:
        if should_upload:
            s3_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=image_bytes,
                ContentType=thumb._content_type_from_ext(ext),
            )
        thumb._update_thumb_link(engine=engine, fb_ad_id=fb_ad_id, thumb_link=object_url, dry_run=False)

    msg = (
        f"[OK] ad_id={ad_id} fb_ad_id={fb_ad_id} "
        f"bytes={len(image_bytes)} dims={width}x{height} "
        f"uploaded={should_upload} key={final_key} dry_run={dry_run}"
    )
    return True, msg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh small thumbnails even when thumb_link already exists",
    )
    parser.add_argument("--account-id", type=int, default=None, help="Filter by ad.account_id")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to inspect (0 = no limit)")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=150000,
        help="Rows with linked object size below this are refreshed",
    )
    parser.add_argument(
        "--min-short-side",
        type=int,
        default=720,
        help="Reject newly fetched images below this short-side size",
    )
    parser.add_argument("--dry-run", action="store_true", help="Inspect and simulate refresh only")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds per refreshed row")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    thumb._load_env_file(Path(__file__).resolve().with_name(".env"))

    access_token = os.getenv("META_ACCESS_TOKEN", "").strip()
    graph_version = os.getenv("META_GRAPH_VERSION", "v22.0").strip() or "v22.0"
    region = os.getenv("AWS_REGION", "ap-northeast-2").strip() or "ap-northeast-2"
    bucket = os.getenv("S3_BUCKET", "").strip()
    prefix = thumb._normalize_prefix(os.getenv("S3_PREFIX", "thumbnails"))
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
    s3 = boto3.client("s3", region_name=region)
    rows = _fetch_target_rows(engine=engine, account_id=args.account_id, limit=args.limit)
    if not rows:
        print("No target rows found.")
        return 0

    inspected = 0
    to_refresh: List[Dict[str, Any]] = []

    for row in rows:
        inspected += 1
        link = str(row.get("thumb_link") or "").strip()
        fb_ad_id = row.get("fb_ad_id")
        s3_loc = _parse_s3_location(link, bucket_hint=bucket, base_url=base_url)
        if not s3_loc:
            row["_reason"] = "unresolvable_link"
            row["_size"] = None
            to_refresh.append(row)
            continue

        obj_bucket, obj_key = s3_loc
        size = _head_object_size(s3, obj_bucket, obj_key)
        if size is None:
            row["_reason"] = "missing_object"
            row["_size"] = None
            to_refresh.append(row)
            continue

        if size < args.max_bytes:
            row["_reason"] = "small_object"
            row["_size"] = size
            to_refresh.append(row)
            continue

        print(f"[KEEP] fb_ad_id={fb_ad_id} size={size} link={link}")

    print(
        f"Scan done. inspected={inspected} refresh_targets={len(to_refresh)} "
        f"max_bytes={args.max_bytes}"
    )

    refreshed = 0
    failed = 0
    for row in to_refresh:
        ad_id = row.get("ad_id")
        fb_ad_id = row.get("fb_ad_id")
        reason = row.get("_reason")
        old_size = row.get("_size")
        print(
            f"[REFRESH] ad_id={ad_id} fb_ad_id={fb_ad_id} "
            f"reason={reason} old_size={old_size}"
        )
        try:
            ok, msg = _refresh_one(
                row=row,
                s3_client=s3,
                engine=engine,
                access_token=access_token,
                graph_version=graph_version,
                bucket=bucket,
                region=region,
                base_url=base_url,
                prefix=prefix,
                min_short_side=args.min_short_side,
                dry_run=args.dry_run,
            )
            if ok:
                refreshed += 1
            else:
                failed += 1
            print(msg)
        except Exception as exc:
            failed += 1
            print(f"[FAIL] ad_id={ad_id} fb_ad_id={fb_ad_id}: {exc}")

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(
        "Done. "
        f"inspected={inspected} refreshed={refreshed} failed={failed} dry_run={args.dry_run}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
