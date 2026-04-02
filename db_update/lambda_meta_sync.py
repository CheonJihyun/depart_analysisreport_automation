import os
import json
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pg8000.native

# =========================
# Config
# =========================
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v21.0")   # 환경변수로 관리
BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

DB_HOST     = os.environ["DB_HOST"]
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = int(os.getenv("DB_PORT", "5432"))

ACCESS_TOKEN   = os.environ["META_ACCESS_TOKEN"]
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "5"))
API_SLEEP_BASE = float(os.getenv("API_SLEEP_BASE", "0.3"))

# SYNC_MODE 환경변수:
#   "upsert"       (기본) — 신규 INSERT + 기존 UPDATE
#   "insert_only"          — 신규 INSERT만, 기존 레코드는 건드리지 않음
#   "dry_run"              — DB 변경 없이 예상 동작만 로그 출력
SYNC_MODE = os.getenv("SYNC_MODE", "upsert").lower()

KST = timezone(timedelta(hours=9))

# =========================
# DB 연결 (함수당 1개 재사용)
# =========================
def db_connect() -> pg8000.native.Connection:
    return pg8000.native.Connection(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_NAME,
        port=DB_PORT,
    )

# =========================
# Meta Graph API helpers
# =========================
def _is_rate_limit(err: Dict) -> bool:
    return err.get("code") in (4, 17, 32) or err.get("error_subcode") in (2446079,)

def graph_get(path: str, params: Dict[str, Any] = {}) -> Dict:
    """
    단건 GET. Rate limit 시 exponential backoff 재시도.
    HTTP 수준 오류(4xx/5xx)도 재시도 대상에 포함.
    """
    url = f"{BASE_URL}/{path.lstrip('/')}"
    p   = {"access_token": ACCESS_TOKEN, **params}

    for attempt in range(1, MAX_RETRIES + 1):
        if API_SLEEP_BASE > 0:
            time.sleep(API_SLEEP_BASE)
        try:
            r = requests.get(url, params=p, timeout=30)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"GET {path} 네트워크 오류: {e}")
            sleep_s = min(60, (2 ** attempt) + random.random() * 2)
            print(f"⚠️  네트워크 오류, {sleep_s:.1f}s 후 재시도 (attempt {attempt}): {e}")
            time.sleep(sleep_s)
            continue

        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"Non-JSON: status={r.status_code} text={r.text[:200]}")

        if "error" in data:
            err = data["error"]
            if _is_rate_limit(err):
                sleep_s = min(60, (2 ** attempt) + random.random() * 2)
                print(f"⚠️  Rate limit code={err.get('code')}, {sleep_s:.1f}s 후 재시도 (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"Graph API 오류: {err}")

        return data

    raise RuntimeError(f"GET {path} — {MAX_RETRIES}회 재시도 후 실패")


def get_all_pages(path: str, params: Dict[str, Any] = {}) -> List[Dict]:
    """페이지네이션을 끝까지 소비해 data[] 전체 반환."""
    results: List[Dict] = []
    data = graph_get(path, {"limit": 200, **params})
    results.extend(data.get("data", []))

    while True:
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        # next URL에는 access_token이 이미 포함돼 있음
        try:
            r    = requests.get(next_url, timeout=30)
            data = r.json()
        except Exception as e:
            print(f"⚠️  페이지네이션 오류, 중단: {e}")
            break
        if "error" in data:
            print(f"⚠️  페이지네이션 API 오류: {data['error']}")
            break
        results.extend(data.get("data", []))

    return results


# =========================
# API 수집 함수
# =========================
def validate_token() -> str:
    """토큰 유효성 확인 후 사용자 이름 반환."""
    data = graph_get("me", {"fields": "id,name"})
    return data.get("name", "Unknown")


def fetch_businesses() -> List[Dict]:
    """
    /me/businesses
    → 토큰 사용자가 '사람(person)' 권한으로 직접 속한 비즈니스 포트폴리오만 반환.
      파트너 권한으로만 연결된 비즈니스는 여기에 나타나지 않음.
    """
    return get_all_pages("me/businesses", {"fields": "id,name"})


def fetch_owned_ig_accounts(business_id: str) -> List[Dict]:
    """
    /{business_id}/owned_instagram_accounts
    → 비즈니스가 실제로 소유(own)한 IG 계정만 반환.
      파트너·클라이언트 연결 IG는 포함되지 않아 이중 매핑 방지.
    """
    try:
        return get_all_pages(
            f"{business_id}/owned_instagram_accounts",
            {"fields": "id,username"},
        )
    except RuntimeError as e:
        print(f"  ⚠️  [{business_id}] owned_instagram_accounts 조회 실패: {e}")
        return []


def fetch_ad_accounts() -> List[Dict]:
    """
    /me/adaccounts
    → ad_account 테이블 갱신용. IG 매핑은 이 결과에 의존하지 않음.
    """
    return get_all_pages(
        "me/adaccounts",
        {
            "fields": (
                "id,name,account_status,created_time,"
                "business,"
                "connected_instagram_account{id,username},"
                "instagram_accounts{id,username}"
            )
        },
    )


# =========================
# 유틸
# =========================
def parse_created_time(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("+0000", "+00:00"))
        return dt.astimezone(KST)
    except Exception:
        try:
            dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc).astimezone(KST)
        except Exception:
            return None


# =========================
# DB Upsert 함수 (공통 Connection 재사용)
# =========================
def upsert_business_portfolios(
    con: pg8000.native.Connection,
    businesses: List[Dict],
    now: datetime,
) -> Dict[str, int]:
    saved = updated = skipped = errors = 0

    for biz in businesses:
        try:
            biz_id   = int(biz["id"])
            biz_name = biz.get("name")

            exists = con.run(
                "SELECT 1 FROM business_portfolio WHERE business_id = :bid",
                bid=biz_id,
            )
            if exists:
                if SYNC_MODE == "dry_run":
                    print(f"  [DRY UPDATE BP] {biz_id}  {biz_name}")
                    updated += 1
                elif SYNC_MODE == "insert_only":
                    skipped += 1
                else:  # upsert
                    con.run(
                        """
                        UPDATE business_portfolio
                        SET business_name = :name, updated_at = :now
                        WHERE business_id = :bid
                        """,
                        name=biz_name, now=now, bid=biz_id,
                    )
                    updated += 1
            else:
                if SYNC_MODE == "dry_run":
                    print(f"  [DRY INSERT BP] {biz_id}  {biz_name}")
                    saved += 1
                else:  # upsert, insert_only 모두 신규는 INSERT
                    con.run(
                        """
                        INSERT INTO business_portfolio (business_id, business_name, created_at, updated_at)
                        VALUES (:bid, :name, :now, :now)
                        """,
                        bid=biz_id, name=biz_name, now=now,
                    )
                    print(f"  [NEW BP] {biz_id}  {biz_name}")
                    saved += 1

        except Exception as e:
            errors += 1
            print(f"  ❌ business_portfolio 오류 biz_id={biz.get('id')}: {e}")

    print(f"  business_portfolio → 신규={saved}  갱신={updated}  스킵={skipped}  오류={errors}")
    return {"saved": saved, "updated": updated, "skipped": skipped, "errors": errors}


def upsert_ig_accounts(
    con: pg8000.native.Connection,
    ig_rows: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, int]:
    """
    ig_account upsert.
    - business_id가 business_portfolio에 없으면 SKIP (NOT NULL FK 제약 위반 방지)
    - ig_user_id 기준 중복 시 업데이트, business_id 변경 시 로그 출력
    """
    saved = updated = skipped = errors = 0

    for row in ig_rows:
        try:
            ig_user_id  = row["ig_user_id"]
            business_id = row["business_id"]
            username    = row.get("username")

            # FK 사전 검증 — business_portfolio에 없으면 skip
            bp_ok = con.run(
                "SELECT 1 FROM business_portfolio WHERE business_id = :bid",
                bid=business_id,
            )
            if not bp_ok:
                print(f"  [SKIP IG] ig_user_id={ig_user_id} — business_id={business_id} not in business_portfolio")
                skipped += 1
                continue

            existing = con.run(
                "SELECT ig_id, business_id FROM ig_account WHERE ig_user_id = :uid",
                uid=ig_user_id,
            )
            if existing:
                ig_id, cur_biz = existing[0]
                if SYNC_MODE == "dry_run":
                    change = f"  business_id: {cur_biz} → {business_id}" if cur_biz != business_id else ""
                    print(f"  [DRY UPDATE IG] ig_user_id={ig_user_id} (@{username}){change}")
                    updated += 1
                elif SYNC_MODE == "insert_only":
                    skipped += 1
                else:  # upsert
                    con.run(
                        """
                        UPDATE ig_account
                        SET business_id = :bid,
                            username    = :uname,
                            updated_at  = :now
                        WHERE ig_user_id = :uid
                        """,
                        bid=business_id, uname=username, now=now, uid=ig_user_id,
                    )
                    if cur_biz != business_id:
                        print(f"  [FIX IG] ig_user_id={ig_user_id} (@{username}) "
                              f"business_id: {cur_biz} → {business_id}")
                    updated += 1
            else:
                if SYNC_MODE == "dry_run":
                    print(f"  [DRY INSERT IG] ig_user_id={ig_user_id} (@{username}) business_id={business_id}")
                    saved += 1
                else:  # upsert, insert_only 모두 신규는 INSERT
                    con.run(
                        """
                        INSERT INTO ig_account
                            (ig_user_id, business_id, username, is_active,
                             connected_at, created_at, updated_at)
                        VALUES
                            (:uid, :bid, :uname, true, :now, :now, :now)
                        """,
                        uid=ig_user_id, bid=business_id, uname=username, now=now,
                    )
                    print(f"  [NEW IG] ig_user_id={ig_user_id} (@{username}) business_id={business_id}")
                    saved += 1

        except Exception as e:
            errors += 1
            print(f"  ❌ ig_account 오류 ig_user_id={row.get('ig_user_id')}: {e}")

    print(f"  ig_account → 신규={saved}  갱신={updated}  스킵={skipped}  오류={errors}")
    return {"saved": saved, "updated": updated, "skipped": skipped, "errors": errors}


def upsert_ad_accounts(
    con: pg8000.native.Connection,
    ad_accounts: List[Dict],
    exclude_names: List[str],
    now: datetime,
) -> Dict[str, int]:
    """
    ad_account upsert.
    - EXCLUDED_ACCOUNT_NAMES에 포함된 계정은 skip
    - business_id FK 검증: business_portfolio에 없으면 NULL로 저장
    - ig_user_id: API에서 None 이면 기존 DB 값 유지
    """
    saved = updated = skipped = errors = 0

    for acc in ad_accounts:
        try:
            fb_id = acc.get("id")
            name  = acc.get("name")

            if not fb_id:
                errors += 1
                continue

            if name in exclude_names:
                skipped += 1
                continue

            # business 파싱
            business    = acc.get("business") or {}
            raw_biz_id  = business.get("id") if isinstance(business, dict) else business
            biz_name    = business.get("name") if isinstance(business, dict) else None

            # business_id FK 검증
            validated_biz_id: Optional[int] = None
            if raw_biz_id:
                chk = con.run(
                    "SELECT 1 FROM business_portfolio WHERE business_id = :bid",
                    bid=int(raw_biz_id),
                )
                if chk:
                    validated_biz_id = int(raw_biz_id)
                else:
                    print(f"  ⚠️  [{fb_id}] business_id={raw_biz_id} not in BP → NULL")

            # connected IG 계정 수집 (ad_account.ig_user_id 전용)
            ig_user_id: Optional[str] = None
            connected_ig = acc.get("connected_instagram_account")
            if isinstance(connected_ig, dict) and connected_ig.get("id"):
                ig_user_id = connected_ig["id"]
            if not ig_user_id:
                ig_raw = acc.get("instagram_accounts") or {}
                for ig in ig_raw.get("data", []):
                    if ig.get("id"):
                        ig_user_id = ig["id"]
                        break

            existing = con.run(
                "SELECT account_id, ig_user_id, business_id, business_name FROM ad_account WHERE fb_ad_account_id = :fb_id",
                fb_id=fb_id,
            )
            if existing:
                account_id, cur_ig, cur_biz_id, cur_biz_name = existing[0]
                # API에서 None이면 기존 DB 값 유지 (수동 입력값 보호)
                final_ig      = ig_user_id      if ig_user_id      is not None else cur_ig
                final_biz_id  = validated_biz_id if validated_biz_id is not None else cur_biz_id
                final_biz_name = biz_name        if biz_name        is not None else cur_biz_name
                if SYNC_MODE == "dry_run":
                    print(f"  [DRY UPDATE AD] {fb_id}  {name}  "
                          f"business_id={final_biz_id}  ig_user_id={final_ig}")
                    updated += 1
                elif SYNC_MODE == "insert_only":
                    skipped += 1
                else:  # upsert
                    con.run(
                        """
                        UPDATE ad_account
                        SET account_name   = :name,
                            account_status = :status,
                            created_at     = :created_at,
                            business_id    = :bid,
                            business_name  = :bname,
                            ig_user_id     = :ig_uid
                        WHERE account_id = :aid
                        """,
                        name=name, status=acc.get("account_status"),
                        created_at=parse_created_time(acc.get("created_time")),
                        bid=final_biz_id, bname=final_biz_name,
                        ig_uid=final_ig, aid=account_id,
                    )
                    updated += 1
                    print(f"  [UPD AD] {fb_id}  {name}  "
                          f"business_id={final_biz_id}  ig_user_id={final_ig}")
            else:
                if SYNC_MODE == "dry_run":
                    print(f"  [DRY INSERT AD] {fb_id}  {name}  ig_user_id={ig_user_id}")
                    saved += 1
                else:  # upsert, insert_only 모두 신규는 INSERT
                    con.run(
                        """
                        INSERT INTO ad_account
                            (fb_ad_account_id, account_name, account_status,
                             created_at, business_id, business_name, ig_user_id)
                        VALUES
                            (:fb_id, :name, :status,
                             :created_at, :bid, :bname, :ig_uid)
                        """,
                        fb_id=fb_id, name=name, status=acc.get("account_status"),
                        created_at=parse_created_time(acc.get("created_time")),
                        bid=validated_biz_id, bname=biz_name,
                        ig_uid=ig_user_id,
                    )
                    saved += 1
                    print(f"  [NEW AD] {fb_id}  {name}")

        except Exception as e:
            errors += 1
            print(f"  ❌ ad_account 오류 {acc.get('id')}: {e}")

    print(f"  ad_account → 신규={saved}  갱신={updated}  제외={skipped}  오류={errors}")
    return {"saved": saved, "updated": updated, "skipped": skipped, "errors": errors}


# =========================
# Lambda handler
# =========================
def lambda_handler(event, context):
    start = time.time()
    now   = datetime.now(KST)
    mode_label = {"upsert": "UPSERT (신규+갱신)", "insert_only": "INSERT ONLY (신규만)", "dry_run": "DRY RUN (변경 없음)"}
    print(f"🚀 [{now.isoformat()}] Meta 전체 동기화 시작  MODE={mode_label.get(SYNC_MODE, SYNC_MODE)}")

    # 제외 계정명
    exclude_names = [
        n.strip()
        for n in os.environ.get("EXCLUDED_ACCOUNT_NAMES", "").split(",")
        if n.strip()
    ]
    if exclude_names:
        print(f"🚫 제외 계정명: {exclude_names}")

    # ── Step 1: 토큰 검증 ────────────────────────────────────────────────────
    print("\n🔑 토큰 검증 중...")
    try:
        user_name = validate_token()
        print(f"  → 유효: {user_name}")
    except RuntimeError as e:
        print(f"❌ 토큰 오류: {e}")
        return {"statusCode": 401, "body": json.dumps({"error": str(e)})}

    # ── Step 2: API 수집 ─────────────────────────────────────────────────────
    #
    # [business_portfolio]
    #   소스 ①  /me/businesses          → 사람 권한 BP (owned_ig 조회 가능)
    #   소스 ②  /me/adaccounts.business → 광고계정 경유 BP 보완
    #   두 소스를 병합해 business_portfolio를 최대한 완성함.
    #   소스 ①이 항상 우선, 소스 ②는 ①에 없는 BP만 추가.
    #
    # [ig_account]
    #   소스 ①의 BP에 대해서만 owned_instagram_accounts 조회 가능.
    #   소스 ②로 추가된 BP는 권한 부족으로 IG 조회 불가 → ig_account 미갱신.
    #
    # [ad_account]
    #   /me/adaccounts (광고계정 메타 정보, IG 매핑 출처로는 사용 안 함)
    #
    print("\n📡 [1/2] /me/businesses 조회...")
    businesses = fetch_businesses()
    print(f"  → {len(businesses)}개 비즈니스 포트폴리오 (사람 권한)")

    # 각 비즈니스의 owned IG 수집 (소스 ① 전용)
    raw_ig_rows: List[Dict[str, Any]] = []
    for biz in businesses:
        biz_id = biz["id"]
        ig_list = fetch_owned_ig_accounts(biz_id)
        print(f"  [{biz_id}] {biz.get('name', '')} — IG {len(ig_list)}개")
        for acc in ig_list:
            raw_ig_rows.append({
                "ig_user_id": int(acc["id"]),
                "business_id": int(biz_id),
                "username": acc.get("username"),
            })

    # ig_user_id 중복 제거: 먼저 나온 비즈니스(사람 권한 순) 우선
    seen_ig: Dict[int, Dict] = {}
    for row in raw_ig_rows:
        if row["ig_user_id"] not in seen_ig:
            seen_ig[row["ig_user_id"]] = row
    ig_rows = list(seen_ig.values())
    print(f"  → IG 총 {len(raw_ig_rows)}개 수집, 중복 제거 후 {len(ig_rows)}개")

    print("\n📡 [2/2] /me/adaccounts 조회...")
    raw_ad_accounts = fetch_ad_accounts()
    print(f"  → {len(raw_ad_accounts)}개 광고계정")

    # 소스 ① 기준 map 생성 후, 소스 ②③으로 누락 BP 보완
    all_biz_map: Dict[int, Dict] = {int(b["id"]): b for b in businesses}

    # 소스 ②: adaccounts 응답의 business 필드
    no_biz_ad_ids: List[str] = []   # business=None 인 광고계정 → 소스 ③ 대상
    for acc in raw_ad_accounts:
        business = acc.get("business") or {}
        if not isinstance(business, dict) or not business.get("id"):
            no_biz_ad_ids.append(acc["id"])
            continue
        biz_id = int(business["id"])
        if biz_id not in all_biz_map:
            all_biz_map[biz_id] = {"id": str(biz_id), "name": business.get("name")}
            print(f"  [BP 보완-API] {biz_id}  {business.get('name')}")

    # 소스 ③: business=None 인 광고계정을 단건 직접 재조회
    # /me/adaccounts 배치 응답과 단건 조회는 다른 결과를 줄 수 있음
    if no_biz_ad_ids:
        print(f"  → business=None 광고계정 {len(no_biz_ad_ids)}개 단건 재조회 중...")
        for fb_id in no_biz_ad_ids:
            try:
                detail = graph_get(fb_id, {"fields": "id,name,business{id,name}"})
                business = detail.get("business") or {}
                if isinstance(business, dict) and business.get("id"):
                    biz_id = int(business["id"])
                    if biz_id not in all_biz_map:
                        all_biz_map[biz_id] = {"id": str(biz_id), "name": business.get("name")}
                        print(f"  [BP 보완-직접조회] {biz_id}  {business.get('name')}  (광고계정: {fb_id})")
                else:
                    print(f"  ⚠️  [{fb_id}] 단건 재조회에도 business=None — 수동 추가 필요")
            except RuntimeError as e:
                print(f"  ⚠️  [{fb_id}] 단건 재조회 실패: {e}")

    all_businesses = list(all_biz_map.values())
    print(f"  → business_portfolio 최종 {len(all_businesses)}개 "
          f"(사람권한 {len(businesses)} + 보완 {len(all_businesses) - len(businesses)})")

    # ── Step 3: DB 저장 (Connection 1개 재사용) ───────────────────────────────
    con = db_connect()
    try:
        print("\n💾 [1/3] business_portfolio 저장...")
        bp_result = upsert_business_portfolios(con, all_businesses, now)

        # ig_account 저장은 반드시 business_portfolio 이후
        # (ig_account.business_id FK → business_portfolio)
        print("\n💾 [2/3] ig_account 저장...")
        ig_result = upsert_ig_accounts(con, ig_rows, now)

        print("\n💾 [3/3] ad_account 저장...")
        ad_result = upsert_ad_accounts(con, raw_ad_accounts, exclude_names, now)

    finally:
        try:
            con.close()
        except Exception:
            pass

    elapsed = round(time.time() - start, 2)
    print(f"\n✅ 완료 elapsed={elapsed}s")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Success",
                "business_portfolio": bp_result,
                "ig_account": ig_result,
                "ad_account": ad_result,
                "elapsed_s": elapsed,
            },
            ensure_ascii=False,
        ),
    }
