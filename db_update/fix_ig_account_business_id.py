"""
fix_ig_account_business_id.py

ig_account 테이블의 business_id를 올바르게 수정하는 스크립트.

배경:
  - 계정들이 비즈니스 포트폴리오에 '사람' 권한과 '파트너' 권한 두 가지로 모두 연결되어 있어
    ig_account.business_id가 잘못된 비즈니스를 가리키는 경우가 발생함.
  - 올바른 business_id = 토큰 소유자가 직접 사람(멤버) 권한으로 속한 비즈니스 포트폴리오.

해결 방법:
  1. /me/businesses  →  토큰에 직접 연결된(사람 권한) 비즈니스 포트폴리오 목록 수집
  2. /{business_id}/instagram_accounts  →  각 비즈니스 산하 IG 계정 목록 수집
  3. business_portfolio 테이블에서 해당 business_id 존재 여부 검증
  4. ig_account 테이블의 business_id 업데이트 (불일치하는 행만)
"""

import os
import json
import time
import random
from typing import Dict, List, Optional, Tuple

import requests
import pg8000.native

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# =========================
# Config
# =========================
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v24.0")
BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
DB_HOST     = os.environ["DB_HOST"]
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "5"))
DRY_RUN      = os.getenv("DRY_RUN", "false").lower() == "true"   # true면 UPDATE 없이 출력만


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


def fetch_known_business_ids(con: pg8000.native.Connection) -> set:
    """business_portfolio 테이블에 존재하는 business_id 집합 반환."""
    rows = con.run("SELECT business_id FROM business_portfolio")
    return {str(r[0]) for r in rows}


def fetch_ig_accounts(con: pg8000.native.Connection) -> List[Tuple[str, str, Optional[str]]]:
    """ig_account 테이블의 (ig_id, ig_user_id, current_business_id) 목록 반환."""
    rows = con.run("SELECT ig_id, ig_user_id, business_id FROM ig_account")
    return [(str(r[0]), str(r[1]), str(r[2]) if r[2] is not None else None) for r in rows]


def update_ig_business_id(
    con: pg8000.native.Connection,
    ig_id: str,
    new_business_id: str,
) -> None:
    con.run(
        "UPDATE ig_account SET business_id = :business_id WHERE ig_id = :ig_id",
        business_id=new_business_id,
        ig_id=ig_id,
    )


# =========================
# Meta Graph helpers
# =========================
def _get(path: str, params: Optional[dict] = None) -> dict:
    """단순 GET 요청. 레이트 리밋 시 지수 백오프 재시도."""
    url = f"{BASE_URL}/{path.lstrip('/')}"
    p = {"access_token": ACCESS_TOKEN, **(params or {})}

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=p, timeout=30)
        data = resp.json()

        if "error" in data:
            err = data["error"]
            code = err.get("code")
            subcode = err.get("error_subcode")
            # 레이트 리밋 코드
            if code in (4, 17, 32) or subcode in (2446079,):
                sleep_s = min(60, (2 ** attempt) + random.random())
                print(f"  ⚠️  rate limit code={code} subcode={subcode} — {sleep_s:.1f}s 후 재시도 (attempt {attempt})")
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"Graph API 오류: {err}")

        return data

    raise RuntimeError(f"GET {path} — {MAX_RETRIES}회 재시도 후 실패")


def _get_paged(path: str, params: Optional[dict] = None) -> List[dict]:
    """페이징을 따라가며 data[] 전체 수집."""
    results = []
    p = dict(params or {})
    p.setdefault("limit", 200)

    while True:
        data = _get(path, p)
        results.extend(data.get("data", []))

        paging = data.get("paging", {})
        after = paging.get("cursors", {}).get("after") or paging.get("next")
        if not after or not paging.get("next"):
            break
        p["after"] = paging["cursors"]["after"]

    return results


# =========================
# 핵심 수집 함수
# =========================
def get_user_businesses() -> List[dict]:
    """
    토큰 소유자가 '사람(멤버)' 권한으로 속한 비즈니스 포트폴리오 목록.
    파트너로만 연결된 비즈니스는 여기에 포함되지 않음.
    """
    print("▶  /me/businesses 조회 중...")
    items = _get_paged("me/businesses", {"fields": "id,name"})
    print(f"   → 비즈니스 포트폴리오 {len(items)}개 발견")
    return items


def get_ig_accounts_for_business(business_id: str) -> List[dict]:
    """
    특정 비즈니스 산하의 Instagram 계정 조회.
    owned_instagram_accounts 엔드포인트 사용.
    """
    fields = "id,username,name"
    return _get_paged(f"{business_id}/owned_instagram_accounts", {"fields": fields})


# =========================
# 메인 픽스 로직
# =========================
def build_ig_to_business_map() -> Dict[str, str]:
    """
    API로부터 {ig_id: business_id} 매핑 생성.
    같은 IG 계정이 여러 비즈니스에 속할 경우 첫 번째(사람 권한 비즈니스)를 사용.
    """
    businesses = get_user_businesses()
    ig_to_biz: Dict[str, str] = {}

    for biz in businesses:
        biz_id   = str(biz["id"])
        biz_name = biz.get("name", "")
        print(f"   비즈니스 [{biz_id}] {biz_name} IG 계정 조회 중...")

        try:
            ig_accounts = get_ig_accounts_for_business(biz_id)
        except Exception as e:
            print(f"   ⚠️  [{biz_id}] IG 계정 조회 실패: {e}")
            continue

        print(f"      → {len(ig_accounts)}개 IG 계정")
        for acc in ig_accounts:
            ig_id = str(acc["id"])
            if ig_id not in ig_to_biz:
                ig_to_biz[ig_id] = biz_id
            else:
                print(f"      ℹ️  ig_id={ig_id} 이미 {ig_to_biz[ig_id]}에 매핑됨, {biz_id} 무시")

    return ig_to_biz


def run_fix():
    print("=" * 60)
    print("  ig_account.business_id 수정 스크립트")
    print(f"  DRY_RUN = {DRY_RUN}")
    print("=" * 60)

    # 1. API에서 올바른 매핑 수집
    ig_to_biz = build_ig_to_business_map()
    print(f"\n✅ API 매핑 완료: IG 계정 {len(ig_to_biz)}개\n")

    # 2. DB 연결
    con = db_connect()
    try:
        known_biz_ids = fetch_known_business_ids(con)
        db_ig_rows    = fetch_ig_accounts(con)

        print(f"DB: business_portfolio {len(known_biz_ids)}개, ig_account {len(db_ig_rows)}개\n")

        # 3. 비교 & 업데이트
        updated = 0
        skipped_no_api = 0
        skipped_no_biz = 0
        skipped_same   = 0

        for ig_id, ig_user_id, current_biz_id in db_ig_rows:
            new_biz_id = ig_to_biz.get(ig_user_id)

            if new_biz_id is None:
                print(f"  [SKIP] ig_id={ig_id} ig_user_id={ig_user_id} — API에서 매핑 없음 (현재: {current_biz_id})")
                skipped_no_api += 1
                continue

            if new_biz_id not in known_biz_ids:
                print(f"  [SKIP] ig_id={ig_id} ig_user_id={ig_user_id} — business_id={new_biz_id} 가 business_portfolio 테이블에 없음")
                skipped_no_biz += 1
                continue

            if current_biz_id == new_biz_id:
                skipped_same += 1
                continue

            print(f"  [UPDATE] ig_id={ig_id} ig_user_id={ig_user_id}  {current_biz_id!s:>20} → {new_biz_id}")
            if not DRY_RUN:
                update_ig_business_id(con, ig_id, new_biz_id)
            updated += 1

        print("\n" + "=" * 60)
        print(f"  완료  업데이트={updated}  이미_정확={skipped_same}"
              f"  API_매핑없음={skipped_no_api}  BP_없음={skipped_no_biz}")
        if DRY_RUN:
            print("  ※ DRY_RUN=true → DB 변경 없이 출력만 했습니다.")
        print("=" * 60)

    finally:
        try:
            con.close()
        except Exception:
            pass


# =========================
# Lambda / 직접 실행 진입점
# =========================
def lambda_handler(event, context):
    run_fix()
    return {"statusCode": 200, "body": json.dumps({"message": "ig_account business_id fix complete"})}


if __name__ == "__main__":
    run_fix()
