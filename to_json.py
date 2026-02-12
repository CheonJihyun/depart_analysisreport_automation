import json
import pandas as pd
import numpy as np
from datetime import datetime
# 기존에 사용하시던 스크립트 임포트 (경로에 맞춰 유지)
from scripts.processor import (
    get_account_name, get_active_ad_count, get_total_content_count,
    get_ad_period, get_content_period, get_total_keyword_count,
    get_instagram_followers, get_ctr_data, get_organic_data, get_organic_monthly_data, get_imp_threshold, 
    get_content_ctr_data, get_a_content_target_ctr_data, get_profile_visits_monthly,
    get_target_avg_imp_ctr, get_target_avg_imp_ctr_threshold,
    get_raw_keyword_performance, filter_keywords_by_pos, get_overall_ctr,
    get_strategic_performance,get_essence_target_performance,get_variable_target_performance
)

def run(target_id, fb_ad_account_id, start, end, main_age="", main_gender="", avoid_age="", avoid_gender=""):
    # 1. 기본 설정 및 파라미터

    acc_name = get_account_name(target_id)
    ad_start, ad_end = get_ad_period(target_id, start, end)
    content_start, content_end = get_content_period(target_id, start, end)
    
    # 2. 결과 저장용 구조 (핵심)
    final_report = {
        "meta": {
            "account_name": acc_name,
            "period": f"{start} ~ {end}",
            "period_ads": f"{ad_start} ~ {ad_end}",
            "period_contents": f"{content_start} ~ {content_end}",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "summary": {
            "total_ads": get_active_ad_count(target_id, start, end),
            "total_contents": get_total_content_count(target_id, start, end),
            "total_keywords": get_total_keyword_count(target_id, start, end)
        },
        "datasets": {}
    }

    # 데이터셋 추가를 위한 헬퍼 함수
    def add_ds(key, kind, title, df, unit="", x=None, ys=None, extra_meta=None):
        if df is None or df.empty: return
        
        # [수정포인트 1] 모든 함수의 인덱스 문제를 한 번에 해결
        df_c = df.copy().reset_index() if df.index.name else df.copy()
        columns = list(df_c.columns)
        
        # [수정포인트 2] X축 매칭 로직 개선 (테이블일 때는 X축을 억지로 잡지 않음)
        if kind != "table" and x not in columns:
            possible_x = [c for c in columns if any(word in c.lower() for word in ['date', 'at', 'start', 'week', 'time'])]
            x = possible_x[0] if possible_x else columns[0]

        # 날짜 변환 (x가 컬럼에 있고, 실제 날짜 성격일 때만 실행)
        if x in columns and ('date' in x.lower() or 'time' in x.lower() or 'at' in x.lower()):
            try:
                df_c[x] = pd.to_datetime(df_c[x], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d')
            except:
                pass

        data_obj = {"kind": kind, "title": title, "unit": unit}
        if extra_meta: data_obj.update(extra_meta)

        if kind == "table":
            # 테이블은 모든 컬럼을 보존하여 rows에 담음
            data_obj["rows"] = df_c.replace({pd.NA: None, pd.NaT: None, np.nan: None}).to_dict(orient='records')
        else:
            # 차트(bar, line 등)일 때만 x를 분리하여 labels로 사용
            data_obj["labels"] = df_c[x].fillna("Unknown").tolist()
            
            series_data = []
            # 지정된 Y축이 있으면 그것만, 없으면 숫자 컬럼 전체를 가져옴
            target_ys = ys if ys else df_c.select_dtypes(include=['number']).columns.tolist()
            
            for y_req in target_ys:
                matched_col = next((c for c in columns if y_req.lower() == c.lower() or y_req.lower() in c.lower()), None)
                if matched_col and matched_col != x: # X축과 중복 방지
                    clean_data = df_c[matched_col].fillna(0).tolist()
                    series_data.append({"name": matched_col, "data": clean_data})
            
            data_obj["series"] = series_data
            
        final_report["datasets"][key] = data_obj

    # --- 데이터 수집 및 변환 시작 ---

    # 1. 인스타그램 및 오가닉 추이
    print("인스타그램 및 오가닉 추이 생성 중...")
    insta_df = get_instagram_followers(fb_ad_account_id, start, end)

    # 'date' -> 'updated_at'으로 수정
    add_ds("insta_followers", "line", "팔로워 추이", insta_df, "명", "updated_at", ["follower_count"])
    
    # 'profile_visit_count' -> 'profile_views'로 수정
    add_ds("insta_profile_visits", "line", "프로필 방문 수", insta_df, "회", "updated_at", ["profile_views"])

    # 월간 프로필 방문수 데이터 로드
    profile_monthly_df = get_profile_visits_monthly(fb_ad_account_id, start, end)

    add_ds(
        "insta_profile_visits_monthly",
        "line",                   # 방문수는 막대 그래프가 보기 편합니다
        "인스타그램 프로필 방문수 (월간)", 
        profile_monthly_df, 
        "회", 
        "updated_at", 
        ["profile_views"]
    )
    
    organic_df = get_organic_data(target_id, start, end)
    add_ds("organic_trend", "line", "오가닉 조회수 추이", organic_df, "회", "date_start", ["organic_impressions"])


    # 4주 단위 월간 데이터 바로 가져오기
    organic_monthly_df = get_organic_monthly_data(target_id, start, end)

    add_ds(
        "organic_trend_monthly", 
        "line", 
        "오가닉 조회수 추이 (월간)", 
        organic_monthly_df, 
        "회", 
        "date_start", 
        ["organic_impressions"]
    )




    # 2. CTR 추이
    print("CTR 추이 생성 중...")
    ctr_df = get_ctr_data(target_id, start, end)
    add_ds("ctr_trend", "line", "주차별 CTR 추이", ctr_df, "%", "week_start", ["ctr"])


    _, threshold = get_imp_threshold(target_id, start, end)
    # 3. 타겟 히트맵 데이터 (노출/CTR)
    print("타겟 히트맵 데이터 (노출/CTR) 생성 중...")
    target_df = get_target_avg_imp_ctr_threshold(target_id, start, end, threshold)
    # 히트맵은 테이블 형태가 시각화하기 좋음
    add_ds("target_heatmap", "table", "타겟별 노출 및 CTR 성과", target_df)

    # 4. 키워드 분석 (전체/메인/기피 + 명사/형용사)
    print("키워드 분석 (전체/메인/기피 + 명사/형용사) 생성 중...")
    target_configs = [
        ("overall", None, None, "전체"),
        ("main", main_age, main_gender, "메인 타겟"),
        ("avoid", avoid_age, avoid_gender, "기피 타겟")
    ]

    for prefix, age, gen, label in target_configs:
        if prefix != "overall" and not (age and gen): continue
        
        # 상위/하위 키워드 성과
        for is_top in [True, False]:
            suffix = "top" if is_top else "bottom"
            raw_kw_df = get_raw_keyword_performance(target_id, start, end, age, gen, is_top=is_top)
            
            # 명사 필터링
            nouns = filter_keywords_by_pos(raw_kw_df, 'noun')
            add_ds(f"{prefix}_{suffix}_noun", "bar_h", f"{label} {suffix.upper()} 10 (명사)", nouns, "%", "keyword", ["ctr"])
            
            # 형용사 필터링
            vas = filter_keywords_by_pos(raw_kw_df, 'verb_adj')
            add_ds(f"{prefix}_{suffix}_va", "bar_h", f"{label} {suffix.upper()} 10 (형용사)", vas, "%", "keyword", ["ctr"])

        # to_json.py 내부
        strat_df = get_strategic_performance(target_id, start, end, age, gen)
        if strat_df is not None:
            top_combos = strat_df[['ess_1', 'ess_2', 'combo_overall_ctr']].drop_duplicates().head(6)
            add_ds(f"{prefix}_keyword_combo", "table", f"{label} 키워드 조합 상위", top_combos)

    # 5. 콘텐츠별 타겟 성과 (상/하위)
    print("콘텐츠별 타겟 성과 (상/하위) 생성 중...")
    for is_top in [True, False]:
        suffix = "top" if is_top else "bottom"
        contents = get_content_ctr_data(target_id, start, end, threshold, is_top=is_top)
        
        content_results = []
        for item in contents[:3]: # 상/하위 3개씩
            detail_df = get_a_content_target_ctr_data(item["ad_id"], start, end)
            if detail_df is not None:
                # 상세 타겟 데이터를 리스트로 변환하여 포함
                item["target_details"] = detail_df.to_dict(orient='records')
            content_results.append(item)
        
        final_report["datasets"][f"content_{suffix}_analysis"] = {
            "kind": "content_card",
            "title": f"성과 {suffix} 콘텐츠 분석",
            "items": content_results
        }

    # --- [추가] 6. 별첨 자료용 키워드 상세 분석 (4페이지 분량) ---
    
    # 데이터 불러오기
    print("별첨 자료용 키워드 상세 분석 생성 중...")
    df_ess = get_essence_target_performance(target_id, start, end)
    df_var = get_variable_target_performance(target_id, start, end)

    def format_rows(df, col_indices):
        """데이터프레임에서 특정 인덱스의 컬럼만 추출하여 리스트로 반환"""
        if df is None or df.empty: return []
        # NaN 처리 후 리스트 변환
        temp_df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
        return temp_df.iloc[:, col_indices].values.tolist()

    # 가공된 아이템 리스트 생성
    print("아이템 리스트 생성 중...")
    appendix_items = [
        {
            "title": "많이 사용한 업종 필수 키워드 - 노출",
            "subtitle": "키워드가 가장 많이 노출된 타겟",
            "headers": ["랭킹", "키워드", "등장 광고 수", "최다 노출 타겟", "타겟 노출량", "노출 비중", "총 노출량"], # '순위' 추가
            "rows": [[i + 1] + row for i, row in enumerate(format_rows(df_ess, [0, 1, 2, 3, 4, 5]))], # 순위 결합
            "footnote": "*등장 광고 수 상위 10개 기준"
        },
        {
            "title": "많이 사용한 업종 필수 키워드 - 클릭",
            "subtitle": "키워드가 가장 많이 노출된 타겟",
            "headers": ["랭킹", "키워드", "등장 광고 수", "최다 클릭 타겟", "타겟 클릭량", "클릭 비중", "총 클릭량"],
            "rows": [[i + 1] + row for i, row in enumerate(format_rows(df_ess, [0, 1, 6, 7, 8, 9]))],
            "footnote": "*등장 광고 수 상위 10개 기준"
        },
        {
            "title": "많이 사용한 업종 변수 키워드 - 노출",
            "subtitle": "키워드가 가장 많이 노출된 타겟",
            "headers": ["랭킹", "키워드", "등장 광고 수", "최다 노출 타겟", "타겟 노출량", "노출 비중", "총 노출량"],
            "rows": [[i + 1] + row for i, row in enumerate(format_rows(df_var, [0, 1, 2, 3, 4, 5]))],
            "footnote": "*등장 광고 수 상위 10개 기준"
        },
        {
            "title": "많이 사용한 업종 변수 키워드 - 클릭",
            "subtitle": "키워드가 가장 많이 노출된 타겟",
            "headers": ["랭킹", "키워드", "등장 광고 수", "최다 클릭 타겟", "타겟 클릭량", "클릭 비중", "총 클릭량"],
            "rows": [[i + 1] + row for i, row in enumerate(format_rows(df_var, [0, 1, 6, 7, 8, 9]))],
            "footnote": "*등장 광고 수 상위 10개 기준"
        }
    ]

    # 최종 리포트 구조에 삽입 (HTML 템플릿의 appendix_groups 구조에 맞춤)
    final_report["appendix_groups"] = [
        {
            "title": "",
            "items": appendix_items
        }
    ]

    # --- [기존] 6. 최종 JSON 저장 ---

    # 6. 최종 JSON 저장
    output_path = "json_reports/integrated_report.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, ensure_ascii=False, indent=4, default=str)
    
    print(f"✅ 모든 요구사항이 반영된 리포트 생성 완료: {output_path}")

if __name__ == "__main__":
    run()