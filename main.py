# main.py
from scripts.processor import get_account_name, get_active_ad_count, get_total_content_count,\
        get_ad_period, get_content_period, get_total_keyword_count,\
    get_instagram_followers, get_ctr_data, get_organic_data, get_imp_threshold, get_content_ctr_data, \
        get_a_content_target_ctr_data,\
    get_target_avg_imp_ctr, get_target_avg_imp_ctr_threshold,\
    get_raw_keyword_performance, filter_keywords_by_pos, get_overall_ctr, \
        get_strategic_performance
    
from scripts.visualizer import visual, create_organic_trend_chart, create_ctr_trend_chart, \
    create_mini_chart_from_df
    
from scripts.reporter import generate_html
from datetime import datetime
import pandas as pd

def run():
    # 사용자가 입력하는 값 (나중에 Streamlit 입력창이 됨)
    target_id = 3
    start = "2025-02-13"
    end = "2025-12-31"

    main_age = ''
    main_gender = '' 
    avoid_age = '18-24'
    avoid_gender = 'male'

    # top_thumbnail1 =''
    # top_thumbnail2 =''
    # top_thumbnail3 =''
    # bottom_thumbnail1 =''
    # bottom_thumbnail2 =''
    # bottom_thumbnail3 =''


    # 계정명 가져오기 /
    acc_name = get_account_name(target_id)

    # 현재 시간을 "2024-05-20 14:30" 형태로 생성 /
    now = datetime.now()
    generated_at_str = now.strftime("%Y-%m-%d %H:%M")

    # -------------------------------

    # 광고 개수 가져오기 /
    total_ads = get_active_ad_count(target_id, start, end)

    # 콘텐츠 개수 가져오기 /
    total_contents = get_total_content_count(target_id, start, end)

    # 광고 진행 기간 /
    ad_start, ad_end = get_ad_period(target_id, start, end)

    # 콘텐츠 업로드 기간 /
    content_start, content_end = get_content_period(target_id, start, end)

    # 광고 콘텐츠 키워드 / 광고 개수와 키워드 개수 둘 다 동일한 기간(start, end)을 사용합니다.
    keyword_count = get_total_keyword_count(target_id, start, end)

    # -------------------------------

    # instagram 데이터 /
    instagram_df = get_instagram_followers(target_id, start, end)
    # 팔로워 //
    followers_chart_path = visual(instagram_df)
    profile_visits_chart_path = visual(instagram_df)

    # ctr 데이터 /
    ctr_df = get_ctr_data(target_id, start, end)
    # //
    ctr_chart_path = visual(ctr_df)

    # 오가닉 데이터 /
    # 2. 주차별 데이터 가져오기
    # 3. 데이터가 있을 때만 그래프 생성
    organic_df = get_organic_data(target_id, start, end)
    # // 오가닉1
    organic_chart_path = None
    if not organic_df.empty:
      organic_chart_path = visual(organic_df)
        # HTML에서 이미지를 불러올 수 있도록 상대 경로로 변환 (필요시)
        # chart_path = chart_path.replace('\\', '/') 

    # // 오가닉2
    organic_chart_path_2 = visual(organic_df)

    # ------------------------------------

    # 노출량 기준 가져오기 /
    total_imp, threshold = get_imp_threshold(target_id, start, end)
    
    content_top_ctr_results = get_content_ctr_data(target_id, start, end, threshold, is_top=True)
    # // 각각의 차트를 생성하고 top_list-chart에 추가

    top_list = [
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""}
    ]
    for i in range(len(content_top_ctr_results)):
        top_list[i]["uploaded_at"] = content_top_ctr_results[i]['uploaded_at']
        top_list[i]["ctr"] = content_top_ctr_results[i]['ctr']
        top_list[i]["thumbnail"] = content_top_ctr_results[i]['thumbnail']

        current_ad_df = get_a_content_target_ctr_data(content_top_ctr_results[i]["ad_id"], start, end)
    
        if current_ad_df is not None and not current_ad_df.empty:
            # 차트 생성 및 경로 할당 (ad_id를 파일명에 써서 중복 방지)
            chart_path = visual(current_ad_df)
            top_list[i]["chart"] = chart_path



    content_bottom_ctr_results = get_content_ctr_data(target_id, start, end, threshold, is_top=False)
    # // 각각의 차트를 생성하고 bottom_list-chart에 추가

    bottom_list = [
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""}
    ]
    for i in range(len(content_bottom_ctr_results)):
        bottom_list[i]["uploaded_at"] = content_bottom_ctr_results[i]['uploaded_at'] # NaT인 경우 있음!
        bottom_list[i]["ctr"] = content_bottom_ctr_results[i]['ctr']
        bottom_list[i]["thumbnail"] = content_bottom_ctr_results[i]['thumbnail']

        current_ad_df = get_a_content_target_ctr_data(content_bottom_ctr_results[i]["ad_id"], start, end)

        if current_ad_df is not None and not current_ad_df.empty:
            # 차트 생성 및 경로 할당 (ad_id를 파일명에 써서 중복 방지)
            chart_path = visual(current_ad_df)
            bottom_list[i]["chart"] = chart_path

    # -------------------------------

    # 타겟별 평균 노출 및 CTR 데이터 가져오기 /
    target_imp_ctr_df = get_target_avg_imp_ctr(target_id, start, end)

        # 1. impressions 기준으로 내림차순 정렬
    top_imp_rows = target_imp_ctr_df.sort_values(by='impressions', ascending=False).head(2)
    # 2. 1위 age + gender 문자열 생성 (예: "25-34 Male")
    top1_imp_target = f"{top_imp_rows.iloc[0]['age']} {top_imp_rows.iloc[0]['gender']}"
    # 3. 2위 age + gender 문자열 생성
    top2_imp_target = f"{top_imp_rows.iloc[1]['age']} {top_imp_rows.iloc[1]['gender']}"

        # 1. 필터링 기준이 되는 최소 노출수 계산 (전체의 0.05%)
    # min_impressions = target_imp_ctr_df['impressions'].sum() * 0.0005
    # 2. 조건에 맞는 데이터만 필터링

    target_imp_ctr_threshold_df = get_target_avg_imp_ctr_threshold(target_id, start, end, threshold)
    # 3. CTR 기준으로 내림차순 정렬 후 상위 2개 추출
    top_ctr_rows = target_imp_ctr_threshold_df.sort_values(by='ctr', ascending=False).head(2)
    # 4. 문자열 변수 생성
    top1_ctr_target = f"{top_ctr_rows.iloc[0]['age']} {top_ctr_rows.iloc[0]['gender']}"
    top2_ctr_target = f"{top_ctr_rows.iloc[1]['age']} {top_ctr_rows.iloc[1]['gender']}"


    # 노출 히트맵//
    target_imp_chart_path = visual(target_imp_ctr_df)
    # CTR 히트맵//
    target_ctr_chart_path = visual(target_imp_ctr_df)

    # -------------------------------

    # 1. DB에서 타겟(예: 20대 여성) 성과가 반영된 전체 키워드 데이터를 한 번에 가져옴
    # 2. Python 단에서 품사별로 쪼개기 / //
    overall_top_raw_keyword_df = get_raw_keyword_performance(target_id, start, end)
    overall_top_noun = filter_keywords_by_pos(overall_top_raw_keyword_df, pos_type='noun')   # 명사 Top 10
    overall_top_va = filter_keywords_by_pos(overall_top_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Top 10
    overall_top_noun_chart_path = visual(overall_top_noun)
    overall_top_va_chart_path = visual(overall_top_va)

    overall_bottom_raw_keyword_df = get_raw_keyword_performance(target_id, start, end, is_top=False)
    overall_bottom_noun = filter_keywords_by_pos(overall_bottom_raw_keyword_df, pos_type='noun')   # 명사 Top 10
    overall_bottom_va = filter_keywords_by_pos(overall_bottom_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Top 10
    overall_bottom_noun_chart_path = visual(overall_bottom_noun)
    overall_bottom_va_chart_path = visual(overall_bottom_va)

    if main_age and main_gender:
        main_top_raw_keyword_df = get_raw_keyword_performance(target_id, start, end, main_age, main_gender)
        main_top_noun = filter_keywords_by_pos(main_top_raw_keyword_df, pos_type='noun')   # 명사 Top 10
        main_top_va = filter_keywords_by_pos(main_top_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Top 10
        main_top_noun_chart_path = visual(main_top_noun)
        main_top_va_chart_path = visual(main_top_va)

        main_bottom_raw_keyword_df = get_raw_keyword_performance(target_id, start, end, main_age, main_gender, is_top=False)
        main_bottom_noun = filter_keywords_by_pos(main_bottom_raw_keyword_df, pos_type='noun')   # 명사 Bottom 10
        main_bottom_va = filter_keywords_by_pos(main_bottom_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Bottom 10
        main_bottom_noun_chart_path = visual(main_bottom_noun)
        main_bottom_va_chart_path = visual(main_bottom_va)

    if avoid_age and avoid_gender:
        avoid_top_raw_keyword_df = get_raw_keyword_performance(target_id, start, end, avoid_age, avoid_gender)
        avoid_top_noun = filter_keywords_by_pos(avoid_top_raw_keyword_df, pos_type='noun')   # 명사 Top 10
        avoid_top_va = filter_keywords_by_pos(avoid_top_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Top 10
        avoid_top_noun_chart_path = visual(avoid_top_noun)
        avoid_top_va_chart_path = visual(avoid_top_va)

        avoid_bottom_raw_keyword_df = get_raw_keyword_performance(target_id, start, end, avoid_age, avoid_gender, is_top=False)
        avoid_bottom_noun = filter_keywords_by_pos(avoid_bottom_raw_keyword_df, pos_type='noun')   # 명사 Bottom 10
        avoid_bottom_va = filter_keywords_by_pos(avoid_bottom_raw_keyword_df, pos_type='verb_adj') # 동사/형용사 Bottom 10
        avoid_bottom_noun_chart_path = visual(avoid_bottom_noun)
        avoid_bottom_va_chart_path = visual(avoid_bottom_va)

    
    age_gender_ctr_df = get_target_avg_imp_ctr(target_id, start, end)
    overall_ctr = get_overall_ctr(target_id, start, end)
    if main_age and main_gender : 
        df_main = age_gender_ctr_df[(age_gender_ctr_df['age']==main_age) & (age_gender_ctr_df['gender']==main_gender)]
        main_ctr = df_main.iloc[0]['ctr']
    if avoid_age and avoid_gender :
        df_avoid = age_gender_ctr_df[(age_gender_ctr_df['age']==avoid_age) & (age_gender_ctr_df['gender']==avoid_gender)]
        avoid_ctr = df_avoid.iloc[0]['ctr']


    # 상위 키워드 조합
    overall_keyword_comb_df = get_strategic_performance(target_id, start, end)
    # 1. 중복 제거하고 상위 6개 조합만 딱 뽑기
    top_combos = overall_keyword_comb_df[['ess_1', 'ess_2', 'combo_overall_ctr']].drop_duplicates().head(6)
    # 2. 리스트로 변환 (수동으로 채우기 가장 편한 상태)
    combo_list = top_combos.values.tolist()

    # 3. cards 구성 (가장 원하셨던 형태)
    cards = []
    for i, (e1, e2, ctr) in enumerate(combo_list, 1):
        cards.append({
            "title": f"업종 필수 키워드 조합 {i}위<br>{e1} {e2} ({ctr}%)",
            "sub": "함께 쓰인 브랜드 변수 키워드별 성과",
            "image": ""
        })
        




    # -------------------------------
    # 리포트 데이터 합치기
    context = {
    # 1. 공통 설정
    "css_path": "./templates/report.css",

    # 2. report 객체 (기본 정보 및 개요)
    "report": {
        "title": "보고서",            # <title>
        "client": acc_name,     # 클라이언트명
        "quarter_label": start+" ~ "+end,    # 분기 레이블 (예: 2024년 1분기)이 아닌 그냥 대상 기간
        # "year": "",             # 연도
        "generated_at": generated_at_str,     # 보고서 생성일
    # -------------------------------
        "period_ads": f"{ad_start} ~ 　　　　{ad_end}",       # 광고 진행 기간
        "period_contents": f"{content_start} ~ 　　　　{content_end}",  # 콘텐츠 업로드 기간
        "keyword_count": str(keyword_count)+"개",    # 광고 콘텐츠 키워드 수
        "overview_notes": [f"광고 {total_ads}개",f"콘텐츠 {total_contents}개"]    # 분석 개요 노트 (리스트: [line1, line2, ...])
    },
    # -------------------------------
    # 5. content 객체 (상/하위 콘텐츠 분석) 
    "content": {
        "top_note": f"노출수가 전체 노출수({total_imp:,})의 0.05%({threshold:,.0f}) 이하인 컨텐츠 및 타겟은 제외",
        "top": top_list,
        "bottom_note": f"노출수가 전체 노출수({total_imp:,})의 0.05%({threshold:,.0f}) 이하인 컨텐츠 및 타겟은 제외",
        "bottom": bottom_list
        
        #[
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""}
        #]
        
    },

    # 3. charts 객체 (이미지 경로들)
    "charts": {
        "followers": followers_chart_path if followers_chart_path else "",
        "ctr": ctr_chart_path if ctr_chart_path else "",
        "organic_views_1": organic_chart_path if organic_chart_path else "",
        "organic_views_2": organic_chart_path_2 if organic_chart_path_2 else "",
        "profile_visits_1": profile_visits_chart_path if profile_visits_chart_path else "",
        "profile_visits_2": profile_visits_chart_path if profile_visits_chart_path else "",

        "heatmap_impressions": target_imp_chart_path if target_imp_chart_path else "",
        "heatmap_ctr": target_ctr_chart_path if target_ctr_chart_path else "",
        # 키워드 차트 (is defined 체크를 하므로 빈 문자열 혹은 None 설정)
        "keyword_overall_top": "",
        "keyword_overall_top_noun": overall_top_noun_chart_path,
        "keyword_overall_top_verb_adj": overall_top_va_chart_path,

        "keyword_overall_bottom": "",
        "keyword_overall_bottom_noun": overall_bottom_noun_chart_path,
        "keyword_overall_bottom_verb_adj": overall_bottom_va_chart_path,

        "keyword_main_top": "",
        "keyword_main_top_noun": main_top_noun_chart_path if main_age and main_gender else None,
        "keyword_main_top_verb_adj": main_top_va_chart_path if main_age and main_gender else None,

        "keyword_main_bottom": "",
        "keyword_main_bottom_noun": main_bottom_noun_chart_path if main_age and main_gender else None,
        "keyword_main_bottom_verb_adj": main_bottom_va_chart_path if main_age and main_gender else None,

        "keyword_avoid_top": "",
        "keyword_avoid_top_noun": avoid_top_noun_chart_path if avoid_age and avoid_gender else None,
        "keyword_avoid_top_verb_adj": avoid_top_va_chart_path if avoid_age and avoid_gender else None,

        "keyword_avoid_bottom": "",
        "keyword_avoid_bottom_noun": avoid_bottom_noun_chart_path if avoid_age and avoid_gender else None,
        "keyword_avoid_bottom_verb_adj": avoid_bottom_va_chart_path if avoid_age and avoid_gender else None
    },

    # 4. annotations 객체 (칩/태그 형태 데이터) 
    "annotations": {
        "ctr": ["test1", "test2"],     # [tag1, tag2, ...]
        "organic": ["test3", "test4"]
    },


    # 6. target 객체 (타겟 성과) imp ctr 순위
    "target": {
        "impressions_rank": [f"1위 : {top1_imp_target}", f"2위 : {top2_imp_target}"], # [line1, line2, ...]
        "ctr_note": "",
        "ctr_rank": [f"1위 : {top1_ctr_target}",f"2위 : {top2_ctr_target}"]
    },

    # 7. keywords 객체 (키워드 분석 리포트)
    "keywords": {
        "overall_top_note": "*3개 이상의 콘텐츠에 등장한 단어만 표시",
        "overall_top_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ],
        "overall_combo_pages": [
            {
                "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어\
                    <br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {overall_ctr}%",
                "cards": cards
            }
        ],
        "overall_bottom_note": "*3개 이상의 콘텐츠에 등장한 단어만 표시",
        "overall_bottom_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ],
        
        # 메인/기피 타겟 (있을 경우만 렌더링)
        "main_target": {"title": f"{main_age} {main_gender} 성과 분석"} if main_age and main_gender else None, # None이면 해당 섹션 패스 (main_target 자체가)
        "main_top_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ] if main_age and main_gender else None,
        "main_combo_pages": [
            {
                "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어\
                    <br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {main_ctr}%",
                "cards": [{f"title": "업종 필수 키워드 조합 1위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 2위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 3위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 4위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 5위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 6위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""}]
            }
        ] if main_age and main_gender else None,
        "main_bottom_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ] if main_age and main_gender else None,
        
        
        "avoid_target": {"title": f"{avoid_age} {avoid_gender} 성과 분석"} if avoid_age and avoid_gender else None, # None이면 해당 섹션 패스
        "avoid_top_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ] if avoid_age and avoid_gender else None,
        "avoid_combo_pages": [
            {
                "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어\
                    <br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {avoid_ctr}%",
                "cards": [{f"title": "업종 필수 키워드 조합 1위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 2위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 3위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 4위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 5위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""},
                          {f"title": "업종 필수 키워드 조합 6위<br>{} {} ({})", "sub": "함께 쓰인 브랜드 변수 키워드별 성과", "image": ""}]
            }
        ] if avoid_age and avoid_gender else None,
        "avoid_bottom_tables": [
            {"title": "table_title_test", "headers": [], "rows": [[]], "footnote": "table_footnote_test"}
        ]  if avoid_age and avoid_gender else None
    },

    # 8. 별첨 자료 (Appendix)
    "appendix_groups": [
        {
            "title": "",
            "items": [
                {"title": "", "subtitle": "", "image": "", "headers": [], "rows": [[]], "footnote": ""}
            ]
        }
    ],
    "appendix": [] # appendix_groups가 없을 경우 사용되는 예비 리스트
    }
    # HTML 생성
    generate_html(context)
    print(f"✅ {acc_name} 리포트 생성 완료!")


if __name__ == "__main__":
    run()