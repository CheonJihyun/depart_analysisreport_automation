# main.py
from scripts.processor import get_account_name, get_active_ad_count, get_total_content_count,\
    get_ad_period, get_content_period, get_total_keyword_count,\
    get_ctr_data, get_organic_data, get_imp_threshold, get_content_ctr_data, get_a_content_target_ctr_data\
    
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


    target_age = ''
    target_gender = '' 
    avoid_age = ''
    avoid_gender = ''

    top_thumbnail1 =''
    top_thumbnail2 =''
    top_thumbnail3 =''
    bottom_thumbnail1 =''
    bottom_thumbnail2 =''
    bottom_thumbnail3 =''


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
        "top_note": f"노출수가 전체 노출수({total_imp})의 0.05%({threshold:.0f}) 이하인 컨텐츠 및 타겟은 제외",
        "top": top_list,
        "bottom_note": f"노출수가 전체 노출수({total_imp})의 0.05%({threshold:.0f}) 이하인 컨텐츠 및 타겟은 제외",
        "bottom": bottom_list
        
        #[
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""},
        #    {"uploaded_at": "", "ctr": "", "thumbnail": "", "chart": ""}
        #]
        
    },

    # 3. charts 객체 (이미지 경로들)
    "charts": {
        "followers": "",
        "ctr": ctr_chart_path if ctr_chart_path else "",
        "organic_views_1": organic_chart_path if organic_chart_path else "",
        "organic_views_2": organic_chart_path_2 if organic_chart_path_2 else "",
        "profile_visits_1": "",
        "profile_visits_2": "",

        "heatmap_impressions": "",
        "heatmap_ctr": "",
        # 키워드 차트 (is defined 체크를 하므로 빈 문자열 혹은 None 설정)
        "keyword_overall_top": "",
        "keyword_overall_top_noun": "",
        "keyword_overall_top_verb_adj": "",
        "keyword_overall_bottom": "",
        "keyword_overall_bottom_noun": "",
        "keyword_overall_bottom_verb_adj": "",
        "keyword_main_top": "",
        "keyword_main_top_noun": "",
        "keyword_main_top_verb_adj": "",
        "keyword_main_bottom": "",
        "keyword_main_bottom_noun": "",
        "keyword_main_bottom_verb_adj": "",
        "keyword_avoid_top": "",
        "keyword_avoid_top_noun": "",
        "keyword_avoid_top_verb_adj": "",
        "keyword_avoid_bottom": "",
        "keyword_avoid_bottom_noun": "",
        "keyword_avoid_bottom_verb_adj": ""
    },

    # 4. annotations 객체 (칩/태그 형태 데이터)
    "annotations": {
        "ctr": [],     # [tag1, tag2, ...]
        "organic": []
    },


    # 6. target 객체 (타겟 성과)
    "target": {
        "impressions_rank": [], # [line1, line2, ...]
        "ctr_note": "",
        "ctr_rank": []
    },

    # 7. keywords 객체 (키워드 분석 리포트)
    "keywords": {
        "overall_top_note": "",
        "overall_top_tables": [
            {"title": "", "headers": [], "rows": [[]], "footnote": ""}
        ],
        "overall_combo_pages": [
            {
                "note": "",
                "cards": [{"title": "", "sub": "", "image": ""}]
            }
        ],
        "overall_bottom_note": "",
        "overall_bottom_tables": [],
        
        # 메인/기피 타겟 (있을 경우만 렌더링)
        "main_target": {"title": ""}, # None이면 해당 섹션 패스 (main_target 자체가)
        "main_top_tables": [],
        "main_combo_pages": [],
        "main_bottom_tables": [],
        
        "avoid_target": {"title": ""}, # None이면 해당 섹션 패스
        "avoid_top_tables": [],
        "avoid_combo_pages": [],
        "avoid_bottom_tables": []
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