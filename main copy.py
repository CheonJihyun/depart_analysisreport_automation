# main.py
from scripts.processor import get_account_name, get_organic_data, get_active_ad_count, \
    get_total_keyword_count, get_weekly_performance_data, get_top_ctr_keywords_direct, \
    get_top_ads_and_target_analysis,  get_bottom_ads_and_target_analysis
from scripts.visualizer import create_organic_trend_chart, create_ctr_trend_chart, \
    create_target_analysis_chart, create_bottom_target_chart
from scripts.reporter import generate_html

def run():
    # 사용자가 입력하는 값 (나중에 Streamlit 입력창이 됨)
    target_id = 3
    start = "2025-02-13"
    end = "2025-12-29"

    target_age = ''
    target_gender = '' 
    avoid_age = ''
    avoid_gender = ''

    # 1. 계정명 가져오기
    acc_name = get_account_name(target_id)

    # 1. 광고 개수 가져오기
    total_ads = get_active_ad_count(target_id, start, end)

    # 광고 개수와 키워드 개수 둘 다 동일한 기간(start, end)을 사용합니다.

    keyword_count = get_total_keyword_count(target_id, start, end)

    # 2. 주차별 데이터 가져오기
    organic_df = get_organic_data(target_id, start, end)

    # 3. 데이터가 있을 때만 그래프 생성
    chart_path = None
    if not organic_df.empty:
        chart_path = create_organic_trend_chart(organic_df, target_id)
        # HTML에서 이미지를 불러올 수 있도록 상대 경로로 변환 (필요시)
        # chart_path = chart_path.replace('\\', '/') 


    weekly_df = get_weekly_performance_data(target_id, start, end)
    ctr_chart_path = create_ctr_trend_chart(weekly_df, target_id)

    top_kws_df = get_top_ctr_keywords_direct(target_id, start, end)





    # 상위 분석 데이터 가공
    top_ads, target_data = get_top_ads_and_target_analysis(target_id, start, end)
    target_chart_path = create_target_analysis_chart(target_data, top_ads, target_id)
    analysis_results = []
    if top_ads is not None:
        for _, ad_row in top_ads.iterrows():
            ad_id = ad_row['ad_id']
            analysis_results.append({
                'ad_name': ad_row['ad_name'],
                'ad_total_ctr': ad_row['ad_total_ctr'], # 광고 전체 CTR 추가
                'targets': target_data[target_data['ad_id'] == ad_id].head(3).to_dict('records')
            })

    # 하위 분석 데이터 가공 (상위 분석과 동일한 로직)
    bottom_ads, b_target_data = get_bottom_ads_and_target_analysis(target_id, start, end)
    bottom_chart_path = create_bottom_target_chart(b_target_data, bottom_ads, target_id)

    bottom_results = []
    if bottom_ads is not None:
        for _, ad_row in bottom_ads.iterrows():
            ad_id = ad_row['ad_id']
            bottom_results.append({
                'ad_name': ad_row['ad_name'],
                'ad_total_ctr': ad_row['ad_total_ctr'], # 광고 전체 CTR 추가
                'targets': b_target_data[b_target_data['ad_id'] == ad_id].head(3).to_dict('records')
            })



    # HTML 전달용 리스트 변환
    top_keywords_list = top_kws_df.to_dict('records')



    # -------------------------------
    # 리포트 데이터 합치기
    context = {
        "css_path": "report.css",
        "account_name": acc_name,
        "date_start": start,
        "date_end": end,
        "total_active_ads": total_ads,
        "total_keyword_types": keyword_count,
        "chart_path": chart_path, # 데이터 없으면 None이 전달됨
        "ctr_chart": ctr_chart_path,
        "avg_ctr": f"{weekly_df['ctr'].mean():.2f}%" if not weekly_df.empty else "0.00%",


        "analysis_results": analysis_results,
        "target_analysis_chart": target_chart_path, # 콘텐츠별 타겟 분석 차트 추가


            # 하위 분석 추가 (동일 구조)
        "bottom_analysis_chart": bottom_chart_path,
        "bottom_results": bottom_results,

        "top_keywords": top_keywords_list
        # HTML 전달용 리스트 변환
        
    }
    # HTML 생성
    generate_html(context)
    print(f"✅ {acc_name} 리포트 생성 완료!")


if __name__ == "__main__":
    run()