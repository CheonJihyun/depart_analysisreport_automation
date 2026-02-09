from scripts.processor import get_account_name, get_active_ad_count, get_total_content_count,\
        get_ad_period, get_content_period, get_total_keyword_count,\
    get_instagram_followers, get_ctr_data, get_organic_data, get_imp_threshold, get_content_ctr_data, \
        get_a_content_target_ctr_data,\
    get_target_avg_imp_ctr, get_target_avg_imp_ctr_threshold,\
    get_raw_keyword_performance, filter_keywords_by_pos, get_overall_ctr, \
        get_strategic_performance
import json
import pandas as pd
import inspect
import os
import shutil

def save_to_json(df, filename):
    # 1. 저장할 폴더명 설정
    folder_name = "json_reports"
    
    # 2. 폴더가 없으면 생성 (스크립트 실행 시 폴더 존재 여부 자동 체크)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    if df is not None and not df.empty:
        # 날짜 데이터 변환 로직 (기존과 동일)
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 3. [핵심 수정] 파일명 앞에 폴더 경로를 붙입니다.
        # 예: "overall_top1.json" -> "json_reports/overall_top1.json"
        file_path = os.path.join(folder_name, filename)
        
        # 4. 수정된 경로(file_path)로 저장
        df.to_json(file_path, orient='records', force_ascii=False, indent=4)
        print(f"✅ {file_path} 추출 완료 ({len(df)}행)")
    else:
        print(f"❌ {filename} 대상 데이터가 없습니다.")
        print(f"❌ {filename} 대상 데이터가 없습니다.")

# 키워드조합 1위~6위까지 분리하는 함수
def export_top6_individual_jsons(df):
    frame = inspect.currentframe().f_back
    var_name = "data" # 기본값
    for name, val in frame.f_locals.items():
        if val is df:
            var_name = name
            break

    clean_prefix = var_name.split('_')[0]

    if df is None or df.empty:
        print("❌ 데이터가 비어있습니다.")
        return

    # 1. 고유한 조합(ess_1, ess_2)을 CTR 순서대로 추출
    # (이미 쿼리에서 정렬되어 오므로 순서대로 6개만 가져옵니다)
    top_combos = df[['ess_1', 'ess_2']].drop_duplicates().head(6).values.tolist()

    print(f"🚀 전체 {len(df):,}행 데이터 중 상위 6개 조합 개별 추출 시작...")

    for i, (e1, e2) in enumerate(top_combos, 1):
        # 2. 해당 순위(i)의 조합에 해당하는 데이터만 필터링
        rank_df = df[(df['ess_1'] == e1) & (df['ess_2'] == e2)].copy()
        

        # 3. 파일명 지정 (예: strategy_rank_1.json)
        filename = f"{clean_prefix}_top{i}_kwcomb.json"
        
        save_to_json(rank_df, filename)


# --- 여기서부터가 실행 코드입니다 ---
if __name__ == "__main__":

    folder_name = "json_reports"
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
        print(f"🧹 기존 '{folder_name}' 폴더를 비웠습니다.")
    os.makedirs(folder_name)


    # 테스트용 파라미터
    target_id = 3
    start = "2025-02-13"
    end = "2025-12-31"

    main_age = '35-44'
    main_gender = 'male' 
    avoid_age = ''
    avoid_gender = ''

    
   # 1. 인스타그램 데이터 추출
    insta_df = get_instagram_followers(target_id,start,end)
    save_to_json(insta_df, "insta_report.json")

    # 2. CTR 데이터 추출
    ctr_df = get_ctr_data(target_id, start, end)
    save_to_json(ctr_df, "ctr_report.json")

    imp, threshold = get_imp_threshold(target_id,start,end)
    # ctr 상위 콘텐츠 리스트
    ctr_top1_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold)[0]['ad_id'], start, end)
    ctr_top2_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold)[1]['ad_id'], start, end)
    ctr_top3_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold)[2]['ad_id'], start, end)
    save_to_json(ctr_top1_content, "ctr_top1_content.json")
    save_to_json(ctr_top2_content, "ctr_top2_content.json")
    save_to_json(ctr_top3_content, "ctr_top3_content.json")

    # ctr 하위 콘텐츠 리스트
    ctr_bot1_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold,is_top=False)[0]['ad_id'], start, end)
    ctr_bot2_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold,is_top=False)[1]['ad_id'], start, end)
    ctr_bot3_content = get_a_content_target_ctr_data(get_content_ctr_data(target_id,start,end, threshold,is_top=False)[2]['ad_id'], start, end)
    save_to_json(ctr_bot1_content, "ctr_bot1_content.json")
    save_to_json(ctr_bot2_content, "ctr_bot2_content.json")
    save_to_json(ctr_bot3_content, "ctr_bot3_content.json")

    # 타겟별 노출 및 CTR 평균
    target_avg_imp = get_target_avg_imp_ctr(target_id,start,end)
    save_to_json(target_avg_imp, "target_avg_imp.json")
    target_avg_ctr = get_target_avg_imp_ctr_threshold(target_id,start,end, threshold)
    save_to_json(target_avg_ctr, "target_avg_ctr.json")

    # 전체 연령 및 성별
    overall_top_raw_keyword_df = get_raw_keyword_performance(target_id,start,end)
    overall_top_keyword_noun = filter_keywords_by_pos(overall_top_raw_keyword_df, pos_type='noun')
    overall_top_keyword_va = filter_keywords_by_pos(overall_top_raw_keyword_df, pos_type='verb_adj')
    save_to_json(overall_top_keyword_noun, "overall_top_keyword_noun.json")
    save_to_json(overall_top_keyword_va, "overall_top_keyword_va.json")

    overall_bot_raw_keyword_df = get_raw_keyword_performance(target_id,start,end,is_top=False)
    overall_bot_keyword_noun = filter_keywords_by_pos(overall_bot_raw_keyword_df, pos_type='noun')
    overall_bot_keyword_va = filter_keywords_by_pos(overall_bot_raw_keyword_df, pos_type='verb_adj')
    save_to_json(overall_bot_keyword_noun, "overall_bot_keyword_noun.json")
    save_to_json(overall_bot_keyword_va, "overall_bot_keyword_va.json")


    if main_age and main_gender:
        main_top_raw_keyword_df = get_raw_keyword_performance(target_id,start,end, main_age,main_gender)
        main_top_keyword_noun = filter_keywords_by_pos(main_top_raw_keyword_df, pos_type='noun')
        main_top_keyword_va = filter_keywords_by_pos(main_top_raw_keyword_df, pos_type='verb_adj')
        save_to_json(main_top_keyword_noun, "main_top_keyword_noun.json")
        save_to_json(main_top_keyword_va, "main_top_keyword_va.json")

        main_bot_raw_keyword_df = get_raw_keyword_performance(target_id,start,end, main_age,main_gender,is_top=False)
        main_bot_keyword_noun = filter_keywords_by_pos(main_bot_raw_keyword_df, pos_type='noun')
        main_bot_keyword_va = filter_keywords_by_pos(main_bot_raw_keyword_df, pos_type='verb_adj')
        save_to_json(main_bot_keyword_noun, "main_bot_keyword_noun.json")
        save_to_json(main_bot_keyword_va, "main_bot_keyword_va.json")

    if avoid_age and avoid_gender:
        avoid_top_raw_keyword_df = get_raw_keyword_performance(target_id,start,end, avoid_age,avoid_gender)
        avoid_top_keyword_noun = filter_keywords_by_pos(avoid_top_raw_keyword_df, pos_type='noun')
        avoid_top_keyword_va = filter_keywords_by_pos(avoid_top_raw_keyword_df, pos_type='verb_adj')
        save_to_json(avoid_top_keyword_noun, "avoid_top_keyword_noun.json")
        save_to_json(avoid_top_keyword_va, "avoid_top_keyword_va.json")

        avoid_bot_raw_keyword_df = get_raw_keyword_performance(target_id,start,end, avoid_age,avoid_gender,is_top=False)
        avoid_bot_keyword_noun = filter_keywords_by_pos(avoid_bot_raw_keyword_df, pos_type='noun')
        avoid_bot_keyword_va = filter_keywords_by_pos(avoid_bot_raw_keyword_df, pos_type='verb_adj')
        save_to_json(avoid_bot_keyword_noun, "avoid_bot_keyword_noun.json")
        save_to_json(avoid_bot_keyword_va, "avoid_bot_keyword_va.json")
        

    # 키워드 조합
    overall_kwcomb_df = get_strategic_performance(target_id, start, end)
    # 1위~6위 개별 파일 생성 함수 호출
    export_top6_individual_jsons(overall_kwcomb_df)

    if main_age and main_gender:
        main_kwcomb_df = get_strategic_performance(target_id,start,end, main_age,main_gender)
        export_top6_individual_jsons(main_kwcomb_df)
    
    if avoid_age and avoid_gender:
        avoid_kwcomb_df = get_strategic_performance(target_id,start,end, avoid_age,avoid_gender)
        export_top6_individual_jsons(avoid_kwcomb_df)



    