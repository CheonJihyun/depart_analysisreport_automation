# scripts/processor.py
import pandas as pd
import numpy as np
from scripts.db_connector import get_engine

# 제목 부분 : (기업명) 광고 계정
def get_account_name(account_id):
    """ID를 받아 DB에서 실제 대행사/광고주 이름을 찾아오는 함수"""
    engine = get_engine()
    query = f"SELECT account_name FROM ad_account WHERE account_id = '{account_id}' LIMIT 1"
    df = pd.read_sql(query, engine)
    
    if not df.empty:
        return df.iloc[0]['account_name']
    return account_id  # 만약 이름이 없으면 ID라도 반환

# ----------------------------------

# 총 광고 개수 
def get_active_ad_count(account_id, date_start, date_end):
    """해당 기간 동안 노출이 1회라도 발생한 광고(ad_id)의 총 개수를 반환"""
    engine = get_engine()

    # COUNT(DISTINCT ad_id)를 사용하여 중복 없이 광고 개수를 셉니다.
    query = f"""
        SELECT COUNT(DISTINCT ad_id) as ad_count
        FROM ad
        WHERE account_id = '{account_id}'
            AND created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
    """

    df = pd.read_sql(query, engine)

    if not df.empty:
        return int(df.iloc[0]['ad_count'])
    return 0

# 총 콘텐츠 개수
def get_total_content_count(account_id, date_start, date_end):
    """해당 기간 동안 업로드된 광고 콘텐츠(광고별 ig_permalink)의 총 개수를 반환"""
    engine = get_engine()

    query = f"""
        SELECT COUNT(DISTINCT ad.ig_permalink) as content_count
        FROM ad
        WHERE ad.account_id = '{account_id}'
            AND created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
    """

    df = pd.read_sql(query, engine)

    if not df.empty:
        return int(df.iloc[0]['content_count'])
    return 0

# 광고 진행 기간
def get_ad_period(account_id, date_start, date_end):
    engine = get_engine()
    query = f"""
        SELECT 
            MIN(created_time) AS start_date,
            -- 필터 조건에서 사용한 '직전 일요일'의 바로 다음 날(월요일)을 고정적으로 반환
            DATE_TRUNC('week', '{date_end}'::date)::date AS end_date
        FROM ad
        WHERE account_id = '{account_id}'
            AND created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        start = df.iloc[0]['start_date']
        end = df.iloc[0]['end_date']
        return start, end
    return None, None

# 콘텐츠 진행 기간
def get_content_period(account_id, date_start, date_end):
    engine = get_engine()
    query = f"""
        SELECT 
            MIN(ig_timestamp) AS start_date,
            -- 가장 늦은 날짜의 직후 월요일 계산
            MAX(ig_timestamp) AS end_date
        FROM ad
        WHERE account_id = '{account_id}'
            AND created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        start = df.iloc[0]['start_date'].date()
        end = df.iloc[0]['end_date'].date() # timestampz to date
        return start, end
    return None, None

# 총 키워드 개수
def get_total_keyword_count(account_id, date_start, date_end):
    engine = get_engine()
    query = f"""
        SELECT DISTINCT ak.essential_keywords, ak.variable_keywords
        FROM ad
        LEFT JOIN ad_keyword ak ON ad.ad_id = ak.ad_id
        WHERE account_id = '{account_id}'
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
    """

    df = pd.read_sql(query, engine)
    
    all_keywords = set()
    
    for _, row in df.iterrows():
        for col in ['essential_keywords', 'variable_keywords']:
            val = row[col]
            
            # [수정 포인트] 리스트/배열 형태여도 에러 나지 않게 검사
            if val is None:
                continue
                
            # 만약 이미 리스트(배열) 형태라면 (예: ['A', 'B'])
            if isinstance(val, (list, np.ndarray)):
                for k in val:
                    if k: all_keywords.add(str(k).strip())
                    
            # 만약 문자열 형태라면 (예: '{A,B}')
            elif isinstance(val, str):
                cleaned = val.replace('{', '').replace('}', '').strip()
                if cleaned:
                    kws = cleaned.split(',')
                    for k in kws:
                        k_strip = k.strip()
                        if k_strip:
                            all_keywords.add(k_strip)
                            
    return len(all_keywords)

# ----------------------------------

# 인스타그램 팔로워 데이터 가져오기

# 주차별 CTR(%) 데이터 가져오기
def get_ctr_data(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 쿼리: apd와 ad를 JOIN하여 account_id 기준으로 데이터 추출

    query = f"""
        SELECT apd.date, apd.impressions, apd.clicks
        FROM ad
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE account_id = '{account_id}'
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
        ORDER BY date
    """

    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df

# 주차별 organic_impressions 데이터 가져오기
def get_organic_data(account_id, date_start, date_end):
    engine = get_engine()
    # 파라미터로 받은 기간 범위 내에 있는 주차 데이터만 가져옴
    query = f"""
        SELECT date_start, date_end, organic_impressions 
        FROM account_organic_weekly 
        WHERE account_id = '{account_id}'
            AND date_start >= '{date_start}'
            AND date_end <= '{date_end}'
        ORDER BY date_start ASC
    """
    df = pd.read_sql(query, engine)
   
    if df.empty:
        return None

    return df

# 인스타그램 프로필 방문수 데이터 가져오기

# 전체 노출 수 및 threshold 가져오기
def get_imp_threshold(account_id, date_start, date_end):
    engine = get_engine()

    # 1. 전체 노출수 및 기준값 계산 (Note용)
    total_stats_query = f"""
        SELECT SUM(impressions) as total_site_imp
        FROM ad
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE account_id = '{account_id}'
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
    """
    
    total_site_imp = pd.read_sql(total_stats_query, engine).iloc[0]['total_site_imp'] or 0
    threshold = total_site_imp * 0.0005  # 0.05% 기준

    return total_site_imp, threshold

# CTR 상위 광고 3개 정보 데이터 가져오기 (임계점 이상 노출 광고)  
def get_content_ctr_data(account_id, date_start, date_end, threshold, is_top=True):
    engine = get_engine()
    
    order_direction = "DESC" if is_top else "ASC"

    # 2. 개별 광고 데이터 가져오기 (uploaded_at, ig_permalink 포함)
    
    ads_query = f"""
    SELECT 
        ad.ad_id, 
        ad.ad_name,
        ad.ig_timestamp as uploaded_at, -- 업로드일로 사용
        ad.ig_permalink as thumbnail, -- 썸네일 경로로 사용
        ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as ctr
    FROM ad 
    LEFT JOIN ad_performance_daily apd ON apd.ad_id = ad.ad_id
    WHERE ad.account_id = '{account_id}'
        AND ad.created_time >= '{date_start}'
        -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
        AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
        AND apd.date >= '{date_start}'
        AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
    GROUP BY ad.ad_id
    HAVING SUM(apd.impressions) >= {threshold}
    ORDER BY ctr {order_direction}
    LIMIT 3;
    """
    ads_df = pd.read_sql(ads_query, engine)

    if ads_df.empty:
        return []

    # 2. 결과 가공 (딕셔너리 리스트 형태로 3개 모두 저장)
    results = []
    for _, row in ads_df.iterrows():
        results.append({
            'ad_id': row['ad_id'],
            'uploaded_at': row['uploaded_at'].date(),
            'thumbnail': row['ad_name'], 
            'ctr': row['ctr']
        })

    return results # 이제 3개의 데이터가 담긴 리스트를 반환합니다.


# 특정 광고들의 타겟별 CTR 데이터
def get_a_content_target_ctr_data(ad_id, date_start, date_end):
    engine = get_engine()
    
    query = f"""
        SELECT 
            apd.age, apd.gender,
            ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as ctr
        FROM ad
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.ad_id = '{ad_id}'
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
        GROUP BY apd.age, apd.gender
        ORDER BY ctr DESC;
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df


# 타겟별 평균 노출, ctr
def get_target_avg_imp_ctr(account_id, date_start, date_end):
    engine = get_engine()
    
    query = f"""
        SELECT 
        apd.age, 
        apd.gender, 
        AVG(apd.impressions) AS impressions, 
        AVG(apd.clicks) AS clicks,
        -- NULLIF를 사용하여 분모(impressions)가 0이면 NULL로 처리
        -- CTR 공식은 (클릭 / 노출) * 100입니다.
        ROUND(
            (SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 
            2
        ) AS ctr
        FROM ad
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = '{account_id}'
            AND ad.created_time >= '{date_start}'
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
        GROUP BY apd.age, apd.gender
    """

    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df
