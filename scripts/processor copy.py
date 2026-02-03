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

# 총 광고 개수
def get_active_ad_count(account_id, date_start, date_end):
    """해당 기간 동안 노출이 1회라도 발생한 광고(ad_id)의 총 개수를 반환"""
    engine = get_engine()
    
    # COUNT(DISTINCT ad_id)를 사용하여 중복 없이 광고 개수를 셉니다.
    query = f"""
        SELECT COUNT(DISTINCT apd.ad_id) as ad_count
        FROM ad_performance_daily apd
        LEFT JOIN ad ON apd.ad_id = ad.ad_id
        WHERE ad.account_id = '{account_id}'
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
    """
    
    df = pd.read_sql(query, engine)
    
    if not df.empty:
        return int(df.iloc[0]['ad_count'])
    return 0


# 총 키워드 개수
def get_total_keyword_count(account_id, date_start, date_end):
    engine = get_engine()
    
    query = f"""
        SELECT DISTINCT ak.essential_keywords, ak.variable_keywords
        FROM ad_performance_daily apd
        LEFT JOIN ad ON apd.ad_id = ad.ad_id
        LEFT JOIN ad_keyword ak ON apd.ad_id = ak.ad_id
        WHERE ad.account_id = '{account_id}'
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
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
    if not df.empty:
        # date_start 기준으로 순위를 매겨 '1주차, 2주차...' 생성
        df['week_label'] = df['date_start'].rank(method='min').astype(int).apply(lambda x: f"{x}주차")
        
    return df

# 주차별 CTR(%) 데이터 가져오기
def get_weekly_performance_data(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 쿼리: apd와 ad를 JOIN하여 account_id 기준으로 데이터 추출
    query = f"""
        SELECT apd.date, apd.impressions, apd.clicks
        FROM ad_performance_daily apd
        JOIN ad ON apd.ad_id = ad.ad_id
        WHERE ad.account_id = {account_id}
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    # 2. 날짜 형식 변환
    df['date'] = pd.to_datetime(df['date'])
    
    # 3. 주차별 그룹화 (월~일 기준)
    # 'W-SUN'은 일요일이 마지막인 주차를 의미합니다.
    weekly_df = df.set_index('date').resample('W-SUN').agg({
        'impressions': 'sum',
        'clicks': 'sum'
    })
    
    # 4. 주차별 CTR 계산 (클릭 / 노출 * 100)
    # 0으로 나누는 에러 방지를 위해 fillna(0) 처리
    weekly_df['ctr'] = (weekly_df['clicks'] / weekly_df['impressions'] * 100).fillna(0)
    
    # 인덱스를 다시 컬럼으로 빼고 날짜 형식을 '01-26~02-01' 같은 주차 범위 문자열로 변환
    weekly_df = weekly_df.reset_index()
    weekly_df['week_label'] = weekly_df['date'].dt.strftime('%m-%d')
    
    return weekly_df


# 상위 광고 3개 및 타겟 분석 데이터 가져오기   
def get_top_ads_and_target_analysis(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 0.05% 필터를 통과한 상위 3개 ad_id 가져오기
    top_ads_query = f"""
    WITH total_stats AS (
        SELECT SUM(impressions) as total_site_imp
        FROM ad_performance_daily apd
        JOIN ad ON apd.ad_id = ad.ad_id
        WHERE ad.account_id = '{account_id}'
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
    )
    SELECT 
        apd.ad_id, 
        ad.ad_name,
        SUM(apd.impressions) as ad_total_imp,
        ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as ad_total_ctr
    FROM ad_performance_daily apd
    JOIN ad ON apd.ad_id = ad.ad_id, total_stats
    WHERE ad.account_id = '{account_id}'
      AND apd.date BETWEEN '{date_start}' AND '{date_end}'
      AND apd.impressions > 0
    GROUP BY apd.ad_id, ad.ad_name, total_stats.total_site_imp
    HAVING SUM(apd.impressions) >= total_stats.total_site_imp * 0.0005
    ORDER BY ad_total_ctr DESC  -- 하위는 ASC
    LIMIT 3;
    """
    top_ads_df = pd.read_sql(top_ads_query, engine)
    top_ad_ids = top_ads_df['ad_id'].tolist()

    if not top_ad_ids:
        return None, None

    # 2. 선정된 3개 광고의 연령+성별 타겟 데이터 가져오기 (apd에 age, gender가 있다고 가정)
    target_query = f"""
    SELECT 
        ad_id, 
        gender, 
        age,
        SUM(impressions) as impressions,
        ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)::numeric) * 100, 2) as ctr
    FROM ad_performance_daily
    WHERE ad_id IN ({','.join(map(str, top_ad_ids))})
      AND date BETWEEN '{date_start}' AND '{date_end}'AND impressions > 0
      AND impressions > 0
    GROUP BY ad_id, gender, age
    ORDER BY ad_id, ctr DESC;
    """
    target_df = pd.read_sql(target_query, engine)
    
    return top_ads_df, target_df


def get_bottom_ads_and_target_analysis(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 0.05% 필터를 통과한 '하위' 3개 ad_id 가져오기 (ORDER BY CTR ASC)
    bottom_ads_query = f"""
    WITH total_stats AS (
        SELECT SUM(impressions) as total_site_imp
        FROM ad_performance_daily apd
        JOIN ad ON apd.ad_id = ad.ad_id
        WHERE ad.account_id = '{account_id}'
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
    )
    SELECT 
        apd.ad_id, 
        ad.ad_name,
        SUM(apd.impressions) as ad_total_imp,
        ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as ad_total_ctr
    FROM ad_performance_daily apd
    JOIN ad ON apd.ad_id = ad.ad_id, total_stats
    WHERE ad.account_id = '{account_id}'
      AND apd.date BETWEEN '{date_start}' AND '{date_end}'
      AND apd.impressions > 0
    GROUP BY apd.ad_id, ad.ad_name, total_stats.total_site_imp
    HAVING SUM(apd.impressions) >= total_stats.total_site_imp * 0.0005
    ORDER BY ad_total_ctr ASC
    LIMIT 3;
    """
    bottom_ads_df = pd.read_sql(bottom_ads_query, engine)
    bottom_ad_ids = bottom_ads_df['ad_id'].tolist()

    if not bottom_ad_ids:
        return None, None

    # 2. 선정된 하위 3개 광고의 타겟 데이터 가져오기
    target_query = f"""
    SELECT 
        ad_id, gender, age,
        SUM(impressions) as impressions,
        ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)::numeric) * 100, 2) as ctr
    FROM ad_performance_daily
    WHERE ad_id IN ({','.join(map(str, bottom_ad_ids))})
      AND date BETWEEN '{date_start}' AND '{date_end}'
      AND impressions > 0
    GROUP BY ad_id, gender, age
    ORDER BY ad_id, ctr DESC; -- 타겟 효율도 높은 순으로 정렬
    """
    target_df = pd.read_sql(target_query, engine)
    
    return bottom_ads_df, target_df






# 키워드별 CTR 상위 N개 가져오기
def get_top_ctr_keywords_direct(account_id, date_start, date_end, top_n=10):
    engine = get_engine()
    
    query = f"""
    WITH ad_performance AS (
        -- 1. 광고별 총 노출과 클릭을 먼저 요약 (중복 계산 방지)
        SELECT 
            apd.ad_id, 
            SUM(impressions) as total_imp, 
            SUM(clicks) as total_click
        FROM ad_performance_daily apd
        JOIN ad ON apd.ad_id = ad.ad_id
        WHERE ad.account_id = '{account_id}'
          AND apd.date BETWEEN '{date_start}' AND '{date_end}'
          AND apd.impressions > 0
        GROUP BY apd.ad_id
    ),
    expanded_keywords AS (
        -- 2. 키워드 배열을 낱개(Row)로 펼침
        SELECT 
            ad_id,
            unnest(essential_keywords || variable_keywords) as keyword
        FROM ad_keyword
    )
    -- 3. 키워드별 합산 및 CTR 계산
    SELECT 
        ek.keyword,
        SUM(ap.total_imp) as impressions,
        SUM(ap.total_click) as clicks,
        CASE 
            WHEN SUM(ap.total_imp) > 0 
            THEN ROUND((SUM(ap.total_click)::numeric / SUM(ap.total_imp)::numeric) * 100, 2)
            ELSE 0 
        END as ctr
    FROM expanded_keywords ek
    JOIN ad_performance ap ON ek.ad_id = ap.ad_id
    WHERE ek.keyword IS NOT NULL AND ek.keyword != ''
    GROUP BY ek.keyword
    HAVING SUM(ap.total_imp) >= 10  -- 신뢰도를 위해 노출 10회 이상만
    ORDER BY ctr DESC
    LIMIT {top_n};
    """
    return pd.read_sql(query, engine)
