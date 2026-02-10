# scripts/processor.py
import pandas as pd
import numpy as np
from scripts.db_connector import get_engine, get_engine_db

# 제목 부분 : (기업명) 광고 계정
def get_account_name(account_id):
    """ID를 받아 DB에서 실제 대행사/광고주 이름을 찾아오는 함수"""
    engine = get_engine()
    query = f"SELECT account_name FROM ad_account WHERE account_id = {account_id} LIMIT 1"
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
        JOIN campaign c ON ad.account_id = c.account_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_keyword ak ON ad.ad_id = ak.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
def get_instagram_followers(fb_ad_account_id, date_start, date_end):
    # engine = get_engine() 
    # query = f"""
    # SELECT DISTINCT ON (iid.updated_at::date)
    #     aa.account_name, 
    #     iid.updated_at, 
    #     ii.follower_count, 
    #     iid.profile_views
    # FROM ig_insights_daily iid
    # JOIN ig_account ia ON iid.ig_id = ia.ig_id
    # JOIN business_portfolio bp ON ia.business_id = bp.business_id
    # JOIN ad_account aa ON bp.business_id = aa.business_id 
    # JOIN campaign c ON aa.account_id = c.account_id
    # WHERE aa.account_id = '{account_id}'
    #     AND iid.updated_at >= '{date_start}'
    #     AND iid.updated_at <= '{date_end}'
    #     AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
    # ORDER BY iid.updated_at::date, iid.updated_at ASC
    # """
    # # ORDER BY의 첫 번째 기준은 DISTINCT ON과 일치해야 하며, 
    # # 그 뒤에 ASC를 붙여 가장 빠른 시점을 선택합니다.

    # df = pd.read_sql(query, engine)

    engine_db = get_engine_db() # 현재 여기만 engine_db로 되어있음! 통합 필요 !!
    query = f"""
    SELECT DISTINCT ON (ig.updated_at::date)
        aa.account_name, 
        ig.updated_at, 
        ig.follower_count, 
        ig.profile_views
    FROM instagram_followers ig
    JOIN facebook_pages fb ON ig.page_id = fb.id
    JOIN ad_accounts aa ON fb.ad_account_id = aa.id
    JOIN campaigns c ON aa.id = c.ad_account_id
    WHERE aa.fb_ad_account_id = '{fb_ad_account_id}'
        AND ig.updated_at >= '{date_start}'
        AND ig.updated_at <= '{date_end}'
        AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
    ORDER BY ig.updated_at::date, ig.updated_at ASC
    """
    # 통합시 campaigns테이블명, ad_account_id컬럼명 주의 !!
    # ORDER BY의 첫 번째 기준은 DISTINCT ON과 일치해야 하며, 
    # 그 뒤에 ASC를 붙여 가장 빠른 시점을 선택합니다.

    df = pd.read_sql(query, engine_db)

    if df.empty:
        return None
        
    return df

# 주차별 CTR(%) 데이터 가져오기
def get_ctr_data(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 쿼리: apd와 ad를 JOIN하여 account_id 기준으로 데이터 추출

    query = f"""
        SELECT 
            DATE_TRUNC('week', apd.date)::date as week_start, -- 해당 주의 월요일 날짜
            SUM(clicks) as total_clicks, 
            SUM(impressions) as total_impressions,
            ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)::numeric) * 100, 2) as ctr
        FROM ad
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
        GROUP BY week_start
        ORDER BY week_start;
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
        WHERE account_id = {account_id}
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
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
    JOIN campaign c ON ad.account_id = c.account_id
    LEFT JOIN ad_performance_daily apd ON apd.ad_id = ad.ad_id
    WHERE ad.account_id = {account_id}
        AND ad.created_time >= '{date_start}'
        -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
        AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
        AND apd.date >= '{date_start}'
        AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
        AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.ad_id = {ad_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
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
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
        GROUP BY apd.age, apd.gender
    """

    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df

def get_target_avg_imp_ctr_threshold(account_id, date_start, date_end, threshold):
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
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
        GROUP BY apd.age, apd.gender
        HAVING SUM(apd.impressions) >= {threshold}
    """

    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df



# 키워드 마다의 imp, click, ctr
def get_raw_keyword_performance(account_id, date_start, date_end, target_age=None, target_gender=None, is_top=True):
    engine = get_engine()
    
    # 1. 정렬 방향 설정
    # 상위 10개는 CTR 높은 순(DESC), 하위 10개는 CTR 낮은 순(ASC)
    order_direction = "DESC" if is_top else "ASC"
    
    # 2. 타겟 필터링 조건
    target_filter = ""
    if target_age: target_filter += f" AND apd.age = '{target_age}'"
    if target_gender: target_filter += f" AND apd.gender = '{target_gender}'"

    query = f"""
        WITH exploded_keywords AS (
            SELECT ad_id, UNNEST(essential_keywords || variable_keywords) as keyword
            FROM ad_keyword
        )
        SELECT 
            ek.keyword,
            COUNT(DISTINCT ad.ad_id) as doc_freq,
            SUM(apd.impressions) as total_impressions,
            SUM(apd.clicks) as total_clicks,
            ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as avg_ctr
        FROM ad
        JOIN campaign c ON ad.account_id = c.account_id
        JOIN exploded_keywords ek ON ad.ad_id = ek.ad_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            {target_filter}
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
        GROUP BY ek.keyword
        HAVING COUNT(DISTINCT ad.ad_id) >= 3
        -- 하위 정렬(ASC)일 때, 노출수가 너무 적어 우연히 CTR이 0인 것들을 방지하기 위해 
        -- 노출수 정렬을 보조로 추가하거나 필요시 HAVING에 노출수 하한선을 추가할 수 있습니다.
        ORDER BY avg_ctr {order_direction}, total_impressions DESC
    """
    return pd.read_sql(query, engine)


# get_raw_keyword_performance로부터 얻은 df를 원하는 type(명,형동)으로 구분하여 나누는 함수 (10개 단어)
from kiwipiepy import Kiwi
kiwi = Kiwi()

def filter_keywords_by_pos(df, pos_type='noun'):
    """
    pos_type: 'noun' (NNG, NNP), 'verb_adj' (VV, VA)
    """
    if df is None or df.empty:
        return None

    def get_cleaned_keyword(text):
        # 1. 형태소 분석
        tokens = kiwi.tokenize(str(text))
        if not tokens: return None
        
        # 기존 로직: 첫 번째 유효한 토큰의 형태와 태그를 가져옴
        # (보통 단어 하나가 들어오므로 tokens[0]으로 충분함)
        t = tokens[0]
        
        # 2. 태그 필터링 (기존 로직 반영)
        valid_tags = {"NNG", "NNP", "VA", "VV"}
        if t.tag not in valid_tags:
            return None
        
        # 3. 길이 및 숫자 조건 (기존 로직 반영)
        tok = t.form
        if len(tok) < 2 or tok.isdigit():
            return None
            
        # 4. 타입 매칭
        cur_type = "noun" if t.tag in {"NNG", "NNP"} else "verb_adj"
        
        # 요청한 타입과 일치하면 해당 형태(원형) 반환
        if cur_type == pos_type:
            return tok
        return None

    # 새로운 컬럼에 정제된 키워드 할당
    df['cleaned_kw'] = df['keyword'].apply(get_cleaned_keyword)
    
    # 필터링 후 상위 10개 추출
    filtered_df = df.dropna(subset=['cleaned_kw']).copy()
    
    # 중복된 원형이 있을 경우(예: '예뻐서', '예쁘니' -> '예쁘') 성과를 합쳐주는 것이 좋지만,
    # 일단 가장 간단하게 상위 10개를 뽑으려면 아래와 같이 처리합니다.
    return filtered_df.head(10).drop(columns=['cleaned_kw'])

# 전체 기간 CTR
def get_overall_ctr(account_id, date_start, date_end):
    engine = get_engine()
    
    # 1. 쿼리: apd와 ad를 JOIN하여 account_id 기준으로 데이터 추출

    query = f"""
        SELECT ROUND((SUM(apd.clicks)::numeric / NULLIF(SUM(apd.impressions), 0)::numeric) * 100, 2) as ctr
        FROM ad
        JOIN campaign c ON ad.account_id = c.account_id
        LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
        WHERE ad.account_id = {account_id}
            AND ad.created_time >= '{date_start}'
            -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
            AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
            AND apd.date >= '{date_start}'
            AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
            AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
    """

    df = pd.read_sql(query, engine)
    
    if df.empty:
        return None

    return df.iloc[0]['ctr']



# 필수 키워드 조합(A+B)마다의 전체ctr성과 + 필수키워드 조합마다의 변수 키워드별마다의 ctr 성과
# essential keywords 조합

def get_strategic_performance(account_id, date_start, date_end, target_age=None, target_gender=None):
    engine = get_engine()
    
    target_filter = ""
    if target_age: target_filter += f" AND apd.age = '{target_age}'"
    if target_gender: target_filter += f" AND apd.gender = '{target_gender}'"

    query = f"""
        WITH ad_raw AS (
            -- 1. 광고별 필수/변수 키워드와 기초 성과를 가져옴
            SELECT 
                ad.ad_id,
                ak.essential_keywords,
                ak.variable_keywords,
                SUM(apd.impressions) as ad_imps,
                SUM(apd.clicks) as ad_clicks
            FROM ad
            JOIN campaign c ON ad.account_id = c.account_id
            JOIN ad_keyword ak ON ad.ad_id = ak.ad_id
            LEFT JOIN ad_performance_daily apd ON ad.ad_id = apd.ad_id
            WHERE ad.account_id = {account_id}
                AND ad.created_time >= '{date_start}'
                -- date_end가 무슨 요일이든, 그 주의 월요일에서 하루를 뺀 '일요일'까지 조회
                AND ad.created_time <= (DATE_TRUNC('week', '{date_end}'::date) - INTERVAL '1 day')::date
                AND apd.date >= '{date_start}'
                AND apd.date <= DATE_TRUNC('week', '{date_end}'::date)::date
                {target_filter}
                AND (c.campaign_name ILIKE '%%depart%%' OR c.campaign_name LIKE '%%디파트%%' OR c.campaign_name ILIKE '%%de;part%%')
            GROUP BY ad.ad_id, ak.essential_keywords, ak.variable_keywords
            HAVING array_length(ak.essential_keywords, 1) >= 2 -- 필수 키워드가 2개 이상인 것만
        ),
        combo_pairs AS (
            -- 2. 필수 키워드 리스트 내에서 가능한 모든 2개 조합(Pair) 생성
            -- SNS, 브랜드, 채널 -> (SNS, 브랜드), (SNS, 채널), (브랜드, 채널)로 확장
            SELECT 
                ad_id,
                ad_imps,
                ad_clicks,
                variable_keywords,
                essential_keywords[i] as ess_1,
                essential_keywords[j] as ess_2
            FROM ad_raw,
            LATERAL generate_series(1, array_length(essential_keywords, 1)) i,
            LATERAL generate_series(i + 1, array_length(essential_keywords, 1)) j
        ),
        essential_agg AS (
            -- 3. 생성된 [ess_1, ess_2] 쌍을 기준으로 전체 성과 합산
            SELECT 
                ess_1, ess_2,
                COUNT(DISTINCT ad_id) as combo_doc_freq,
                SUM(ad_imps) as total_imps,
                ROUND((SUM(ad_clicks)::numeric / NULLIF(SUM(ad_imps), 0)::numeric) * 100, 2) as combo_overall_ctr
            FROM combo_pairs
            GROUP BY ess_1, ess_2
            HAVING COUNT(DISTINCT ad_id) >= 3 -- 3개 이상의 광고에서 발견된 조합만
        ),
        variable_agg AS (
            -- 4. 해당 조합이 포함된 광고들 내에서 변수 키워드별 성과 계산
            SELECT 
                cp.ess_1, cp.ess_2,
                UNNEST(cp.variable_keywords) as var_keyword,
                SUM(cp.ad_imps) as v_imps,
                SUM(cp.ad_clicks) as v_clicks
            FROM combo_pairs cp
            INNER JOIN essential_agg ea ON cp.ess_1 = ea.ess_1 AND cp.ess_2 = ea.ess_2
            GROUP BY cp.ess_1, cp.ess_2, var_keyword
        )
        -- 5. 최종 결합 및 정렬
        SELECT 
            ea.ess_1, ea.ess_2,
            ea.combo_doc_freq,
            ea.combo_overall_ctr,
            va.var_keyword,
            ROUND((va.v_clicks::numeric / NULLIF(va.v_imps, 0)::numeric) * 100, 2) as with_var_ctr,
            va.v_imps as var_imps
        FROM essential_agg ea
        JOIN variable_agg va ON ea.ess_1 = va.ess_1 AND ea.ess_2 = va.ess_2
        ORDER BY ea.combo_overall_ctr DESC, with_var_ctr DESC
    """
    return pd.read_sql(query, engine)