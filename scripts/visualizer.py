# scripts/visualizer.py
import matplotlib.pyplot as plt
import pandas as pd
import os
import time

def visual(df):
    # 1. 최소한의 데이터 체크
    if df is None or df.empty:
        return None

    try:
        # 2. 저장 경로 및 유니크한 파일명 설정 (현재 시간 기반)
        save_dir = "static/charts"
        os.makedirs(save_dir, exist_ok=True)
        file_path = f"{save_dir}/temp_chart_{int(time.time() * 1000)}.png"

        # 3. 데이터 추출 (컬럼명 몰라도 됨: 첫 번째=X, 마지막=Y)
        plt.figure(figsize=(4, 2))
        plt.plot(df.iloc[:, 0], df.iloc[:, -1], marker='.', color='blue')
        
        # 4. 부가 설정 없이 바로 저장
        plt.tight_layout()
        plt.savefig(file_path)
        plt.close()
        
        return file_path

    except:
        # 어떤 에러가 나도 무조건 None 반환하여 메인 프로세스 보호
        return None



def create_ctr_trend_chart(df, account_id):
    if df is None or df.empty:
        return None

    # 1. 저장 경로 설정 및 폴더 생성
    save_dir = "static/charts"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    file_path = f"{save_dir}/{account_id}_ctr_trend.png"

    df.columns = [str(col).lower().strip() for col in df.columns]

    # 2. 그래도 'date'가 없다면 첫 번째 컬럼을 'date'라고 간주함
    if 'date' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'date'})


    # 2. 데이터 전처리 (주차별 CTR 계산)
    df['date'] = pd.to_datetime(df['date'])
    weekly = df.set_index('date').resample('W-SUN').sum()
    weekly['ctr'] = (weekly['clicks'] / weekly['impressions'] * 100).fillna(0)

    # 3. 시각화 (최대한 심플하게)
    plt.figure(figsize=(6, 3))
    plt.plot(weekly.index, weekly['ctr'], marker='o', linestyle='-', color='#007bff')
    
    # 불필요한 테두리 제거 및 최소한의 정보만 표시
    plt.title("Weekly CTR Trend (%)")
    plt.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()

    # 4. 저장 및 종료
    plt.savefig(file_path)
    plt.close()
    
    return file_path

def create_organic_trend_chart(df, account_id):
    """주차별 organic_impressions 추세를 그려 이미지로 저장"""
    if df.empty:
        return None

    # 날짜순 정렬 (혹시 모르니)
    df = df.sort_values('date_start')

    plt.figure(figsize=(10, 5))
    plt.plot(df['date_start'], df['organic_impressions'], marker='o', linestyle='-', color='green')
    
    plt.title(f"Weekly Organic Trend: {account_id}")
    plt.xlabel("Week Start Date")
    plt.ylabel("Impressions")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()

    # 이미지 저장 경로 설정
    os.makedirs('static/charts', exist_ok=True)
    file_path = f"static/charts/{account_id}_trend.png"
    plt.savefig(file_path)
    plt.close() # 메모리 해제
    
    return file_path



def create_mini_chart_from_df(ad_id, daily_df):
    """
    이미 쿼리된 daily_df에서 특정 ad_id의 데이터만 필터링하여 차트 생성
    """
    # 해당 광고의 일별 데이터만 추출
    df = daily_df[daily_df['ad_id'] == ad_id].sort_values('uploaded_at')

    plt.figure(figsize=(3, 1.2))
    
    if not df.empty:
        # daily_ctr 계산이 이미 되어있다고 가정 (또는 여기서 계산)
        plt.plot(df['date'], df['daily_ctr'], color='#4e73df', linewidth=2)
        plt.fill_between(df['date'], df['daily_ctr'], color='#4e73df', alpha=0.1)
    
    plt.axis('off') 
    plt.tight_layout(pad=0)

    os.makedirs("static/charts", exist_ok=True)
    file_path = f"static/charts/mini_chart_{ad_id}.png"
    
    plt.savefig(file_path, transparent=True, dpi=100)
    plt.close()
    
    return file_path