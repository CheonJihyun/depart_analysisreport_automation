# scripts/visualizer.py
import matplotlib.pyplot as plt
import os

def create_organic_trend_chart(df, account_id):
    """주차별 organic_impressions 추세를 그려 이미지로 저장"""
    if df.empty:
        return None

    # 날짜순 정렬 (혹시 모르니)
    df = df.sort_values('date_start')

    plt.figure(figsize=(10, 5))
    plt.plot(df['week_label'], df['organic_impressions'], marker='o', linestyle='-', color='green')
    
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



def create_ctr_trend_chart(df, account_id):
    """주차별 CTR(%) 추세만 그려 이미지로 저장"""
    if df is None or df.empty:
        return None

    # 주차순 정렬
    df = df.sort_values('date')

    plt.figure(figsize=(10, 5))
    
    # CTR 선 그래프만 생성
    plt.plot(df['week_label'], df['ctr'], 
             marker='o', linestyle='-', color='royalblue', 
             linewidth=2.5, markersize=8, label='CTR (%)')
    
    # 데이터 포인트 위에 수치 표시
    for i, txt in enumerate(df['ctr']):
        plt.annotate(f"{txt:.2f}%", 
                     (df['week_label'].iloc[i], df['ctr'].iloc[i]),
                     textcoords="offset points", 
                     xytext=(0, 12), 
                     ha='center', 
                     fontsize=10, 
                     fontweight='bold',
                     color='navy')

    plt.title(f"Weekly CTR Trend: {account_id}", fontsize=14, pad=20)
    plt.xlabel("Week (Sunday ending)", fontsize=11)
    plt.ylabel("Click-Through Rate (%)", fontsize=11)
    
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.ylim(0, df['ctr'].max() * 1.3)  # 수치 표시를 위해 상단 여유 확보
    
    plt.tight_layout()

    # 이미지 저장 경로 설정
    os.makedirs('static/charts', exist_ok=True)
    file_path = f"static/charts/{account_id}_ctr_trend.png"
    plt.savefig(file_path)
    plt.close() 
    
    return file_path

# New function to create target analysis chart
def create_target_analysis_chart(target_df, top_ads_df, account_id):
    if target_df.empty: return None

    # 3개 광고를 하나의 이미지(Subplots)에 담기
    ad_ids = top_ads_df['ad_id'].tolist()
    fig, axes = plt.subplots(len(ad_ids), 1, figsize=(10, 5 * len(ad_ids)))
    if len(ad_ids) == 1: axes = [axes]

    for i, aid in enumerate(ad_ids):
        ad_name = top_ads_df[top_ads_df['ad_id'] == aid]['ad_name'].values[0]
        subset = target_df[target_df['ad_id'] == aid].head(5) # 광고별 상위 5개 타겟
        
        # '남성 25-34' 형태의 레이블 생성
        subset['target_label'] = subset['gender'] + " " + subset['age']
        
        axes[i].barh(subset['target_label'], subset['ctr'], color='orange')
        axes[i].set_title(f"AD: {ad_name} - Top Target CTR", fontsize=12)
        axes[i].set_xlabel("CTR (%)")
        axes[i].invert_yaxis() # 높은 순위가 위로 오게

    plt.tight_layout()
    file_path = f"static/charts/{account_id}_target_analysis.png"
    plt.savefig(file_path)
    plt.close()
    return file_path

def create_bottom_target_chart(target_df, bottom_ads_df, account_id):
    if target_df is None or target_df.empty: return None

    ad_ids = bottom_ads_df['ad_id'].tolist()
    fig, axes = plt.subplots(len(ad_ids), 1, figsize=(10, 5 * len(ad_ids)))
    if len(ad_ids) == 1: axes = [axes]

    for i, aid in enumerate(ad_ids):
        ad_name = bottom_ads_df[bottom_ads_df['ad_id'] == aid]['ad_name'].values[0]
        # 하위 효율 타겟 상위 5개 (반응이 가장 없는 타겟들)
        subset = target_df[target_df['ad_id'] == aid].head(5)
        subset['target_label'] = subset['gender'] + " " + subset['age']
        
        axes[i].barh(subset['target_label'], subset['ctr'], color='salmon') # 색상 변경
        axes[i].set_title(f"Low Performance AD: {ad_name}", fontsize=12, color='red')
        axes[i].set_xlabel("CTR (%)")
        axes[i].invert_yaxis()

    plt.tight_layout()
    file_path = f"static/charts/{account_id}_bottom_analysis.png"
    plt.savefig(file_path)
    plt.close()
    return file_path