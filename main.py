import json
from datetime import datetime
import pandas as pd
from scripts.visualizer import build_color_map, render_dataset, is_dark_color, render_bar_v_chart
from scripts.reporter import generate_html
from to_json import run as generate_json
import time
def _load_report(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _top_targets(rows, metric: str, limit: int = 2):
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda r: r.get(metric, float("-inf")), reverse=True)
    results = []
    for idx, row in enumerate(sorted_rows[:limit], 1):
        age = row.get("age", "")
        gender = row.get("gender", "")
        label = f"{idx}위 : {age} {gender}".strip()
        results.append(label)
    return results


def _find_metric(rows, age: str, gender: str, metric: str):
    if not rows:
        return None
    for row in rows:
        if row.get("age") == age and row.get("gender") == gender:
            return row.get(metric)
    return None


def _average_series(dataset: dict):
    if not dataset:
        return None
    series = dataset.get("series") or []
    if not series:
        return None
    data = series[0].get("data") or []
    if not data:
        return None
    return sum(data) / len(data)


import pandas as pd
from scripts.visualizer import build_color_map, render_bubble_chart

def _combo_cards(dataset: dict):
    rows = (dataset or {}).get("rows") or []
    if not rows:
        return []

    import pandas as pd
    from scripts.visualizer import build_color_map, render_bubble_chart

    df = pd.DataFrame(rows)
    color_map = build_color_map("#4e73df")
    
    # [수정 핵심] 1. 전체 데이터를 combo_overall_ctr 기준으로 먼저 정렬합니다.
    # (높은 CTR이 위로 오도록 내림차순 정렬)
    if 'combo_overall_ctr' in df.columns:
        df = df.sort_values(by='combo_overall_ctr', ascending=False)

    # 필수 조합별로 그룹화
    grouped = df.groupby(['ess_1', 'ess_2', 'combo_overall_ctr'], sort=False)

    cards = []
    for i, ((e1, e2, ctr), group_df) in enumerate(grouped, 1):
        if i > 6: break
        
        # 1. 버블 차트용 데이터셋 구성
        mini_ds = {
            "kind": "bubble",
            "labels": group_df['var_keyword'].tolist(),
            "series": [
                {"name": "CTR", "data": group_df['with_var_ctr'].tolist()},
                {"name": "Imps", "data": group_df.get('var_imps', group_df['with_var_ctr']).tolist()}
            ],
            "unit": "%"
        }

        # 2. 차트 생성 및 정제 (타임스탬프 등 제거)
        chart_svg = render_bubble_chart(mini_ds, color_map, compact=True)
        if chart_svg and "<svg" in chart_svg:
            chart_svg = chart_svg[chart_svg.find("<svg"):]

        # 3. [형식 수정] 첫 번째 함수와 동일한 ctr_text 로직 적용
        if isinstance(ctr, (int, float)):
            ctr_text = f"{ctr:.2f}"
        else:
            ctr_text = str(ctr) if ctr is not None else "-"

        # 4. 카드 데이터 구성 (요청하신 Title 형식 적용)
        cards.append({
            "rank": i,
            "title": f"조합 {i}위 : {e1} + {e2} ({ctr_text}%)",
            "sub": "함께 쓰인 브랜드 변수 키워드별 성과",
            "image": chart_svg,
            "ctr_text": f"{ctr_text}%"  # 혹시 다른 곳에서 쓸까봐 남겨둡니다
        })
        
    return cards


import asyncio
from playwright.sync_api import sync_playwright
# pdf 변환 함수
def export_to_pdf(html_path, output_pdf_path):
    with sync_playwright() as p:
        # 브라우저 실행 (백그라운드)
        browser = p.chromium.launch()
        page = browser.new_page()
        
        # 1. HTML 파일 로드 (절대 경로 권장)
        import os
        file_url = f"file://{os.path.abspath(html_path)}"
        page.goto(file_url, wait_until="networkidle") # 네트워크 활동이 멈출 때까지 대기
        
        # 2. PDF 저장 설정
        page.pdf(
            path=output_pdf_path,
            format="A4",           # 용지 규격
            print_background=True, # 배경 색상/이미지 포함 (중요!)
            margin={"top": "20px", "bottom": "20px", "left": "20px", "right": "20px"}
        )
        
        browser.close()
    print(f" PDF 저장 완료: {output_pdf_path}")


def run():
    start_time = time.time()

    config = {
        "target_id": 8,
        "fb_ad_account_id":"act_618278251632554",
        "start":"2025-02-13",
        "end": "2026-02-19",
        "main_age": "35-44",
        "main_gender": "female",
        "avoid_age": "",
        "avoid_gender":"male"
    }
    target_id, fb_ad_account_id = config["target_id"], config["fb_ad_account_id"]
    start, end = config["start"], config["end"]
    main_age, main_gender = config["main_age"], config["main_gender"]
    avoid_age, avoid_gender = config["avoid_age"], config["avoid_gender"]

    # 3. to_json 실행코드 (수정된 파라미터 방식)
    generate_json(target_id=target_id, fb_ad_account_id=fb_ad_account_id,\
                  start=start, end=end,\
                   main_age=main_age, main_gender=main_gender,\
                    avoid_age=avoid_age, avoid_gender=avoid_gender)
    
    # 사용자 입력
    report_path = "json_reports/integrated_report.json"
    with open(report_path, 'r', encoding='utf-8') as f:
        full_data = json.load(f)

    # 3. JSON 안에 담긴 datasets 꺼내기
    # 주의: JSON에서 불러오면 데이터프레임이 아니라 '리스트' 형태이므로 그에 맞춰 처리합니다.
    raw_datasets = full_data.get("datasets", {})
    theme_color = "#2A3D1E"

    report_json = _load_report(report_path)
    meta = report_json.get("meta", {})
    summary = report_json.get("summary", {})
    datasets = report_json.get("datasets", {})

    acc_name = meta.get("account_name", "")
    period = meta.get("period", "")
    period_ads = meta.get("period_ads", "")
    period_contents = meta.get("period_contents", "")
    year = period.split("-")[0] if period else ""
    generated_at = meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M")

    color_map = build_color_map(theme_color)
    theme = {
        "base": color_map["base"],
        "title": color_map["darker"],
        "cover_text": "#ffffff" if is_dark_color(color_map["base"]) else "#000000",
    }

    charts = {}

    def add_chart(key: str, dataset_key: str, **kwargs):
        svg = render_dataset(datasets.get(dataset_key), color_map, **kwargs)
        if isinstance(svg, str) and svg:
            charts[key] = svg

    add_chart("followers", "insta_followers")
    add_chart("ctr", "ctr_trend")
    add_chart("organic_views_1", "organic_trend")
    add_chart("organic_views_2", "organic_trend_monthly")
    add_chart("profile_visits_1", "insta_profile_visits")
    add_chart("profile_visits_2", "insta_profile_visits_monthly")

    heatmap_ds = datasets.get("target_heatmap")
    heatmap_imp = render_dataset(heatmap_ds, color_map, metric="impressions")
    if heatmap_imp:
        charts["heatmap_impressions"] = heatmap_imp
    heatmap_ctr = render_dataset(heatmap_ds, color_map, metric="ctr")
    if heatmap_ctr:
        charts["heatmap_ctr"] = heatmap_ctr

    add_chart("keyword_overall_top_noun", "overall_top_noun")
    add_chart("keyword_overall_top_verb_adj", "overall_top_va")
    add_chart("keyword_overall_bottom_noun", "overall_bottom_noun")
    add_chart("keyword_overall_bottom_verb_adj", "overall_bottom_va")

    add_chart("keyword_main_top_noun", "main_top_noun")
    add_chart("keyword_main_top_verb_adj", "main_top_va")
    add_chart("keyword_main_bottom_noun", "main_bottom_noun")
    add_chart("keyword_main_bottom_verb_adj", "main_bottom_va")

    add_chart("keyword_avoid_top_noun", "avoid_top_noun")
    add_chart("keyword_avoid_top_verb_adj", "avoid_top_va")
    add_chart("keyword_avoid_bottom_noun", "avoid_bottom_noun")
    add_chart("keyword_avoid_bottom_verb_adj", "avoid_bottom_va")

    def add_table(dataset_key: str, title: str, rank_head: str, kw_head: str):
        ds = datasets.get(dataset_key)
        
        # [수정] 데이터프레임 형식이 아니라 labels/series 형식을 체크합니다.
        if not ds or "labels" not in ds or "series" not in ds:
            return None
        
        labels = ds.get("labels", [])
        # series 안의 첫 번째 요소에서 data 리스트를 가져옵니다.
        series_data = ds.get("series", [{}])[0].get("data", [])
        
        rows = []
        # labels(키워드)와 series_data(CTR 값)를 매칭
        for i, (label, value) in enumerate(zip(labels, series_data), 1):
            rows.append([
                f"{i}위", 
                label, 
                f"{value:.2f}%"
            ])
        
        if not rows:
            return None

        return {
            "title": title,
            "headers": [rank_head, kw_head, "평균 CTR"],
            "rows": rows,
            "footnote": ""
        }

    # 2. 각 계층별(Overall, Main, Avoid) 테이블 묶음 생성
    # [Overall]
    o_top = [
        add_table("overall_top_noun", "전체 TOP 10 (명사)", "순위(상위)", "키워드(명사)"),
        add_table("overall_top_va", "전체 TOP 10 (형용사/동사)", "순위(상위)", "키워드(형용사/동사)")
    ]
    o_bot = [
        add_table("overall_bottom_noun", "전체 BOTTOM 10 (명사)", "순위(하위)", "키워드(명사)"),
        add_table("overall_bottom_va", "전체 BOTTOM 10 (형용사/동사)", "순위(하위)", "키워드(형용사/동사)")
    ]

    # [Main Target] - 조건부 생성
    m_top, m_bot = [], []
    if main_age and main_gender:
        m_top = [
            add_table("main_top_noun", f"{main_age} {main_gender} TOP 10 (명사)", "순위(상위)", "키워드(명사)"),
            add_table("main_top_va", f"{main_age} {main_gender} TOP 10 (형용사/동사)", "순위(상위)", "키워드(형용사/동사)")
        ]
        m_bot = [
            add_table("main_bottom_noun", f"{main_age} {main_gender} BOTTOM 10 (명사)", "순위(하위)", "키워드(명사)"),
            add_table("main_bottom_va", f"{main_age} {main_gender} BOTTOM 10 (형용사/동사)", "순위(하위)", "키워드(형용사/동사)")
        ]

    # [Avoid Target] - 조건부 생성
    a_top, a_bot = [], []
    if avoid_age and avoid_gender:
        a_top = [
            add_table("avoid_top_noun", f"{avoid_age} {avoid_gender} TOP 10 (명사)", "순위(상위)", "키워드(명사)"),
            add_table("avoid_top_va", f"{avoid_age} {avoid_gender} TOP 10 (형용사/동사)", "순위(상위)", "키워드(형용사/동사)")
        ]
        a_bot = [
            add_table("avoid_bottom_noun", f"{avoid_age} {avoid_gender} BOTTOM 10 (명사)", "순위(하위)", "키워드(명사)"),
            add_table("avoid_bottom_va", f"{avoid_age} {avoid_gender} BOTTOM 10 (형용사/동사)", "순위(하위)", "키워드(형용사/동사)")
        ]

    # 3. None 값(데이터 없음) 필터링 함수
    filter_none = lambda lst: [t for t in lst if t is not None]

    top_items = render_dataset(datasets.get("content_top_analysis"), color_map)
    if not isinstance(top_items, list):
        top_items = []
    bottom_items = render_dataset(datasets.get("content_bottom_analysis"), color_map)
    if not isinstance(bottom_items, list):
        bottom_items = []

    target_rows = (datasets.get("target_heatmap") or {}).get("rows") or []
    impressions_rank = _top_targets(target_rows, "impressions")
    ctr_rank = _top_targets(target_rows, "ctr")

    overall_ctr_val = _average_series(datasets.get("ctr_trend"))
    overall_ctr = f"{overall_ctr_val:.2f}" if isinstance(overall_ctr_val, (int, float)) else "-"

    main_ctr_val = _find_metric(target_rows, main_age, main_gender, "ctr") if main_age and main_gender else None
    main_ctr = f"{main_ctr_val:.2f}" if isinstance(main_ctr_val, (int, float)) else "-"

    avoid_ctr_val = _find_metric(target_rows, avoid_age, avoid_gender, "ctr") if avoid_age and avoid_gender else None
    avoid_ctr = f"{avoid_ctr_val:.2f}" if isinstance(avoid_ctr_val, (int, float)) else "-"



    cards = _combo_cards(datasets.get("overall_keyword_combo_detail"))
    cards_main = _combo_cards(datasets.get("main_keyword_combo_detail")) if main_age and main_gender else []
    cards_avoid = _combo_cards(datasets.get("avoid_keyword_combo_detail")) if avoid_age and avoid_gender else []






    context = {
        "css_path": "./templates/report.css",
        "theme": theme,
        "report": {
            "title": "보고서",
            "client": acc_name,
            "quarter_label": period,
            "year": year,
            "generated_at": generated_at,
            "brand": "De:part",
            "period_ads": period_ads or "-",
            "period_contents": period_contents or "-",
            "keyword_count": f"{summary.get('total_keywords', '-') }개",
            "overview_notes": [
                f"광고 {summary.get('total_ads', '-') }개",
                f"콘텐츠 {summary.get('total_contents', '-') }개",
            ],
        },
        "content": {
            "top_note": "",
            "top": top_items,
            "bottom_note": "",
            "bottom": bottom_items,
        },
        "charts": charts,
        "annotations": {
            "ctr": [],
            "organic": [],
        },
        "target": {
            "impressions_rank": impressions_rank,
            "ctr_note": "",
            "ctr_rank": ctr_rank,
        },
        "keywords": {
            "overall_top_note": "*3개 이상의 콘텐츠에 등장한 단어만 표시",
            "overall_top_tables": filter_none(o_top),
            "overall_combo_pages": [
                {
                    "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어"
                    f"<br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {overall_ctr}%",
                    "cards": cards,
                }
            ],
            "overall_bottom_note": "*3개 이상의 콘텐츠에 등장한 단어만 표시",
            "overall_bottom_tables": filter_none(o_bot),
            "main_target": {"title": f"{main_age} {main_gender} 성과 분석"} if main_age and main_gender else None,
            "main_top_tables": filter_none(m_top) if m_top else None,
            "main_combo_pages": [
                {
                    "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어"
                    f"<br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {main_ctr}%",
                    "cards": cards_main,
                }
            ] if main_age and main_gender else None,
            "main_bottom_tables": filter_none(m_bot) if m_bot else None,
            "avoid_target": {"title": f"{avoid_age} {avoid_gender} 성과 분석"} if avoid_age and avoid_gender else None,
            "avoid_top_tables":filter_none(a_top) if a_top else None,
            "avoid_combo_pages": [
                {
                    "note": f"*3개 이상의 콘텐츠에 등장한 조합만 표시<br>*업종 필수 키워드: 동일 업종의 상위 브랜드 10개의 웹사이트에서 자주 사용된 단어"
                    f"<br>*브랜드 변수 키워드: 필수 키워드 외 콘텐츠에 활용된 단어<br><br>*계정 전체 평균 CTR: {avoid_ctr}%",
                    "cards": cards_avoid,
                }
            ] if avoid_age and avoid_gender else None,
            "avoid_bottom_tables":filter_none(a_bot) if a_bot else None,
        },
        "appendix_groups": report_json.get("appendix_groups", []),
        # [
        #     {
        #         "title": "",
        #         "items": [
        #             {"title": "", "subtitle": "", "image": "", "headers": [], "rows": [[]], "footnote": ""}
        #         ],
        #     }
        # ]
        "appendix": [],
    }




    generate_html(context)
    
    # PDF 변환 추가
    export_to_pdf("report.html", f"outputs/{acc_name}_리포트.pdf")
    
    print(f"✅ {acc_name} 리포트 생성 완료!")

    end_time = time.time()
    elapsed_time = end_time - start_time # 소요 시간(초)

    print("-" * 50)
    print(f"⏳ 총 소요 시간: {elapsed_time:.2f}초") # 소수점 2자리까지 표시
    print("-" * 50)


if __name__ == "__main__":
    run()

