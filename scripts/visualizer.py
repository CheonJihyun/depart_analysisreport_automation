# scripts/visualizer.py
import io
import math
import os
import colorsys
from typing import Any, Dict, List, Optional

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import pandas as pd

DEFAULT_THEME = "#4e73df"


def _configure_matplotlib_fonts() -> None:
    # Prefer Korean-capable fonts to avoid broken glyphs in SVG.
    preferred = [
        "Apple SD Gothic Neo",
        "Noto Sans KR",
        "Malgun Gothic",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    plt.rcParams["font.family"] = preferred
    plt.rcParams["axes.unicode_minus"] = False
    # Embed glyphs as paths for consistent rendering in SVG/PDF.
    plt.rcParams["svg.fonttype"] = "path"


_configure_matplotlib_fonts()


def _normalize_hex(hex_color: str) -> str:
    if not hex_color:
        return DEFAULT_THEME
    color = hex_color.strip().lower()
    if color.startswith("#"):
        color = color[1:]
    if len(color) == 3:
        color = "".join([c * 2 for c in color])
    if len(color) != 6 or any(c not in "0123456789abcdef" for c in color):
        return DEFAULT_THEME
    return "#" + color


def _hex_to_rgb01(hex_color: str) -> tuple:
    hex_color = _normalize_hex(hex_color)[1:]
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    return r, g, b


def _rgb01_to_hex(r: float, g: float, b: float) -> str:
    r_i = int(max(0.0, min(1.0, r)) * 255)
    g_i = int(max(0.0, min(1.0, g)) * 255)
    b_i = int(max(0.0, min(1.0, b)) * 255)
    return "#{:02x}{:02x}{:02x}".format(r_i, g_i, b_i)


def _adjust_lightness(hex_color: str, delta: float) -> str:
    r, g, b = _hex_to_rgb01(hex_color)
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = max(0.0, min(1.0, l + delta))
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return _rgb01_to_hex(r, g, b)


def build_color_map(theme_color: str) -> Dict[str, Any]:
    base = _normalize_hex(theme_color)
    light = _adjust_lightness(base, 0.22)
    lighter = _adjust_lightness(base, 0.38)
    dark = _adjust_lightness(base, -0.18)
    darker = _adjust_lightness(base, -0.32)
    series = [base, dark, light, darker]
    return {
        "base": base,
        "light": light,
        "lighter": lighter,
        "dark": dark,
        "darker": darker,
        "series": series,
        "grid": "#e6e6e6",
        "text": "#111111",
        "muted": "#666666",
    }


def relative_luminance(hex_color: str) -> float:
    r, g, b = _hex_to_rgb01(hex_color)

    def to_lin(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r_l, g_l, b_l = map(to_lin, (r, g, b))
    return 0.2126 * r_l + 0.7152 * g_l + 0.0722 * b_l


def is_dark_color(hex_color: str) -> bool:
    return relative_luminance(hex_color) < 0.5


def _fig_to_svg(fig) -> str:
    buf = io.StringIO()
    fig.savefig(buf, format="svg", bbox_inches="tight")
    plt.close(fig)
    svg = buf.getvalue()
    idx = svg.find("<svg")
    if idx != -1:
        svg = svg[idx:]
    return svg


def _style_axes(ax, color_map: Dict[str, Any], grid_axis: Optional[str] = "y") -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#dddddd")
    ax.spines["bottom"].set_color("#dddddd")
    ax.tick_params(colors="#666666", labelsize=8)
    if grid_axis in ("x", "y", "both"):
        ax.grid(True, axis=grid_axis, color=color_map["grid"], linewidth=0.8)
    else:
        ax.grid(False)


def _value_colors(values: List[float], color_map: Dict[str, Any]):
    if not values:
        return [color_map["base"]]
    vmin, vmax = min(values), max(values)
    if vmax == vmin:
        return [color_map["base"] for _ in values]
    cmap = LinearSegmentedColormap.from_list(
        "theme",
        [color_map["lighter"], color_map["light"], color_map["base"], color_map["dark"]],
    )
    return [cmap((v - vmin) / (vmax - vmin)) for v in values]


def render_line_chart(dataset: Dict[str, Any], color_map: Dict[str, Any], compact: bool = False) -> str:
    if not dataset:
        return ""
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return ""

    x = list(range(len(labels)))
    fig, ax = plt.subplots(figsize=(6, 3) if not compact else (3.2, 1.6))

    for idx, s in enumerate(series):
        data = s.get("data") or []
        if not data:
            continue
        color = color_map["series"][idx % len(color_map["series"])]
        ax.plot(x[: len(data)], data, color=color, linewidth=2)

    if compact:
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
    else:
        tick_count = min(6, len(labels))
        if tick_count > 1:
            step = max(1, math.floor(len(labels) / (tick_count - 1)))
            idxs = list(range(0, len(labels), step))
            if idxs[-1] != len(labels) - 1:
                idxs.append(len(labels) - 1)
        else:
            idxs = [0]
        ax.set_xticks(idxs)
        ax.set_xticklabels([labels[i] for i in idxs], rotation=30, ha="right", fontsize=8)
        unit = dataset.get("unit", "")
        if unit:
            ax.set_ylabel(unit, fontsize=8, color=color_map["muted"])
        _style_axes(ax, color_map)

    fig.tight_layout(pad=0.6)
    return _fig_to_svg(fig)


def render_bar_h_chart(
    dataset: Dict[str, Any],
    color_map: Dict[str, Any],
    compact: bool = False,
    chart_width: float = None,
    chart_height: float = None,
) -> str:
    if not dataset:
        return ""
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return ""

    values = series[0].get("data") or []
    if not values:
        return ""

    labels = labels[: len(values)]
    y = list(range(len(labels)))
    if compact:
        width, height = 3.2, 1.8
    else:
        width = chart_width if isinstance(chart_width, (int, float)) else 6
        height = chart_height if isinstance(chart_height, (int, float)) else 3.4
    fig, ax = plt.subplots(figsize=(width, height))

    colors = _value_colors(values, color_map)
    ax.barh(y, values, color=colors)
    ax.invert_yaxis()

    if compact:
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
    else:
        ax.set_yticks(y)
        ax.set_yticklabels(labels, fontsize=8)
        unit = dataset.get("unit", "")
        if unit:
            ax.set_xlabel(unit, fontsize=8, color=color_map["muted"])
        _style_axes(ax, color_map, grid_axis=None)
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.tight_layout(pad=0.6)
    return _fig_to_svg(fig)

def render_bar_v_chart(dataset: Dict[str, Any], color_map: Dict[str, Any], compact: bool = False) -> str:
    if not dataset:
        return ""
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return ""

    values = series[0].get("data") or []
    if not values:
        return ""

    labels = labels[: len(values)]
    x = list(range(len(labels)))
    fig, ax = plt.subplots(figsize=(6, 3.4) if not compact else (3.2, 1.8))

    colors = _value_colors(values, color_map)
    ax.bar(x, values, color=colors)

    if compact:
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8, rotation=30, ha="right")
        unit = dataset.get("unit", "")
        if unit:
            ax.set_ylabel(unit, fontsize=8, color=color_map["muted"])
        _style_axes(ax, color_map, grid_axis=None)
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.tight_layout(pad=0.6)
    return _fig_to_svg(fig)


def _render_heatmap(rows: List[Dict[str, Any]], metric: str, color_map: Dict[str, Any]) -> str:
    df = pd.DataFrame(rows)
    if df.empty or metric not in df.columns:
        return ""
    if "age" not in df.columns or "gender" not in df.columns:
        return ""

    pivot = df.pivot_table(index="gender", columns="age", values=metric, aggfunc="mean")

    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    gender_order = ["female", "male"]
    pivot = pivot.reindex(
        index=[g for g in gender_order if g in pivot.index],
        columns=[a for a in age_order if a in pivot.columns],
    )
    if pivot.empty:
        return ""

    fig, ax = plt.subplots(figsize=(6, 3.2))
    cmap = LinearSegmentedColormap.from_list(
        "theme",
        [color_map["lighter"], color_map["light"], color_map["base"], color_map["dark"]],
    )
    im = ax.imshow(pivot.values, cmap=cmap)

    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.iloc[i, j]
            if pd.isna(val):
                continue
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=7, color="#111111")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(c) for c in pivot.columns], fontsize=8)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(c) for c in pivot.index], fontsize=8)
    ax.tick_params(axis="x", bottom=True, top=False)

    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout(pad=0.6)
    return _fig_to_svg(fig)


def _render_simple_table(rows: List[Dict[str, Any]]) -> str:
    df = pd.DataFrame(rows)
    if df.empty:
        return ""

    max_rows = 12
    if len(df) > max_rows:
        df = df.head(max_rows)

    fig_height = 0.35 * len(df) + 1.2
    fig, ax = plt.subplots(figsize=(6, fig_height))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(7)
    table.scale(1, 1.2)

    fig.tight_layout(pad=0.6)
    return _fig_to_svg(fig)


def render_table_chart(dataset: Dict[str, Any], color_map: Dict[str, Any], metric: str = None) -> str:
    if not dataset:
        return ""
    rows = dataset.get("rows") or []
    if not rows:
        return ""

    if metric:
        heatmap_svg = _render_heatmap(rows, metric, color_map)
        if heatmap_svg:
            return heatmap_svg

    if "age" in rows[0] and "gender" in rows[0] and "ctr" in rows[0]:
        heatmap_svg = _render_heatmap(rows, "ctr", color_map)
        if heatmap_svg:
            return heatmap_svg

    return _render_simple_table(rows)


def render_content_card(dataset: Dict[str, Any], color_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not dataset:
        return []

    items = dataset.get("items") or []
    rendered = []

    for item in items:
        new_item = dict(item)
        details = item.get("target_details") or []
        chart_svg = ""

        if details:
            detail_df = pd.DataFrame(details)
            if not detail_df.empty and "ctr" in detail_df.columns:
                detail_df = detail_df.sort_values("ctr", ascending=False).head(6)
                labels = [f"{row['age']} {row['gender']}" for _, row in detail_df.iterrows()]
                values = detail_df["ctr"].tolist()
                mini_ds = {
                    "kind": "bar_v",
                    "labels": labels,
                    "series": [{"name": "ctr", "data": values}],
                    "unit": "%",
                }
                chart_svg = render_bar_v_chart(mini_ds, color_map, compact=True)

        new_item["chart"] = chart_svg
        rendered.append(new_item)

    return rendered


def render_dataset(dataset: Dict[str, Any], color_map: Dict[str, Any], **kwargs):
    if not dataset:
        return ""

    kind = dataset.get("kind")
    renderers = {
        "line": render_line_chart,
        "bar_h": render_bar_h_chart,
        "bar_v": render_bar_v_chart,
        "table": render_table_chart,
        "content_card": render_content_card,
    }

    renderer = renderers.get(kind)
    if not renderer:
        return ""

    return renderer(dataset, color_map, **kwargs)






import numpy as np

def render_bubble_chart(dataset, color_map, compact=True):
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    ctr_data = series[0]["data"] if series else []
    if not labels or not ctr_data: return ""

    # 1. 원의 크기 (면적 s와 반지름 r 계산)
    max_ctr = max(ctr_data) if max(ctr_data) > 0 else 1
    # 사진처럼 큼직하게 보이도록 면적 대폭 상향
    sizes = [(c / max_ctr) * 5000 + 1200 for c in ctr_data]
    radii = [np.sqrt(s / 500) for s in sizes] # 배치용 가상 반지름

    # 2. 수동 밀착 배치 좌표 (원 8개 기준 가장 촘촘한 구조)
    # [중앙, 상, 하, 좌, 우, 대각선...] 순서로 꽂아버립니다.
    # r 값을 이용해 서로 '접하게' 좌표를 설정합니다.
    num = len(labels)
    x = np.zeros(num)
    y = np.zeros(num)
    
    if num > 0: x[0], y[0] = 0, 0 # 1등은 정중앙
    if num > 1: x[1], y[1] = 0, radii[0] + radii[1] * 0.7 # 2등은 바로 위
    if num > 2: x[2], y[2] = -(radii[0] + radii[2]) * 0.8, 0 # 3등은 왼쪽
    if num > 3: x[3], y[3] = (radii[0] + radii[3]) * 0.8, 0 # 4등은 오른쪽
    if num > 4: x[4], y[4] = 0, -(radii[0] + radii[4]) * 0.7 # 5등은 아래
    # 나머지 6, 7, 8등은 빈 구석 대각선에 배치
    if num > 5: x[5], y[5] = -radii[2], radii[1] 
    if num > 6: x[6], y[6] = radii[3], radii[1]
    if num > 7: x[7], y[7] = -radii[2], -radii[4]

    fig, ax = plt.subplots(figsize=(4, 4))

    # 3. 버블 그리기
    bubble_color = color_map.get("primary") or color_map.get("main") or "#4e73df"
    
    # 테두리를 두껍게(linewidth) 주면 원끼리 접한 경계가 선명해서 사진 느낌이 납니다.
    ax.scatter(x, y, s=sizes, alpha=0.9, color=bubble_color, 
               edgecolors="white", linewidth=2.5, zorder=2)

    # 4. 텍스트 라벨 (이름\n(수치%))
    for i, txt in enumerate(labels):
        display_text = f"{txt}\n({ctr_data[i]:.2f}%)"
        ax.text(x[i], y[i], display_text, fontsize=9, ha='center', va='center', 
                fontweight='bold', color='white', zorder=3)

    # 5. 스타일 정리 (범위는 데이터에 맞춰 자동 조절)
    all_r = max(radii) * 2.5
    ax.set_xlim(-all_r, all_r)
    ax.set_ylim(-all_r, all_r)
    ax.axis('off')
    
    fig.tight_layout(pad=0)
    return _fig_to_svg(fig)
