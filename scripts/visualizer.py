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
import numpy as np
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


def _contrast_text_color(rgba, threshold: float = 0.45) -> str:
    r, g, b = rgba[:3]
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "white" if luminance < threshold else "#1a1a1a"


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
        # Match example.py barh ratio (4:6) so charts are quarter-width and vertically long.
        width = chart_width if isinstance(chart_width, (int, float)) else 4
        height = chart_height if isinstance(chart_height, (int, float)) else 6
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

def _format_chart_value(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:.2f}"


def render_bar_v_chart(
    dataset: Dict[str, Any],
    color_map: Dict[str, Any],
    compact: bool = False,
    show_labels: bool = False,
    show_values: bool = False,
) -> str:
    if not dataset:
        return ""
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return ""

    values = pd.Series(pd.to_numeric(series[0].get("data") or [], errors="coerce")).fillna(0.0).tolist()
    if not values:
        return ""

    labels = labels[: len(values)]
    x = list(range(len(labels)))
    if compact and (show_labels or show_values):
        fig_size = (3.2, 2.5)
    else:
        fig_size = (6, 3.4) if not compact else (3.2, 1.8)
    fig, ax = plt.subplots(figsize=fig_size)

    colors = _value_colors(values, color_map)
    bars = ax.bar(x, values, color=colors)

    max_val = max(values) if values else 0.0
    y_pad = max(max_val * 0.2, 0.4)
    y_top = max(max_val + y_pad, 1.0)
    ax.set_ylim(0, y_top)

    if show_values:
        unit = str(dataset.get("unit") or "").strip()
        suffix = unit if unit in {"%", "회", "명"} else ""
        for bar, value in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + y_pad * 0.08,
                f"{_format_chart_value(float(value))}{suffix}",
                ha="center",
                va="bottom",
                fontsize=6 if compact else 7,
                color=color_map["muted"],
            )

    if compact:
        if show_labels:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontsize=5.5, rotation=30, ha="right")
            ax.tick_params(axis="x", length=0, pad=1, colors="#666666")
        else:
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
                chart_svg = render_bar_v_chart(
                    mini_ds,
                    color_map,
                    compact=True,
                    show_labels=True,
                    show_values=True,
                )

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
        "bubble": render_bubble_chart,
        "table": render_table_chart,
        "content_card": render_content_card,
    }

    renderer = renderers.get(kind)
    if not renderer:
        return ""

    return renderer(dataset, color_map, **kwargs)

def render_bubble_chart(
    dataset: Dict[str, Any],
    color_map: Dict[str, Any],
    compact: bool = True,
    palette: Optional[List[str]] = None,
) -> str:
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return ""

    ctr_raw = pd.to_numeric(series[0].get("data") or [], errors="coerce")
    size_raw = ctr_raw
    if len(series) > 1:
        size_raw = pd.to_numeric(series[1].get("data") or [], errors="coerce")

    n = min(len(labels), len(ctr_raw), len(size_raw))
    if n == 0:
        return ""

    rows = []
    for i in range(n):
        ctr = ctr_raw[i]
        size_val = size_raw[i]
        if pd.isna(ctr) or pd.isna(size_val):
            continue
        rows.append({
            "label": str(labels[i]),
            "ctr": float(ctr),
            "size": max(float(size_val), 1.0),
        })
    if not rows:
        return ""

    rows.sort(key=lambda x: x["size"], reverse=True)

    size_values = np.sqrt(np.array([r["size"] for r in rows], dtype=float))
    size_norm = size_values / size_values.max() if size_values.max() > 0 else np.ones(len(rows))
    max_r = 0.58 if len(rows) <= 3 else (0.45 if len(rows) <= 6 else 0.36)
    min_r = max(0.12, max_r * 0.38)
    radii = (min_r + (max_r - min_r) * size_norm).tolist()

    positions = [np.array([0.0, 0.0])]
    if len(radii) > 1:
        positions.append(np.array([radii[0] + radii[1], 0.0]))

    for i in range(2, len(radii)):
        r_new = radii[i]
        placed = False
        for j in range(len(positions)):
            for k in range(j + 1, len(positions)):
                r1, r2 = radii[j], radii[k]
                p1, p2 = positions[j], positions[k]
                dist = np.linalg.norm(p1 - p2)
                if dist <= 1e-9:
                    continue
                if dist > (r1 + r_new) + (r2 + r_new):
                    continue

                d1 = r1 + r_new
                d2 = r2 + r_new
                a = (d1**2 - d2**2 + dist**2) / (2 * dist)
                h = np.sqrt(max(0.0, d1**2 - a**2))
                p3 = p1 + a * (p2 - p1) / dist

                for sign in (-1, 1):
                    test_pos = np.array([
                        p3[0] + sign * h * (p2[1] - p1[1]) / dist,
                        p3[1] - sign * h * (p2[0] - p1[0]) / dist,
                    ])
                    if all(
                        np.linalg.norm(test_pos - p) >= (radii[idx] + r_new) * 0.99
                        for idx, p in enumerate(positions)
                    ):
                        positions.append(test_pos)
                        placed = True
                        break
                if placed:
                    break
            if placed:
                break
        if not placed:
            positions.append(np.array([radii[0] + r_new, r_new * i]))

    if palette:
        bubble_palette = palette
    else:
        bubble_palette = [
            color_map["lighter"],
            color_map["light"],
            color_map["base"],
            color_map["dark"],
        ]
    cmap = LinearSegmentedColormap.from_list("bubble_palette", bubble_palette, N=256).reversed()

    ctr_values = [r["ctr"] for r in rows]
    vmin, vmax = min(ctr_values), max(ctr_values)

    fig_size = (4, 4) if compact else (5.4, 5.4)
    fig, ax = plt.subplots(figsize=fig_size)

    for idx, pos in enumerate(positions):
        ctr = rows[idx]["ctr"]
        label = rows[idx]["label"]
        radius = radii[idx]
        norm = 0.5 if abs(vmax - vmin) < 1e-12 else (ctr - vmin) / (vmax - vmin)
        color = cmap(norm)
        circle = plt.Circle(pos, radius, facecolor=color, edgecolor="white", linewidth=1.6, alpha=1.0)
        ax.add_patch(circle)

        font_size = max(6, min(11, 4 + radius * 10))
        ax.text(
            pos[0],
            pos[1],
            f"{label}\n({ctr:.2f}%)",
            fontsize=font_size,
            ha="center",
            va="center",
            color=_contrast_text_color(color, threshold=0.45),
            fontweight="bold",
        )

    all_points = np.array(
        [p + np.array([r, r]) for p, r in zip(positions, radii)] +
        [p - np.array([r, r]) for p, r in zip(positions, radii)]
    )
    x_min, x_max = float(all_points[:, 0].min()), float(all_points[:, 0].max())
    y_min, y_max = float(all_points[:, 1].min()), float(all_points[:, 1].max())
    x_pad = max((x_max - x_min) * 0.05, 0.08)
    y_pad = max((y_max - y_min) * 0.05, 0.08)
    ax.set_xlim(x_min - x_pad, x_max + x_pad)
    ax.set_ylim(y_min - y_pad, y_max + y_pad)
    ax.set_aspect("equal")
    ax.axis("off")

    return _fig_to_svg(fig)
