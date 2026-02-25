#!/usr/bin/env python3
"""
Visualize processor-derived JSON report data using example.ipynb-style charts.

Input:
- json_reports/integrated_report.json (default)

Output:
- static/output/*.png
- static/output/result.txt
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

try:
    import circlify
except Exception:
    circlify = None

try:
    import koreanize_matplotlib  # noqa: F401
except Exception:
    pass


# 컬러 팔레트 (matplotlib)
# 상위
g_cmap = ["#0B3D02", "#659348", "#E3CC97"]

# 하위
b_cmap = ["#EE8C8C", "#D7A9A9", "#E3CC97"]

b_map = b_cmap  # 호환용 alias

# Heatmap 컬러맵 (seaborn)
sea = "Greens"

# Existing internal aliases
G_CMAP = g_cmap
B_CMAP = b_cmap
SEA = sea

FIGSIZE_LINE = (10, 4)
FIGSIZE_BARH = (4, 6)
FIGSIZE_BARV = (10, 4)
FIGSIZE_HEATMAP = (10, 4)
FIGSIZE_CIRCLEPACK = (9, 7)
DPI_CIRCLEPACK = 120


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render charts from integrated_report.json into chart png files."
    )
    parser.add_argument(
        "--json-path",
        default="json_reports/integrated_report.json",
        help="Path to integrated report json.",
    )
    parser.add_argument(
        "--output-dir",
        default="./static/output",
        help="Directory to save chart png files and result.txt.",
    )
    parser.add_argument(
        "--main-age",
        default="35-44",
        help="Main target age for CTR summary (empty string means all ages).",
    )
    parser.add_argument(
        "--main-gender",
        default="female",
        help="Main target gender for CTR summary (empty string means all genders).",
    )
    parser.add_argument(
        "--avoid-age",
        default="",
        help="Avoid target age for CTR summary (empty string means all ages).",
    )
    parser.add_argument(
        "--avoid-gender",
        default="male",
        help="Avoid target gender for CTR summary (empty string means all genders).",
    )
    return parser.parse_args()


class OutputWriter:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.index = 0
        self.files: list[Path] = []

    @staticmethod
    def _safe_name(name: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_ " else "_" for ch in name).strip().replace(" ", "_")

    def save_fig(self, fig: plt.Figure, title: str) -> Path:
        self.index += 1
        path = self.output_dir / f"{self.index:03d}_{self._safe_name(title)}.png"
        fig.patch.set_facecolor("none")
        fig.patch.set_alpha(0.0)
        for ax in fig.axes:
            ax.set_facecolor("none")
        fig.tight_layout()
        fig.savefig(
            path,
            dpi=200,
            bbox_inches="tight",
            transparent=True,
            facecolor="none",
            edgecolor="none",
        )
        plt.close(fig)
        self.files.append(path)
        return path


def _series_df(dataset: dict) -> pd.DataFrame | None:
    labels = dataset.get("labels") or []
    series = dataset.get("series") or []
    if not labels or not series:
        return None
    base = pd.DataFrame({"label": labels})
    for s in series:
        name = s.get("name") or "value"
        base[name] = s.get("data") or []
    return base


def _contrast_text_color(rgba: Iterable[float], threshold: float = 0.5) -> str:
    r, g, b = rgba[:3]
    lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "white" if lum < threshold else "black"


def _format_point_value(value: float) -> str:
    if pd.isna(value):
        return ""
    if abs(float(value) - round(float(value))) < 1e-9:
        return f"{int(round(float(value))):,}"
    return f"{float(value):.2f}"


def _normalize_selector(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _format_ctr(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}"


def _avg_ctr_from_trend(dataset: dict | None) -> float | None:
    if not dataset:
        return None
    series = dataset.get("series") or []
    if not series:
        return None
    values = pd.to_numeric(series[0].get("data") or [], errors="coerce")
    values = pd.Series(values).dropna()
    if values.empty:
        return None
    return float(values.mean())


def _target_ctr(rows: list[dict], age: str | None = None, gender: str | None = None) -> float | None:
    if not rows:
        return None
    df = pd.DataFrame(rows)
    required = {"impressions", "clicks"}
    if not required.issubset(df.columns):
        return None

    age_sel = _normalize_selector(age)
    gender_sel = _normalize_selector(gender)
    if age_sel:
        df = df[df["age"].astype(str) == age_sel]
    if gender_sel:
        df = df[df["gender"].astype(str) == gender_sel]
    if df.empty:
        return None

    imps = pd.to_numeric(df["impressions"], errors="coerce").fillna(0)
    clicks = pd.to_numeric(df["clicks"], errors="coerce").fillna(0)
    total_imps = float(imps.sum())
    total_clicks = float(clicks.sum())
    if total_imps <= 0:
        return None
    return (total_clicks / total_imps) * 100.0


def plot_line(dataset_key: str, dataset: dict, writer: OutputWriter, logs: list[str]) -> None:
    df = _series_df(dataset)
    if df is None or df.empty:
        logs.append(f"[SKIP] line:{dataset_key}")
        return

    value_col = [c for c in df.columns if c != "label"]
    if not value_col:
        logs.append(f"[SKIP] line:{dataset_key}")
        return
    value_col = value_col[0]

    d = df.copy()
    d["label_dt"] = pd.to_datetime(d["label"], errors="coerce")
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col]).sort_values("label_dt", na_position="last")
    if d.empty:
        logs.append(f"[SKIP] line:{dataset_key}")
        return

    x = d["label_dt"] if d["label_dt"].notna().any() else d["label"]
    y = d[value_col]
    y_floor = 9000.0 if dataset_key == "insta_followers" else 0.0
    y_plot = y.where(y >= y_floor, np.nan)
    if y_plot.notna().sum() == 0:
        logs.append(f"[SKIP] line:{dataset_key} no_values_above_floor({y_floor:g})")
        return

    fig, ax = plt.subplots(figsize=FIGSIZE_LINE)
    ax.plot(x, y_plot, marker="o", markersize=3.0, linewidth=2.0, color=G_CMAP[0])
    ax.fill_between(x, y_plot, alpha=0.12, color=G_CMAP[0])
    for xi, yi in zip(x, y_plot):
        if pd.isna(yi):
            continue
        label = _format_point_value(float(yi))
        if label:
            ax.annotate(
                label,
                (xi, yi),
                textcoords="offset points",
                xytext=(0, 6),
                ha="center",
                va="bottom",
                fontsize=8,
                color=G_CMAP[0],
            )
    y_max = float(np.nanmax(y_plot.to_numpy()))
    if y_max <= y_floor:
        y_max = y_floor + max(1.0, y_floor * 0.01)
    y_pad = max((y_max - y_floor) * 0.12, 1.0)
    ax.set_ylim(bottom=y_floor, top=y_max + y_pad)
    ax.set_xlabel("")
    ax.set_ylabel(dataset.get("unit") or value_col)
    ax.grid(axis="y", alpha=0.3)
    writer.save_fig(fig, f"{dataset_key}_line")
    logs.append(f"[OK] line:{dataset_key} rows={len(d)}")


def plot_bar_h(dataset_key: str, dataset: dict, writer: OutputWriter, logs: list[str]) -> None:
    df = _series_df(dataset)
    if df is None or df.empty:
        logs.append(f"[SKIP] bar_h:{dataset_key}")
        return

    value_cols = [c for c in df.columns if c != "label"]
    if not value_cols:
        logs.append(f"[SKIP] bar_h:{dataset_key}")
        return
    value_col = value_cols[0]

    d = df[["label", value_col]].copy()
    d["label"] = d["label"].astype(str)
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[value_col])
    if d.empty:
        logs.append(f"[SKIP] bar_h:{dataset_key}")
        return

    # top/bottom 키워드 바차트는 표시 순서를 위아래 반전
    if dataset_key.startswith(
        (
            "overall_top_",
            "overall_bottom_",
            "main_top_",
            "main_bottom_",
            "avoid_top_",
            "avoid_bottom_",
        )
    ):
        d = d.iloc[::-1].reset_index(drop=True)

    if dataset_key.startswith("avoid_top_"):
        palette = B_CMAP
    elif dataset_key.startswith("avoid_bottom_"):
        palette = G_CMAP
    else:
        palette = G_CMAP if ("top" in dataset_key and "bottom" not in dataset_key) else B_CMAP
    cmap = mcolors.LinearSegmentedColormap.from_list(f"{dataset_key}_cmap", palette, N=256).reversed()
    vmin, vmax = float(d[value_col].min()), float(d[value_col].max())
    if abs(vmax - vmin) < 1e-12:
        colors = [cmap(0.5)] * len(d)
    else:
        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        colors = [cmap(norm(v)) for v in d[value_col]]

    fig, ax = plt.subplots(figsize=FIGSIZE_BARH)
    ax.barh(d["label"], d[value_col], color=colors)
    ax.set_xlabel(dataset.get("unit") or value_col)
    ax.grid(axis="x", alpha=0.25)
    writer.save_fig(fig, f"{dataset_key}_barh")
    logs.append(f"[OK] bar_h:{dataset_key} rows={len(d)}")


def plot_target_heatmaps(dataset_key: str, dataset: dict, writer: OutputWriter, logs: list[str]) -> None:
    rows = dataset.get("rows") or []
    if not rows:
        logs.append(f"[SKIP] table:{dataset_key}")
        return
    df = pd.DataFrame(rows)
    required = {"age", "gender", "ctr", "impressions"}
    if not required.issubset(df.columns):
        logs.append(f"[SKIP] table:{dataset_key} missing_cols")
        return

    pvt_ctr = df.pivot_table(index="gender", columns="age", values="ctr", aggfunc="mean")
    pvt_imp = df.pivot_table(index="gender", columns="age", values="impressions", aggfunc="mean")

    if not pvt_ctr.empty:
        pvt_imp_for_ctr = pvt_imp.reindex(index=pvt_ctr.index, columns=pvt_ctr.columns)
        annot_ctr = np.empty(pvt_ctr.shape, dtype=object)
        for i in range(pvt_ctr.shape[0]):
            for j in range(pvt_ctr.shape[1]):
                ctr_val = pvt_ctr.iat[i, j]
                imp_val = pvt_imp_for_ctr.iat[i, j] if not pvt_imp_for_ctr.empty else np.nan
                if pd.notna(ctr_val) and pd.notna(imp_val):
                    annot_ctr[i, j] = f"{float(ctr_val):.2f}\n({int(round(float(imp_val))):,})"
                elif pd.notna(ctr_val):
                    annot_ctr[i, j] = f"{float(ctr_val):.2f}"
                else:
                    annot_ctr[i, j] = ""

        fig, ax = plt.subplots(figsize=FIGSIZE_HEATMAP)
        hm = sns.heatmap(
            pvt_ctr,
            annot=annot_ctr,
            fmt="",
            cmap=SEA,
            linewidths=0,
            linecolor=None,
            cbar=True,
            ax=ax,
        )
        mesh = hm.collections[0]
        mesh.set_linewidth(0.0)
        mesh.set_edgecolor("face")
        mesh.set_antialiased(False)
        facecolors = mesh.get_facecolors()
        for text_obj, rgba in zip(hm.texts, facecolors):
            text_obj.set_color(_contrast_text_color(rgba, threshold=0.45))
        writer.save_fig(fig, f"{dataset_key}_ctr_heatmap")

    if not pvt_imp.empty:
        annot_imp = np.empty(pvt_imp.shape, dtype=object)
        for i in range(pvt_imp.shape[0]):
            for j in range(pvt_imp.shape[1]):
                imp_val = pvt_imp.iat[i, j]
                annot_imp[i, j] = f"{int(round(float(imp_val))):,}" if pd.notna(imp_val) else ""

        fig, ax = plt.subplots(figsize=FIGSIZE_HEATMAP)
        hm = sns.heatmap(
            pvt_imp,
            annot=annot_imp,
            fmt="",
            cmap=SEA,
            linewidths=0,
            linecolor=None,
            cbar=True,
            ax=ax,
        )
        mesh = hm.collections[0]
        mesh.set_linewidth(0.0)
        mesh.set_edgecolor("face")
        mesh.set_antialiased(False)
        facecolors = mesh.get_facecolors()
        for text_obj, rgba in zip(hm.texts, facecolors):
            text_obj.set_color(_contrast_text_color(rgba, threshold=0.45))
        writer.save_fig(fig, f"{dataset_key}_impressions_heatmap")
    logs.append(f"[OK] table:{dataset_key} rows={len(df)}")


def _draw_circlepack(
    group_df: pd.DataFrame,
    title: str,
    writer: OutputWriter,
    palette: list[str],
) -> None:
    children = []
    id_to_label: dict[str, str] = {}
    id_to_ctr: dict[str, float] = {}
    for i, (_, row) in enumerate(group_df.iterrows()):
        label = str(row.get("var_keyword", "unknown"))
        value = float(row.get("var_imps", 0.0) or 0.0)
        if value <= 0:
            value = 1.0
        circle_id = f"{label}__{i}"
        children.append({"id": circle_id, "datum": value})
        id_to_label[circle_id] = label
        id_to_ctr[circle_id] = float(row.get("with_var_ctr", 0.0) or 0.0)
    if not children:
        return

    ctr_vals = pd.to_numeric(group_df["with_var_ctr"], errors="coerce")
    ctr_vals = ctr_vals.fillna(0.0)
    vmin, vmax = float(ctr_vals.min()), float(ctr_vals.max())
    cmap = mcolors.LinearSegmentedColormap.from_list("combo_cmap", palette, N=256).reversed()
    norm = mcolors.PowerNorm(gamma=0.8, vmin=vmin, vmax=vmax) if abs(vmax - vmin) > 1e-12 else None

    if circlify is not None:
        circles = circlify.circlify(children, show_enclosure=False, target_enclosure=circlify.Circle(0, 0, 1))
        fig, ax = plt.subplots(figsize=FIGSIZE_CIRCLEPACK, dpi=DPI_CIRCLEPACK)
        ax.set_aspect("equal", adjustable="box")
        ax.axis("off")

        circles_with_meta: list[tuple[circlify.Circle, str]] = []
        for c in circles:
            ex = getattr(c, "ex", None)
            if not ex or "id" not in ex:
                continue
            cid = str(ex["id"])
            if cid not in id_to_label:
                continue
            circles_with_meta.append((c, cid))

        for c, cid in sorted(circles_with_meta, key=lambda x: x[0].r, reverse=True):
            x, y, r = c.x, c.y, c.r
            ctr = id_to_ctr[cid]
            norm_v = float(norm(ctr)) if norm else 0.5
            color = cmap(norm_v)
            patch = plt.Circle(
                (x, y),
                r,
                facecolor=color,
                edgecolor="none",
                linewidth=0.0,
                alpha=1.0,
                antialiased=False,
            )
            ax.add_patch(patch)
            if r >= 0.08:
                txt_color = "white" if norm_v > 0.4 else "#1a1a1a"
                ax.text(
                    x,
                    y,
                    f"{id_to_label[cid]}({ctr:.2f})",
                    ha="center",
                    va="center",
                    fontsize=max(7, min(16, r * 40)),
                    color=txt_color,
                )
        ax.set_xlim(-1.05, 1.05)
        ax.set_ylim(-1.05, 1.05)
        writer.save_fig(fig, title)
        return

    fig, ax = plt.subplots(figsize=FIGSIZE_CIRCLEPACK, dpi=DPI_CIRCLEPACK)
    x = np.arange(len(group_df))
    y = np.zeros(len(group_df))
    sizes = pd.to_numeric(group_df["var_imps"], errors="coerce").fillna(1.0).to_numpy()
    ctr = pd.to_numeric(group_df["with_var_ctr"], errors="coerce").fillna(0.0).to_numpy()
    colors = [cmap(norm(v)) if norm else cmap(0.5) for v in ctr]
    ax.scatter(
        x,
        y,
        s=np.clip(sizes, 100, 5000),
        c=colors,
        alpha=1.0,
        edgecolors="none",
        linewidths=0.0,
    )
    labels = group_df["var_keyword"].astype(str).tolist()
    for i, label in enumerate(labels):
        ax.text(x[i], y[i], f"{label}({float(ctr[i]):.2f})", ha="center", va="center", fontsize=8)
    ax.set_xticks([])
    ax.set_yticks([])
    writer.save_fig(fig, title)


def _append_dataset_details(logs: list[str], dataset_key: str, ds: dict) -> None:
    kind = ds.get("kind")
    logs.append("")
    logs.append(f"[DATASET:{dataset_key}]")
    logs.append(f"kind={kind}")
    if ds.get("title"):
        logs.append(f"title={ds.get('title')}")
    if ds.get("unit"):
        logs.append(f"unit={ds.get('unit')}")

    if kind in {"line", "bar_h", "bar_v", "bubble"}:
        labels = ds.get("labels") or []
        series = ds.get("series") or []
        logs.append(f"labels_count={len(labels)}")
        logs.append(f"series_count={len(series)}")
        for i, label in enumerate(labels):
            parts = [f"label={label}"]
            for s in series:
                s_name = s.get("name", "value")
                data = s.get("data") or []
                value = data[i] if i < len(data) else None
                parts.append(f"{s_name}={value}")
            logs.append(" | ".join(parts))
        return

    if kind == "table":
        rows = ds.get("rows") or []
        logs.append(f"rows_count={len(rows)}")
        for idx, row in enumerate(rows, start=1):
            if isinstance(row, dict):
                row_text = ", ".join(f"{k}={row.get(k)}" for k in row.keys())
            else:
                row_text = str(row)
            logs.append(f"{idx}. {row_text}")
        return

    if kind == "content_card":
        items = ds.get("items") or []
        logs.append(f"items_count={len(items)}")
        for idx, item in enumerate(items, start=1):
            logs.append(
                "item_{}: ad_id={} ad_name={} ctr={} impressions={} clicks={}".format(
                    idx,
                    item.get("ad_id"),
                    item.get("ad_name"),
                    item.get("ctr"),
                    item.get("impressions"),
                    item.get("clicks"),
                )
            )
            details = item.get("target_details") or []
            logs.append(f"item_{idx}_target_details_count={len(details)}")
            for j, row in enumerate(details, start=1):
                if isinstance(row, dict):
                    row_text = ", ".join(f"{k}={row.get(k)}" for k in row.keys())
                else:
                    row_text = str(row)
                logs.append(f"item_{idx}_target_{j}. {row_text}")
        return

    logs.append(f"raw={ds}")


def plot_keyword_combo(dataset_key: str, dataset: dict, writer: OutputWriter, logs: list[str]) -> None:
    rows = dataset.get("rows") or []
    if not rows:
        logs.append(f"[SKIP] table:{dataset_key}")
        return
    df = pd.DataFrame(rows)
    required = {"ess_1", "ess_2", "combo_overall_ctr", "var_keyword", "with_var_ctr"}
    if not required.issubset(df.columns):
        logs.append(f"[SKIP] table:{dataset_key} missing_cols")
        return

    rendered = 0
    df = df.copy()
    df["combo_overall_ctr_num"] = pd.to_numeric(df["combo_overall_ctr"], errors="coerce")
    df["with_var_ctr"] = pd.to_numeric(df["with_var_ctr"], errors="coerce")

    combo_keys = ["ess_1", "ess_2", "combo_overall_ctr_num"]
    combo_sizes = df.groupby(combo_keys, dropna=False).size().reset_index(name="item_count")
    combo_rank = (
        combo_sizes[combo_sizes["item_count"] >= 2]
        .dropna(subset=["combo_overall_ctr_num"])
        .sort_values("combo_overall_ctr_num", ascending=False)
        .head(6)
        .reset_index(drop=True)
    )

    for idx, row in enumerate(combo_rank.itertuples(index=False), start=1):
        e1 = getattr(row, "ess_1")
        e2 = getattr(row, "ess_2")
        combo_ctr = float(getattr(row, "combo_overall_ctr_num"))
        gdf = df[
            (df["ess_1"] == e1)
            & (df["ess_2"] == e2)
            & (df["combo_overall_ctr_num"] == combo_ctr)
        ].copy()
        gdf = gdf.sort_values("with_var_ctr", ascending=False).head(8)
        if len(gdf) < 2:
            continue
        title = f"{dataset_key}_{idx}_{e1}+{e2}_{combo_ctr:.2f}"
        is_avoid_combo = dataset_key.startswith("avoid_keyword_combo")
        palette = b_cmap if is_avoid_combo else g_cmap
        _draw_circlepack(gdf, title, writer, palette)
        rendered += 1
    logs.append(f"[OK] table:{dataset_key} groups_rendered={rendered}")


def plot_content_cards(dataset_key: str, dataset: dict, writer: OutputWriter, logs: list[str]) -> None:
    items = dataset.get("items") or []
    if not items:
        logs.append(f"[SKIP] content_card:{dataset_key}")
        return

    palette = G_CMAP if "top" in dataset_key else B_CMAP
    cmap = mcolors.LinearSegmentedColormap.from_list(f"{dataset_key}_cmap", palette, N=256).reversed()

    rendered = 0
    for idx, item in enumerate(items, start=1):
        target_details = item.get("target_details") or []
        if not target_details:
            continue
        df = pd.DataFrame(target_details)
        if not {"age", "gender", "ctr"}.issubset(df.columns):
            continue
        df["ctr"] = pd.to_numeric(df["ctr"], errors="coerce")
        df = df.dropna(subset=["ctr"]).sort_values("ctr", ascending=False).head(10)
        if df.empty:
            continue
        df["label"] = df["age"].astype(str) + " " + df["gender"].astype(str)
        vmin, vmax = float(df["ctr"].min()), float(df["ctr"].max())
        if abs(vmax - vmin) < 1e-12:
            colors = [cmap(0.5)] * len(df)
        else:
            norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
            colors = [cmap(norm(v)) for v in df["ctr"]]

        fig, ax = plt.subplots(figsize=FIGSIZE_BARV)
        ax.bar(df["label"], df["ctr"], color=colors)
        ax.set_ylabel("CTR (%)")
        ax.set_xlabel("")
        ax.grid(axis="y", alpha=0.25)
        ax.tick_params(axis="x", labelrotation=30)
        writer.save_fig(fig, f"{dataset_key}_{idx}_ad_{item.get('ad_id')}")
        rendered += 1
    logs.append(f"[OK] content_card:{dataset_key} rendered={rendered}")


def main() -> int:
    args = parse_args()
    json_path = Path(args.json_path).resolve()
    output_dir = Path(args.output_dir).resolve()
    writer = OutputWriter(output_dir)
    logs: list[str] = []

    if not json_path.exists():
        raise FileNotFoundError(f"JSON not found: {json_path}")

    # Read with json module because top-level is nested dict, not tabular.
    import json

    with open(json_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    meta = report.get("meta", {})
    summary = report.get("summary", {})
    datasets = report.get("datasets", {})

    logs.append(f"generated_at={dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logs.append(f"json_path={json_path}")
    logs.append(f"dataset_count={len(datasets)}")
    logs.append("")
    logs.append("[META]")
    for k, v in meta.items():
        logs.append(f"{k}={v}")
    logs.append("")
    logs.append("[SUMMARY]")
    for k, v in summary.items():
        logs.append(f"{k}={v}")

    for key, ds in datasets.items():
        kind = ds.get("kind")
        if kind == "line":
            plot_line(key, ds, writer, logs)
        elif kind == "bar_h":
            plot_bar_h(key, ds, writer, logs)
        elif kind == "table" and key == "target_heatmap":
            plot_target_heatmaps(key, ds, writer, logs)
        elif kind == "table" and key.endswith("keyword_combo_detail"):
            plot_keyword_combo(key, ds, writer, logs)
        elif kind == "content_card":
            plot_content_cards(key, ds, writer, logs)
        else:
            logs.append(f"[SKIP] {key}: unsupported kind={kind}")

    logs.append("")
    logs.append("[DATA_DETAILS]")
    for key, ds in datasets.items():
        _append_dataset_details(logs, key, ds)

    logs.append("")
    logs.append("[CHART_FILES]")
    for path in writer.files:
        logs.append(str(path))
    logs.append(f"chart_count={len(writer.files)}")

    target_rows = (datasets.get("target_heatmap") or {}).get("rows") or []
    overall_ctr = _avg_ctr_from_trend(datasets.get("ctr_trend"))
    main_ctr = _target_ctr(target_rows, args.main_age, args.main_gender)
    avoid_ctr = _target_ctr(target_rows, args.avoid_age, args.avoid_gender)
    logs.append("")
    logs.append(f"overall_ctr={_format_ctr(overall_ctr)}")
    logs.append(f"main_ctr={_format_ctr(main_ctr)}")
    logs.append(f"avoid_ctr={_format_ctr(avoid_ctr)}")

    result_path = output_dir / "result.txt"
    result_path.write_text("\n".join(logs), encoding="utf-8")

    print(f"Saved summary: {result_path}")
    print(f"Saved charts: {len(writer.files)} file(s) in {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
