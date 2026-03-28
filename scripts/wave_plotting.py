from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd

try:
    import seaborn as sns
except ImportError:  # pragma: no cover - preview only degrades gracefully
    sns = None


REQUIRED_OHLC_COLUMNS = {"trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq"}
DEFAULT_FONT_FALLBACK = [
    "PingFang SC",
    "Hiragino Sans GB",
    "Heiti SC",
    "Arial Unicode MS",
    "DejaVu Sans",
]


def plot_candlestick_waves(
    df: pd.DataFrame,
    waves: list[dict[str, Any]],
    output_path: str | Path,
    title: str,
    style: str = "classic",
) -> dict[str, Any]:
    missing = REQUIRED_OHLC_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"missing required columns: {sorted(missing)}")

    ordered = df.copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"])
    ordered = ordered.sort_values("trade_date").reset_index(drop=True)

    plt.rcParams["font.sans-serif"] = DEFAULT_FONT_FALLBACK
    plt.rcParams["axes.unicode_minus"] = False
    fig, ax = _build_figure(style=style, title=title)

    x_values = mdates.date2num(ordered["trade_date"].tolist())
    candle_width = 0.68 if style == "enhanced" else 0.6

    for x, row in zip(x_values, ordered.itertuples(index=False), strict=True):
        color = "#e11d48" if row.close_qfq >= row.open_qfq else "#0891b2"
        edgecolor = "#9f1239" if row.close_qfq >= row.open_qfq else "#155e75"
        wick_width = 1.0 if style == "enhanced" else 1.2
        ax.plot([x, x], [float(row.low_qfq), float(row.high_qfq)], color=edgecolor, linewidth=wick_width, alpha=0.85, zorder=2)
        body_low = min(float(row.open_qfq), float(row.close_qfq))
        body_height = max(abs(float(row.close_qfq) - float(row.open_qfq)), 0.001)
        rect = Rectangle(
            (x - candle_width / 2, body_low),
            candle_width,
            body_height,
            facecolor=color,
            edgecolor=edgecolor,
            linewidth=0.8,
            alpha=0.92 if style == "enhanced" else 1.0,
            zorder=3,
        )
        ax.add_patch(rect)

    waves_annotated = 0
    for idx, wave in enumerate(waves, start=1):
        start_date = pd.to_datetime(wave["start_date"])
        peak_date = pd.to_datetime(wave["peak_date"])
        mask = (ordered["trade_date"] >= start_date) & (ordered["trade_date"] <= peak_date)
        segment = ordered.loc[mask]
        if segment.empty:
            continue

        if style == "enhanced":
            start_num = mdates.date2num(start_date)
            peak_num = mdates.date2num(peak_date)
            ax.axvspan(start_num - 0.45, peak_num + 0.45, color="#f59e0b", alpha=0.10, zorder=0)
            ax.plot(
                segment["trade_date"],
                segment["close_qfq"],
                color="#b45309",
                linewidth=1.4,
                linestyle="--",
                alpha=0.85,
                zorder=4,
            )
        else:
            ax.plot(
                segment["trade_date"],
                segment["close_qfq"],
                color="#2563eb",
                linewidth=2.2,
                zorder=4,
            )
        ax.annotate(
            (
                f"W{idx}: {start_date.date()} -> {peak_date.date()}\n"
                f"{float(wave['start_price']):.2f} -> {float(wave['peak_price']):.2f} "
                f"({float(wave['wave_gain_pct']):.1f}%)"
            ),
            xy=(peak_date, float(wave["peak_price"])),
            xytext=(8, 12 if style == "enhanced" else 10),
            textcoords="offset points",
            fontsize=9,
            color="#7c2d12" if style == "enhanced" else "#b91c1c",
            bbox={
                "boxstyle": "round,pad=0.28",
                "fc": "#fffbeb" if style == "enhanced" else "mistyrose",
                "ec": "#f59e0b" if style == "enhanced" else "salmon",
                "alpha": 0.95,
            },
            arrowprops=None if style != "enhanced" else {"arrowstyle": "-", "color": "#b45309", "lw": 0.9, "alpha": 0.8},
        )
        waves_annotated += 1

    if style == "enhanced":
        ax.set_title(title, loc="left", fontsize=16, fontweight="bold", color="#111827")
        ax.text(
            0.01,
            0.98,
            "增强预览版: 用暖色波段背景替代粗蓝线遮挡",
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=10,
            color="#6b7280",
        )
        ax.set_facecolor("#fcfcfd")
        ax.grid(axis="y", color="#d1d5db", alpha=0.35, linewidth=0.8)
        ax.grid(axis="x", visible=False)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    else:
        ax.set_title(title)
        ax.grid(alpha=0.2)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()
    plt.tight_layout()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)

    return {
        "output_path": str(output),
        "candles_plotted": len(ordered),
        "waves_annotated": waves_annotated,
        "style": style,
    }


def _build_figure(style: str, title: str) -> tuple[Any, Any]:
    if style == "enhanced":
        if sns is not None:
            sns.set_theme(style="whitegrid", context="talk")
        else:
            plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(15, 7.5))
        return fig, ax

    fig, ax = plt.subplots(figsize=(14, 6))
    return fig, ax
