from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from html import escape
from pathlib import Path
from typing import Any, Callable

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - lightweight CLI commands should still work
    pd = None


SKILL_ROOT = Path(__file__).resolve().parents[1]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))


from runtime.config import DEFAULT_CONFIG_PATH, load_skill_config


DEFAULT_CONFIG = load_skill_config()
CHATGPT_BROWSER_SCRIPT = Path(DEFAULT_CONFIG["chatgpt"]["script_path"])
REPORT_CONTRACT_PATH = SKILL_ROOT / "templates" / "detailed_report_contract.md"
DEFAULT_ANALYSIS_DIR = Path(DEFAULT_CONFIG["paths"]["analysis_dir"])
DEFAULT_PLOT_DIR = Path(DEFAULT_CONFIG["paths"]["plot_dir"])
LOCAL_CALL_CHAIN = [
    "runtime/wave_segmentation.py",
    "runtime/wave_plotting.py",
    "runtime/attribution_data.py",
]
CHATGPT_CALL_CHAIN = ["skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs"]
CALL_CHAIN = LOCAL_CALL_CHAIN + CHATGPT_CALL_CHAIN


def _require_pandas():
    if pd is None:
        raise ModuleNotFoundError("pandas is required for wave attribution orchestration")


def _default_plotter(*args, **kwargs):
    from runtime.wave_plotting import plot_candlestick_waves

    return plot_candlestick_waves(*args, **kwargs)


def _build_validation_table(*args, **kwargs):
    from runtime.attribution_data import build_validation_table

    return build_validation_table(*args, **kwargs)


def _fetch_stock_window_bundle(*args, **kwargs):
    from runtime.attribution_data import fetch_stock_window_bundle

    return fetch_stock_window_bundle(*args, **kwargs)


def _fetch_stock_concept_frames(*args, **kwargs):
    from runtime.attribution_data import fetch_stock_concept_frames

    return fetch_stock_concept_frames(*args, **kwargs)


def _fetch_news_evidence(*args, **kwargs):
    from runtime.attribution_data import fetch_news_evidence

    return fetch_news_evidence(*args, **kwargs)


def _segment_ma_trend_waves(*args, **kwargs):
    from runtime.wave_segmentation import segment_ma_trend_waves

    return segment_ma_trend_waves(*args, **kwargs)


def _segment_price_waves(*args, **kwargs):
    from runtime.wave_segmentation import segment_price_waves

    return segment_price_waves(*args, **kwargs)


def build_chatgpt_browser_command(
    prompt: str,
    mode: str = "plain",
    node_bin: str | None = None,
    script_path: str | Path | None = None,
) -> list[str]:
    command = "send" if mode == "plain" else "submit-search"
    runtime = load_skill_config()
    resolved_node_bin = node_bin or runtime["chatgpt"]["node_bin"]
    resolved_script = Path(script_path) if script_path else Path(runtime["chatgpt"]["script_path"])
    return [resolved_node_bin, str(resolved_script), command, prompt]


def run_chatgpt_browser(
    prompt: str,
    mode: str = "plain",
    runner: Callable[..., Any] = subprocess.run,
    node_bin: str | None = None,
    script_path: str | Path | None = None,
) -> str:
    command = build_chatgpt_browser_command(prompt, mode=mode, node_bin=node_bin, script_path=script_path)
    result = runner(command, check=True, capture_output=True, text=True, cwd=str(SKILL_ROOT))
    if mode == "plain":
        return result.stdout.strip()

    task = json.loads(result.stdout or "{}")
    task_id = task.get("id")
    if not task_id:
        raise ValueError("submit-search did not return task id")
    wait_command = command[:2] + ["wait", task_id]
    waited = runner(wait_command, check=True, capture_output=True, text=True, cwd=str(SKILL_ROOT))
    return waited.stdout.strip()


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join(["---"] * len(headers)) + "|"
    body = ["| " + " | ".join("" if value is None else str(value) for value in row) + " |" for row in rows]
    return "\n".join([header_line, separator, *body])


def _render_news_section(rows: list[dict[str, Any]]) -> str:
    metadata_rows: list[list[str]] = []
    raw_blocks: list[str] = ["### 证据原文", ""]
    for index, row in enumerate(rows, start=1):
        published_at = str(row.get("published_at", ""))
        source_id = str(row.get("source_id", ""))
        title = str(row.get("title", ""))
        raw_text = str(row.get("raw_text", ""))
        url = str(row.get("url", ""))
        metadata_rows.append([str(index), published_at, f"`{source_id}`", title, f"[link]({url})"])
        raw_blocks.extend(
            [
                f"#### 证据 {index}",
                f"- 时间：`{published_at}`",
                f"- 来源：`{source_id}`",
                f"- 标题：{title}",
                f"- 链接：[link]({url})",
                "- 原文：",
                "```text",
                raw_text,
                "```",
                "",
            ]
        )
    metadata_table = _table(["序号", "时间", "来源", "标题", "链接"], metadata_rows)
    return metadata_table + "\n\n" + "\n".join(raw_blocks).rstrip()


def _format_percent(value: float | int | str) -> str:
    if isinstance(value, str):
        return value
    return f"{float(value):.2f}%"


def build_wave_attribution_search_prompt(
    stock_name: str,
    ts_code: str,
    wave_start: str,
    wave_end: str,
    wave_gain_pct: float | int | str,
    sample_label: str,
    candidate_mainline: str,
    cross_themes: list[str] | tuple[str, ...] | None = None,
) -> str:
    cross_themes = [str(theme).strip() for theme in (cross_themes or []) if str(theme).strip()]
    cross_theme_text = " / ".join(cross_themes) if cross_themes else "其他交叉题材"
    return (
        "请只分析 A 股个股波段归因，不要写过程话术。\n\n"
        f"标的：{stock_name}（{ts_code}）\n"
        f"波段：{wave_start} 到 {wave_end}\n"
        f"波段涨幅：{_format_percent(wave_gain_pct)}\n"
        f"样本标签：{sample_label}\n\n"
        "请联网后只输出以下结构：\n"
        "主因：\n"
        "备选：\n"
        "搜索依据：\n"
        "时间线：\n"
        "结论说明：\n\n"
        "要求：\n"
        "1. 只讨论这个波段时间窗内能解释主升的催化。\n"
        f"2. 明确判断真实主线是否是{candidate_mainline}，而不是{sample_label}。\n"
        f"3. 如果存在 {cross_theme_text} 等交叉题材，放到备选，不要混成主因。\n"
        "4. 输出要短、结论化、可直接粘到研究报告。\n"
    )


def _review_decision(review_text: str) -> str:
    lowered = str(review_text).lower()
    for candidate in ("up_valid", "down_valid", "noise", "merge_adjacent"):
        if candidate in lowered:
            return candidate
    return "manual_review"


def _extract_label(text: str, label: str) -> str:
    for line in str(text).splitlines():
        if line.startswith(f"{label}：") or line.startswith(f"{label}:"):
            return line.split("：", 1)[-1].split(":", 1)[-1].strip()
    return ""


def _resolve_output_paths(
    output_root: Path | None,
    analysis_dir: Path | None = None,
    plot_dir: Path | None = None,
) -> tuple[Path, Path]:
    if analysis_dir is not None and plot_dir is not None:
        return analysis_dir, plot_dir
    if output_root is None:
        return DEFAULT_ANALYSIS_DIR, DEFAULT_PLOT_DIR
    return output_root / "docs" / "analysis", output_root / "data" / "plots"


def _build_news_keywords(
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    concept_labels = concept_labels or {}
    raw_terms = [stock_name, sample_label, *[item.get("name", "") for item in concept_labels.values()]]
    variants: list[str] = []
    for term in raw_terms:
        text = str(term).strip()
        if not text:
            continue
        variants.append(text)
        for suffix in ("概念", "概念股", "主题", "主线"):
            if text.endswith(suffix):
                trimmed = text[: -len(suffix)].strip()
                if trimmed:
                    variants.append(trimmed)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in variants:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _chatgpt_enabled(use_chatgpt: bool | None = None) -> bool:
    if use_chatgpt is not None:
        return use_chatgpt
    return bool(DEFAULT_CONFIG.get("chatgpt", {}).get("enabled", False))


DEFAULT_TIMELINE_NEWS_LIMIT = 10
DEFAULT_EVIDENCE_NEWS_LIMIT = 6
SOURCE_PRIORITY = {
    "zsxq_saidao_touyan": 4,
    "zsxq_damao": 3,
    "zsxq_zhuwang": 3,
    "wscn_live": 1,
}


def _summarize_timeline_impact(raw_text: str) -> str:
    text = str(raw_text or "")
    for line in text.splitlines():
        summary = line.strip()
        if summary:
            return summary
    return ""


def _normalize_news_key(row: dict[str, Any]) -> tuple[str, str]:
    title = " ".join(str(row.get("title", "")).split())
    summary = _summarize_timeline_impact(str(row.get("raw_text", "")))
    return title, summary


def _collect_news_terms(
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    return _build_news_keywords(stock_name, sample_label, concept_labels)


def _anchor_dates_from_waves(waves: list[dict[str, Any]]) -> list[pd.Timestamp]:
    anchors: list[pd.Timestamp] = []
    for wave in waves:
        start_date = wave.get("start_date")
        if start_date:
            anchors.append(_to_naive_timestamp(start_date))
    return anchors


def _to_naive_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_localize(None)
    return timestamp


def _news_distance_score(published_at: pd.Timestamp, anchors: list[pd.Timestamp]) -> tuple[int, int]:
    if not anchors:
        return 0, 9999
    days = min(abs((published_at.normalize() - anchor.normalize()).days) for anchor in anchors)
    if days <= 1:
        return 40, days
    if days <= 3:
        return 28, days
    if days <= 7:
        return 18, days
    if days <= 14:
        return 10, days
    if days <= 30:
        return 4, days
    return 0, days


def _score_news_row(
    row: dict[str, Any],
    *,
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    anchors: list[pd.Timestamp],
) -> tuple[int, int, pd.Timestamp]:
    title = str(row.get("title", ""))
    raw_text = str(row.get("raw_text", ""))
    published_at = _to_naive_timestamp(row.get("published_at"))
    source_id = str(row.get("source_id", ""))
    terms = _collect_news_terms(stock_name, sample_label, concept_labels)

    score = SOURCE_PRIORITY.get(source_id, 0)
    if stock_name and stock_name in title:
        score += 80
    elif stock_name and stock_name in raw_text:
        score += 40

    title_hits = sum(1 for term in terms if term and term != stock_name and term in title)
    body_hits = sum(1 for term in terms if term and term != stock_name and term in raw_text)
    score += title_hits * 12
    score += body_hits * 4

    distance_score, distance_days = _news_distance_score(published_at, anchors)
    score += distance_score
    return score, distance_days, published_at


def _select_news_evidence(
    *,
    news_evidence: list[dict[str, Any]],
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    waves: list[dict[str, Any]],
    top_k: int = DEFAULT_EVIDENCE_NEWS_LIMIT,
) -> list[dict[str, Any]]:
    anchors = _anchor_dates_from_waves(waves)
    best_by_key: dict[tuple[str, str], tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]] = {}
    for row in news_evidence:
        score_tuple = _score_news_row(
            row,
            stock_name=stock_name,
            sample_label=sample_label,
            concept_labels=concept_labels,
            anchors=anchors,
        )
        key = _normalize_news_key(row)
        current = best_by_key.get(key)
        score_key = (-score_tuple[0], score_tuple[1], score_tuple[2])
        current_key = None if current is None else (-current[0][0], current[0][1], current[0][2])
        if current is None or score_key < current_key:
            best_by_key[key] = (score_tuple, row)

    ranked = sorted(
        (item for item in best_by_key.values()),
        key=lambda item: (-item[0][0], item[0][1], item[0][2]),
    )
    return [row for _, row in ranked[:top_k]]


def _build_timeline_rows(news_evidence: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    ordered = sorted(news_evidence, key=lambda row: _to_naive_timestamp(row.get("published_at")))
    for row in ordered[:DEFAULT_TIMELINE_NEWS_LIMIT]:
        rows.append(
            {
                "time": str(row.get("published_at", "")),
                "category": "本地证据",
                "event": str(row.get("title", "")),
                "impact": _summarize_timeline_impact(str(row.get("raw_text", ""))),
                "source": str(row.get("source_id", "")),
            }
        )
    return rows


def _build_quant_rows(stock_bundle: dict[str, pd.DataFrame]) -> list[dict[str, str]]:
    _require_pandas()
    stock_df = stock_bundle["raw_stock_daily_qfq"].copy()
    first_close = float(stock_df.iloc[0]["close_qfq"])
    last_close = float(stock_df.iloc[-1]["close_qfq"])
    rows = [
        {
            "metric": "区间涨幅",
            "value": _format_percent((last_close / first_close - 1.0) * 100.0),
            "evidence": f"close_qfq {first_close:.2f} -> {last_close:.2f}",
            "interpretation": "观察个股在分析窗口内的总涨幅",
        }
    ]
    daily_basic = stock_bundle.get("raw_daily_basic", pd.DataFrame())
    if not daily_basic.empty and "turnover_rate" in daily_basic.columns:
        rows.append(
            {
                "metric": "平均换手率",
                "value": f"{float(daily_basic['turnover_rate'].astype(float).mean()):.2f}",
                "evidence": "raw_daily_basic.turnover_rate",
                "interpretation": "衡量换手活跃程度",
            }
        )
    moneyflow = stock_bundle.get("raw_moneyflow", pd.DataFrame())
    if not moneyflow.empty and "net_mf_amount" in moneyflow.columns:
        rows.append(
            {
                "metric": "净流入均值",
                "value": f"{float(moneyflow['net_mf_amount'].astype(float).mean()):.2f}",
                "evidence": "raw_moneyflow.net_mf_amount",
                "interpretation": "观察主力资金流向",
            }
        )
    limit_df = stock_bundle.get("raw_limit_list_d", pd.DataFrame())
    if not limit_df.empty:
        rows.append(
            {
                "metric": "涨停记录数",
                "value": str(len(limit_df)),
                "evidence": "raw_limit_list_d",
                "interpretation": "观察极端情绪与加速特征",
            }
        )
    return rows


def _build_concept_rows(
    stock_bundle: dict[str, pd.DataFrame],
    concept_frames: dict[str, pd.DataFrame],
    concept_labels: dict[str, dict[str, str]] | None,
) -> list[dict[str, str]]:
    _require_pandas()
    table = _build_validation_table(
        stock_bundle["raw_stock_daily_qfq"],
        concept_frames,
        labels=concept_labels or {},
        top_n=5,
    )
    rows = []
    for row in table.to_dict("records"):
        rows.append(
            {
                "concept_name": row["name"],
                "concept_code": row["code"],
                "period_return_pct": _format_percent(row["period_return_pct"]),
                "close_corr": f"{float(row['close_corr']):.4f}",
                "ret_corr": f"{float(row['ret_corr']):.4f}",
                "interpretation": "相关系数越高，联动越强",
            }
        )
    return rows


def render_detailed_markdown(payload: dict[str, Any]) -> str:
    timeline_table = _table(
        ["时间", "事件类别", "事件", "对波段影响", "来源"],
        [[row["time"], row["category"], row["event"], row["impact"], row["source"]] for row in payload["timeline_rows"]],
    )
    wave_table = _table(
        ["波段", "区间", "涨幅", "波段审查", "主因", "备选"],
        [[row["wave_id"], row["period"], row["gain_pct"], row["review"], row["main_cause"], row["alt_cause"]] for row in payload["wave_rows"]],
    )
    news_table = _render_news_section(payload["news_rows"])
    quant_table = _table(
        ["维度", "数值", "证据", "解释"],
        [[row["metric"], row["value"], row["evidence"], row["interpretation"]] for row in payload["quant_rows"]],
    )
    concept_table = _table(
        ["概念", "代码", "区间涨幅", "收盘价相关系数", "日收益率相关系数", "解释"],
        [
            [
                row["concept_name"],
                row["concept_code"],
                row["period_return_pct"],
                row["close_corr"],
                row["ret_corr"],
                row["interpretation"],
            ]
            for row in payload["concept_rows"]
        ],
    )
    conclusion_table = _table(
        ["维度", "结论", "置信度", "说明"],
        [[row["dimension"], row["value"], row["confidence"], row["notes"]] for row in payload["conclusion_rows"]],
    )

    return f"""# {payload['stock_name']}波段归因

## 基础信息

- 标的名称：{payload['stock_name']}
- 股票代码：`{payload['ts_code']}`
- 分析窗口：`{payload['start_date']}` 到 `{payload['end_date']}`

波段图：

![]({payload['plot_relpath']})

## 事件时间线表

{timeline_table}

## 波段分段归因表

{wave_table}

## 本地 news 证据表

{news_table}

## 量价验证表

{quant_table}

## 概念联动验证表

{concept_table}

## 结论与置信度表

{conclusion_table}
"""


def run_stock_wave_attribution(
    case_context: dict[str, str],
    stock_bundle: dict[str, pd.DataFrame],
    news_evidence: list[dict[str, Any]],
    concept_frames: dict[str, pd.DataFrame],
    concept_labels: dict[str, dict[str, str]] | None = None,
    output_root: Path | None = None,
    analysis_dir: Path | None = None,
    plot_dir: Path | None = None,
    segmenter: Callable[[pd.DataFrame], list[dict[str, Any]]] | None = None,
    plotter: Callable[..., dict[str, Any]] = _default_plotter,
    chatgpt_runner: Callable[..., str] = run_chatgpt_browser,
    use_chatgpt: bool | None = None,
) -> dict[str, Any]:
    _require_pandas()
    report_dir, plot_dir = _resolve_output_paths(output_root, analysis_dir=analysis_dir, plot_dir=plot_dir)
    stock_df = stock_bundle["raw_stock_daily_qfq"].copy()
    stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
    stock_df = stock_df.sort_values("trade_date").reset_index(drop=True)

    waves = (segmenter or _segment_ma_trend_waves)(stock_df)
    if not waves:
        waves = _segment_price_waves(stock_df, min_wave_gain=0.2, min_pullback=0.05, min_bars=3)
    if not waves:
        raise ValueError("no candidate waves generated")

    plot_path = plot_dir / f"{case_context['ts_code'].replace('.', '_')}_orchestrator.png"
    plot_meta = plotter(
        df=stock_df,
        waves=waves,
        output_path=plot_path,
        title=f"{case_context['stock_name']} 波段图",
        style="enhanced",
    )

    wave_rows: list[dict[str, str]] = []
    conclusion_rows: list[dict[str, str]] = []
    chatgpt_enabled = _chatgpt_enabled(use_chatgpt=use_chatgpt)
    for idx, wave in enumerate(waves, start=1):
        review = "rule_based"
        attribution_text = ""
        if chatgpt_enabled:
            review_prompt = (
                f"请审查波段 W{idx} 是否属于有效主升段，只返回 up_valid/down_valid/noise/merge_adjacent。\n"
                f"区间：{wave['start_date']} -> {wave['peak_date']}，涨幅：{float(wave['wave_gain_pct']):.2f}%"
            )
            review_text = chatgpt_runner(review_prompt, mode="plain")
            review = _review_decision(review_text)
        if chatgpt_enabled and review in {"up_valid", "down_valid"}:
            attribution_prompt = (
                f"请分析波段 W{idx} 在 {wave['start_date']} 到 {wave['peak_date']} 的主因、备选和搜索依据。"
            )
            attribution_text = chatgpt_runner(attribution_prompt, mode="search")

        wave_rows.append(
            {
                "wave_id": f"W{idx}",
                "period": f"{wave['start_date']} -> {wave['peak_date']}",
                "gain_pct": _format_percent(wave["wave_gain_pct"]),
                "review": review,
                "main_cause": _extract_label(attribution_text, "主因") or ("待结合本地证据裁决" if not chatgpt_enabled else ""),
                "alt_cause": _extract_label(attribution_text, "备选"),
            }
        )
        if attribution_text:
            conclusion_rows.append(
                {
                    "dimension": f"W{idx}",
                    "value": _extract_label(attribution_text, "主因") or "待补充",
                    "confidence": "中高" if review == "up_valid" else "中",
                    "notes": _extract_label(attribution_text, "搜索依据") or "需补本地证据",
                }
            )

    quant_rows = _build_quant_rows(stock_bundle)
    concept_rows = _build_concept_rows(stock_bundle, concept_frames, concept_labels)
    selected_news = _select_news_evidence(
        news_evidence=news_evidence,
        stock_name=case_context["stock_name"],
        sample_label=case_context.get("sample_label", ""),
        concept_labels=concept_labels,
        waves=waves,
        top_k=DEFAULT_EVIDENCE_NEWS_LIMIT,
    )
    timeline_rows = _build_timeline_rows(selected_news)
    if not conclusion_rows:
        conclusion_rows.append(
            {
                "dimension": "综合",
                "value": "待结合本地证据裁决",
                "confidence": "中",
                "notes": "ChatGPT 当前默认关闭，先以本地 news、量价和概念联动验证为准",
            }
        )

    report_payload = {
        "stock_name": case_context["stock_name"],
        "ts_code": case_context["ts_code"],
        "start_date": case_context["start_date"],
        "end_date": case_context["end_date"],
        "plot_relpath": os.path.relpath(plot_meta["output_path"], start=report_dir),
        "timeline_rows": timeline_rows,
        "wave_rows": wave_rows,
        "news_rows": selected_news,
        "quant_rows": quant_rows,
        "concept_rows": concept_rows,
        "conclusion_rows": conclusion_rows,
    }
    markdown = render_detailed_markdown(report_payload)

    report_path = report_dir / (
        f"{pd.Timestamp.now().date()}-{case_context['ts_code'].replace('.', '')}-{case_context['stock_name']}-wave-attribution.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(markdown, encoding="utf-8")

    return {
        "report_path": str(report_path),
        "plot_path": str(plot_meta["output_path"]),
        "report_contract_path": str(REPORT_CONTRACT_PATH),
        "call_chain": LOCAL_CALL_CHAIN + (CHATGPT_CALL_CHAIN if chatgpt_enabled else []),
        "wave_count": len(wave_rows),
    }


def run_local_attribution_task(
    *,
    stock_name: str,
    ts_code: str,
    start_date: str,
    end_date: str,
    sample_label: str,
    config_path: str | Path | None = None,
    db_connect: Callable[..., Any] | None = None,
    stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] = _fetch_stock_window_bundle,
    concept_fetcher: Callable[..., tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]] = _fetch_stock_concept_frames,
    news_fetcher: Callable[..., list[dict[str, Any]]] = _fetch_news_evidence,
    attribution_runner: Callable[..., dict[str, Any]] = run_stock_wave_attribution,
) -> dict[str, Any]:
    runtime = load_skill_config(config_path)
    if db_connect is None:
        import psycopg

        db_connect = psycopg.connect

    with db_connect(runtime["postgres"]["event_quant_dsn"]) as quant_conn:
        stock_bundle = stock_bundle_fetcher(quant_conn, ts_code, start_date, end_date)
        stock_daily = stock_bundle.get("raw_stock_daily_qfq")
        if stock_daily is None or stock_daily.empty:
            raise ValueError(f"未获取到 {ts_code} 在 {start_date} 到 {end_date} 的量价数据")
        concept_frames, concept_labels = concept_fetcher(quant_conn, ts_code, start_date, end_date)

    keywords = _build_news_keywords(stock_name, sample_label, concept_labels)
    with db_connect(runtime["postgres"]["event_news_dsn"]) as news_conn:
        news_evidence = news_fetcher(news_conn, start_date, end_date, keywords)

    return attribution_runner(
        case_context={
            "stock_name": stock_name,
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
            "sample_label": sample_label,
        },
        stock_bundle=stock_bundle,
        news_evidence=news_evidence,
        concept_frames=concept_frames,
        concept_labels=concept_labels,
        analysis_dir=Path(runtime["paths"]["analysis_dir"]),
        plot_dir=Path(runtime["paths"]["plot_dir"]),
        use_chatgpt=bool(runtime.get("chatgpt", {}).get("enabled", False)),
    )


def _build_run_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestrator.py run")
    parser.add_argument("--stock-name", required=True)
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample-label", required=True)
    parser.add_argument("--config", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(
            "Usage:\n"
            "  orchestrator.py deps\n"
            "  orchestrator.py contract-path\n"
            "  orchestrator.py run --stock-name 名称 --ts-code 代码 --start-date YYYY-MM-DD --end-date YYYY-MM-DD --sample-label 标签 [--config 路径]\n"
        )
        return 0
    if argv[0] == "deps":
        print(json.dumps({"call_chain": CALL_CHAIN}, ensure_ascii=False, indent=2))
        return 0
    if argv[0] == "contract-path":
        print(str(REPORT_CONTRACT_PATH))
        return 0
    if argv[0] == "run":
        parser = _build_run_parser()
        args = parser.parse_args(argv[1:])
        result = run_local_attribution_task(
            stock_name=args.stock_name,
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            sample_label=args.sample_label,
            config_path=args.config,
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    raise SystemExit(f"Unknown command: {argv[0]}")


if __name__ == "__main__":
    raise SystemExit(main())
