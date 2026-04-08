from __future__ import annotations

import argparse
import json
import logging
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
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from runtime.config import DEFAULT_CONFIG_PATH, load_skill_config


logger = logging.getLogger(__name__)
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


def _validate_stock_window_coverage(*args, **kwargs):
    from runtime.attribution_data import validate_stock_window_coverage

    return validate_stock_window_coverage(*args, **kwargs)


def _fetch_stock_window_bundle_from_akshare(ts_code: str, start_date: str, end_date: str):
    from scripts.event_quant_sync import fetch_case_stock_bundle_from_akshare

    return fetch_case_stock_bundle_from_akshare(
        ts_code=ts_code,
        start_date=_to_akshare_trade_date(start_date),
        end_date=_to_akshare_trade_date(end_date),
    )


def _persist_quant_bundle_to_db(conn, ts_code: str, frames):
    from scripts.event_quant_sync import persist_frames_to_db

    return persist_frames_to_db(conn, ts_code, frames)


def _fetch_stock_concept_bundle_from_tushare(
    ts_code: str,
    start_date: str,
    end_date: str,
    token: str,
    http_url: str,
):
    from scripts.event_quant_sync import fetch_case_stock_concept_bundle_from_tushare

    return fetch_case_stock_concept_bundle_from_tushare(
        ts_code=ts_code,
        start_date=_to_akshare_trade_date(start_date),
        end_date=_to_akshare_trade_date(end_date),
        token=token,
        http_url=http_url,
    )


def _persist_concept_bundle_to_db(conn, ts_code: str, frames):
    from scripts.event_quant_sync import persist_frames_to_db

    return persist_frames_to_db(conn, ts_code, frames)


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


def _render_news_raw_blocks(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "暂无匹配证据原文。"

    raw_blocks: list[str] = []
    ordered = sorted(rows, key=lambda row: _to_naive_timestamp(row.get("published_at")))
    for index, row in enumerate(ordered, start=1):
        published_at = _format_display_time(row.get("published_at", ""))
        source_id = str(row.get("source_id", ""))
        title = str(row.get("title", ""))
        raw_text = str(row.get("raw_text", ""))
        url = str(row.get("url", ""))
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
    return "\n".join(raw_blocks).rstrip()


def _format_percent(value: float | int | str) -> str:
    if isinstance(value, str):
        return value
    return f"{float(value):.2f}%"


def _clean_theme_label(value: str) -> str:
    text = str(value or "").strip()
    for suffix in ("概念股", "概念", "主线", "主题"):
        if text.endswith(suffix):
            stripped = text[: -len(suffix)].strip()
            if stripped:
                return stripped
    return text


def _compact_catalyst_label(title: str, stock_name: str) -> str:
    text = str(title or "").strip()
    if stock_name:
        text = text.replace(stock_name, "").strip(" -_:：，,")
    return text or "启动催化"


def _display_final_judgment(text: str) -> str:
    sentence = str(text or "").strip()
    for prefix in ("这轮主升更偏向", "这轮主升主要由"):
        if sentence.startswith(prefix):
            return sentence[len(prefix) :].strip()
    return sentence


def _has_usable_tushare_token(token: str | None) -> bool:
    value = str(token or "").strip()
    return bool(value) and value != "REPLACE_WITH_TUSHARE_TOKEN"


def _pick_main_concept(
    *,
    concept_rows: list[dict[str, str]],
    sample_label_clean: str,
    news_corpus: str,
) -> str:
    best_name = ""
    best_score = -1
    for index, row in enumerate(concept_rows):
        concept_name = _clean_theme_label(str(row.get("concept_name", "")))
        if not concept_name:
            continue
        score = max(20 - index * 2, 0)
        if sample_label_clean and (sample_label_clean in concept_name or concept_name in sample_label_clean):
            score += 120
        if concept_name and concept_name in news_corpus:
            score += 60
        if score > best_score:
            best_score = score
            best_name = concept_name
    return best_name


def _pick_catalyst_label(selected_news: list[dict[str, Any]], stock_name: str) -> str:
    ordered = sorted(
        selected_news,
        key=lambda row: len(str(row.get("title", ""))),
    )
    for row in ordered:
        title = str(row.get("title", "")).strip()
        raw_first_line = _summarize_timeline_impact(str(row.get("raw_text", "")))
        candidate = raw_first_line if "..." in title or len(title) > 28 else title
        compact = _compact_catalyst_label(candidate, stock_name)
        if compact and len(compact) <= 18:
            return compact
        for needle, label in (
            ("T链", "T链催化"),
            ("丝杠", "丝杠催化"),
            ("人形机器人", "人形机器人催化"),
            ("机器人", "机器人催化"),
            ("商业航天", "商业航天催化"),
            ("卫星", "卫星催化"),
            ("液冷", "液冷催化"),
            ("算力", "算力催化"),
            ("AIDC", "AIDC催化"),
            ("柔性直流", "柔性直流催化"),
            ("特高压", "特高压催化"),
            ("空间电源", "空间电源催化"),
            ("砷化镓", "砷化镓催化"),
            ("CPO", "CPO催化"),
            ("硅光", "硅光催化"),
        ):
            if needle in candidate:
                return label
    return "启动催化"


def _compose_final_judgment(main_cause: str, sample_label: str) -> str:
    sample_label_clean = _clean_theme_label(sample_label)
    if sample_label and sample_label_clean and sample_label_clean != main_cause:
        return f"这轮主升更偏向{main_cause}主线，不是泛{sample_label_clean}概念跟涨。"
    return f"这轮主升主要由{main_cause}驱动。"


def _build_local_verdict(
    *,
    case_context: dict[str, str],
    selected_news: list[dict[str, Any]],
    concept_rows: list[dict[str, str]],
    quant_rows: list[dict[str, str]],
) -> dict[str, Any]:
    sample_label = str(case_context.get("sample_label", "")).strip()
    sample_label_clean = _clean_theme_label(sample_label)
    news_corpus = " ".join(
        " ".join([str(row.get("title", "")), str(row.get("raw_text", ""))]) for row in selected_news
    )

    main_cause = _pick_main_concept(
        concept_rows=concept_rows,
        sample_label_clean=sample_label_clean,
        news_corpus=news_corpus,
    )
    if not main_cause:
        main_cause = sample_label_clean or sample_label or str(case_context.get("stock_name", "")).strip()
    refinements: list[str] = []
    for needle, label in (
        ("T链", "T链"),
        ("丝杠", "丝杠平台化"),
        ("人形机器人", "人形机器人"),
        ("机器人", "机器人"),
        ("商业航天", "商业航天"),
        ("卫星", "卫星互联网"),
        ("液冷", "液冷"),
        ("算力", "算力"),
        ("AIDC", "AIDC"),
        ("柔性直流", "柔性直流"),
        ("特高压", "特高压"),
        ("空间电源", "空间电源"),
        ("砷化镓", "砷化镓"),
        ("CPO", "CPO"),
        ("硅光", "硅光"),
    ):
        if needle in news_corpus and label not in refinements and label != sample_label_clean and label != main_cause:
            refinements.append(label)
    if refinements:
        main_parts = [main_cause, *refinements[:2]]
        main_cause = " / ".join(dict.fromkeys(main_parts))

    catalyst = _pick_catalyst_label(selected_news, str(case_context.get("stock_name", "")))
    alt_cause = f"{sample_label_clean}板块情绪强化" if sample_label_clean else "板块情绪强化"
    if alt_cause == main_cause and len(selected_news) > 1:
        alt_cause = _compact_catalyst_label(
            str(selected_news[1].get("title", "")),
            str(case_context.get("stock_name", "")),
        )
    if not alt_cause:
        alt_cause = "量价共振强化"

    one_line_parts = [f"{main_cause}主线驱动"]
    if catalyst and catalyst not in {main_cause, "启动催化"}:
        one_line_parts.append(f"{catalyst}点火")
    if alt_cause and alt_cause not in {main_cause, catalyst}:
        one_line_parts.append(alt_cause if alt_cause.endswith("强化") else f"{alt_cause}强化")
    one_line_logic = "，".join(one_line_parts).rstrip("，")
    if one_line_logic:
        one_line_logic = f"{one_line_logic}。"

    confidence = "中高" if selected_news and concept_rows and quant_rows else "中"
    final_judgment = _compose_final_judgment(main_cause, sample_label)
    notes = "精选本地 news、量价与概念联动验证共同支撑。"
    if catalyst and catalyst not in {main_cause, "启动催化"}:
        notes = f"启动阶段由{catalyst}点火，精选本地 news、量价与概念联动验证共同支撑。"

    conclusion_rows = [
        {
            "dimension": "主因",
            "value": main_cause,
            "confidence": confidence,
            "notes": "概念联动与精选 news 共振验证。",
        }
    ]
    if alt_cause and alt_cause != main_cause:
        conclusion_rows.append(
            {
                "dimension": "备选",
                "value": alt_cause,
                "confidence": "中",
                "notes": "作为辅助催化或板块情绪，不改写主因。",
            }
        )

    return {
        "one_line_logic": one_line_logic,
        "main_cause": main_cause,
        "alt_cause": alt_cause,
        "final_verdict": {
            "main_cause": main_cause,
            "alt_cause": alt_cause,
            "final_judgment": final_judgment,
            "notes": notes,
            "confidence": confidence,
        },
        "conclusion_rows": conclusion_rows,
    }


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
DEFAULT_TOP_WAVE_LIMIT = 2
DEFAULT_NEWS_LOOKBACK_DAYS = 60
DEFAULT_AGENT_RERANK_CHUNK_SIZE = 100
DEFAULT_AGENT_RERANK_PICK_MIN = 3
DEFAULT_AGENT_RERANK_PICK_MAX = 5
DEFAULT_AGENT_FINAL_PICK_COUNT = 10
DEFAULT_AGENT_RERANK_DIRNAME = "agent_rerank"
DEFAULT_SERVICE_TASK_DIR = PROJECT_ROOT / "data" / "service_tasks"
SOURCE_PRIORITY = {
    "zsxq_saidao_touyan": 4,
    "zsxq_damao": 3,
    "zsxq_zhuwang": 3,
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


def _news_window_from_waves(
    waves: list[dict[str, Any]],
    lookback_days: int = DEFAULT_NEWS_LOOKBACK_DAYS,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    starts = [_to_naive_timestamp(wave["start_date"]) for wave in waves if wave.get("start_date")]
    peaks = [_to_naive_timestamp(wave["peak_date"]) for wave in waves if wave.get("peak_date")]
    if not starts or not peaks:
        return None, None
    return min(starts) - pd.Timedelta(days=lookback_days), max(peaks)


def _to_naive_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return timestamp


def _format_display_time(value: Any) -> str:
    return _to_naive_timestamp(value).strftime("%Y-%m-%d %H:%M")


def _format_wave_date(value: Any) -> str:
    return _to_naive_timestamp(value).date().isoformat()


def _format_wave_period(wave_section: dict[str, Any]) -> str:
    if wave_section.get("start_date") and wave_section.get("peak_date"):
        return f"{_format_wave_date(wave_section['start_date'])} -> {_format_wave_date(wave_section['peak_date'])}"
    return str(wave_section.get("period", ""))


def _format_news_source_distribution(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        source_id = str(row.get("source_id", "")).strip()
        if not source_id:
            continue
        counts[source_id] = counts.get(source_id, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " / ".join(f"{source}({count}条)" for source, count in ordered)


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


def _collect_ranked_news_candidates(
    *,
    news_evidence: list[dict[str, Any]],
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    waves: list[dict[str, Any]],
) -> list[tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]]:
    anchors = _anchor_dates_from_waves(waves)
    window_start, window_end = _news_window_from_waves(waves)
    ranked_candidates: list[tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]] = []
    for row in news_evidence:
        if str(row.get("source_id", "")) not in SOURCE_PRIORITY:
            continue
        published_at = _to_naive_timestamp(row.get("published_at"))
        if window_start is not None and published_at < window_start:
            continue
        if window_end is not None and published_at > window_end:
            continue
        score_tuple = _score_news_row(
            row,
            stock_name=stock_name,
            sample_label=sample_label,
            concept_labels=concept_labels,
            anchors=anchors,
        )
        ranked_candidates.append((score_tuple, row))
    return sorted(
        ranked_candidates,
        key=lambda item: (-item[0][0], item[0][1], item[0][2]),
    )


def _select_news_evidence(
    *,
    news_evidence: list[dict[str, Any]],
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    waves: list[dict[str, Any]],
    top_k: int = DEFAULT_EVIDENCE_NEWS_LIMIT,
) -> list[dict[str, Any]]:
    ranked_candidates = _collect_ranked_news_candidates(
        news_evidence=news_evidence,
        stock_name=stock_name,
        sample_label=sample_label,
        concept_labels=concept_labels,
        waves=waves,
    )
    best_by_key: dict[tuple[str, str], tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]] = {}
    for score_tuple, row in ranked_candidates:
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


def _collect_wave_news_rows(
    *,
    news_evidence: list[dict[str, Any]],
    waves: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    window_start, window_end = _news_window_from_waves(waves)
    rows: list[dict[str, Any]] = []
    for row in news_evidence:
        if str(row.get("source_id", "")) not in SOURCE_PRIORITY:
            continue
        published_at = _to_naive_timestamp(row.get("published_at"))
        if window_start is not None and published_at < window_start:
            continue
        if window_end is not None and published_at > window_end:
            continue
        copied = dict(row)
        copied["published_at"] = _format_display_time(published_at)
        rows.append(copied)
    return sorted(rows, key=lambda row: _to_naive_timestamp(row.get("published_at")))


def _candidate_title_key(row: dict[str, Any]) -> str:
    title = str(row.get("title", "")).strip()
    if title:
        return title
    key = _normalize_news_key(row)
    return " | ".join(part for part in key if part).strip() or "untitled"


def _build_agent_candidate_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: _to_naive_timestamp(item.get("published_at"))):
        key = _candidate_title_key(row)
        bucket = merged.setdefault(
            key,
            {
                "title": str(row.get("title", "")).strip(),
                "first_published_at": str(row.get("published_at", "")),
                "last_published_at": str(row.get("published_at", "")),
                "raw_count": 0,
                "source_counts": {},
                "representative_row": dict(row),
                "sample_rows": [],
            },
        )
        bucket["raw_count"] += 1
        published_at = str(row.get("published_at", ""))
        if published_at < bucket["first_published_at"]:
            bucket["first_published_at"] = published_at
            bucket["representative_row"] = dict(row)
        if published_at > bucket["last_published_at"]:
            bucket["last_published_at"] = published_at
        source_id = str(row.get("source_id", "")).strip()
        bucket["source_counts"][source_id] = int(bucket["source_counts"].get(source_id, 0)) + 1
        if len(bucket["sample_rows"]) < 5:
            bucket["sample_rows"].append(
                {
                    "published_at": published_at,
                    "source_id": source_id,
                    "raw_text": str(row.get("raw_text", "")),
                    "url": str(row.get("url", "")),
                }
            )

    items: list[dict[str, Any]] = []
    for index, bucket in enumerate(
        sorted(merged.values(), key=lambda item: (item["first_published_at"], item["title"])),
        start=1,
    ):
        ordered_source_counts = dict(
            sorted(bucket["source_counts"].items(), key=lambda item: (-item[1], item[0]))
        )
        items.append(
            {
                "item_id": f"I{index:05d}",
                "title": bucket["title"],
                "first_published_at": bucket["first_published_at"],
                "last_published_at": bucket["last_published_at"],
                "raw_count": bucket["raw_count"],
                "source_counts": ordered_source_counts,
                "representative_row": bucket["representative_row"],
                "sample_rows": bucket["sample_rows"],
            }
        )
    return items


def _render_agent_rerank_chunk_markdown(
    *,
    wave: dict[str, Any],
    chunk_index: int,
    candidates: list[dict[str, Any]],
) -> str:
    lines = [
        f"# Chunk {chunk_index:03d}",
        "",
        f"- 波段：`{_format_wave_date(wave['start_date'])} -> {_format_wave_date(wave['peak_date'])}`",
        f"- 涨幅：`{_format_percent(wave['wave_gain_pct'])}`",
        f"- 粗排规则：`每个 chunk 直接选 {DEFAULT_AGENT_RERANK_PICK_MIN}-{DEFAULT_AGENT_RERANK_PICK_MAX} 条，不要逐条打分`",
        "",
    ]
    for item in candidates:
        source_desc = " / ".join(f"{key}({value})" for key, value in item["source_counts"].items())
        lines.append(
            f"- {item['item_id']} | {item['first_published_at']} | {item['raw_count']}条同题 | {source_desc} | {item['title']}"
        )
    return "\n".join(lines) + "\n"


def _write_agent_wave_artifacts(
    *,
    wave_dir: Path,
    wave: dict[str, Any],
    rough_rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    wave_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir = wave_dir / "rough_chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    with (wave_dir / "candidates.jsonl").open("w", encoding="utf-8") as fh:
        for item in candidates:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")

    chunk_paths: list[str] = []
    for chunk_index, start in enumerate(range(0, len(candidates), DEFAULT_AGENT_RERANK_CHUNK_SIZE), start=1):
        chunk_candidates = candidates[start : start + DEFAULT_AGENT_RERANK_CHUNK_SIZE]
        chunk_path = chunks_dir / f"chunk_{chunk_index:03d}.md"
        chunk_path.write_text(
            _render_agent_rerank_chunk_markdown(
                wave=wave,
                chunk_index=chunk_index,
                candidates=chunk_candidates,
            ),
            encoding="utf-8",
        )
        chunk_paths.append(str(chunk_path))

    wave_summary = {
        "wave_id": str(wave["wave_id"]),
        "start_date": _format_wave_date(wave["start_date"]),
        "peak_date": _format_wave_date(wave["peak_date"]),
        "wave_gain_pct": round(float(wave["wave_gain_pct"]), 4),
        "rough_news_count": len(rough_rows),
        "dedup_title_count": len(candidates),
        "chunk_count": len(chunk_paths),
        "wave_dir": str(wave_dir),
        "chunk_paths": chunk_paths,
    }
    (wave_dir / "wave_summary.json").write_text(json.dumps(wave_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return wave_summary


def _load_agent_candidates(wave_dir: Path) -> dict[str, dict[str, Any]]:
    path = wave_dir / "candidates.jsonl"
    mapping: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            mapping[str(row["item_id"])] = row
    return mapping


def _selection_for_wave(selection_payload: dict[str, Any], wave_id: str) -> dict[str, Any]:
    for wave in selection_payload.get("waves", []):
        if str(wave.get("wave_id", "")) == wave_id:
            return wave
    return {}


def _build_timeline_rows(news_evidence: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    ordered = sorted(news_evidence, key=lambda row: _to_naive_timestamp(row.get("published_at")))
    for row in ordered[:DEFAULT_TIMELINE_NEWS_LIMIT]:
        rows.append(
            {
                "time": _format_display_time(row.get("published_at", "")),
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


def _slice_frame_by_trade_date(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if frame.empty or "trade_date" not in frame.columns:
        return frame.copy()
    ordered = frame.copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"])
    start_ts = _to_naive_timestamp(start_date)
    end_ts = _to_naive_timestamp(end_date)
    return ordered[(ordered["trade_date"] >= start_ts) & (ordered["trade_date"] <= end_ts)].reset_index(drop=True)


def _slice_stock_bundle_for_wave(stock_bundle: dict[str, pd.DataFrame], wave: dict[str, Any]) -> dict[str, pd.DataFrame]:
    sliced: dict[str, pd.DataFrame] = {}
    for key, frame in stock_bundle.items():
        if isinstance(frame, pd.DataFrame):
            sliced[key] = _slice_frame_by_trade_date(frame, str(wave["start_date"]), str(wave["peak_date"]))
        else:
            sliced[key] = frame
    if sliced["raw_stock_daily_qfq"].empty:
        raise ValueError(f"wave window has no stock rows: {wave['start_date']} -> {wave['peak_date']}")
    return sliced


def _slice_concept_frames_for_wave(concept_frames: dict[str, pd.DataFrame], wave: dict[str, Any]) -> dict[str, pd.DataFrame]:
    return {
        code: _slice_frame_by_trade_date(frame, str(wave["start_date"]), str(wave["peak_date"]))
        for code, frame in concept_frames.items()
    }


def _select_top_waves(waves: list[dict[str, Any]], top_k: int = DEFAULT_TOP_WAVE_LIMIT) -> list[dict[str, Any]]:
    ranked = sorted(
        (dict(wave) for wave in waves),
        key=lambda wave: (
            -float(wave.get("wave_gain_pct", 0.0)),
            _to_naive_timestamp(wave.get("start_date")),
            _to_naive_timestamp(wave.get("peak_date")),
        ),
    )
    selected = ranked[:top_k]
    for index, wave in enumerate(selected, start=1):
        wave["wave_id"] = f"W{index}"
    return selected


def _build_wave_section_markdown(wave_section: dict[str, Any]) -> str:
    raw_blocks = _render_news_raw_blocks(wave_section["news_rows"])
    quant_table = _table(
        ["维度", "数值", "证据", "解释"],
        [[row["metric"], row["value"], row["evidence"], row["interpretation"]] for row in wave_section["quant_rows"]],
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
            for row in wave_section["concept_rows"]
        ],
    )
    conclusion_table = _table(
        ["维度", "结论", "置信度", "说明"],
        [[row["dimension"], row["value"], row["confidence"], row["notes"]] for row in wave_section["conclusion_rows"]],
    )
    final_verdict = wave_section.get("final_verdict", {})
    final_verdict_lines = []
    if final_verdict:
        final_verdict_lines.extend(
            [
                "## 综合裁决",
                "",
                f"- 主因：`{final_verdict.get('main_cause', '')}`",
                f"- 备选：`{final_verdict.get('alt_cause', '')}`",
                f"- 最终判定：{_display_final_judgment(final_verdict.get('final_judgment', ''))}",
                f"- 说明：{final_verdict.get('notes', '')}",
                f"- 置信度：`{final_verdict.get('confidence', '')}`",
            ]
        )

    return f"""# 波段 {wave_section['wave_id']}

- 区间：`{_format_wave_period(wave_section)}`
- 涨幅：`{wave_section['gain_pct']}`
- 波段审查：`{wave_section['review']}`
- 粗排新闻来源分布：`{_format_news_source_distribution(wave_section.get("rough_news_rows", wave_section["news_rows"]))}`
- 一句话逻辑：`{wave_section.get('one_line_logic', '')}`

## 证据原文

{raw_blocks}

## 量价验证表

{quant_table}

## 概念联动验证表

{concept_table}

## 结论与置信度表

{conclusion_table}

{chr(10).join(final_verdict_lines)}
"""


def render_detailed_markdown(payload: dict[str, Any]) -> str:
    wave_sections = "\n\n".join(_build_wave_section_markdown(wave) for wave in payload["wave_sections"])
    return f"""# {payload['stock_name']}波段归因

## 基础信息

- 标的名称：{payload['stock_name']}
- 股票代码：`{payload['ts_code']}`
- 分析窗口：`{payload['start_date']}` 到 `{payload['end_date']}`
- 报告时间：`{payload.get('report_time', '')}`
- 分析波段数：`{len(payload.get('wave_sections', []))}`
- 一句话逻辑：`{payload.get('one_line_logic', '')}`

波段图：

![]({payload['plot_relpath']})

{wave_sections}
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
    waves = _select_top_waves(waves)
    if not waves:
        raise ValueError("no top waves selected")

    plot_path = plot_dir / f"{case_context['ts_code'].replace('.', '_')}_orchestrator.png"
    plot_meta = plotter(
        df=stock_df,
        waves=waves,
        output_path=plot_path,
        title=f"{case_context['stock_name']} 波段图",
        style="enhanced",
    )

    chatgpt_enabled = _chatgpt_enabled(use_chatgpt=use_chatgpt)
    wave_sections: list[dict[str, Any]] = []
    for wave in waves:
        wave_id = str(wave["wave_id"])
        wave_stock_bundle = _slice_stock_bundle_for_wave(stock_bundle, wave)
        wave_concept_frames = _slice_concept_frames_for_wave(concept_frames, wave)
        quant_rows = _build_quant_rows(wave_stock_bundle)
        concept_rows = _build_concept_rows(wave_stock_bundle, wave_concept_frames, concept_labels)
        ranked_news_candidates = _collect_ranked_news_candidates(
            news_evidence=news_evidence,
            stock_name=case_context["stock_name"],
            sample_label=case_context.get("sample_label", ""),
            concept_labels=concept_labels,
            waves=[wave],
        )
        selected_news = _select_news_evidence(
            news_evidence=news_evidence,
            stock_name=case_context["stock_name"],
            sample_label=case_context.get("sample_label", ""),
            concept_labels=concept_labels,
            waves=[wave],
            top_k=DEFAULT_EVIDENCE_NEWS_LIMIT,
        )
        local_verdict = _build_local_verdict(
            case_context=case_context,
            selected_news=selected_news,
            concept_rows=concept_rows,
            quant_rows=quant_rows,
        )

        review = "rule_based"
        attribution_text = ""
        if chatgpt_enabled:
            review_prompt = (
                f"请审查波段 {wave_id} 是否属于有效主升段，只返回 up_valid/down_valid/noise/merge_adjacent。\n"
                f"区间：{wave['start_date']} -> {wave['peak_date']}，涨幅：{float(wave['wave_gain_pct']):.2f}%"
            )
            review_text = chatgpt_runner(review_prompt, mode="plain")
            review = _review_decision(review_text)
        if chatgpt_enabled and review in {"up_valid", "down_valid"}:
            attribution_prompt = (
                f"请分析波段 {wave_id} 在 {wave['start_date']} 到 {wave['peak_date']} 的主因、备选和搜索依据。"
            )
            attribution_text = chatgpt_runner(attribution_prompt, mode="search")
        main_cause = _extract_label(attribution_text, "主因") or str(local_verdict["main_cause"])
        alt_cause = _extract_label(attribution_text, "备选") or str(local_verdict["alt_cause"])
        search_basis = _extract_label(attribution_text, "搜索依据")
        final_verdict = dict(local_verdict["final_verdict"])
        final_verdict["main_cause"] = main_cause
        final_verdict["alt_cause"] = alt_cause
        final_verdict["final_judgment"] = _compose_final_judgment(main_cause, str(case_context.get("sample_label", "")))
        if search_basis:
            final_verdict["notes"] = search_basis
        if chatgpt_enabled and review == "up_valid":
            final_verdict["confidence"] = "中高"

        conclusion_rows = local_verdict["conclusion_rows"]
        if attribution_text:
            conclusion_rows = [
                {
                    "dimension": "主因",
                    "value": main_cause,
                    "confidence": "中高" if review == "up_valid" else "中",
                    "notes": search_basis or "需补本地证据",
                }
            ]
            if alt_cause and alt_cause != main_cause:
                conclusion_rows.append(
                    {
                        "dimension": "备选",
                        "value": alt_cause,
                        "confidence": "中",
                        "notes": "作为辅助催化或交叉题材参考。",
                    }
                )

        wave_sections.append(
            {
                "wave_id": wave_id,
                "start_date": str(wave["start_date"]),
                "peak_date": str(wave["peak_date"]),
                "period": f"{_format_wave_date(wave['start_date'])} -> {_format_wave_date(wave['peak_date'])}",
                "gain_pct": _format_percent(wave["wave_gain_pct"]),
                "review": review,
                "one_line_logic": local_verdict["one_line_logic"],
                "rough_news_rows": [row for _, row in ranked_news_candidates],
                "news_rows": selected_news,
                "quant_rows": quant_rows,
                "concept_rows": concept_rows,
                "conclusion_rows": conclusion_rows,
                "final_verdict": final_verdict,
            }
        )

    report_payload = {
        "stock_name": case_context["stock_name"],
        "ts_code": case_context["ts_code"],
        "start_date": case_context["start_date"],
        "end_date": case_context["end_date"],
        "report_time": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(timespec="seconds"),
        "one_line_logic": f"仅分析涨幅 Top{len(waves)} 波段，并按波段独立完成本地 news、量价与概念联动归因。",
        "plot_relpath": os.path.relpath(plot_meta["output_path"], start=report_dir),
        "wave_sections": wave_sections,
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
        "wave_count": len(wave_sections),
    }


def prepare_agent_rerank_task(
    *,
    case_context: dict[str, str],
    stock_bundle: dict[str, pd.DataFrame],
    news_evidence: list[dict[str, Any]],
    concept_frames: dict[str, pd.DataFrame],
    concept_labels: dict[str, dict[str, str]] | None = None,
    rerank_root: Path,
    task_id: str,
    segmenter: Callable[[pd.DataFrame], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    _require_pandas()
    rerank_root.mkdir(parents=True, exist_ok=True)
    stock_df = stock_bundle["raw_stock_daily_qfq"].copy()
    stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
    stock_df = stock_df.sort_values("trade_date").reset_index(drop=True)

    waves = (segmenter or _segment_ma_trend_waves)(stock_df)
    if not waves:
        waves = _segment_price_waves(stock_df, min_wave_gain=0.2, min_pullback=0.05, min_bars=3)
    if not waves:
        raise ValueError("no candidate waves generated")
    waves = _select_top_waves(waves)
    if not waves:
        raise ValueError("no top waves selected")

    wave_dirs: dict[str, str] = {}
    wave_summaries: list[dict[str, Any]] = []
    for wave in waves:
        wave_id = str(wave["wave_id"])
        wave_dir = rerank_root / wave_id
        rough_rows = _collect_wave_news_rows(news_evidence=news_evidence, waves=[wave])
        candidates = _build_agent_candidate_items(rough_rows)
        wave_summary = _write_agent_wave_artifacts(
            wave_dir=wave_dir,
            wave=wave,
            rough_rows=rough_rows,
            candidates=candidates,
        )
        wave_dirs[wave_id] = str(wave_dir)
        wave_summaries.append(wave_summary)

    summary = {
        "task_id": task_id,
        "stock_name": case_context["stock_name"],
        "ts_code": case_context["ts_code"],
        "start_date": case_context["start_date"],
        "end_date": case_context["end_date"],
        "sample_label": case_context.get("sample_label", ""),
        "wave_count": len(waves),
        "waves": wave_summaries,
    }
    summary_path = rerank_root / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "task_id": task_id,
        "rerank_root": str(rerank_root),
        "summary_path": str(summary_path),
        "wave_count": len(waves),
        "wave_dirs": wave_dirs,
    }


def finalize_agent_rerank_task(
    *,
    case_context: dict[str, str],
    stock_bundle: dict[str, pd.DataFrame],
    news_evidence: list[dict[str, Any]],
    concept_frames: dict[str, pd.DataFrame],
    rerank_root: Path,
    selection_path: Path,
    concept_labels: dict[str, dict[str, str]] | None = None,
    output_root: Path | None = None,
    analysis_dir: Path | None = None,
    plot_dir: Path | None = None,
    segmenter: Callable[[pd.DataFrame], list[dict[str, Any]]] | None = None,
    plotter: Callable[..., dict[str, Any]] = _default_plotter,
) -> dict[str, Any]:
    _require_pandas()
    selection_payload = json.loads(selection_path.read_text(encoding="utf-8"))
    report_dir, plot_dir = _resolve_output_paths(output_root, analysis_dir=analysis_dir, plot_dir=plot_dir)
    stock_df = stock_bundle["raw_stock_daily_qfq"].copy()
    stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
    stock_df = stock_df.sort_values("trade_date").reset_index(drop=True)

    waves = (segmenter or _segment_ma_trend_waves)(stock_df)
    if not waves:
        waves = _segment_price_waves(stock_df, min_wave_gain=0.2, min_pullback=0.05, min_bars=3)
    if not waves:
        raise ValueError("no candidate waves generated")
    waves = _select_top_waves(waves)
    if not waves:
        raise ValueError("no top waves selected")

    plot_path = plot_dir / f"{case_context['ts_code'].replace('.', '_')}_orchestrator.png"
    plot_meta = plotter(
        df=stock_df,
        waves=waves,
        output_path=plot_path,
        title=f"{case_context['stock_name']} 波段图",
        style="enhanced",
    )

    wave_sections: list[dict[str, Any]] = []
    for wave in waves:
        wave_id = str(wave["wave_id"])
        wave_dir = rerank_root / wave_id
        candidate_map = _load_agent_candidates(wave_dir)
        wave_selection = _selection_for_wave(selection_payload, wave_id)
        final_picks = wave_selection.get("final_picks", []) if isinstance(wave_selection, dict) else []
        selected_news = [
            dict(candidate_map[str(item.get("item_id", ""))]["representative_row"])
            for item in final_picks
            if str(item.get("item_id", "")) in candidate_map
        ]

        wave_stock_bundle = _slice_stock_bundle_for_wave(stock_bundle, wave)
        wave_concept_frames = _slice_concept_frames_for_wave(concept_frames, wave)
        quant_rows = _build_quant_rows(wave_stock_bundle)
        concept_rows = _build_concept_rows(wave_stock_bundle, wave_concept_frames, concept_labels)
        local_verdict = _build_local_verdict(
            case_context=case_context,
            selected_news=selected_news,
            concept_rows=concept_rows,
            quant_rows=quant_rows,
        )

        one_line_logic = str(
            wave_selection.get("one_line_logic")
            or selection_payload.get("one_liner")
            or local_verdict["one_line_logic"]
        ).strip()
        if one_line_logic and not one_line_logic.endswith("。"):
            one_line_logic = f"{one_line_logic}。"
        rough_rows = _collect_wave_news_rows(news_evidence=news_evidence, waves=[wave])
        wave_sections.append(
            {
                "wave_id": wave_id,
                "start_date": str(wave["start_date"]),
                "peak_date": str(wave["peak_date"]),
                "period": f"{_format_wave_date(wave['start_date'])} -> {_format_wave_date(wave['peak_date'])}",
                "gain_pct": _format_percent(wave["wave_gain_pct"]),
                "review": "agent_rerank",
                "one_line_logic": one_line_logic,
                "rough_news_rows": rough_rows,
                "news_rows": selected_news,
                "quant_rows": quant_rows,
                "concept_rows": concept_rows,
                "conclusion_rows": local_verdict["conclusion_rows"],
                "final_verdict": local_verdict["final_verdict"],
            }
        )

    report_payload = {
        "stock_name": case_context["stock_name"],
        "ts_code": case_context["ts_code"],
        "start_date": case_context["start_date"],
        "end_date": case_context["end_date"],
        "report_time": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(timespec="seconds"),
        "plot_relpath": os.path.relpath(plot_meta["output_path"], report_dir).replace(os.sep, "/"),
        "one_line_logic": str(selection_payload.get("one_liner", "")).strip(),
        "wave_sections": wave_sections,
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
        "call_chain": LOCAL_CALL_CHAIN,
        "wave_count": len(wave_sections),
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
    akshare_stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_window_bundle_from_akshare,
    quant_bundle_persister: Callable[..., None] | None = _persist_quant_bundle_to_db,
    window_validator: Callable[..., None] = _validate_stock_window_coverage,
    concept_fetcher: Callable[..., tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]] = _fetch_stock_concept_frames,
    tushare_concept_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_concept_bundle_from_tushare,
    concept_bundle_persister: Callable[..., None] | None = _persist_concept_bundle_to_db,
    news_fetcher: Callable[..., list[dict[str, Any]]] = _fetch_news_evidence,
    attribution_runner: Callable[..., dict[str, Any]] = run_stock_wave_attribution,
) -> dict[str, Any]:
    runtime = load_skill_config(config_path)
    if db_connect is None:
        import psycopg

        db_connect = psycopg.connect

    with db_connect(runtime["postgres"]["event_quant_dsn"]) as quant_conn:
        stock_bundle = _ensure_stock_window_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            stock_bundle_fetcher=stock_bundle_fetcher,
            window_validator=window_validator,
            akshare_stock_bundle_fetcher=akshare_stock_bundle_fetcher,
            quant_bundle_persister=quant_bundle_persister,
        )
        concept_frames, concept_labels = _ensure_stock_concept_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            concept_fetcher=concept_fetcher,
            tushare_concept_bundle_fetcher=tushare_concept_bundle_fetcher,
            concept_bundle_persister=concept_bundle_persister,
            tushare_token=str(runtime.get("tushare", {}).get("token", "")),
            tushare_http_url=str(runtime.get("tushare", {}).get("http_url", "")),
        )

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


def run_local_prepare_agent_rerank_task(
    *,
    stock_name: str,
    ts_code: str,
    start_date: str,
    end_date: str,
    sample_label: str,
    task_id: str,
    config_path: str | Path | None = None,
    db_connect: Callable[..., Any] | None = None,
    stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] = _fetch_stock_window_bundle,
    akshare_stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_window_bundle_from_akshare,
    quant_bundle_persister: Callable[..., None] | None = _persist_quant_bundle_to_db,
    window_validator: Callable[..., None] = _validate_stock_window_coverage,
    concept_fetcher: Callable[..., tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]] = _fetch_stock_concept_frames,
    tushare_concept_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_concept_bundle_from_tushare,
    concept_bundle_persister: Callable[..., None] | None = _persist_concept_bundle_to_db,
    news_fetcher: Callable[..., list[dict[str, Any]]] = _fetch_news_evidence,
) -> dict[str, Any]:
    runtime = load_skill_config(config_path)
    if db_connect is None:
        import psycopg

        db_connect = psycopg.connect

    with db_connect(runtime["postgres"]["event_quant_dsn"]) as quant_conn:
        stock_bundle = _ensure_stock_window_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            stock_bundle_fetcher=stock_bundle_fetcher,
            window_validator=window_validator,
            akshare_stock_bundle_fetcher=akshare_stock_bundle_fetcher,
            quant_bundle_persister=quant_bundle_persister,
        )
        concept_frames, concept_labels = _ensure_stock_concept_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            concept_fetcher=concept_fetcher,
            tushare_concept_bundle_fetcher=tushare_concept_bundle_fetcher,
            concept_bundle_persister=concept_bundle_persister,
            tushare_token=str(runtime.get("tushare", {}).get("token", "")),
            tushare_http_url=str(runtime.get("tushare", {}).get("http_url", "")),
        )

    keywords = _build_news_keywords(stock_name, sample_label, concept_labels)
    with db_connect(runtime["postgres"]["event_news_dsn"]) as news_conn:
        news_evidence = news_fetcher(news_conn, start_date, end_date, keywords)

    rerank_root = DEFAULT_SERVICE_TASK_DIR / task_id / DEFAULT_AGENT_RERANK_DIRNAME
    return prepare_agent_rerank_task(
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
        rerank_root=rerank_root,
        task_id=task_id,
    )


def run_local_finalize_agent_rerank_task(
    *,
    stock_name: str,
    ts_code: str,
    start_date: str,
    end_date: str,
    sample_label: str,
    task_id: str,
    selection_path: str | Path,
    config_path: str | Path | None = None,
    db_connect: Callable[..., Any] | None = None,
    stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] = _fetch_stock_window_bundle,
    akshare_stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_window_bundle_from_akshare,
    quant_bundle_persister: Callable[..., None] | None = _persist_quant_bundle_to_db,
    window_validator: Callable[..., None] = _validate_stock_window_coverage,
    concept_fetcher: Callable[..., tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]] = _fetch_stock_concept_frames,
    tushare_concept_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None = _fetch_stock_concept_bundle_from_tushare,
    concept_bundle_persister: Callable[..., None] | None = _persist_concept_bundle_to_db,
    news_fetcher: Callable[..., list[dict[str, Any]]] = _fetch_news_evidence,
) -> dict[str, Any]:
    runtime = load_skill_config(config_path)
    if db_connect is None:
        import psycopg

        db_connect = psycopg.connect

    with db_connect(runtime["postgres"]["event_quant_dsn"]) as quant_conn:
        stock_bundle = _ensure_stock_window_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            stock_bundle_fetcher=stock_bundle_fetcher,
            window_validator=window_validator,
            akshare_stock_bundle_fetcher=akshare_stock_bundle_fetcher,
            quant_bundle_persister=quant_bundle_persister,
        )
        concept_frames, concept_labels = _ensure_stock_concept_bundle(
            conn=quant_conn,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            concept_fetcher=concept_fetcher,
            tushare_concept_bundle_fetcher=tushare_concept_bundle_fetcher,
            concept_bundle_persister=concept_bundle_persister,
            tushare_token=str(runtime.get("tushare", {}).get("token", "")),
            tushare_http_url=str(runtime.get("tushare", {}).get("http_url", "")),
        )

    keywords = _build_news_keywords(stock_name, sample_label, concept_labels)
    with db_connect(runtime["postgres"]["event_news_dsn"]) as news_conn:
        news_evidence = news_fetcher(news_conn, start_date, end_date, keywords)

    rerank_root = DEFAULT_SERVICE_TASK_DIR / task_id / DEFAULT_AGENT_RERANK_DIRNAME
    return finalize_agent_rerank_task(
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
        rerank_root=rerank_root,
        selection_path=Path(selection_path),
        analysis_dir=Path(runtime["paths"]["analysis_dir"]),
        plot_dir=Path(runtime["paths"]["plot_dir"]),
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


def _build_prepare_agent_rerank_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestrator.py prepare-agent-rerank")
    parser.add_argument("--stock-name", required=True)
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample-label", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--config", default=None)
    return parser


def _build_finalize_agent_rerank_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orchestrator.py finalize-agent-rerank")
    parser.add_argument("--stock-name", required=True)
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample-label", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--selection-path", required=True)
    parser.add_argument("--config", default=None)
    return parser


def _to_akshare_trade_date(value: str) -> str:
    return str(value).replace("-", "")


def _ensure_stock_window_bundle(
    *,
    conn,
    ts_code: str,
    start_date: str,
    end_date: str,
    stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]],
    window_validator: Callable[..., None],
    akshare_stock_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None,
    quant_bundle_persister: Callable[..., None] | None,
) -> dict[str, pd.DataFrame]:
    stock_bundle = stock_bundle_fetcher(conn, ts_code, start_date, end_date)
    try:
        window_validator(stock_bundle, ts_code, start_date, end_date)
        return stock_bundle
    except ValueError:
        if akshare_stock_bundle_fetcher is None or quant_bundle_persister is None:
            raise

    logger.info(
        "量价窗口不足，开始使用 Akshare 补库: ts_code=%s start_date=%s end_date=%s",
        ts_code,
        start_date,
        end_date,
    )
    fallback_bundle = akshare_stock_bundle_fetcher(
        ts_code=ts_code,
        start_date=_to_akshare_trade_date(start_date),
        end_date=_to_akshare_trade_date(end_date),
    )
    quant_bundle_persister(conn, ts_code, fallback_bundle)
    stock_bundle = stock_bundle_fetcher(conn, ts_code, start_date, end_date)
    window_validator(stock_bundle, ts_code, start_date, end_date)
    logger.info(
        "Akshare 补库后数据库回读完成: ts_code=%s start_date=%s end_date=%s",
        ts_code,
        start_date,
        end_date,
    )
    return stock_bundle


def _ensure_stock_concept_bundle(
    *,
    conn,
    ts_code: str,
    start_date: str,
    end_date: str,
    concept_fetcher: Callable[..., tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]],
    tushare_concept_bundle_fetcher: Callable[..., dict[str, pd.DataFrame]] | None,
    concept_bundle_persister: Callable[..., None] | None,
    tushare_token: str,
    tushare_http_url: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]:
    concept_frames, concept_labels = concept_fetcher(conn, ts_code, start_date, end_date)
    if concept_frames or concept_labels:
        return concept_frames, concept_labels
    if (
        tushare_concept_bundle_fetcher is None
        or concept_bundle_persister is None
        or not _has_usable_tushare_token(tushare_token)
    ):
        return concept_frames, concept_labels

    logger.info(
        "概念映射为空，开始使用 Tushare 代理补库: ts_code=%s start_date=%s end_date=%s",
        ts_code,
        start_date,
        end_date,
    )
    fallback_bundle = tushare_concept_bundle_fetcher(
        ts_code=ts_code,
        start_date=_to_akshare_trade_date(start_date),
        end_date=_to_akshare_trade_date(end_date),
        token=tushare_token,
        http_url=tushare_http_url,
    )
    if any(isinstance(frame, pd.DataFrame) and not frame.empty for frame in fallback_bundle.values()):
        concept_bundle_persister(conn, ts_code, fallback_bundle)
        concept_frames, concept_labels = concept_fetcher(conn, ts_code, start_date, end_date)
    logger.info(
        "Tushare 概念补库后数据库回读完成: ts_code=%s concept_count=%s",
        ts_code,
        len(concept_frames),
    )
    return concept_frames, concept_labels


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(
            "Usage:\n"
            "  orchestrator.py deps\n"
            "  orchestrator.py contract-path\n"
            "  orchestrator.py run --stock-name 名称 --ts-code 代码 --start-date YYYY-MM-DD --end-date YYYY-MM-DD --sample-label 标签 [--config 路径]\n"
            "  orchestrator.py prepare-agent-rerank --stock-name 名称 --ts-code 代码 --start-date YYYY-MM-DD --end-date YYYY-MM-DD --sample-label 标签 --task-id 任务ID [--config 路径]\n"
            "  orchestrator.py finalize-agent-rerank --stock-name 名称 --ts-code 代码 --start-date YYYY-MM-DD --end-date YYYY-MM-DD --sample-label 标签 --task-id 任务ID --selection-path 路径 [--config 路径]\n"
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
    if argv[0] == "prepare-agent-rerank":
        parser = _build_prepare_agent_rerank_parser()
        args = parser.parse_args(argv[1:])
        result = run_local_prepare_agent_rerank_task(
            stock_name=args.stock_name,
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            sample_label=args.sample_label,
            task_id=args.task_id,
            config_path=args.config,
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if argv[0] == "finalize-agent-rerank":
        parser = _build_finalize_agent_rerank_parser()
        args = parser.parse_args(argv[1:])
        result = run_local_finalize_agent_rerank_task(
            stock_name=args.stock_name,
            ts_code=args.ts_code,
            start_date=args.start_date,
            end_date=args.end_date,
            sample_label=args.sample_label,
            task_id=args.task_id,
            selection_path=args.selection_path,
            config_path=args.config,
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    raise SystemExit(f"Unknown command: {argv[0]}")


if __name__ == "__main__":
    raise SystemExit(main())
