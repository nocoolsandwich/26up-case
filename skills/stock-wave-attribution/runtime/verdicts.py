from __future__ import annotations

from typing import Any

from runtime.news_selection import summarize_timeline_impact


def clean_theme_label(value: str) -> str:
    text = str(value or "").strip()
    for suffix in ("概念股", "概念", "主线", "主题"):
        if text.endswith(suffix):
            stripped = text[: -len(suffix)].strip()
            if stripped:
                return stripped
    return text


def compact_catalyst_label(title: str, stock_name: str) -> str:
    text = str(title or "").strip()
    if stock_name:
        text = text.replace(stock_name, "").strip(" -_:：，,")
    return text or "启动催化"


def display_final_judgment(text: str) -> str:
    sentence = str(text or "").strip()
    for prefix in ("这轮主升更偏向", "这轮主升主要由"):
        if sentence.startswith(prefix):
            return sentence[len(prefix) :].strip()
    return sentence


def pick_main_concept(
    *,
    concept_rows: list[dict[str, str]],
    sample_label_clean: str,
    news_corpus: str,
) -> str:
    best_name = ""
    best_score = -1
    for index, row in enumerate(concept_rows):
        concept_name = clean_theme_label(str(row.get("concept_name", "")))
        if not concept_name:
            continue
        score = max(20 - index * 2, 0)
        if concept_name and concept_name in news_corpus:
            score += 60
        if score > best_score:
            best_score = score
            best_name = concept_name
    return best_name


def build_news_signal_segments(selected_news: list[dict[str, Any]]) -> list[str]:
    segments: list[str] = []
    for row in selected_news:
        title = " ".join(str(row.get("title", "")).split())
        summary = summarize_timeline_impact(str(row.get("raw_text", "")))
        segment = " ".join(part for part in [title, summary] if part).strip()
        if segment:
            segments.append(segment)
    return segments


def count_signal_hits(signal_segments: list[str], needles: tuple[str, ...]) -> int:
    return sum(1 for segment in signal_segments if any(needle and needle in segment for needle in needles))


def pick_mainline_from_news_signals(signal_segments: list[str]) -> str:
    best_label = ""
    best_hits = 0
    for needles, label in (
        (("数据中心", "IDC"), "数据中心"),
        (("AIDC",), "AIDC"),
        (("算力租赁",), "算力租赁"),
        (("算力",), "算力"),
        (("液冷",), "液冷"),
        (("人形机器人",), "人形机器人"),
        (("机器人",), "机器人"),
        (("商业航天",), "商业航天"),
        (("卫星",), "卫星互联网"),
        (("CPO",), "CPO"),
        (("硅光",), "硅光"),
    ):
        hits = count_signal_hits(signal_segments, needles)
        if hits > best_hits:
            best_label = label
            best_hits = hits
    return best_label


def pick_catalyst_label(selected_news: list[dict[str, Any]], stock_name: str) -> str:
    ordered = sorted(
        selected_news,
        key=lambda row: len(str(row.get("title", ""))),
    )
    for row in ordered:
        title = str(row.get("title", "")).strip()
        raw_first_line = summarize_timeline_impact(str(row.get("raw_text", "")))
        candidate = raw_first_line if "..." in title or len(title) > 28 else title
        compact = compact_catalyst_label(candidate, stock_name)
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


def compose_final_judgment(main_cause: str, sample_label: str) -> str:
    return f"这轮主升主要由{main_cause}驱动。"


def build_local_verdict(
    *,
    case_context: dict[str, str],
    selected_news: list[dict[str, Any]],
    concept_rows: list[dict[str, str]],
    quant_rows: list[dict[str, str]],
    skip_concept: bool = False,
) -> dict[str, Any]:
    sample_label = str(case_context.get("sample_label", "")).strip()
    sample_label_clean = clean_theme_label(sample_label)
    signal_segments = build_news_signal_segments(selected_news)
    news_corpus = " ".join(signal_segments)

    main_cause = pick_main_concept(
        concept_rows=concept_rows,
        sample_label_clean=sample_label_clean,
        news_corpus=news_corpus,
    )
    if not main_cause:
        main_cause = pick_mainline_from_news_signals(signal_segments)
    if not main_cause:
        main_cause = sample_label_clean or sample_label or str(case_context.get("stock_name", "")).strip()
    refinements: list[str] = []
    for needles, label in (
        (("T链",), "T链"),
        (("丝杠",), "丝杠平台化"),
        (("人形机器人",), "人形机器人"),
        (("机器人",), "机器人"),
        (("商业航天",), "商业航天"),
        (("卫星",), "卫星互联网"),
        (("液冷",), "液冷"),
        (("算力",), "算力"),
        (("AIDC",), "AIDC"),
        (("柔性直流",), "柔性直流"),
        (("特高压",), "特高压"),
        (("空间电源",), "空间电源"),
        (("砷化镓",), "砷化镓"),
        (("CPO",), "CPO"),
        (("硅光",), "硅光"),
    ):
        if (
            count_signal_hits(signal_segments, needles) >= 2
            and label not in refinements
            and label != sample_label_clean
            and label != main_cause
        ):
            refinements.append(label)
    if refinements:
        main_parts = [main_cause, *refinements[:2]]
        main_cause = " / ".join(dict.fromkeys(main_parts))

    catalyst = pick_catalyst_label(selected_news, str(case_context.get("stock_name", "")))
    alt_anchor = clean_theme_label(main_cause.split("/")[0].strip()) or sample_label_clean
    alt_cause = f"{alt_anchor}板块情绪强化" if alt_anchor else "板块情绪强化"
    if alt_cause == main_cause and len(selected_news) > 1:
        alt_cause = compact_catalyst_label(
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

    concept_available = bool(concept_rows)
    confidence = "中高" if selected_news and concept_available and quant_rows else "中"
    final_judgment = compose_final_judgment(main_cause, sample_label)
    if skip_concept:
        support_notes = "已显式跳过概念联动，仅以精选本地 news 与量价验证共同支撑。"
        main_dimension_notes = "已显式跳过概念联动，本次依据精选 news 与量价验证。"
    elif concept_available:
        support_notes = "精选本地 news、量价与概念联动验证共同支撑。"
        main_dimension_notes = "概念联动与精选 news 共振验证。"
    else:
        support_notes = "精选本地 news 与量价验证共同支撑，概念联动数据暂缺。"
        main_dimension_notes = "概念联动数据暂缺，本次依据精选 news 与量价验证。"

    notes = support_notes
    if catalyst and catalyst not in {main_cause, "启动催化"}:
        notes = f"启动阶段由{catalyst}点火，{support_notes}"

    conclusion_rows = [
        {
            "dimension": "主因",
            "value": main_cause,
            "confidence": confidence,
            "notes": main_dimension_notes,
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
