from __future__ import annotations

from typing import Any

from runtime.news_selection import format_display_time, format_news_source_distribution, to_naive_timestamp
from runtime.verdicts import display_final_judgment


def table(headers: list[str], rows: list[list[Any]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join(["---"] * len(headers)) + "|"
    body = ["| " + " | ".join("" if value is None else str(value) for value in row) + " |" for row in rows]
    return "\n".join([header_line, separator, *body])


def render_news_raw_blocks(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "暂无匹配证据原文。"

    raw_blocks: list[str] = []
    ordered = sorted(rows, key=lambda row: to_naive_timestamp(row.get("published_at")))
    for index, row in enumerate(ordered, start=1):
        published_at = format_display_time(row.get("published_at", ""))
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


def format_wave_date(value: Any) -> str:
    return to_naive_timestamp(value).date().isoformat()


def format_wave_period(wave_section: dict[str, Any]) -> str:
    if wave_section.get("start_date") and wave_section.get("peak_date"):
        return f"{format_wave_date(wave_section['start_date'])} -> {format_wave_date(wave_section['peak_date'])}"
    return str(wave_section.get("period", ""))


def render_concept_section(wave_section: dict[str, Any]) -> str:
    concept_rows = wave_section.get("concept_rows", [])
    if concept_rows:
        return table(
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
                for row in concept_rows
            ],
        )
    if wave_section.get("skip_concept"):
        return "已显式跳过概念联动，本次不做概念联动验证。"
    return "暂无概念联动数据。"


def build_wave_section_markdown(wave_section: dict[str, Any]) -> str:
    raw_blocks = render_news_raw_blocks(wave_section["news_rows"])
    quant_table = table(
        ["维度", "数值", "证据", "解释"],
        [[row["metric"], row["value"], row["evidence"], row["interpretation"]] for row in wave_section["quant_rows"]],
    )
    concept_table = render_concept_section(wave_section)
    conclusion_table = table(
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
                f"- 最终判定：{display_final_judgment(final_verdict.get('final_judgment', ''))}",
                f"- 说明：{final_verdict.get('notes', '')}",
                f"- 置信度：`{final_verdict.get('confidence', '')}`",
            ]
        )

    return f"""# 波段 {wave_section['wave_id']}

- 区间：`{format_wave_period(wave_section)}`
- 涨幅：`{wave_section['gain_pct']}`
- 波段审查：`{wave_section['review']}`
- 粗排新闻来源分布：`{format_news_source_distribution(wave_section.get("rough_news_rows", wave_section["news_rows"]))}`
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
    wave_sections = "\n\n".join(build_wave_section_markdown(wave) for wave in payload["wave_sections"])
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
