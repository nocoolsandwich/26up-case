from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_SOURCE_PRIORITY = {
    "zsxq_saidao_touyan": 4,
    "zsxq_damao": 3,
    "zsxq_zhuwang": 3,
}
DEFAULT_TIMELINE_NEWS_LIMIT = 10


def build_news_keywords(
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


def to_naive_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return timestamp


def format_display_time(value: Any) -> str:
    return to_naive_timestamp(value).strftime("%Y-%m-%d %H:%M")


def summarize_timeline_impact(raw_text: str) -> str:
    text = str(raw_text or "")
    for line in text.splitlines():
        summary = line.strip()
        if summary:
            return summary
    return ""


def normalize_news_key(row: dict[str, Any]) -> tuple[str, str]:
    title = " ".join(str(row.get("title", "")).split())
    summary = summarize_timeline_impact(str(row.get("raw_text", "")))
    return title, summary


def collect_news_terms(
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    return build_news_keywords(stock_name, sample_label, concept_labels)


def anchor_dates_from_waves(waves: list[dict[str, Any]]) -> list[pd.Timestamp]:
    anchors: list[pd.Timestamp] = []
    for wave in waves:
        start_date = wave.get("start_date")
        if start_date:
            anchors.append(to_naive_timestamp(start_date))
    return anchors


def news_window_from_waves(
    waves: list[dict[str, Any]],
    lookback_days: int,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    starts = [to_naive_timestamp(wave["start_date"]) for wave in waves if wave.get("start_date")]
    peaks = [to_naive_timestamp(wave["peak_date"]) for wave in waves if wave.get("peak_date")]
    if not starts or not peaks:
        return None, None
    return min(starts) - pd.Timedelta(days=lookback_days), max(peaks)


def format_news_source_distribution(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        source_id = str(row.get("source_id", "")).strip()
        if not source_id:
            continue
        counts[source_id] = counts.get(source_id, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " / ".join(f"{source}({count}条)" for source, count in ordered)


def news_distance_score(published_at: pd.Timestamp, anchors: list[pd.Timestamp]) -> tuple[int, int]:
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


def score_news_row(
    row: dict[str, Any],
    *,
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    anchors: list[pd.Timestamp],
    source_priority: dict[str, int],
) -> tuple[int, int, pd.Timestamp]:
    title = str(row.get("title", ""))
    raw_text = str(row.get("raw_text", ""))
    published_at = to_naive_timestamp(row.get("published_at"))
    source_id = str(row.get("source_id", ""))
    terms = collect_news_terms(stock_name, sample_label, concept_labels)

    score = source_priority.get(source_id, 0)
    if stock_name and stock_name in title:
        score += 80
    elif stock_name and stock_name in raw_text:
        score += 40

    title_hits = sum(1 for term in terms if term and term != stock_name and term in title)
    body_hits = sum(1 for term in terms if term and term != stock_name and term in raw_text)
    score += title_hits * 12
    score += body_hits * 4

    distance_score, distance_days = news_distance_score(published_at, anchors)
    score += distance_score
    return score, distance_days, published_at


def collect_ranked_news_candidates(
    *,
    news_evidence: list[dict[str, Any]],
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    waves: list[dict[str, Any]],
    lookback_days: int,
    source_priority: dict[str, int] | None = None,
) -> list[tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]]:
    source_priority = source_priority or DEFAULT_SOURCE_PRIORITY
    anchors = anchor_dates_from_waves(waves)
    window_start, window_end = news_window_from_waves(waves, lookback_days=lookback_days)
    ranked_candidates: list[tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]] = []
    for row in news_evidence:
        if str(row.get("source_id", "")) not in source_priority:
            continue
        published_at = to_naive_timestamp(row.get("published_at"))
        if window_start is not None and published_at < window_start:
            continue
        if window_end is not None and published_at > window_end:
            continue
        score_tuple = score_news_row(
            row,
            stock_name=stock_name,
            sample_label=sample_label,
            concept_labels=concept_labels,
            anchors=anchors,
            source_priority=source_priority,
        )
        ranked_candidates.append((score_tuple, row))
    return sorted(
        ranked_candidates,
        key=lambda item: (-item[0][0], item[0][1], item[0][2]),
    )


def select_news_evidence(
    *,
    news_evidence: list[dict[str, Any]],
    stock_name: str,
    sample_label: str,
    concept_labels: dict[str, dict[str, str]] | None,
    waves: list[dict[str, Any]],
    top_k: int,
    lookback_days: int,
    source_priority: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    ranked_candidates = collect_ranked_news_candidates(
        news_evidence=news_evidence,
        stock_name=stock_name,
        sample_label=sample_label,
        concept_labels=concept_labels,
        waves=waves,
        lookback_days=lookback_days,
        source_priority=source_priority,
    )
    best_by_key: dict[tuple[str, str], tuple[tuple[int, int, pd.Timestamp], dict[str, Any]]] = {}
    for score_tuple, row in ranked_candidates:
        key = normalize_news_key(row)
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


def collect_wave_news_rows(
    *,
    news_evidence: list[dict[str, Any]],
    waves: list[dict[str, Any]],
    lookback_days: int,
    source_priority: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    source_priority = source_priority or DEFAULT_SOURCE_PRIORITY
    window_start, window_end = news_window_from_waves(waves, lookback_days=lookback_days)
    rows: list[dict[str, Any]] = []
    for row in news_evidence:
        if str(row.get("source_id", "")) not in source_priority:
            continue
        published_at = to_naive_timestamp(row.get("published_at"))
        if window_start is not None and published_at < window_start:
            continue
        if window_end is not None and published_at > window_end:
            continue
        copied = dict(row)
        copied["published_at"] = format_display_time(published_at)
        rows.append(copied)
    return sorted(rows, key=lambda row: to_naive_timestamp(row.get("published_at")))


def candidate_title_key(row: dict[str, Any]) -> str:
    title = str(row.get("title", "")).strip()
    if title:
        return title
    key = normalize_news_key(row)
    return " | ".join(part for part in key if part).strip() or "untitled"


def build_agent_candidate_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: to_naive_timestamp(item.get("published_at"))):
        key = candidate_title_key(row)
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


def build_timeline_rows(
    news_evidence: list[dict[str, Any]],
    limit: int = DEFAULT_TIMELINE_NEWS_LIMIT,
) -> list[dict[str, str]]:
    rows = []
    ordered = sorted(news_evidence, key=lambda row: to_naive_timestamp(row.get("published_at")))
    for row in ordered[:limit]:
        rows.append(
            {
                "time": format_display_time(row.get("published_at", "")),
                "category": "本地证据",
                "event": str(row.get("title", "")),
                "impact": summarize_timeline_impact(str(row.get("raw_text", ""))),
                "source": str(row.get("source_id", "")),
            }
        )
    return rows
