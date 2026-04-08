from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd


DEFAULT_EVENT_NEWS_DSN = "postgresql://postgres:postgres@localhost:5432/event_news"
DEFAULT_EVENT_QUANT_DSN = "postgresql://postgres:postgres@localhost:5432/event_quant"
DEFAULT_NEWS_SOURCES = (
    "zsxq_zhuwang",
    "zsxq_damao",
    "zsxq_saidao_touyan",
)
DEFAULT_MAX_WINDOW_EDGE_GAP_DAYS = 10
MARKET_BENCHMARKS = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
]


def postgres_bootstrap_commands() -> dict[str, str]:
    return {
        "colima_start": "colima start",
        "docker_start": (
            "docker start event-news-pg || docker run -d "
            "--name event-news-pg "
            "-e POSTGRES_USER=postgres "
            "-e POSTGRES_PASSWORD=postgres "
            "-e POSTGRES_DB=event_news "
            "-p 5432:5432 "
            "postgres:16"
        ),
        "event_news_dsn": DEFAULT_EVENT_NEWS_DSN,
        "event_quant_dsn": DEFAULT_EVENT_QUANT_DSN,
    }


def build_stock_window_bundle_queries(ts_code: str, start_date: str, end_date: str) -> dict[str, str]:
    predicate = (
        "ts_code = %(ts_code)s "
        "and trade_date between %(start_date)s and %(end_date)s "
    )
    return {
        "raw_stock_daily_qfq": (
            "select ts_code, trade_date, open_qfq, high_qfq, low_qfq, close_qfq, pct_chg, vol, amount "
            f"from raw_stock_daily_qfq where {predicate} order by trade_date"
        ),
        "raw_daily_basic": (
            "select ts_code, trade_date, turnover_rate, turnover_rate_f, volume_ratio, pe, pb, total_mv, circ_mv "
            f"from raw_daily_basic where {predicate} order by trade_date"
        ),
        "raw_moneyflow": (
            "select ts_code, trade_date, buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount, net_mf_amount "
            f"from raw_moneyflow where {predicate} order by trade_date"
        ),
        "raw_limit_list_d": (
            "select ts_code, trade_date, fd_amount, first_time, last_time, open_times, limit_status "
            f"from raw_limit_list_d where {predicate} order by trade_date"
        ),
    }


def fetch_stock_window_bundle(conn, ts_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    queries = build_stock_window_bundle_queries(ts_code, start_date, end_date)
    frames: dict[str, pd.DataFrame] = {}
    params = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
    with conn.cursor() as cur:
        for table_name, sql in queries.items():
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            frames[table_name] = pd.DataFrame(cur.fetchall(), columns=columns)
    return frames


def validate_stock_window_coverage(
    stock_bundle: Mapping[str, pd.DataFrame],
    ts_code: str,
    start_date: str,
    end_date: str,
    max_edge_gap_days: int = DEFAULT_MAX_WINDOW_EDGE_GAP_DAYS,
) -> None:
    stock_df = stock_bundle.get("raw_stock_daily_qfq")
    if stock_df is None or stock_df.empty:
        raise ValueError(f"未获取到 {ts_code} 在 {start_date} 到 {end_date} 的量价数据")

    trade_dates = pd.to_datetime(stock_df["trade_date"]).sort_values().reset_index(drop=True)
    actual_start = trade_dates.iloc[0]
    actual_end = trade_dates.iloc[-1]
    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)

    start_gap_days = (actual_start.normalize() - requested_start.normalize()).days
    end_gap_days = (requested_end.normalize() - actual_end.normalize()).days

    if start_gap_days > max_edge_gap_days or end_gap_days > max_edge_gap_days:
        raise ValueError(
            "量价数据窗口被截断："
            f"{ts_code} 请求 {start_date} -> {end_date}，"
            f"实际仅覆盖 {actual_start.date()} -> {actual_end.date()}"
        )


def fetch_stock_concept_frames(
    conn,
    ts_code: str,
    start_date: str,
    end_date: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]:
    sql = """
select m.concept_code, m.concept_name, d.trade_date, d.close
from ana_stock_concept_map m
join ana_concept_day d
  on m.concept_code = d.concept_code
where m.ts_code = %(ts_code)s
  and d.trade_date between %(start_date)s and %(end_date)s
order by m.concept_code, d.trade_date
""".strip()
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        rows = cur.fetchall()

    if not rows:
        return {}, {}

    frame = pd.DataFrame(rows, columns=["concept_code", "concept_name", "trade_date", "close"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])

    concept_frames: dict[str, pd.DataFrame] = {}
    concept_labels: dict[str, dict[str, str]] = {}
    for concept_code, group in frame.groupby("concept_code", sort=True):
        code = str(concept_code)
        concept_name = str(group["concept_name"].iloc[0])
        concept_frames[code] = group[["trade_date", "close"]].reset_index(drop=True)
        concept_labels[code] = {
            "code": code,
            "name": concept_name,
        }
    return concept_frames, concept_labels


def standardize_news_evidence_rows(
    rows: Sequence[tuple[object, object, object, object, object]],
) -> list[dict[str, object]]:
    standardized: list[dict[str, object]] = []
    for published_at, source_id, title, summary, url in rows:
        standardized.append(
            {
                "published_at": published_at,
                "source_id": source_id,
                "title": title,
                "raw_text": summary,
                "url": url,
            }
        )
    return standardized


def fetch_news_evidence(
    conn,
    start_date: str,
    end_date: str,
    keywords: Sequence[str],
    sources: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    sources = tuple(sources or DEFAULT_NEWS_SOURCES)
    sql = """
select published_at, source_id, title, coalesce(summary, '') as summary, url
from event_metadata
where published_at::date between %(start_date)s and %(end_date)s
  and source_id = any(%(sources)s)
order by published_at asc
""".strip()
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "start_date": start_date,
                "end_date": end_date,
                "sources": list(sources),
            },
        )
        rows = cur.fetchall()

    lowered_keywords = [str(keyword) for keyword in keywords]
    filtered = []
    for row in rows:
        text = f"{row[2]}\n{row[3]}"
        if any(keyword in text for keyword in lowered_keywords):
            filtered.append(row)
    return standardize_news_evidence_rows(filtered)


def build_validation_table(
    stock_df: pd.DataFrame,
    benchmark_frames: Mapping[str, pd.DataFrame],
    labels: Mapping[str, Mapping[str, str]] | None = None,
    top_n: int = 5,
) -> pd.DataFrame:
    ordered_stock = stock_df.copy()
    ordered_stock["trade_date"] = pd.to_datetime(ordered_stock["trade_date"])
    ordered_stock = ordered_stock.sort_values("trade_date").reset_index(drop=True)
    ordered_stock["ret"] = ordered_stock["close_qfq"].astype(float).pct_change()

    rows: list[dict[str, object]] = []
    for key, benchmark_df in benchmark_frames.items():
        ordered_benchmark = benchmark_df.copy()
        ordered_benchmark["trade_date"] = pd.to_datetime(ordered_benchmark["trade_date"])
        ordered_benchmark = ordered_benchmark.sort_values("trade_date").reset_index(drop=True)
        ordered_benchmark["ret"] = ordered_benchmark["close"].astype(float).pct_change()
        merged = ordered_stock.merge(
            ordered_benchmark[["trade_date", "close", "ret"]],
            on="trade_date",
            how="inner",
            suffixes=("_stock", "_benchmark"),
        )
        if len(merged) < 2:
            continue
        label = (labels or {}).get(key, {})
        rows.append(
            {
                "key": key,
                "code": label.get("code", key),
                "name": label.get("name", key),
                "period_return_pct": (float(merged.iloc[-1]["close"]) / float(merged.iloc[0]["close"]) - 1.0) * 100.0,
                "close_corr": float(merged["close_qfq"].corr(merged["close"])),
                "ret_corr": float(merged["ret_stock"].corr(merged["ret_benchmark"])),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(["close_corr", "ret_corr"], ascending=[False, False]).head(top_n).reset_index(drop=True)
