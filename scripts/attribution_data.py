from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd


DEFAULT_EVENT_NEWS_DSN = "postgresql://postgres:postgres@localhost:5432/event_news"
DEFAULT_EVENT_QUANT_DSN = "postgresql://postgres:postgres@localhost:5432/event_quant"
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
    sources = tuple(sources or ("zsxq_zhuwang", "zsxq_damao", "wscn_live"))
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
