from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Iterable, Sequence

import akshare as ak
import psycopg

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/event_news"
SCHEMA = "market_timeseries"
VALID_KINDS = {"industry", "concept", "sector"}


def _to_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_ak_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _calc_period_change(rows: Sequence[dict] | object) -> float | None:
    """Return pct change (%) from first close to last close.

    Accepts a sequence of dict rows with key "收盘价" or a pandas DataFrame-like object.
    """
    if rows is None:
        return None

    try:
        if hasattr(rows, "empty"):
            if rows.empty or len(rows) < 2:
                return None
            start = float(rows.iloc[0]["收盘价"])
            end = float(rows.iloc[-1]["收盘价"])
        else:
            if len(rows) < 2:
                return None
            start = float(rows[0]["收盘价"])
            end = float(rows[-1]["收盘价"])
    except Exception:
        return None

    if start == 0:
        return None
    return (end / start - 1.0) * 100.0


def _split_cached_missing(all_names: Sequence[str], cached_pct: dict[str, float]) -> tuple[list[str], list[str]]:
    cached_names = [name for name in all_names if name in cached_pct]
    missing_names = [name for name in all_names if name not in cached_pct]
    return cached_names, missing_names


def _ensure_tables(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA};
            CREATE TABLE IF NOT EXISTS {SCHEMA}.board_daily_bar (
                id BIGSERIAL PRIMARY KEY,
                provider TEXT NOT NULL DEFAULT 'akshare',
                board_kind TEXT NOT NULL,
                board_code TEXT,
                board_name TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open NUMERIC(20,6),
                high NUMERIC(20,6),
                low NUMERIC(20,6),
                close NUMERIC(20,6) NOT NULL,
                volume NUMERIC(28,4),
                amount NUMERIC(28,4),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_board_kind CHECK (board_kind IN ('industry','concept','sector')),
                CONSTRAINT uq_board_daily UNIQUE (provider, board_kind, board_name, trade_date)
            );

            CREATE TABLE IF NOT EXISTS {SCHEMA}.board_top_snapshot (
                id BIGSERIAL PRIMARY KEY,
                provider TEXT NOT NULL DEFAULT 'akshare',
                board_kind TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                top_n INTEGER NOT NULL,
                rank_no INTEGER NOT NULL,
                board_name TEXT NOT NULL,
                pct_change NUMERIC(12,6) NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_top_board_kind CHECK (board_kind IN ('industry','concept','sector')),
                CONSTRAINT ck_top_n_positive CHECK (top_n > 0),
                CONSTRAINT ck_rank_no_positive CHECK (rank_no > 0),
                CONSTRAINT ck_date_range CHECK (end_date >= start_date),
                CONSTRAINT uq_top_snapshot UNIQUE (provider, board_kind, start_date, end_date, top_n, rank_no)
            );
            """
        )


def _load_snapshot(
    conn: psycopg.Connection,
    kind: str,
    start_date: date,
    end_date: date,
    top_n: int,
    provider: str,
) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT rank_no, board_name, pct_change
        FROM {SCHEMA}.board_top_snapshot
        WHERE provider=%s AND board_kind=%s AND start_date=%s AND end_date=%s AND top_n=%s
        ORDER BY rank_no
        """,
        (provider, kind, start_date, end_date, top_n),
    ).fetchall()
    return [
        {"rank": int(r[0]), "name": str(r[1]), "pct_change": float(r[2])}
        for r in rows
    ]


def _load_cached_pct_from_daily(
    conn: psycopg.Connection,
    kind: str,
    start_date: date,
    end_date: date,
    provider: str,
) -> dict[str, float]:
    rows = conn.execute(
        f"""
        WITH rng AS (
          SELECT
            board_name,
            trade_date,
            close,
            ROW_NUMBER() OVER (PARTITION BY board_name ORDER BY trade_date ASC) AS rn_asc,
            ROW_NUMBER() OVER (PARTITION BY board_name ORDER BY trade_date DESC) AS rn_desc
          FROM {SCHEMA}.board_daily_bar
          WHERE provider=%s AND board_kind=%s AND trade_date BETWEEN %s AND %s
        )
        SELECT
          board_name,
          MAX(CASE WHEN rn_asc=1 THEN close END) AS first_close,
          MAX(CASE WHEN rn_desc=1 THEN close END) AS last_close,
          COUNT(*) AS bar_count
        FROM rng
        GROUP BY board_name
        HAVING COUNT(*) >= 2
        """,
        (provider, kind, start_date, end_date),
    ).fetchall()
    out: dict[str, float] = {}
    for name, first_close, last_close, _ in rows:
        try:
            first_v = float(first_close)
            last_v = float(last_close)
            if first_v == 0:
                continue
            out[str(name)] = (last_v / first_v - 1.0) * 100.0
        except Exception:
            continue
    return out


def _save_snapshot(
    conn: psycopg.Connection,
    kind: str,
    start_date: date,
    end_date: date,
    top_n: int,
    provider: str,
    top_rows: Sequence[dict],
) -> None:
    with conn.transaction():
        conn.execute(
            f"""
            DELETE FROM {SCHEMA}.board_top_snapshot
            WHERE provider=%s AND board_kind=%s AND start_date=%s AND end_date=%s AND top_n=%s
            """,
            (provider, kind, start_date, end_date, top_n),
        )
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {SCHEMA}.board_top_snapshot(
                    provider, board_kind, start_date, end_date, top_n, rank_no, board_name, pct_change
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        provider,
                        kind,
                        start_date,
                        end_date,
                        top_n,
                        int(x["rank"]),
                        str(x["name"]),
                        float(x["pct_change"]),
                    )
                    for x in top_rows
                ],
            )


def _save_daily_rows(
    conn: psycopg.Connection,
    kind: str,
    board_name: str,
    provider: str,
    hist,
) -> None:
    if hist is None or hist.empty:
        return
    rows = []
    for _, row in hist.iterrows():
        rows.append(
            (
                provider,
                kind,
                board_name,
                str(row["日期"])[:10],
                float(row.get("开盘价", 0) or 0),
                float(row.get("最高价", 0) or 0),
                float(row.get("最低价", 0) or 0),
                float(row["收盘价"]),
                float(row.get("成交量", 0) or 0),
                float(row.get("成交额", 0) or 0),
            )
        )
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {SCHEMA}.board_daily_bar(
                    provider, board_kind, board_name, trade_date,
                    open, high, low, close, volume, amount
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (provider, board_kind, board_name, trade_date)
                DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    volume=EXCLUDED.volume,
                    amount=EXCLUDED.amount,
                    updated_at=NOW()
                """,
                rows,
            )


def _get_board_names(kind: str) -> list[str]:
    if kind == "industry":
        return ak.stock_board_industry_name_ths()["name"].tolist()
    if kind == "concept":
        return ak.stock_board_concept_name_ths()["name"].tolist()
    if kind == "sector":
        try:
            return ak.stock_board_industry_name_em()["板块名称"].tolist()
        except Exception:
            return ak.stock_board_industry_name_ths()["name"].tolist()
    raise ValueError(f"unsupported kind: {kind}")


def _fetch_history(kind: str, board_name: str, start_ak: str, end_ak: str):
    if kind == "industry":
        return ak.stock_board_industry_index_ths(symbol=board_name, start_date=start_ak, end_date=end_ak)
    if kind == "concept":
        return ak.stock_board_concept_index_ths(symbol=board_name, start_date=start_ak, end_date=end_ak)
    if kind == "sector":
        try:
            return ak.stock_board_industry_hist_em(
                symbol=board_name,
                start_date=start_ak,
                end_date=end_ak,
                period="日k",
                adjust="",
            )
        except Exception:
            return ak.stock_board_industry_index_ths(symbol=board_name, start_date=start_ak, end_date=end_ak)
    raise ValueError(f"unsupported kind: {kind}")


def _compute_and_cache_kind(
    conn: psycopg.Connection,
    kind: str,
    start_date: date,
    end_date: date,
    top_n: int,
    provider: str,
    max_workers: int,
) -> list[dict]:
    names = _get_board_names(kind)
    start_ak = _to_ak_date(start_date)
    end_ak = _to_ak_date(end_date)
    stats: list[dict] = []

    # Step 1: use local daily bars first to avoid unnecessary upstream calls.
    cached_pct = _load_cached_pct_from_daily(conn, kind, start_date, end_date, provider)
    cached_names, missing_names = _split_cached_missing(names, cached_pct)
    for board_name in cached_names:
        stats.append({"name": board_name, "pct_change": float(cached_pct[board_name])})

    def _one(board_name: str):
        try:
            hist = _fetch_history(kind, board_name, start_ak, end_ak)
            pct = _calc_period_change(hist)
            if pct is None:
                return None
            return board_name, pct, hist
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_one, name) for name in missing_names]
        for fut in as_completed(futures):
            res = fut.result()
            if not res:
                continue
            board_name, pct, hist = res
            stats.append({"name": board_name, "pct_change": float(pct)})
            _save_daily_rows(conn, kind, board_name, provider, hist)

    stats.sort(key=lambda x: x["pct_change"], reverse=True)
    top_rows = [
        {"rank": i + 1, "name": x["name"], "pct_change": x["pct_change"]}
        for i, x in enumerate(stats[:top_n])
    ]
    _save_snapshot(conn, kind, start_date, end_date, top_n, provider, top_rows)
    return top_rows


def get_top(
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    top_n: int = 5,
    kinds: Iterable[str] = ("industry", "sector", "concept"),
    dsn: str = DEFAULT_DSN,
    provider: str = "akshare",
    max_workers: int = 8,
) -> dict[str, list[dict]]:
    start = _to_date(start_date)
    end = _to_date(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")
    if top_n <= 0:
        raise ValueError("top_n must be > 0")

    normalized = []
    for kind in kinds:
        if kind not in VALID_KINDS:
            raise ValueError(f"unsupported kind: {kind}")
        normalized.append(kind)

    output: dict[str, list[dict]] = {}
    with psycopg.connect(dsn) as conn:
        _ensure_tables(conn)
        for kind in normalized:
            cached = _load_snapshot(conn, kind, start, end, top_n, provider)
            if len(cached) >= top_n:
                output[kind] = cached
                continue
            output[kind] = _compute_and_cache_kind(
                conn=conn,
                kind=kind,
                start_date=start,
                end_date=end,
                top_n=top_n,
                provider=provider,
                max_workers=max_workers,
            )
    return output


def _main() -> None:
    parser = argparse.ArgumentParser(description="Get top boards by date range with PG cache")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--kinds", default="industry,sector,concept")
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    args = parser.parse_args()

    kinds = [x.strip() for x in args.kinds.split(",") if x.strip()]
    result = get_top(
        start_date=args.start_date,
        end_date=args.end_date,
        top_n=args.top_n,
        kinds=kinds,
        dsn=args.dsn,
    )
    for kind, rows in result.items():
        print(f"\n[{kind}] {args.start_date} ~ {args.end_date}")
        for row in rows:
            print(f"{row['rank']:>2}. {row['name']}  {row['pct_change']:.2f}%")


if __name__ == "__main__":
    _main()
