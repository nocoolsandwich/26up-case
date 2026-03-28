from __future__ import annotations

from collections import OrderedDict

DATABASE_NAME = "event_quant"
DEFAULT_MARKET_INDEX_CODES = ("000001.SH", "399001.SZ", "399006.SZ")


def get_table_specs() -> OrderedDict[str, dict[str, object]]:
    return OrderedDict(
        [
            (
                "raw_stock_daily_qfq",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "raw_index_daily",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "raw_ths_concept_daily",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "raw_ths_member",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "con_code", "mapping_asof_date"),
                },
            ),
            (
                "raw_daily_basic",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "raw_moneyflow",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "raw_limit_list_d",
                {
                    "kind": "raw",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "ana_stock_day",
                {
                    "kind": "analysis",
                    "primary_key": ("ts_code", "trade_date"),
                },
            ),
            (
                "ana_concept_day",
                {
                    "kind": "analysis",
                    "primary_key": ("concept_code", "trade_date"),
                },
            ),
            (
                "ana_stock_concept_map",
                {
                    "kind": "analysis",
                    "primary_key": ("ts_code", "concept_code"),
                },
            ),
            (
                "ana_market_day",
                {
                    "kind": "analysis",
                    "primary_key": ("trade_date",),
                },
            ),
        ]
    )


def build_create_database_sql(database_name: str = DATABASE_NAME) -> str:
    return f"""
SELECT 'CREATE DATABASE {database_name}'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = '{database_name}'
);
""".strip()


def build_bootstrap_statements(database_name: str = DATABASE_NAME) -> tuple[str, str, str]:
    return (
        build_create_database_sql(database_name),
        build_schema_sql(),
        get_analysis_view_sql(),
    )


def build_schema_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS raw_stock_daily_qfq (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open_qfq NUMERIC(20,6),
    high_qfq NUMERIC(20,6),
    low_qfq NUMERIC(20,6),
    close_qfq NUMERIC(20,6),
    pct_chg NUMERIC(20,6),
    vol NUMERIC(28,4),
    amount NUMERIC(28,4),
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_index_daily (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(20,6),
    high NUMERIC(20,6),
    low NUMERIC(20,6),
    close NUMERIC(20,6),
    pct_chg NUMERIC(20,6),
    vol NUMERIC(28,4),
    amount NUMERIC(28,4),
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_ths_concept_daily (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    concept_name TEXT NOT NULL,
    open NUMERIC(20,6),
    high NUMERIC(20,6),
    low NUMERIC(20,6),
    close NUMERIC(20,6),
    pct_change NUMERIC(20,6),
    vol NUMERIC(28,4),
    turnover_rate NUMERIC(20,6),
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_ths_member (
    ts_code TEXT NOT NULL,
    con_code TEXT NOT NULL,
    con_name TEXT NOT NULL,
    mapping_asof_date DATE NOT NULL,
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, con_code, mapping_asof_date)
);

CREATE TABLE IF NOT EXISTS raw_daily_basic (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    turnover_rate NUMERIC(20,6),
    turnover_rate_f NUMERIC(20,6),
    volume_ratio NUMERIC(20,6),
    pe NUMERIC(20,6),
    pb NUMERIC(20,6),
    total_mv NUMERIC(28,4),
    circ_mv NUMERIC(28,4),
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_moneyflow (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    buy_lg_amount NUMERIC(28,4),
    sell_lg_amount NUMERIC(28,4),
    buy_elg_amount NUMERIC(28,4),
    sell_elg_amount NUMERIC(28,4),
    net_mf_amount NUMERIC(28,4),
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_limit_list_d (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    fd_amount NUMERIC(28,4),
    first_time TEXT,
    last_time TEXT,
    open_times INTEGER,
    limit_status TEXT,
    source TEXT NOT NULL DEFAULT 'tushare',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS ana_stock_day (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    close_qfq NUMERIC(20,6),
    pct_chg NUMERIC(20,6),
    vol NUMERIC(28,4),
    amount NUMERIC(28,4),
    turnover_rate NUMERIC(20,6),
    float_mv NUMERIC(28,4),
    total_mv NUMERIC(28,4),
    main_net_mf NUMERIC(28,4),
    is_limit_up BOOLEAN NOT NULL DEFAULT FALSE,
    is_limit_down BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE TABLE IF NOT EXISTS ana_concept_day (
    concept_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    concept_name TEXT NOT NULL,
    close NUMERIC(20,6),
    pct_change NUMERIC(20,6),
    vol NUMERIC(28,4),
    turnover_rate NUMERIC(20,6),
    PRIMARY KEY (concept_code, trade_date)
);

CREATE TABLE IF NOT EXISTS ana_stock_concept_map (
    ts_code TEXT NOT NULL,
    concept_code TEXT NOT NULL,
    concept_name TEXT NOT NULL,
    mapping_asof_date DATE NOT NULL,
    map_source TEXT NOT NULL DEFAULT 'ths_member',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, concept_code)
);

CREATE TABLE IF NOT EXISTS ana_market_day (
    trade_date DATE PRIMARY KEY,
    sh_close NUMERIC(20,6),
    sh_pct NUMERIC(20,6),
    sz_close NUMERIC(20,6),
    sz_pct NUMERIC(20,6),
    cyb_close NUMERIC(20,6),
    cyb_pct NUMERIC(20,6)
);

CREATE TABLE IF NOT EXISTS sync_job_state (
    job_name TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_key TEXT NOT NULL,
    last_success_cursor TEXT,
    last_success_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_name, target_table, target_key)
);
""".strip()


def get_analysis_view_sql() -> str:
    return """
CREATE OR REPLACE VIEW vw_case_attribution_base AS
SELECT
    s.ts_code,
    s.trade_date,
    s.close_qfq,
    s.pct_chg,
    s.turnover_rate,
    s.float_mv,
    s.total_mv,
    s.main_net_mf,
    s.is_limit_up,
    m.concept_code,
    m.concept_name,
    c.close AS concept_close,
    c.pct_change AS concept_pct_change,
    md.sh_close,
    md.sh_pct,
    md.sz_close,
    md.sz_pct,
    md.cyb_close,
    md.cyb_pct
FROM ana_stock_day s
LEFT JOIN ana_stock_concept_map m
    ON s.ts_code = m.ts_code
LEFT JOIN ana_concept_day c
    ON m.concept_code = c.concept_code
   AND s.trade_date = c.trade_date
LEFT JOIN ana_market_day md
    ON s.trade_date = md.trade_date;
""".strip()
