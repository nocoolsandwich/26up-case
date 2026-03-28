import unittest

import pandas as pd

from scripts.attribution_data import (
    DEFAULT_EVENT_NEWS_DSN,
    DEFAULT_EVENT_QUANT_DSN,
    MARKET_BENCHMARKS,
    build_stock_window_bundle_queries,
    build_validation_table,
    fetch_news_evidence,
    fetch_stock_window_bundle,
    postgres_bootstrap_commands,
    standardize_news_evidence_rows,
)


class AttributionDataTest(unittest.TestCase):
    class _FakeCursor:
        def __init__(self, scripted_results):
            self._scripted_results = scripted_results
            self._index = 0
            self.description = []

        def execute(self, sql, params=None):
            columns, rows = self._scripted_results[self._index]
            self.description = [(col,) for col in columns]
            self._rows = rows
            self._index += 1

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConnection:
        def __init__(self, scripted_results):
            self._scripted_results = scripted_results

        def cursor(self):
            return AttributionDataTest._FakeCursor(self._scripted_results)

    def test_postgres_bootstrap_commands_follow_project_convention(self):
        commands = postgres_bootstrap_commands()

        self.assertEqual(commands["colima_start"], "colima start")
        self.assertIn("docker start event-news-pg", commands["docker_start"])
        self.assertEqual(commands["event_news_dsn"], DEFAULT_EVENT_NEWS_DSN)
        self.assertEqual(commands["event_quant_dsn"], DEFAULT_EVENT_QUANT_DSN)

    def test_market_benchmarks_include_shanghai_shenzhen_and_chinext(self):
        self.assertEqual(
            MARKET_BENCHMARKS,
            [
                ("000001.SH", "上证指数"),
                ("399001.SZ", "深证成指"),
                ("399006.SZ", "创业板指"),
            ],
        )

    def test_build_stock_window_bundle_queries_cover_core_tables(self):
        queries = build_stock_window_bundle_queries("603667.SH", "2025-11-05", "2026-01-22")

        self.assertEqual(
            sorted(queries.keys()),
            ["raw_daily_basic", "raw_limit_list_d", "raw_moneyflow", "raw_stock_daily_qfq"],
        )
        self.assertIn("from raw_stock_daily_qfq", queries["raw_stock_daily_qfq"].lower())
        self.assertIn("ts_code = %(ts_code)s", queries["raw_stock_daily_qfq"])

    def test_build_stock_window_bundle_queries_keep_order_by_active(self):
        queries = build_stock_window_bundle_queries("688125.SH", "2025-09-10", "2026-03-09")

        sql = queries["raw_stock_daily_qfq"].lower()

        self.assertIn("order by trade_date", sql)
        self.assertNotIn("-- params:", sql)

    def test_standardize_news_evidence_rows_keeps_full_text(self):
        rows = [
            (
                "2025-11-05T19:37:22+08:00",
                "zsxq_zhuwang",
                "小鹏科技日",
                "这里是完整摘要，不应该被截断。",
                "https://example.com/1",
            )
        ]

        result = standardize_news_evidence_rows(rows)

        self.assertEqual(result[0]["title"], "小鹏科技日")
        self.assertEqual(result[0]["raw_text"], "这里是完整摘要，不应该被截断。")
        self.assertEqual(result[0]["url"], "https://example.com/1")

    def test_fetch_stock_window_bundle_returns_frames_by_table_name(self):
        conn = self._FakeConnection(
            [
                (
                    ["ts_code", "trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq", "pct_chg", "vol", "amount"],
                    [("603667.SH", "2025-11-05", 10, 11, 9, 10.5, 5.0, 100, 200)],
                ),
                (
                    ["ts_code", "trade_date", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pb", "total_mv", "circ_mv"],
                    [("603667.SH", "2025-11-05", 1, 2, 3, 4, 5, 6, 7)],
                ),
                (
                    ["ts_code", "trade_date", "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount", "net_mf_amount"],
                    [("603667.SH", "2025-11-05", 1, 2, 3, 4, 5)],
                ),
                (
                    ["ts_code", "trade_date", "fd_amount", "first_time", "last_time", "open_times", "limit_status"],
                    [("603667.SH", "2025-11-05", 1, "093000", "145700", 0, "U")],
                ),
            ]
        )

        result = fetch_stock_window_bundle(conn, "603667.SH", "2025-11-05", "2026-01-22")

        self.assertEqual(sorted(result.keys()), ["raw_daily_basic", "raw_limit_list_d", "raw_moneyflow", "raw_stock_daily_qfq"])
        self.assertEqual(list(result["raw_stock_daily_qfq"].columns), ["ts_code", "trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq", "pct_chg", "vol", "amount"])

    def test_fetch_news_evidence_returns_standardized_rows(self):
        conn = self._FakeConnection(
            [
                (
                    ["published_at", "source_id", "title", "summary", "url"],
                    [("2025-11-05T19:37:22+08:00", "zsxq_zhuwang", "小鹏机器人科技日", "完整摘要", "https://example.com/1")],
                )
            ]
        )

        result = fetch_news_evidence(
            conn,
            start_date="2025-11-05",
            end_date="2025-11-06",
            keywords=["五洲新春", "机器人"],
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "小鹏机器人科技日")
        self.assertEqual(result[0]["raw_text"], "完整摘要")

    def test_build_validation_table_returns_top5_sorted_by_close_corr(self):
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-01-01", "close_qfq": 10.0},
                {"trade_date": "2025-01-02", "close_qfq": 11.0},
                {"trade_date": "2025-01-03", "close_qfq": 12.0},
                {"trade_date": "2025-01-04", "close_qfq": 11.5},
                {"trade_date": "2025-01-05", "close_qfq": 13.0},
            ]
        )
        benchmark_frames = {
            "A": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [1, 2, 3, 2.5, 4]}),
            "B": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [4, 3, 2, 2.5, 1]}),
            "C": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [1, 1.2, 1.3, 1.4, 1.5]}),
            "D": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [2, 2.1, 2.2, 2.1, 2.3]}),
            "E": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [5, 5.1, 5.2, 5.0, 5.3]}),
            "F": pd.DataFrame({"trade_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"], "close": [1, 0.9, 0.8, 0.85, 0.7]}),
        }
        labels = {
            "A": {"code": "A.TI", "name": "概念A"},
            "B": {"code": "B.TI", "name": "概念B"},
            "C": {"code": "C.TI", "name": "概念C"},
            "D": {"code": "D.TI", "name": "概念D"},
            "E": {"code": "E.TI", "name": "概念E"},
            "F": {"code": "F.TI", "name": "概念F"},
        }

        result = build_validation_table(stock_df, benchmark_frames, labels=labels, top_n=5)

        self.assertEqual(len(result), 5)
        self.assertEqual(result.iloc[0]["name"], "概念A")
        self.assertIn("period_return_pct", result.columns)
        self.assertIn("close_corr", result.columns)
        self.assertIn("ret_corr", result.columns)


if __name__ == "__main__":
    unittest.main()
