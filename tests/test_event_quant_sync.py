import unittest
from contextlib import contextmanager
from unittest import mock
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from openpyxl import Workbook

from scripts.event_quant_sync import (
    DEFAULT_FEATURE_REQUESTS_PER_MIN,
    DEFAULT_MAX_WORKERS,
    DEFAULT_REGULAR_REQUESTS_PER_MIN,
    DEFAULT_ALL_CONCEPTS_JOB_NAME,
    DEFAULT_ALL_CONCEPTS_TARGET_KEY,
    DEFAULT_ALL_CONCEPT_MEMBERS_JOB_NAME,
    DEFAULT_ALL_CONCEPT_MEMBERS_TARGET_KEY,
    build_argument_parser,
    build_market_qfq_daily_frame,
    build_code_resume_plan,
    build_trade_date_resume_plan,
    build_theme_concept_matches,
    build_stock_resume_plan,
    build_sync_state_upsert_sql,
    build_upsert_sql,
    compute_resume_start_date,
    fetch_market_trade_date_bundle,
    filter_ths_index_targets,
    format_state_cursor,
    is_tushare_rate_limit_error,
    is_tushare_retryable_error,
    load_case_stock_names,
    load_latest_adj_factor_snapshot,
    load_stock_codes_from_csv,
    resolve_case_stock_codes,
    fetch_case_stock_bundle,
    call_with_rate_limit_retry,
    build_case_stock_sync_config,
    build_stock_bundle_sync_config,
    build_akshare_limit_list_frame,
    build_qfq_daily_frame,
    fetch_concept_daily_bundle,
    fetch_concept_member_bundle,
    fetch_case_stock_bundle_from_akshare,
    fetch_case_stock_concept_bundle_from_tushare,
    run_concept_daily_sync,
    run_concept_member_sync,
    run_stock_concept_bundle_sync,
    run_case_stock_bundle_sync,
    parse_state_cursor,
    requests_per_min_to_sleep_seconds,
    normalize_daily_basic,
    normalize_index_daily,
    normalize_limit_list_d,
    normalize_moneyflow,
    normalize_ana_concept_day,
    normalize_ana_stock_concept_map,
    normalize_stock_daily_qfq,
    requests_sessions_without_proxy,
    normalize_ths_concept_daily,
    normalize_ths_member,
    sync_targets,
)


class EventQuantSyncTest(unittest.TestCase):
    def test_requests_sessions_without_proxy_disables_trust_env_temporarily(self):
        default_session = requests.Session()
        self.assertTrue(default_session.trust_env)

        with requests_sessions_without_proxy():
            patched_session = requests.Session()
            self.assertFalse(patched_session.trust_env)

        restored_session = requests.Session()
        self.assertTrue(restored_session.trust_env)

    def test_build_argument_parser_does_not_use_env_for_db_dsn_defaults(self):
        parser = build_argument_parser()
        args = parser.parse_args([
            "sync-stock-file",
            "--stock-file",
            "data/sample.csv",
            "--start-date",
            "20260101",
            "--end-date",
            "20260318",
            "--job-name",
            "sync_demo",
            "--target-key",
            "demo_target",
            "--token",
            "demo-token",
        ])

        self.assertIsNone(args.db_dsn)
        self.assertIsNone(args.http_url)

    def test_build_argument_parser_supports_sync_all_stocks(self):
        parser = build_argument_parser()
        args = parser.parse_args([
            "sync-all-stocks",
            "--start-date",
            "20250101",
            "--end-date",
            "20260407",
            "--token",
            "demo-token",
        ])

        self.assertEqual(args.command, "sync-all-stocks")
        self.assertEqual(args.job_name, "sync_all_stocks_by_trade_date")
        self.assertEqual(args.target_key, "all_stocks")
        self.assertEqual(args.max_workers, DEFAULT_MAX_WORKERS)
        self.assertIsNone(args.db_dsn)
        self.assertIsNone(args.http_url)

    def test_build_argument_parser_supports_sync_all_concepts(self):
        parser = build_argument_parser()
        args = parser.parse_args([
            "sync-all-concepts",
            "--start-date",
            "20250101",
            "--end-date",
            "20260407",
            "--token",
            "demo-token",
        ])

        self.assertEqual(args.command, "sync-all-concepts")
        self.assertEqual(args.job_name, DEFAULT_ALL_CONCEPTS_JOB_NAME)
        self.assertEqual(args.target_key, DEFAULT_ALL_CONCEPTS_TARGET_KEY)
        self.assertIsNone(args.db_dsn)
        self.assertIsNone(args.http_url)

    def test_build_argument_parser_supports_sync_all_concept_members(self):
        parser = build_argument_parser()
        args = parser.parse_args([
            "sync-all-concept-members",
            "--start-date",
            "20250101",
            "--end-date",
            "20260407",
            "--token",
            "demo-token",
        ])

        self.assertEqual(args.command, "sync-all-concept-members")
        self.assertEqual(args.job_name, DEFAULT_ALL_CONCEPT_MEMBERS_JOB_NAME)
        self.assertEqual(args.target_key, DEFAULT_ALL_CONCEPT_MEMBERS_TARGET_KEY)
        self.assertIsNone(args.db_dsn)
        self.assertIsNone(args.http_url)

    def test_default_tushare_limits_follow_one_fifth_rule(self):
        self.assertEqual(DEFAULT_REGULAR_REQUESTS_PER_MIN, 100)
        self.assertEqual(DEFAULT_FEATURE_REQUESTS_PER_MIN, 60)
        self.assertEqual(DEFAULT_MAX_WORKERS, 4)

    def test_sync_targets_cover_first_phase_tables(self):
        self.assertIn("raw_stock_daily_qfq", sync_targets())
        self.assertIn("raw_index_daily", sync_targets())
        self.assertIn("raw_ths_concept_daily", sync_targets())
        self.assertIn("raw_ths_member", sync_targets())
        self.assertIn("raw_daily_basic", sync_targets())
        self.assertIn("raw_moneyflow", sync_targets())
        self.assertIn("raw_limit_list_d", sync_targets())

    def test_build_upsert_sql_uses_conflict_keys(self):
        sql = build_upsert_sql(
            "raw_stock_daily_qfq",
            ["ts_code", "trade_date", "close_qfq", "pct_chg"],
            ["ts_code", "trade_date"],
        )

        self.assertIn("INSERT INTO raw_stock_daily_qfq", sql)
        self.assertIn("ON CONFLICT (ts_code, trade_date)", sql)
        self.assertIn("DO UPDATE SET", sql)
        self.assertIn("close_qfq = EXCLUDED.close_qfq", sql)

    def test_filter_ths_index_targets_keeps_robot_related_concepts(self):
        df = pd.DataFrame(
            [
                {"ts_code": "885517.TI", "name": "机器人概念", "type": "N"},
                {"ts_code": "886069.TI", "name": "人形机器人", "type": "N"},
                {"ts_code": "884218.TI", "name": "机器人", "type": "I"},
                {"ts_code": "883300.TI", "name": "沪深300样本股", "type": "N"},
                {"ts_code": "882001.TI", "name": "安徽", "type": "R"},
            ]
        )

        result = filter_ths_index_targets(df)

        self.assertEqual(
            list(result["ts_code"]),
            ["885517.TI", "886069.TI", "884218.TI"],
        )

    def test_build_theme_concept_matches_returns_only_top_stock_hits(self):
        index_df = pd.DataFrame(
            [
                {"ts_code": "885517.TI", "name": "机器人概念", "type": "N"},
                {"ts_code": "886042.TI", "name": "存储芯片", "type": "N"},
            ]
        )

        def fetch_members(concept_code: str) -> pd.DataFrame:
            if concept_code == "885517.TI":
                return pd.DataFrame(
                    [
                        {"ts_code": "885517.TI", "con_code": "603667.SH", "con_name": "五洲新春"},
                        {"ts_code": "885517.TI", "con_code": "000001.SZ", "con_name": "平安银行"},
                    ]
                )
            return pd.DataFrame(
                [
                    {"ts_code": "886042.TI", "con_code": "688525.SH", "con_name": "佰维存储"},
                ]
            )

        member_frame, matched_concepts = build_theme_concept_matches(
            index_df=index_df,
            stock_codes=["603667.SH", "688525.SH"],
            fetch_members=fetch_members,
            mapping_asof_date="20260317",
        )

        self.assertEqual(
            member_frame[["ts_code", "con_code"]].to_dict("records"),
            [
                {"ts_code": "885517.TI", "con_code": "603667.SH"},
                {"ts_code": "886042.TI", "con_code": "688525.SH"},
            ],
        )
        self.assertEqual(
            matched_concepts.to_dict("records"),
            [
                {"concept_code": "885517.TI", "concept_name": "机器人概念"},
                {"concept_code": "886042.TI", "concept_name": "存储芯片"},
            ],
        )

    def test_normalize_ana_stock_concept_map_builds_stock_to_concept_rows(self):
        member_frame = pd.DataFrame(
            [
                {"ts_code": "885517.TI", "con_code": "603667.SH", "con_name": "五洲新春", "mapping_asof_date": "2026-03-17"},
                {"ts_code": "886042.TI", "con_code": "688525.SH", "con_name": "佰维存储", "mapping_asof_date": "2026-03-17"},
            ]
        )
        concept_meta = pd.DataFrame(
            [
                {"concept_code": "885517.TI", "concept_name": "机器人概念"},
                {"concept_code": "886042.TI", "concept_name": "存储芯片"},
            ]
        )

        result = normalize_ana_stock_concept_map(member_frame, concept_meta, updated_at="2026-03-17T12:00:00+00:00")

        self.assertEqual(
            list(result.columns),
            ["ts_code", "concept_code", "concept_name", "mapping_asof_date", "map_source", "updated_at"],
        )
        self.assertEqual(
            result[["ts_code", "concept_code", "concept_name"]].to_dict("records"),
            [
                {"ts_code": "603667.SH", "concept_code": "885517.TI", "concept_name": "机器人概念"},
                {"ts_code": "688525.SH", "concept_code": "886042.TI", "concept_name": "存储芯片"},
            ],
        )

    def test_normalize_ana_concept_day_renames_fields(self):
        raw_df = pd.DataFrame(
            [
                {
                    "ts_code": "885517.TI",
                    "trade_date": "20260306",
                    "concept_name": "机器人概念",
                    "open": 4220.055,
                    "high": 4296.122,
                    "low": 4218.024,
                    "close": 4291.007,
                    "pct_change": 1.2636,
                    "vol": 279207810.0,
                    "turnover_rate": 2.729561,
                }
            ]
        )

        result = normalize_ana_concept_day(raw_df)

        self.assertEqual(
            list(result.columns),
            ["concept_code", "trade_date", "concept_name", "close", "pct_change", "vol", "turnover_rate"],
        )
        self.assertEqual(result.iloc[0]["concept_code"], "885517.TI")

    def test_normalize_stock_daily_qfq_renames_qfq_columns(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260122",
                    "open": 88.01,
                    "high": 94.27,
                    "low": 88.00,
                    "close": 90.83,
                    "pct_chg": 1.93,
                    "vol": 604086.82,
                    "amount": 5484940.574,
                }
            ]
        )

        result = normalize_stock_daily_qfq(df)

        self.assertEqual(
            list(result.columns),
            [
                "ts_code",
                "trade_date",
                "open_qfq",
                "high_qfq",
                "low_qfq",
                "close_qfq",
                "pct_chg",
                "vol",
                "amount",
            ],
        )
        self.assertAlmostEqual(result.iloc[0]["close_qfq"], 90.83)

    def test_normalize_ths_member_adds_mapping_date(self):
        df = pd.DataFrame(
            [
                {"ts_code": "885517.TI", "con_code": "603667.SH", "con_name": "五洲新春"},
            ]
        )

        result = normalize_ths_member(df, "20260309")

        self.assertEqual(
            list(result.columns),
            ["ts_code", "con_code", "con_name", "mapping_asof_date"],
        )
        self.assertEqual(result.iloc[0]["mapping_asof_date"], "2026-03-09")

    def test_normalize_daily_basic_keeps_analysis_fields(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260122",
                    "turnover_rate": 16.4960,
                    "turnover_rate_f": 25.9165,
                    "volume_ratio": 1.04,
                    "pe": 364.0379,
                    "pb": 11.1518,
                    "total_mv": 3326220.0,
                    "circ_mv": 3326220.0,
                }
            ]
        )

        result = normalize_daily_basic(df)

        self.assertEqual(
            list(result.columns),
            [
                "ts_code",
                "trade_date",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pb",
                "total_mv",
                "circ_mv",
            ],
        )

    def test_normalize_moneyflow_keeps_main_flow_fields(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260122",
                    "buy_lg_amount": 114429.72,
                    "sell_lg_amount": 109524.23,
                    "buy_elg_amount": 36549.80,
                    "sell_elg_amount": 34308.49,
                    "net_mf_amount": -10694.45,
                }
            ]
        )

        result = normalize_moneyflow(df)

        self.assertEqual(
            list(result.columns),
            [
                "ts_code",
                "trade_date",
                "buy_lg_amount",
                "sell_lg_amount",
                "buy_elg_amount",
                "sell_elg_amount",
                "net_mf_amount",
            ],
        )

    def test_normalize_limit_list_renames_limit_status(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260119",
                    "fd_amount": 106804958.0,
                    "first_time": "105847",
                    "last_time": "135708",
                    "open_times": 4,
                    "limit": "U",
                }
            ]
        )

        result = normalize_limit_list_d(df)

        self.assertEqual(
            list(result.columns),
            [
                "ts_code",
                "trade_date",
                "fd_amount",
                "first_time",
                "last_time",
                "open_times",
                "limit_status",
            ],
        )
        self.assertEqual(result.iloc[0]["limit_status"], "U")

    def test_normalize_index_daily_keeps_market_fields(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SH",
                    "trade_date": "20260306",
                    "open": 4085.89,
                    "high": 4129.46,
                    "low": 4085.89,
                    "close": 4124.19,
                    "pct_chg": 0.3804,
                    "vol": 646765760.0,
                    "amount": 978805900.0,
                }
            ]
        )

        result = normalize_index_daily(df)

        self.assertEqual(
            list(result.columns),
            ["ts_code", "trade_date", "open", "high", "low", "close", "pct_chg", "vol", "amount"],
        )

    def test_normalize_ths_concept_daily_keeps_concept_fields(self):
        df = pd.DataFrame(
            [
                {
                    "ts_code": "885517.TI",
                    "trade_date": "20260306",
                    "open": 4220.055,
                    "high": 4296.122,
                    "low": 4218.024,
                    "close": 4291.007,
                    "pct_change": 1.2636,
                    "vol": 279207810.0,
                    "turnover_rate": 2.729561,
                }
            ]
        )

        result = normalize_ths_concept_daily(df, "机器人概念")

        self.assertEqual(
            list(result.columns),
            [
                "ts_code",
                "trade_date",
                "concept_name",
                "open",
                "high",
                "low",
                "close",
                "pct_change",
                "vol",
                "turnover_rate",
            ],
        )
        self.assertEqual(result.iloc[0]["concept_name"], "机器人概念")

    def test_parse_and_format_state_cursor_roundtrip(self):
        cursor = format_state_cursor("603667.SH", "2026-03-09")

        self.assertEqual(cursor, "603667.SH|2026-03-09")
        self.assertEqual(parse_state_cursor(cursor), ("603667.SH", "2026-03-09"))
        self.assertEqual(parse_state_cursor("2026-03-09"), (None, "2026-03-09"))

    def test_compute_resume_start_date_uses_overlap_days(self):
        self.assertEqual(
            compute_resume_start_date("2026-03-09", overlap_days=7),
            "20260302",
        )
        self.assertEqual(
            compute_resume_start_date(None, overlap_days=7),
            None,
        )

    def test_build_sync_state_upsert_sql_uses_composite_key(self):
        sql = build_sync_state_upsert_sql()

        self.assertIn("INSERT INTO sync_job_state", sql)
        self.assertIn("ON CONFLICT (job_name, target_table, target_key)", sql)
        self.assertIn("last_success_cursor = EXCLUDED.last_success_cursor", sql)

    def test_build_stock_resume_plan_without_cursor_uses_same_start(self):
        plan = build_stock_resume_plan(
            ["000001.SZ", "000002.SZ"],
            requested_start_date="20240101",
            last_cursor=None,
            overlap_days=7,
        )

        self.assertEqual(
            plan,
            [("000001.SZ", "20240101"), ("000002.SZ", "20240101")],
        )

    def test_build_stock_resume_plan_resumes_from_cursor_stock(self):
        plan = build_stock_resume_plan(
            ["000001.SZ", "000002.SZ", "000003.SZ"],
            requested_start_date="20240101",
            last_cursor="000002.SZ|2026-03-09",
            overlap_days=7,
        )

        self.assertEqual(
            plan,
            [("000002.SZ", "20260302"), ("000003.SZ", "20240101")],
        )

    def test_build_stock_resume_plan_ignores_unknown_cursor_stock(self):
        plan = build_stock_resume_plan(
            ["000001.SZ", "000002.SZ"],
            requested_start_date="20240101",
            last_cursor="999999.SZ|2026-03-09",
            overlap_days=7,
        )

        self.assertEqual(
            plan,
            [("000001.SZ", "20240101"), ("000002.SZ", "20240101")],
        )

    def test_build_code_resume_plan_skips_last_success_code(self):
        plan = build_code_resume_plan(
            ["885517.TI", "886042.TI", "886069.TI"],
            last_cursor="886042.TI|2026-04-07",
        )

        self.assertEqual(plan, ["886069.TI"])

    def test_build_trade_date_resume_plan_uses_overlap_window(self):
        trade_dates = ["20250102", "20250103", "20250106", "20250107"]

        plan = build_trade_date_resume_plan(
            trade_dates=trade_dates,
            last_cursor="2025-01-06",
            overlap_days=3,
        )

        self.assertEqual(plan, ["20250103", "20250106", "20250107"])

    def test_build_trade_date_resume_plan_without_cursor_returns_all_dates(self):
        trade_dates = ["20250102", "20250103"]

        plan = build_trade_date_resume_plan(
            trade_dates=trade_dates,
            last_cursor=None,
            overlap_days=7,
        )

        self.assertEqual(plan, trade_dates)

    def test_requests_per_min_to_sleep_seconds_is_conservative(self):
        self.assertAlmostEqual(requests_per_min_to_sleep_seconds(100), 0.6)
        self.assertAlmostEqual(requests_per_min_to_sleep_seconds(20), 3.0)

    def test_is_tushare_rate_limit_error_detects_limit_message(self):
        self.assertTrue(is_tushare_rate_limit_error(Exception("抱歉，您每分钟最多访问该接口1500次")))
        self.assertFalse(is_tushare_rate_limit_error(Exception("network timeout")))

    def test_is_tushare_retryable_error_detects_server_error(self):
        self.assertTrue(is_tushare_retryable_error(Exception("服务器内部错误，请稍后重试")))
        self.assertTrue(is_tushare_retryable_error(Exception("抱歉，您每分钟最多访问该接口1500次")))
        self.assertTrue(
            is_tushare_retryable_error(
                Exception("HTTPConnectionPool(host='lianghua.nanyangqiankun.top', port=80): Max retries exceeded")
            )
        )
        self.assertFalse(is_tushare_retryable_error(Exception("未找到股票代码")))

    def test_load_case_stock_names_reads_unique_names_from_excel(self):
        with TemporaryDirectory() as tmpdir:
            workbook_path = Path(tmpdir) / "stock.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "案例库"
            sheet.append(["case_id", "标的名称", "开始日期"])
            sheet.append(["案例001", "五洲新春", "2025-11-05"])
            sheet.append(["案例002", "信维通信", "2025-12-24"])
            sheet.append(["案例003", "五洲新春", "2026-01-01"])
            workbook.save(workbook_path)

            names = load_case_stock_names(workbook_path)

        self.assertEqual(names, ["五洲新春", "信维通信"])

    def test_load_stock_codes_from_csv_reads_unique_codes_in_order(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "top200.csv"
            pd.DataFrame(
                [
                    {"股票代码": "002969.SZ", "股票简称": "嘉美包装"},
                    {"股票代码": "688125.SH", "股票简称": "安达智能"},
                    {"股票代码": "002969.SZ", "股票简称": "嘉美包装"},
                ]
            ).to_csv(csv_path, index=False)

            codes = load_stock_codes_from_csv(csv_path)

        self.assertEqual(codes, ["002969.SZ", "688125.SH"])

    def test_resolve_case_stock_codes_raises_for_unknown_names(self):
        stock_basic_df = pd.DataFrame(
            [
                {"ts_code": "603667.SH", "name": "五洲新春"},
                {"ts_code": "300136.SZ", "name": "信维通信"},
            ]
        )

        with self.assertRaisesRegex(ValueError, "未找到股票代码"):
            resolve_case_stock_codes(["五洲新春", "未知股票"], stock_basic_df)

    def test_build_case_stock_sync_config_returns_sorted_codes(self):
        stock_basic_df = pd.DataFrame(
            [
                {"ts_code": "300136.SZ", "name": "信维通信"},
                {"ts_code": "603667.SH", "name": "五洲新春"},
            ]
        )

        config = build_case_stock_sync_config(
            stock_names=["五洲新春", "信维通信"],
            stock_basic_df=stock_basic_df,
            start_date="20240311",
        )

        self.assertEqual(config["job_name"], "sync_case_stock_bundle")
        self.assertEqual(config["target_key"], "case_stocks")
        self.assertEqual(config["start_date"], "20240311")
        self.assertEqual(config["stock_codes"], ["300136.SZ", "603667.SH"])

    def test_build_case_stock_sync_config_supports_excluding_names(self):
        stock_basic_df = pd.DataFrame(
            [
                {"ts_code": "300136.SZ", "name": "信维通信"},
                {"ts_code": "603667.SH", "name": "五洲新春"},
                {"ts_code": "002156.SZ", "name": "通富微电"},
            ]
        )

        config = build_case_stock_sync_config(
            stock_names=["五洲新春", "通富微电", "信维通信"],
            stock_basic_df=stock_basic_df,
            start_date="20240311",
            exclude_names=["通富微电"],
        )

        self.assertEqual(config["stock_names"], ["五洲新春", "信维通信"])
        self.assertEqual(config["stock_codes"], ["300136.SZ", "603667.SH"])

    def test_build_stock_bundle_sync_config_uses_direct_codes(self):
        config = build_stock_bundle_sync_config(
            stock_codes=["688125.SH", "002969.SZ", "688125.SH"],
            start_date="20250910",
            job_name="sync_wencai_top200_bundle",
            target_key="wencai_top200_20250910_20260309",
        )

        self.assertEqual(config["job_name"], "sync_wencai_top200_bundle")
        self.assertEqual(config["target_key"], "wencai_top200_20250910_20260309")
        self.assertEqual(config["start_date"], "20250910")
        self.assertEqual(config["stock_codes"], ["002969.SZ", "688125.SH"])

    def test_run_case_stock_bundle_sync_uses_resume_plan_and_updates_state(self):
        fetched = []
        persisted = []
        state_updates = []
        sleep_calls = []

        def fetch_stock_bundle(ts_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
            fetched.append((ts_code, start_date, end_date))
            payload = pd.DataFrame([{"ts_code": ts_code, "trade_date": end_date}])
            return {
                "raw_stock_daily_qfq": payload,
                "raw_daily_basic": payload,
                "raw_moneyflow": payload,
                "raw_limit_list_d": payload,
            }

        def persist_frames(ts_code: str, frames: dict[str, pd.DataFrame]) -> None:
            persisted.append((ts_code, sorted(frames)))

        def persist_sync_state(payload: dict[str, object]) -> None:
            state_updates.append(payload)

        run_case_stock_bundle_sync(
            sync_config={
                "job_name": "sync_case_stock_bundle",
                "target_key": "case_stocks",
                "start_date": "20240311",
                "stock_codes": ["000001.SZ", "000002.SZ"],
            },
            last_cursor="000001.SZ|2026-03-09",
            fetch_stock_bundle=fetch_stock_bundle,
            persist_frames=persist_frames,
            persist_sync_state=persist_sync_state,
            end_date="20260310",
            overlap_days=7,
            sleep_seconds=3.0,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(
            fetched,
            [
                ("000001.SZ", "20260302", "20260310"),
                ("000002.SZ", "20240311", "20260310"),
            ],
        )
        self.assertEqual(
            persisted,
            [
                ("000001.SZ", ["raw_daily_basic", "raw_limit_list_d", "raw_moneyflow", "raw_stock_daily_qfq"]),
                ("000002.SZ", ["raw_daily_basic", "raw_limit_list_d", "raw_moneyflow", "raw_stock_daily_qfq"]),
            ],
        )
        self.assertEqual(
            [item["last_success_cursor"] for item in state_updates],
            ["000001.SZ|2026-03-10", "000002.SZ|2026-03-10"],
        )
        self.assertEqual([item["status"] for item in state_updates], ["success", "success"])
        self.assertEqual(sleep_calls, [3.0, 3.0])

    def test_run_case_stock_bundle_sync_marks_failure_before_raising(self):
        state_updates = []

        def fetch_stock_bundle(ts_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
            raise RuntimeError("boom")

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_case_stock_bundle_sync(
                sync_config={
                    "job_name": "sync_case_stock_bundle",
                    "target_key": "case_stocks",
                    "start_date": "20240311",
                    "stock_codes": ["000001.SZ"],
                },
                last_cursor=None,
                fetch_stock_bundle=fetch_stock_bundle,
                persist_frames=lambda ts_code, frames: None,
                persist_sync_state=state_updates.append,
                end_date="20260310",
            )

        self.assertEqual(len(state_updates), 1)
        self.assertEqual(state_updates[0]["status"], "failed")
        self.assertEqual(state_updates[0]["last_success_cursor"], None)
        self.assertIn("boom", state_updates[0]["error_message"])

    def test_run_case_stock_bundle_sync_failure_keeps_latest_success_cursor(self):
        state_updates = []
        calls = []

        def fetch_stock_bundle(ts_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
            calls.append(ts_code)
            if ts_code == "000002.SZ":
                raise RuntimeError("boom")
            return {"raw_stock_daily_qfq": pd.DataFrame([{"ts_code": ts_code, "trade_date": end_date}])}

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_case_stock_bundle_sync(
                sync_config={
                    "job_name": "sync_case_stock_bundle",
                    "target_key": "case_stocks",
                    "start_date": "20240311",
                    "stock_codes": ["000001.SZ", "000002.SZ"],
                },
                last_cursor=None,
                fetch_stock_bundle=fetch_stock_bundle,
                persist_frames=lambda ts_code, frames: None,
                persist_sync_state=state_updates.append,
                end_date="20260310",
            )

        self.assertEqual(calls, ["000001.SZ", "000002.SZ"])
        self.assertEqual(state_updates[-1]["status"], "failed")
        self.assertEqual(state_updates[-1]["last_success_cursor"], "000001.SZ|2026-03-10")

    def test_run_stock_concept_bundle_sync_updates_state_with_latest_concept_cursor(self):
        persisted = []
        state_updates = []

        def fetch_concept_bundle(concept_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
            return {
                "raw_ths_member": pd.DataFrame(
                    [{"ts_code": concept_code, "con_code": "603667.SH", "con_name": "五洲新春", "mapping_asof_date": "2026-03-17"}]
                ),
                "raw_ths_concept_daily": pd.DataFrame(
                    [{"ts_code": concept_code, "trade_date": end_date, "concept_name": "机器人概念", "close": 1.0, "pct_change": 2.0, "vol": 3.0, "turnover_rate": 4.0}]
                ),
                "ana_stock_concept_map": pd.DataFrame(
                    [{"ts_code": "603667.SH", "concept_code": concept_code, "concept_name": "机器人概念", "mapping_asof_date": "2026-03-17", "map_source": "ths_member", "updated_at": "2026-03-17T12:00:00+00:00"}]
                ),
                "ana_concept_day": pd.DataFrame(
                    [{"concept_code": concept_code, "trade_date": end_date, "concept_name": "机器人概念", "close": 1.0, "pct_change": 2.0, "vol": 3.0, "turnover_rate": 4.0}]
                ),
            }

        run_stock_concept_bundle_sync(
            sync_config={
                "job_name": "sync_top400_concepts",
                "target_key": "top400",
                "start_date": "20250910",
                "concept_codes": ["885517.TI", "886042.TI"],
            },
            last_cursor="885517.TI|2026-03-09",
            fetch_concept_bundle=fetch_concept_bundle,
            persist_frames=lambda concept_code, frames: persisted.append((concept_code, sorted(frames))),
            persist_sync_state=state_updates.append,
            end_date="20260309",
        )

        self.assertEqual(
            persisted,
            [
                ("885517.TI", ["ana_concept_day", "ana_stock_concept_map", "raw_ths_concept_daily", "raw_ths_member"]),
                ("886042.TI", ["ana_concept_day", "ana_stock_concept_map", "raw_ths_concept_daily", "raw_ths_member"]),
            ],
        )
        self.assertEqual(
            [item["last_success_cursor"] for item in state_updates],
            ["885517.TI|2026-03-09", "886042.TI|2026-03-09"],
        )
        self.assertEqual([item["status"] for item in state_updates], ["success", "success"])

    def test_run_concept_member_sync_skips_last_success_cursor(self):
        persisted = []
        state_updates = []

        def fetch_concept_member(concept_code: str) -> dict[str, pd.DataFrame]:
            return {
                "raw_ths_member": pd.DataFrame(
                    [{"ts_code": concept_code, "con_code": "300476.SZ", "con_name": "胜宏科技", "mapping_asof_date": "2026-04-07"}]
                ),
                "ana_stock_concept_map": pd.DataFrame(
                    [{"ts_code": "300476.SZ", "concept_code": concept_code, "concept_name": "机器人概念", "mapping_asof_date": "2026-04-07", "map_source": "ths_member", "updated_at": "2026-04-07T12:00:00+00:00"}]
                ),
            }

        run_concept_member_sync(
            sync_config={
                "job_name": "sync_all_concept_members",
                "target_key": "all_concept_members",
                "start_date": "20260407",
                "concept_codes": ["885517.TI", "886042.TI", "886069.TI"],
            },
            last_cursor="886042.TI|2026-04-07",
            fetch_concept_member_bundle=fetch_concept_member,
            persist_frames=lambda concept_code, frames: persisted.append((concept_code, sorted(frames))),
            persist_sync_state=state_updates.append,
            end_date="20260407",
        )

        self.assertEqual(
            persisted,
            [("886069.TI", ["ana_stock_concept_map", "raw_ths_member"])],
        )
        self.assertEqual([item["last_success_cursor"] for item in state_updates], ["886069.TI|2026-04-07"])
        self.assertEqual([item["status"] for item in state_updates], ["success"])

    def test_run_concept_daily_sync_updates_state_with_latest_concept_cursor(self):
        persisted = []
        state_updates = []

        def fetch_concept_daily(concept_code: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
            return {
                "raw_ths_concept_daily": pd.DataFrame(
                    [{"ts_code": concept_code, "trade_date": end_date, "concept_name": "机器人概念", "close": 1.0, "pct_change": 2.0, "vol": 3.0, "turnover_rate": 4.0}]
                ),
                "ana_concept_day": pd.DataFrame(
                    [{"concept_code": concept_code, "trade_date": end_date, "concept_name": "机器人概念", "close": 1.0, "pct_change": 2.0, "vol": 3.0, "turnover_rate": 4.0}]
                ),
            }

        run_concept_daily_sync(
            sync_config={
                "job_name": "sync_all_concepts",
                "target_key": "all_concepts",
                "start_date": "20250101",
                "concept_codes": ["885517.TI", "886042.TI"],
            },
            last_cursor="885517.TI|2026-03-09",
            fetch_concept_daily_bundle=fetch_concept_daily,
            persist_frames=lambda concept_code, frames: persisted.append((concept_code, sorted(frames))),
            persist_sync_state=state_updates.append,
            end_date="20260407",
        )

        self.assertEqual(
            persisted,
            [
                ("885517.TI", ["ana_concept_day", "raw_ths_concept_daily"]),
                ("886042.TI", ["ana_concept_day", "raw_ths_concept_daily"]),
            ],
        )
        self.assertEqual(
            [item["last_success_cursor"] for item in state_updates],
            ["885517.TI|2026-04-07", "886042.TI|2026-04-07"],
        )
        self.assertEqual([item["status"] for item in state_updates], ["success", "success"])

    def test_fetch_case_stock_bundle_uses_daily_and_adj_factor(self):
        calls = []
        sleep_calls = []

        class FakePro:
            def daily(self, **kwargs):
                calls.append(("daily", kwargs))
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "603667.SH",
                            "trade_date": "20260309",
                            "open": 20.0,
                            "high": 22.0,
                            "low": 19.0,
                            "close": 21.0,
                            "pct_chg": 5.0,
                            "vol": 1000.0,
                            "amount": 2000.0,
                        }
                    ]
                )

            def adj_factor(self, **kwargs):
                calls.append(("adj_factor", kwargs))
                return pd.DataFrame(
                    [
                        {"ts_code": "603667.SH", "trade_date": "20260309", "adj_factor": 2.0},
                    ]
                )

            def daily_basic(self, **kwargs):
                calls.append(("daily_basic", kwargs))
                return pd.DataFrame()

            def moneyflow(self, **kwargs):
                calls.append(("moneyflow", kwargs))
                return pd.DataFrame()

            def limit_list_d(self, **kwargs):
                calls.append(("limit_list_d", kwargs))
                return pd.DataFrame()

        fake_pro = FakePro()
        result = fetch_case_stock_bundle(
            fake_pro,
            "603667.SH",
            "20240311",
            "20260309",
            per_request_sleep_seconds=1.5,
            sleeper=sleep_calls.append,
        )

        self.assertEqual([item[0] for item in calls], ["daily", "adj_factor", "daily_basic", "moneyflow", "limit_list_d"])
        self.assertAlmostEqual(result["raw_stock_daily_qfq"].iloc[0]["close_qfq"], 21.0)
        self.assertEqual(sleep_calls, [1.5, 1.5, 1.5, 1.5, 1.5])

    def test_fetch_case_stock_bundle_falls_back_to_akshare_when_tushare_fails(self):
        class FakePro:
            def daily(self, **kwargs):
                raise Exception("无效的 token")

        class FakeAk:
            def stock_zh_a_hist(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "日期": pd.Timestamp("2025-01-02"),
                            "股票代码": "601869",
                            "开盘": 29.46,
                            "收盘": 28.25,
                            "最高": 29.77,
                            "最低": 27.92,
                            "成交量": 67119,
                            "成交额": 194404400.0,
                            "振幅": 6.25,
                            "涨跌幅": -4.63,
                            "涨跌额": -1.37,
                            "换手率": 1.65,
                        },
                        {
                            "日期": pd.Timestamp("2025-01-03"),
                            "股票代码": "601869",
                            "开盘": 28.39,
                            "收盘": 30.21,
                            "最高": 30.21,
                            "最低": 27.23,
                            "成交量": 61617,
                            "成交额": 174160889.0,
                            "振幅": 10.86,
                            "涨跌幅": 10.0,
                            "涨跌额": 2.79,
                            "换手率": 1.52,
                        },
                    ]
                )

            def stock_individual_fund_flow(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "日期": pd.Timestamp("2025-01-03"),
                            "收盘价": 30.21,
                            "涨跌幅": 10.0,
                            "主力净流入-净额": 8888.0,
                            "主力净流入-净占比": 1.0,
                            "超大单净流入-净额": 4444.0,
                            "超大单净流入-净占比": 0.5,
                            "大单净流入-净额": 4444.0,
                            "大单净流入-净占比": 0.5,
                            "中单净流入-净额": 0.0,
                            "中单净流入-净占比": 0.0,
                            "小单净流入-净额": 0.0,
                            "小单净流入-净占比": 0.0,
                        }
                    ]
                )

            def stock_zt_pool_em(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "代码": "601869",
                            "封板资金": 123456.0,
                            "首次封板时间": "093000",
                            "最后封板时间": "145700",
                            "炸板次数": 1,
                        }
                    ]
                )

            def stock_zt_pool_dtgc_em(self, **kwargs):
                return pd.DataFrame()

        result = fetch_case_stock_bundle(
            FakePro(),
            "601869.SH",
            "20250101",
            "20250110",
            ak_client=FakeAk(),
        )

        self.assertEqual(result["raw_stock_daily_qfq"].iloc[0]["ts_code"], "601869.SH")
        self.assertEqual(list(result["raw_daily_basic"]["turnover_rate"]), [1.65, 1.52])
        self.assertEqual(list(result["raw_moneyflow"]["net_mf_amount"]), [8888.0])
        self.assertEqual(result["raw_limit_list_d"].iloc[0]["limit_status"], "U")
        self.assertEqual(result["raw_limit_list_d"].iloc[0]["fd_amount"], 123456.0)

    def test_fetch_case_stock_bundle_raises_when_tushare_and_akshare_both_fail(self):
        class FakePro:
            def daily(self, **kwargs):
                raise Exception("无效的 token")

        class FakeAk:
            def stock_zh_a_hist(self, **kwargs):
                raise RuntimeError("akshare boom")

        with self.assertRaisesRegex(RuntimeError, "Tushare 与 Akshare 均失败"):
            fetch_case_stock_bundle(
                FakePro(),
                "601869.SH",
                "20250101",
                "20250110",
                ak_client=FakeAk(),
            )

    def test_fetch_case_stock_bundle_from_akshare_uses_module_default_client(self):
        class FakeAk:
            def stock_zh_a_hist(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "日期": pd.Timestamp("2025-01-02"),
                            "股票代码": "601869",
                            "开盘": 29.46,
                            "收盘": 28.25,
                            "最高": 29.77,
                            "最低": 27.92,
                            "成交量": 67119,
                            "成交额": 194404400.0,
                            "振幅": 6.25,
                            "涨跌幅": -4.63,
                            "涨跌额": -1.37,
                            "换手率": 1.65,
                        }
                    ]
                )

            def stock_individual_fund_flow(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "日期": pd.Timestamp("2025-01-02"),
                            "主力净流入-净额": 1234.0,
                        }
                    ]
                )

            def stock_zt_pool_em(self, **kwargs):
                return pd.DataFrame()

            def stock_zt_pool_dtgc_em(self, **kwargs):
                return pd.DataFrame()

        with mock.patch("scripts.event_quant_sync.ak", FakeAk()):
            frames = fetch_case_stock_bundle_from_akshare("601869.SH", "20250101", "20250110")

        self.assertEqual(list(frames["raw_daily_basic"]["turnover_rate"]), [1.65])
        self.assertEqual(list(frames["raw_moneyflow"]["net_mf_amount"]), [1234.0])

    def test_fetch_case_stock_bundle_from_akshare_retries_default_env_after_no_proxy_failure(self):
        state = {"no_proxy": False, "hist_calls": 0}

        @contextmanager
        def fake_no_proxy():
            previous = state["no_proxy"]
            state["no_proxy"] = True
            try:
                yield
            finally:
                state["no_proxy"] = previous

        class FakeAk:
            def stock_zh_a_hist(self, **kwargs):
                state["hist_calls"] += 1
                if state["no_proxy"]:
                    raise RuntimeError("no proxy path failed")
                return pd.DataFrame(
                    [
                        {
                            "日期": pd.Timestamp("2025-01-02"),
                            "股票代码": "601869",
                            "开盘": 29.46,
                            "收盘": 28.25,
                            "最高": 29.77,
                            "最低": 27.92,
                            "成交量": 67119,
                            "成交额": 194404400.0,
                            "振幅": 6.25,
                            "涨跌幅": -4.63,
                            "涨跌额": -1.37,
                            "换手率": 1.65,
                        }
                    ]
                )

            def stock_individual_fund_flow(self, **kwargs):
                return pd.DataFrame([{"日期": pd.Timestamp("2025-01-02"), "主力净流入-净额": 1234.0}])

            def stock_zt_pool_em(self, **kwargs):
                return pd.DataFrame()

            def stock_zt_pool_dtgc_em(self, **kwargs):
                return pd.DataFrame()

        with mock.patch("scripts.event_quant_sync.requests_sessions_without_proxy", fake_no_proxy):
            frames = fetch_case_stock_bundle_from_akshare(
                "601869.SH",
                "20250101",
                "20250110",
                ak_client=FakeAk(),
            )

        self.assertEqual(state["hist_calls"], 2)
        self.assertEqual(len(frames["raw_stock_daily_qfq"]), 1)

    def test_fetch_case_stock_concept_bundle_from_tushare_returns_matched_concepts(self):
        class FakePro:
            def ths_index(self):
                return pd.DataFrame(
                    [
                        {"ts_code": "886001.TI", "name": "算力PCB", "type": "N"},
                        {"ts_code": "886999.TI", "name": "无关概念", "type": "N"},
                    ]
                )

            def ths_member(self, ts_code):
                if ts_code == "886001.TI":
                    return pd.DataFrame(
                        [
                            {"ts_code": "886001.TI", "con_code": "300476.SZ", "con_name": "胜宏科技"},
                        ]
                    )
                return pd.DataFrame(
                    [
                        {"ts_code": ts_code, "con_code": "000001.SZ", "con_name": "平安银行"},
                    ]
                )

            def ths_daily(self, ts_code, start_date, end_date):
                self.last_daily_args = (ts_code, start_date, end_date)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": ts_code,
                            "trade_date": "2025-01-02",
                            "open": 100.0,
                            "high": 103.0,
                            "low": 99.0,
                            "close": 101.0,
                            "pct_change": 1.0,
                            "vol": 10.0,
                            "turnover_rate": 3.2,
                        },
                        {
                            "ts_code": ts_code,
                            "trade_date": "2026-04-07",
                            "open": 130.0,
                            "high": 132.0,
                            "low": 129.0,
                            "close": 131.0,
                            "pct_change": 0.8,
                            "vol": 12.0,
                            "turnover_rate": 3.5,
                        },
                    ]
                )

        fake_pro = FakePro()
        frames = fetch_case_stock_concept_bundle_from_tushare(
            ts_code="300476.SZ",
            start_date="20250101",
            end_date="20260407",
            token="demo-token",
            http_url="http://example.com",
            pro=fake_pro,
        )

        self.assertEqual(list(frames["ana_stock_concept_map"]["concept_code"]), ["886001.TI"])
        self.assertEqual(list(frames["ana_stock_concept_map"]["map_source"]), ["ths_member"])
        self.assertEqual(list(frames["ana_concept_day"]["concept_name"].unique()), ["算力PCB"])
        self.assertEqual(fake_pro.last_daily_args, ("886001.TI", "20250101", "20260407"))

    def test_fetch_concept_daily_bundle_returns_normalized_concept_frames(self):
        sleep_calls = []

        class FakePro:
            def ths_daily(self, ts_code, start_date, end_date):
                self.last_daily_args = (ts_code, start_date, end_date)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": ts_code,
                            "trade_date": "2025-01-02",
                            "open": 100.0,
                            "high": 103.0,
                            "low": 99.0,
                            "close": 101.0,
                            "pct_change": 1.0,
                            "vol": 10.0,
                            "turnover_rate": 3.2,
                        },
                        {
                            "ts_code": ts_code,
                            "trade_date": "2026-04-07",
                            "open": 130.0,
                            "high": 132.0,
                            "low": 129.0,
                            "close": 131.0,
                            "pct_change": 0.8,
                            "vol": 12.0,
                            "turnover_rate": 3.5,
                        },
                    ]
                )

        fake_pro = FakePro()
        frames = fetch_concept_daily_bundle(
            pro=fake_pro,
            concept_code="886001.TI",
            concept_name="算力PCB",
            start_date="20250101",
            end_date="20260407",
            per_request_sleep_seconds=1.2,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(fake_pro.last_daily_args, ("886001.TI", "20250101", "20260407"))
        self.assertEqual(list(frames["raw_ths_concept_daily"]["concept_name"].unique()), ["算力PCB"])
        self.assertEqual(list(frames["ana_concept_day"]["concept_code"].unique()), ["886001.TI"])
        self.assertEqual(sleep_calls, [1.2])

    def test_fetch_concept_member_bundle_returns_full_member_frames(self):
        sleep_calls = []

        class FakePro:
            def ths_member(self, ts_code):
                self.last_member_args = ts_code
                return pd.DataFrame(
                    [
                        {"ts_code": ts_code, "con_code": "300476.SZ", "con_name": "胜宏科技"},
                        {"ts_code": ts_code, "con_code": "603667.SH", "con_name": "五洲新春"},
                    ]
                )

        fake_pro = FakePro()
        frames = fetch_concept_member_bundle(
            pro=fake_pro,
            concept_code="885517.TI",
            concept_name="机器人概念",
            mapping_asof_date="20260407",
            per_request_sleep_seconds=0.8,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(fake_pro.last_member_args, "885517.TI")
        self.assertEqual(len(frames["raw_ths_member"]), 2)
        self.assertEqual(sorted(frames["ana_stock_concept_map"]["ts_code"].tolist()), ["300476.SZ", "603667.SH"])
        self.assertEqual(list(frames["ana_stock_concept_map"]["concept_name"].unique()), ["机器人概念"])
        self.assertEqual(sleep_calls, [0.8])

    def test_build_akshare_limit_list_frame_only_uses_up_limit_pool(self):
        hist_df = pd.DataFrame(
            [
                {"日期": pd.Timestamp("2025-01-03"), "涨跌幅": 10.0},
                {"日期": pd.Timestamp("2025-01-06"), "涨跌幅": -9.8},
            ]
        )
        calls = {"dtgc": 0}

        class FakeAk:
            def stock_zt_pool_em(self, **kwargs):
                return pd.DataFrame(
                    [
                        {
                            "代码": "601869",
                            "封板资金": 123456.0,
                            "首次封板时间": "093000",
                            "最后封板时间": "145700",
                            "炸板次数": 1,
                        }
                    ]
                )

            def stock_zt_pool_dtgc_em(self, **kwargs):
                calls["dtgc"] += 1
                return pd.DataFrame()

        result = build_akshare_limit_list_frame(hist_df, "601869.SH", FakeAk())

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["limit_status"], "U")
        self.assertEqual(calls["dtgc"], 0)

    def test_call_with_rate_limit_retry_retries_after_limit_error(self):
        calls = []
        sleep_calls = []

        def flaky():
            calls.append("x")
            if len(calls) < 3:
                raise Exception("抱歉，您每分钟最多访问该接口1500次")
            return "ok"

        result = call_with_rate_limit_retry(
            flaky,
            max_attempts=3,
            sleep_seconds=5.0,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 3)
        self.assertEqual(sleep_calls, [5.0, 5.0])

    def test_call_with_rate_limit_retry_raises_non_limit_error(self):
        with self.assertRaisesRegex(RuntimeError, "boom"):
            call_with_rate_limit_retry(
                lambda: (_ for _ in ()).throw(RuntimeError("boom")),
                max_attempts=3,
                sleep_seconds=5.0,
                sleeper=lambda _: None,
            )

    def test_load_latest_adj_factor_snapshot_returns_code_to_factor_mapping(self):
        sleep_calls = []

        class FakePro:
            def adj_factor(self, **kwargs):
                self.last_kwargs = kwargs
                return pd.DataFrame(
                    [
                        {"ts_code": "000001.SZ", "trade_date": "20260407", "adj_factor": 2.5},
                        {"ts_code": "000002.SZ", "trade_date": "20260407", "adj_factor": 1.5},
                    ]
                )

        fake_pro = FakePro()
        result = load_latest_adj_factor_snapshot(
            fake_pro,
            anchor_trade_date="20260407",
            per_request_sleep_seconds=1.2,
            sleeper=sleep_calls.append,
        )

        self.assertEqual(fake_pro.last_kwargs, {"trade_date": "20260407"})
        self.assertEqual(result, {"000001.SZ": 2.5, "000002.SZ": 1.5})
        self.assertEqual(sleep_calls, [1.2])

    def test_build_market_qfq_daily_frame_uses_latest_factor_map(self):
        daily_df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20250102",
                    "open": 20.0,
                    "high": 22.0,
                    "low": 19.0,
                    "close": 21.0,
                    "pct_chg": 5.0,
                    "vol": 1000.0,
                    "amount": 2000.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20250102",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "pct_chg": 3.0,
                    "vol": 500.0,
                    "amount": 900.0,
                },
            ]
        )
        adj_factor_df = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20250102", "adj_factor": 1.0},
                {"ts_code": "000002.SZ", "trade_date": "20250102", "adj_factor": 2.0},
            ]
        )

        result = build_market_qfq_daily_frame(
            daily_df,
            adj_factor_df,
            latest_adj_factor_map={"000001.SZ": 2.0, "000002.SZ": 4.0},
        )

        self.assertEqual(list(result.columns), ["ts_code", "trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq", "pct_chg", "vol", "amount"])
        self.assertAlmostEqual(result.iloc[0]["close_qfq"], 10.5)
        self.assertAlmostEqual(result.iloc[1]["close_qfq"], 5.25)

    def test_fetch_market_trade_date_bundle_uses_trade_date_queries(self):
        calls = []
        sleep_calls = []

        class FakePro:
            def daily(self, **kwargs):
                calls.append(("daily", kwargs))
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260407",
                            "open": 20.0,
                            "high": 22.0,
                            "low": 19.0,
                            "close": 21.0,
                            "pct_chg": 5.0,
                            "vol": 1000.0,
                            "amount": 2000.0,
                        }
                    ]
                )

            def adj_factor(self, **kwargs):
                calls.append(("adj_factor", kwargs))
                return pd.DataFrame(
                    [
                        {"ts_code": "000001.SZ", "trade_date": "20260407", "adj_factor": 2.0},
                    ]
                )

            def daily_basic(self, **kwargs):
                calls.append(("daily_basic", kwargs))
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260407",
                            "turnover_rate": 1.0,
                            "turnover_rate_f": 1.1,
                            "volume_ratio": 1.2,
                            "pe": 10.0,
                            "pb": 1.5,
                            "total_mv": 100.0,
                            "circ_mv": 80.0,
                        }
                    ]
                )

            def moneyflow(self, **kwargs):
                calls.append(("moneyflow", kwargs))
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260407",
                            "buy_lg_amount": 1.0,
                            "sell_lg_amount": 2.0,
                            "buy_elg_amount": 3.0,
                            "sell_elg_amount": 4.0,
                            "net_mf_amount": 5.0,
                        }
                    ]
                )

            def limit_list_d(self, **kwargs):
                calls.append(("limit_list_d", kwargs))
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260407",
                            "fd_amount": 1.0,
                            "first_time": "093000",
                            "last_time": "145700",
                            "open_times": 0,
                            "limit": "U",
                        }
                    ]
                )

        result = fetch_market_trade_date_bundle(
            FakePro(),
            trade_date="20260407",
            latest_adj_factor_map={"000001.SZ": 2.0},
            per_request_sleep_seconds=0.5,
            sleeper=sleep_calls.append,
            max_workers=1,
        )

        self.assertEqual(
            [item[0] for item in calls],
            ["daily", "adj_factor", "daily_basic", "moneyflow", "limit_list_d"],
        )
        self.assertTrue(all(item[1] == {"trade_date": "20260407"} for item in calls))
        self.assertAlmostEqual(result["raw_stock_daily_qfq"].iloc[0]["close_qfq"], 21.0)
        self.assertEqual(result["raw_limit_list_d"].iloc[0]["limit_status"], "U")
        self.assertEqual(sleep_calls, [0.5, 0.5, 0.5, 0.5, 0.5])

    def test_build_qfq_daily_frame_uses_latest_adj_factor(self):
        daily_df = pd.DataFrame(
            [
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260309",
                    "open": 20.0,
                    "high": 22.0,
                    "low": 19.0,
                    "close": 21.0,
                    "pct_chg": 5.0,
                    "vol": 1000.0,
                    "amount": 2000.0,
                },
                {
                    "ts_code": "603667.SH",
                    "trade_date": "20260308",
                    "open": 10.0,
                    "high": 12.0,
                    "low": 9.0,
                    "close": 11.0,
                    "pct_chg": 10.0,
                    "vol": 900.0,
                    "amount": 1800.0,
                },
            ]
        )
        adj_factor_df = pd.DataFrame(
            [
                {"ts_code": "603667.SH", "trade_date": "20260309", "adj_factor": 2.0},
                {"ts_code": "603667.SH", "trade_date": "20260308", "adj_factor": 1.0},
            ]
        )

        result = build_qfq_daily_frame(daily_df, adj_factor_df)

        self.assertEqual(list(result.columns), ["ts_code", "trade_date", "open_qfq", "high_qfq", "low_qfq", "close_qfq", "pct_chg", "vol", "amount"])
        self.assertAlmostEqual(result.iloc[0]["close_qfq"], 21.0)
        self.assertAlmostEqual(result.iloc[1]["close_qfq"], 5.5)

    def test_build_qfq_daily_frame_falls_back_to_raw_daily_when_adj_factor_missing(self):
        daily_df = pd.DataFrame(
            [
                {
                    "ts_code": "605060.SH",
                    "trade_date": "20260309",
                    "open": 13.0,
                    "high": 14.0,
                    "low": 12.5,
                    "close": 13.8,
                    "pct_chg": 3.0,
                    "vol": 100.0,
                    "amount": 200.0,
                }
            ]
        )
        adj_factor_df = pd.DataFrame([{"foo": "bar"}])

        result = build_qfq_daily_frame(daily_df, adj_factor_df)

        self.assertAlmostEqual(result.iloc[0]["open_qfq"], 13.0)
        self.assertAlmostEqual(result.iloc[0]["high_qfq"], 14.0)
        self.assertAlmostEqual(result.iloc[0]["low_qfq"], 12.5)
        self.assertAlmostEqual(result.iloc[0]["close_qfq"], 13.8)


if __name__ == "__main__":
    unittest.main()
