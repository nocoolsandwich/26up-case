import unittest

from scripts.event_quant_bootstrap import (
    DATABASE_NAME,
    DEFAULT_MARKET_INDEX_CODES,
    build_bootstrap_statements,
    build_create_database_sql,
    build_schema_sql,
    get_analysis_view_sql,
    get_table_specs,
)


class EventQuantBootstrapTest(unittest.TestCase):
    def test_table_specs_include_first_phase_tables(self):
        specs = get_table_specs()

        self.assertIn("raw_stock_daily_qfq", specs)
        self.assertIn("raw_index_daily", specs)
        self.assertIn("raw_ths_concept_daily", specs)
        self.assertIn("raw_ths_member", specs)
        self.assertIn("raw_daily_basic", specs)
        self.assertIn("raw_moneyflow", specs)
        self.assertIn("raw_limit_list_d", specs)
        self.assertIn("ana_stock_day", specs)
        self.assertIn("ana_concept_day", specs)
        self.assertIn("ana_stock_concept_map", specs)
        self.assertIn("ana_market_day", specs)

    def test_database_name_defaults_to_event_quant(self):
        self.assertEqual(DATABASE_NAME, "event_quant")
        self.assertEqual(DEFAULT_MARKET_INDEX_CODES, ("000001.SH", "399001.SZ", "399006.SZ"))

    def test_schema_sql_contains_primary_keys_and_mapping_date(self):
        ddl = build_schema_sql()

        self.assertIn("CREATE TABLE IF NOT EXISTS raw_stock_daily_qfq", ddl)
        self.assertIn("PRIMARY KEY (ts_code, trade_date)", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS raw_ths_member", ddl)
        self.assertIn("mapping_asof_date", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS ana_stock_day", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS ana_market_day", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS sync_job_state", ddl)

    def test_schema_sql_avoids_reserved_limit_column_name(self):
        ddl = build_schema_sql()

        self.assertIn("limit_status TEXT", ddl)
        self.assertNotIn("\n    limit TEXT", ddl)

    def test_create_database_sql_is_idempotent(self):
        sql = build_create_database_sql("event_quant")

        self.assertIn("SELECT 'CREATE DATABASE event_quant'", sql)
        self.assertIn("WHERE NOT EXISTS", sql)

    def test_case_attribution_view_sql_contains_three_layers(self):
        view_sql = get_analysis_view_sql()

        self.assertIn("CREATE OR REPLACE VIEW vw_case_attribution_base", view_sql)
        self.assertIn("FROM ana_stock_day s", view_sql)
        self.assertIn("LEFT JOIN ana_stock_concept_map m", view_sql)
        self.assertIn("LEFT JOIN ana_concept_day c", view_sql)
        self.assertIn("LEFT JOIN ana_market_day md", view_sql)

    def test_case_attribution_view_sql_exposes_market_columns(self):
        view_sql = get_analysis_view_sql()

        self.assertIn("md.sh_close", view_sql)
        self.assertIn("md.sh_pct", view_sql)
        self.assertIn("md.sz_close", view_sql)
        self.assertIn("md.sz_pct", view_sql)
        self.assertIn("md.cyb_close", view_sql)
        self.assertIn("md.cyb_pct", view_sql)

    def test_bootstrap_statements_include_db_schema_and_view(self):
        statements = build_bootstrap_statements()

        self.assertEqual(len(statements), 3)
        self.assertIn("CREATE DATABASE event_quant", statements[0])
        self.assertIn("CREATE TABLE IF NOT EXISTS raw_stock_daily_qfq", statements[1])
        self.assertIn("CREATE OR REPLACE VIEW vw_case_attribution_base", statements[2])

    def test_sync_job_state_schema_contains_cursor_and_status(self):
        ddl = build_schema_sql()

        self.assertIn("sync_job_state", ddl)
        self.assertIn("last_success_cursor TEXT", ddl)
        self.assertIn("status TEXT NOT NULL", ddl)
        self.assertIn("error_message TEXT", ddl)


if __name__ == "__main__":
    unittest.main()
