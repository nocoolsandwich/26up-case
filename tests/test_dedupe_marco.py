import unittest

import pandas as pd

from scripts.dedupe_marco import dedupe_marco_df, score_row


class DedupeMarcoTest(unittest.TestCase):
    def test_score_prefers_complete_numeric_fields(self):
        row_low = pd.Series(
            {
                "政策/事件日期": pd.Timestamp("2025-04-03"),
                "A股定价日T0": pd.Timestamp("2025-07-07"),
                "结构事实一句话（尽量客观）": "说明较长",
                "来源详情（写清政策+市场数据）": "来源较长",
                "T0上证涨跌幅": None,
                "T0深成涨跌幅": None,
                "T0创业板涨跌幅": None,
                "T0成交额（亿元）": None,
            }
        )
        row_high = pd.Series(
            {
                "政策/事件日期": pd.Timestamp("2025-07-07"),
                "A股定价日T0": pd.Timestamp("2025-07-07"),
                "结构事实一句话（尽量客观）": "说明",
                "来源详情（写清政策+市场数据）": "来源",
                "T0上证涨跌幅": 0.1,
                "T0深成涨跌幅": 0.2,
                "T0创业板涨跌幅": 0.3,
                "T0成交额（亿元）": 100.0,
            }
        )
        self.assertGreater(score_row(row_high), score_row(row_low))

    def test_dedupe_keeps_best_row(self):
        df = pd.DataFrame(
            [
                {
                    "事件ID": "E-1",
                    "事件名称": "X",
                    "A股定价日T0": pd.Timestamp("2025-07-07"),
                    "类型": "Y",
                    "政策/事件日期": pd.Timestamp("2025-04-03"),
                    "结构事实一句话（尽量客观）": "更长说明",
                    "来源详情（写清政策+市场数据）": "更长来源",
                    "T0上证涨跌幅": None,
                    "T0深成涨跌幅": None,
                    "T0创业板涨跌幅": None,
                    "T0成交额（亿元）": None,
                },
                {
                    "事件ID": "P-1",
                    "事件名称": "X",
                    "A股定价日T0": pd.Timestamp("2025-07-07"),
                    "类型": "Y",
                    "政策/事件日期": pd.Timestamp("2025-07-07"),
                    "结构事实一句话（尽量客观）": "说明",
                    "来源详情（写清政策+市场数据）": "来源",
                    "T0上证涨跌幅": 0.1,
                    "T0深成涨跌幅": 0.2,
                    "T0创业板涨跌幅": 0.3,
                    "T0成交额（亿元）": 100.0,
                },
            ]
        )
        out, removed = dedupe_marco_df(df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["事件ID"], "P-1")
        self.assertEqual(removed, ["E-1"])

    def test_dedupe_prefers_policy_date_match_even_if_text_is_longer(self):
        df = pd.DataFrame(
            [
                {
                    "事件ID": "E-1",
                    "事件名称": "X",
                    "A股定价日T0": pd.Timestamp("2025-07-07"),
                    "类型": "Y",
                    "政策/事件日期": pd.Timestamp("2025-04-03"),
                    "结构事实一句话（尽量客观）": "更长说明" * 20,
                    "来源详情（写清政策+市场数据）": "更长来源" * 20,
                    "T0上证涨跌幅": 0.1,
                    "T0深成涨跌幅": 0.2,
                    "T0创业板涨跌幅": 0.3,
                    "T0成交额（亿元）": 100.0,
                },
                {
                    "事件ID": "P-1",
                    "事件名称": "X",
                    "A股定价日T0": pd.Timestamp("2025-07-07"),
                    "类型": "Y",
                    "政策/事件日期": pd.Timestamp("2025-07-07"),
                    "结构事实一句话（尽量客观）": "说明",
                    "来源详情（写清政策+市场数据）": "来源",
                    "T0上证涨跌幅": 0.1,
                    "T0深成涨跌幅": 0.2,
                    "T0创业板涨跌幅": 0.3,
                    "T0成交额（亿元）": 100.0,
                },
            ]
        )
        out, removed = dedupe_marco_df(df)
        self.assertEqual(out.iloc[0]["事件ID"], "P-1")
        self.assertEqual(removed, ["E-1"])


if __name__ == "__main__":
    unittest.main()
