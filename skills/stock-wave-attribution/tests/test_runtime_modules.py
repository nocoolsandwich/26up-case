import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path("/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data")
SKILL_ROOT = PROJECT_ROOT / "skills" / "stock-wave-attribution"
if str(SKILL_ROOT) not in sys.path:
    sys.path.insert(0, str(SKILL_ROOT))

from runtime import news_selection, report_rendering, verdicts


class RuntimeModulesTest(unittest.TestCase):
    def test_news_selection_module_respects_explicit_lookback_days(self):
        selected = news_selection.select_news_evidence(
            news_evidence=[
                {
                    "published_at": "2025-07-20 08:00",
                    "source_id": "zsxq_damao",
                    "title": "过早信号",
                    "raw_text": "早于显式窗口，不应入选。",
                    "url": "https://example.com/too-early",
                },
                {
                    "published_at": "2025-07-28 09:00",
                    "source_id": "zsxq_saidao_touyan",
                    "title": "启动前强信号",
                    "raw_text": "发生在启动前两周内，应进入粗排。",
                    "url": "https://example.com/pre-wave",
                },
                {
                    "published_at": "2025-08-12 10:00",
                    "source_id": "zsxq_damao",
                    "title": "波段内催化",
                    "raw_text": "波段启动后的强化事件。",
                    "url": "https://example.com/in-wave",
                },
                {
                    "published_at": "2025-08-20 10:00",
                    "source_id": "zsxq_zhuwang",
                    "title": "波段后复盘",
                    "raw_text": "峰值之后的内容，不应进入该波段。",
                    "url": "https://example.com/post-wave",
                },
            ],
            stock_name="佰维存储",
            sample_label="存储芯片",
            concept_labels={"886069.TI": {"name": "存储芯片", "code": "886069.TI"}},
            waves=[{"start_date": "2025-08-11", "peak_date": "2025-08-13"}],
            top_k=5,
            lookback_days=14,
            source_priority={
                "zsxq_saidao_touyan": 4,
                "zsxq_damao": 3,
                "zsxq_zhuwang": 3,
            },
        )

        self.assertEqual(sorted(row["title"] for row in selected), ["启动前强信号", "波段内催化"])
        self.assertEqual(
            news_selection.format_news_source_distribution(selected),
            "zsxq_damao(1条) / zsxq_saidao_touyan(1条)",
        )

    def test_verdicts_module_ignores_sample_label_bias_and_single_noise_terms(self):
        verdict = verdicts.build_local_verdict(
            case_context={"stock_name": "数据港", "sample_label": "算力租赁"},
            selected_news=[
                {
                    "title": "数据港调研纪要",
                    "raw_text": "数据中心需求持续增长，数据港与阿里云深度绑定。",
                },
                {
                    "title": "IDC观点更新",
                    "raw_text": "数据中心供需改善，项目交付在即。",
                },
                {
                    "title": "阿里云产业链梳理",
                    "raw_text": "IDC：数据港（主供）。液冷：英维克。",
                },
            ],
            concept_rows=[
                {
                    "concept_name": "数据中心",
                    "concept_code": "885887.TI",
                    "period_return_pct": "9.76%",
                    "close_corr": "0.6425",
                    "ret_corr": "0.3321",
                    "interpretation": "主线更贴近。",
                },
                {
                    "concept_name": "液冷服务器",
                    "concept_code": "886044.TI",
                    "period_return_pct": "19.70%",
                    "close_corr": "0.7383",
                    "ret_corr": "0.3177",
                    "interpretation": "仅相关配套。",
                },
            ],
            quant_rows=[{"metric": "区间涨幅", "value": "52.96%", "evidence": "close_qfq", "interpretation": "观察总涨幅"}],
        )

        self.assertEqual(verdict["main_cause"], "数据中心")
        self.assertEqual(verdict["alt_cause"], "数据中心板块情绪强化")
        self.assertNotIn("液冷", verdict["main_cause"])
        self.assertNotIn("算力租赁", verdict["final_verdict"]["final_judgment"])

    def test_report_rendering_module_contains_required_sections(self):
        markdown = report_rendering.render_detailed_markdown(
            {
                "stock_name": "五洲新春",
                "ts_code": "603667.SH",
                "start_date": "2025-11-05",
                "end_date": "2026-01-22",
                "report_time": "2026-04-06 19:00:00+08:00",
                "plot_relpath": "../../data/plots/603667_SH_wave_candles.png",
                "one_line_logic": "机器人T链主线驱动，小鹏科技日点火，跨年情绪强化。",
                "wave_sections": [
                    {
                        "wave_id": "W1",
                        "period": "2025-11-28 -> 2026-01-22",
                        "gain_pct": "102.25%",
                        "review": "up_valid",
                        "one_line_logic": "机器人T链主线驱动，小鹏科技日点火，跨年情绪强化。",
                        "news_rows": [
                            {
                                "published_at": "2025-12-30 14:08:42",
                                "source_id": "zsxq_zhuwang",
                                "title": "机器人板块回血",
                                "raw_text": "开启跨年主线行情",
                                "url": "https://example.com/news",
                            }
                        ],
                        "quant_rows": [
                            {
                                "metric": "区间涨幅",
                                "value": "105.08%",
                                "evidence": "close_qfq 44.91 -> 90.83",
                                "interpretation": "显著强于大盘",
                            }
                        ],
                        "concept_rows": [
                            {
                                "concept_name": "人形机器人",
                                "concept_code": "886069.TI",
                                "period_return_pct": "11.53%",
                                "close_corr": "0.9439",
                                "ret_corr": "0.4718",
                                "interpretation": "同步性最高",
                            }
                        ],
                        "conclusion_rows": [
                            {
                                "dimension": "主因",
                                "value": "机器人主线 + 跨年情绪",
                                "confidence": "中高",
                                "notes": "与案例颗粒度对齐",
                            }
                        ],
                        "final_verdict": {
                            "main_cause": "机器人T链 / 丝杠平台化",
                            "alt_cause": "机器人板块跨年情绪强化",
                            "final_judgment": "这轮主升更偏向机器人T链主线，不是泛机器人概念跟涨。",
                            "notes": "启动与加速阶段均有本地证据支撑。",
                            "confidence": "中高",
                        },
                    }
                ],
            }
        )

        self.assertIn("- 报告时间：`2026-04-06 19:00:00+08:00`", markdown)
        self.assertIn("## 证据原文", markdown)
        self.assertIn("## 量价验证表", markdown)
        self.assertIn("## 概念联动验证表", markdown)
        self.assertIn("## 结论与置信度表", markdown)
        self.assertIn("- 粗排新闻来源分布：`zsxq_zhuwang(1条)`", markdown)
        self.assertNotIn("## 本地news归因", markdown)


if __name__ == "__main__":
    unittest.main()
