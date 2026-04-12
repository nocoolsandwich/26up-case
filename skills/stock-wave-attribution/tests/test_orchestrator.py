import importlib.util
import io
import json
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path("/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data")
MODULE_PATH = PROJECT_ROOT / "skills" / "stock-wave-attribution" / "scripts" / "orchestrator.py"
SKILL_ROOT = PROJECT_ROOT / "skills" / "stock-wave-attribution"


def load_module():
    spec = importlib.util.spec_from_file_location("stock_wave_orchestrator", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class StockWaveOrchestratorTest(unittest.TestCase):
    def test_prepare_agent_rerank_task_writes_chunk_files_and_summary(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-08-08", "open_qfq": 10.0, "high_qfq": 10.1, "low_qfq": 9.9, "close_qfq": 10.0},
                {"trade_date": "2025-08-11", "open_qfq": 10.0, "high_qfq": 10.6, "low_qfq": 9.9, "close_qfq": 10.5},
                {"trade_date": "2025-08-12", "open_qfq": 10.5, "high_qfq": 11.2, "low_qfq": 10.4, "close_qfq": 11.0},
                {"trade_date": "2025-08-13", "open_qfq": 11.0, "high_qfq": 11.8, "low_qfq": 10.9, "close_qfq": 11.6},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame(),
            "raw_moneyflow": pd.DataFrame(),
            "raw_limit_list_d": pd.DataFrame(),
        }
        news_rows = []
        for index in range(105):
            news_rows.append(
                {
                    "published_at": f"2025-08-12 {9 + index // 60:02d}:{index % 60:02d}",
                    "source_id": "zsxq_damao" if index % 2 == 0 else "zsxq_saidao_touyan",
                    "title": f"存储候选标题 {index:03d}",
                    "raw_text": f"这是第 {index:03d} 条存储候选原文。",
                    "url": f"https://example.com/{index}",
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = module.prepare_agent_rerank_task(
                case_context={
                    "stock_name": "佰维存储",
                    "ts_code": "688525.SH",
                    "start_date": "2025-01-01",
                    "end_date": "2026-04-07",
                    "sample_label": "存储芯片",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames={},
                concept_labels={},
                rerank_root=Path(tmpdir),
                task_id="attr-rerank",
                segmenter=lambda df: [
                    {
                        "start_date": "2025-08-11",
                        "peak_date": "2025-08-13",
                        "start_price": 10.0,
                        "peak_price": 11.6,
                        "wave_gain_pct": 16.0,
                        "bars": 3,
                    }
                ],
            )

            summary_path = Path(result["summary_path"])
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["task_id"], "attr-rerank")
            self.assertEqual(summary["wave_count"], 1)
            self.assertEqual(summary["waves"][0]["dedup_title_count"], 105)
            self.assertEqual(summary["waves"][0]["chunk_count"], 2)

            wave_dir = Path(result["wave_dirs"]["W1"])
            self.assertTrue((wave_dir / "candidates.jsonl").exists())
            self.assertTrue((wave_dir / "rough_chunks" / "chunk_001.md").exists())
            self.assertTrue((wave_dir / "rough_chunks" / "chunk_002.md").exists())
            chunk_001 = (wave_dir / "rough_chunks" / "chunk_001.md").read_text(encoding="utf-8")
            chunk_002 = (wave_dir / "rough_chunks" / "chunk_002.md").read_text(encoding="utf-8")
            self.assertIn("每个 chunk 直接选 3-5 条", chunk_001)
            self.assertIn("I00001", chunk_001)
            self.assertIn("I00101", chunk_002)

    def test_finalize_agent_rerank_task_uses_selected_ids_instead_of_local_rule_ranking(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-08-11", "open_qfq": 10.0, "high_qfq": 10.6, "low_qfq": 9.9, "close_qfq": 10.5},
                {"trade_date": "2025-08-12", "open_qfq": 10.5, "high_qfq": 11.2, "low_qfq": 10.4, "close_qfq": 11.0},
                {"trade_date": "2025-08-13", "open_qfq": 11.0, "high_qfq": 11.8, "low_qfq": 10.9, "close_qfq": 11.6},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-08-11", "turnover_rate": 5.1}]),
            "raw_moneyflow": pd.DataFrame([{"trade_date": "2025-08-13", "net_mf_amount": 3200.0}]),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2025-08-13", "limit_status": "U", "open_times": 0}]),
        }
        news_rows = [
            {
                "published_at": "2025-08-01 09:00",
                "source_id": "zsxq_damao",
                "title": "被最终选中的存储证据",
                "raw_text": "这是被最终选中的原文。",
                "url": "https://example.com/selected",
            },
            {
                "published_at": "2025-08-02 09:00",
                "source_id": "zsxq_saidao_touyan",
                "title": "本地规则更偏爱的另一条证据",
                "raw_text": "这条不应该出现在最终报告里。",
                "url": "https://example.com/other",
            },
        ]
        concept_frames = {
            "886069.TI": pd.DataFrame(
                [
                    {"trade_date": "2025-08-11", "close": 100.0},
                    {"trade_date": "2025-08-12", "close": 103.0},
                    {"trade_date": "2025-08-13", "close": 107.0},
                ]
            )
        }
        concept_labels = {"886069.TI": {"code": "886069.TI", "name": "存储芯片"}}

        with tempfile.TemporaryDirectory() as tmpdir:
            rerank_root = Path(tmpdir)
            prepared = module.prepare_agent_rerank_task(
                case_context={
                    "stock_name": "佰维存储",
                    "ts_code": "688525.SH",
                    "start_date": "2025-01-01",
                    "end_date": "2026-04-07",
                    "sample_label": "存储芯片",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                rerank_root=rerank_root,
                task_id="attr-rerank",
                segmenter=lambda df: [
                    {
                        "start_date": "2025-08-11",
                        "peak_date": "2025-08-13",
                        "start_price": 10.0,
                        "peak_price": 11.6,
                        "wave_gain_pct": 16.0,
                        "bars": 3,
                    }
                ],
            )
            selection_path = rerank_root / "final_selection.json"
            selection_path.write_text(
                json.dumps(
                    {
                        "one_liner": "存储涨价与 AI 存储升级共振。",
                        "waves": [
                            {
                                "wave_id": "W1",
                                "one_line_logic": "存储涨价与 AI 存储升级共振。",
                                "final_picks": [
                                    {
                                        "item_id": "I00001",
                                        "role": "启动前强信号",
                                        "reason": "直接由 agent 入围。",
                                    }
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.finalize_agent_rerank_task(
                case_context={
                    "stock_name": "佰维存储",
                    "ts_code": "688525.SH",
                    "start_date": "2025-01-01",
                    "end_date": "2026-04-07",
                    "sample_label": "存储芯片",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                rerank_root=rerank_root,
                selection_path=selection_path,
                output_root=rerank_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2025-08-11",
                        "peak_date": "2025-08-13",
                        "start_price": 10.0,
                        "peak_price": 11.6,
                        "wave_gain_pct": 16.0,
                        "bars": 3,
                    }
                ],
                plotter=fake_plotter,
            )

            markdown = Path(result["report_path"]).read_text(encoding="utf-8")
            self.assertIn("被最终选中的存储证据", markdown)
            self.assertNotIn("本地规则更偏爱的另一条证据", markdown)
            self.assertIn("- 波段审查：`agent_rerank`", markdown)
            self.assertIn("- 一句话逻辑：`存储涨价与 AI 存储升级共振。`", markdown)

    def test_finalize_agent_rerank_task_marks_concept_skipped_explicitly(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2026-02-09", "open_qfq": 10.0, "high_qfq": 10.6, "low_qfq": 9.9, "close_qfq": 10.5},
                {"trade_date": "2026-04-09", "open_qfq": 17.0, "high_qfq": 17.5, "low_qfq": 16.8, "close_qfq": 17.3},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame([{"trade_date": "2026-02-09", "turnover_rate": 5.1}]),
            "raw_moneyflow": pd.DataFrame([{"trade_date": "2026-04-09", "net_mf_amount": 3200.0}]),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-04-09", "limit_status": "U", "open_times": 0}]),
        }
        news_rows = [
            {
                "published_at": "2026-02-09 15:30",
                "source_id": "zsxq_saidao_touyan",
                "title": "奥瑞德进入算力电源涨停板",
                "raw_text": "美国数据中心建设热潮引发用电荒，奥瑞德进入数据中心电源涨停板。",
                "url": "https://example.com/aurora",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            rerank_root = Path(tmpdir)
            module.prepare_agent_rerank_task(
                case_context={
                    "stock_name": "奥瑞德",
                    "ts_code": "600666.SH",
                    "start_date": "2026-01-01",
                    "end_date": "2026-04-09",
                    "sample_label": "算力",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames={},
                concept_labels={},
                rerank_root=rerank_root,
                task_id="attr-rerank",
                segmenter=lambda df: [
                    {
                        "start_date": "2026-02-09",
                        "peak_date": "2026-04-09",
                        "start_price": 10.0,
                        "peak_price": 17.3,
                        "wave_gain_pct": 73.0,
                        "bars": 2,
                    }
                ],
            )
            selection_path = rerank_root / "final_selection.json"
            selection_path.write_text(
                json.dumps(
                    {
                        "one_liner": "奥瑞德受益于算力租赁与智算中心重估。",
                        "waves": [
                            {
                                "wave_id": "W1",
                                "one_line_logic": "奥瑞德受益于算力租赁与智算中心重估。",
                                "final_picks": [
                                    {
                                        "item_id": "I00001",
                                        "role": "启动期强化",
                                        "reason": "直接由 agent 入围。",
                                    }
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.finalize_agent_rerank_task(
                case_context={
                    "stock_name": "奥瑞德",
                    "ts_code": "600666.SH",
                    "start_date": "2026-01-01",
                    "end_date": "2026-04-09",
                    "sample_label": "算力",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames={},
                concept_labels={},
                rerank_root=rerank_root,
                selection_path=selection_path,
                output_root=rerank_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2026-02-09",
                        "peak_date": "2026-04-09",
                        "start_price": 10.0,
                        "peak_price": 17.3,
                        "wave_gain_pct": 73.0,
                        "bars": 2,
                    }
                ],
                plotter=fake_plotter,
                skip_concept=True,
            )

            markdown = Path(result["report_path"]).read_text(encoding="utf-8")
            self.assertIn("已显式跳过概念联动，本次不做概念联动验证。", markdown)
            self.assertIn("已显式跳过概念联动，本次依据精选 news 与量价验证。", markdown)
            self.assertNotIn("概念联动与精选 news 共振验证。", markdown)

    def test_render_news_raw_blocks_orders_by_published_at_ascending(self):
        module = load_module()

        markdown = module._render_news_raw_blocks(
            [
                {
                    "published_at": "2025-12-30 14:08:42",
                    "source_id": "zsxq_zhuwang",
                    "title": "后面的证据",
                    "raw_text": "后面的原文",
                    "url": "https://example.com/later",
                },
                {
                    "published_at": "2025-11-05 19:37:22",
                    "source_id": "zsxq_damao",
                    "title": "更早的证据",
                    "raw_text": "更早的原文",
                    "url": "https://example.com/earlier",
                },
            ]
        )

        self.assertLess(markdown.index("更早的证据"), markdown.index("后面的证据"))
        self.assertLess(markdown.index("2025-11-05 19:37"), markdown.index("2025-12-30 14:08"))

    def test_build_timeline_rows_uses_brief_impact_summary_instead_of_full_raw_text(self):
        module = load_module()

        rows = module._build_timeline_rows(
            [
                {
                    "published_at": "2025-11-05 19:37",
                    "source_id": "zsxq_zhuwang",
                    "title": "小鹏科技日",
                    "raw_text": "第一段影响说明。\n第二段很长的补充细节。\n第三段继续展开。",
                }
            ]
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["time"], "2025-11-05 19:37")
        self.assertEqual(rows[0]["event"], "小鹏科技日")
        self.assertEqual(rows[0]["source"], "zsxq_zhuwang")
        self.assertEqual(rows[0]["impact"], "第一段影响说明。")
        self.assertNotIn("第二段很长的补充细节", rows[0]["impact"])

    def test_select_news_evidence_ignores_wscn_live_and_deduplicates(self):
        module = load_module()

        selected = module._select_news_evidence(
            news_evidence=[
                {
                    "published_at": "2025-11-27 08:00",
                    "source_id": "zsxq_saidao_touyan",
                    "title": "五洲新春切入机器人丝杠",
                    "raw_text": "五洲新春与机器人丝杠主线直接相关。",
                    "url": "https://example.com/1",
                },
                {
                    "published_at": "2025-12-28 09:00",
                    "source_id": "wscn_live",
                    "title": "机器人板块回暖",
                    "raw_text": "机器人板块回暖，但未直接提到五洲新春。",
                    "url": "https://example.com/2",
                },
                {
                    "published_at": "2025-11-28 10:00",
                    "source_id": "zsxq_damao",
                    "title": "五洲新春切入机器人丝杠",
                    "raw_text": "五洲新春与机器人丝杠主线直接相关。",
                    "url": "https://example.com/3",
                },
            ],
            stock_name="五洲新春",
            sample_label="机器人概念",
            concept_labels={"886069.TI": {"name": "人形机器人", "code": "886069.TI"}},
            waves=[
                {
                    "start_date": "2025-11-28",
                    "peak_date": "2026-01-22",
                }
            ],
            top_k=2,
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["title"], "五洲新春切入机器人丝杠")
        self.assertEqual(selected[0]["source_id"], "zsxq_saidao_touyan")

    def test_select_news_evidence_accepts_tz_aware_published_at(self):
        module = load_module()

        selected = module._select_news_evidence(
            news_evidence=[
                {
                    "published_at": "2025-11-27 08:00:00+08:00",
                    "source_id": "zsxq_saidao_touyan",
                    "title": "五洲新春切入机器人丝杠",
                    "raw_text": "五洲新春与机器人丝杠主线直接相关。",
                    "url": "https://example.com/1",
                }
            ],
            stock_name="五洲新春",
            sample_label="机器人概念",
            concept_labels={"886069.TI": {"name": "人形机器人", "code": "886069.TI"}},
            waves=[
                {
                    "start_date": "2025-11-28",
                    "peak_date": "2026-01-22",
                }
            ],
            top_k=1,
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["title"], "五洲新春切入机器人丝杠")

    def test_select_news_evidence_excludes_news_after_wave_peak(self):
        module = load_module()

        selected = module._select_news_evidence(
            news_evidence=[
                {
                    "published_at": "2025-11-27 08:00",
                    "source_id": "zsxq_saidao_touyan",
                    "title": "波段启动催化",
                    "raw_text": "发生在波段启动前。",
                    "url": "https://example.com/1",
                },
                {
                    "published_at": "2026-02-01 08:00",
                    "source_id": "zsxq_damao",
                    "title": "波段结束后的点评",
                    "raw_text": "发布时间晚于波段峰值，不应进入该波段分析。",
                    "url": "https://example.com/2",
                },
            ],
            stock_name="五洲新春",
            sample_label="机器人概念",
            concept_labels={"886069.TI": {"name": "人形机器人", "code": "886069.TI"}},
            waves=[
                {
                    "start_date": "2025-11-28",
                    "peak_date": "2025-12-20",
                }
            ],
            top_k=5,
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["title"], "波段启动催化")

    def test_run_stock_wave_attribution_only_keeps_selected_news_rows(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "open_qfq": 44.0, "high_qfq": 45.0, "low_qfq": 43.8, "close_qfq": 44.91},
                {"trade_date": "2025-12-01", "open_qfq": 45.2, "high_qfq": 48.0, "low_qfq": 45.0, "close_qfq": 47.5},
                {"trade_date": "2025-12-02", "open_qfq": 47.6, "high_qfq": 50.8, "low_qfq": 47.2, "close_qfq": 50.2},
                {"trade_date": "2025-12-03", "open_qfq": 50.1, "high_qfq": 55.0, "low_qfq": 49.8, "close_qfq": 54.8},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-11-28", "turnover_rate": 5.1}]),
            "raw_moneyflow": pd.DataFrame([{"trade_date": "2025-12-03", "net_mf_amount": 4200.0}]),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2025-12-03", "limit_status": "U", "open_times": 0}]),
        }
        news_rows = [
            {
                "published_at": "2025-11-27 08:00",
                "source_id": "zsxq_saidao_touyan",
                "title": "五洲新春切入机器人丝杠",
                "raw_text": "五洲新春与机器人丝杠主线直接相关。\n第二段补充。",
                "url": "https://example.com/1",
            },
            {
                "published_at": "2025-11-28 10:00",
                "source_id": "zsxq_damao",
                "title": "五洲新春切入机器人丝杠",
                "raw_text": "五洲新春与机器人丝杠主线直接相关。\n第二段补充。",
                "url": "https://example.com/dup",
            },
            {
                "published_at": "2025-12-28 09:00",
                "source_id": "wscn_live",
                "title": "机器人板块回暖",
                "raw_text": "机器人板块回暖，但未直接提到五洲新春。",
                "url": "https://example.com/2",
            },
        ]
        concept_frames = {
            "886069.TI": pd.DataFrame(
                [
                    {"trade_date": "2025-11-28", "close": 100.0},
                    {"trade_date": "2025-12-01", "close": 104.0},
                    {"trade_date": "2025-12-02", "close": 108.0},
                    {"trade_date": "2025-12-03", "close": 112.0},
                ]
            )
        }
        concept_labels = {"886069.TI": {"code": "886069.TI", "name": "人形机器人"}}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir)

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.run_stock_wave_attribution(
                case_context={
                    "stock_name": "五洲新春",
                    "ts_code": "603667.SH",
                    "start_date": "2025-11-05",
                    "end_date": "2026-01-22",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                output_root=output_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2025-11-28",
                        "peak_date": "2025-12-03",
                        "start_price": 44.91,
                        "peak_price": 54.8,
                        "wave_gain_pct": 22.02,
                        "bars": 4,
                    }
                ],
                plotter=fake_plotter,
            )

            markdown = Path(result["report_path"]).read_text(encoding="utf-8")
            self.assertEqual(markdown.count("#### 证据 "), 1)
            self.assertEqual(markdown.count("五洲新春切入机器人丝杠"), 1)
            self.assertIn("- 粗排新闻来源分布：`zsxq_damao(1条) / zsxq_saidao_touyan(1条)`", markdown)
            self.assertNotIn("机器人板块回暖", markdown)
            self.assertNotIn("https://example.com/dup", markdown)

    def test_build_wave_attribution_search_prompt_uses_standardized_fields_and_order(self):
        module = load_module()

        prompt = module.build_wave_attribution_search_prompt(
            stock_name="乾照光电",
            ts_code="300102.SZ",
            wave_start="2025-10-27",
            wave_end="2026-01-28",
            wave_gain_pct=256.03,
            sample_label="光伏概念",
            candidate_mainline="太空算力 / 空间电源 / 砷化镓",
            cross_themes=["MiniLED", "MicroLED", "第三代半导体"],
        )

        self.assertIn("标的：乾照光电（300102.SZ）", prompt)
        self.assertIn("波段：2025-10-27 到 2026-01-28", prompt)
        self.assertIn("波段涨幅：256.03%", prompt)
        self.assertIn("样本标签：光伏概念", prompt)
        self.assertIn("请联网后只输出以下结构：", prompt)
        self.assertIn("主因：", prompt)
        self.assertIn("备选：", prompt)
        self.assertIn("搜索依据：", prompt)
        self.assertIn("时间线：", prompt)
        self.assertIn("结论说明：", prompt)
        self.assertIn("明确判断真实主线是否是太空算力 / 空间电源 / 砷化镓，而不是光伏概念。", prompt)
        self.assertIn("如果存在 MiniLED / MicroLED / 第三代半导体 等交叉题材，放到备选，不要混成主因。", prompt)

        self.assertLess(prompt.index("标的："), prompt.index("波段："))
        self.assertLess(prompt.index("波段："), prompt.index("波段涨幅："))
        self.assertLess(prompt.index("波段涨幅："), prompt.index("样本标签："))

    def test_run_chatgpt_browser_uses_skill_local_submit_search_then_wait(self):
        module = load_module()
        calls = []

        def fake_runner(args, check, capture_output, text, cwd):
            calls.append({"args": args, "cwd": cwd})
            if "submit-search" in args:
                return types.SimpleNamespace(stdout=json.dumps({"id": "task-123"}), stderr="", returncode=0)
            if "wait" in args:
                return types.SimpleNamespace(stdout="主因：机器人主线\n备选：跨年情绪\n搜索依据：公开材料", stderr="", returncode=0)
            raise AssertionError(args)

        result = module.run_chatgpt_browser("解释这段波段", mode="search", runner=fake_runner)

        self.assertIn("主因：机器人主线", result)
        self.assertEqual(len(calls), 2)
        self.assertIn("skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs", " ".join(calls[0]["args"]))
        self.assertNotIn(".codex/skills/chatgpt-plus-browser", " ".join(calls[0]["args"]))
        self.assertIn("submit-search", calls[0]["args"])
        self.assertIn("wait", calls[1]["args"])
        self.assertEqual(calls[0]["cwd"], str(SKILL_ROOT))

    def test_default_config_path_points_to_skill_local_yaml(self):
        module = load_module()

        self.assertEqual(module.DEFAULT_CONFIG_PATH, SKILL_ROOT / "stock-wave-attribution.yaml")
        self.assertTrue(module.DEFAULT_CONFIG_PATH.exists())
        self.assertFalse(module.DEFAULT_CONFIG["chatgpt"]["enabled"])

    def test_render_detailed_markdown_contract_contains_required_tables(self):
        module = load_module()

        markdown = module.render_detailed_markdown(
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
                        "timeline_rows": [
                            {
                                "time": "2025-11-05 19:37",
                                "category": "主题催化",
                                "event": "小鹏科技日",
                                "impact": "打开机器人主题预期",
                                "source": "zsxq_zhuwang",
                            }
                        ],
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
        self.assertIn("# 波段 W1", markdown)
        self.assertIn("## 证据原文", markdown)
        self.assertIn("## 量价验证表", markdown)
        self.assertIn("## 概念联动验证表", markdown)
        self.assertIn("## 结论与置信度表", markdown)
        self.assertNotIn("## 事件时间线表", markdown)
        self.assertNotIn("## 波段分段归因表", markdown)
        self.assertNotIn("## 本地news归因", markdown)
        self.assertIn("- 一句话逻辑：`机器人T链主线驱动，小鹏科技日点火，跨年情绪强化。`", markdown)
        self.assertIn("- 区间：`2025-11-28 -> 2026-01-22`", markdown)
        self.assertIn("- 涨幅：`102.25%`", markdown)
        self.assertIn("- 波段审查：`up_valid`", markdown)
        self.assertIn("- 粗排新闻来源分布：`zsxq_zhuwang(1条)`", markdown)
        self.assertNotIn("| 时间 | 事件 | 来源 |", markdown)
        self.assertNotIn("| 时间 | 事件类别 | 事件 | 对波段影响 | 来源 |", markdown)
        self.assertNotIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", markdown)
        self.assertIn("2025-12-30 14:08", markdown)
        self.assertNotIn("2025-12-30 14:08:42", markdown)
        self.assertIn("#### 证据 1", markdown)
        self.assertIn("```text", markdown)
        self.assertIn("开启跨年主线行情", markdown)
        self.assertIn("| 维度 | 数值 | 证据 | 解释 |", markdown)
        self.assertIn("| 概念 | 代码 | 区间涨幅 | 收盘价相关系数 | 日收益率相关系数 | 解释 |", markdown)
        self.assertIn("| 维度 | 结论 | 置信度 | 说明 |", markdown)
        self.assertIn("## 综合裁决", markdown)
        self.assertIn("- 主因：`机器人T链 / 丝杠平台化`", markdown)
        self.assertIn("- 备选：`机器人板块跨年情绪强化`", markdown)
        self.assertIn("- 最终判定：机器人T链主线，不是泛机器人概念跟涨。", markdown)

    def test_orchestrator_defaults_to_local_flow_without_chatgpt(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "open_qfq": 44.0, "high_qfq": 45.0, "low_qfq": 43.8, "close_qfq": 44.91},
                {"trade_date": "2025-12-01", "open_qfq": 45.2, "high_qfq": 48.0, "low_qfq": 45.0, "close_qfq": 47.5},
                {"trade_date": "2025-12-02", "open_qfq": 47.6, "high_qfq": 50.8, "low_qfq": 47.2, "close_qfq": 50.2},
                {"trade_date": "2025-12-03", "open_qfq": 50.1, "high_qfq": 55.0, "low_qfq": 49.8, "close_qfq": 54.8},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame(
                [
                    {"trade_date": "2025-11-28", "turnover_rate": 5.1, "volume_ratio": 1.4},
                    {"trade_date": "2025-12-03", "turnover_rate": 8.6, "volume_ratio": 2.2},
                ]
            ),
            "raw_moneyflow": pd.DataFrame(
                [
                    {"trade_date": "2025-11-28", "net_mf_amount": 1200.0},
                    {"trade_date": "2025-12-03", "net_mf_amount": 4200.0},
                ]
            ),
            "raw_limit_list_d": pd.DataFrame(
                [
                    {"trade_date": "2025-12-03", "limit_status": "U", "open_times": 0},
                ]
            ),
        }
        news_rows = [
            {
                "published_at": "2025-11-05 19:37",
                "source_id": "zsxq_zhuwang",
                "title": "小鹏科技日",
                "raw_text": "机器人主题启动",
                "url": "https://example.com/1",
            }
        ]
        concept_frames = {
            "886069.TI": pd.DataFrame(
                [
                    {"trade_date": "2025-11-28", "close": 100.0},
                    {"trade_date": "2025-12-01", "close": 104.0},
                    {"trade_date": "2025-12-02", "close": 108.0},
                    {"trade_date": "2025-12-03", "close": 112.0},
                ]
            )
        }
        concept_labels = {"886069.TI": {"code": "886069.TI", "name": "人形机器人"}}

        chatgpt_calls = []

        def fake_chatgpt(prompt, mode, **kwargs):
            chatgpt_calls.append(mode)
            if mode == "plain":
                return "结论：up_valid"
            return "主因：机器人跨年主线\n备选：小鹏科技日\n搜索依据：公开市场材料"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir)

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.run_stock_wave_attribution(
                case_context={
                    "stock_name": "五洲新春",
                    "ts_code": "603667.SH",
                    "start_date": "2025-11-05",
                    "end_date": "2026-01-22",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                output_root=output_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2025-11-28",
                        "peak_date": "2025-12-03",
                        "start_price": 44.91,
                        "peak_price": 54.8,
                        "wave_gain_pct": 22.02,
                        "bars": 4,
                    }
                ],
                plotter=fake_plotter,
                chatgpt_runner=fake_chatgpt,
            )

            report_path = Path(result["report_path"])
            self.assertTrue(report_path.exists())
            markdown = report_path.read_text(encoding="utf-8")
            self.assertIn("- 报告时间：`", markdown)
            self.assertIn("# 波段 W1", markdown)
            self.assertIn("## 证据原文", markdown)
            self.assertIn("## 结论与置信度表", markdown)
            self.assertIn("## 综合裁决", markdown)
            self.assertNotIn("## 事件时间线表", markdown)
            self.assertNotIn("## 本地news归因", markdown)
            self.assertIn("- 一句话逻辑：`", markdown)
            self.assertIn("- 粗排新闻来源分布：`zsxq_zhuwang(1条)`", markdown)
            self.assertNotIn("| 时间 | 事件 | 来源 |", markdown)
            self.assertNotIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", markdown)
            self.assertIn("#### 证据 1", markdown)
            self.assertIn("```text", markdown)
            self.assertEqual(chatgpt_calls, [])
            self.assertNotIn("待结合本地证据裁决", markdown)
            self.assertIn("runtime/wave_segmentation.py", result["call_chain"])
            self.assertIn("runtime/wave_plotting.py", result["call_chain"])
            self.assertIn("runtime/attribution_data.py", result["call_chain"])
            self.assertNotIn("skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs", result["call_chain"])
            self.assertTrue(Path(result["plot_path"]).exists())
            self.assertTrue(Path(result["report_contract_path"]).exists())

    def test_run_stock_wave_attribution_only_analyzes_top_2_gain_waves(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-12-01", "open_qfq": 10.0, "high_qfq": 10.3, "low_qfq": 9.9, "close_qfq": 10.0},
                {"trade_date": "2025-12-02", "open_qfq": 10.0, "high_qfq": 11.0, "low_qfq": 9.9, "close_qfq": 10.8},
                {"trade_date": "2025-12-03", "open_qfq": 10.8, "high_qfq": 11.1, "low_qfq": 10.6, "close_qfq": 11.0},
                {"trade_date": "2025-12-04", "open_qfq": 11.0, "high_qfq": 12.0, "low_qfq": 10.9, "close_qfq": 11.8},
                {"trade_date": "2025-12-05", "open_qfq": 11.8, "high_qfq": 12.5, "low_qfq": 11.6, "close_qfq": 12.2},
                {"trade_date": "2025-12-08", "open_qfq": 12.2, "high_qfq": 13.5, "low_qfq": 12.1, "close_qfq": 13.2},
                {"trade_date": "2025-12-09", "open_qfq": 13.2, "high_qfq": 15.0, "low_qfq": 13.0, "close_qfq": 14.6},
                {"trade_date": "2025-12-10", "open_qfq": 14.6, "high_qfq": 15.5, "low_qfq": 14.4, "close_qfq": 15.2},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame(
                [{"trade_date": row["trade_date"], "turnover_rate": 5.0 + index} for index, row in enumerate(stock_df.to_dict("records"))]
            ),
            "raw_moneyflow": pd.DataFrame(
                [{"trade_date": row["trade_date"], "net_mf_amount": 1000.0 + index * 100.0} for index, row in enumerate(stock_df.to_dict("records"))]
            ),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2025-12-10", "limit_status": "U", "open_times": 0}]),
        }
        news_rows = [
            {
                "published_at": "2025-12-01 09:00",
                "source_id": "zsxq_zhuwang",
                "title": "第一波催化",
                "raw_text": "对应低涨幅波段。",
                "url": "https://example.com/w1",
            },
            {
                "published_at": "2025-12-04 09:00",
                "source_id": "zsxq_zhuwang",
                "title": "第二波催化",
                "raw_text": "对应中等涨幅波段。",
                "url": "https://example.com/w2",
            },
            {
                "published_at": "2025-12-08 09:00",
                "source_id": "zsxq_zhuwang",
                "title": "第三波催化",
                "raw_text": "对应最高涨幅波段。",
                "url": "https://example.com/w3",
            },
            {
                "published_at": "2025-12-09 09:00",
                "source_id": "zsxq_zhuwang",
                "title": "第四波催化",
                "raw_text": "对应次高涨幅波段。",
                "url": "https://example.com/w4",
            },
        ]
        concept_frames = {
            "886069.TI": pd.DataFrame(
                [
                    {"trade_date": "2025-12-01", "close": 100.0},
                    {"trade_date": "2025-12-02", "close": 102.0},
                    {"trade_date": "2025-12-03", "close": 103.0},
                    {"trade_date": "2025-12-04", "close": 106.0},
                    {"trade_date": "2025-12-05", "close": 108.0},
                    {"trade_date": "2025-12-08", "close": 111.0},
                    {"trade_date": "2025-12-09", "close": 116.0},
                    {"trade_date": "2025-12-10", "close": 118.0},
                ]
            )
        }
        concept_labels = {"886069.TI": {"code": "886069.TI", "name": "人形机器人"}}
        plot_calls = []

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir)

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                plot_calls.append(list(waves))
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.run_stock_wave_attribution(
                case_context={
                    "stock_name": "示例股票",
                    "ts_code": "000001.SZ",
                    "start_date": "2025-12-01",
                    "end_date": "2025-12-10",
                    "sample_label": "机器人概念",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                output_root=output_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2025-12-01",
                        "peak_date": "2025-12-02",
                        "start_price": 10.0,
                        "peak_price": 10.8,
                        "wave_gain_pct": 8.0,
                        "bars": 2,
                    },
                    {
                        "start_date": "2025-12-03",
                        "peak_date": "2025-12-04",
                        "start_price": 11.0,
                        "peak_price": 11.8,
                        "wave_gain_pct": 7.27,
                        "bars": 2,
                    },
                    {
                        "start_date": "2025-12-04",
                        "peak_date": "2025-12-08",
                        "start_price": 11.8,
                        "peak_price": 13.2,
                        "wave_gain_pct": 11.86,
                        "bars": 3,
                    },
                    {
                        "start_date": "2025-12-08",
                        "peak_date": "2025-12-10",
                        "start_price": 13.2,
                        "peak_price": 15.2,
                        "wave_gain_pct": 15.15,
                        "bars": 3,
                    },
                ],
                plotter=fake_plotter,
            )

            self.assertEqual(result["wave_count"], 2)
            self.assertEqual(len(plot_calls), 1)
            self.assertEqual(len(plot_calls[0]), 2)
            self.assertEqual(
                [wave["wave_gain_pct"] for wave in plot_calls[0]],
                [15.15, 11.86],
            )
            self.assertEqual(
                [wave["wave_id"] for wave in plot_calls[0]],
                ["W1", "W2"],
            )
            markdown = Path(result["report_path"]).read_text(encoding="utf-8")
            self.assertEqual(markdown.count("# 波段 W"), 2)
            self.assertNotIn("2025-12-03 -> 2025-12-04", markdown)
            self.assertIn("# 波段 W1\n\n- 区间：`2025-12-08 -> 2025-12-10`", markdown)
            self.assertIn("# 波段 W2\n\n- 区间：`2025-12-04 -> 2025-12-08`", markdown)
            self.assertIn("2025-12-04 -> 2025-12-08", markdown)
            self.assertIn("2025-12-08 -> 2025-12-10", markdown)
            self.assertNotIn("2025-12-01 -> 2025-12-02", markdown)

    def test_orchestrator_can_still_enable_chatgpt_flow_explicitly(self):
        module = load_module()
        stock_df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "open_qfq": 44.0, "high_qfq": 45.0, "low_qfq": 43.8, "close_qfq": 44.91},
                {"trade_date": "2025-12-01", "open_qfq": 45.2, "high_qfq": 48.0, "low_qfq": 45.0, "close_qfq": 47.5},
                {"trade_date": "2025-12-02", "open_qfq": 47.6, "high_qfq": 50.8, "low_qfq": 47.2, "close_qfq": 50.2},
                {"trade_date": "2025-12-03", "open_qfq": 50.1, "high_qfq": 55.0, "low_qfq": 49.8, "close_qfq": 54.8},
            ]
        )
        stock_bundle = {
            "raw_stock_daily_qfq": stock_df,
            "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-11-28", "turnover_rate": 5.1}]),
            "raw_moneyflow": pd.DataFrame([{"trade_date": "2025-12-03", "net_mf_amount": 4200.0}]),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2025-12-03", "limit_status": "U", "open_times": 0}]),
        }
        news_rows = [
            {
                "published_at": "2025-11-05 19:37",
                "source_id": "zsxq_zhuwang",
                "title": "小鹏科技日",
                "raw_text": "机器人主题启动",
                "url": "https://example.com/1",
            }
        ]
        concept_frames = {
            "886069.TI": pd.DataFrame(
                [
                    {"trade_date": "2025-11-28", "close": 100.0},
                    {"trade_date": "2025-12-01", "close": 104.0},
                    {"trade_date": "2025-12-02", "close": 108.0},
                    {"trade_date": "2025-12-03", "close": 112.0},
                ]
            )
        }
        concept_labels = {"886069.TI": {"code": "886069.TI", "name": "人形机器人"}}

        chatgpt_calls = []

        def fake_chatgpt(prompt, mode, **kwargs):
            chatgpt_calls.append(mode)
            if mode == "plain":
                return "结论：up_valid"
            return "主因：机器人跨年主线\n备选：小鹏科技日\n搜索依据：公开市场材料"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir)

            def fake_plotter(df, waves, output_path, title, style="enhanced"):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(b"fake-png")
                return {"output_path": str(output), "candles_plotted": len(df), "waves_annotated": len(waves), "style": style}

            result = module.run_stock_wave_attribution(
                case_context={
                    "stock_name": "五洲新春",
                    "ts_code": "603667.SH",
                    "start_date": "2025-11-05",
                    "end_date": "2026-01-22",
                },
                stock_bundle=stock_bundle,
                news_evidence=news_rows,
                concept_frames=concept_frames,
                concept_labels=concept_labels,
                output_root=output_root,
                segmenter=lambda df: [
                    {
                        "start_date": "2025-11-28",
                        "peak_date": "2025-12-03",
                        "start_price": 44.91,
                        "peak_price": 54.8,
                        "wave_gain_pct": 22.02,
                        "bars": 4,
                    }
                ],
                plotter=fake_plotter,
                chatgpt_runner=fake_chatgpt,
                use_chatgpt=True,
            )

            self.assertEqual(chatgpt_calls, ["plain", "search"])
            self.assertIn("skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs", result["call_chain"])

    def test_run_local_attribution_task_uses_db_fetchers_and_local_runner(self):
        module = load_module()
        calls = {
            "dsn": [],
            "keywords": None,
            "analysis_dir": None,
            "plot_dir": None,
        }

        class _DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_connect(dsn):
            calls["dsn"].append(dsn)
            return _DummyConnection()

        def fake_stock_bundle_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            self.assertEqual(ts_code, "603667.SH")
            return {
                "raw_stock_daily_qfq": pd.DataFrame(
                    [
                        {"trade_date": "2025-11-05", "open_qfq": 10.0, "high_qfq": 10.5, "low_qfq": 9.8, "close_qfq": 10.0},
                        {"trade_date": "2026-01-21", "open_qfq": 10.1, "high_qfq": 10.8, "low_qfq": 10.0, "close_qfq": 10.6},
                    ]
                ),
                "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-11-05", "turnover_rate": 5.0}]),
                "raw_moneyflow": pd.DataFrame([{"trade_date": "2026-01-21", "net_mf_amount": 1200.0}]),
                "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-01-21", "limit_status": "U"}]),
            }

        def fake_concept_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            return (
                {
                    "886069.TI": pd.DataFrame(
                        [
                            {"trade_date": "2025-11-05", "close": 100.0},
                            {"trade_date": "2026-01-21", "close": 103.0},
                        ]
                    )
                },
                {"886069.TI": {"code": "886069.TI", "name": "人形机器人"}},
            )

        def fake_news_fetcher(conn, start_date, end_date, keywords):
            self.assertIsInstance(conn, _DummyConnection)
            calls["keywords"] = list(keywords)
            return [
                {
                    "published_at": "2025-11-05 19:37",
                    "source_id": "zsxq_zhuwang",
                    "title": "小鹏科技日",
                    "raw_text": "机器人主题启动",
                    "url": "https://example.com/1",
                }
            ]

        def fake_runner(case_context, stock_bundle, news_evidence, concept_frames, concept_labels, analysis_dir=None, plot_dir=None, **kwargs):
            calls["analysis_dir"] = analysis_dir
            calls["plot_dir"] = plot_dir
            self.assertEqual(case_context["stock_name"], "五洲新春")
            self.assertEqual(sorted(concept_frames.keys()), ["886069.TI"])
            self.assertEqual(concept_labels["886069.TI"]["name"], "人形机器人")
            return {
                "report_path": "/tmp/fake-report.md",
                "plot_path": "/tmp/fake-plot.png",
                "report_contract_path": "/tmp/contract.md",
                "call_chain": ["runtime/wave_segmentation.py"],
                "wave_count": 1,
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "stock-wave-attribution.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "postgres:",
                        "  event_news_dsn: postgresql://tester@localhost:5432/event_news",
                        "  event_quant_dsn: postgresql://tester@localhost:5432/event_quant",
                        "tushare:",
                        "  token: dummy-token",
                        "  http_url: http://example.com",
                        "paths:",
                        f"  analysis_dir: {tmpdir}/outputs/analysis",
                        f"  plot_dir: {tmpdir}/data/plots",
                        f"  cache_dir: {tmpdir}/data/stock_cache",
                        "chatgpt:",
                        "  enabled: false",
                        "  node_bin: node",
                        "  script_path: ../chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
                    ]
                ),
                encoding="utf-8",
            )

            result = module.run_local_attribution_task(
                stock_name="五洲新春",
                ts_code="603667.SH",
                start_date="2025-11-05",
                end_date="2026-01-22",
                sample_label="机器人概念",
                config_path=config_path,
                db_connect=fake_connect,
                stock_bundle_fetcher=fake_stock_bundle_fetcher,
                concept_fetcher=fake_concept_fetcher,
                news_fetcher=fake_news_fetcher,
                attribution_runner=fake_runner,
            )

        self.assertEqual(
            calls["dsn"],
            [
                "postgresql://tester@localhost:5432/event_quant",
                "postgresql://tester@localhost:5432/event_news",
            ],
        )
        self.assertIn("五洲新春", calls["keywords"])
        self.assertIn("机器人", calls["keywords"])
        self.assertIn("人形机器人", calls["keywords"])
        self.assertTrue(str(calls["analysis_dir"]).endswith("/outputs/analysis"))
        self.assertTrue(str(calls["plot_dir"]).endswith("/data/plots"))
        self.assertEqual(result["report_path"], "/tmp/fake-report.md")

    def test_run_local_attribution_task_rejects_truncated_stock_window(self):
        module = load_module()

        class _DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_connect(_dsn):
            return _DummyConnection()

        def fake_stock_bundle_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            return {
                "raw_stock_daily_qfq": pd.DataFrame(
                    [
                        {"trade_date": "2025-09-10", "open_qfq": 87.0, "high_qfq": 88.0, "low_qfq": 86.0, "close_qfq": 87.61},
                        {"trade_date": "2026-03-09", "open_qfq": 208.0, "high_qfq": 214.0, "low_qfq": 205.0, "close_qfq": 211.91},
                    ]
                ),
                "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-09-10", "turnover_rate": 1.0}]),
                "raw_moneyflow": pd.DataFrame([{"trade_date": "2026-03-09", "net_mf_amount": 1.0}]),
                "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-01-28", "limit_status": "U"}]),
            }

        def fake_concept_fetcher(conn, ts_code, start_date, end_date):
            return {}, {}

        def fake_news_fetcher(conn, start_date, end_date, keywords):
            return []

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "stock-wave-attribution.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "postgres:",
                        "  event_news_dsn: postgresql://tester@localhost:5432/event_news",
                        "  event_quant_dsn: postgresql://tester@localhost:5432/event_quant",
                        "tushare:",
                        "  token: dummy-token",
                        "  http_url: http://example.com",
                        "paths:",
                        f"  analysis_dir: {tmpdir}/outputs/analysis",
                        f"  plot_dir: {tmpdir}/data/plots",
                        f"  cache_dir: {tmpdir}/data/stock_cache",
                        "chatgpt:",
                        "  enabled: false",
                        "  node_bin: node",
                        "  script_path: ../chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "量价数据窗口被截断"):
                module.run_local_attribution_task(
                    stock_name="长飞光纤",
                    ts_code="601869.SH",
                    start_date="2025-01-01",
                    end_date="2026-04-02",
                    sample_label="光纤概念",
                    config_path=config_path,
                    db_connect=fake_connect,
                    stock_bundle_fetcher=fake_stock_bundle_fetcher,
                    concept_fetcher=fake_concept_fetcher,
                    news_fetcher=fake_news_fetcher,
                    akshare_stock_bundle_fetcher=None,
                    quant_bundle_persister=None,
                    tushare_concept_bundle_fetcher=None,
                    concept_bundle_persister=None,
                )

    def test_run_local_attribution_task_backfills_truncated_window_from_akshare_then_refetches_db(self):
        module = load_module()
        calls = {
            "stock_bundle_fetch": 0,
            "akshare_fetch": [],
            "persist": [],
        }

        class _DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_connect(_dsn):
            return _DummyConnection()

        truncated_bundle = {
            "raw_stock_daily_qfq": pd.DataFrame(
                [
                    {"trade_date": "2025-09-10", "open_qfq": 87.0, "high_qfq": 88.0, "low_qfq": 86.0, "close_qfq": 87.61},
                    {"trade_date": "2026-03-09", "open_qfq": 208.0, "high_qfq": 214.0, "low_qfq": 205.0, "close_qfq": 211.91},
                ]
            ),
            "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-09-10", "turnover_rate": 1.0}]),
            "raw_moneyflow": pd.DataFrame([{"trade_date": "2026-03-09", "net_mf_amount": 1.0}]),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-01-28", "limit_status": "U"}]),
        }
        full_bundle = {
            "raw_stock_daily_qfq": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "open_qfq": 28.0, "high_qfq": 29.0, "low_qfq": 27.5, "close_qfq": 28.25},
                    {"trade_date": "2026-04-07", "open_qfq": 212.0, "high_qfq": 214.0, "low_qfq": 208.0, "close_qfq": 211.91},
                ]
            ),
            "raw_daily_basic": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "turnover_rate": 1.6},
                    {"trade_date": "2026-04-07", "turnover_rate": 2.1},
                ]
            ),
            "raw_moneyflow": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "net_mf_amount": 8888.0},
                    {"trade_date": "2026-04-07", "net_mf_amount": 9999.0},
                ]
            ),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-01-28", "limit_status": "U"}]),
        }

        def fake_stock_bundle_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            self.assertEqual(ts_code, "688525.SH")
            calls["stock_bundle_fetch"] += 1
            return truncated_bundle if calls["stock_bundle_fetch"] == 1 else full_bundle

        def fake_concept_fetcher(conn, ts_code, start_date, end_date):
            return {}, {}

        def fake_news_fetcher(conn, start_date, end_date, keywords):
            return []

        def fake_akshare_fetcher(ts_code, start_date, end_date):
            calls["akshare_fetch"].append((ts_code, start_date, end_date))
            self.assertEqual(ts_code, "688525.SH")
            self.assertEqual(start_date, "20250101")
            self.assertEqual(end_date, "20260407")
            return full_bundle

        def fake_quant_bundle_persister(conn, ts_code, frames):
            self.assertIsInstance(conn, _DummyConnection)
            calls["persist"].append((ts_code, sorted(frames.keys())))

        def fake_runner(case_context, stock_bundle, news_evidence, concept_frames, concept_labels, **kwargs):
            self.assertEqual(case_context["stock_name"], "佰维存储")
            self.assertEqual(stock_bundle["raw_stock_daily_qfq"].iloc[0]["trade_date"], "2025-01-02")
            self.assertEqual(stock_bundle["raw_stock_daily_qfq"].iloc[-1]["trade_date"], "2026-04-07")
            return {
                "report_path": "/tmp/fake-report.md",
                "plot_path": "/tmp/fake-plot.png",
                "report_contract_path": "/tmp/contract.md",
                "call_chain": ["runtime/wave_segmentation.py"],
                "wave_count": 1,
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "stock-wave-attribution.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "postgres:",
                        "  event_news_dsn: postgresql://tester@localhost:5432/event_news",
                        "  event_quant_dsn: postgresql://tester@localhost:5432/event_quant",
                        "tushare:",
                        "  token: dummy-token",
                        "  http_url: http://example.com",
                        "paths:",
                        f"  analysis_dir: {tmpdir}/outputs/analysis",
                        f"  plot_dir: {tmpdir}/data/plots",
                        f"  cache_dir: {tmpdir}/data/stock_cache",
                        "chatgpt:",
                        "  enabled: false",
                        "  node_bin: node",
                        "  script_path: ../chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
                    ]
                ),
                encoding="utf-8",
            )

            result = module.run_local_attribution_task(
                stock_name="佰维存储",
                ts_code="688525.SH",
                start_date="2025-01-01",
                end_date="2026-04-07",
                sample_label="存储芯片",
                config_path=config_path,
                db_connect=fake_connect,
                stock_bundle_fetcher=fake_stock_bundle_fetcher,
                concept_fetcher=fake_concept_fetcher,
                news_fetcher=fake_news_fetcher,
                akshare_stock_bundle_fetcher=fake_akshare_fetcher,
                quant_bundle_persister=fake_quant_bundle_persister,
                tushare_concept_bundle_fetcher=None,
                concept_bundle_persister=None,
                attribution_runner=fake_runner,
            )

        self.assertEqual(calls["stock_bundle_fetch"], 2)
        self.assertEqual(calls["akshare_fetch"], [("688525.SH", "20250101", "20260407")])
        self.assertEqual(
            calls["persist"],
            [
                (
                    "688525.SH",
                    ["raw_daily_basic", "raw_limit_list_d", "raw_moneyflow", "raw_stock_daily_qfq"],
                )
            ],
        )
        self.assertEqual(result["report_path"], "/tmp/fake-report.md")

    def test_run_local_attribution_task_backfills_missing_concepts_from_tushare_proxy_then_refetches_db(self):
        module = load_module()
        calls = {
            "concept_fetch": 0,
            "tushare_concept_fetch": [],
            "persist": [],
            "keywords": None,
        }

        class _DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_connect(_dsn):
            return _DummyConnection()

        stock_bundle = {
            "raw_stock_daily_qfq": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "open_qfq": 28.0, "high_qfq": 29.0, "low_qfq": 27.5, "close_qfq": 28.25},
                    {"trade_date": "2026-04-07", "open_qfq": 212.0, "high_qfq": 214.0, "low_qfq": 208.0, "close_qfq": 211.91},
                ]
            ),
            "raw_daily_basic": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "turnover_rate": 1.6},
                    {"trade_date": "2026-04-07", "turnover_rate": 2.1},
                ]
            ),
            "raw_moneyflow": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "net_mf_amount": 8888.0},
                    {"trade_date": "2026-04-07", "net_mf_amount": 9999.0},
                ]
            ),
            "raw_limit_list_d": pd.DataFrame([{"trade_date": "2026-01-28", "limit_status": "U"}]),
        }
        concept_frames = {
            "BK9999": pd.DataFrame(
                [
                    {"trade_date": "2025-01-02", "close": 100.0},
                    {"trade_date": "2026-04-07", "close": 132.0},
                ]
            )
        }
        concept_labels = {"BK9999": {"code": "BK9999", "name": "算力PCB"}}
        concept_bundle = {
            "ana_stock_concept_map": pd.DataFrame(
                [
                    {
                        "ts_code": "300476.SZ",
                        "concept_code": "BK9999",
                        "concept_name": "算力PCB",
                        "mapping_asof_date": "20260407",
                        "map_source": "akshare_em_concept",
                        "updated_at": "2026-04-07T19:00:00+08:00",
                    }
                ]
            ),
            "ana_concept_day": pd.DataFrame(
                [
                    {
                        "concept_code": "BK9999",
                        "trade_date": "2025-01-02",
                        "concept_name": "算力PCB",
                        "close": 100.0,
                        "pct_change": 1.2,
                        "vol": 10.0,
                        "turnover_rate": 3.1,
                    },
                    {
                        "concept_code": "BK9999",
                        "trade_date": "2026-04-07",
                        "concept_name": "算力PCB",
                        "close": 132.0,
                        "pct_change": 0.8,
                        "vol": 11.0,
                        "turnover_rate": 3.4,
                    },
                ]
            ),
        }

        def fake_stock_bundle_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            return stock_bundle

        def fake_concept_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            self.assertEqual(ts_code, "300476.SZ")
            calls["concept_fetch"] += 1
            if calls["concept_fetch"] == 1:
                return {}, {}
            return concept_frames, concept_labels

        def fake_news_fetcher(conn, start_date, end_date, keywords):
            calls["keywords"] = list(keywords)
            return []

        def fake_tushare_concept_fetcher(ts_code, start_date, end_date, token, http_url):
            calls["tushare_concept_fetch"].append((ts_code, start_date, end_date, token, http_url))
            self.assertEqual(ts_code, "300476.SZ")
            self.assertEqual(start_date, "20250101")
            self.assertEqual(end_date, "20260407")
            self.assertEqual(token, "dummy-token")
            self.assertEqual(http_url, "http://example.com")
            return concept_bundle

        def fake_concept_bundle_persister(conn, ts_code, frames):
            self.assertIsInstance(conn, _DummyConnection)
            calls["persist"].append((ts_code, sorted(frames.keys())))

        def fake_runner(case_context, stock_bundle, news_evidence, concept_frames, concept_labels, **kwargs):
            self.assertEqual(case_context["stock_name"], "胜宏科技")
            self.assertEqual(sorted(concept_frames.keys()), ["BK9999"])
            self.assertEqual(concept_labels["BK9999"]["name"], "算力PCB")
            return {
                "report_path": "/tmp/fake-report.md",
                "plot_path": "/tmp/fake-plot.png",
                "report_contract_path": "/tmp/contract.md",
                "call_chain": ["runtime/wave_segmentation.py"],
                "wave_count": 1,
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "stock-wave-attribution.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "postgres:",
                        "  event_news_dsn: postgresql://tester@localhost:5432/event_news",
                        "  event_quant_dsn: postgresql://tester@localhost:5432/event_quant",
                        "tushare:",
                        "  token: dummy-token",
                        "  http_url: http://example.com",
                        "paths:",
                        f"  analysis_dir: {tmpdir}/outputs/analysis",
                        f"  plot_dir: {tmpdir}/data/plots",
                        f"  cache_dir: {tmpdir}/data/stock_cache",
                        "chatgpt:",
                        "  enabled: false",
                        "  node_bin: node",
                        "  script_path: ../chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
                    ]
                ),
                encoding="utf-8",
            )

            result = module.run_local_attribution_task(
                stock_name="胜宏科技",
                ts_code="300476.SZ",
                start_date="2025-01-01",
                end_date="2026-04-07",
                sample_label="算力PCB",
                config_path=config_path,
                db_connect=fake_connect,
                stock_bundle_fetcher=fake_stock_bundle_fetcher,
                concept_fetcher=fake_concept_fetcher,
                news_fetcher=fake_news_fetcher,
                tushare_concept_bundle_fetcher=fake_tushare_concept_fetcher,
                concept_bundle_persister=fake_concept_bundle_persister,
                attribution_runner=fake_runner,
            )

        self.assertEqual(calls["concept_fetch"], 2)
        self.assertEqual(
            calls["tushare_concept_fetch"],
            [("300476.SZ", "20250101", "20260407", "dummy-token", "http://example.com")],
        )
        self.assertEqual(calls["persist"], [("300476.SZ", ["ana_concept_day", "ana_stock_concept_map"])])
        self.assertIn("算力PCB", calls["keywords"])
        self.assertEqual(result["report_path"], "/tmp/fake-report.md")

    def test_run_local_attribution_task_can_skip_concepts_explicitly(self):
        module = load_module()
        stock_bundle = {
            "raw_stock_daily_qfq": pd.DataFrame(
                [
                    {
                        "trade_date": "2025-01-02",
                        "open_qfq": 10.0,
                        "high_qfq": 10.5,
                        "low_qfq": 9.8,
                        "close_qfq": 10.4,
                    },
                    {
                        "trade_date": "2026-04-07",
                        "open_qfq": 15.0,
                        "high_qfq": 15.5,
                        "low_qfq": 14.8,
                        "close_qfq": 15.2,
                    },
                ]
            ),
            "raw_daily_basic": pd.DataFrame(),
            "raw_moneyflow": pd.DataFrame(),
            "raw_limit_list_d": pd.DataFrame(),
        }
        calls = {"news_keywords": None}

        class _DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_connect(_dsn):
            return _DummyConnection()

        def fake_stock_bundle_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            return stock_bundle

        def fake_concept_fetcher(*_args, **_kwargs):
            raise AssertionError("skip_concept=True 时不应读取概念数据")

        def fake_tushare_concept_fetcher(*_args, **_kwargs):
            raise AssertionError("skip_concept=True 时不应触发 Tushare 概念补库")

        def fake_news_fetcher(conn, start_date, end_date, keywords):
            self.assertIsInstance(conn, _DummyConnection)
            calls["news_keywords"] = list(keywords)
            return []

        def fake_runner(case_context, stock_bundle, news_evidence, concept_frames, concept_labels, **kwargs):
            self.assertEqual(case_context["stock_name"], "奥瑞德")
            self.assertEqual(concept_frames, {})
            self.assertEqual(concept_labels, {})
            return {
                "report_path": "/tmp/aurora-report.md",
                "plot_path": "/tmp/aurora-plot.png",
                "report_contract_path": "/tmp/contract.md",
                "call_chain": ["runtime/wave_segmentation.py"],
                "wave_count": 1,
            }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "stock-wave-attribution.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "postgres:",
                        "  event_news_dsn: postgresql://tester@localhost:5432/event_news",
                        "  event_quant_dsn: postgresql://tester@localhost:5432/event_quant",
                        "tushare:",
                        "  token: invalid-token",
                        "  http_url: http://example.com",
                        "paths:",
                        f"  analysis_dir: {tmpdir}/outputs/analysis",
                        f"  plot_dir: {tmpdir}/data/plots",
                        f"  cache_dir: {tmpdir}/data/stock_cache",
                        "chatgpt:",
                        "  enabled: false",
                        "  node_bin: node",
                        "  script_path: ../chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
                    ]
                ),
                encoding="utf-8",
            )

            result = module.run_local_attribution_task(
                stock_name="奥瑞德",
                ts_code="600666.SH",
                start_date="2025-09-01",
                end_date="2026-04-09",
                sample_label="算力",
                skip_concept=True,
                config_path=config_path,
                db_connect=fake_connect,
                stock_bundle_fetcher=fake_stock_bundle_fetcher,
                concept_fetcher=fake_concept_fetcher,
                news_fetcher=fake_news_fetcher,
                tushare_concept_bundle_fetcher=fake_tushare_concept_fetcher,
                attribution_runner=fake_runner,
            )

        self.assertEqual(calls["news_keywords"], ["奥瑞德", "算力"])
        self.assertEqual(result["report_path"], "/tmp/aurora-report.md")

    def test_prepare_agent_rerank_parser_accepts_skip_concept(self):
        module = load_module()

        parser = module._build_prepare_agent_rerank_parser()
        args = parser.parse_args(
            [
                "--stock-name",
                "奥瑞德",
                "--ts-code",
                "600666.SH",
                "--start-date",
                "2025-09-01",
                "--end-date",
                "2026-04-09",
                "--sample-label",
                "算力",
                "--task-id",
                "attr-test",
                "--skip-concept",
            ]
        )

        self.assertTrue(args.skip_concept)

    def test_build_local_verdict_marks_skip_concept_in_notes(self):
        module = load_module()

        verdict = module._build_local_verdict(
            case_context={"stock_name": "奥瑞德", "sample_label": "算力"},
            selected_news=[
                {
                    "title": "奥瑞德进入算力租赁涨停板",
                    "raw_text": "奥瑞德进入算力租赁涨停板并强化智算中心预期。",
                }
            ],
            concept_rows=[],
            quant_rows=[{"metric": "区间涨幅", "value": "73.54%", "evidence": "close_qfq", "interpretation": "观察总涨幅"}],
            skip_concept=True,
        )

        self.assertIn("显式跳过概念联动", verdict["final_verdict"]["notes"])
        self.assertIn("显式跳过概念联动", verdict["conclusion_rows"][0]["notes"])

    def test_collect_wave_news_rows_respects_explicit_lookback_days(self):
        module = load_module()

        rows = module._collect_wave_news_rows(
            news_evidence=[
                {
                    "published_at": "2025-06-25 09:00",
                    "source_id": "zsxq_damao",
                    "title": "过早旧闻",
                    "raw_text": "不应进入 14 天窗口。",
                    "url": "https://example.com/old",
                },
                {
                    "published_at": "2025-07-31 09:00",
                    "source_id": "zsxq_damao",
                    "title": "启动前强信号",
                    "raw_text": "应进入 14 天窗口。",
                    "url": "https://example.com/pre",
                },
                {
                    "published_at": "2025-08-12 09:00",
                    "source_id": "zsxq_saidao_touyan",
                    "title": "波段内催化",
                    "raw_text": "位于波段内。",
                    "url": "https://example.com/in",
                },
            ],
            waves=[
                {
                    "start_date": "2025-08-11",
                    "peak_date": "2025-08-13",
                }
            ],
            lookback_days=14,
        )

        titles = [row["title"] for row in rows]
        self.assertEqual(titles, ["启动前强信号", "波段内催化"])

    def test_build_local_verdict_ignores_sample_label_bias_and_single_noise_terms(self):
        module = load_module()

        verdict = module._build_local_verdict(
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

    def test_main_run_prints_json_result(self):
        module = load_module()
        buffer = io.StringIO()

        def fake_run_local_attribution_task(**kwargs):
            self.assertEqual(kwargs["stock_name"], "五洲新春")
            self.assertEqual(kwargs["ts_code"], "603667.SH")
            return {"report_path": "/tmp/fake-report.md", "plot_path": "/tmp/fake-plot.png"}

        module.run_local_attribution_task = fake_run_local_attribution_task

        with redirect_stdout(buffer):
            status = module.main(
                [
                    "run",
                    "--stock-name",
                    "五洲新春",
                    "--ts-code",
                    "603667.SH",
                    "--start-date",
                    "2025-11-05",
                    "--end-date",
                    "2026-01-22",
                    "--sample-label",
                    "机器人概念",
                ]
            )

        self.assertEqual(status, 0)
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload["report_path"], "/tmp/fake-report.md")
        self.assertEqual(payload["plot_path"], "/tmp/fake-plot.png")


if __name__ == "__main__":
    unittest.main()
