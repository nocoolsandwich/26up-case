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

    def test_select_news_evidence_prefers_wave_start_proximity_and_deduplicates(self):
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

        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0]["title"], "五洲新春切入机器人丝杠")
        self.assertEqual(selected[0]["source_id"], "zsxq_saidao_touyan")
        self.assertEqual(selected[1]["title"], "机器人板块回暖")

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
            self.assertEqual(markdown.count("#### 证据 "), 2)
            self.assertEqual(markdown.count("五洲新春切入机器人丝杠"), 3)
            self.assertIn("机器人板块回暖", markdown)
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
                "plot_relpath": "../../data/plots/603667_SH_wave_candles.png",
                "timeline_rows": [
                    {"time": "2025-11-05 19:37", "category": "主题催化", "event": "小鹏科技日", "impact": "打开机器人主题预期", "source": "zsxq_zhuwang"}
                ],
                "wave_rows": [
                    {"wave_id": "W1", "period": "2025-11-28 -> 2026-01-22", "gain_pct": "102.25%", "review": "up_valid", "main_cause": "机器人跨年主线", "alt_cause": "小鹏科技日"}
                ],
                "news_rows": [
                    {"published_at": "2025-12-30 14:08", "source_id": "zsxq_zhuwang", "title": "机器人板块回血", "raw_text": "开启跨年主线行情", "url": "https://example.com/news"}
                ],
                "quant_rows": [
                    {"metric": "区间涨幅", "value": "105.08%", "evidence": "close_qfq 44.91 -> 90.83", "interpretation": "显著强于大盘"}
                ],
                "concept_rows": [
                    {"concept_name": "人形机器人", "concept_code": "886069.TI", "period_return_pct": "11.53%", "close_corr": "0.9439", "ret_corr": "0.4718", "interpretation": "同步性最高"}
                ],
                "one_line_logic": "机器人T链主线驱动，小鹏科技日点火，跨年情绪强化。",
                "final_verdict": {
                    "main_cause": "机器人T链 / 丝杠平台化",
                    "alt_cause": "机器人板块跨年情绪强化",
                    "final_judgment": "这轮主升更偏向机器人T链主线，不是泛机器人概念跟涨。",
                    "notes": "启动与加速阶段均有本地证据支撑。",
                    "confidence": "中高",
                },
                "conclusion_rows": [
                    {"dimension": "主因", "value": "机器人主线 + 跨年情绪", "confidence": "中高", "notes": "与案例颗粒度对齐"}
                ],
            }
        )

        self.assertIn("## 事件时间线表", markdown)
        self.assertIn("## 波段分段归因表", markdown)
        self.assertIn("## 本地 news 证据表", markdown)
        self.assertIn("## 量价验证表", markdown)
        self.assertIn("## 概念联动验证表", markdown)
        self.assertIn("## 结论与置信度表", markdown)
        self.assertIn("- 一句话逻辑：`机器人T链主线驱动，小鹏科技日点火，跨年情绪强化。`", markdown)
        self.assertIn("| 时间 | 事件类别 | 事件 | 对波段影响 | 来源 |", markdown)
        self.assertIn("| 波段 | 区间 | 涨幅 | 波段审查 | 主因 | 备选 |", markdown)
        self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", markdown)
        self.assertIn("### 证据原文", markdown)
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
            self.assertIn("## 事件时间线表", markdown)
            self.assertIn("## 结论与置信度表", markdown)
            self.assertIn("## 综合裁决", markdown)
            self.assertIn("- 一句话逻辑：`", markdown)
            self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", markdown)
            self.assertIn("### 证据原文", markdown)
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
                        {"trade_date": "2025-11-06", "open_qfq": 10.1, "high_qfq": 10.8, "low_qfq": 10.0, "close_qfq": 10.6},
                    ]
                ),
                "raw_daily_basic": pd.DataFrame([{"trade_date": "2025-11-05", "turnover_rate": 5.0}]),
                "raw_moneyflow": pd.DataFrame([{"trade_date": "2025-11-06", "net_mf_amount": 1200.0}]),
                "raw_limit_list_d": pd.DataFrame([{"trade_date": "2025-11-06", "limit_status": "U"}]),
            }

        def fake_concept_fetcher(conn, ts_code, start_date, end_date):
            self.assertIsInstance(conn, _DummyConnection)
            return (
                {
                    "886069.TI": pd.DataFrame(
                        [
                            {"trade_date": "2025-11-05", "close": 100.0},
                            {"trade_date": "2025-11-06", "close": 103.0},
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
                        f"  analysis_dir: {tmpdir}/docs/analysis",
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
        self.assertTrue(str(calls["analysis_dir"]).endswith("/docs/analysis"))
        self.assertTrue(str(calls["plot_dir"]).endswith("/data/plots"))
        self.assertEqual(result["report_path"], "/tmp/fake-report.md")

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
