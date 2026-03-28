import importlib.util
import json
import tempfile
import types
import unittest
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

    def test_orchestrator_writes_report_and_records_skill_local_call_chain(self):
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
            self.assertIn("| 序号 | 时间 | 来源 | 标题 | 链接 |", markdown)
            self.assertIn("### 证据原文", markdown)
            self.assertIn("#### 证据 1", markdown)
            self.assertIn("```text", markdown)
            self.assertEqual(chatgpt_calls, ["plain", "search"])
            self.assertIn("runtime/wave_segmentation.py", result["call_chain"])
            self.assertIn("runtime/wave_plotting.py", result["call_chain"])
            self.assertIn("runtime/attribution_data.py", result["call_chain"])
            self.assertIn("skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs", result["call_chain"])
            self.assertTrue(Path(result["plot_path"]).exists())
            self.assertTrue(Path(result["report_contract_path"]).exists())


if __name__ == "__main__":
    unittest.main()
