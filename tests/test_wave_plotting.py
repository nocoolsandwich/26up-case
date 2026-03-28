import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.wave_plotting import plot_candlestick_waves


class WavePlottingTest(unittest.TestCase):
    def test_plot_candlestick_waves_renders_png_with_wave_metadata(self):
        df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "open_qfq": 10.0, "high_qfq": 10.8, "low_qfq": 9.9, "close_qfq": 10.6},
                {"trade_date": "2025-12-01", "open_qfq": 10.6, "high_qfq": 11.2, "low_qfq": 10.4, "close_qfq": 11.0},
                {"trade_date": "2025-12-02", "open_qfq": 11.0, "high_qfq": 11.7, "low_qfq": 10.9, "close_qfq": 11.5},
                {"trade_date": "2025-12-03", "open_qfq": 11.4, "high_qfq": 12.2, "low_qfq": 11.3, "close_qfq": 12.0},
            ]
        )
        waves = [
            {
                "start_date": "2025-11-28",
                "peak_date": "2025-12-03",
                "start_price": 10.6,
                "peak_price": 12.0,
                "wave_gain_pct": 13.2075,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "candles.png"

            result = plot_candlestick_waves(
                df=df,
                waves=waves,
                output_path=output_path,
                title="测试K线图",
            )

            self.assertTrue(output_path.exists())
            self.assertGreater(output_path.stat().st_size, 0)
            self.assertEqual(result["candles_plotted"], 4)
            self.assertEqual(result["waves_annotated"], 1)
            self.assertEqual(result["output_path"], str(output_path))

    def test_plot_candlestick_waves_requires_ohlc_columns(self):
        df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "close_qfq": 10.6},
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "candles.png"

            with self.assertRaises(ValueError) as ctx:
                plot_candlestick_waves(
                    df=df,
                    waves=[],
                    output_path=output_path,
                    title="测试K线图",
                )

        self.assertIn("missing required columns", str(ctx.exception))

    def test_plot_candlestick_waves_supports_enhanced_style_preview(self):
        df = pd.DataFrame(
            [
                {"trade_date": "2025-11-28", "open_qfq": 10.0, "high_qfq": 10.8, "low_qfq": 9.9, "close_qfq": 10.6},
                {"trade_date": "2025-12-01", "open_qfq": 10.6, "high_qfq": 11.2, "low_qfq": 10.4, "close_qfq": 11.0},
                {"trade_date": "2025-12-02", "open_qfq": 11.0, "high_qfq": 11.7, "low_qfq": 10.9, "close_qfq": 11.5},
                {"trade_date": "2025-12-03", "open_qfq": 11.4, "high_qfq": 12.2, "low_qfq": 11.3, "close_qfq": 12.0},
            ]
        )
        waves = [
            {
                "start_date": "2025-11-28",
                "peak_date": "2025-12-03",
                "start_price": 10.6,
                "peak_price": 12.0,
                "wave_gain_pct": 13.2075,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "candles-enhanced.png"

            result = plot_candlestick_waves(
                df=df,
                waves=waves,
                output_path=output_path,
                title="测试增强版K线图",
                style="enhanced",
            )

            self.assertTrue(output_path.exists())
            self.assertEqual(result["style"], "enhanced")


if __name__ == "__main__":
    unittest.main()
