import unittest

import pandas as pd

from scripts.wave_segmentation import segment_ma_trend_waves, segment_price_waves


class WaveSegmentationTest(unittest.TestCase):
    def test_segment_price_waves_finds_two_major_waves(self):
        df = pd.DataFrame(
            [
                {"trade_date": "2025-09-10", "close_qfq": 10.0},
                {"trade_date": "2025-09-11", "close_qfq": 11.0},
                {"trade_date": "2025-09-12", "close_qfq": 12.0},
                {"trade_date": "2025-09-15", "close_qfq": 13.0},
                {"trade_date": "2025-09-16", "close_qfq": 14.0},
                {"trade_date": "2025-09-17", "close_qfq": 15.0},
                {"trade_date": "2025-09-18", "close_qfq": 16.0},
                {"trade_date": "2025-09-19", "close_qfq": 14.0},
                {"trade_date": "2025-09-22", "close_qfq": 13.0},
                {"trade_date": "2025-09-23", "close_qfq": 12.5},
                {"trade_date": "2025-09-24", "close_qfq": 13.5},
                {"trade_date": "2025-09-25", "close_qfq": 15.0},
                {"trade_date": "2025-09-26", "close_qfq": 16.5},
                {"trade_date": "2025-09-29", "close_qfq": 18.0},
                {"trade_date": "2025-09-30", "close_qfq": 19.5},
                {"trade_date": "2025-10-08", "close_qfq": 21.0},
            ]
        )

        waves = segment_price_waves(
            df,
            price_col="close_qfq",
            min_wave_gain=0.35,
            min_pullback=0.15,
            min_bars=4,
        )

        self.assertEqual(len(waves), 2)
        self.assertEqual(waves[0]["start_date"], "2025-09-10")
        self.assertEqual(waves[0]["peak_date"], "2025-09-18")
        self.assertEqual(waves[1]["start_date"], "2025-09-23")
        self.assertEqual(waves[1]["peak_date"], "2025-10-08")

    def test_segment_price_waves_ignores_small_moves(self):
        df = pd.DataFrame(
            [
                {"trade_date": "2025-09-10", "close_qfq": 10.0},
                {"trade_date": "2025-09-11", "close_qfq": 10.2},
                {"trade_date": "2025-09-12", "close_qfq": 10.5},
                {"trade_date": "2025-09-15", "close_qfq": 10.1},
                {"trade_date": "2025-09-16", "close_qfq": 10.4},
            ]
        )

        waves = segment_price_waves(
            df,
            price_col="close_qfq",
            min_wave_gain=0.35,
            min_pullback=0.15,
            min_bars=4,
        )

        self.assertEqual(waves, [])

    def test_segment_ma_trend_waves_splits_two_stage_trend(self):
        dates = pd.date_range("2025-09-01", periods=55, freq="B")
        close_prices = (
            [10.0, 10.2, 10.4, 10.7, 11.0, 11.3, 11.7, 12.1, 12.6, 13.1]
            + [13.5, 13.9, 14.2, 14.6, 15.0, 15.3, 15.7, 16.0, 15.8, 15.5]
            + [15.2, 14.8, 14.4, 14.0, 13.7, 13.5, 13.4, 13.6, 13.9, 14.3]
            + [14.8, 15.4, 16.0, 16.7, 17.5, 18.3, 19.2, 20.0, 20.8, 21.5]
            + [22.1, 22.6, 23.0, 23.3, 23.5, 23.3, 23.1, 22.8, 22.5, 22.2]
            + [21.9, 21.6, 21.3, 21.1, 20.9]
        )
        df = pd.DataFrame({"trade_date": dates, "close_qfq": close_prices})

        waves = segment_ma_trend_waves(
            df,
            price_col="close_qfq",
            ma_window=10,
            signal_window=20,
            negative_streak=5,
            positive_streak=3,
            below_signal_streak=3,
            min_bars=8,
        )

        self.assertEqual(len(waves), 2)
        self.assertLess(waves[0]["peak_idx"], waves[1]["start_idx"])
        self.assertLess(waves[0]["peak_price"], waves[1]["peak_price"])
        self.assertGreater(waves[0]["wave_gain_pct"], 40.0)
        self.assertGreater(waves[1]["wave_gain_pct"], 40.0)

    def test_segment_ma_trend_waves_keeps_single_persistent_trend(self):
        dates = pd.date_range("2025-09-01", periods=50, freq="B")
        close_prices = [
            10.0,
            10.1,
            10.2,
            10.4,
            10.6,
            10.9,
            11.1,
            11.4,
            11.6,
            11.9,
            12.2,
            12.5,
            12.8,
            13.1,
            13.5,
            13.9,
            14.2,
            14.5,
            14.9,
            15.2,
            15.5,
            15.8,
            16.0,
            16.3,
            16.6,
            16.9,
            17.2,
            17.5,
            17.8,
            18.1,
            18.4,
            18.7,
            19.0,
            19.2,
            19.5,
            19.8,
            20.0,
            20.2,
            20.4,
            20.6,
            20.8,
            21.0,
            21.2,
            21.3,
            21.4,
            21.5,
            21.6,
            21.7,
            21.8,
            21.9,
        ]
        df = pd.DataFrame({"trade_date": dates, "close_qfq": close_prices})

        waves = segment_ma_trend_waves(
            df,
            price_col="close_qfq",
            ma_window=10,
            signal_window=20,
            negative_streak=5,
            positive_streak=3,
            below_signal_streak=3,
            min_bars=8,
        )

        self.assertEqual(len(waves), 1)

    def test_segment_ma_trend_waves_can_start_before_full_ma_window(self):
        dates = pd.date_range("2025-09-01", periods=12, freq="B")
        close_prices = [10.0, 10.5, 11.0, 11.6, 12.3, 13.1, 14.0, 14.8, 15.5, 16.0, 16.3, 16.5]
        df = pd.DataFrame({"trade_date": dates, "close_qfq": close_prices})

        waves = segment_ma_trend_waves(
            df,
            price_col="close_qfq",
            ma_window=10,
            signal_window=20,
            negative_streak=5,
            positive_streak=3,
            below_signal_streak=3,
            min_bars=5,
        )

        self.assertEqual(len(waves), 1)
        self.assertLess(waves[0]["start_idx"], 9)


if __name__ == "__main__":
    unittest.main()
