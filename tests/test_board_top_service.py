import unittest

from board_top_service import _calc_period_change


class CalcPeriodChangeTest(unittest.TestCase):
    def test_calc_period_change_uses_first_and_last_close(self):
        rows = [
            {"收盘价": 100.0},
            {"收盘价": 110.0},
            {"收盘价": 130.0},
        ]
        self.assertAlmostEqual(_calc_period_change(rows), 30.0, places=6)

    def test_calc_period_change_returns_none_when_insufficient(self):
        self.assertIsNone(_calc_period_change([]))
        self.assertIsNone(_calc_period_change([{"收盘价": 100.0}]))


if __name__ == "__main__":
    unittest.main()
