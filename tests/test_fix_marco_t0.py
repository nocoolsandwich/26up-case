import unittest
import pandas as pd

from scripts.fix_marco_t0_sh_change import (
    apply_t0_amount,
    apply_t0_sh_change,
    build_amount_map,
    build_sh_change_map,
)


class FixMarcoT0Test(unittest.TestCase):
    def test_build_and_apply(self):
        idx = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-03", "2026-03-04", "2026-03-05"]),
                "close": [4122.676, 4082.474, 4108.567],
            }
        )
        m = build_sh_change_map(idx)
        self.assertAlmostEqual(m["2026-03-04"], (4082.474 / 4122.676 - 1), places=12)

        src = pd.DataFrame(
            {
                "A股定价日T0": [pd.Timestamp("2026-03-04"), pd.Timestamp("2026-03-06")],
                "T0上证涨跌幅": [0.0, 0.1234],
            }
        )
        out, changed, missing = apply_t0_sh_change(src, m, "T0上证涨跌幅")
        self.assertEqual(changed, 1)
        self.assertEqual(missing, ["2026-03-06"])
        self.assertAlmostEqual(out.loc[0, "T0上证涨跌幅"], m["2026-03-04"], places=12)
        self.assertAlmostEqual(out.loc[1, "T0上证涨跌幅"], 0.1234, places=12)

    def test_build_and_apply_amount(self):
        idx = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-03", "2026-03-04"]),
                "amount": [1.2e9, 1.5e9],
            }
        )
        m = build_amount_map(idx)
        self.assertAlmostEqual(m["2026-03-03"], 12000.0, places=8)

        src = pd.DataFrame(
            {
                "A股定价日T0": [pd.Timestamp("2026-03-03"), pd.Timestamp("2026-03-06")],
                "T0成交额（亿元）": [0.0, 1234.0],
            }
        )
        out, changed, missing = apply_t0_amount(src, m)
        self.assertEqual(changed, 1)
        self.assertEqual(missing, ["2026-03-06"])
        self.assertAlmostEqual(out.loc[0, "T0成交额（亿元）"], 12000.0, places=8)


if __name__ == "__main__":
    unittest.main()
