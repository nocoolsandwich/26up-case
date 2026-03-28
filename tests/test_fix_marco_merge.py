import unittest
import pandas as pd

from scripts.fix_marco_t0_sh_change import merge_index_data


class MergeIndexDataTest(unittest.TestCase):
    def test_merge_uses_fallback_when_primary_missing_date(self):
        primary = pd.DataFrame(
            {
                "date": ["2008-09-18"],
                "close": [1895.84],
                "amount": [123.0],
            }
        )
        fallback = pd.DataFrame(
            {
                "date": ["2008-09-18", "2008-09-19"],
                "close": [1895.84, 2075.09],
            }
        )
        merged = merge_index_data(primary, fallback)
        dates = set(merged["date"].tolist())
        self.assertIn("2008-09-19", dates)
        row = merged[merged["date"] == "2008-09-19"].iloc[0]
        self.assertAlmostEqual(float(row["close"]), 2075.09, places=8)


if __name__ == "__main__":
    unittest.main()
