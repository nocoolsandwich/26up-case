import unittest

from board_top_service import _split_cached_missing


class SplitCachedMissingTest(unittest.TestCase):
    def test_split_cached_missing(self):
        all_names = ["A", "B", "C", "D"]
        cached = {"A": 1.2, "C": -0.3}

        cached_names, missing = _split_cached_missing(all_names, cached)

        self.assertEqual(cached_names, ["A", "C"])
        self.assertEqual(missing, ["B", "D"])


if __name__ == "__main__":
    unittest.main()
