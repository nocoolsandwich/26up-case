import unittest

from scripts.build_marco_source_review import find_issues


class MarcoSourceReviewTest(unittest.TestCase):
    def test_explicit_event_chain_is_not_flagged(self):
        text = (
            "事件：6月13日以色列对伊朗核设施、导弹工厂及军事指挥体系发动大规模空袭，并称行动将持续；"
            "伊朗随后向以色列发射导弹报复，冲突迅速升级。"
            "市场：6月16日A股三大指数反弹，上证指数收涨0.35%，市场继续交易中东冲突对能源、航运与避险资产的影响。"
        )
        self.assertEqual(find_issues(text), [])


if __name__ == "__main__":
    unittest.main()
