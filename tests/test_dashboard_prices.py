"""列表与详情共用的实时盘口价拉取 helper(_live_outcome_prices)。

回归点:详情曾只读缓存价、列表读实时价 → 同一盘两处显示不同价。两处现共用此 helper。
"""
import unittest

from poly_fight import dashboard


class _FakeClient:
    def __init__(self, markets, *, raise_exc=False):
        self._markets = markets
        self._raise = raise_exc
        self.calls = []

    def gamma(self, path, **params):
        self.calls.append((path, params))
        if self._raise:
            raise RuntimeError("boom")
        return self._markets


class LiveOutcomePricesTest(unittest.TestCase):
    def test_maps_condition_to_live_prices(self):
        client = _FakeClient([
            {"conditionId": "0xABC", "outcomes": ["A", "B"], "outcomePrices": ["0.715", "0.285"]},
        ])
        out = dashboard._live_outcome_prices(client, ["0xabc"])
        self.assertEqual(out, {"0xabc": [0.715, 0.285]})
        # condition_ids 去重 + 小写后传给 gamma
        self.assertEqual(client.calls[0][0], "/markets")

    def test_none_client_returns_empty(self):
        self.assertEqual(dashboard._live_outcome_prices(None, ["0xabc"]), {})

    def test_no_condition_ids_skips_call(self):
        client = _FakeClient([])
        self.assertEqual(dashboard._live_outcome_prices(client, []), {})
        self.assertEqual(client.calls, [])  # 没有可查的盘 → 不发请求

    def test_exception_returns_empty_not_raise(self):
        client = _FakeClient([], raise_exc=True)
        self.assertEqual(dashboard._live_outcome_prices(client, ["0xabc"]), {})

    def test_dedupes_and_lowercases(self):
        client = _FakeClient([])
        dashboard._live_outcome_prices(client, ["0xAbC", "0xabc", "", None])
        # 去重 + 小写 → 只剩一个 cid
        self.assertEqual(client.calls[0][1].get("condition_ids"), ["0xabc"])

    def test_skips_markets_without_prices(self):
        client = _FakeClient([
            {"conditionId": "0xabc", "outcomes": ["A", "B"], "outcomePrices": []},
        ])
        self.assertEqual(dashboard._live_outcome_prices(client, ["0xabc"]), {})


if __name__ == "__main__":
    unittest.main()
