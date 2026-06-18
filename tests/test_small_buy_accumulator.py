"""小单累加器(pending_small_buys):未达最小下单额的零散 BUY 缓存累加,凑够即跟,
卖出/未触发清缓存。买入侧与卖出侧 MIN_FOLLOW_SELL_USDC 对称——都"攒够最低额才动手"。"""
import unittest
from datetime import datetime, timezone

from poly_fight.follow import process_follow_trades


def _market(now):
    return {
        "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
        "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        "title": "Match", "market_type": "main_match",
    }


def _buy(tid, cash, now, price=0.5):
    return {"id": tid, "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0,
            "side": "BUY", "price": price, "size": cash / price, "timestamp": now}


def _run(open_signals, trades, pending, now):
    return process_follow_trades(
        open_signals, wallet="0xA", trades=trades, markets_by_condition={"m1": _market(now)},
        now_ts=now, stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=0.05,
        bankroll_usdc=1000, min_wallet_trade_cash_usdc=50, pending_small_buys=pending,
    )


class TestSmallBuyAccumulator(unittest.TestCase):
    KEY = "0xa|m1|0"

    def test_sub_threshold_buy_is_cached_not_followed(self):
        now = 1000
        pending = {}
        signals, stats = _run([], [_buy("b1", 20, now)], pending, now)
        self.assertEqual(signals, [])                       # 没开仓
        self.assertIn(self.KEY, pending)
        self.assertAlmostEqual(pending[self.KEY]["cash"], 20)
        self.assertEqual(stats["small_buy_cached_count"], 1)
        self.assertEqual(stats["small_buy_triggered_count"], 0)

    def test_accumulates_across_ticks_then_triggers(self):
        now = 1000
        pending = {}
        signals, _ = _run([], [_buy("b1", 20, now)], pending, now)
        signals, _ = _run(signals, [_buy("b2", 20, now + 60)], pending, now + 60)
        self.assertEqual(signals, [])
        self.assertAlmostEqual(pending[self.KEY]["cash"], 40)   # 40 < 50,仍攒着
        signals, stats = _run(signals, [_buy("b3", 20, now + 120)], pending, now + 120)
        self.assertEqual(len(signals), 1)                       # 60 >= 50 → 跟
        self.assertNotIn(self.KEY, pending)                     # 触发后清零
        self.assertEqual(stats["small_buy_triggered_count"], 1)
        # 合成腿用累计股数(20+20+20 = 60 现金 / 0.5 = 120 股)
        self.assertAlmostEqual(signals[0]["legs"][0]["wallet_trade_cash"], 60)

    def test_large_single_buy_bypasses_accumulator(self):
        now = 1000
        pending = {}
        signals, stats = _run([], [_buy("b1", 60, now)], pending, now)
        self.assertEqual(len(signals), 1)
        self.assertEqual(pending, {})
        self.assertEqual(stats["small_buy_cached_count"], 0)
        self.assertEqual(stats["small_buy_triggered_count"], 0)

    def test_sell_clears_pending(self):
        now = 1000
        pending = {}
        _run([], [_buy("b1", 20, now)], pending, now)
        self.assertIn(self.KEY, pending)
        sell = {"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0,
                "side": "SELL", "price": 0.5, "size": 10, "timestamp": now + 60}
        _run([], [sell], pending, now + 60)
        self.assertNotIn(self.KEY, pending)                     # 没凑够就卖 → 清缓存


if __name__ == "__main__":
    unittest.main()
