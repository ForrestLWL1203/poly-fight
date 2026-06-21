"""病A 价格门:跟单仅在目标卖价 ≥ HIGH_EXIT_PRICE(0.90)时镜像;<0.90 的提前卖持有到结算。"""
import unittest

from poly_fight.follow import HIGH_EXIT_PRICE, apply_follow_sell


def make_signal(entry=0.50, size=100.0, stake=10.0):
    return {
        "status": "open",
        "wallet": "0xabc",
        "condition_id": "0xcond",
        "outcome_index": 1,
        "legs": [{"our_entry_price": entry, "wallet_trade_size": size, "funded_stake": stake}],
        "wallet_sell_size": 0.0,
        "our_sold_fraction": 0.0,
        "our_partial_exit_pnl": 0.0,
        "behavior_events": [],
    }


def make_sell(size=100.0, price=0.50):
    return {
        "size": size,
        "amount": size,
        "price": price,
        "conditionId": "0xcond",
        "outcomeIndex": 1,
        "timestamp": 1000,
        "transactionHash": "0xtx",
    }


class TestFollowSellGate(unittest.TestCase):
    def test_low_price_sell_is_not_mirrored_and_holds(self):
        sig = make_signal(entry=0.50)
        state = apply_follow_sell(sig, make_sell(size=100.0, price=0.50), exit_price=0.50, now_ts=1000)
        self.assertEqual(state, "hold")
        # 我方仓位完全不变 → 留到结算
        self.assertEqual(sig["our_sold_fraction"], 0.0)
        self.assertEqual(sig.get("our_partial_exit_pnl", 0.0), 0.0)
        self.assertEqual(sig["status"], "open")
        # 但目标卖出仍记录供审计/行为分析
        self.assertGreater(sig["wallet_sell_size"], 0.0)
        self.assertTrue(sig["wallet_behavior"]["wallet_sold_before_resolution"])

    def test_flat_dump_full_position_low_price_still_holds(self):
        # 目标在 0.55(略高于 0.50 入场但 <0.90)清空整仓 → 我们不跟,持有到结算
        sig = make_signal(entry=0.50, size=100.0, stake=10.0)
        state = apply_follow_sell(sig, make_sell(size=100.0, price=0.55), exit_price=0.55, now_ts=1000)
        self.assertEqual(state, "hold")
        self.assertEqual(sig["our_sold_fraction"], 0.0)
        self.assertEqual(sig["status"], "open")

    def test_high_price_sell_is_mirrored_full_exit(self):
        sig = make_signal(entry=0.50, size=100.0, stake=10.0)
        state = apply_follow_sell(sig, make_sell(size=100.0, price=0.95), exit_price=0.95, now_ts=1000)
        self.assertEqual(state, "exited")
        self.assertAlmostEqual(sig["our_sold_fraction"], 1.0, places=6)
        # exit pnl = stake*(exit-entry)/entry = 10*(0.95-0.5)/0.5 = +9.0
        self.assertAlmostEqual(sig["our_partial_exit_pnl"], 9.0, places=4)
        self.assertEqual(sig["status"], "exited")

    def test_threshold_exactly_090_is_mirrored(self):
        sig = make_signal(entry=0.50)
        state = apply_follow_sell(sig, make_sell(size=100.0, price=HIGH_EXIT_PRICE), exit_price=HIGH_EXIT_PRICE, now_ts=1000)
        self.assertEqual(state, "exited")


if __name__ == "__main__":
    unittest.main()
