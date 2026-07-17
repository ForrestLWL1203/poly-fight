"""主盘/子盘预算解耦 + 每场最大跟单笔数(2026-06-21 加)。"""
import unittest

from poly_fight.follow_strategy import (
    default_follow_strategy,
    evaluate_follow_candidate,
    normalize_follow_strategy,
    validate_follow_strategy,
)


def _strategy(**sizing_over):
    s = default_follow_strategy(balance_usdc=10000)
    s["sizing"].update(sizing_over)
    return s


COMMON = dict(
    target_wallet_order_cash_usdc=100.0,
    available_balance_usdc=10000.0,
    entry_price=0.5,
    bankroll_usdc=10000.0,
    theta=0.9,
    bucket_edge_lb=0.2,
)


class TestBudgetSplit(unittest.TestCase):
    def test_default_has_new_fields(self):
        sz = default_follow_strategy()["sizing"]
        self.assertIn("per_match_percent_sub", sz)
        self.assertIn("max_follow_orders_per_match", sz)

    def test_old_config_sub_defaults_to_main(self):
        # 旧策略只有 per_match_percent、无子盘字段 → 子盘回退主盘值(行为不变)
        old = {"schema_version": 2, "sizing": {"per_signal_percent": 1, "per_match_percent": 2}}
        sz = normalize_follow_strategy(old)["sizing"]
        self.assertEqual(sz["per_match_percent_sub"], 2.0)
        self.assertEqual(sz["max_follow_orders_per_match"], 0)

    def test_main_uses_main_budget(self):
        s = _strategy(per_match_percent=1.0, per_match_percent_sub=0.5)
        r = evaluate_follow_candidate(strategy=s, market_type="main_match",
                                      wallet_condition_funded_stake_usdc=0.0, **COMMON)
        self.assertEqual(r["target_stake"], 100)  # 10000 × 1%

    def test_submarket_uses_sub_budget(self):
        s = _strategy(per_match_percent=1.0, per_match_percent_sub=0.5)
        for mt in ("map_winner", "game_winner"):
            r = evaluate_follow_candidate(strategy=s, market_type=mt,
                                          wallet_condition_funded_stake_usdc=0.0, **COMMON)
            self.assertEqual(r["target_stake"], 50, mt)  # 被子盘 0.5% = $50 夹住

    def test_sub_can_be_below_per_signal(self):
        # 子盘预算允许 < 单笔基数(把子盘单局压到低于一整注正是用途)
        s = _strategy(per_signal_percent=1.0, per_match_percent=1.0, per_match_percent_sub=0.3)
        ok, errs = validate_follow_strategy(s)
        self.assertTrue(ok, errs)
        r = evaluate_follow_candidate(strategy=s, market_type="game_winner",
                                      wallet_condition_funded_stake_usdc=0.0, **COMMON)
        self.assertEqual(r["target_stake"], 30)


class TestCashBasedSizing(unittest.TestCase):
    """单笔注码 = per_signal_percent% × 可动用现金(available),不含未结算持仓权益。"""

    def test_sizes_off_available_cash_not_equity(self):
        # 现金 3794(< 旧 bankroll/权益 6000)→ 1% 按现金算 = $37,不是 $60。
        s = _strategy(per_signal_percent=1.0, per_match_percent=100.0)
        r = evaluate_follow_candidate(
            strategy=s, market_type="main_match",
            target_wallet_order_cash_usdc=100.0, entry_price=0.5,
            available_balance_usdc=3794.0, bankroll_usdc=6000.0,  # 权益更高但应被忽略
            theta=0.9, bucket_edge_lb=0.2,
        )
        self.assertTrue(r["would_follow"])
        self.assertEqual(r["target_stake"], 37)  # floor(3794 × 1%)

    def test_no_cash_blocks(self):
        s = _strategy(per_signal_percent=1.0, per_match_percent=100.0)
        r = evaluate_follow_candidate(
            strategy=s, market_type="main_match",
            target_wallet_order_cash_usdc=100.0, entry_price=0.5,
            available_balance_usdc=0.0, bankroll_usdc=6000.0,
            theta=0.9, bucket_edge_lb=0.2,
        )
        self.assertFalse(r["would_follow"])
        self.assertEqual(r["block_reason"], "no_bankroll")


class TestPerMatchTotalBudget(unittest.TestCase):
    """每场预算 = 整场所有钱包合计(不是每钱包)。预算 = 余额 × per_match_percent%。"""

    def test_total_across_wallets_caps_at_budget(self):
        # 预算 $500(10000×5%);整场已被别的钱包投满 $500 → 即使本钱包该场自己投=0,也被挡。
        s = _strategy(per_signal_percent=1.0, per_match_percent=5.0, min_stake_usdc=30)
        r = evaluate_follow_candidate(strategy=s, market_type="main_match",
                                      condition_funded_stake_usdc=500.0,
                                      wallet_condition_funded_stake_usdc=0.0, **COMMON)
        self.assertFalse(r["would_follow"])
        self.assertEqual(r["block_reason"], "match_budget_reached")

    def test_total_below_budget_caps_stake_to_remaining(self):
        # 整场已投 $480 / 预算 $500 → 剩 $20,新单被夹到整场剩余 $20(< 单笔 1%=$100)。
        s = _strategy(per_signal_percent=1.0, per_match_percent=5.0, min_stake_usdc=10)
        r = evaluate_follow_candidate(strategy=s, market_type="main_match",
                                      condition_funded_stake_usdc=480.0,
                                      wallet_condition_funded_stake_usdc=0.0, **COMMON)
        self.assertTrue(r["would_follow"])
        self.assertEqual(r["target_stake"], 20)

    def test_per_wallet_funded_no_longer_caps(self):
        # 回归:每钱包口径已弃用。本钱包该场自己已投很多($480),但整场总额还没满($100/$500)→ 不挡。
        s = _strategy(per_signal_percent=1.0, per_match_percent=5.0, min_stake_usdc=30)
        r = evaluate_follow_candidate(strategy=s, market_type="main_match",
                                      condition_funded_stake_usdc=100.0,
                                      wallet_condition_funded_stake_usdc=480.0, **COMMON)
        self.assertTrue(r["would_follow"])  # 整场未满 → 放行(证明不再看每钱包)
        self.assertEqual(r["target_stake"], 100)


class TestMaxOrdersPerMatch(unittest.TestCase):
    def test_blocks_at_cap(self):
        s = _strategy(max_follow_orders_per_match=2)
        r = evaluate_follow_candidate(strategy=s, market_type="game_winner",
                                      wallet_condition_funded_order_count=2, **COMMON)
        self.assertFalse(r["would_follow"])
        self.assertEqual(r["block_reason"], "wallet_condition_order_cap_reached")

    def test_allows_below_cap(self):
        s = _strategy(max_follow_orders_per_match=2)
        r = evaluate_follow_candidate(strategy=s, market_type="game_winner",
                                      wallet_condition_funded_order_count=1, **COMMON)
        self.assertTrue(r["would_follow"])

    def test_zero_means_unlimited(self):
        s = _strategy(max_follow_orders_per_match=0)
        r = evaluate_follow_candidate(strategy=s, market_type="game_winner",
                                      wallet_condition_funded_order_count=99, **COMMON)
        self.assertTrue(r["would_follow"])

    def test_cap_applies_to_main_too(self):
        s = _strategy(max_follow_orders_per_match=1)
        r = evaluate_follow_candidate(strategy=s, market_type="main_match",
                                      wallet_condition_funded_order_count=1, **COMMON)
        self.assertFalse(r["would_follow"])
        self.assertEqual(r["block_reason"], "wallet_condition_order_cap_reached")


if __name__ == "__main__":
    unittest.main()
