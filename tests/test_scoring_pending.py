"""评分完整性:未结算在册市场(pending)检测 + 缓存复用收紧。
修复"钱包在结算落库前一刻被打分、提前止损卖出的清仓亏损被长期漏计"的时间竞态。"""
import unittest

from poly_fight.core import SCORING_VERSION, summarize_trade_reconstructed_positions
from poly_fight.cli import should_use_cached_profile, PENDING_PROFILE_REUSE_TTL_SECONDS


def _market(prices):
    return {
        "category": "esports",
        "game_family": "cs2",
        "market_type": "main_match",
        "outcomes": ["TeamA", "TeamB"],
        "outcome_prices": prices,
    }


def _buy(cid, *, idx=0, size=100.0, price=0.6, ts=1000):
    return {"conditionId": cid, "outcomeIndex": idx, "side": "BUY",
            "size": size, "price": price, "timestamp": ts}


class PendingResolutionTest(unittest.TestCase):
    def test_unresolved_bought_market_counts_as_pending(self):
        markets = {"0xresolved": _market([1.0, 0.0]), "0xpending": _market([0.5, 0.5])}
        trades = [_buy("0xresolved"), _buy("0xpending")]
        summary = summarize_trade_reconstructed_positions(trades, markets, now_ts=2000)
        # 一个已结算(计分)、一个未结算(pending)
        self.assertEqual(summary["pending_resolution_market_count"], 1)

    def test_all_resolved_no_pending(self):
        markets = {"0xa": _market([1.0, 0.0]), "0xb": _market([0.0, 1.0])}
        trades = [_buy("0xa"), _buy("0xb", idx=1)]
        summary = summarize_trade_reconstructed_positions(trades, markets, now_ts=2000)
        self.assertEqual(summary["pending_resolution_market_count"], 0)


class CachedProfileTtlTest(unittest.TestCase):
    def _profile(self, *, pending, profiled_at):
        return {
            "scoring_version": SCORING_VERSION,
            "esports_condition_ids": ["0xabc"],
            "profiled_at": profiled_at,
            "pending_resolution_market_count": pending,
        }

    def test_no_pending_uses_full_ttl(self):
        ttl = 24 * 3600
        now = 100000
        prof = self._profile(pending=0, profiled_at=now - 5 * 3600)  # 5h 前
        self.assertTrue(should_use_cached_profile(prof, now_ts=now, ttl_seconds=ttl))

    def test_pending_shortens_ttl(self):
        ttl = 24 * 3600
        now = 100000
        # 5h 前 + 有 pending → 超过 1h 收紧窗口 → 不复用(强制重评)
        prof = self._profile(pending=1, profiled_at=now - 5 * 3600)
        self.assertFalse(should_use_cached_profile(prof, now_ts=now, ttl_seconds=ttl))

    def test_pending_within_short_window_still_reused(self):
        ttl = 24 * 3600
        now = 100000
        prof = self._profile(pending=2, profiled_at=now - 600)  # 10min 前,< 1h
        self.assertLess(600, PENDING_PROFILE_REUSE_TTL_SECONDS)
        self.assertTrue(should_use_cached_profile(prof, now_ts=now, ttl_seconds=ttl))


if __name__ == "__main__":
    unittest.main()
