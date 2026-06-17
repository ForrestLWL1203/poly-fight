"""评分完整性:未结算在册市场(pending)检测 + 缓存复用收紧。
修复"钱包在结算落库前一刻被打分、提前止损卖出的清仓亏损被长期漏计"的时间竞态。"""
import unittest

from poly_fight.core import SCORING_VERSION, summarize_trade_reconstructed_positions
from poly_fight.cli import (
    should_use_cached_profile,
    PENDING_PROFILE_REUSE_TTL_SECONDS,
    backfill_market_resolutions,
)


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


class FakeResolutionClient:
    """markets_by_condition_ids 返回 gamma 风格市场(conditionId + outcomePrices)。"""
    def __init__(self, resolved):
        self._resolved = resolved  # {cid: outcomePrices or None}

    def markets_by_condition_ids(self, cids, *, limit=None):
        out = []
        for cid in cids:
            prices = self._resolved.get(cid)
            if prices is None:
                continue  # gamma 侧也还没结算
            out.append({"conditionId": cid, "outcomePrices": prices, "closed": True})
        return out


class BackfillResolutionTest(unittest.TestCase):
    NOW = 2_000_000  # now_ts
    PAST = "2026-06-16T21:18:00Z"     # 已过结束时间(用相对久远的过去更稳:见下)
    FUTURE_TS = 9_999_999_999

    def _rec(self, cid, *, end_ts, prices=None):
        # end_date 用 ISO;这里用 epoch 转 ISO 避免时区歧义
        import datetime
        end_iso = datetime.datetime.fromtimestamp(end_ts, tz=datetime.timezone.utc).isoformat()
        rec = {"condition_id": cid, "end_date": end_iso, "outcomes": ["A", "B"]}
        if prices is not None:
            rec["outcome_prices"] = prices
        return rec

    def test_fills_closed_unresolved_market(self):
        cset = [self._rec("0xa", end_ts=self.NOW - 3600)]  # 1h 前结束,无结果
        client = FakeResolutionClient({"0xa": ["0", "1"]})  # gamma 已结算
        stats = backfill_market_resolutions(client, cset, now_ts=self.NOW)
        self.assertEqual(stats, {"checked": 1, "filled": 1})
        self.assertEqual(cset[0]["outcome_prices"], [0.0, 1.0])

    def test_skips_already_resolved(self):
        cset = [self._rec("0xa", end_ts=self.NOW - 3600, prices=[1.0, 0.0])]
        client = FakeResolutionClient({"0xa": ["0", "1"]})
        stats = backfill_market_resolutions(client, cset, now_ts=self.NOW)
        self.assertEqual(stats["checked"], 0)
        self.assertEqual(cset[0]["outcome_prices"], [1.0, 0.0])  # 未被改

    def test_skips_not_yet_ended(self):
        cset = [self._rec("0xa", end_ts=self.NOW + 3600)]  # 还没结束
        client = FakeResolutionClient({"0xa": ["0", "1"]})
        stats = backfill_market_resolutions(client, cset, now_ts=self.NOW)
        self.assertEqual(stats, {"checked": 0, "filled": 0})

    def test_gamma_still_unresolved_not_filled(self):
        cset = [self._rec("0xa", end_ts=self.NOW - 3600)]
        client = FakeResolutionClient({"0xa": None})  # gamma 也没结果
        stats = backfill_market_resolutions(client, cset, now_ts=self.NOW)
        self.assertEqual(stats, {"checked": 1, "filled": 0})
        self.assertNotIn("outcome_prices", cset[0])


if __name__ == "__main__":
    unittest.main()
