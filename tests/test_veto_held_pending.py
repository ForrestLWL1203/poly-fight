"""held-pending-veto:CS2 map-winner 在【开赛前】检测到的买单先 held 暂存、不开仓,跨 tick
复跟,直到开赛(now>=start)才落到 veto 门判 fade/跟;开赛后仍无 veto → 按原逻辑(no_veto)跟。
卖出清暂存。盘前 fade 不会被裸跟——这正是本次改动要堵的口。"""
import unittest

import poly_fight.follow as follow
from poly_fight.follow import process_follow_trades

START = 100_000  # 开赛时刻


def _market():
    return {
        "condition_id": "m1", "outcomes": ["FURIA", "Falcons"], "outcome_prices": [0.5, 0.5],
        "match_start_time": "2026-06-21T15:00:00Z",
        "title": "FURIA vs Falcons - Map 1 Winner",
        "question": "Counter-Strike: FURIA vs Falcons - Map 1 Winner",
        "market_type": "map_winner", "game_family": "cs2",
    }


def _buy(tid, now, cash=100, price=0.5):
    return {"id": tid, "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0,
            "side": "BUY", "price": price, "size": cash / price, "timestamp": now}


def _run(open_signals, trades, held, now, *, market=None):
    # market_start_ts 读 match_start_time;这里用一个固定 start 的市场,但把 now 相对 START 移动。
    mk = market if market is not None else _market()
    # 让 market_start_ts 返回 START:用 start_ts 注入更稳——直接给一个 epoch 字段。
    mk = {**mk, "match_start_time": _iso(START)}
    return process_follow_trades(
        open_signals, wallet="0xA", trades=trades, markets_by_condition={"m1": mk},
        now_ts=now, stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=1.0,
        bankroll_usdc=1000, min_wallet_trade_cash_usdc=1, held_pending_veto=held,
        require_pre_match=False,   # runner 默认;盘前/盘后由 held 暂存而非此门裁
    )


def _iso(epoch):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


class TestHeldPendingVeto(unittest.TestCase):
    KEY = "0xa|m1|0"

    def setUp(self):
        self._orig_gate = follow.veto_gate
        # veto 佐证默认已停用(无可靠数据源);本套测试验证【启用时】的 held 机制,故显式打开开关。
        self._orig_enabled = follow._cs2_veto_corroboration_enabled
        follow._cs2_veto_corroboration_enabled = lambda s: True

    def tearDown(self):
        follow.veto_gate = self._orig_gate
        follow._cs2_veto_corroboration_enabled = self._orig_enabled

    def test_disabled_by_default_premarket_buy_follows_normally(self):
        # 默认关:cs2 map_winner 盘前买单不再被 held,按 veto 上线前行为正常跟。
        follow._cs2_veto_corroboration_enabled = self._orig_enabled  # 还原成真·默认(关)
        held = {}
        signals, stats = _run([], [_buy("b1", START - 600)], held, START - 600)
        self.assertEqual(held, {})
        self.assertEqual(stats["veto_held_count"], 0)
        self.assertEqual(len(signals), 1)              # 正常开仓,无 veto 干预

    def test_premarket_buy_is_held_not_followed(self):
        held = {}
        signals, stats = _run([], [_buy("b1", START - 600)], held, START - 600)
        self.assertEqual(signals, [])                  # 盘前不开仓
        self.assertIn(self.KEY, held)
        self.assertEqual(stats["veto_held_count"], 1)

    def test_held_reinjected_and_still_held_before_start(self):
        held = {}
        _run([], [_buy("b1", START - 600)], held, START - 600)
        # 第二个 tick:钱包没有新成交,但 held 仍应被复跟、且仍盘前 → 继续 held。
        signals, stats = _run([], [], held, START - 300)
        self.assertEqual(signals, [])
        self.assertIn(self.KEY, held)
        self.assertEqual(stats["veto_held_count"], 1)  # 复跟一次

    def test_release_after_start_follows_when_no_veto(self):
        follow.veto_gate = lambda *a, **k: {"applies": True, "decision": "no_veto"}
        held = {}
        _run([], [_buy("b1", START - 600)], held, START - 600)   # 盘前 held
        self.assertIn(self.KEY, held)
        signals, _ = _run([], [], held, START + 5)               # 开赛后:无 veto → 原逻辑跟
        self.assertEqual(len(signals), 1)
        self.assertNotIn(self.KEY, held)                         # 已释放

    def test_release_after_start_skips_on_fade(self):
        follow.veto_gate = lambda *a, **k: {
            "applies": True, "decision": follow.VETO_DECISION_SKIP, "score": -1.0}
        held = {}
        _run([], [_buy("b1", START - 600)], held, START - 600)   # 盘前 held
        signals, stats = _run([], [], held, START + 5)           # 开赛后:fade → 不跟
        self.assertEqual(signals, [])
        self.assertNotIn(self.KEY, held)                         # 释放但被 fade 拦
        self.assertEqual(stats["veto_fade_skip_count"], 1)

    def test_sell_before_start_clears_held(self):
        held = {}
        _run([], [_buy("b1", START - 600)], held, START - 600)
        self.assertIn(self.KEY, held)
        sell = {"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0,
                "side": "SELL", "price": 0.5, "size": 50, "timestamp": START - 300}
        _run([], [sell], held, START - 300)
        self.assertNotIn(self.KEY, held)

    def test_release_survives_require_pre_match_gate(self):
        # 即便开了 --require-pre-match,held 释放也不该被"检测过晚"门当 in-play 丢弃(它本是盘前检测)。
        follow.veto_gate = lambda *a, **k: {"applies": True, "decision": "no_veto"}
        held = {}
        process_follow_trades(
            [], wallet="0xA", trades=[_buy("b1", START - 600)],
            markets_by_condition={"m1": {**_market(), "match_start_time": _iso(START)}},
            now_ts=START - 600, stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10,
            max_slippage=1.0, bankroll_usdc=1000, min_wallet_trade_cash_usdc=1,
            held_pending_veto=held, require_pre_match=True, post_start_grace_seconds=900,
        )
        self.assertIn(self.KEY, held)
        signals, _ = process_follow_trades(
            [], wallet="0xA", trades=[], markets_by_condition={"m1": {**_market(), "match_start_time": _iso(START)}},
            now_ts=START + 5000, stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10,
            max_slippage=1.0, bankroll_usdc=1000, min_wallet_trade_cash_usdc=1,
            held_pending_veto=held, require_pre_match=True, post_start_grace_seconds=900,
        )
        self.assertEqual(len(signals), 1)   # 释放且跟上,未被 in-play 门误杀
        self.assertNotIn(self.KEY, held)

    def test_buy_after_start_is_not_held(self):
        # 开赛后才检测到的买单不进 held,直接走 veto 门(此处注入 no_veto → 跟)。
        follow.veto_gate = lambda *a, **k: {"applies": True, "decision": "no_veto"}
        held = {}
        signals, stats = _run([], [_buy("b1", START + 10)], held, START + 20)
        self.assertEqual(stats["veto_held_count"], 0)
        self.assertEqual(held, {})
        self.assertEqual(len(signals), 1)


if __name__ == "__main__":
    unittest.main()
