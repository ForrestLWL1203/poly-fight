"""运行期持仓对账(build_position_exit_reconcile_trades)单测:
目标钱包已清仓而我们仍持有 → 合成全量 SELL;仍持有/价格过低/空响应 → 不补卖。"""
import unittest

from poly_fight.cli import build_position_exit_reconcile_trades


WALLET = "0xa927cdfebebe4ba309386f12201f4fb0bccb8d9f"
CID = "0x1186cfa1e93a31370394dca66905b806f3966d98761d1f211f9d61a06ef7b0fb"


def _signal(*, outcome="KOLESIE", outcome_index=0, bought=100.0, sold=0.0, status="open"):
    return {
        "signal_id": f"{WALLET}:{CID}:{outcome_index}",
        "status": status,
        "wallet": WALLET,
        "condition_id": CID,
        "outcome": outcome,
        "outcome_index": outcome_index,
        "wallet_sell_size": sold,
        "legs": [{"wallet_trade_size": bought}],
    }


class FakeClient:
    def __init__(self, positions):
        self._positions = positions

    def positions(self, wallet, *, limit=500):
        return list(self._positions)


def _markets(price):
    # 二元市场,KOLESIE=idx0 现价 price
    return {CID: {"category": "esports", "outcome_prices": [price, round(1 - price, 8)]}}


class ExitReconcileTest(unittest.TestCase):
    def test_wallet_exited_synthesizes_full_sell(self):
        # 钱包持仓里没有该 (cid, outcome)(已清仓),但持仓非空(还有别的仓)
        client = FakeClient([{"conditionId": "0xother", "outcome": "TeamX", "size": 50.0}])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal(bought=100.0)], _markets(0.45), now_ts=1000,
        )
        self.assertEqual(stats["exited_detected"], 1)
        self.assertEqual(stats["synth_sells"], 1)
        sells = out[WALLET]
        self.assertEqual(len(sells), 1)
        self.assertEqual(sells[0]["side"], "SELL")
        self.assertEqual(sells[0]["outcomeIndex"], 0)
        self.assertAlmostEqual(sells[0]["size"], 100.0)   # 推满全量
        self.assertAlmostEqual(sells[0]["price"], 0.45)

    def test_wallet_still_holding_no_sell(self):
        client = FakeClient([{"conditionId": CID, "outcome": "KOLESIE", "size": 80.0}])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal()], _markets(0.45), now_ts=1000,
        )
        self.assertEqual(stats["still_holding"], 1)
        self.assertEqual(stats["synth_sells"], 0)
        self.assertNotIn(WALLET, out)

    def test_low_price_skipped(self):
        # 已清仓但现价 < 0.1 → 不补卖,留到结算
        client = FakeClient([{"conditionId": "0xother", "outcome": "TeamX", "size": 50.0}])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal()], _markets(0.05), now_ts=1000,
        )
        self.assertEqual(stats["exited_detected"], 1)
        self.assertEqual(stats["low_price_skipped"], 1)
        self.assertEqual(stats["synth_sells"], 0)
        self.assertNotIn(WALLET, out)

    def test_empty_positions_skipped_for_safety(self):
        # 持仓查询返回空 → 不当作全清仓(防 API 抖动误平)
        client = FakeClient([])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal()], _markets(0.45), now_ts=1000,
        )
        self.assertEqual(stats["empty_positions_skipped"], 1)
        self.assertEqual(stats["synth_sells"], 0)
        self.assertNotIn(WALLET, out)

    def test_already_partially_sold_pushes_to_full(self):
        # 已记录卖出 40/100,钱包现已清仓 → 合成剩余 60 的卖出
        client = FakeClient([{"conditionId": "0xother", "outcome": "TeamX", "size": 1.0}])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal(bought=100.0, sold=40.0)], _markets(0.45), now_ts=1000,
        )
        self.assertEqual(stats["synth_sells"], 1)
        self.assertAlmostEqual(out[WALLET][0]["size"], 60.0)

    def test_closed_signal_ignored(self):
        client = FakeClient([{"conditionId": "0xother", "outcome": "TeamX", "size": 1.0}])
        out, stats = build_position_exit_reconcile_trades(
            client, [_signal(status="settled")], _markets(0.45), now_ts=1000,
        )
        self.assertEqual(stats["synth_sells"], 0)
        self.assertEqual(stats["wallets_checked"], 0)


if __name__ == "__main__":
    unittest.main()
