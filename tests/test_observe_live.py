"""observe-live(3.1)发现端的关键接缝:只读 follow active 缓存 + 活跃度门 + 未结算过滤
+ 钱包级去重 early-exit。评分/发布复用 observe-v2 已在产线验证的模块级函数,不在此重测。"""
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from poly_fight.cli import _command_observe_live, build_parser, write_json
from poly_fight.storage import FollowStore


def _args(data_dir: Path, follow_dir: Path):
    return build_parser().parse_args([
        "--data-dir", str(data_dir),
        "observe-live", "--loop-minutes", "0", "--follow-dir", str(follow_dir),
    ])


def _seed_active_cache(follow_dir: Path, markets: dict, now_ts: int):
    store = FollowStore(follow_dir / "follow.db")
    store.save_market_cache(markets, cache_kind="active", updated_at=now_ts)


class _FakeClient:
    def __init__(self, market_positions_by_cid=None):
        self._mp = market_positions_by_cid or {}
        self.market_positions_calls = []
        self.list_events_calls = 0

    def market_positions(self, condition_id, *, limit=20, sort_by="TOTAL_PNL", sort_direction="DESC"):
        self.market_positions_calls.append(condition_id)
        self.last_sort_by = sort_by
        return self._mp.get(condition_id, [])

    def list_events_paginated(self, **_kwargs):
        self.list_events_calls += 1
        return []


class TestObserveLiveGates(unittest.TestCase):
    def _run(self, data_dir, markets, client):
        follow_dir = data_dir / "follow"
        now = datetime.now(timezone.utc)
        _seed_active_cache(follow_dir, markets, int(now.timestamp()))
        args = _args(data_dir, follow_dir)
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            _command_observe_live(args, client=client)
        # 取最后一行 JSON 事件
        lines = [ln for ln in buf.getvalue().splitlines() if ln.strip().startswith("{")]
        return json.loads(lines[-1]) if lines else {}

    def test_skips_low_volume_and_resolved_markets(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            start = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
            markets = {
                "m_low": {  # 未结算但 volume 不够 → 不扫
                    "condition_id": "m_low", "outcome_prices": [0.5, 0.5], "volume": 1000,
                    "game_family": "lol", "market_type": "main_match", "match_start_time": start,
                },
                "m_resolved": {  # volume 够但已结算([1,0] → 有 winner)→ 不扫
                    "condition_id": "m_resolved", "outcome_prices": [1.0, 0.0], "volume": 500000,
                    "game_family": "lol", "market_type": "main_match", "match_start_time": start,
                },
            }
            client = _FakeClient()
            event = self._run(data_dir, markets, client)
            self.assertEqual(event["live_markets"], 0)
            self.assertEqual(client.market_positions_calls, [])  # 没有可扫的盘 → 不发 market_positions

    def test_discovers_then_dedups_seen_wallet_early_exit(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            start = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
            markets = {
                "m_live": {
                    "condition_id": "m_live", "outcome_prices": [0.5, 0.5], "volume": 100000,
                    "game_family": "lol", "market_type": "main_match", "match_start_time": start,
                },
            }
            # 该参与者已在 collector_v2 profiles 里 → 去重后无新候选 → early-exit,不进评分
            (data_dir / "collector_v2").mkdir(parents=True, exist_ok=True)
            write_json(data_dir / "collector_v2" / "collector_v2_wallet_profiles.json",
                       [{"wallet": "0xseen", "grade": "A"}])
            client = _FakeClient({"m_live": [
                {"positions": [{"proxyWallet": "0xSEEN", "outcomeIndex": 0,
                                "avgPrice": 0.5, "totalBought": 1000, "totalPnl": 100}]},
                {"positions": []},
            ]})
            event = self._run(data_dir, markets, client)
            self.assertEqual(event["live_markets"], 1)
            self.assertEqual(client.market_positions_calls, ["m_live"])  # 发现层跑了
            # 防回归:必须用 data-api 合法枚举 TOTAL_PNL;旧值 "CASHPNL" 被拒 → 每场抛异常被吞 → 0 seeds。
            self.assertEqual(client.last_sort_by, "TOTAL_PNL")
            self.assertEqual(event["new_candidates"], 0)                  # 去重后无新候选
            self.assertEqual(client.list_events_calls, 0)                 # early-exit:没进评分(无分类集拉取)


if __name__ == "__main__":
    unittest.main()
