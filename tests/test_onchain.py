"""Unit tests for the on-chain follow detection layer (poly_fight/onchain.py).

Pure-logic only (no network): asset-map building, TransferSingle decoding, and
crucially that on-chain-derived trade dicts are compatible with the existing
follow cursor + accessors (select_new_trades / trade_* helpers), so the detection
source can be swapped without touching process_follow_trades.
"""
import json
import ssl
import time
import unittest
from unittest import mock

from poly_fight import onchain as oc
from poly_fight.follow import (
    select_new_trades,
    trade_condition_id,
    trade_id,
    trade_outcome_index,
    trade_price,
    trade_side,
    trade_size,
    trade_timestamp,
)

TOKEN_YES = "83018638864815951618146503523437146898427799678988213654942363129633019093366"
TOKEN_NO = "109733537501696853656685707745847270956834896272832657829734576524670612586706"
COND = "0xc730c890a9ccddc685b8e60034b482b781b1d8ca119b331ad74c36f3b4352789"
WALLET = "0x47138dc1eef25f1ea91f3b2fda0e0f455c634d21"
OTHER = "0xa42f127d7e8df9f16881ffcc9ed0bc0326875f5a"


def make_order_filled(*, maker, taker=OTHER, maker_asset, taker_asset, maker_amt, taker_amt,
                      tx, block=100, ts=1781455861, log_index=0, address=None) -> dict:
    """Build a CTF Exchange OrderFilled log. Amounts are raw 6-decimal ints."""
    data = "0x" + "".join(format(int(x), "064x")
                          for x in (maker_asset, taker_asset, maker_amt, taker_amt, 0))
    return {
        "address": address or oc.EXCHANGE_ADDRESSES[1],
        "topics": [
            oc.ORDER_FILLED_TOPIC,
            "0x" + "00" * 32,                 # orderHash (indexed)
            oc.topic_for_address(maker),
            oc.topic_for_address(taker),
        ],
        "data": data,
        "transactionHash": tx,
        "logIndex": hex(log_index),
        "blockNumber": hex(block),
        "blockTimestamp": hex(ts),
    }


def buy_log(*, wallet=WALLET, token=TOKEN_YES, price, shares, tx, **kw) -> dict:
    """wallet buys `shares` of `token` at `price` (maker_asset=USDC=0)."""
    return make_order_filled(maker=wallet, maker_asset=0, taker_asset=int(token),
                             maker_amt=round(price * shares * 1e6), taker_amt=round(shares * 1e6),
                             tx=tx, **kw)


def sell_log(*, wallet=WALLET, token=TOKEN_YES, price, shares, tx, **kw) -> dict:
    """wallet sells `shares` of `token` at `price` (maker_asset=1 marker, USDC in takerAmt)."""
    return make_order_filled(maker=wallet, maker_asset=1, taker_asset=int(token),
                             maker_amt=round(shares * 1e6), taker_amt=round(price * shares * 1e6),
                             tx=tx, **kw)


class TestAssetMap(unittest.TestCase):
    def test_build_from_list_with_json_string(self):
        markets = [{"conditionId": COND, "clobTokenIds": f'["{TOKEN_YES}", "{TOKEN_NO}"]'}]
        amap = oc.build_asset_map(markets)
        self.assertEqual(amap[TOKEN_YES], {"conditionId": COND, "outcomeIndex": 0})
        self.assertEqual(amap[TOKEN_NO], {"conditionId": COND, "outcomeIndex": 1})

    def test_build_from_dict_and_list_tokenids(self):
        markets = {COND: {"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}}
        amap = oc.build_asset_map(markets)
        self.assertEqual(len(amap), 2)
        self.assertEqual(amap[TOKEN_NO]["outcomeIndex"], 1)

    def test_skips_markets_without_tokens_or_condition(self):
        amap = oc.build_asset_map([{"conditionId": COND}, {"clobTokenIds": [TOKEN_YES]}])
        self.assertEqual(amap, {})


class TestDecode(unittest.TestCase):
    def setUp(self):
        self.amap = oc.build_asset_map([{"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}])

    def test_buy_decodes_exact_price(self):
        log = buy_log(price=0.62, shares=5, tx="0xabc")
        fill = oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap)
        self.assertEqual(fill["wallet"], WALLET)
        self.assertEqual(fill["side"], "BUY")
        self.assertEqual(fill["conditionId"], COND)
        self.assertEqual(fill["outcomeIndex"], 0)
        self.assertEqual(fill["size"], 5.0)
        self.assertEqual(fill["price"], 0.62)
        self.assertEqual(fill["transactionHash"], "0xabc")

    def test_sell_decodes_exact_price(self):
        log = sell_log(token=TOKEN_NO, price=0.8, shares=12.5, tx="0xdef")
        fill = oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap)
        self.assertEqual(fill["wallet"], WALLET)
        self.assertEqual(fill["side"], "SELL")
        self.assertEqual(fill["outcomeIndex"], 1)
        self.assertEqual(fill["size"], 12.5)
        self.assertEqual(fill["price"], 0.8)

    def test_complementary_taker_leg_ignored(self):
        # The mint-complement leg has the wallet as TAKER (maker=other) -> ignored,
        # so the spurious 0.25 NO-leg never becomes a follow.
        log = make_order_filled(maker=OTHER, taker=WALLET, maker_asset=0, taker_asset=int(TOKEN_NO),
                                maker_amt=round(0.25 * 5 * 1e6), taker_amt=round(5 * 1e6), tx="0xc")
        self.assertIsNone(oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap))

    def test_off_scope_token_returns_none(self):
        log = buy_log(token="999999", price=0.5, shares=1, tx="0x1")
        self.assertIsNone(oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap))

    def test_insane_price_guarded(self):
        # price > 1 (unseen encoding) -> rejected, never a corrupt fill.
        log = make_order_filled(maker=WALLET, maker_asset=0, taker_asset=int(TOKEN_YES),
                                maker_amt=round(5 * 1e6), taker_amt=round(3 * 1e6), tx="0xbad")
        self.assertIsNone(oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap))

    def test_malformed_log_returns_none(self):
        self.assertIsNone(oc.decode_order_filled({"topics": [], "data": "0x"},
                                                 wallets={WALLET}, asset_map=self.amap))


class TestTradeCompatibility(unittest.TestCase):
    """On-chain trade dicts must work with the existing follow accessors/cursor."""

    def setUp(self):
        self.amap = oc.build_asset_map([{"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}])

    def _trade(self, *, tx, ts, side="BUY", token=TOKEN_YES, price=0.62):
        builder = buy_log if side == "BUY" else sell_log
        log = builder(token=token, price=price, shares=5, tx=tx, ts=ts)
        fill = oc.decode_order_filled(log, wallets={WALLET}, asset_map=self.amap)
        return oc.fill_to_trade(fill)   # uses the exact on-chain price in the fill

    def test_accessors_read_onchain_trade(self):
        t = self._trade(tx="0xaaa", ts=1781455900, side="BUY", price=0.62)
        self.assertEqual(trade_condition_id(t), COND)
        self.assertEqual(trade_outcome_index(t), 0)
        self.assertEqual(trade_side(t), "BUY")
        self.assertEqual(trade_timestamp(t), 1781455900)
        self.assertEqual(trade_id(t), "0xaaa")
        self.assertEqual(trade_price(t), 0.62)
        self.assertEqual(trade_size(t), 5.0)

    def test_select_new_trades_cursor_advances(self):
        older = self._trade(tx="0xaaa", ts=1781455900)
        newer = self._trade(tx="0xbbb", ts=1781455950)
        # cold start (no cursor) -> nothing emitted, cursor set to latest
        new, cursor, cold = select_new_trades([older, newer], None)
        self.assertTrue(cold)
        self.assertEqual(new, [])
        self.assertEqual(cursor["timestamp"], 1781455950)
        self.assertEqual(cursor["id"], "0xbbb")
        # a third fill after that cursor is detected as new
        newest = self._trade(tx="0xccc", ts=1781456000)
        new2, cursor2, cold2 = select_new_trades([older, newer, newest], cursor)
        self.assertFalse(cold2)
        self.assertEqual([trade_id(x) for x in new2], ["0xccc"])

    def test_cursor_compatible_with_dataapi_scheme(self):
        # A data-api-style cursor (timestamp + bare tx hash) must order correctly
        # against an on-chain trade, so a WS<->data-api fallback keeps cursors valid.
        dataapi_cursor = {"timestamp": 1781455900, "id": "0xaaa"}
        onchain_newer = self._trade(tx="0xzzz", ts=1781455999)
        new, _, cold = select_new_trades([onchain_newer], dataapi_cursor)
        self.assertFalse(cold)
        self.assertEqual([trade_id(x) for x in new], ["0xzzz"])


class TestWSClientNoFdLeak(unittest.TestCase):
    """回归:WS 连接/握手失败必须关掉已开 socket,否则反复重连耗尽 fd → EMFILE →
    SQLite "unable to open database file" → runner 自停(实测多次拖垮生产 runner)。"""

    class _FakeSock:
        def __init__(self, handshake=b"HTTP/1.1 500 Bad\r\n\r\n"):
            self.closed = False
            self._hs = handshake
        def sendall(self, data): pass
        def settimeout(self, t): pass
        def recv(self, n):
            out, self._hs = self._hs[:n], self._hs[n:]
            return out
        def close(self): self.closed = True

    def test_closes_socket_on_handshake_failure(self):
        from poly_fight import ws_client
        fake = self._FakeSock(b"HTTP/1.1 500 Internal\r\n\r\n")  # 非 101 → 握手失败
        with mock.patch.object(ws_client.WSClient, "_proxy_for", return_value=None), \
             mock.patch.object(ws_client.socket, "create_connection", return_value=fake):
            client = ws_client.WSClient("ws://example.test/ws")  # ws:// 免 SSL
            with self.assertRaises(ws_client.WSError):
                client.connect()
        self.assertTrue(fake.closed, "握手失败后 socket 未关闭 → fd 泄漏")
        self.assertIsNone(client.sock)

    def test_closes_raw_socket_on_ssl_failure(self):
        from poly_fight import ws_client
        fake = self._FakeSock()
        def boom(*a, **k): raise ssl.SSLError("handshake boom")
        with mock.patch.object(ws_client.WSClient, "_proxy_for", return_value=None), \
             mock.patch.object(ws_client.socket, "create_connection", return_value=fake), \
             mock.patch.object(ws_client.ssl, "create_default_context") as ctx:
            ctx.return_value.wrap_socket.side_effect = boom
            client = ws_client.WSClient("wss://example.test/ws")  # wss:// 触发 SSL 包装
            with self.assertRaises(ssl.SSLError):
                client.connect()
        self.assertTrue(fake.closed, "SSL 包装失败后底层 socket 未关闭 → fd 泄漏")


class TestPolling(unittest.TestCase):
    """getLogs cursor-polling collector: exact price, self-heal, dedup."""

    def _collector(self, **kw):
        events = []
        col = oc.OnchainFollowCollector(
            https_url="http://x", wallets={WALLET},
            asset_map=oc.build_asset_map([{"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}]),
            on_event=lambda k, d: events.append((k, d)),
            **kw,
        )
        return col, events

    def test_poll_buffers_exact_priced_fill(self):
        col, _ = self._collector()
        col._cursor = 990
        log = buy_log(price=0.62, shares=5, tx="0xabc", block=995)
        with mock.patch.object(oc, "block_number", return_value=1000), \
             mock.patch.object(oc, "rpc_call", return_value=[log]):
            col._poll_once()
        self.assertTrue(col.healthy)
        drained = col.drain()
        self.assertIn(WALLET, drained)
        self.assertEqual(drained[WALLET][0]["price"], 0.62)
        self.assertEqual(col._cursor, 1000)

    def test_cursor_self_heals_on_failure(self):
        col, _ = self._collector(unhealthy_after_failures=2)
        col._cursor = 990
        with mock.patch.object(oc, "block_number", return_value=1000), \
             mock.patch.object(oc, "rpc_call", side_effect=RuntimeError("rpc down")):
            col._poll_once()
            col._poll_once()
        self.assertFalse(col.healthy)
        self.assertEqual(col._cursor, 990)  # never advanced -> gap re-covered next round
        # recovery: the fill from the outage window is caught on the next success
        log = buy_log(price=0.7, shares=3, tx="0xrec", block=996)
        with mock.patch.object(oc, "block_number", return_value=1001), \
             mock.patch.object(oc, "rpc_call", return_value=[log]):
            col._poll_once()
        self.assertTrue(col.healthy)
        self.assertIn(WALLET, col.drain())

    def test_dedup_across_overlap(self):
        col, _ = self._collector()
        col._cursor = 990
        log = buy_log(price=0.5, shares=2, tx="0xa", block=995, log_index=3)
        with mock.patch.object(oc, "block_number", return_value=1000), \
             mock.patch.object(oc, "rpc_call", return_value=[log]):
            col._poll_once()
        with mock.patch.object(oc, "block_number", return_value=1003), \
             mock.patch.object(oc, "rpc_call", return_value=[log]):  # overlap re-returns it
            col._poll_once()
        self.assertEqual(col.fill_count, 1)

    def test_no_wallets_is_healthy_idle(self):
        col = oc.OnchainFollowCollector(https_url="http://x", wallets=set())
        col._poll_once(current_hint=1000)   # no wallets -> healthy-idle, no RPC needed
        self.assertTrue(col.healthy)

    class _FakeWS:
        # 连上后第一条消息即 OP_CLOSE → _run_ws_session 在连接块(冷启动/回补)跑完后干净退出。
        OP_TEXT = 0x1
        OP_CLOSE = 0x8
        def __init__(self, url): pass
        def connect(self): pass
        def send_text(self, s): pass
        def set_timeout(self, t): pass
        def recv_message(self): return (0x8, b"")
        def close(self): pass

    def _run_one_session(self, col):
        rpc_methods = []
        with mock.patch.object(oc, "WSClient", self._FakeWS), \
             mock.patch.object(oc, "block_number", return_value=2000), \
             mock.patch.object(oc, "rpc_call", side_effect=lambda *a, **k: rpc_methods.append(a[1]) or []):
            col._run_ws_session({WALLET})
        return rpc_methods

    def test_cold_start_uses_dataapi_not_getlogs(self):
        # 进程冷启动(cursor<=0)默认不烧 getLogs:只把游标定到当前块头(1 次
        # eth_blockNumber),置 cold_catchup_pending,交给 runner 走 data-api 补单。
        col, events = self._collector(wss_url="wss://x", catchup_via_dataapi=True)
        col._cursor = 0  # cold
        rpc_methods = self._run_one_session(col)
        self.assertTrue(col.cold_catchup_pending)
        self.assertEqual(col._cursor, 2000)                 # 游标定到块头
        self.assertNotIn("eth_getLogs", rpc_methods)        # 冷启动不发 getLogs
        self.assertIn(("dataapi_catchup", {"cursor": 2000, "cold_start": True}), events)
        col.clear_cold_catchup()
        self.assertFalse(col.cold_catchup_pending)

    def test_reconnect_also_uses_dataapi_not_getlogs(self):
        # 重连(cursor>0,健康期 stale/recycle/异常/钱包集变更)默认也不烧 getLogs:同冷启动一样
        # 定游标到块头 + 置 cold_catchup_pending,缺口交 data-api。这是省 Alchemy 额度的关键。
        col, events = self._collector(wss_url="wss://x", catchup_via_dataapi=True)
        col._cursor = 1500  # warm reconnect
        rpc_methods = self._run_one_session(col)
        self.assertTrue(col.cold_catchup_pending)
        self.assertEqual(col._cursor, 2000)
        self.assertNotIn("eth_getLogs", rpc_methods)        # 重连也不发 getLogs
        self.assertIn(("dataapi_catchup", {"cursor": 2000, "cold_start": False}), events)

    def test_legacy_getlogs_when_disabled(self):
        # 关掉新行为(catchup_via_dataapi=False)→ 回到旧版冷启动/重连 getLogs 回补。
        col, _ = self._collector(wss_url="wss://x", catchup_via_dataapi=False)
        col._cursor = 0
        rpc_methods = self._run_one_session(col)
        self.assertFalse(col.cold_catchup_pending)
        self.assertIn("eth_getLogs", rpc_methods)           # 旧行为:仍 getLogs

    def test_backfill_buffers_fill_and_advances_cursor(self):
        col, events = self._collector()
        col._cursor = 990
        log = buy_log(price=0.62, shares=5, tx="0xbf", block=995)
        with mock.patch.object(oc, "block_number", return_value=1000), \
             mock.patch.object(oc, "rpc_call", return_value=[log]):
            col._backfill({WALLET})
        self.assertIn(WALLET, col.drain())
        self.assertEqual(col._cursor, 1000)
        self.assertTrue(any(k == "backfill" for k, _ in events))

    def test_backfill_rewind_scans_further_back(self):
        col, _ = self._collector(reconnect_rewind_blocks=150, poll_overlap_blocks=5)
        col._cursor = 1000
        seen = {}
        with mock.patch.object(oc, "block_number", return_value=1010), \
             mock.patch.object(col, "_scan", side_effect=lambda fb, tb, w: seen.update(fb=fb, tb=tb)):
            col._backfill({WALLET}, rewind=150)
        self.assertEqual(seen["fb"], 1000 + 1 - 5 - 150)  # cursor+1-overlap-rewind = 846
        self.assertEqual(seen["tb"], 1010)

    def test_update_wallets_sets_dirty_only_on_change(self):
        col, _ = self._collector()
        col._dirty.clear()
        col.update_wallets(set(col._wallets))           # unchanged -> not dirty
        self.assertFalse(col._dirty.is_set())
        col.update_wallets(set(col._wallets) | {"0xother"})  # changed -> dirty (resubscribe)
        self.assertTrue(col._dirty.is_set())


if __name__ == "__main__":
    unittest.main()

