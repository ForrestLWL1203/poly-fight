"""Unit tests for the on-chain follow detection layer (poly_fight/onchain.py).

Pure-logic only (no network): asset-map building, TransferSingle decoding, and
crucially that on-chain-derived trade dicts are compatible with the existing
follow cursor + accessors (select_new_trades / trade_* helpers), so the detection
source can be swapped without touching process_follow_trades.
"""
import json
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


def make_log(token_id: str, *, frm: str, to: str, value_shares: float,
             tx: str, block: int = 100, ts: int = 1781455861, log_index: int = 0) -> dict:
    data = "0x" + format(int(token_id), "064x") + format(int(value_shares * 1e6), "064x")
    return {
        "topics": [
            oc.TRANSFER_SINGLE_TOPIC,
            oc.topic_for_address(OTHER),   # operator
            oc.topic_for_address(frm),
            oc.topic_for_address(to),
        ],
        "data": data,
        "transactionHash": tx,
        "logIndex": hex(log_index),
        "blockNumber": hex(block),
        "blockTimestamp": hex(ts),
    }


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

    def test_buy_to_wallet(self):
        log = make_log(TOKEN_YES, frm=OTHER, to=WALLET, value_shares=5, tx="0xabc")
        fill = oc.decode_transfer_single(log, is_sell=False, asset_map=self.amap)
        self.assertEqual(fill["wallet"], WALLET)
        self.assertEqual(fill["side"], "BUY")
        self.assertEqual(fill["conditionId"], COND)
        self.assertEqual(fill["outcomeIndex"], 0)
        self.assertEqual(fill["size"], 5.0)
        self.assertEqual(fill["transactionHash"], "0xabc")

    def test_sell_from_wallet(self):
        log = make_log(TOKEN_NO, frm=WALLET, to=OTHER, value_shares=12.5, tx="0xdef")
        fill = oc.decode_transfer_single(log, is_sell=True, asset_map=self.amap)
        self.assertEqual(fill["wallet"], WALLET)
        self.assertEqual(fill["side"], "SELL")
        self.assertEqual(fill["outcomeIndex"], 1)
        self.assertEqual(fill["size"], 12.5)

    def test_off_scope_token_returns_none(self):
        log = make_log("999999", frm=OTHER, to=WALLET, value_shares=1, tx="0x1")
        self.assertIsNone(oc.decode_transfer_single(log, is_sell=False, asset_map=self.amap))

    def test_malformed_log_returns_none(self):
        self.assertIsNone(oc.decode_transfer_single({"topics": [], "data": "0x"},
                                                    is_sell=False, asset_map=self.amap))


class TestTradeCompatibility(unittest.TestCase):
    """On-chain trade dicts must work with the existing follow accessors/cursor."""

    def setUp(self):
        self.amap = oc.build_asset_map([{"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}])

    def _trade(self, *, tx, ts, side="BUY", token=TOKEN_YES, price=0.62):
        frm, to = (OTHER, WALLET) if side == "BUY" else (WALLET, OTHER)
        log = make_log(token, frm=frm, to=to, value_shares=5, tx=tx, ts=ts)
        fill = oc.decode_transfer_single(log, is_sell=(side == "SELL"), asset_map=self.amap)
        return oc.fill_to_trade(fill, price=price)

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


class _ScriptedWS:
    """Fake WSClient: replays a script of payloads / exceptions for recv_message."""

    def __init__(self, url, script):
        self.url = url
        self._script = list(script)
        self.sent = []

    def connect(self):
        pass

    def set_timeout(self, _t):
        pass

    def send_text(self, text):
        self.sent.append(text)

    def recv_message(self):
        if not self._script:
            raise oc.WSError("script exhausted")
        item = self._script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return (oc.WSClient.OP_TEXT, item)

    def close(self):
        pass


class TestHeartbeat(unittest.TestCase):
    def _collector(self, script, **kw):
        events = []
        col = oc.WSFollowCollector(
            wss_url="ws://x", https_url="http://x",
            wallets={WALLET},
            asset_map=oc.build_asset_map([{"conditionId": COND, "clobTokenIds": [TOKEN_YES, TOKEN_NO]}]),
            recv_timeout=0.001,
            on_event=lambda k, d: events.append((k, d)),
            ws_factory=lambda url: _ScriptedWS(url, script),
            **kw,
        )
        return col, events

    def test_silent_stall_flips_unhealthy_and_reconnects(self):
        # WS never pushes anything (not even newHeads) -> stale detection fires.
        class _StallWS(_ScriptedWS):
            def recv_message(self):
                time.sleep(0.002)  # let wall-clock advance toward stale_timeout
                raise oc.WSTimeout()

        events = []
        col = oc.WSFollowCollector(
            wss_url="ws://x", https_url="http://x", wallets={WALLET},
            recv_timeout=0.001, stale_timeout=0.05,
            on_event=lambda k, d: events.append((k, d)),
            ws_factory=lambda url: _StallWS(url, []),
        )
        with mock.patch.object(oc, "block_number", return_value=1000):
            with self.assertRaises(oc.WSError):
                col._connect_and_listen()
        self.assertFalse(col._healthy)
        self.assertTrue(any(k == "ws_stale" for k, _ in events))

    def test_heartbeat_keeps_alive_and_buffers_fill(self):
        buy_log = make_log(TOKEN_YES, frm=OTHER, to=WALLET, value_shares=100, tx="0xtx1", block=1001)
        script = [
            json.dumps({"id": 1, "result": "S1"}),
            json.dumps({"id": 2, "result": "S2"}),
            json.dumps({"id": 3, "result": "S3"}),
            json.dumps({"method": "eth_subscription", "params": {"subscription": "S3", "result": {"number": "0x3e8"}}}),
            json.dumps({"method": "eth_subscription", "params": {"subscription": "S1", "result": buy_log}}),
            oc.WSError("end"),
        ]
        col, events = self._collector(script, stale_timeout=5.0)
        with mock.patch.object(oc, "block_number", return_value=900):
            with self.assertRaises(oc.WSError):
                col._connect_and_listen()
        buffered = col.drain()
        self.assertIn(WALLET, buffered)
        self.assertEqual(col._last_block, 1001)  # max(newHeads 1000, fill 1001)
        self.assertFalse(any(k == "ws_stale" for k, _ in events))
        self.assertTrue(any(k == "ws_connected" for k, _ in events))


if __name__ == "__main__":
    unittest.main()
