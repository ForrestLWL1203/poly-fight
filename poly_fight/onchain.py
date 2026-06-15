"""On-chain follow detection: near-real-time CTF TransferSingle stream.

Replaces the data-api per-wallet polling as the follow detector. A
``WSFollowCollector`` thread holds two ``eth_subscribe`` log filters (buy: the
target wallet in `to`; sell: in `from`) over the Polygon CTF contract, decodes
each fill, maps the ERC1155 token id to (conditionId, outcomeIndex) via the
watched markets' ``clobTokenIds``, and buffers it per wallet. The follow loop
drains the buffer on a short cadence and feeds the SAME ``process_follow_trades``
pipeline, so CLV/quarantine/settlement/persistence are untouched.

On-chain ``TransferSingle`` carries shares but not the wallet's fill price; the
follow entry price comes from the live CLOB ask (``clob_price``). Because WS
detection is near-instant (sub-second after the block), the price hasn't drifted,
so using the current ask as both our entry and the wallet's reference is sound.

stdlib only. Read-only (we observe public chain logs; no orders, no keys).
Measured rationale + latency numbers: review/onchain-probe-findings.md.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

from .ws_client import WSClient, WSError, WSTimeout

# Polygon mainnet.
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
GETLOGS_MAX_SPAN = 10  # Alchemy free tier caps eth_getLogs range at 10 blocks.
CLOB_BASE = "https://clob.polymarket.com"


# --------------------------------------------------------------------------- #
# config + low-level helpers
# --------------------------------------------------------------------------- #
def load_rpc_endpoints(path: str = "secret/rpc") -> tuple[str | None, str | None]:
    """Return (https_url, wss_url) from secret/rpc, or (None, None) if absent."""
    p = Path(path)
    if not p.exists():
        return None, None
    https_url = wss_url = None
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("https://"):
            https_url = line
        elif line.startswith("wss://"):
            wss_url = line
    if https_url and not wss_url:
        wss_url = https_url.replace("https://", "wss://", 1)
    if wss_url and not https_url:
        https_url = wss_url.replace("wss://", "https://", 1)
    return https_url, wss_url


def topic_for_address(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:].lower()


def addr_from_topic(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def rpc_call(url: str, method: str, params: list, *, timeout: float = 20.0, retries: int = 3) -> Any:
    """JSON-RPC over stdlib urllib with retries (Alchemy TLS is occasionally flaky)."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, data=payload, headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if "error" in data:
                raise RuntimeError(str(data["error"]))
            return data.get("result")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"rpc {method} failed after {retries}: {last}")


def block_number(https_url: str) -> int:
    return int(rpc_call(https_url, "eth_blockNumber", []), 16)


def clob_price(token_id: str, side: str = "buy", *, base: str = CLOB_BASE, timeout: float = 8.0) -> float | None:
    """Live CLOB price for a token id. side=buy -> the ask we'd pay to follow."""
    url = f"{base}/price?token_id={token_id}&side={side.lower()}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        value = data.get("price") if isinstance(data, dict) else None
        return float(value) if value is not None else None
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# decoding
# --------------------------------------------------------------------------- #
def build_asset_map(markets: list[dict] | dict[str, dict]) -> dict[str, dict]:
    """{ tokenId(str) -> {"conditionId": <lower>, "outcomeIndex": int} }.

    Accepts a list of market dicts or a {conditionId: market} mapping. Reads
    ``clobTokenIds`` (JSON string or list) paired positionally with outcomes.
    """
    market_iter = markets.values() if isinstance(markets, dict) else markets
    asset_map: dict[str, dict] = {}
    for market in market_iter:
        raw = market.get("clobTokenIds") or market.get("clob_token_ids")
        if not raw:
            continue
        try:
            token_ids = json.loads(raw) if isinstance(raw, str) else list(raw)
        except (ValueError, TypeError):
            continue
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").lower()
        if not condition_id:
            continue
        for idx, token in enumerate(token_ids):
            token_str = str(token)
            if token_str:
                asset_map[token_str] = {"conditionId": condition_id, "outcomeIndex": idx}
    return asset_map


def decode_transfer_single(log: dict, *, is_sell: bool, asset_map: dict[str, dict]) -> dict | None:
    """Decode a CTF TransferSingle log into a fill dict, or None if off-scope.

    is_sell=True -> the watched wallet is `from` (topic2, sold); else `to`
    (topic3, bought). Returns None when the token id isn't in asset_map.
    """
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    data = (log.get("data") or "0x")[2:]
    if len(data) < 128:
        return None
    token_id = str(int(data[0:64], 16))
    mapped = asset_map.get(token_id)
    if mapped is None:
        return None
    value = int(data[64:128], 16)
    wallet = addr_from_topic(topics[2] if is_sell else topics[3])
    return {
        "wallet": wallet,
        "conditionId": mapped["conditionId"],
        "outcomeIndex": mapped["outcomeIndex"],
        "tokenId": token_id,
        "side": "SELL" if is_sell else "BUY",
        "size": round(value / 1e6, 6),       # ERC1155 shares carry 6 decimals
        "transactionHash": str(log.get("transactionHash") or "").lower(),
        "logIndex": int(log.get("logIndex"), 16) if log.get("logIndex") else 0,
        "blockNumber": int(log.get("blockNumber"), 16) if log.get("blockNumber") else 0,
        "blockTs": int(log.get("blockTimestamp"), 16) if log.get("blockTimestamp") else 0,
    }


def fill_to_trade(fill: dict, *, price: float | None, fallback_ts: int | None = None) -> dict:
    """Build a trade dict in the shape process_follow_trades / select_new_trades expect.

    timestamp <- blockTimestamp (the canonical on-chain settlement time);
    id/transactionHash <- tx hash (matches the data-api cursor scheme so a
    WS<->data-api fallback switch keeps cursors comparable);
    price/curPrice <- live CLOB ask (used for our entry + slippage reference).
    """
    ts = fill.get("blockTs") or fallback_ts or 0
    trade: dict[str, Any] = {
        "conditionId": fill["conditionId"],
        "outcomeIndex": fill["outcomeIndex"],
        "asset": fill["tokenId"],
        "side": fill["side"],
        "size": fill["size"],
        "timestamp": int(ts),
        "transactionHash": fill["transactionHash"],
        "id": fill["transactionHash"],
        "source": "onchain",
    }
    if price is not None:
        trade["price"] = price
        trade["curPrice"] = price
    return trade


# --------------------------------------------------------------------------- #
# collector
# --------------------------------------------------------------------------- #
class WSFollowCollector(threading.Thread):
    """Background thread: subscribe to CTF TransferSingle for a wallet set.

    Buffers decoded fills per wallet (deduped by tx+token). The follow loop calls
    ``drain()`` each short cycle. ``healthy`` is False whenever the WS is down so
    the loop can fall back to data-api polling. ``update_wallets`` /
    ``update_asset_map`` trigger a clean resubscribe.
    """

    def __init__(
        self,
        *,
        wss_url: str,
        https_url: str,
        wallets: set[str] | None = None,
        asset_map: dict[str, dict] | None = None,
        recv_timeout: float = 1.0,
        stale_timeout: float = 90.0,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        on_event: Callable[[str, dict], None] | None = None,
        ws_factory: Callable[[str], Any] | None = None,
    ):
        super().__init__(daemon=True, name="ws-follow-collector")
        self.wss_url = wss_url
        self.https_url = https_url
        self.recv_timeout = recv_timeout
        # 静默停推检测:WS 不断开但停止推送时(healthy 仍 True、却收不到任何成交,
        # data-api 兜底也不触发)。订阅 newHeads 当独立心跳,超过 stale_timeout 没收到
        # 任何消息就判定停推 → 翻 unhealthy + 重连(进而触发 data-api 兜底)。
        self.stale_timeout = stale_timeout
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        self.on_event = on_event or (lambda *_: None)
        self._ws_factory = ws_factory or WSClient

        self._lock = threading.Lock()
        self._wallets = {w.lower() for w in (wallets or set())}
        self._asset_map = dict(asset_map or {})
        self._buffer: dict[str, list[dict]] = {}
        self._seen: set[tuple[str, str]] = set()
        self._dirty = threading.Event()
        self._stop_evt = threading.Event()
        self._healthy = False
        self._last_block = 0
        self._fill_count = 0

    # -- public API ------------------------------------------------------- #
    @property
    def healthy(self) -> bool:
        return self._healthy and not self._stop_evt.is_set()

    @property
    def fill_count(self) -> int:
        return self._fill_count

    def update_wallets(self, wallets: set[str]) -> None:
        new = {w.lower() for w in wallets}
        with self._lock:
            if new == self._wallets:
                return
            self._wallets = new
        self._dirty.set()

    def update_asset_map(self, asset_map: dict[str, dict]) -> None:
        with self._lock:
            self._asset_map = dict(asset_map)

    def drain(self) -> dict[str, list[dict]]:
        """Return and clear buffered fills, grouped by wallet."""
        with self._lock:
            out = self._buffer
            self._buffer = {}
        return out

    def stop(self) -> None:
        self._stop_evt.set()
        self._dirty.set()

    # -- thread loop ------------------------------------------------------ #
    def run(self) -> None:
        backoff = self.reconnect_min
        while not self._stop_evt.is_set():
            try:
                self._connect_and_listen()
                backoff = self.reconnect_min
            except Exception as exc:  # noqa: BLE001
                self._healthy = False
                self.on_event("ws_error", {"error": str(exc)[:200]})
                if self._stop_evt.is_set():
                    break
                self._stop_evt.wait(backoff)
                backoff = min(self.reconnect_max, backoff * 2)
        self._healthy = False

    def _connect_and_listen(self) -> None:
        with self._lock:
            wallets = set(self._wallets)
        self._dirty.clear()
        ws = self._ws_factory(self.wss_url)
        ws.connect()
        ws.set_timeout(self.recv_timeout)
        try:
            # Backfill any gap since the last disconnect, then track from current.
            current = block_number(self.https_url)
            if self._last_block and current > self._last_block:
                self._backfill(self._last_block + 1, current, wallets)
            self._last_block = current

            sub_buy = sub_sell = sub_block = None
            if wallets:
                topics = [topic_for_address(w) for w in wallets]
                if len(topics) > 900:
                    self.on_event("wallet_set_large", {"count": len(topics)})
                ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                                         "params": ["logs", {"address": CTF_ADDRESS,
                                                             "topics": [TRANSFER_SINGLE_TOPIC, None, None, topics]}]}))
                ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
                                         "params": ["logs", {"address": CTF_ADDRESS,
                                                             "topics": [TRANSFER_SINGLE_TOPIC, None, topics, None]}]}))
            # Heartbeat: newHeads streams a block ~every 2s regardless of trades, so
            # silence here (unlike on the logs subs) unambiguously means a stall.
            ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 3, "method": "eth_subscribe",
                                     "params": ["newHeads"]}))
            self._healthy = True
            self.on_event("ws_connected", {"wallets": len(wallets)})

            last_msg_mono = time.monotonic()
            while not self._stop_evt.is_set() and not self._dirty.is_set():
                try:
                    opcode, payload = ws.recv_message()
                except WSTimeout:
                    if time.monotonic() - last_msg_mono > self.stale_timeout:
                        self._healthy = False  # flip first so command_follow falls back now
                        idle = round(time.monotonic() - last_msg_mono, 1)
                        self.on_event("ws_stale", {"idle_seconds": idle})
                        raise WSError(f"stale: no ws message for {idle}s")
                    continue
                last_msg_mono = time.monotonic()  # any frame (incl. newHeads) is a heartbeat
                if opcode == WSClient.OP_CLOSE:
                    raise WSError("server closed")
                try:
                    msg = json.loads(payload)
                except ValueError:
                    continue
                if "id" in msg and "result" in msg:
                    if msg["id"] == 1:
                        sub_buy = msg["result"]
                    elif msg["id"] == 2:
                        sub_sell = msg["result"]
                    elif msg["id"] == 3:
                        sub_block = msg["result"]
                    continue
                if msg.get("method") != "eth_subscription":
                    continue
                params = msg.get("params", {})
                sub = params.get("subscription")
                log = params.get("result", {})
                if sub == sub_buy:
                    self._handle_log(log, is_sell=False)
                elif sub == sub_sell:
                    self._handle_log(log, is_sell=True)
                elif sub == sub_block:
                    # heartbeat only; advance the block cursor so a reconnect backfills
                    # exactly the gap.
                    try:
                        num = int(str(log.get("number")), 16)
                    except (TypeError, ValueError):
                        num = 0
                    if num > self._last_block:
                        self._last_block = num
        finally:
            self._healthy = False
            ws.close()

    def _handle_log(self, log: dict, *, is_sell: bool) -> None:
        with self._lock:
            asset_map = self._asset_map
            wallets = self._wallets
        fill = decode_transfer_single(log, is_sell=is_sell, asset_map=asset_map)
        if fill is None or fill["wallet"] not in wallets:
            return
        key = (fill["transactionHash"], fill["tokenId"])
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            self._buffer.setdefault(fill["wallet"], []).append(fill)
            self._fill_count += 1
            if fill["blockNumber"] > self._last_block:
                self._last_block = fill["blockNumber"]
        self.on_event("fill", fill)

    def _backfill(self, from_block: int, to_block: int, wallets: set[str]) -> None:
        if not wallets:
            return
        topics = [topic_for_address(w) for w in wallets]
        block = from_block
        while block <= to_block and not self._stop_evt.is_set():
            end = min(block + GETLOGS_MAX_SPAN - 1, to_block)
            for is_sell, topic_filter in (
                (False, [TRANSFER_SINGLE_TOPIC, None, None, topics]),
                (True, [TRANSFER_SINGLE_TOPIC, None, topics, None]),
            ):
                logs = rpc_call(self.https_url, "eth_getLogs", [{
                    "fromBlock": hex(block), "toBlock": hex(end),
                    "address": CTF_ADDRESS, "topics": topic_filter,
                }])
                for log in logs or []:
                    self._handle_log(log, is_sell=is_sell)
            block = end + 1
        self.on_event("backfill", {"from": from_block, "to": to_block})
