"""On-chain follow detection: cursor-based eth_getLogs polling of CTF Exchange
``OrderFilled`` events.

Replaces the old WS ``eth_subscribe`` collector (which silently dropped log
subscriptions while newHeads kept the connection "healthy" → 10h blind windows)
and the old ``TransferSingle`` decode (which carried shares but NOT price, so the
follow entry price was a ``clob_price`` proxy — wrong by up to 0.20 vs the real
fill). A background thread polls ``eth_getLogs`` on a cursor every
``poll_interval`` seconds, decodes each ``OrderFilled`` where ``maker == watched
wallet`` (the wallet's own signed order; verified 12/12 fills carry maker=wallet
regardless of market-taker vs limit-maker), and buffers it per wallet with the
EXACT fill price (USDC / shares from the event). The follow loop drains the
buffer and feeds the SAME ``process_follow_trades`` pipeline.

Reliability: the cursor only advances after a successful poll, so a failed poll
just re-covers the gap next round — no permanent misses (the WS path's fatal
flaw). Dedup by (txHash, tokenId, logIndex), bounded by block age.

stdlib only. Read-only (we observe public chain logs; no orders, no keys).
Design: review/onchain-polling-design.md.
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
# CTF Exchange contracts that emit OrderFilled (v1 + v2). Lowercase.
EXCHANGE_ADDRESSES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",  # v1
    "0xe111180000d2663c0091e4f400237545b87b996b",  # v2
)
# keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
# — verified empirically against on-chain receipts (not computed; stdlib has no keccak).
# topics: [topic0, orderHash, maker(indexed), taker(indexed)]
# data:   [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
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
    """Live CLOB price for a token id. side=buy -> the ask we'd pay to follow.

    Retained as a utility; the follow path no longer uses it for entry price
    (we read the exact on-chain fill price from OrderFilled instead)."""
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


def decode_order_filled(log: dict, *, wallets: set[str], asset_map: dict[str, dict]) -> dict | None:
    """Decode a CTF Exchange ``OrderFilled`` log into the watched wallet's own fill.

    Only the wallet's OWN order leg is decoded (maker == wallet — the order's
    signer; verified that every fill carries maker=wallet whether the order took
    or made liquidity). Complementary mint legs (taker == wallet) are ignored.

    Layout (verified on-chain): topics = [topic0, orderHash, maker, taker];
    data = [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee].
    For maker=wallet: token = takerAssetId; makerAssetId==0 (USDC) => BUY
    (usdc=makerAmt, shares=takerAmt); else => SELL (usdc=takerAmt, shares=makerAmt).
    Price = usdc/shares (both 6-decimals → raw ratio). Returns None if off-scope
    or the price is not a sane (0, 1] probability (guard against unseen encodings).
    """
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    maker = addr_from_topic(topics[2])
    if maker not in wallets:
        return None
    data = (log.get("data") or "0x")[2:]
    if len(data) < 256:
        return None
    maker_asset = int(data[0:64], 16)
    taker_asset = int(data[64:128], 16)
    maker_amt = int(data[128:192], 16)
    taker_amt = int(data[192:256], 16)
    token_id = str(taker_asset)
    mapped = asset_map.get(token_id)
    if mapped is None:
        return None
    if maker_asset == 0:           # wallet gives USDC -> BUY
        usdc, shares, side = maker_amt, taker_amt, "BUY"
    else:                          # wallet gives token -> SELL (receives USDC)
        usdc, shares, side = taker_amt, maker_amt, "SELL"
    if shares <= 0 or usdc <= 0:
        return None
    price = usdc / shares          # 6-dec / 6-dec cancels -> probability price
    if not (0.0 < price <= 1.0):   # guard: unseen encoding -> skip, don't corrupt
        return None
    return {
        "wallet": maker,
        "conditionId": mapped["conditionId"],
        "outcomeIndex": mapped["outcomeIndex"],
        "tokenId": token_id,
        "side": side,
        "size": round(shares / 1e6, 6),
        "price": round(price, 6),
        "cash": round(usdc / 1e6, 6),
        "transactionHash": str(log.get("transactionHash") or "").lower(),
        "logIndex": int(log.get("logIndex"), 16) if log.get("logIndex") else 0,
        "blockNumber": int(log.get("blockNumber"), 16) if log.get("blockNumber") else 0,
        "blockTs": int(log.get("blockTimestamp"), 16) if log.get("blockTimestamp") else 0,
    }


def fill_to_trade(fill: dict, *, price: float | None = None, fallback_ts: int | None = None) -> dict:
    """Build a trade dict in the shape process_follow_trades / select_new_trades expect.

    timestamp <- blockTimestamp (canonical on-chain settlement time);
    id/transactionHash <- tx hash (matches the data-api cursor scheme so a
    getLogs<->data-api fallback switch keeps cursors comparable);
    price/curPrice <- the EXACT on-chain fill price decoded from OrderFilled
    (``fill["price"]``); the optional ``price`` arg overrides only if given.
    """
    ts = fill.get("blockTs") or fallback_ts or 0
    fill_price = price if price is not None else fill.get("price")
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
    if fill_price is not None:
        trade["price"] = fill_price
        trade["curPrice"] = fill_price
    return trade


# --------------------------------------------------------------------------- #
# collector
# --------------------------------------------------------------------------- #
class OnchainFollowCollector(threading.Thread):
    """Background thread: WS ``eth_subscribe`` logs for CTF Exchange ``OrderFilled``
    where maker is a watched wallet, decode the exact fill, buffer per wallet.

    主路 = WS 推送(空闲零 CU)。三层保活:① 主动 ``eth_blockNumber`` 心跳(``heartbeat_interval``,
    防空闲被踢 + 推进游标)② 无帧静默超 ``stale_timeout`` → 重连 ③ 满 ``ws_session_seconds`` → 主动
    整体重连(连接卫生)。**每次(重)连用 ``eth_getLogs`` 回补 [cursor−rewind → 当前块] 的缺口**
    (``_backfill``)——这补上了旧 WS 缺的一环:断线/卡顿丢的成交重连后补回,不再"踢掉就永久停"。
    WS 健康期间不发 getLogs。无 ``wss_url`` → 退回纯 getLogs 轮询(``_run_getlogs_poll``)。

    ``healthy`` 失败后置 False,follow 循环可回退 data-api。``drain()`` 取缓冲;``update_wallets``
    变更钱包集时置 ``_dirty`` 触发重订阅;``update_asset_map`` 仅换快照。
    """

    def __init__(
        self,
        *,
        https_url: str,
        wss_url: str | None = None,
        wallets: set[str] | None = None,
        asset_map: dict[str, dict] | None = None,
        poll_interval: float = 30.0,
        poll_overlap_blocks: int = 5,
        cold_start_lookback_blocks: int = 300,
        max_catchup_blocks: int = 600,
        unhealthy_after_failures: int = 2,
        seen_prune_margin_blocks: int = 40,
        # ── WS 三层保活 ──
        heartbeat_interval: float = 30.0,      # 主动发 eth_blockNumber:保活 + 推进游标
        heartbeat_lag_blocks: int = 5,         # 心跳块头减此余量作游标(末几块可能仍在途)
        stale_timeout: float = 90.0,           # 任何帧静默超此 → 判卡顿、重连
        ws_session_seconds: float = 3600.0,    # 每满此时长主动整体重连一次(连接卫生)
        reconnect_rewind_blocks: int = 150,    # 每次重连 getLogs 回补时回退的窗口(抓近期静默漏单,~5min)
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        cold_start_via_dataapi: bool = True,   # 冷启动(进程首连)不烧 getLogs 全量回补,交给 runner 走免费 data-api 补单
        on_event: Callable[[str, dict], None] | None = None,
    ):
        super().__init__(daemon=True, name="onchain-follow-collector")
        self.https_url = https_url
        self.wss_url = wss_url
        self.poll_interval = poll_interval
        self.poll_overlap_blocks = poll_overlap_blocks
        self.cold_start_lookback_blocks = cold_start_lookback_blocks
        self.max_catchup_blocks = max_catchup_blocks
        self.unhealthy_after_failures = max(1, int(unhealthy_after_failures))
        self.seen_prune_margin_blocks = seen_prune_margin_blocks
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_lag_blocks = max(0, int(heartbeat_lag_blocks))
        self.stale_timeout = stale_timeout
        self.ws_session_seconds = ws_session_seconds
        self.reconnect_rewind_blocks = max(0, int(reconnect_rewind_blocks))
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        self.cold_start_via_dataapi = bool(cold_start_via_dataapi)
        self._cold_catchup_pending = False
        self.on_event = on_event or (lambda *_: None)

        self._lock = threading.Lock()
        self._wallets = {w.lower() for w in (wallets or set())}
        self._asset_map = dict(asset_map or {})
        self._buffer: dict[str, list[dict]] = {}
        self._seen: dict[tuple[str, str, int], int] = {}  # (tx, token, logIndex) -> block
        self._stop_evt = threading.Event()
        self._dirty = threading.Event()  # wallet set changed → 重订阅
        self._healthy = False
        self._cursor = 0
        self._consecutive_failures = 0
        self._fill_count = 0

    # -- public API ------------------------------------------------------- #
    @property
    def healthy(self) -> bool:
        return self._healthy and not self._stop_evt.is_set()

    @property
    def fill_count(self) -> int:
        return self._fill_count

    @property
    def cold_catchup_pending(self) -> bool:
        """进程冷启动后置 True:runner 应本 tick 强制走 data-api 补停机期漏单,补完调 clear。"""
        return self._cold_catchup_pending

    def clear_cold_catchup(self) -> None:
        self._cold_catchup_pending = False

    def update_wallets(self, wallets: set[str]) -> None:
        new = {w.lower() for w in wallets}
        with self._lock:
            changed = new != self._wallets
            self._wallets = new
        if changed:
            self._dirty.set()  # 触发重订阅(只在集合真变时,避免每 tick 重连)

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

    # -- thread loop ------------------------------------------------------ #
    def run(self) -> None:
        # WS 主路(eth_subscribe logs);无 wss 配置 → 退回 getLogs 轮询。
        if not self.wss_url:
            self._run_getlogs_poll()
            return
        backoff = self.reconnect_min
        while not self._stop_evt.is_set():
            with self._lock:
                wallets = set(self._wallets)
            if not wallets:
                # 无钱包可跟 → 不连 WS,idle(零 CU);视为健康。
                self._healthy = True
                self._consecutive_failures = 0
                self._dirty.clear()
                if self._stop_evt.wait(min(self.poll_interval, 5.0)):
                    break
                continue
            try:
                self._run_ws_session(wallets)
                backoff = self.reconnect_min
            except Exception as exc:  # noqa: BLE001
                self._on_failure(exc, phase="ws")
                if self._stop_evt.wait(backoff):
                    break
                backoff = min(self.reconnect_max, backoff * 2)
        self._healthy = False

    def _run_getlogs_poll(self) -> None:
        """Fallback(无 wss):cursor getLogs 轮询(WS 之前的行为)。"""
        try:
            current = block_number(self.https_url)
            self._cursor = max(0, current - self.cold_start_lookback_blocks)
            self._poll_once(current_hint=current, cold_start=True)
        except Exception as exc:  # noqa: BLE001
            self._on_failure(exc, phase="cold_start")
        while not self._stop_evt.is_set():
            if self._stop_evt.wait(self.poll_interval):
                break
            self._poll_once()
        self._healthy = False

    def _backfill(self, wallets: set[str], *, rewind: int = 0, cold_start: bool = False) -> None:
        """每次(重)连用 getLogs 补 [cursor-rewind → 当前块] 的缺口。WS 健康期间不调用。"""
        current = block_number(self.https_url)
        if self._cursor <= 0:
            self._cursor = max(0, current - self.cold_start_lookback_blocks)
        from_block = max(0, self._cursor + 1 - self.poll_overlap_blocks - max(0, rewind))
        if current - from_block > self.max_catchup_blocks:
            skipped = from_block
            from_block = current - self.max_catchup_blocks
            self.on_event("backfill_gap_skipped", {"from": skipped, "to": from_block})
        before = self._fill_count
        self._scan(from_block, current, wallets)
        self._cursor = max(self._cursor, current)
        self._prune_seen()
        self.on_event("backfill", {"fills": self._fill_count - before, "from": from_block, "to": current, "cold_start": cold_start})

    def _run_ws_session(self, wallets: set[str]) -> None:
        """一次 WS 会话:订阅 OrderFilled logs → 连上即 getLogs 回补缺口 → 收推送 +
        三层保活(主动 eth_blockNumber 心跳 / 无帧卡顿重连 / 满 session 时长主动重连)。
        退出本函数 = 需要重连(钱包变更 / 卡顿 / 定期 / 异常),由 run() 重连并再次回补。"""
        ws = WSClient(self.wss_url)
        try:
            # connect() 放进 try:连接失败时 finally 仍会 ws.close()(虽然 connect 内部已自关,
            # 这里再兜一层,确保任何路径都不漏 socket fd)。
            ws.connect()
            maker_topics = [topic_for_address(w) for w in wallets]
            ws.send_text(json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                "params": ["logs", {"address": list(EXCHANGE_ADDRESSES),
                                    "topics": [ORDER_FILLED_TOPIC, None, maker_topics, None]}],
            }))
            # 连上即回补。**冷启动(进程首连,cursor<=0)默认不用 getLogs**:补单不要求时效,
            # 交给 runner 走免费 data-api 兜(置 cold_catchup_pending,只用 1 次 eth_blockNumber 把游标
            # 定到当前块头)。同进程内的**重连**(cursor>0,健康期 stale/recycle/异常)仍用 rewind 小窗
            # getLogs 保证 live 续连的时效。这样 getLogs 只在"真有实时缺口"时发,重启不再烧全量回补。
            cold = self._cursor <= 0
            if cold and self.cold_start_via_dataapi:
                self._cursor = block_number(self.https_url)
                self._cold_catchup_pending = True
                self.on_event("cold_start_dataapi", {"cursor": self._cursor})
            else:
                self._backfill(wallets, rewind=0 if cold else self.reconnect_rewind_blocks, cold_start=cold)
            self._dirty.clear()
            self._healthy = True
            self._consecutive_failures = 0
            ws.set_timeout(1.0)
            hb_id = 1000
            now = time.monotonic()
            session_start = now
            last_hb = now
            last_alive = now
            while not self._stop_evt.is_set() and not self._dirty.is_set():
                now = time.monotonic()
                if now - session_start >= self.ws_session_seconds:
                    self.on_event("ws_session_recycle", {"elapsed_s": round(now - session_start)})
                    return  # 定期整体重连(连接卫生)→ run() 重连 + 回补
                if now - last_hb >= self.heartbeat_interval:
                    hb_id += 1
                    ws.send_text(json.dumps({"jsonrpc": "2.0", "id": hb_id, "method": "eth_blockNumber", "params": []}))
                    last_hb = now
                try:
                    opcode, payload = ws.recv_message()
                except WSTimeout:
                    if time.monotonic() - last_alive > self.stale_timeout:
                        self.on_event("ws_stale", {"silent_s": round(time.monotonic() - last_alive, 1)})
                        return  # 卡顿 → 重连 + 回补
                    continue
                if opcode == WSClient.OP_CLOSE:
                    self.on_event("ws_closed", {})
                    return
                if opcode != WSClient.OP_TEXT:
                    continue
                last_alive = time.monotonic()
                try:
                    msg = json.loads(payload)
                except Exception:  # noqa: BLE001
                    continue
                if msg.get("method") == "eth_subscription":
                    log = (msg.get("params") or {}).get("result") or {}
                    self._handle_log(log)
                    bn = log.get("blockNumber")
                    if bn:
                        try:
                            self._cursor = max(self._cursor, int(bn, 16))
                        except Exception:  # noqa: BLE001
                            pass
                elif "result" in msg and isinstance(msg.get("id"), int) and msg["id"] >= 1000:
                    # 心跳 eth_blockNumber 响应:活着 + 推进游标到块头(减余量)。
                    try:
                        head = int(msg["result"], 16)
                    except Exception:  # noqa: BLE001
                        head = 0
                    if head:
                        self._cursor = max(self._cursor, head - self.heartbeat_lag_blocks)
                    self._healthy = True
                    self._consecutive_failures = 0
        finally:
            ws.close()

    def _poll_once(self, *, current_hint: int | None = None, cold_start: bool = False) -> None:
        with self._lock:
            wallets = set(self._wallets)
        if not wallets:
            self._healthy = True  # nothing to watch; healthy-idle, don't flap to data-api
            self._consecutive_failures = 0
            return
        try:
            current = current_hint if current_hint is not None else block_number(self.https_url)
        except Exception as exc:  # noqa: BLE001
            self._on_failure(exc, phase="block_number")
            return
        from_block = max(0, self._cursor + 1 - self.poll_overlap_blocks)
        if current - from_block > self.max_catchup_blocks:
            skipped = from_block
            from_block = current - self.max_catchup_blocks
            self.on_event("poll_gap_skipped", {"from": skipped, "to": from_block})
        before = self._fill_count
        try:
            self._scan(from_block, current, wallets)
        except Exception as exc:  # noqa: BLE001
            self._on_failure(exc, phase="getlogs")
            return
        self._cursor = current
        self._consecutive_failures = 0
        self._healthy = True
        self._prune_seen()
        recovered = self._fill_count - before
        if recovered or cold_start:
            self.on_event("poll", {"fills": recovered, "from": from_block, "to": current, "cold_start": cold_start})

    def _on_failure(self, exc: Exception, *, phase: str) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.unhealthy_after_failures:
            self._healthy = False
        self.on_event("poll_error", {"error": str(exc)[:200], "phase": phase, "consecutive": self._consecutive_failures})

    def _scan(self, from_block: int, to_block: int, wallets: set[str]) -> None:
        """getLogs OrderFilled with maker in `wallets`, chunked by GETLOGS_MAX_SPAN.

        One topic filter (maker = topic index 2) suffices: the wallet's own order
        always carries maker=wallet, so the complementary taker=wallet legs (which
        we must NOT count) are naturally excluded."""
        if from_block > to_block or not wallets:
            return
        maker_topics = [topic_for_address(w) for w in wallets]
        block = from_block
        while block <= to_block and not self._stop_evt.is_set():
            end = min(block + GETLOGS_MAX_SPAN - 1, to_block)
            logs = rpc_call(self.https_url, "eth_getLogs", [{
                "fromBlock": hex(block), "toBlock": hex(end),
                "address": list(EXCHANGE_ADDRESSES),
                "topics": [ORDER_FILLED_TOPIC, None, maker_topics, None],
            }])
            for log in logs or []:
                self._handle_log(log)
            block = end + 1

    def _handle_log(self, log: dict) -> None:
        with self._lock:
            asset_map = self._asset_map
            wallets = self._wallets
        fill = decode_order_filled(log, wallets=wallets, asset_map=asset_map)
        if fill is None:
            return
        key = (fill["transactionHash"], fill["tokenId"], fill["logIndex"])
        with self._lock:
            if key in self._seen:
                return
            self._seen[key] = fill["blockNumber"]
            self._buffer.setdefault(fill["wallet"], []).append(fill)
            self._fill_count += 1
        self.on_event("fill", fill)

    def _prune_seen(self) -> None:
        cutoff = self._cursor - self.poll_overlap_blocks - self.seen_prune_margin_blocks
        if cutoff <= 0:
            return
        with self._lock:
            self._seen = {k: b for k, b in self._seen.items() if b >= cutoff}
