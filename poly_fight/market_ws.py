"""Polymarket market-channel WS: near-real-time market resolution (settlement).

Subscribes to the public CLOB **market channel** for the token ids of the markets
we currently hold open follows on, and buffers ``market_resolved`` events into
``{conditionId -> winning outcomeIndex}``. The follow loop drains this instead of
polling data-api for settlement; data-api stays the fallback whenever the WS is
unavailable (``healthy`` is False) or for markets not covered by the subscription.

Read-only (public market data; no orders, no keys). stdlib only — reuses
``ws_client.WSClient`` (the same hand-rolled WS client the on-chain collector uses,
which we keep because live trading will also need it for the user channel).

Design mirrors ``onchain.WSFollowCollector``: a daemon thread with reconnect +
backoff, a ``healthy`` flag, a drained buffer, and a dirty-flag resubscribe when
the watched token set changes. Two robustness nets for "resolved while we were
disconnected": a one-shot ``reconcile`` data-api sweep right after every (re)connect.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

from .ws_client import WSClient, WSError, WSTimeout

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10.0  # market channel: send PING every 10s or the server drops us.


class WSResolutionCollector(threading.Thread):
    """Background thread: subscribe to the market channel for a token set and
    buffer market resolutions.

    ``asset_map`` ({tokenId -> {"conditionId","outcomeIndex"}}) is accumulated
    (merged) so a market's tokens, once learned, survive after the match leaves
    the upcoming-window watch list. ``set_conditions`` selects which conditions'
    tokens to actually subscribe to (= the markets we hold open follows on).
    """

    def __init__(
        self,
        *,
        ws_url: str = MARKET_WS_URL,
        reconcile: Callable[[list[str]], dict[str, int]] | None = None,
        recv_timeout: float = 1.0,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        on_event: Callable[[str, dict], None] | None = None,
    ):
        super().__init__(daemon=True, name="ws-resolution-collector")
        self.ws_url = ws_url
        self.reconcile = reconcile
        self.recv_timeout = recv_timeout
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        self.on_event = on_event or (lambda *_: None)

        self._lock = threading.Lock()
        self._asset_map: dict[str, dict] = {}          # tokenId -> {conditionId, outcomeIndex}
        self._conditions: set[str] = set()             # conditions we want subscribed
        self._sub_tokens: tuple[str, ...] = ()         # token ids currently subscribed
        self._buffer: dict[str, int] = {}              # conditionId -> winning outcomeIndex
        self._dirty = threading.Event()
        self._stop_evt = threading.Event()
        self._healthy = False
        self._resolved_count = 0

    # -- public API ------------------------------------------------------- #
    @property
    def healthy(self) -> bool:
        return self._healthy and not self._stop_evt.is_set()

    @property
    def resolved_count(self) -> int:
        return self._resolved_count

    def mapped_conditions(self) -> set[str]:
        with self._lock:
            return {m["conditionId"] for m in self._asset_map.values()}

    def merge_asset_map(self, asset_map: dict[str, dict]) -> None:
        if not asset_map:
            return
        with self._lock:
            self._asset_map.update(asset_map)

    def retain_conditions(self, conditions: set[str]) -> None:
        """Drop token mappings for conditions we no longer track, so historical
        (ended) matches don't accumulate in the map without bound."""
        keep = {str(c).lower() for c in conditions if c}
        with self._lock:
            self._asset_map = {
                token: m for token, m in self._asset_map.items() if m.get("conditionId") in keep
            }

    def set_conditions(self, conditions: set[str]) -> None:
        """Set which conditions' tokens to subscribe to; resubscribe if the
        resulting token set changed."""
        conditions = {str(c).lower() for c in conditions if c}
        with self._lock:
            self._conditions = conditions
            tokens = tuple(sorted(
                token for token, m in self._asset_map.items()
                if m.get("conditionId") in conditions
            ))
            if tokens == self._sub_tokens:
                return
            self._sub_tokens = tokens
        self._dirty.set()

    def drain(self) -> dict[str, int]:
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
            with self._lock:
                tokens = self._sub_tokens
            if not tokens:
                # Nothing to watch yet; wait for a subscription (or stop).
                self._healthy = False
                self._dirty.wait(timeout=2.0)
                self._dirty.clear()
                continue
            try:
                self._connect_and_listen(tokens)
                backoff = self.reconnect_min
            except Exception as exc:  # noqa: BLE001
                self._healthy = False
                self.on_event("ws_error", {"error": str(exc)[:200]})
                if self._stop_evt.is_set():
                    break
                self._stop_evt.wait(backoff)
                backoff = min(self.reconnect_max, backoff * 2)
        self._healthy = False

    def _connect_and_listen(self, tokens: tuple[str, ...]) -> None:
        self._dirty.clear()
        ws = WSClient(self.ws_url)
        ws.connect()
        ws.set_timeout(self.recv_timeout)
        try:
            ws.send_text(json.dumps({
                "assets_ids": list(tokens),
                "type": "market",
                "custom_feature_enabled": True,
            }))
            # Catch anything that resolved while we were disconnected.
            self._reconcile_now()
            self._healthy = True
            self.on_event("ws_connected", {"tokens": len(tokens)})

            last_ping = time.monotonic()
            while not self._stop_evt.is_set() and not self._dirty.is_set():
                now = time.monotonic()
                if now - last_ping >= PING_INTERVAL:
                    ws.send_text("PING")
                    last_ping = now
                try:
                    opcode, payload = ws.recv_message()
                except WSTimeout:
                    continue
                if opcode == WSClient.OP_CLOSE:
                    raise WSError("server closed")
                self._handle_payload(payload)
        finally:
            self._healthy = False
            ws.close()

    def _handle_payload(self, payload: str) -> None:
        text = payload.strip()
        if not text or text == "PONG":
            return
        try:
            msg = json.loads(text)
        except ValueError:
            return
        events = msg if isinstance(msg, list) else [msg]
        for event in events:
            if isinstance(event, dict) and event.get("event_type") == "market_resolved":
                self._record_resolution(event)

    def _record_resolution(self, event: dict) -> None:
        win_asset = str(event.get("winning_asset_id") or "")
        with self._lock:
            mapped = self._asset_map.get(win_asset)
        condition_id = ""
        outcome_index: int | None = None
        if mapped:
            condition_id = mapped["conditionId"]
            outcome_index = int(mapped["outcomeIndex"])
        if outcome_index is None:
            # winning_asset_id unknown to us -> let the data-api fallback settle it.
            return
        with self._lock:
            self._buffer[condition_id] = outcome_index
            self._resolved_count += 1
        self.on_event("market_resolved", {"conditionId": condition_id, "outcomeIndex": outcome_index})

    def _reconcile_now(self) -> None:
        if self.reconcile is None:
            return
        with self._lock:
            conditions = sorted(self._conditions)
        if not conditions:
            return
        try:
            resolved = self.reconcile(conditions) or {}
        except Exception as exc:  # noqa: BLE001
            self.on_event("reconcile_error", {"error": str(exc)[:200]})
            return
        if resolved:
            with self._lock:
                for cid, idx in resolved.items():
                    self._buffer[str(cid).lower()] = int(idx)
            self.on_event("reconcile", {"resolved": len(resolved)})
