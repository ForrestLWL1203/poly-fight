from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import mimetypes
import os
import re
import signal
import shlex
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .control import _pid_alive, read_follow_control, reconcile_wallet_refresh_status, set_pause_new_signals, update_wallet_refresh_status, write_follow_control
from .ai_risk import AiConfigStore, DeepSeekClient, ai_audit_summary
from .core import (
    GAME_FAMILY_LABELS,
    LEAGUE_LABELS,
    MARKET_TYPE_LABELS,
    bucket_label,
    parse_dt,
    parse_jsonish,
    to_float,
)
from .cli import V2_DEFAULT_MAX_PROFILE_WALLETS, enrich_esports_bucket_scores, prepare_category_refresh_dir
from .follow_strategy import default_follow_strategy, strategy_summary, validate_follow_strategy
from .storage import FollowStore, LeaderboardStore


COOKIE_NAME = "poly_fight_session"
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
FAILED_LOGINS: dict[str, list[float]] = {}
_TRADES_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_TRADES_CACHE_LOCK = threading.Lock()
# leaderboard 排名映射缓存:键=各类目 leaderboard.db 的 mtime;榜单未重建则复用,避免每次打开
# 跟单详情都重新 enrich+排序整张榜(~93 钱包的 bucket 分重算,实测占详情耗时 90%)。
_RANK_CACHE: dict[str, tuple[tuple[int, ...], dict[str, int]]] = {}
_RANK_CACHE_LOCK = threading.Lock()
_MATCH_TITLE_RE = re.compile(r"^([^:]+):\s+(.+?)\s+vs\s+(.+?)(\s+\([^)]+\))?\s+-\s+(.+)$", re.IGNORECASE)
_SPORTS_TITLE_RE = re.compile(r"^(.+?)\s+vs\.?\s+(.+?)(?:\s+-\s+(.+))?$", re.IGNORECASE)
_ESPORTS_GAME_ORDER = {"dota2": 0, "cs2": 1, "lol": 2, "valorant": 3}


@dataclass(frozen=True)
class DashboardConfig:
    data_dir: Path
    follow_dir: Path | None = None
    log_dir: Path | None = None
    host: str = "127.0.0.1"
    port: int = 8787
    username: str = "admin"
    password: str = ""
    cookie_secret: str = ""
    session_ttl_seconds: int = 7 * 24 * 3600  # 滑动续期下这是"真正闲置"超时(标签页开着会一直续)
    cookie_secure: bool = False
    static_dir: Path | None = None
    client: Any = None
    trades_cache_ttl_seconds: int = 30
    observe_window_hours: float = 24.0
    wallet_refresh_runner: Any = None
    wallet_refresh_timeout_seconds: int = 7200
    runner_stake_usdc: float = 1.0
    runner_stake_ratio_percent: float = 10.0
    runner_max_stake_usdc: float = 0.0
    runner_max_signal_stake_balance_percent: float = 0.0
    stream_poll_seconds: float = 2.0
    stream_heartbeat_seconds: float = 15.0
    max_stream_clients: int = 8
    runner_process_starter: Any = None
    runner_process_lister: Any = None
    runner_process_stopper: Any = None


def short_addr(addr: str | None) -> str:
    text = str(addr or "")
    if len(text) <= 11:
        return text
    return f"{text[:5]}...{text[-3:]}"


def _request_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def make_session_token(username: str, secret: str, *, now: int | None = None) -> str:
    issued_at = int(now if now is not None else time.time())
    payload = _b64(json.dumps({"u": username, "iat": issued_at}, separators=(",", ":")).encode())
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return f"{payload}.{_b64(sig)}"


def verify_session_token(token: str, secret: str, *, max_age_seconds: int, now: int | None = None) -> str | None:
    if not token or "." not in token or not secret:
        return None
    payload, sig = token.rsplit(".", 1)
    expected = _b64(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        data = json.loads(_unb64(payload))
    except (ValueError, json.JSONDecodeError):
        return None
    issued_at = int(data.get("iat") or 0)
    current = int(now if now is not None else time.time())
    if issued_at <= 0 or current - issued_at > max_age_seconds:
        return None
    username = str(data.get("u") or "")
    return username or None


def create_server(config: DashboardConfig) -> ThreadingHTTPServer:
    if not config.password:
        raise ValueError("POLY_FIGHT_DASH_PASSWORD is required")
    if not config.cookie_secret:
        raise ValueError("POLY_FIGHT_DASH_COOKIE_SECRET is required")
    static_dir = config.static_dir or Path(__file__).with_name("dashboardV2")
    resolved = DashboardConfig(
        data_dir=config.data_dir,
        follow_dir=config.follow_dir or config.data_dir / "follow",
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        cookie_secret=config.cookie_secret,
        session_ttl_seconds=config.session_ttl_seconds,
        cookie_secure=config.cookie_secure,
        static_dir=static_dir,
        log_dir=config.log_dir,
        client=config.client,
        trades_cache_ttl_seconds=config.trades_cache_ttl_seconds,
        observe_window_hours=config.observe_window_hours,
        wallet_refresh_runner=config.wallet_refresh_runner,
        wallet_refresh_timeout_seconds=config.wallet_refresh_timeout_seconds,
        runner_stake_usdc=config.runner_stake_usdc,
        runner_stake_ratio_percent=config.runner_stake_ratio_percent,
        runner_max_stake_usdc=config.runner_max_stake_usdc,
        runner_max_signal_stake_balance_percent=config.runner_max_signal_stake_balance_percent,
        stream_poll_seconds=config.stream_poll_seconds,
        stream_heartbeat_seconds=config.stream_heartbeat_seconds,
        max_stream_clients=config.max_stream_clients,
        runner_process_starter=config.runner_process_starter,
        runner_process_lister=config.runner_process_lister,
        runner_process_stopper=config.runner_process_stopper,
    )

    class Handler(DashboardHandler):
        dashboard_config = resolved
        started_at = time.time()

    server = ThreadingHTTPServer((resolved.host, resolved.port), Handler)
    server.active_stream_clients = 0  # type: ignore[attr-defined]
    server.stream_clients_lock = threading.Lock()  # type: ignore[attr-defined]
    return server


class DashboardHandler(BaseHTTPRequestHandler):
    dashboard_config: DashboardConfig
    started_at: float

    server_version = "PolyFightDashboard/0.1"

    def handle(self) -> None:
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError):
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            if not self._authenticated():
                self._error("unauthorized", status=HTTPStatus.UNAUTHORIZED)
                return
            self._handle_api_get(parsed)
            return
        self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/login":
            self._login()
            return
        if parsed.path == "/api/logout":
            self._logout()
            return
        if parsed.path.startswith("/api/"):
            if not self._authenticated():
                self._error("unauthorized", status=HTTPStatus.UNAUTHORIZED)
                return
            if parsed.path == "/api/wallet-refresh":
                self._wallet_refresh()
                return
            if parsed.path == "/api/wallet-favorites":
                self._wallet_favorite()
                return
            if parsed.path == "/api/wallet-quarantine":
                self._wallet_quarantine()
                return
            if parsed.path == "/api/account-balance":
                self._account_balance()
                return
            if parsed.path == "/api/follow-strategy":
                self._follow_strategy()
                return
            if parsed.path == "/api/follow-strategies":
                self._follow_strategy_create()
                return
            action = re.match(r"^/api/follow-strategies/([^/]+)/(activate|update|delete)$", parsed.path)
            if action:
                self._follow_strategy_action(urllib.parse.unquote(action.group(1)), action.group(2))
                return
            if parsed.path == "/api/runner/start":
                self._runner_start()
                return
            if parsed.path == "/api/runner/stop":
                self._runner_stop()
                return
            if parsed.path == "/api/reset-data":
                self._reset_data()
                return
            if parsed.path == "/api/ai-risk/credential":
                self._ai_credential_save()
                return
            if parsed.path == "/api/ai-risk/credential/test":
                self._ai_credential_test()
                return
            if parsed.path == "/api/ai-risk/credential/delete":
                self._ai_credential_delete()
                return
            if parsed.path == "/api/ai-risk/settings":
                self._ai_settings()
                return
        self._error("not_found", status=HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, parsed: urllib.parse.ParseResult) -> None:
        follow_dir = _follow_dir(self.dashboard_config)
        if parsed.path == "/api/stream":
            self._serve_stream()
            return
        if parsed.path == "/api/health":
            self._ok(
                build_health(
                    self.dashboard_config.data_dir,
                    started_at=self.started_at,
                    log_dir=self.dashboard_config.log_dir,
                    follow_dir=follow_dir,
                )
            )
            return
        if parsed.path == "/api/overview":
            self._ok(build_overview(self.dashboard_config.data_dir, follow_dir=follow_dir))
            return
        if parsed.path == "/api/ai-risk":
            config_store = AiConfigStore(self.dashboard_config.data_dir)
            follow_store = FollowStore(_follow_db_path(self.dashboard_config))
            audit = follow_store.load_ai_audit(limit=20)
            self._ok({
                **config_store.status(),
                "summary": ai_audit_summary(follow_store),
                "recent_assessments": audit.get("assessments") or [],
                "recent_intents": audit.get("intents") or [],
            })
            return
        if parsed.path == "/api/ai-risk/wrap-key":
            self._ok(AiConfigStore(self.dashboard_config.data_dir).public_wrap_key())
            return
        if parsed.path == "/api/follow-strategy":
            store = FollowStore(_follow_db_path(self.dashboard_config))
            strategy = store.load_follow_strategy_readonly()
            if not strategy.get("configured"):
                balance = store.load_account_balance_readonly()
                balance_value = to_float(balance.get("balance_usdc")) if balance.get("configured") else None
                strategy = default_follow_strategy(balance_usdc=balance_value)
                strategy["configured"] = False
            self._ok(strategy)
            return
        if parsed.path == "/api/follow-strategies":
            store = FollowStore(_follow_db_path(self.dashboard_config))
            self._ok(store.list_follow_strategies_readonly())
            return
        if parsed.path == "/api/wallets":
            self._ok(build_wallets(self.dashboard_config.data_dir, follow_dir=follow_dir))
            return
        if parsed.path == "/api/wallet-follows":
            query = urllib.parse.parse_qs(parsed.query)
            wallet = str(query.get("wallet", [""])[0] or "").lower()
            status = str(query.get("status", [""])[0] or "").lower()
            page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
            size = _int_param(query.get("size", ["20"])[0], default=20, minimum=1, maximum=200)
            self._ok(
                build_wallet_follow_detail(
                    self.dashboard_config.data_dir,
                    wallet,
                    status=status,
                    page=page,
                    size=size,
                    follow_dir=follow_dir,
                )
            )
            return
        match = re.match(r"^/api/wallets/([^/]+)/follows$", parsed.path)
        if match:
            wallet = urllib.parse.unquote(match.group(1)).lower()
            query = urllib.parse.parse_qs(parsed.query)
            status = str(query.get("status", [""])[0] or "").lower()
            page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
            size = _int_param(query.get("size", ["20"])[0], default=20, minimum=1, maximum=200)
            self._ok(
                build_wallet_follow_detail(
                    self.dashboard_config.data_dir,
                    wallet,
                    status=status,
                    page=page,
                    size=size,
                    follow_dir=follow_dir,
                )
            )
            return
        if parsed.path == "/api/follows":
            query = urllib.parse.parse_qs(parsed.query)
            page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
            size = _int_param(query.get("size", ["25"])[0], default=25, minimum=1, maximum=200)
            status = str(query.get("status", [""])[0] or "").lower()
            category = normalize_category(query.get("category", [""])[0])
            self._ok(
                build_follows(
                    self.dashboard_config.data_dir,
                    page=page,
                    size=size,
                    status=status,
                    category=category,
                    follow_dir=follow_dir,
                    client=self.dashboard_config.client,
                )
            )
            return
        if parsed.path.startswith("/api/follows/"):
            condition_id = urllib.parse.unquote(parsed.path.rsplit("/", 1)[-1]).lower()
            self._ok(build_follow_detail(self.dashboard_config.data_dir, condition_id, follow_dir=follow_dir, client=self.dashboard_config.client))
            return
        if parsed.path == "/api/events":
            self._ok(
                build_events(
                    self.dashboard_config.data_dir,
                    observe_window_hours=self.dashboard_config.observe_window_hours,
                    follow_dir=follow_dir,
                )
            )
            return
        if parsed.path == "/api/wallet-refresh":
            self._ok(build_wallet_refresh_status(self.dashboard_config.data_dir, follow_dir=follow_dir))
            return
        if parsed.path == "/api/runner":
            self._ok(build_runner_status(self.dashboard_config))
            return
        match = re.match(r"^/api/markets/([^/]+)/prices$", parsed.path)
        if match:
            condition_id = urllib.parse.unquote(match.group(1)).lower()
            try:
                self._ok(fetch_market_prices(self.dashboard_config.data_dir, self.dashboard_config.client, condition_id, follow_dir=follow_dir))
            except Exception as exc:
                self._error(str(exc) or "price_refresh_failed", status=HTTPStatus.BAD_GATEWAY)
            return
        match = re.match(r"^/api/wallets/([^/]+)/trades$", parsed.path)
        if match:
            self._wallet_trades(match.group(1), urllib.parse.parse_qs(parsed.query))
            return
        self._error("not_found", status=HTTPStatus.NOT_FOUND)

    def _wallet_refresh(self) -> None:
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        category = normalize_category(query.get("category", [""])[0])
        if not category:
            self._error("invalid_category", status=HTTPStatus.BAD_REQUEST)
            return
        form = self._read_request_form() if category == "esports" else {}
        extra_args = v2_refresh_extra_args(form) if category == "esports" else None
        full_recollect = str(form.get("full_recollect")).lower() in ("true", "1", "yes", "on")
        try:
            max_profile_wallets = int(form.get("max_profile_wallets") or V2_DEFAULT_MAX_PROFILE_WALLETS)
        except (TypeError, ValueError):
            max_profile_wallets = V2_DEFAULT_MAX_PROFILE_WALLETS
        try:
            status = start_wallet_refresh(
                self.dashboard_config.data_dir,
                category=category,
                follow_dir=_follow_dir(self.dashboard_config),
                runner=self.dashboard_config.wallet_refresh_runner,
                timeout_seconds=self.dashboard_config.wallet_refresh_timeout_seconds,
                extra_args=extra_args,
                full_recollect=full_recollect,
                max_profile_wallets=max_profile_wallets,
            )
        except WalletRefreshAlreadyRunning as exc:
            self._json({"ok": False, "error": "wallet_refresh_running", "data": exc.status}, status=HTTPStatus.CONFLICT)
            return
        self._json({"ok": True, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED)

    def _wallet_favorite(self) -> None:
        form = self._read_request_form()
        wallet = str(form.get("wallet") or "").lower()
        category = normalize_category(str(form.get("category") or "esports"))
        favorite = _request_bool(form.get("favorite"), default=False)
        if not ADDRESS_RE.match(wallet):
            self._error("invalid_wallet", status=HTTPStatus.BAD_REQUEST)
            return
        if not category:
            self._error("invalid_category", status=HTTPStatus.BAD_REQUEST)
            return
        follow_dir = _follow_dir(self.dashboard_config)
        store = FollowStore(_follow_db_path(self.dashboard_config))
        snapshot = None
        if favorite:
            quarantine = store.load_wallet_quarantine(category=category)
            if wallet in quarantine or f"{category}:{wallet}" in quarantine:
                self._error("wallet_quarantined", status=HTTPStatus.CONFLICT)
                return
            wallet_payload = build_wallets(self.dashboard_config.data_dir, follow_dir=follow_dir)
            snapshot = next(
                (
                    compact_wallet_favorite_snapshot(row)
                    for row in wallet_payload.get("wallets", [])
                    if str(row.get("wallet") or "").lower() == wallet
                    and normalize_category(str(row.get("category") or "esports")) == category
                ),
                None,
            )
            if snapshot is None:
                self._error("wallet_not_found", status=HTTPStatus.NOT_FOUND)
                return
        ts = int(time.time())
        store.upsert_wallet_favorite(wallet, category=category, favorite=favorite, ts=ts, snapshot=snapshot)
        self._ok(
            {
                "wallet": wallet,
                "category": category,
                "favorite": favorite,
                "favorited_at": ts if favorite else None,
            }
        )

    def _wallet_quarantine(self) -> None:
        form = self._read_request_form()
        wallet = str(form.get("wallet") or "").lower()
        category = normalize_category(str(form.get("category") or "esports"))
        quarantined = _request_bool(form.get("quarantined"), default=True)
        if not ADDRESS_RE.match(wallet):
            self._error("invalid_wallet", status=HTTPStatus.BAD_REQUEST)
            return
        if not category:
            self._error("invalid_category", status=HTTPStatus.BAD_REQUEST)
            return
        ts = int(time.time())
        store = FollowStore(_follow_db_path(self.dashboard_config))
        if quarantined is False:
            store.clear_wallet_quarantine_wallets({f"{category}:{wallet}"})
            self._ok(
                {
                    "wallet": wallet,
                    "category": category,
                    "quarantined": False,
                    "unquarantined_at": ts,
                }
            )
            return
        reason = "manual_dashboard_quarantine"
        store.upsert_wallet_quarantine(
            wallet,
            reason=reason,
            ts=ts,
            category=category,
            details={"source": "dashboard"},
        )
        store.upsert_wallet_favorite(wallet, category=category, favorite=False, ts=ts)
        self._ok(
            {
                "wallet": wallet,
                "category": category,
                "quarantined": True,
                "reason": reason,
                "quarantined_at": ts,
            }
        )

    def _runner_start(self) -> None:
        current = build_runner_status(self.dashboard_config)
        if current.get("status") == "running":
            self._json({"ok": False, "error": "runner_already_running", "data": current}, status=HTTPStatus.CONFLICT)
            return
        try:
            status = start_runner(self.dashboard_config)
        except RunnerAlreadyRunning as exc:
            self._json({"ok": False, "error": "runner_already_running", "data": exc.status}, status=HTTPStatus.CONFLICT)
            return
        except ValueError as exc:
            self._error(str(exc), status=HTTPStatus.BAD_REQUEST)
            return
        self._json({"ok": True, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED)

    def _account_balance(self) -> None:
        current = build_runner_status(self.dashboard_config)
        if current.get("status") in {"running", "stopping"}:
            self._json({"ok": False, "error": "account_balance_locked", "data": current}, status=HTTPStatus.CONFLICT)
            return
        form = self._read_request_form()
        balance = to_float(form.get("balance_usdc") or form.get("balance"))
        if not math.isfinite(balance) or balance < 0:
            self._error("invalid_balance_usdc", status=HTTPStatus.BAD_REQUEST)
            return
        store = FollowStore(_follow_db_path(self.dashboard_config))
        state = store.set_account_balance(balance, ts=int(time.time()), source="manual")
        self._ok(state)

    def _follow_strategy(self) -> None:
        current = build_runner_status(self.dashboard_config)
        if current.get("status") in {"running", "stopping"}:
            self._json({"ok": False, "error": "follow_strategy_locked", "data": current}, status=HTTPStatus.CONFLICT)
            return
        strategy = self._read_request_form()
        valid, errors = validate_follow_strategy(strategy)
        if not valid:
            self._json(
                {"ok": False, "error": "invalid_follow_strategy", "errors": errors},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        store = FollowStore(_follow_db_path(self.dashboard_config))
        try:
            saved = store.save_follow_strategy(strategy, ts=int(time.time()))
        except ValueError as exc:
            self._json(
                {"ok": False, "error": "invalid_follow_strategy", "detail": str(exc)},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        self._ok(saved)

    def _follow_strategy_create(self) -> None:
        form = self._read_request_form()
        name = str(form.get("name") or "").strip()
        strategy = form.get("strategy") if isinstance(form.get("strategy"), dict) else form
        store = FollowStore(_follow_db_path(self.dashboard_config))
        # 库为空时,新建的第一条会自动激活并覆盖 active strategy。运行中禁止(与
        # activate/update/delete 同锁),否则能绕过 _follow_strategy_action 的锁在跑单中改策略。
        current = build_runner_status(self.dashboard_config)
        if current.get("status") in {"running", "stopping"} and not (
            store.list_follow_strategies().get("strategies") or []
        ):
            self._json(
                {"ok": False, "error": "follow_strategy_locked", "data": current},
                status=HTTPStatus.CONFLICT,
            )
            return
        try:
            entry = store.create_follow_strategy(name, strategy, ts=int(time.time()))
        except ValueError as exc:
            self._strategy_value_error(exc)
            return
        self._ok(entry)

    def _follow_strategy_action(self, slug: str, action: str) -> None:
        store = FollowStore(_follow_db_path(self.dashboard_config))
        # mutations that change the *active* strategy are locked while the runner is busy
        locks_active = action in {"activate", "delete", "update"}
        if locks_active:
            current = build_runner_status(self.dashboard_config)
            if current.get("status") in {"running", "stopping"}:
                listing = store.list_follow_strategies()
                touches_active = action in {"activate"} or slug == listing.get("active_slug")
                if touches_active:
                    self._json(
                        {"ok": False, "error": "follow_strategy_locked", "data": current},
                        status=HTTPStatus.CONFLICT,
                    )
                    return
        try:
            if action == "activate":
                result = store.activate_follow_strategy(slug, ts=int(time.time()))
            elif action == "delete":
                result = store.delete_follow_strategy_entry(slug, ts=int(time.time()))
            else:  # update
                form = self._read_request_form()
                name = str(form.get("name") or "").strip()
                strategy = form.get("strategy") if isinstance(form.get("strategy"), dict) else form
                result = store.update_follow_strategy_entry(slug, name, strategy, ts=int(time.time()))
        except ValueError as exc:
            self._strategy_value_error(exc)
            return
        self._ok(result)

    def _strategy_value_error(self, exc: ValueError) -> None:
        msg = str(exc)
        if msg == "strategy_not_found":
            self._error("strategy_not_found", status=HTTPStatus.NOT_FOUND)
        elif msg == "duplicate_name":
            self._json({"ok": False, "error": "duplicate_name"}, status=HTTPStatus.CONFLICT)
        elif msg == "name_required":
            self._error("name_required", status=HTTPStatus.BAD_REQUEST)
        elif msg.startswith("invalid_follow_strategy"):
            _, _, detail = msg.partition(":")
            errors = [e for e in detail.split(",") if e]
            self._json(
                {"ok": False, "error": "invalid_follow_strategy", "errors": errors},
                status=HTTPStatus.BAD_REQUEST,
            )
        else:
            self._error("invalid_follow_strategy", status=HTTPStatus.BAD_REQUEST, detail=msg)

    def _runner_stop(self) -> None:
        status = stop_runner(self.dashboard_config)
        accepted = status.get("status") in {"stopping", "stopped"}
        self._json({"ok": accepted, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED if accepted else HTTPStatus.CONFLICT)

    def _reset_data(self) -> None:
        try:
            result = reset_dashboard_data(self.dashboard_config)
        except DataResetBlocked as exc:
            self._json({"ok": False, "error": exc.reason, "data": exc.status}, status=HTTPStatus.CONFLICT)
            return
        self._json({"ok": True, "data": result, "generated_at": int(time.time())}, status=HTTPStatus.OK)

    def _ai_credential_save(self) -> None:
        form = self._read_request_form()
        envelope = form.get("envelope") if isinstance(form.get("envelope"), dict) else form
        store = AiConfigStore(self.dashboard_config.data_dir)
        try:
            secret = store.decrypt_envelope(envelope)
            balance = DeepSeekClient(secret).balance()
            store.save_credential(envelope)
            safe_balance = store.save_balance(balance)
        except Exception as exc:
            self._error("deepseek_credential_invalid", status=HTTPStatus.BAD_GATEWAY, detail=str(exc)[:200])
            return
        self._ok({"configured": True, "status": "valid", "balance": safe_balance})

    def _ai_credential_test(self) -> None:
        store = AiConfigStore(self.dashboard_config.data_dir)
        try:
            secret = store.secret()
            if not secret:
                self._error("deepseek_not_configured", status=HTTPStatus.BAD_REQUEST)
                return
            balance = DeepSeekClient(secret).balance()
            store.mark_credential_valid()
            safe_balance = store.save_balance(balance)
        except Exception as exc:
            store.mark_credential_error(str(exc))
            store.save_balance(None, error=str(exc))
            self._error("deepseek_connection_failed", status=HTTPStatus.BAD_GATEWAY, detail=str(exc)[:200])
            return
        self._ok({"configured": True, "status": "valid", "balance": safe_balance})

    def _ai_credential_delete(self) -> None:
        store = AiConfigStore(self.dashboard_config.data_dir)
        store.save_settings(enabled=False)
        self._ok({"deleted": store.delete_credential(), "enabled": False})

    def _ai_settings(self) -> None:
        form = self._read_request_form()
        enabled = _request_bool(form.get("enabled"), default=False)
        store = AiConfigStore(self.dashboard_config.data_dir)
        if enabled and not store.credential_envelope():
            self._error("deepseek_not_configured", status=HTTPStatus.CONFLICT)
            return
        self._ok(store.save_settings(enabled=enabled))

    def _wallet_trades(self, raw_addr: str, query: dict[str, list[str]]) -> None:
        wallet = urllib.parse.unquote(raw_addr).lower()
        if not ADDRESS_RE.match(wallet):
            self._error("invalid_wallet", status=HTTPStatus.BAD_REQUEST)
            return
        page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
        size = _int_param(query.get("size", ["10"])[0], default=10, minimum=1, maximum=50)
        client = self.dashboard_config.client
        if client is None:
            self._error("client_unavailable", status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        cache_key = wallet
        now = time.time()
        with _TRADES_CACHE_LOCK:
            cached = _TRADES_CACHE.get(cache_key)
        if cached and now - cached[0] < self.dashboard_config.trades_cache_ttl_seconds:
            trades = cached[1]
        else:
            try:
                trades = client.trades_for_user(wallet, limit=50)
            except Exception as exc:
                self._error("data_api_error", status=HTTPStatus.BAD_GATEWAY, detail=str(exc))
                return
            with _TRADES_CACHE_LOCK:
                _TRADES_CACHE[cache_key] = (now, trades)
        watched = build_events(
            self.dashboard_config.data_dir,
            observe_window_hours=self.dashboard_config.observe_window_hours,
            follow_dir=_follow_dir(self.dashboard_config),
        )
        watched_ids = {str(row.get("condition_id") or "").lower() for row in watched.get("events", [])}
        open_snapshot = FollowStore(_follow_db_path(self.dashboard_config)).load_dashboard_open_signals()
        followed = {
            (str(signal.get("wallet") or "").lower(), str(signal.get("condition_id") or "").lower())
            for signal in open_snapshot.get("open_signals", [])
        }
        offset = (page - 1) * size
        rows = []
        for trade in trades[offset : offset + size]:
            condition_id = _trade_condition_id(trade)
            rows.append(
                {
                    **trade,
                    "condition_id": condition_id,
                    "watched": condition_id in watched_ids,
                    "followed": (wallet, condition_id) in followed,
                }
            )
        self._ok(
            {
                "wallet": wallet,
                "short_addr": short_addr(wallet),
                "polymarket_profile_url": f"https://polymarket.com/@{wallet}?tab=activity",
                "page": page,
                "size": size,
                "total_cached": len(trades),
                "trades": rows,
            }
        )

    def _login(self) -> None:
        form = self._read_request_form()
        username = str(form.get("username") or "")
        password = str(form.get("password") or "")
        client = self.client_address[0] if self.client_address else "unknown"
        failures = [stamp for stamp in FAILED_LOGINS.get(client, []) if time.time() - stamp < 60.0]
        if len(failures) >= 5:
            time.sleep(min(2.0, 0.25 * len(failures)))
        config = self.dashboard_config
        if not (
            hmac.compare_digest(username, config.username)
            and hmac.compare_digest(password, config.password)
        ):
            failures.append(time.time())
            FAILED_LOGINS[client] = failures[-10:]
            self._error("invalid_login", status=HTTPStatus.UNAUTHORIZED)
            return
        FAILED_LOGINS.pop(client, None)
        token = make_session_token(username, config.cookie_secret)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Set-Cookie", self._session_cookie(token))
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def _logout(self) -> None:
        cookie = f"{COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
        if self.dashboard_config.cookie_secure:
            cookie += "; Secure"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Set-Cookie", cookie)
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def _authenticated(self) -> bool:
        token = _cookie_value(self.headers.get("Cookie", ""), COOKIE_NAME)
        username = verify_session_token(
            token,
            self.dashboard_config.cookie_secret,
            max_age_seconds=self.dashboard_config.session_ttl_seconds,
        )
        return username == self.dashboard_config.username

    def _session_cookie(self, token: str) -> str:
        cookie = (
            f"{COOKIE_NAME}={token}; Path=/; Max-Age={self.dashboard_config.session_ttl_seconds}; "
            "HttpOnly; SameSite=Lax"
        )
        if self.dashboard_config.cookie_secure:
            cookie += "; Secure"
        return cookie

    def _maybe_renew_session_cookie(self) -> None:
        """滑动续期:本次请求带着有效 session → 顺手重发 cookie 刷新有效期(issued_at 归零)。
        在 _json(所有 API 响应出口)调用,故每次读/写都续期。只要 dashboard 标签页开着(会定时
        轮询 API),会话就一直续命、不会过夜被踢;只有真正闲置超过 TTL 才需重登。"""
        config = self.dashboard_config
        if not config.cookie_secret:
            return
        token = _cookie_value(self.headers.get("Cookie", ""), COOKIE_NAME)
        if not token:
            return
        if verify_session_token(token, config.cookie_secret, max_age_seconds=config.session_ttl_seconds) != config.username:
            return
        self.send_header("Set-Cookie", self._session_cookie(make_session_token(config.username, config.cookie_secret)))

    def _serve_static(self, path: str) -> None:
        static_dir = self.dashboard_config.static_dir or Path(__file__).with_name("dashboardV2")
        if path in {"", "/"} and not (static_dir / "index.html").exists():
            self._send_bytes(b"Poly Fight dashboard API", content_type="text/plain; charset=utf-8")
            return
        name = "index.html" if path in {"", "/"} else path.lstrip("/")
        target = (static_dir / name).resolve()
        try:
            target.relative_to(static_dir.resolve())
        except ValueError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_bytes(target.read_bytes(), content_type=mimetypes.guess_type(str(target))[0] or "application/octet-stream")

    def _send_bytes(self, payload: bytes, *, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        # 静态资源(app.jsx / adapt.js / css)从磁盘按请求实时读取,服务端永远是最新的。
        # 但无缓存头时浏览器会启发式缓存,普通刷新复用旧 JS → "UI 改了代码没生效 / 保存无效"
        # 这类幽灵 bug。no-store 强制每次重新拉取,代码改动普通刷新即生效。
        self.send_header("Cache-Control", "no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(payload)

    def _serve_stream(self) -> None:
        config = self.dashboard_config
        lock = getattr(self.server, "stream_clients_lock")
        with lock:
            active = int(getattr(self.server, "active_stream_clients", 0))
            if active >= config.max_stream_clients:
                self._error("too_many_stream_clients", status=HTTPStatus.SERVICE_UNAVAILABLE)
                return
            setattr(self.server, "active_stream_clients", active + 1)
        conn: sqlite3.Connection | None = None
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            store = FollowStore(_follow_db_path(config))
            previous: StreamSignal | None = None
            last_heartbeat = 0.0
            while True:
                if conn is None:
                    conn = store.connect_readonly()
                signal = read_stream_signal(config.data_dir, log_dir=config.log_dir, follow_dir=_follow_dir(config), store=store, conn=conn)
                if conn is not None and signal.snapshot_updated_at == 0 and not _follow_db_path(config).exists():
                    conn.close()
                    conn = None
                now = time.time()
                dirty = stream_dirty_flags(previous, signal)
                if previous is None or signal != previous:
                    payload = {
                        **build_stream_header(config, started_at=self.started_at),
                        **dirty,
                    }
                    self._write_sse_data(payload)
                    previous = signal
                    last_heartbeat = now
                elif now - last_heartbeat >= config.stream_heartbeat_seconds:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    last_heartbeat = now
                time.sleep(max(0.25, float(config.stream_poll_seconds)))
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            return
        finally:
            if conn is not None:
                conn.close()
            with lock:
                current = int(getattr(self.server, "active_stream_clients", 0))
                setattr(self.server, "active_stream_clients", max(0, current - 1))

    def _write_sse_data(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.wfile.write(b"data: " + body + b"\n\n")
        self.wfile.flush()

    def _read_request_form(self) -> dict[str, Any]:
        body = self.rfile.read(_int_param(self.headers.get("Content-Length"), default=0, minimum=0, maximum=16384))
        content_type = self.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                form = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                form = {}
            return form if isinstance(form, dict) else {}
        return {key: values[0] for key, values in urllib.parse.parse_qs(body.decode()).items()}

    def _ok(self, data: Any) -> None:
        self._json({"ok": True, "data": data, "generated_at": int(time.time())})

    def _error(self, error: str, *, status: HTTPStatus, detail: str | None = None) -> None:
        payload = {"ok": False, "error": error}
        if detail:
            payload["detail"] = detail
        self._json(payload, status=status)

    def _json(self, payload: Any, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._maybe_renew_session_cookie()  # 滑动续期:已登录的每次 API 响应都刷新 cookie 有效期
        self.end_headers()
        self.wfile.write(body)


def _latest_collection_summary(data_dir: Path) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for category, category_dir in category_data_dirs(data_dir).items():
        row = LeaderboardStore(_leaderboard_db_file(category_dir)).load_latest_collection_run(category=category)
        if row:
            candidates.append(row)
    legacy_row = LeaderboardStore(_leaderboard_db_file(data_dir)).load_latest_collection_run(category="esports")
    if legacy_row:
        candidates.append(legacy_row)
    if candidates:
        return max(
            candidates,
            key=lambda row: _parse_timestamp(row.get("published_at") or row.get("updated_at") or row.get("created_at")),
        )
    return {}


def build_health(data_dir: Path, *, started_at: float, log_dir: Path | None = None, follow_dir: Path | None = None) -> dict[str, Any]:
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    rows = store.load_run_ticks(limit=100)
    db_ready = store.dashboard_db_ready()
    last_tick = rows[-1] if rows else {}
    build_summary = _latest_collection_summary(data_dir)
    leaderboard_updated_at = max(_category_leaderboard_mtimes(data_dir).values() or [0])
    last_tick_at = int(last_tick.get("created_at") or 0)
    interval = int(last_tick.get("desired_next_interval_seconds") or 900)
    now_ts = int(time.time())
    healthy = bool(last_tick_at and now_ts - last_tick_at <= max(1, 3 * interval))
    errors = [row for row in rows[-50:] if row.get("status") == "run_iteration_error" or row.get("error")]
    status = "healthy" if healthy else "stale"
    if not db_ready:
        status = "waiting_for_runner"
    return {
        "db_ready": db_ready,
        "status": status,
        "healthy": healthy,
        "last_tick_at": last_tick_at,
        "desired_next_interval_seconds": interval,
        "gate_open": bool(last_tick.get("gate_open")),
        "watched_market_count": int(last_tick.get("watched_market_count") or 0),
        "open_signal_count": int(last_tick.get("open_signal_count") or 0),
        "leaderboard_updated_at": leaderboard_updated_at,
        "build_summary": build_summary if isinstance(build_summary, dict) else {},
        "scoring_version": _latest_scoring_version(data_dir),
        "recent_error_count": len(errors),
        "last_error": errors[-1] if errors else None,
        "last_tick": last_tick,
        "by_category": last_tick.get("by_category") if isinstance(last_tick.get("by_category"), dict) else {},
        "uptime_seconds": int(time.time() - started_at),
        # 链上检测健康(供概览健康面板):来源 onchain/data_api、WS 是否健康、订阅钱包数。
        "detection_source": str(last_tick.get("detection_source") or ""),
        "onchain_configured": bool(last_tick.get("onchain_configured")),
        "onchain_healthy": bool(last_tick.get("onchain_healthy")),
        "follow_wallet_count": int(last_tick.get("eligible_follow_wallet_count") or 0),
    }


@dataclass(frozen=True)
class StreamSignal:
    snapshot_updated_at: int
    run_log_mtime: int
    control_mtime: int
    leaderboard_mtime: int


def read_stream_signal(
    data_dir: Path,
    *,
    log_dir: Path | None = None,
    follow_dir: Path | None = None,
    store: FollowStore | None = None,
    conn: sqlite3.Connection | None = None,
) -> StreamSignal:
    follow_root = _follow_dir_from(data_dir, follow_dir=follow_dir)
    store = store or FollowStore(_follow_db_file(data_dir, follow_dir=follow_root))
    run_ticks_updated_at = store.read_meta_int("run_ticks_updated_at", conn=conn)
    return StreamSignal(
        snapshot_updated_at=store.read_meta_int("follow_snapshot_updated_at", conn=conn),
        run_log_mtime=run_ticks_updated_at,
        control_mtime=_file_mtime(follow_root / "follow_control.json"),
        leaderboard_mtime=max(_category_leaderboard_mtimes(data_dir).values() or [0]),
    )


def stream_dirty_flags(previous: StreamSignal | None, current: StreamSignal) -> dict[str, bool]:
    if previous is None:
        return {"follows_dirty": True, "events_dirty": True, "wallets_dirty": True}
    snapshot_dirty = current.snapshot_updated_at != previous.snapshot_updated_at
    return {
        "follows_dirty": snapshot_dirty,
        "events_dirty": snapshot_dirty,
        "wallets_dirty": snapshot_dirty or current.leaderboard_mtime != previous.leaderboard_mtime,
    }


def build_stream_header(config: DashboardConfig, *, started_at: float) -> dict[str, Any]:
    control = read_follow_control(_follow_dir(config))
    return {
        "health": build_health(config.data_dir, started_at=started_at, log_dir=config.log_dir, follow_dir=_follow_dir(config)),
        "overview": build_overview(config.data_dir, follow_dir=_follow_dir(config)),
        "runner": build_runner_status(config),
        "refresh": build_wallet_refresh_status(config.data_dir, follow_dir=_follow_dir(config)).get("status") or {"status": "idle"},
        "pause_follow": control.get("pause_follow") if isinstance(control, dict) else None,
        "live": {
            "status": "connected",
            "generated_at": int(time.time()),
        },
    }


def build_overview(data_dir: Path, *, follow_dir: Path | None = None) -> dict[str, Any]:
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    snapshot = store.load_dashboard_snapshot()
    account_balance = store.load_account_balance_readonly()
    open_signals = [signal for signal in snapshot.get("open_signals", []) if _signal_has_actual_follow(signal)]
    results = [signal for signal in snapshot.get("results", []) if _signal_has_actual_follow(signal)]
    all_signals = [*open_signals, *results]
    settled = [row for row in results if row.get("status") == "settled"]
    exited = [row for row in results if row.get("status") == "exited"]
    # 跟单胜负 = 每笔跟单(含提前卖出 exited + 自然结算 settled)整体 PnL 符号:
    # >0 胜 / <0 负 / =0 中性不计。与市场结算到哪边无关——提前卖出盈利同样算胜。
    decided = [row for row in (*settled, *exited) if _signal_our_pnl(row) != 0]
    wins = [row for row in decided if _signal_our_pnl(row) > 0]
    legs = [leg for signal in all_signals for leg in signal.get("legs") or []]
    result_legs = [leg for signal in results for leg in signal.get("legs") or []]
    total_stake = sum(_leg_actual_stake(leg) for leg in legs)
    resolved_stake = sum(_leg_actual_stake(leg) for leg in result_legs)
    would_follow = [leg for leg in legs if leg.get("would_follow", True)]
    quality = _signal_quality_summary(all_signals)
    clv_values = [_to_float(signal.get("wallet_clv")) for signal in all_signals if signal.get("wallet_clv") is not None]
    our_pnl = sum(_signal_our_pnl(row) for row in results)
    hypothetical_pnl = sum(_signal_hypothetical_pnl(row) for row in results)
    wallet_basis_pnl = sum(_signal_wallet_pnl(row) for row in results)
    behavior = _behavior_counts(all_signals)
    tracking_started_at = _tracking_started_at(all_signals)
    now_ts = int(time.time())
    open_exposure = sum(sum(_leg_actual_stake(leg) for leg in signal.get("legs") or []) for signal in open_signals)
    account_balance_usdc = _to_float(account_balance.get("balance_usdc")) if account_balance.get("configured") else float("nan")
    account_total_equity_usdc = account_balance_usdc + open_exposure if math.isfinite(account_balance_usdc) else None
    overview = {
        "db_ready": bool(snapshot.get("db_ready")),
        "open_signal_count": len(open_signals),
        "result_count": len(results),
        "settled_count": len(settled),
        "exited_count": len(exited),
        "win_rate": (len(wins) / len(decided)) if decided else None,
        "our_realized_pnl": our_pnl,
        "hypothetical_pnl": hypothetical_pnl,
        "wallet_basis_realized_pnl": wallet_basis_pnl,
        "total_stake": total_stake,
        "resolved_stake": resolved_stake,
        "realized_roi": (our_pnl / resolved_stake) if resolved_stake else None,
        "wallet_basis_realized_roi": (wallet_basis_pnl / resolved_stake) if resolved_stake else None,
        "delay_cost": wallet_basis_pnl - our_pnl,
        "would_follow_capture_rate": (len(would_follow) / len(legs)) if legs else None,
        **quality,
        "avg_wallet_clv": (sum(clv_values) / len(clv_values)) if clv_values else None,
        "open_exposure": open_exposure,
        "account_total_equity_usdc": account_total_equity_usdc,
        "account_balance": account_balance,
        "behavior_counts": behavior,
        "performance": snapshot.get("performance") or {},
        "tracking_started_at": tracking_started_at or None,
        "tracking_duration_seconds": max(0, now_ts - tracking_started_at) if tracking_started_at else None,
    }
    overview["by_category"] = {
        category: _overview_for_signals(
            [signal for signal in open_signals if _signal_category(signal) == category],
            [signal for signal in results if _signal_category(signal) == category],
        )
        for category in CATEGORIES
    }
    overview.update(_overview_full_app_aggregates(open_signals, results))
    # A normal overview read must not create or mutate the credential store.
    ai_config = AiConfigStore.read_existing_status(Path(data_dir))
    overview["ai_risk"] = {
        **ai_audit_summary(store),
        "enabled": bool((ai_config.get("settings") or {}).get("enabled")),
        "credential_configured": bool((ai_config.get("credential") or {}).get("configured")),
        "credential_status": str((ai_config.get("credential") or {}).get("status") or "not_configured"),
    }
    return overview


def _overview_full_app_aggregates(open_signals: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    esports_open = [signal for signal in open_signals if _overview_signal_game(signal)]
    esports_results = [signal for signal in results if _overview_signal_game(signal)]
    return {
        "equity_points": _overview_equity_points(esports_results),
        "win_rates_by_game": _overview_win_rates_by_game(esports_results),
        "follow_type_distribution": _overview_follow_type_distribution(esports_results),
        "open_by_game": _overview_open_by_game(esports_open),
    }


def _overview_equity_points(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cumulative = 0.0
    points: list[dict[str, Any]] = []
    ordered = sorted(
        enumerate(results),
        key=lambda item: (_signal_activity_at(item[1]) or 0, item[0]),
    )
    for _index, signal in ordered:
        pnl = _round_dashboard_float(_signal_our_pnl(signal))
        cumulative = _round_dashboard_float(cumulative + pnl)
        points.append(
            {
                "timestamp": _signal_activity_at(signal) or None,
                "pnl": pnl,
                "cumulative_pnl": cumulative,
            }
        )
    return points


def _overview_win_rates_by_game(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for signal in results:
        # 含提前卖出(exited)与自然结算(settled);胜负只看每笔跟单整体 PnL 符号
        # (>0 胜 / <0 负 / =0 中性不计),与市场结算到哪边无关。
        if str(signal.get("status") or "") not in ("settled", "exited"):
            continue
        pnl = _signal_our_pnl(signal)
        if pnl == 0:
            continue
        game = _overview_signal_game(signal)
        if not game:
            continue
        row = grouped.setdefault(
            game,
            {
                "game": game,
                "game_label": GAME_FAMILY_LABELS.get(game, game.upper()),
                "wins": 0,
                "losses": 0,
                "settled_count": 0,  # = wins+losses(纳入胜负的笔数,含 exited)
                "win_rate": None,
            },
        )
        if pnl > 0:
            row["wins"] += 1
        else:
            row["losses"] += 1
        row["settled_count"] = row["wins"] + row["losses"]
    for row in grouped.values():
        total = int(row["settled_count"] or 0)
        row["win_rate"] = (row["wins"] / total) if total else None
    return sorted(grouped.values(), key=lambda row: _overview_game_sort_key(str(row.get("game") or "")))


def _overview_follow_type_distribution(signals: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    by_game: dict[str, dict[str, Any]] = {}
    for signal in signals:
        game = _overview_signal_game(signal)
        if not game:
            continue
        follow_type, follow_label = _overview_signal_follow_type(signal)
        stake = _round_dashboard_float(sum(_leg_actual_stake(leg) for leg in signal.get("legs") or []))
        row = grouped.setdefault(
            follow_type,
            {
                "type": follow_type,
                "label": follow_label,
                "count": 0,
                "stake": 0.0,
            },
        )
        row["count"] += 1
        row["stake"] = _round_dashboard_float(row["stake"] + stake)
        game_row = by_game.setdefault(
            game,
            {
                "game": game,
                "game_label": GAME_FAMILY_LABELS.get(game, game.upper()),
                "types": {},
                "total": 0,
                "total_stake": 0.0,
            },
        )
        type_row = game_row["types"].setdefault(
            follow_type,
            {
                "type": follow_type,
                "label": follow_label,
                "count": 0,
                "stake": 0.0,
            },
        )
        type_row["count"] += 1
        type_row["stake"] = _round_dashboard_float(type_row["stake"] + stake)
        game_row["total"] += 1
        game_row["total_stake"] = _round_dashboard_float(_to_float(game_row["total_stake"]) + stake)
    segments = sorted(
        grouped.values(),
        key=lambda row: (0 if str(row.get("type") or "") == "main_match" else 1, str(row.get("type") or "")),
    )
    game_rows = []
    for game_row in by_game.values():
        types = [
            game_row["types"].get(
                follow_type,
                {"type": follow_type, "label": follow_label, "count": 0, "stake": 0.0},
            )
            for follow_type, follow_label in (("main_match", "主盘"), ("sub_game", "Sub Game"))
        ]
        game_rows.append(
            {
                "game": game_row["game"],
                "game_label": game_row["game_label"],
                "total": game_row["total"],
                "total_stake": _round_dashboard_float(game_row["total_stake"]),
                "types": types,
            }
        )
    return {
        "total": sum(int(row.get("count") or 0) for row in segments),
        "total_stake": _round_dashboard_float(sum(_to_float(row.get("stake")) for row in segments)),
        "segments": segments,
        "by_game": sorted(game_rows, key=lambda row: _overview_game_sort_key(str(row.get("game") or ""))),
    }


def _overview_open_by_game(open_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for signal in open_signals:
        game = _overview_signal_game(signal)
        if not game:
            continue
        row = grouped.setdefault(
            game,
            {
                "game": game,
                "game_label": GAME_FAMILY_LABELS.get(game, game.upper()),
                "count": 0,
            },
        )
        row["count"] += 1
    return sorted(grouped.values(), key=lambda row: _overview_game_sort_key(str(row.get("game") or "")))


def _overview_signal_game(signal: dict[str, Any]) -> str:
    if _signal_category(signal) != "esports":
        return ""
    raw_values: list[Any] = [
        signal.get("game_family"),
        signal.get("best_game_family"),
        signal.get("game"),
        signal.get("league"),
    ]
    parts = _match_parts_for_row(signal)
    if parts:
        raw_values.append(parts.get("game"))
    for value in raw_values:
        game = _normalize_esports_game(value)
        if game:
            return game
    return ""


def _normalize_esports_game(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    compact = text.replace(" ", "")
    if compact in {"cs2", "counterstrike2", "counterstrike"}:
        return "cs2"
    if compact in {"dota2", "dota"}:
        return "dota2"
    if compact in {"lol", "leagueoflegends", "league"}:
        return "lol"
    if compact in {"valorant", "valo"}:
        return "valorant"
    return ""


def _overview_signal_market_type(signal: dict[str, Any]) -> str:
    market_type = str(signal.get("market_type") or "").strip()
    if market_type:
        return market_type
    label = str(signal.get("market_type_label") or "").strip()
    for key, value in MARKET_TYPE_LABELS.items():
        if label == value:
            return key
    return "main_match"


def _overview_signal_follow_type(signal: dict[str, Any]) -> tuple[str, str]:
    market_type = _overview_signal_market_type(signal)
    if market_type in {"game_winner", "map_winner"}:
        return "sub_game", "Sub Game"
    return "main_match", MARKET_TYPE_LABELS.get("main_match", "主盘")


def _overview_game_sort_key(game: str) -> tuple[int, str]:
    return (_ESPORTS_GAME_ORDER.get(game, 99), game)


def _round_dashboard_float(value: float) -> float:
    return round(float(value or 0.0), 10)


def _signal_category(signal: dict[str, Any]) -> str:
    return normalize_category(str(signal.get("category") or "")) or "esports"


def _signal_condition_id(signal: dict[str, Any]) -> str:
    return str(signal.get("condition_id") or "").lower()


def _signal_wallet(signal: dict[str, Any]) -> str:
    return str(signal.get("wallet") or "").lower()


def _signal_has_two_sided_behavior(signal: dict[str, Any]) -> bool:
    behavior = signal.get("wallet_behavior") if isinstance(signal.get("wallet_behavior"), dict) else {}
    if behavior.get("hedged"):
        return True
    for event in signal.get("behavior_events") or []:
        if isinstance(event, dict) and str(event.get("kind") or "") == "hedge":
            return True
    return False


def _signal_has_actual_follow(signal: dict[str, Any]) -> bool:
    return any(_leg_actual_stake(leg) > 0 for leg in (signal or {}).get("legs") or [])


def _signal_quality_flags(signals: list[dict[str, Any]]) -> dict[str, bool]:
    wallet_outcomes: dict[str, set[str]] = {}
    comparable: list[tuple[str, str]] = []
    two_sided = False
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        wallet = _signal_wallet(signal)
        side = _signal_side(signal)
        if _signal_has_two_sided_behavior(signal):
            two_sided = True
        if not wallet or not side:
            continue
        wallet_outcomes.setdefault(wallet, set()).add(side)
        comparable.append((wallet, side))
    if any(len(outcomes) > 1 for outcomes in wallet_outcomes.values()):
        two_sided = True
    disagreement = any(
        wallet_a != wallet_b and side_a != side_b
        for index, (wallet_a, side_a) in enumerate(comparable)
        for wallet_b, side_b in comparable[index + 1 :]
    )
    return {"two_sided": two_sided, "disagreement": disagreement}


def _signal_quality_label(flags: dict[str, bool]) -> str:
    two_sided = bool(flags.get("two_sided"))
    disagreement = bool(flags.get("disagreement"))
    if two_sided and disagreement:
        return "two_sided_disagreement"
    if two_sided:
        return "two_sided"
    if disagreement:
        return "disagreement"
    return "one_way"


def _signal_quality_summary(signals: list[dict[str, Any]]) -> dict[str, int]:
    groups: dict[str, list[dict[str, Any]]] = {}
    ungrouped: list[dict[str, Any]] = []
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        condition_id = _signal_condition_id(signal)
        if condition_id:
            groups.setdefault(condition_id, []).append(signal)
        else:
            ungrouped.append(signal)

    summary = {
        "clean_signal_count": 0,
        "two_sided_signal_count": 0,
        "disagreement_signal_count": 0,
        "contested_signal_count": 0,
        "clean_condition_count": 0,
        "two_sided_condition_count": 0,
        "disagreement_condition_count": 0,
        "contested_condition_count": 0,
        "mixed_quality_condition_count": 0,
        "legacy_contested_signal_count": sum(1 for signal in signals if isinstance(signal, dict) and signal.get("contested")),
    }

    for group in [*groups.values(), *([signal] for signal in ungrouped)]:
        flags = _signal_quality_flags(group)
        signal_count = len(group)
        if flags["two_sided"]:
            summary["two_sided_signal_count"] += signal_count
            summary["two_sided_condition_count"] += 1
        if flags["disagreement"]:
            summary["disagreement_signal_count"] += signal_count
            summary["disagreement_condition_count"] += 1
        if flags["two_sided"] and flags["disagreement"]:
            summary["mixed_quality_condition_count"] += 1
        if not flags["two_sided"] and not flags["disagreement"]:
            summary["clean_signal_count"] += signal_count
            summary["clean_condition_count"] += 1

    summary["contested_signal_count"] = summary["disagreement_signal_count"]
    summary["contested_condition_count"] = summary["disagreement_condition_count"]
    return summary


def _annotate_signal_quality(signals: list[dict[str, Any]]) -> None:
    by_condition: dict[str, list[dict[str, Any]]] = {}
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        by_condition.setdefault(_signal_condition_id(signal), []).append(signal)
    for group in by_condition.values():
        flags = _signal_quality_flags(group)
        label = _signal_quality_label(flags)
        for signal in group:
            signal["quality_two_sided"] = flags["two_sided"]
            signal["quality_disagreement"] = flags["disagreement"]
            signal["quality_label"] = label


def _overview_for_signals(open_signals: list[dict[str, Any]], results: list[dict[str, Any]]) -> dict[str, Any]:
    open_signals = [signal for signal in open_signals if _signal_has_actual_follow(signal)]
    results = [signal for signal in results if _signal_has_actual_follow(signal)]
    all_signals = [*open_signals, *results]
    settled = [row for row in results if row.get("status") == "settled"]
    exited = [row for row in results if row.get("status") == "exited"]
    # 跟单胜负 = 每笔跟单(含提前卖出 exited + 自然结算 settled)整体 PnL 符号:
    # >0 胜 / <0 负 / =0 中性不计。与市场结算到哪边无关——提前卖出盈利同样算胜。
    decided = [row for row in (*settled, *exited) if _signal_our_pnl(row) != 0]
    wins = [row for row in decided if _signal_our_pnl(row) > 0]
    legs = [leg for signal in all_signals for leg in signal.get("legs") or []]
    result_legs = [leg for signal in results for leg in signal.get("legs") or []]
    resolved_stake = sum(_to_float(leg.get("stake")) for leg in result_legs)
    our_pnl = sum(_signal_our_pnl(row) for row in results)
    wallet_basis_pnl = sum(_signal_wallet_pnl(row) for row in results)
    quality = _signal_quality_summary(all_signals)
    return {
        "open_signal_count": len(open_signals),
        "result_count": len(results),
        "settled_count": len(settled),
        "exited_count": len(exited),
        "win_rate": (len(wins) / len(decided)) if decided else None,
        "our_realized_pnl": our_pnl,
        "wallet_basis_realized_pnl": wallet_basis_pnl,
        "total_stake": sum(_to_float(leg.get("stake")) for leg in legs),
        "resolved_stake": resolved_stake,
        "realized_roi": (our_pnl / resolved_stake) if resolved_stake else None,
        **quality,
    }


def _eligible_display_metrics(row: dict[str, Any]) -> dict[str, Any]:
    """Stats to show for a leaderboard row.

    When a wallet qualifies via per-type buckets, return the best eligible bucket so
    the displayed win rate / ROI / edge align with the actual follow scope. Falls
    back to the overall row for legacy profiles that predate per-type grading.
    """
    enriched = enrich_esports_bucket_scores(row, now_ts=int(time.time()))
    best_bucket = str(enriched.get("best_bucket") or "")
    best_market_type = str(enriched.get("best_market_type") or "")
    bucket_scores = enriched.get("bucket_scores") if isinstance(enriched.get("bucket_scores"), dict) else {}
    if best_bucket and isinstance(bucket_scores.get(best_bucket), dict):
        return bucket_scores[best_bucket]
    if best_market_type and isinstance(bucket_scores.get(best_market_type), dict):
        return bucket_scores[best_market_type]
    eligible_buckets = enriched.get("eligible_buckets") or []
    per_game_type = row.get("per_game_type_grades") or row.get("per_game_type") or {}
    buckets = [per_game_type[key] for key in eligible_buckets if isinstance(per_game_type.get(key), dict)]
    if buckets:
        return max(buckets, key=lambda bucket: int(bucket.get("esports_closed_count") or 0))
    eligible = enriched.get("eligible_market_types") or []
    per_type = row.get("per_type_grades") or {}
    buckets = [per_type[market_type] for market_type in eligible if isinstance(per_type.get(market_type), dict)]
    if not buckets:
        return row
    return max(buckets, key=lambda bucket: int(bucket.get("esports_closed_count") or 0))


_PRICE_BAND_ORDER = ("<0.40", "0.40-0.55", "0.55-0.70", ">=0.70")
# 价档上界(与 core.entry_price_buckets 分档一致),用于标"可跟区"(上界 ≤ 跟单上限)。
_PRICE_BAND_UPPER = {"<0.40": 0.40, "0.40-0.55": 0.55, "0.55-0.70": 0.70, ">=0.70": 1.0}


def _resolve_entry_price_buckets(row: dict[str, Any]) -> dict[str, Any] | None:
    """取价档胜率的数据源 entry_price_buckets。

    必须从 **per_game_type_grades[最佳eligible桶]** 取(那里稳定带 entry_price_buckets);
    不能用 _eligible_display_metrics 的返回值——它对有 eligible 桶的钱包会走 bucket_scores
    精简分支、**不含 entry_price_buckets**,导致正经钱包反而没价档条(实测 bug)。
    回退:per_type_grades 最佳桶 → 钱包级顶层 entry_price_buckets。"""
    def _best(grades: dict[str, Any], keys) -> dict[str, Any] | None:
        cands = [grades[k] for k in keys
                 if isinstance(grades.get(k), dict) and isinstance(grades[k].get("entry_price_buckets"), dict)]
        if not cands:
            return None
        best = max(cands, key=lambda b: int(to_float(b.get("esports_closed_count"))))
        return best.get("entry_price_buckets")
    pgt = row.get("per_game_type_grades") or {}
    epb = _best(pgt, row.get("eligible_buckets") or list(pgt.keys()))
    if epb is None:
        ptg = row.get("per_type_grades") or {}
        epb = _best(ptg, list(ptg.keys()))
    if epb is None and isinstance(row.get("entry_price_buckets"), dict):
        epb = row["entry_price_buckets"]
    return epb


def _price_bands_for_display(row: dict[str, Any], *, follow_ceiling: float = 0.68) -> list[dict[str, Any]]:
    """展示用:把最佳 eligible 桶的 entry_price_buckets 拍平成有序价档胜率分布。

    纯展示——让用户一眼看出钱包在各价位的真实胜率(低价烂/中价金矿/高价薄),
    并标出"可跟区"(档上界 ≤ 跟单上限)。数据评分时已算好,这里不重算。"""
    epb = _resolve_entry_price_buckets(row)
    if not isinstance(epb, dict):
        return []
    out: list[dict[str, Any]] = []
    for name in _PRICE_BAND_ORDER:
        bucket = epb.get(name)
        if not isinstance(bucket, dict):
            continue
        n = int(to_float(bucket.get("market_count")))
        if n <= 0:
            continue
        out.append({
            "band": name,
            "win_rate": to_float(bucket.get("win_rate")),
            "n": n,
            "hold_pnl": round(to_float(bucket.get("hold_pnl")), 2),
            # 档完全在可跟上限内 → 可跟;0.55-0.70 这种跨上限的标 partial。
            "followable": "full" if _PRICE_BAND_UPPER.get(name, 1.0) <= follow_ceiling + 1e-9
            else "partial" if name == "0.55-0.70" and follow_ceiling > 0.55 else "none",
        })
    return out


def _market_type_labels(market_types: list[str]) -> list[str]:
    labels = {"main_match": "主盘", "game_winner": "单局", "map_winner": "地图"}
    return [labels.get(value, value) for value in market_types if value]


def _observed_market_types(row: dict[str, Any]) -> list[str]:
    observed = [str(value) for value in (row.get("observed_market_types") or []) if value]
    if observed:
        order = {"main_match": 0, "game_winner": 1, "map_winner": 2}
        return sorted(set(observed), key=lambda value: order.get(value, 99))
    per_type = row.get("per_type_grades") or row.get("per_type") or {}
    order = {"main_match": 0, "game_winner": 1, "map_winner": 2}
    return sorted((str(value) for value in per_type if value), key=lambda value: order.get(value, 99))


def _observed_buckets(row: dict[str, Any]) -> list[str]:
    observed = [str(value) for value in (row.get("observed_buckets") or []) if value]
    if observed:
        return sorted(set(observed))
    per_game_type = row.get("per_game_type_grades") or row.get("per_game_type") or {}
    return sorted(str(value) for value in per_game_type if value)


def _split_bucket(b: Any) -> tuple[str, str]:
    s = str(b or "")
    if ":" in s:
        g, mt = s.split(":", 1)
        return g, (mt or "main_match")
    return "", (s or "main_match")


def _leaderboard_scope(row: dict[str, Any]) -> list[dict[str, str]]:
    """专精列:每个会跟的桶 → {game, market_type}。跨游戏(multi/per-type)桶展开成它在该盘口
    实际玩过的各游戏(从 per_game_type_grades 推),列表才显示真实游戏 logo 而非笼统"跨"。"""
    buckets = row.get("eligible_buckets") or _observed_buckets(row)
    pgt = row.get("per_game_type_grades") if isinstance(row.get("per_game_type_grades"), dict) else {}
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for bucket in buckets:
        game, market_type = _split_bucket(bucket)
        if game and game != "multi":
            games = [game]
        else:  # 跨游戏:展开成它在该盘口实际有过的各游戏
            games = sorted({_split_bucket(k)[0] for k in pgt if _split_bucket(k)[1] == market_type and _split_bucket(k)[0]})
            games = games or ["multi"]  # 推不出就保留 multi 兜底
        for g in games:
            key = (g, market_type)
            if key not in seen:
                seen.add(key)
                out.append({"game": g, "market_type": market_type})
    return out


def _category_leaderboards(root: Path) -> list[tuple[str, Path, list[dict[str, Any]], int]]:
    rows: list[tuple[str, Path, list[dict[str, Any]], int]] = []
    for category, data_dir in category_data_dirs(root).items():
        db_rows, db_mtimes = LeaderboardStore(_leaderboard_db_file(data_dir)).load_leaderboard(category=category)
        leaderboard = [{**row, "category": category} for row in db_rows if isinstance(row, dict)]
        rows.append((category, data_dir, leaderboard, int(db_mtimes.get(category) or 0)))
    if not any(leaderboard for _, _, leaderboard, _ in rows):
        legacy_db_rows, legacy_db_mtimes = LeaderboardStore(_leaderboard_db_file(root)).load_leaderboard(category="esports")
        if legacy_db_rows:
            legacy_db = [{**row, "category": "esports"} for row in legacy_db_rows if isinstance(row, dict)]
            rows.append(("esports", root, legacy_db, int(legacy_db_mtimes.get("esports") or 0)))
    return rows


def _category_leaderboard_mtimes(root: Path) -> dict[str, int]:
    mtimes = {}
    for category, data_dir, _rows, mtime in _category_leaderboards(root):
        mtimes[category] = int(mtime or 0)
    return mtimes


def _signal_wallet_trade_at(signal: dict[str, Any]) -> int:
    values = [
        _parse_timestamp(leg.get("wallet_trade_at") or leg.get("trade_at") or leg.get("created_at"))
        for leg in signal.get("legs") or []
        if isinstance(leg, dict)
    ]
    direct = _parse_timestamp(signal.get("wallet_trade_at") or signal.get("last_trade_at"))
    return max([value for value in [direct, *values] if value] or [0])


FAVORITE_SNAPSHOT_FIELDS = (
    "wallet",
    "category",
    "league",
    "league_label",
    "grade",
    "last_esports_trade_at",
    "best_market_type",
    "best_market_type_label",
    "best_bucket",
    "best_bucket_label",
    "best_game_family",
    "best_bucket_score",
    "overall_esports_roi",
    "overall_wilson_win_rate_lower_bound",
    "overall_positive_market_rate",
    "wilson_win_rate_lower_bound",
    "entry_edge",
    "capital_weighted_edge",
    "esports_roi",
    "median_market_roi",
    "median_entry_price",
    "avg_market_cash",
    "participated_market_count",
    "total_cash_volume",
    "recent_bucket_market_count",
    "recent_bucket_window_days",
    "recent_bucket_roi",
    "recent_bucket_positive_rate",
    "recent_bucket_pnl",
    "recent_7d_market_count",
    "recent_7d_roi",
    "recent_7d_positive_rate",
    "recent_14d_market_count",
    "recent_14d_roi",
    "recent_14d_positive_rate",
    "scoring_version",
    "esports_win_count",
    "esports_loss_count",
    "esports_closed_count",
    "positive_market_rate",
    "eligible_market_types",
    "eligible_market_type_labels",
    "eligible_buckets",
    "eligible_bucket_labels",
    "eligible_game_families",
    "eligible_game_family_labels",
    "observed_market_types",
    "observed_market_type_labels",
    "observed_buckets",
    "observed_bucket_labels",
    "participation_rate",
    "participated_events",
    "eligible_event_count",
)


def compact_wallet_favorite_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    snapshot = {field: row.get(field) for field in FAVORITE_SNAPSHOT_FIELDS if field in row}
    snapshot["wallet"] = str(row.get("wallet") or "").lower()
    snapshot["category"] = normalize_category(str(row.get("category") or "esports")) or "esports"
    return snapshot


def build_wallets(data_dir: Path, *, follow_dir: Path | None = None) -> dict[str, Any]:
    leaderboard_sources = _category_leaderboards(data_dir)
    leaderboard = [row for _category, _dir, rows, _mtime in leaderboard_sources for row in rows]
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    perf_snapshot = store.load_dashboard_performance()
    follow_snapshot = store.load_dashboard_snapshot()
    quarantine_snapshot = store.load_dashboard_wallet_quarantine()
    favorite_snapshot = store.load_dashboard_wallet_favorites()
    performance = (perf_snapshot.get("performance") or {}).get("wallets") or {}
    quarantine = quarantine_snapshot.get("wallet_quarantine") or {}
    favorites = favorite_snapshot.get("wallet_favorites") or {}
    leaderboard_keys = {
        f"{normalize_category(str(row.get('category') or 'esports')) or 'esports'}:{str(row.get('wallet') or '').lower()}"
        for row in leaderboard
        if isinstance(row, dict) and row.get("wallet")
    }
    for key, favorite in (favorites or {}).items():
        if not isinstance(favorite, dict):
            continue
        wallet = str(favorite.get("wallet") or str(key).split(":", 1)[-1]).lower()
        category = normalize_category(str(favorite.get("category") or str(key).split(":", 1)[0] or "esports")) or "esports"
        favorite_key = f"{category}:{wallet}"
        if not wallet or favorite_key in leaderboard_keys:
            continue
        snapshot = favorite.get("snapshot") if isinstance(favorite.get("snapshot"), dict) else {}
        if not snapshot:
            continue
        row = dict(snapshot)
        row["wallet"] = wallet
        row["category"] = category
        row["favorite_snapshot_only"] = True
        leaderboard.append(row)
        leaderboard_keys.add(favorite_key)
    open_by_wallet: dict[str, list[dict[str, Any]]] = {}
    observed_trade_at_by_wallet: dict[str, int] = {}
    open_signals = follow_snapshot.get("open_signals", [])
    result_signals = follow_snapshot.get("results", [])
    all_follow_signals = [*open_signals, *result_signals]
    result_observed_by_wallet = observed_performance_from_results(result_signals)
    for signal in open_signals:
        wallet = str(signal.get("wallet") or "").lower()
        open_by_wallet.setdefault(wallet, []).append(signal)
    for signal in all_follow_signals:
        wallet = str(signal.get("wallet") or "").lower()
        if not wallet:
            continue
        observed_trade_at_by_wallet[wallet] = max(observed_trade_at_by_wallet.get(wallet, 0), _signal_wallet_trade_at(signal))
    rows = []
    for row in leaderboard if isinstance(leaderboard, list) else []:
        row = enrich_esports_bucket_scores(row, now_ts=int(time.time())) if normalize_category(str(row.get("category") or "")) != "sports" else row
        # Grading is per market_type, so a wallet may qualify only on a sub-bucket
        # (e.g. game_winner) while its blended overall record looks weak. Display the
        # eligible bucket's own stats so the shown numbers match the grade + type label.
        metrics = _eligible_display_metrics(row)
        # 胜率列 = "我们会跟的专精桶"的 θ̂:1 桶=该桶,多桶=各桶平均(与评分胜率门同口径)。
        # 不用整体 positive_market_rate(口径不对)。前端拿不到 eligible_bucket_details,故在后端算好。
        _bucket_details = row.get("eligible_bucket_details") or []
        _bucket_wrs = [to_float(d.get("win_rate")) for d in _bucket_details if d.get("win_rate") is not None]
        followed_win_rate = (
            sum(_bucket_wrs) / len(_bucket_wrs) if _bucket_wrs
            else row.get("best_bucket_win_rate") if row.get("best_bucket_win_rate") is not None
            else metrics.get("bucket_win_rate")
        )
        # 场均交易额是钱包级,在顶层或 candidate 里(best-bucket metrics 没有 → 之前显示 $0)。
        resolved_avg_market_cash = (
            row.get("avg_market_cash")
            or (row.get("candidate") or {}).get("avg_market_cash")
            or metrics.get("avg_market_cash")
        )
        wallet = str(row.get("wallet") or "").lower()
        category = normalize_category(str(row.get("category") or "")) or "esports"
        favorite_key = f"{category}:{wallet}"
        favorite_row = favorites.get(favorite_key) or favorites.get(wallet)
        quarantine_key = f"{category}:{wallet}"
        is_quarantined = quarantine_key in quarantine or wallet in quarantine
        is_favorite = bool(favorite_row) and not is_quarantined
        # v17:删展示层的「拉通整体胜率门」(positive_market_rate<0.63)。评分已是分桶 edge_lb 单轴,
        # 专精钱包(某桶高 θ̂、整体胜率低)本就该上榜;此处再按整体胜率过滤会把它们藏掉(DB 有但不显示)。
        league = normalize_league(row.get("league"))
        row_league_label = str(row.get("league_label") or league_label(league)).strip()
        observed_source = result_observed_by_wallet.get(wallet) or performance.get(wallet, {})
        observed = wallet_observed_performance(observed_source, open_count=len(open_by_wallet.get(wallet, [])))
        observed_market_types = _observed_market_types(row)
        observed_buckets = _observed_buckets(row)
        last_trade_at = max(_parse_timestamp(row.get("last_esports_trade_at")), observed_trade_at_by_wallet.get(wallet, 0))
        rows.append(
            {
                # 仅返回前端 dashboardV2(adapt.js/app.jsx)实际引用的字段;一批 0 引用的
                # 派生/未展示指标(short_addr、各 *_market_types/*_labels、recent_7d/14d/bucket
                # 明细、wilson/edge/median 等)已剔除以瘦身 /api/wallets 负载。
                "wallet": wallet,
                "category": category,
                "league": league,
                "game": league if category == "sports" else "",
                "game_label": row_league_label if category == "sports" else "",
                "grade": row.get("grade"),
                "primary_game": row.get("primary_game"),
                "observed_at": row.get("observed_at"),  # M4 动态观测并入榜时间(供 2h "new" 标记)
                "last_esports_trade_at": last_trade_at or row.get("last_esports_trade_at"),
                "best_bucket": row.get("best_bucket"),
                "best_game_family": row.get("best_game_family"),
                "best_bucket_score": row.get("best_bucket_score"),
                "overall_esports_roi": row.get("overall_esports_roi", row.get("esports_roi")),
                "esports_roi": metrics.get("esports_roi"),
                "avg_market_cash": resolved_avg_market_cash,
                "recent_bucket_roi": metrics.get("recent_bucket_roi"),
                "scoring_version": row.get("scoring_version"),
                "esports_closed_count": metrics.get("esports_closed_count"),
                "positive_market_rate": metrics.get("positive_market_rate"),
                "followed_win_rate": followed_win_rate,  # 会跟桶的 θ̂(1 桶=该桶,多桶=平均)
                "price_bands": _price_bands_for_display(row),  # 价档胜率分布(纯展示,标可跟区)
                # 以下几项前端不展示,但服务端需要:wallet_leaderboard_rank_key 排序读取
                # wilson/entry_edge/capital_weighted_edge/median_market_roi/esports_loss_count;
                # favorite_snapshot_only 供 active_rows 过滤。删了会坏排序/收藏过滤,保留。
                "wilson_win_rate_lower_bound": metrics.get("wilson_win_rate_lower_bound"),
                "entry_edge": metrics.get("entry_edge"),
                "capital_weighted_edge": metrics.get("capital_weighted_edge"),
                "median_market_roi": metrics.get("median_market_roi"),
                "esports_loss_count": metrics.get("esports_loss_count"),
                "favorite_snapshot_only": bool(row.get("favorite_snapshot_only")),
                "eligible_buckets": row.get("eligible_buckets") or [],
                "scope": _leaderboard_scope(row),  # 专精列:跨游戏桶已展开;label 前端 scopeList 自推
                "observed_buckets": observed_buckets,
                "favorite": is_favorite,
                "quarantined": is_quarantined,
                "quarantine": quarantine.get(quarantine_key) or quarantine.get(wallet),
                "observed": observed,
            }
        )
    for category in CATEGORIES:
        category_rows = [row for row in rows if row.get("category") == category]
        category_rows.sort(key=wallet_leaderboard_rank_key)
        for row in category_rows:
            row.pop("rank", None)
        active_rows = [
            row
            for row in category_rows
            if not row.get("quarantined")
            and not row.get("favorite_snapshot_only")
        ]
        for index, row in enumerate(active_rows, start=1):
            row["rank"] = index
    rows.sort(key=lambda row: (CATEGORIES.index(row.get("category")) if row.get("category") in CATEGORIES else len(CATEGORIES), row.get("rank") or 999999))
    rows_by_category = {
        category: [row for row in rows if row.get("category") == category]
        for category in CATEGORIES
    }
    source_mtimes = {category: mtime for category, _source_dir, _rows, mtime in leaderboard_sources if category in CATEGORIES}
    return {
        "wallets": rows,
        "count": len(rows),
        "active_count": sum(1 for row in rows if not row.get("quarantined") and not row.get("favorite_snapshot_only")),
        "favorite_count": sum(1 for row in rows if row.get("favorite")),
        "quarantined_count": sum(1 for row in rows if not row.get("favorite") and row.get("quarantined")),
        "leaderboard_updated_at": max([mtime for *_rest, mtime in leaderboard_sources] or [0]),
        "by_category": {
            category: {
                "count": len(rows_by_category.get(category, [])),
                "active_count": sum(
                    1
                    for row in rows_by_category.get(category, [])
                    if not row.get("quarantined")
                    and not row.get("favorite_snapshot_only")
                ),
                "favorite_count": sum(1 for row in rows_by_category.get(category, []) if row.get("favorite")),
                "leaderboard_updated_at": source_mtimes.get(category, 0),
                "scoring_version": max([int(row.get("scoring_version") or 0) for row in rows_by_category.get(category, [])] or [0]) or None,
                "quarantined_count": sum(
                    1
                    for row in rows_by_category.get(category, [])
                    if not row.get("favorite")
                    and (
                        f"{category}:{str(row.get('wallet') or '').lower()}" in quarantine
                    or str(row.get("wallet") or "").lower() in quarantine
                    )
                ),
            }
            for category in CATEGORIES
        },
        "scoring_version": max([int(row.get("scoring_version") or 0) for row in rows] or [0]) or None,
        "db_ready": bool(perf_snapshot.get("db_ready") or follow_snapshot.get("db_ready") or quarantine_snapshot.get("db_ready") or favorite_snapshot.get("db_ready")),
    }


def wallet_observed_performance(performance: dict[str, Any], *, open_count: int = 0) -> dict[str, Any]:
    signals = int(performance.get("signals") or 0)
    wins = int(performance.get("wins") or 0)
    losses = max(0, signals - wins)
    exits = int(performance.get("exits") or 0)
    our_pnl = round(to_float(performance.get("our_pnl")), 8)
    wallet_pnl = round(to_float(performance.get("wallet_pnl")), 8)
    win_rate = to_float(performance.get("win_rate")) if signals else None
    return {
        "signals": signals,
        "wins": wins,
        "losses": losses,
        "exits": exits,
        "open": open_count,
        "our_pnl": our_pnl,
        "wallet_pnl": wallet_pnl,
        "win_rate": win_rate,
        "has_loss": losses > 0 or our_pnl < -0.000001,
    }


def observed_performance_from_results(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    observed: dict[str, dict[str, Any]] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        wallet = str(result.get("wallet") or "").lower()
        if not wallet:
            continue
        pnl = _signal_our_pnl(result)
        row = observed.setdefault(wallet, {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0})
        row["signals"] += 1
        row["wins"] += 1 if _result_win(result) else 0
        if str(result.get("status") or "") == "exited":
            row["exits"] += 1
        row["our_pnl"] = round(to_float(row.get("our_pnl")) + pnl, 8)
        row["wallet_pnl"] = round(to_float(row.get("wallet_pnl")) + _signal_wallet_pnl(result), 8)
    for row in observed.values():
        signals = int(row.get("signals") or 0)
        row["win_rate"] = round(to_float(row.get("wins")) / signals, 8) if signals else None
    return observed


def wallet_leaderboard_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    score = row.get("best_bucket_score")
    if score is not None:
        return (
            bool(row.get("quarantined")),
            -to_float(score),
            -to_float(row.get("wilson_win_rate_lower_bound")),
            -to_float(row.get("capital_weighted_edge") or row.get("entry_edge")),
            -to_float(row.get("positive_market_rate")),
            -int(row.get("esports_closed_count") or 0),
            str(row.get("wallet") or ""),
        )
    loss_count = int(row.get("esports_loss_count") or 0)
    return (
        bool(row.get("quarantined")),
        loss_count > 0,
        loss_count,
        -to_float(row.get("positive_market_rate")),
        -to_float(row.get("wilson_win_rate_lower_bound")),
        -to_float(row.get("entry_edge")),
        -to_float(row.get("median_market_roi") or row.get("esports_roi")),
        str(row.get("wallet") or ""),
    )


def _merge_ai_audit_groups(
    groups: dict[str, dict[str, Any]],
    audit: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    assessments = {
        str(row.get("condition_id") or "").lower(): row
        for row in audit.get("assessments") or []
        if row.get("condition_id")
    }
    intents_by_condition: dict[str, list[dict[str, Any]]] = {}
    shadows_by_intent = {
        str(row.get("intent_id") or ""): row
        for row in audit.get("shadows") or []
        if row.get("intent_id")
    }
    for intent in audit.get("intents") or []:
        cid = str(intent.get("condition_id") or "").lower()
        if cid:
            intents_by_condition.setdefault(cid, []).append(intent)

    # A condition with only blocked intentions has no follow_signal, but it is a
    # first-class research record and must remain visible after settlement.
    for cid, intents in intents_by_condition.items():
        if cid in groups or not any(row.get("action") == "blocked" for row in intents):
            continue
        sample = intents[0]
        pseudo = []
        for intent in intents:
            if intent.get("action") != "blocked":
                continue
            shadow = shadows_by_intent.get(str(intent.get("intent_id") or "")) or {}
            pseudo.append({
                "signal_id": "blocked:" + str(intent.get("intent_id") or ""),
                "condition_id": cid,
                "wallet": intent.get("wallet"),
                "outcome_index": intent.get("outcome_index"),
                "outcome": intent.get("outcome"),
                "event_title": intent.get("event_title"),
                "market_question": intent.get("market_question"),
                "market_type": intent.get("market_type") or "main_match",
                "category": intent.get("category") or "esports",
                "game_family": intent.get("game_family"),
                "match_start_time": intent.get("match_start_time"),
                "end_date": intent.get("end_date"),
                "status": "open" if shadow.get("status") == "open" else "settled",
                "created_at": intent.get("created_at"),
                "updated_at": shadow.get("updated_at") or intent.get("updated_at"),
                "legs": [],
            })
        built = _follow_groups_from_signals(pseudo).get(cid)
        if built:
            # Keep AI-blocked as a first-class list status after settlement; the
            # shadow lifecycle is carried separately for the audit/result view.
            built["status"] = "ai_blocked"
            built["ai_shadow_status"] = "open" if any(
                (shadows_by_intent.get(str(row.get("intent_id") or "")) or {}).get("status") == "open"
                for row in intents if row.get("action") == "blocked"
            ) else "settled"
            built["title"] = built.get("title") or sample.get("event_title")
            groups[cid] = built

    priority = {"blocked": 4, "agree": 3, "insufficient": 2, "unavailable": 1}
    for cid, group in groups.items():
        intents = sorted(
            intents_by_condition.get(cid) or [],
            key=lambda row: (priority.get(str(row.get("action") or ""), 0), int(row.get("updated_at") or 0)),
            reverse=True,
        )
        if not intents:
            continue
        action_counts: dict[str, int] = {}
        for intent in intents:
            action = str(intent.get("action") or "unavailable")
            action_counts[action] = action_counts.get(action, 0) + 1
        shadow_rows = [
            shadows_by_intent[str(intent.get("intent_id"))]
            for intent in intents
            if str(intent.get("intent_id") or "") in shadows_by_intent
        ]
        group["ai_risk"] = assessments.get(cid)
        group["ai_action"] = str(intents[0].get("action") or "unavailable")
        group["ai_action_counts"] = action_counts
        group["ai_intent_count"] = len(intents)
        group["ai_blocked_intended_stake"] = round(sum(
            to_float(row.get("intended_stake")) for row in intents if row.get("action") == "blocked"
        ), 8)
        group["ai_net_effect"] = round(sum(to_float(row.get("ai_net_effect")) for row in shadow_rows), 8)
    return groups


def build_follows(data_dir: Path, *, page: int = 1, size: int = 25, status: str = "", category: str = "", follow_dir: Path | None = None, client: Any = None) -> dict[str, Any]:
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    result = store.load_dashboard_follow_rows(page=1, size=10_000)
    logo_cache = _load_team_logo_cache(data_dir)
    allowed_statuses = {"open", "settled", "insufficient_balance", "ai_blocked"}
    status_filter = status if status in allowed_statuses else ""
    category_filter = normalize_category(category)
    groups = _follow_groups_from_signals(
        [signal for signal in result.get("signals", []) if _signal_has_actual_follow(signal)]
    )
    groups = _merge_ai_audit_groups(groups, store.load_ai_audit(limit=10_000))
    # 进行中(未结算)的跟单排最前;组内再按开单时间(最近一次建腿动作)最近的在前。
    rows = sorted(
        groups.values(),
        key=lambda row: (
            1 if str(row.get("status") or "") in {"open", "insufficient_balance", "ai_blocked"} else 0,
            int(row.get("last_follow_action_at") or 0),
            int(row.get("last_activity_at") or 0),
            str(row.get("condition_id") or ""),
        ),
        reverse=True,
    )
    if status_filter == "open":
        rows = [row for row in rows if str(row.get("status") or "") in {"open", "insufficient_balance"}]
    elif status_filter == "ai_blocked":
        rows = [row for row in rows if str(row.get("ai_action") or "") == "blocked"]
    elif status_filter:
        rows = [row for row in rows if str(row.get("status") or "") == status_filter]
    if category_filter:
        rows = [row for row in rows if _signal_category(row) == category_filter]
    total = len(rows)
    page = max(1, int(page or 1))
    size = max(1, int(size or 25))
    start = (page - 1) * size
    rows = rows[start : start + size]
    # 浮盈现价:活跃缓存最多滞后 event_cache_ttl_minutes(默认60min)。对**进行中(open)**
    # 的跟单,渲染时批量拉一次实时盘口价覆盖缓存价(只读内存覆盖,不写库);失败回退缓存。
    open_cids = [
        str(row.get("condition_id") or "")
        for row in rows
        if str(row.get("status") or "") in {"open", "insufficient_balance"} and row.get("condition_id")
    ]
    live_prices_by_cid = _live_outcome_prices(client, open_cids)
    # 脱榜标记:源钱包已不在当前 leaderboard(被刷新排除)。跟单仍跟至结算,但需清晰标出。
    active_leaderboard = {
        str(lb_row.get("wallet") or "").lower()
        for _cat, _dir, lb_rows, _mt in _category_leaderboards(data_dir)
        for lb_row in lb_rows
        if lb_row.get("wallet")
    }
    for row in rows:
        sources = [str(w).lower() for w in (row.get("wallets") or []) if w]
        off = [w for w in sources if w not in active_leaderboard]
        row["off_leaderboard_wallets"] = off
        row["off_leaderboard_wallet_count"] = len(off)
        # 全部源钱包都脱榜 → 这条跟单整体"源已脱榜";部分脱榜的多源跟单仍有在榜源,不标。
        row["source_off_leaderboard"] = bool(sources) and len(off) == len(sources)
        market = _active_market_by_condition(data_dir, str(row.get("condition_id") or ""), follow_dir=follow_dir)
        row["match_start_time"] = row.get("match_start_time") or market.get("match_start_time") or market.get("market_start_time")
        row["end_date"] = row.get("end_date") or market.get("end_date")
        live = live_prices_by_cid.get(str(row.get("condition_id") or "").lower())
        if live:
            market = {**market, "outcome_prices": live}
            row["price_live"] = True
        _attach_follow_unrealized_pnl(row, market)
        row["match_parts"] = _match_parts_for_row(row)
        row["team_logos"] = _team_logos_for_parts(row.get("match_parts"), logo_cache)
    return {
        "page": page,
        "size": size,
        "total": total,
        "status": status_filter,
        "category": category_filter,
        "follows": rows,
        "db_ready": bool(result.get("db_ready")),
    }


def _attach_follow_unrealized_pnl(row: dict[str, Any], market: dict[str, Any]) -> None:
    prices = [_to_float(value) for value in (market.get("outcome_prices") or market.get("outcomePrices") or [])]
    open_legs = row.get("open_pnl_legs") if isinstance(row.get("open_pnl_legs"), list) else []
    unrealized = 0.0
    priced_count = 0
    current_prices = []
    for leg in open_legs:
        if not isinstance(leg, dict):
            continue
        if leg.get("would_follow") is False:
            continue
        outcome_index = int(leg.get("outcome_index") or 0)
        if outcome_index < 0 or outcome_index >= len(prices):
            continue
        current_price = prices[outcome_index]
        stake = _leg_actual_stake(leg)
        entry = _to_float(leg.get("our_entry_price"))
        if stake <= 0 or entry <= 0:
            continue
        unrealized += stake * (current_price - entry) / entry
        priced_count += 1
        current_prices.append(current_price)
    if priced_count:
        row["current_price"] = round(sum(current_prices) / len(current_prices), 8)
        row["unrealized_pnl"] = round(unrealized, 8)
    else:
        row["current_price"] = None
        row["unrealized_pnl"] = None
    row["display_pnl"] = row["unrealized_pnl"] if row.get("status") == "open" and row["unrealized_pnl"] is not None else row.get("our_realized_pnl")
    row["display_pnl_kind"] = "unrealized" if row.get("status") == "open" and row["unrealized_pnl"] is not None else "realized"
    row.pop("open_pnl_legs", None)


def _signal_follow_entry_summary(signal: dict[str, Any]) -> dict[str, Any]:
    total_stake = 0.0
    priced_stake = 0.0
    weighted_entry = 0.0
    for leg in signal.get("legs") or []:
        if not isinstance(leg, dict) or leg.get("would_follow") is False:
            continue
        stake = _leg_hypothetical_stake(leg)
        if stake <= 0:
            continue
        total_stake += stake
        entry = to_float(leg.get("our_entry_price"))
        if entry > 0:
            priced_stake += stake
            weighted_entry += stake * entry
    return {
        "follow_total_stake": round(total_stake, 8),
        "follow_avg_entry_price": round(weighted_entry / priced_stake, 8) if priced_stake > 0 else None,
        "_priced_stake": priced_stake,
        "_weighted_entry": weighted_entry,
    }


def _signal_follow_exit_price(signal: dict[str, Any]) -> float | None:
    """提前卖出的加权卖出价:按各次 partial_exits 的 sold_stake 加权;
    无 partial_exits 退化到 exit_price。仅对镜像平仓(提前卖出)有意义,等结算的为 None。"""
    weighted = 0.0
    stake = 0.0
    for record in signal.get("partial_exits") or []:
        if not isinstance(record, dict):
            continue
        price = to_float(record.get("price"))
        sold = to_float(record.get("sold_stake"))
        if price > 0 and sold > 0:
            weighted += price * sold
            stake += sold
    if stake > 0:
        return round(weighted / stake, 8)
    price = to_float(signal.get("exit_price"))
    return round(price, 8) if price > 0 else None


def _signal_follow_outcome_key(signal: dict[str, Any]) -> str:
    condition_id = str(signal.get("condition_id") or "").lower()
    outcome_index = signal.get("outcome_index")
    if outcome_index is not None and str(outcome_index) != "":
        return f"{condition_id}:idx:{outcome_index}"
    return f"{condition_id}:outcome:{str(signal.get('outcome') or '').strip().lower()}"


def build_follow_detail(data_dir: Path, condition_id: str, *, follow_dir: Path | None = None, client: Any = None) -> dict[str, Any]:
    condition_id = condition_id.lower()
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    result = store.load_dashboard_follow_detail(condition_id)
    ai_audit = store.load_ai_audit(condition_id=condition_id, limit=1000)
    ai_assessment = (ai_audit.get("assessments") or [None])[0]
    ai_intents = ai_audit.get("intents") or []
    ai_shadows = ai_audit.get("shadows") or []
    ai_shadow_by_intent = {str(row.get("intent_id") or ""): row for row in ai_shadows}
    signals = result.get("signals", [])
    _annotate_signal_quality(signals)
    market = _active_market_by_condition(data_dir, condition_id, follow_dir=follow_dir)
    # 与列表口径一致:渲染时拉一次盘口价覆盖缓存价(只读,不写库)。open 盘取实时价,
    # 已结束/已结算盘取结算价(1/0)——否则结束盘缓存被清空后详情不显示价。失败回退缓存价。
    price_live = False
    live = _live_outcome_prices(client, [condition_id]).get(condition_id)
    if live:
        market = {**market, "outcome_prices": live}
        price_live = True
    logo_cache = _load_team_logo_cache(data_dir)
    leaderboard_ranks = _leaderboard_rank_by_wallet(data_dir, follow_dir=follow_dir)
    by_wallet: dict[str, dict[str, Any]] = {}
    title = ""
    question = ""
    match_start_time = None
    end_date = None
    event_slug = ""
    market_type = ""
    market_type_label = ""
    category = ""
    for signal in signals:
        title = title or str(signal.get("event_title") or signal.get("title") or signal.get("market_title") or "")
        question = question or str(signal.get("market_question") or signal.get("question") or "")
        match_start_time = match_start_time or signal.get("match_start_time") or signal.get("market_start_time")
        end_date = end_date or signal.get("end_date")
        event_slug = event_slug or _event_slug(signal)
        market_type = market_type or str(signal.get("market_type") or "")
        market_type_label = market_type_label or str(signal.get("market_type_label") or "")
        signal_category = normalize_category(str(signal.get("category") or ""))
        category = category or signal_category
        wallet = str(signal.get("wallet") or "").lower()
        bucket = by_wallet.setdefault(
            wallet,
            {
                "wallet": wallet,
                "short_addr": short_addr(wallet),
                "leaderboard_rank": leaderboard_ranks.get(f"{signal_category}:{wallet}") or leaderboard_ranks.get(wallet),
                "signals": [],
                "leg_count": 0,
                "_follow_total_stake": 0.0,
                "_follow_priced_stake": 0.0,
                "_follow_weighted_entry": 0.0,
                "_follow_outcome_keys": set(),
                "_follow_realized_pnl": 0.0,
                "_follow_realized_count": 0,
                "_follow_exit_stake": 0.0,
                "_follow_weighted_exit": 0.0,
            },
        )
        entry_summary = _signal_follow_entry_summary(signal)
        signal["follow_total_stake"] = entry_summary["follow_total_stake"]
        signal["follow_avg_entry_price"] = entry_summary["follow_avg_entry_price"]
        exit_price = _signal_follow_exit_price(signal)
        signal["follow_exit_price"] = exit_price
        exit_stake = sum(
            to_float(record.get("sold_stake"))
            for record in (signal.get("partial_exits") or [])
            if isinstance(record, dict)
        )
        if exit_price is not None and exit_stake > 0:
            bucket["_follow_exit_stake"] += exit_stake
            bucket["_follow_weighted_exit"] += exit_price * exit_stake
        if str(signal.get("status") or "") in {"settled", "exited"}:
            realized_pnl = _signal_our_pnl(signal)
            signal["follow_realized_pnl"] = round(realized_pnl, 8)
            bucket["_follow_realized_pnl"] += realized_pnl
            bucket["_follow_realized_count"] += 1
        bucket["signals"].append(signal)
        bucket["leg_count"] += len(signal.get("legs") or [])
        bucket["_follow_total_stake"] += entry_summary["follow_total_stake"] or 0.0
        bucket["_follow_priced_stake"] += entry_summary["_priced_stake"] or 0.0
        bucket["_follow_weighted_entry"] += entry_summary["_weighted_entry"] or 0.0
        if entry_summary["follow_total_stake"] > 0:
            bucket["_follow_outcome_keys"].add(_signal_follow_outcome_key(signal))
    ai_sample = ai_intents[0] if ai_intents else {}
    for bucket in by_wallet.values():
        total_stake = to_float(bucket.pop("_follow_total_stake", 0.0))
        priced_stake = to_float(bucket.pop("_follow_priced_stake", 0.0))
        weighted_entry = to_float(bucket.pop("_follow_weighted_entry", 0.0))
        outcome_keys = bucket.pop("_follow_outcome_keys", set())
        realized_pnl = to_float(bucket.pop("_follow_realized_pnl", 0.0))
        realized_count = int(bucket.pop("_follow_realized_count", 0) or 0)
        outcome_count = len(outcome_keys) if isinstance(outcome_keys, set) else 0
        mixed_outcomes = outcome_count > 1
        bucket["follow_total_stake"] = round(total_stake, 8)
        bucket["followed_outcome_count"] = outcome_count
        bucket["follow_mixed_outcomes"] = mixed_outcomes
        bucket["follow_avg_entry_price"] = (
            round(weighted_entry / priced_stake, 8) if priced_stake > 0 and not mixed_outcomes else None
        )
        exit_stake = to_float(bucket.pop("_follow_exit_stake", 0.0))
        weighted_exit = to_float(bucket.pop("_follow_weighted_exit", 0.0))
        bucket["follow_exit_price"] = (
            round(weighted_exit / exit_stake, 8) if exit_stake > 0 and not mixed_outcomes else None
        )
        # 提前卖出的镜像平仓金额(各次 partial_exits 的 sold_stake 之和),供前端在钱包头部
        # 与"提前卖出"标签一起显示(比只显示卖出价更直观)。
        bucket["follow_exit_stake"] = round(exit_stake, 8) if exit_stake > 0 and not mixed_outcomes else None
        bucket["follow_realized_pnl"] = round(realized_pnl, 8) if realized_count else None
    title = title or str(market.get("title") or ai_sample.get("event_title") or "")
    question = question or str(market.get("question") or ai_sample.get("market_question") or "")
    match_start_time = match_start_time or market.get("match_start_time") or market.get("market_start_time") or ai_sample.get("match_start_time")
    end_date = end_date or market.get("end_date") or ai_sample.get("end_date")
    event_slug = event_slug or _event_slug(market)
    market_type = market_type or str(market.get("market_type") or ai_sample.get("market_type") or "")
    market_type_label = market_type_label or str(market.get("market_type_label") or "")
    category = category or normalize_category(str(market.get("category") or ai_sample.get("category") or "")) or "esports"
    match_parts = _match_parts_for_row(
        {
            "title": title,
            "question": question,
            "category": category,
            "market_type_label": market_type_label,
        }
    )
    action_counts: dict[str, int] = {}
    for intent in ai_intents:
        action = str(intent.get("action") or "unavailable")
        action_counts[action] = action_counts.get(action, 0) + 1
    blocked_wallets = []
    for intent in ai_intents:
        if intent.get("action") != "blocked":
            continue
        shadow = ai_shadow_by_intent.get(str(intent.get("intent_id") or "")) or {}
        blocked_wallets.append({
            "wallet": intent.get("wallet"),
            "short_addr": short_addr(str(intent.get("wallet") or "")),
            "outcome": intent.get("outcome"),
            "outcome_index": intent.get("outcome_index"),
            "intended_stake": intent.get("intended_stake"),
            "entry_price": intent.get("entry_price"),
            "created_at": intent.get("created_at"),
            "shadow_status": shadow.get("status"),
            "baseline_pnl": shadow.get("realized_pnl") if shadow.get("status") in {"settled", "exited"} else None,
            "ai_net_effect": shadow.get("ai_net_effect") if shadow.get("status") in {"settled", "exited"} else None,
            "outcome_won": shadow.get("outcome_won"),
        })
    ai_detail = None
    if ai_assessment or ai_intents:
        ai_detail = {
            "assessment": ai_assessment,
            "action_counts": action_counts,
            "intent_count": len(ai_intents),
            "blocked_intent_count": action_counts.get("blocked", 0),
            "blocked_intended_stake": round(sum(to_float(row.get("intended_stake")) for row in ai_intents if row.get("action") == "blocked"), 8),
            "blocked_wallets": blocked_wallets,
            "net_effect": round(sum(to_float(row.get("ai_net_effect")) for row in ai_shadows), 8),
            "counterfactual_label": "被拦截意图级反事实；不包含释放资金后续用途",
        }
    return {
        "condition_id": condition_id,
        "category": category,
        "title": title,
        "question": question,
        "match_start_time": match_start_time,
        "end_date": end_date,
        "event_slug": event_slug,
        "event_url": _polymarket_event_url(event_slug),
        "market_type": market_type,
        "market_type_label": market_type_label,
        "match_parts": match_parts,
        "team_logos": _team_logos_for_parts(match_parts, logo_cache),
        "outcomes": market.get("outcomes") or ai_sample.get("outcomes") or (
            [ai_assessment.get("team_a"), ai_assessment.get("team_b")]
            if isinstance(ai_assessment, dict) and ai_assessment.get("team_a") and ai_assessment.get("team_b")
            else None
        ),
        "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
        "price_live": price_live,
        "wallets": list(by_wallet.values()),
        "signal_count": len(signals),
        "ai_risk": ai_detail,
        "db_ready": bool(result.get("db_ready")),
    }


def _leaderboard_rank_by_wallet(data_dir: Path, *, follow_dir: Path | None = None) -> dict[str, int]:
    cats = _category_leaderboards(data_dir)
    # 缓存键 = 各类目 leaderboard.db mtime;榜单未重建直接复用(排名只在重建时变;手动隔离的
    # rank 数字略陈到下次重建为止,仅展示无碍)。
    mtime_key = tuple(int(mtime) for _cat, _dir, _lb, mtime in cats)
    cache_key = str(data_dir)
    with _RANK_CACHE_LOCK:
        cached = _RANK_CACHE.get(cache_key)
        if cached is not None and cached[0] == mtime_key:
            return cached[1]
    rows_by_category: dict[str, list[dict[str, Any]]] = {category: [] for category in CATEGORIES}
    quarantine_snapshot = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir)).load_dashboard_wallet_quarantine()
    quarantine = quarantine_snapshot.get("wallet_quarantine") or {}
    for source_category, _dir, leaderboard, _mtime in cats:
        for row in leaderboard if isinstance(leaderboard, list) else []:
            if not isinstance(row, dict):
                continue
            wallet = str(row.get("wallet") or "").lower()
            if not wallet:
                continue
            category = normalize_category(str(row.get("category") or source_category or "")) or "esports"
            row = enrich_esports_bucket_scores(row, now_ts=int(time.time())) if category != "sports" else row
            metrics = _eligible_display_metrics(row)
            # v17:同上,删拉通整体胜率门(与分桶 edge_lb 评分轴一致)。
            quarantine_key = f"{category}:{wallet}"
            merged = {
                **row,
                **metrics,
                "wallet": wallet,
                "category": category,
                "quarantined": quarantine_key in quarantine or wallet in quarantine,
            }
            if merged.get("quarantined"):
                continue
            rows_by_category.setdefault(category, []).append(merged)
    ranks: dict[str, int] = {}
    for category, rows in rows_by_category.items():
        rows.sort(key=wallet_leaderboard_rank_key)
        for index, row in enumerate(rows, start=1):
            wallet = str(row.get("wallet") or "").lower()
            ranks[f"{category}:{wallet}"] = index
            ranks.setdefault(wallet, index)
    with _RANK_CACHE_LOCK:
        _RANK_CACHE[cache_key] = (mtime_key, ranks)
    return ranks


def build_wallet_follow_detail(
    data_dir: Path,
    wallet: str,
    *,
    status: str = "",
    page: int = 1,
    size: int = 20,
    follow_dir: Path | None = None,
) -> dict[str, Any]:
    wallet = wallet.lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", wallet):
        return {
            "wallet": wallet,
            "short_addr": short_addr(wallet),
            "signals": [],
            "count": 0,
            "total": 0,
            "page": max(1, int(page or 1)),
            "size": max(1, int(size or 20)),
            "db_ready": False,
        }
    allowed_statuses = {"open", "settled", "exited", "closed"}
    status_filter = status if status in allowed_statuses else ""
    statuses = {"settled", "exited"} if status_filter == "closed" else ({status_filter} if status_filter else set())
    result = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir)).load_dashboard_wallet_follow_detail(wallet, statuses=statuses)
    signals = result.get("signals", [])
    signals = sorted(
        [signal for signal in signals if isinstance(signal, dict)],
        key=lambda signal: _signal_activity_at(signal),
        reverse=True,
    )
    _annotate_signal_quality(signals)
    for signal in signals:
        signal["settlement_type"] = _signal_settlement_type(signal)
        # 简易跟单列表用:均价(我们的加权入场价)+ 卖出价(提前卖出的加权出场价)+ 结算 PnL。
        signal["follow_avg_entry_price"] = _signal_follow_entry_summary(signal).get("follow_avg_entry_price")
        signal["follow_exit_price"] = _signal_follow_exit_price(signal)
        signal["our_pnl"] = _signal_our_pnl(signal)
    total = len(signals)
    page = max(1, int(page or 1))
    size = max(1, int(size or 20))
    start = (page - 1) * size
    page_signals = signals[start : start + size]
    return {
        "wallet": wallet,
        "short_addr": short_addr(wallet),
        "status": status_filter,
        "signals": page_signals,
        "count": total,
        "total": total,
        "page": page,
        "size": size,
        "db_ready": bool(result.get("db_ready")),
    }


def _signal_settlement_type(signal: dict[str, Any]) -> str:
    status = str(signal.get("status") or "")
    if status == "exited":
        # 我们自己的主盘止损强平,区别于镜像钱包卖出的"提前卖出"(manual_exit)。
        return "stop_loss" if str(signal.get("exit_reason") or "") == "stop_loss" else "manual_exit"
    if status == "settled":
        return "auto_settlement"
    return ""


def _signal_activity_at(signal: dict[str, Any]) -> int:
    direct = (
        signal.get("settled_at")
        or signal.get("exit_at")
        or signal.get("updated_at")
        or signal.get("created_at")
    )
    ts = _parse_timestamp(direct)
    if ts:
        return ts
    leg_times = [
        _parse_timestamp(leg.get("leg_at") or leg.get("created_at"))
        for leg in signal.get("legs") or []
        if isinstance(leg, dict)
    ]
    return max([value for value in leg_times if value] or [0])


def _tracking_started_at(signals: list[dict[str, Any]]) -> int:
    timestamps = [_signal_tracking_started_at(signal) for signal in signals if isinstance(signal, dict)]
    return min([value for value in timestamps if value] or [0])


def _signal_tracking_started_at(signal: dict[str, Any]) -> int:
    timestamps: list[int] = []
    for leg in signal.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        for key in ("wallet_trade_at", "trade_at", "leg_at", "created_at", "observed_at"):
            ts = _parse_timestamp(leg.get(key))
            if ts:
                timestamps.append(ts)
    for key in ("wallet_trade_at", "last_trade_at", "created_at", "updated_at", "settled_at", "exit_at"):
        ts = _parse_timestamp(signal.get(key))
        if ts:
            timestamps.append(ts)
    return min(timestamps or [0])


def _signal_follow_action_at(signal: dict[str, Any]) -> int:
    direct = _parse_timestamp(signal.get("wallet_trade_at") or signal.get("last_trade_at"))
    leg_times = [
        _parse_timestamp(
            leg.get("wallet_trade_at")
            or leg.get("trade_at")
            or leg.get("leg_at")
            or leg.get("created_at")
        )
        for leg in signal.get("legs") or []
        if isinstance(leg, dict)
    ]
    fallback = _parse_timestamp(signal.get("created_at"))
    return max([value for value in [direct, fallback, *leg_times] if value] or [0])


def _event_slug(row: dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    return str(row.get("event_slug") or row.get("eventSlug") or row.get("slug") or "").strip()


def _first_event_slug(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        slug = _event_slug(row)
        if slug:
            return slug
    return ""


def _polymarket_event_url(event_slug: Any) -> str:
    slug = str(event_slug or "").strip().strip("/")
    if not slug:
        return ""
    if slug.startswith(("https://", "http://")):
        return slug
    return f"https://polymarket.com/event/{urllib.parse.quote(slug, safe='')}"


def _dashboard_event_group_key(market: dict[str, Any], *, start_ts: int) -> tuple[Any, ...]:
    category = normalize_category(str(market.get("category") or "")) or "esports"
    event_id = str(market.get("event_id") or market.get("eventId") or "").strip().lower()
    if event_id:
        return ("event_id", category, event_id)
    event_slug = str(market.get("event_slug") or market.get("eventSlug") or "").strip().lower()
    if event_slug:
        return ("event_slug", category, event_slug)
    title = str(market.get("title") or market.get("question") or "").strip().lower()
    league = normalize_league(market.get("league"))
    return ("title_start", category, league, title, start_ts)


def _dashboard_market_group_rank(market: dict[str, Any]) -> int:
    market_type = str(market.get("market_type") or "").strip()
    return {
        "main_match": 0,
        "moneyline": 0,
        "game_winner": 1,
        "map_winner": 1,
    }.get(market_type, 9)


def _dashboard_market_group_sequence(market: dict[str, Any]) -> int:
    market_type = str(market.get("market_type") or "").strip()
    if market_type in {"main_match", "moneyline"}:
        return 0
    text = " ".join(str(market.get(key) or "") for key in ("question", "title", "market_type_label"))
    match = re.search(r"\b(?:map|game)\s*(\d+)\b", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"(?:地图|第)\s*(\d+)", text)
    if match:
        return int(match.group(1))
    return 10_000


def _unique_nonempty(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _active_market_cache_rows(data_dir: Path, *, follow_dir: Path | None = None) -> tuple[list[dict[str, Any]], int, bool]:
    now_ts = int(time.time())
    store = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir))
    markets, updated_at, fresh = store.load_market_cache_readonly(
        cache_kind="active",
        now_ts=now_ts,
        ttl_seconds=15 * 60,
    )
    return list(markets.values()), updated_at, fresh


def build_events(
    data_dir: Path,
    *,
    observe_window_hours: float = 24.0,
    post_start_grace_seconds: int = 900,
    follow_dir: Path | None = None,
) -> dict[str, Any]:
    logo_cache = _load_team_logo_cache(data_dir)
    rows, updated_at, fresh = _active_market_cache_rows(data_dir, follow_dir=follow_dir)
    market_by_condition = {
        str(market.get("condition_id") or market.get("conditionId") or "").lower(): market
        for market in rows
        if isinstance(market, dict)
    }
    now_ts = int(time.time())
    window_end = now_ts + int(observe_window_hours * 3600)
    recent_start_cutoff = now_ts - max(0, int(post_start_grace_seconds))
    events = []
    follow_snapshot = FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir)).load_dashboard_snapshot()
    open_by_condition: dict[str, list[dict[str, Any]]] = {}
    for signal in follow_snapshot.get("open_signals", []):
        open_by_condition.setdefault(str(signal.get("condition_id") or "").lower(), []).append(signal)
    results_by_condition: dict[str, list[dict[str, Any]]] = {}
    for result in follow_snapshot.get("results", []):
        condition_id = str(result.get("condition_id") or "").lower()
        if condition_id:
            results_by_condition.setdefault(condition_id, []).append(result)
    grouped_markets: dict[tuple[Any, ...], list[tuple[dict[str, Any], str, int, list[dict[str, Any]], list[dict[str, Any]]]]] = {}
    for market in rows:
        if not isinstance(market, dict):
            continue
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        start_ts = _parse_timestamp(market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"))
        open_signals = open_by_condition.get(condition_id, [])
        results = results_by_condition.get(condition_id, [])
        if (start_ts and recent_start_cutoff <= start_ts <= window_end) or open_signals:
            grouped_markets.setdefault(_dashboard_event_group_key(market, start_ts=start_ts), []).append(
                (market, condition_id, start_ts, open_signals, results)
            )
    for group_rows in grouped_markets.values():
        ordered_group = sorted(
            group_rows,
            key=lambda row: (_dashboard_market_group_rank(row[0]), _dashboard_market_group_sequence(row[0]), row[2] or 0, row[1]),
        )
        market, condition_id, _start_ts, _open_signals, _results = ordered_group[0]
        open_signals = [signal for _market, _condition_id, _start, signals, _results in ordered_group for signal in signals]
        results = [result for _market, _condition_id, _start, _signals, group_results in ordered_group for result in group_results]
        market_types = _unique_nonempty(str(row_market.get("market_type") or "") for row_market, *_rest in ordered_group)
        market_type_labels = _unique_nonempty(str(row_market.get("market_type_label") or "") for row_market, *_rest in ordered_group)
        match_parts = _match_parts_for_row(market)
        category = normalize_category(str(market.get("category") or "")) or "esports"
        league = normalize_league(market.get("league"))
        row_league_label = str(market.get("league_label") or league_label(league)).strip()
        event_slug = _first_event_slug([row_market for row_market, *_rest in ordered_group])
        events.append(
            {
                "condition_id": condition_id,
                "event_slug": event_slug,
                "event_url": _polymarket_event_url(event_slug),
                "category": category,
                "league": league,
                "league_label": row_league_label,
                "game": league if category == "sports" else "",
                "game_label": row_league_label if category == "sports" else "",
                "title": market.get("title"),
                "question": market.get("question"),
                "match_parts": match_parts,
                "team_logos": _team_logos_for_parts(match_parts, logo_cache),
                "match_start_time": market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"),
                "end_date": market.get("end_date") or market.get("endDate"),
                "outcomes": market.get("outcomes"),
                "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
                "market_type": market.get("market_type"),
                "market_type_label": market.get("market_type_label") if len(ordered_group) == 1 else f"{len(ordered_group)}盘口",
                "market_count": len(ordered_group),
                "market_types": market_types,
                "market_type_labels": market_type_labels,
                "open_signal_count": len(open_signals),
                "result_count": len(results),
                "signal_count": len(open_signals) + len(results),
                "settled_count": sum(1 for result in results if result.get("status") == "settled"),
                "exited_count": sum(1 for result in results if result.get("status") == "exited"),
                "contested": _signals_contested([*open_signals, *results]),
                "side_counts": _signal_side_counts([*open_signals, *results]),
            }
        )
    events.sort(
        key=lambda row: (
            0 if row.get("open_signal_count") else 1 if row.get("result_count") else 2,
            _parse_timestamp(row.get("match_start_time")) or 0,
        )
    )
    archived_events = []
    for condition_id, results in results_by_condition.items():
        if not results:
            continue
        market = market_by_condition.get(condition_id, {})
        title = market.get("title") or next(
            (result.get("event_title") or result.get("title") or result.get("market_title") for result in results if isinstance(result, dict)),
            "",
        )
        question = market.get("question") or next(
            (result.get("market_question") or result.get("question") for result in results if isinstance(result, dict)),
            "",
        )
        match_start_time = (
            market.get("match_start_time")
            or market.get("market_start_time")
            or market.get("startTime")
            or next((result.get("match_start_time") or result.get("market_start_time") for result in results if isinstance(result, dict)), None)
        )
        end_date = market.get("end_date") or market.get("endDate") or next(
            (result.get("end_date") for result in results if isinstance(result, dict)),
            None,
        )
        category = normalize_category(str(market.get("category") or next((result.get("category") for result in results if isinstance(result, dict)), "") or "")) or "esports"
        league = normalize_league(market.get("league") or next((result.get("league") for result in results if isinstance(result, dict)), ""))
        row_league_label = str(market.get("league_label") or next((result.get("league_label") for result in results if isinstance(result, dict)), "") or league_label(league)).strip()
        market_type_label = market.get("market_type_label") or next((result.get("market_type_label") for result in results if isinstance(result, dict)), None)
        event_slug = _event_slug(market) or _first_event_slug([result for result in results if isinstance(result, dict)])
        match_parts = _match_parts_for_row(
            {
                "title": title,
                "question": question,
                "category": category,
                "league": league,
                "league_label": row_league_label,
                "market_type_label": market_type_label,
            }
        )
        archived_events.append(
            {
                "condition_id": condition_id,
                "event_slug": event_slug,
                "event_url": _polymarket_event_url(event_slug),
                "category": category,
                "league": league,
                "league_label": row_league_label,
                "game": league if category == "sports" else "",
                "game_label": row_league_label if category == "sports" else "",
                "title": title,
                "question": question,
                "match_parts": match_parts,
                "team_logos": _team_logos_for_parts(match_parts, logo_cache),
                "match_start_time": match_start_time,
                "end_date": end_date,
                "outcomes": market.get("outcomes"),
                "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
                "market_type": market.get("market_type") or next((result.get("market_type") for result in results if isinstance(result, dict)), None),
                "market_type_label": market_type_label,
                "open_signal_count": 0,
                "result_count": len(results),
                "signal_count": len(results),
                "settled_count": sum(1 for result in results if result.get("status") == "settled"),
                "exited_count": sum(1 for result in results if result.get("status") == "exited"),
                "contested": _signals_contested(results),
                "side_counts": _signal_side_counts(results),
                "our_realized_pnl": sum(_signal_our_pnl(result) for result in results),
                "wallet_basis_realized_pnl": sum(_signal_wallet_pnl(result) for result in results),
                "last_activity_at": max((_signal_activity_at(result) for result in results), default=0),
                "archived": True,
            }
        )
    archived_events.sort(key=lambda row: row.get("last_activity_at") or 0, reverse=True)
    return {
        "events": events,
        "archived_events": archived_events,
        "count": len(events),
        "archived_count": len(archived_events),
        "cache_updated_at": updated_at,
        "cache_stale": not fresh,
    }


def _load_team_logo_cache(data_dir: Path) -> dict[str, str]:
    static_path = Path(__file__).with_name("dashboardV2") / "logo" / "team_logos.json"
    raw = _read_json(static_path, {})
    if not isinstance(raw, dict):
        return {}
    teams = raw.get("teams") if isinstance(raw.get("teams"), dict) else raw
    logos: dict[str, str] = {}
    for key, value in teams.items():
        url = str(value or "").strip()
        if not url:
            continue
        normalized = _normalize_logo_key(str(key))
        if normalized:
            logos[normalized] = url
    return logos


def _team_logos_for_parts(parts: dict[str, str] | None, logo_cache: dict[str, str]) -> dict[str, str]:
    if not parts:
        return {}
    team_a = parts.get("teamA") or ""
    team_b = parts.get("teamB") or ""
    return {
        "teamA": _team_logo_url(logo_cache, parts.get("game") or "", team_a),
        "teamB": _team_logo_url(logo_cache, parts.get("game") or "", team_b),
    }


def _team_logo_url(logo_cache: dict[str, str], game: str, team: str) -> str:
    team_key = _normalize_logo_key(team)
    if not team_key:
        return ""
    game_key = _normalize_logo_key(game)
    candidates = [_normalize_logo_key(f"{game_key}:{team_key}") if game_key else "", team_key]
    return next((logo_cache[key] for key in candidates if key and key in logo_cache), "")


def _match_title_parts(title: str) -> dict[str, str] | None:
    match = _MATCH_TITLE_RE.match(str(title or ""))
    if not match:
        return None
    return {
        "game": match.group(1).strip(),
        "teamA": match.group(2).strip(),
        "teamB": match.group(3).strip(),
        "meta": f"{(match.group(4) or '').strip()} {match.group(5).strip()}".strip(),
    }


def _match_parts_for_row(row: dict[str, Any]) -> dict[str, str] | None:
    title = str(row.get("title") or row.get("question") or "")
    parts = _match_title_parts(title)
    if parts:
        return parts
    category = normalize_category(str(row.get("category") or ""))
    if category != "sports":
        return None
    match = _SPORTS_TITLE_RE.match(title)
    if not match:
        return None
    meta = str(row.get("market_type_label") or match.group(3) or "").strip()
    game = str(row.get("league_label") or league_label(row.get("league")) or "Sports").strip()
    return {
        "game": game,
        "teamA": match.group(1).strip(),
        "teamB": match.group(2).strip(),
        "meta": meta,
    }


def _normalize_logo_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _active_market_by_condition(data_dir: Path, condition_id: str, *, follow_dir: Path | None = None) -> dict[str, Any]:
    condition_id = str(condition_id or "").lower()
    return FollowStore(_follow_db_file(data_dir, follow_dir=follow_dir)).get_market_cache_item_readonly("active", condition_id)


def fetch_market_prices(data_dir: Path, client: Any, condition_id: str, *, follow_dir: Path | None = None) -> dict[str, Any]:
    """Return the runner-maintained SQLite snapshot without network or writes."""
    condition_id = condition_id.lower()
    market = _active_market_by_condition(data_dir, condition_id, follow_dir=follow_dir)
    if not market:
        raise RuntimeError("market_not_found")
    record = _market_price_record(market)
    if not record.get("outcomes") or not record.get("outcome_prices"):
        raise RuntimeError("market_prices_unavailable")
    return record


def _market_price_record(market: dict[str, Any]) -> dict[str, Any]:
    condition_id = str(market.get("conditionId") or market.get("condition_id") or "").lower()
    return {
        "condition_id": condition_id,
        "title": market.get("question") or market.get("title"),
        "question": market.get("question"),
        "event_slug": market.get("slug"),
        "outcomes": parse_jsonish(market.get("outcomes"), []),
        "outcome_prices": [to_float(value) for value in parse_jsonish(market.get("outcomePrices") or market.get("outcome_prices"), [])],
        "updated_at": int(time.time()),
    }


def _live_outcome_prices(client: Any, condition_ids: list[str]) -> dict[str, list[float]]:
    """批量拉盘口价(只读,不写库)。返回 {condition_id: [prices]};无 client → {}。
    先查未关闭盘的**实时价**;对查不到的(已结算/已归档,普通查询不返回)再用 closed=true
    兜底拿**结算价(1/0)**。列表与详情**共用**,保证两处同一盘同价,且结束盘也能正确显示。"""
    if client is None:
        return {}
    cids = list(dict.fromkeys(str(cid).lower() for cid in condition_ids if cid))
    if not cids:
        return {}
    out: dict[str, list[float]] = {}

    def _collect(markets: Any) -> None:
        for market_row in (markets or []):
            record = _market_price_record(market_row)
            cid = record.get("condition_id")
            if cid and record.get("outcome_prices"):
                out[cid] = record["outcome_prices"]

    try:
        _collect(client.gamma("/markets", condition_ids=cids, limit=max(1, len(cids))))
    except Exception:
        pass
    missing = [cid for cid in cids if cid not in out]
    if missing:
        try:
            _collect(client.markets_by_condition_ids(missing, limit=len(missing)))
        except Exception:
            pass
    return out


CATEGORIES = ("esports",)   # sports 已退役;esports 是唯一类目


def normalize_category(category: str | None) -> str:
    value = str(category or "").lower()
    return value if value in CATEGORIES else ""


def normalize_league(value: Any) -> str:
    league = str(value or "").strip().lower()
    return league if league in LEAGUE_LABELS else ""


def league_label(value: Any) -> str:
    league = normalize_league(value)
    return LEAGUE_LABELS.get(league, str(value or "").strip().upper() if value else "")


def category_data_dirs(root: Path) -> dict[str, Path]:
    root = Path(root)
    return {"esports": root / "esports"}


def _follow_dir(config_or_data_dir: DashboardConfig | Path) -> Path:
    if isinstance(config_or_data_dir, DashboardConfig):
        return config_or_data_dir.follow_dir or config_or_data_dir.data_dir / "follow"
    data_dir = Path(config_or_data_dir)
    return data_dir if data_dir.name == "follow" else data_dir / "follow"


def _follow_dir_from(data_dir: Path, *, follow_dir: Path | None = None) -> Path:
    return Path(follow_dir) if follow_dir is not None else _follow_dir(data_dir)


def _follow_db_path(config_or_data_dir: DashboardConfig | Path) -> Path:
    return _follow_dir(config_or_data_dir) / "follow.db"


def _follow_db_file(data_dir: Path, *, follow_dir: Path | None = None) -> Path:
    return _follow_dir_from(data_dir, follow_dir=follow_dir) / "follow.db"


def _leaderboard_db_file(data_dir: Path) -> Path:
    """Read the canonical DB, with a legacy filename fallback for upgrades."""
    canonical = Path(data_dir) / "leaderboard.db"
    legacy = Path(data_dir) / "leaderboard_v2.db"
    return canonical if canonical.exists() or not legacy.exists() else legacy


class WalletRefreshAlreadyRunning(RuntimeError):
    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__("wallet refresh already running")
        self.status = status


class RunnerAlreadyRunning(RuntimeError):
    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__("runner already running")
        self.status = status


class DataResetBlocked(RuntimeError):
    def __init__(self, reason: str, status: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status = status


def build_runner_status(config: DashboardConfig) -> dict[str, Any]:
    follow_dir = _follow_dir(config)
    control = read_follow_control(follow_dir)
    recorded = control.get("runner") if isinstance(control.get("runner"), dict) else {}
    defaults = _runner_default_params(config)
    recorded_pid = int(recorded.get("pid") or 0) if isinstance(recorded, dict) else 0
    if recorded_pid:
        _reap_child_process(recorded_pid)
    processes = _find_runner_processes(config)
    matched = next((row for row in processes if int(row.get("pid") or 0) == recorded_pid), None)
    if matched is None and processes:
        matched = processes[0]
    if matched:
        source = "dashboard" if int(matched.get("pid") or 0) == recorded_pid else "external"
        status = "stopping" if source == "dashboard" and recorded.get("status") == "stopping" else "running"
        return {
            "status": status,
            "pid": int(matched.get("pid") or 0),
            "pgid": int(matched.get("pgid") or 0),
            "source": source,
            "command": matched.get("command") or recorded.get("command"),
            "started_at": recorded.get("started_at") if source == "dashboard" else None,
            "stop_requested_at": recorded.get("stop_requested_at") if status == "stopping" else None,
            "stake_usdc": recorded.get("stake_usdc"),
            "stake_ratio_percent": recorded.get("stake_ratio_percent"),
            "max_stake_usdc": recorded.get("max_stake_usdc"),
            "max_signal_stake_balance_percent": recorded.get("max_signal_stake_balance_percent"),
            "strategy_configured": recorded.get("strategy_configured", defaults.get("strategy_configured")),
            "strategy_updated_at": recorded.get("strategy_updated_at", defaults.get("strategy_updated_at")),
            "strategy_summary": recorded.get("strategy_summary", defaults.get("strategy_summary")),
            "log_path": recorded.get("log_path") if source == "dashboard" else None,
            "realtime_refresh": bool(recorded.get("realtime_refresh")) if source == "dashboard" else False,
            "observe_live_running": bool(source == "dashboard" and _pid_alive(int(recorded.get("observe_live_pid") or 0))),
            "observe_live_pid": recorded.get("observe_live_pid") if source == "dashboard" else None,
            "observe_live_pgid": recorded.get("observe_live_pgid") if source == "dashboard" else None,
            "observe_live_log_path": recorded.get("observe_live_log_path") if source == "dashboard" else None,
            "data_dir": str(config.data_dir),
            "follow_dir": str(follow_dir),
        }
    if recorded:
        stopped = {
            **defaults,
            **recorded,
            "status": "stopped",
            "pid": recorded_pid or None,
            "data_dir": str(config.data_dir),
            "follow_dir": str(follow_dir),
        }
        if recorded.get("status") != "stopped":
            _update_runner_control(follow_dir, stopped)
        return stopped
    return {"status": "stopped", **defaults, "data_dir": str(config.data_dir), "follow_dir": str(follow_dir)}


def _runner_default_params(config: DashboardConfig) -> dict[str, Any]:
    strategy = FollowStore(_follow_db_path(config)).load_follow_strategy_readonly()
    strategy_configured = bool(strategy.get("configured"))
    return {
        "stake_usdc": to_float(config.runner_stake_usdc),
        "stake_ratio_percent": to_float(config.runner_stake_ratio_percent),
        "max_stake_usdc": to_float(config.runner_max_stake_usdc),
        "max_signal_stake_balance_percent": to_float(config.runner_max_signal_stake_balance_percent),
        "strategy_configured": strategy_configured,
        "strategy_updated_at": strategy.get("updated_at") if strategy_configured else None,
        "strategy_summary": strategy_summary(strategy) if strategy_configured else "",
    }


def _prune_old_logs(log_dir: Path, *, max_age_days: int = 7, pattern: str = "dashboard-*.out") -> None:
    """删除 log_dir 下 mtime 超过 max_age_days 的 spawn 日志(best-effort,绝不向 spawn 路径抛错)。

    runner/observe 日志名是 dashboard-<role>-<spawn_ts>.out:每次重启换一个新文件、永不原地轮转,
    不清理就无界堆积。在每次 runner spawn(新文件唯一出现的时机)调用即可封顶。"""
    cutoff = time.time() - max_age_days * 86400
    try:
        for f in log_dir.glob(pattern):
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
            except OSError:
                pass
    except OSError:
        pass


def _spawn_detached_process(command: list[str], log_path: Path, starter: Any) -> tuple[int, int]:
    """起一个分离的子进程,返回 (pid, pgid)。starter 给定时走注入(便于测试)。"""
    if starter is not None:
        process = starter(command, log_path)
        pid = int(getattr(process, "pid", process if isinstance(process, int) else 0) or 0)
        pgid = int(getattr(process, "pgid", 0) or 0)
        return pid, pgid
    with log_path.open("ab") as log_file:
        process = subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[1],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid = int(process.pid)
    return pid, _process_group_id(pid) or pid


def start_runner(
    config: DashboardConfig,
    *,
    stake_ratio_percent: float | None = None,
    max_stake_usdc: float | None = None,
    max_signal_stake_balance_percent: float | None = None,
) -> dict[str, Any]:
    current = build_runner_status(config)
    if current.get("status") == "running":
        raise RunnerAlreadyRunning(current)
    # 起新进程前先扫掉任何残留的 follow 进程(上一轮 run 崩了只剩 observe 孤儿、或控制文件 pid 已失配),
    # 否则新 runner 会与旧 sidecar 并存 → 重复 observe + 之后无从 stop。status!=running 才走到这,安全。
    _reap_follow_processes(_find_follow_processes(config))
    min_stake = to_float(config.runner_stake_usdc)
    if not math.isfinite(min_stake) or min_stake <= 0:
        raise ValueError("invalid_stake_usdc")
    now_ts = int(time.time())
    follow_dir = _follow_dir(config)
    follow_dir.mkdir(parents=True, exist_ok=True)
    strategy = FollowStore(follow_dir / "follow.db").load_follow_strategy()
    if not strategy.get("configured"):
        raise ValueError("follow_strategy_required")
    strategy_valid, strategy_errors = validate_follow_strategy(strategy)
    if not strategy_valid:
        raise ValueError("invalid_follow_strategy")
    # 实时刷新现在是策略字段(运行中不可改),启动时从生效策略读取。
    realtime_refresh = bool(strategy.get("realtime_refresh"))
    log_dir = _follow_log_dir(config.data_dir, log_dir=config.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    _prune_old_logs(log_dir)  # 删 >7 天的旧 spawn 日志(runner/observe),封顶总量
    log_path = log_dir / f"dashboard-runner-{now_ts}.out"
    command = [
        sys.executable,
        "-u",
        "-m",
        "poly_fight.cli",
        "--data-dir",
        str(config.data_dir),
        "--log-dir",
        str(log_dir.parent),
        "run",
        "--follow-dir",
        str(follow_dir),
        "--skip-initial-build",
        "--strategy-source",
        "db",
    ]
    if config.runner_process_starter is not None:
        process = config.runner_process_starter(command, log_path)
        pid = int(getattr(process, "pid", process if isinstance(process, int) else 0) or 0)
        pgid = int(getattr(process, "pgid", 0) or 0)
    else:
        with log_path.open("ab") as log_file:
            process = subprocess.Popen(
                command,
                cwd=Path(__file__).resolve().parents[1],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        pid = int(process.pid)
        pgid = _process_group_id(pid) or pid
    # 实时刷新:勾选则随 runner 起 observe-live 快循环(从活跃/未结算 watchlist 盘提前发现优质
    # 钱包并晋升,发布到 dashboard 读的同一个 esports/leaderboard.db)。
    observe_live_pid = 0
    observe_live_pgid = 0
    observe_live_log_path = ""
    if realtime_refresh:
        observe_data_dir = category_data_dirs(config.data_dir)["esports"]
        observe_live_command = [
            sys.executable, "-u", "-m", "poly_fight.cli",
            "--data-dir", str(observe_data_dir),
            "observe-live", "--category", "esports",
            "--loop-minutes", "60",
            "--defer-first-tick",
            "--follow-dir", str(follow_dir),
        ]
        observe_live_log = log_dir / f"dashboard-observe-live-{now_ts}.out"
        observe_live_pid, observe_live_pgid = _spawn_detached_process(
            observe_live_command, observe_live_log, config.runner_process_starter
        )
        observe_live_log_path = str(observe_live_log)
    status = {
        "status": "running",
        "source": "dashboard",
        "pid": pid,
        "pgid": pgid,
        "realtime_refresh": bool(realtime_refresh),
        "observe_live_pid": observe_live_pid or None,
        "observe_live_pgid": observe_live_pgid or None,
        "observe_live_log_path": observe_live_log_path or None,
        "started_at": now_ts,
        "command": command,
        "stake_usdc": min_stake,
        "stake_ratio_percent": to_float(config.runner_stake_ratio_percent),
        "max_stake_usdc": to_float(config.runner_max_stake_usdc),
        "max_signal_stake_balance_percent": to_float(config.runner_max_signal_stake_balance_percent),
        "strategy_configured": True,
        "strategy_updated_at": strategy.get("updated_at"),
        "strategy_summary": strategy_summary(strategy),
        "log_path": str(log_path),
        "data_dir": str(config.data_dir),
        "follow_dir": str(follow_dir),
    }
    _update_runner_control(follow_dir, status)
    return status


def stop_runner(config: DashboardConfig) -> dict[str, Any]:
    current = build_runner_status(config)
    # 按命令模式扫出所有 follow 进程(run + observe sidecar,含控制文件没记的孤儿)。
    follow_processes = _find_follow_processes(config)
    pid = int(current.get("pid") or 0)
    if not pid and not follow_processes:
        return {"status": "stopped", "data_dir": str(config.data_dir), "follow_dir": str(_follow_dir(config))}
    # 给注入的 stopper 也看到扫描结果(便于测试 + 自定义停法覆盖全集)。
    stop_target = {**current, "follow_processes": follow_processes}
    if config.runner_process_stopper is not None:
        config.runner_process_stopper(stop_target)
    else:
        _terminate_runner_process(current)
        _terminate_observe_process(current)   # 控制文件记录的 observe-live sidecar
        _reap_follow_processes(follow_processes)  # 兜底:杀掉控制文件没记的孤儿(run/observe)
    status = {
        **current,
        "status": "stopping",
        "stop_requested_at": int(time.time()),
    }
    _update_runner_control(_follow_dir(config), status)
    return {**status, "reaped_pids": [int(row.get("pid") or 0) for row in follow_processes]}


def reset_dashboard_data(config: DashboardConfig) -> dict[str, Any]:
    runner = build_runner_status(config)
    if runner.get("status") == "running":
        raise DataResetBlocked("runner_running", runner)

    follow_dir = _follow_dir(config)
    control = read_follow_control(follow_dir)
    wallet_refresh = control.get("wallet_refresh") if isinstance(control.get("wallet_refresh"), dict) else {}
    running_refresh = {
        category: status
        for category, status in wallet_refresh.items()
        if isinstance(status, dict) and status.get("status") == "running"
    }
    if running_refresh:
        raise DataResetBlocked("wallet_refresh_running", {"wallet_refresh": running_refresh})

    targets = [*category_data_dirs(config.data_dir).values(), follow_dir, _follow_log_dir(config.data_dir, log_dir=config.log_dir)]
    removed: list[str] = []
    for target in targets:
        target = Path(target)
        if target.exists():
            shutil.rmtree(target)
            removed.append(str(target))
        target.mkdir(parents=True, exist_ok=True)

    return {
        "status": "reset",
        "reset_at": int(time.time()),
        "data_dir": str(config.data_dir),
        "follow_dir": str(follow_dir),
        "removed": removed,
    }


def _update_runner_control(data_dir: Path, status: dict[str, Any]) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    control["runner"] = status
    write_follow_control(data_dir, control)
    return control


def _find_runner_processes(config: DashboardConfig) -> list[dict[str, Any]]:
    if config.runner_process_lister is not None:
        rows = config.runner_process_lister()
    else:
        rows = _system_processes()
    data_dir = config.data_dir
    candidates = [_normalize_process_row(row) for row in rows]
    return [row for row in candidates if _process_matches_runner(row, data_dir)]


def _is_poly_fight_follow_command(tokens: list[str], command: str) -> bool:
    """匹配 follow 全家:run 主进程 + observe-live sidecar。"""
    has_cli = (
        "poly_fight.cli" in tokens
        or any(token.endswith("poly_fight/cli.py") or token.endswith("poly_fight\\cli.py") for token in tokens)
        or "poly_fight.cli" in command
    )
    return has_cli and any(sub in tokens for sub in ("run", "observe-live"))


def _follow_data_dir_match_values(data_dir: Path) -> set[str]:
    """root data_dir + 各 category 子目录(observe 用 <root>/esports 起,run 用 root)。"""
    values = {str(data_dir), str(data_dir.resolve())}
    for sub in category_data_dirs(data_dir).values():
        values.add(str(sub))
        values.add(str(Path(sub).resolve()))
    return values


def _process_matches_follow(row: dict[str, Any], data_dir: Path) -> bool:
    command = str(row.get("command") or "")
    stat = str(row.get("stat") or "").upper()
    pid = int(row.get("pid") or 0)
    if stat.startswith("Z") or "<defunct>" in command:
        return False
    tokens = _command_tokens(command)
    if pid <= 0 or not _is_poly_fight_follow_command(tokens, command):
        return False
    data_dir_values = _data_dir_values_from_command(tokens)
    if not data_dir_values:
        return _runner_default_data_dir_matches(data_dir)
    expected = _follow_data_dir_match_values(data_dir)
    return any(value in expected or str(Path(value).resolve()) in expected for value in data_dir_values)


def _find_follow_processes(config: DashboardConfig) -> list[dict[str, Any]]:
    """扫描进程表里**所有** follow 进程(run + observe sidecar),按命令模式匹配,
    **不依赖控制文件 pid** —— 这样重启覆盖了控制文件、或 run 崩了只剩 observe 孤儿,也能找全。"""
    if config.runner_process_lister is not None:
        rows = config.runner_process_lister()
    else:
        rows = _system_processes()
    candidates = [_normalize_process_row(row) for row in rows]
    return [row for row in candidates if _process_matches_follow(row, config.data_dir)]


def _reap_follow_processes(processes: list[dict[str, Any]]) -> list[int]:
    """杀掉给定 follow 进程(run + observe,含控制文件没记的孤儿)。返回处理的 pid。"""
    reaped: list[int] = []
    for row in processes:
        pid = int(row.get("pid") or 0)
        if pid <= 0:
            continue
        _terminate_pgid_or_pid(int(row.get("pgid") or 0), pid)
        reaped.append(pid)
    return reaped


def _adopted_follow_pids(config: DashboardConfig) -> set[int]:
    """控制文件里记录的、仍合法运行的 runner + observe sidecar pid —— serve 重启后这些进程
    pid 不变,留着让 ``build_runner_status`` 自动接管(source=dashboard,停止/日志/策略续上),
    不当孤儿误杀。仅当 runner 状态为 running/stopping 时保留;否则回到"全清"的干净起点语义。"""
    recorded = read_follow_control(_follow_dir(config)).get("runner")
    if not isinstance(recorded, dict) or recorded.get("status") not in {"running", "stopping"}:
        return set()
    return {
        pid
        for pid in (int(recorded.get("pid") or 0), int(recorded.get("observe_live_pid") or 0))
        if pid > 0
    }


def reap_orphan_follow_processes(config: DashboardConfig) -> list[int]:
    """serve 启动时调用:清掉上一代遗留的 follow 孤儿,但**保留并接管**仍在合法运行的 runner
    (及其 observe sidecar)。

    unit ``KillMode=process`` + spawn 时 ``start_new_session=True`` 让 run/observe 活过 serve
    重启;它们的 pid/pgid 落在控制文件(磁盘持久),pid 不变 → ``build_runner_status`` 按命令
    扫描 + 控制文件 pid 比对即可自动接管(停止按钮、日志、策略全续上)。所以这里只杀**真正的
    孤儿**(pid 跟控制文件对不上的,例如 run 崩了只剩的 observe sidecar);留下合法 runner ——
    "忘了停 runner 就重启 dashboard" 不再让它被误杀空跑。返回杀掉的 pid。"""
    keep = _adopted_follow_pids(config)
    orphans = [row for row in _find_follow_processes(config) if int(row.get("pid") or 0) not in keep]
    return _reap_follow_processes(orphans)


def _system_processes() -> list[dict[str, Any]]:
    for command in (
        ["ps", "-axo", "pid=,ppid=,pgid=,stat=,command="],
        ["ps", "-eo", "pid=,ppid=,pgid=,stat=,command="],
        ["ps", "-axo", "pid=,ppid=,pgid=,command="],
        ["ps", "-eo", "pid=,ppid=,pgid=,command="],
    ):
        try:
            result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        except OSError:
            continue
        if result.returncode == 0:
            return [_parse_ps_line(line) for line in result.stdout.splitlines()]
    return []


def _parse_ps_line(line: str) -> dict[str, Any]:
    parts = line.strip().split(None, 4)
    if len(parts) < 4:
        return {}
    if len(parts) >= 5:
        return {
            "pid": _safe_int(parts[0]),
            "ppid": _safe_int(parts[1]),
            "pgid": _safe_int(parts[2]),
            "stat": parts[3],
            "command": parts[4],
        }
    return {
        "pid": _safe_int(parts[0]),
        "ppid": _safe_int(parts[1]),
        "pgid": _safe_int(parts[2]),
        "command": parts[3],
    }


def _normalize_process_row(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    return {
        "pid": _safe_int(row.get("pid")),
        "ppid": _safe_int(row.get("ppid")),
        "pgid": _safe_int(row.get("pgid")),
        "stat": str(row.get("stat") or ""),
        "command": str(row.get("command") or ""),
    }


def _process_matches_runner(row: dict[str, Any], data_dir: Path) -> bool:
    command = str(row.get("command") or "")
    stat = str(row.get("stat") or "").upper()
    pid = int(row.get("pid") or 0)
    if stat.startswith("Z") or "<defunct>" in command:
        return False
    tokens = _command_tokens(command)
    if pid <= 0 or not _is_poly_fight_run_command(tokens, command):
        return False
    data_dir_values = _data_dir_values_from_command(tokens)
    if not data_dir_values:
        return _runner_default_data_dir_matches(data_dir)
    expected = _data_dir_match_values(data_dir)
    return any(value in expected or str(Path(value).resolve()) in expected for value in data_dir_values)


def _command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def _is_poly_fight_run_command(tokens: list[str], command: str) -> bool:
    if "run" not in tokens:
        return False
    if "poly_fight.cli" in tokens:
        return True
    if any(token.endswith("poly_fight/cli.py") or token.endswith("poly_fight\\cli.py") for token in tokens):
        return True
    return "poly_fight.cli" in command


def _runner_default_data_dir_matches(data_dir: Path) -> bool:
    # A `run`/`collect` started without --data-dir resolves to data/esports (the esports
    # default after the per-category split); keep matching the legacy bare "data" too.
    data_dir = Path(data_dir)
    candidates = {"data", "data/esports"}
    if str(data_dir) in candidates:
        return True
    resolved = str(data_dir.resolve())
    return any(resolved == str(Path(c).resolve()) for c in candidates)


def _data_dir_match_values(data_dir: Path) -> set[str]:
    return {str(data_dir), str(data_dir.resolve())}


def _data_dir_values_from_command(tokens: list[str]) -> list[str]:
    values: list[str] = []
    for idx, token in enumerate(tokens):
        if token == "--data-dir" and idx + 1 < len(tokens):
            values.append(tokens[idx + 1])
        elif token.startswith("--data-dir="):
            values.append(token.split("=", 1)[1])
    return values


def _terminate_runner_process(status: dict[str, Any]) -> None:
    pid = int(status.get("pid") or 0)
    pgid = int(status.get("pgid") or 0)
    source = status.get("source")
    if source == "dashboard" and pgid:
        try:
            os.killpg(pgid, signal.SIGTERM)
            return
        except ProcessLookupError:
            return
        except OSError:
            pass
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return


def _terminate_pgid_or_pid(pgid: int, pid: int) -> None:
    """best-effort 杀进程组(失败回退单 pid),进程不在直接忽略。"""
    if pgid:
        try:
            os.killpg(pgid, signal.SIGTERM)
            return
        except ProcessLookupError:
            return
        except OSError:
            pass
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return


def _terminate_observe_process(status: dict[str, Any]) -> None:
    """停掉实时刷新挂的 observe-live sidecar 子进程(best-effort)。"""
    _terminate_pgid_or_pid(int(status.get("observe_live_pgid") or 0), int(status.get("observe_live_pid") or 0))


def _reap_child_process(pid: int) -> None:
    if pid <= 0:
        return
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return
    except OSError:
        return


def _process_group_id(pid: int) -> int:
    try:
        return int(os.getpgid(pid))
    except OSError:
        return 0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def build_wallet_refresh_status(data_dir: Path, *, follow_dir: Path | None = None) -> dict[str, Any]:
    target = follow_dir or data_dir
    reconcile_wallet_refresh_status(target)   # 读时自愈:属主(serve)已死的 running → failed,按钮恢复
    control = read_follow_control(target)
    status = control.get("wallet_refresh") if isinstance(control.get("wallet_refresh"), dict) else {}
    return {
        "status": status or {"status": "idle"},
    }


def v2_refresh_extra_args(form: dict[str, Any]) -> list[str]:
    """采集不再接受前端阈值参数:门槛(胜率/edge/n_eff/价格带)全在 core.py 评分常量里。
    旧的 --v2-min-positive-rate / --v2-max-median-entry 已从 CLI 删除,传入会让 collect-v2
    报 unrecognized arguments 而崩,因此这里恒返回空。"""
    return []


def _wipe_collector_data(category_dir: Path) -> None:
    """完整重采:清空该类目采集目录(profiles / leaderboard.db / 交易缓存 collector_v2 / 校准),
    从 0 重建。**保留 follow.db** —— 它在独立的 follow_dir(data/follow),不在 category_dir 下。"""
    category_dir = Path(category_dir)
    if category_dir.exists():
        for child in category_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except OSError:
                    pass
    category_dir.mkdir(parents=True, exist_ok=True)


def start_wallet_refresh(
    data_dir: Path,
    *,
    category: str = "esports",
    follow_dir: Path | None = None,
    runner: Any = None,
    timeout_seconds: int = 7200,
    extra_args: list[str] | None = None,
    full_recollect: bool = False,
    max_profile_wallets: int = V2_DEFAULT_MAX_PROFILE_WALLETS,
) -> dict[str, Any]:
    category = normalize_category(category)
    if not category:
        raise ValueError("invalid category")
    now_ts = int(time.time())
    root = Path(data_dir)
    category_dir = category_data_dirs(root)[category]
    follow_dir = follow_dir or root / "follow"
    control = read_follow_control(follow_dir)
    wallet_refresh = control.get("wallet_refresh") if isinstance(control.get("wallet_refresh"), dict) else {}
    existing = wallet_refresh.get(category) if isinstance(wallet_refresh.get(category), dict) else {}
    if isinstance(existing, dict) and existing.get("status") == "running":
        started_at = int(existing.get("started_at") or 0)
        if not started_at or now_ts - started_at < timeout_seconds:
            raise WalletRefreshAlreadyRunning(existing)

    follow_dir.mkdir(parents=True, exist_ok=True)
    log_path = follow_dir / f"wallet-refresh-{category}-{now_ts}.out"
    # v2 是唯一管线:刷新走 collect-v2(esports;sports 已退役)。可透传胜率/买入价阈值。
    base = [sys.executable, "-u", "-m", "poly_fight.cli", "--data-dir", str(category_dir)]
    max_profile_wallets = max(
        1,
        min(20000, int(max_profile_wallets or V2_DEFAULT_MAX_PROFILE_WALLETS)),
    )  # 钳到 [1, 20000]
    command = [
        *base, "collect-v2", "--category", category,
        # 注:--refresh-classification 已在 commit b6794a4 从 collect-v2 parser 删除(死开关),
        # 此处遗留未同步 → 点"重采"时 argparse 报 unrecognized 秒退。已移除。
        "--max-profile-wallets", str(max_profile_wallets),  # 由 dashboard 输入
        *(extra_args or []),
    ]
    status = {
        "status": "running",
        "category": category,
        "started_at": now_ts,
        "command": command,
        "full_recollect": bool(full_recollect),
        "max_profile_wallets": max_profile_wallets,
        "log_path": str(log_path),
        "owner_pid": os.getpid(),   # 监控线程所在的 serve;serve 死亡 → 孤儿 → 读时自愈为 failed
    }
    set_pause_new_signals(
        follow_dir,
        category,
        {"status": "paused", "reason": "wallet_refresh", "started_at": now_ts},
    )
    update_wallet_refresh_status(follow_dir, {**wallet_refresh, category: status})

    def worker() -> None:
        finished_at = int(time.time())
        try:
            if full_recollect:
                _wipe_collector_data(category_dir)  # 清采集库从 0 重建(保留 follow.db)
            prepare_category_refresh_dir(category_dir, max_lookback_days=30, now_ts=int(time.time()))
            if runner is not None:
                try:
                    returncode = int(runner(category, category_dir, log_path) or 0)
                except TypeError:
                    returncode = int(runner(category_dir, log_path) or 0)
            else:
                with log_path.open("ab") as log_file:
                    result = subprocess.run(command, cwd=Path(__file__).resolve().parents[1], stdout=log_file, stderr=subprocess.STDOUT, check=False)
                    returncode = int(result.returncode)
            finished_at = int(time.time())
            current = read_follow_control(follow_dir).get("wallet_refresh")
            current = current if isinstance(current, dict) else {}
            update_wallet_refresh_status(
                follow_dir,
                {**current, category: {**status, "status": "succeeded" if returncode == 0 else "failed", "finished_at": finished_at, "returncode": returncode}},
            )
        except Exception as exc:
            finished_at = int(time.time())
            current = read_follow_control(follow_dir).get("wallet_refresh")
            current = current if isinstance(current, dict) else {}
            update_wallet_refresh_status(
                follow_dir,
                {**current, category: {**status, "status": "failed", "finished_at": finished_at, "error": str(exc)}},
            )
        finally:
            set_pause_new_signals(follow_dir, category, None)

    thread = threading.Thread(target=worker, name="poly-fight-wallet-refresh", daemon=True)
    thread.start()
    return status


def _follow_groups_from_signals(signals: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for signal in signals:
        condition_id = str(signal.get("condition_id") or "").lower()
        if not condition_id:
            continue
        bucket = groups.setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "title": signal.get("event_title") or signal.get("title") or signal.get("market_title"),
                "question": signal.get("market_question") or signal.get("question"),
                "match_start_time": signal.get("match_start_time") or signal.get("market_start_time"),
                "end_date": signal.get("end_date"),
                "market_type": signal.get("market_type"),
                "market_type_label": signal.get("market_type_label"),
                "category": _signal_category(signal),
                "wallets": set(),
                "leg_count": 0,
                "side_counts": {},
                "stake": 0.0,
                "signal_stakes": [],
                "stake_mode_counts": {},
                "status_counts": {},
                "funded_open_leg_count": 0,
                "insufficient_balance_leg_count": 0,
                "settlement_type_counts": {},
                "settled_by_price_count": 0,
                "open_pnl_legs": [],
                "our_realized_pnl": 0.0,
                "wallet_basis_realized_pnl": 0.0,
                "last_activity_at": 0,
                "last_follow_action_at": 0,
                "contested_signal_count": 0,
                "two_sided_signal_count": 0,
                "disagreement_signal_count": 0,
                "quality_label": "one_way",
                "quality_signals": [],
                "clv_sum": 0.0,
                "clv_count": 0,
                "_exit_stake": 0.0,
                "_weighted_exit": 0.0,
            },
        )
        bucket["match_start_time"] = bucket.get("match_start_time") or signal.get("match_start_time") or signal.get("market_start_time")
        bucket["end_date"] = bucket.get("end_date") or signal.get("end_date")
        bucket["market_type"] = bucket.get("market_type") or signal.get("market_type")
        bucket["market_type_label"] = bucket.get("market_type_label") or signal.get("market_type_label")
        bucket["category"] = bucket.get("category") or _signal_category(signal)
        wallet = str(signal.get("wallet") or "").lower()
        if wallet:
            bucket["wallets"].add(wallet)
        bucket["quality_signals"].append(signal)
        legs = signal.get("legs") or []
        bucket["leg_count"] += len(legs)
        # 我们买入哪一边:按 outcome 名聚合(对手盘/自对冲时一场会出现两个 outcome)。
        outcome_name = signal.get("outcome")
        if outcome_name in (None, ""):
            _oi = signal.get("outcome_index")
            outcome_name = str(_oi) if _oi not in (None, "") else "unknown"
        side = bucket["side_counts"].setdefault(
            str(outcome_name),
            {"outcome": str(outcome_name), "outcome_index": int(signal.get("outcome_index") or 0),
             "signal_count": 0, "leg_count": 0},
        )
        side["signal_count"] += 1
        side["leg_count"] += len(legs)
        entry_summary = _signal_follow_entry_summary(signal)
        bucket["stake"] += entry_summary["follow_total_stake"]
        signal_stake = entry_summary["follow_total_stake"]
        if signal_stake > 0:
            bucket["signal_stakes"].append(signal_stake)
        mode = str(signal.get("stake_mode") or signal.get("conviction_tier") or "fixed")
        bucket["stake_mode_counts"][mode] = bucket["stake_mode_counts"].get(mode, 0) + 1
        status = str(signal.get("status") or "open")
        bucket["status_counts"][status] = bucket["status_counts"].get(status, 0) + 1
        settlement_type = _signal_settlement_type(signal)
        if settlement_type:
            bucket["settlement_type_counts"][settlement_type] = bucket["settlement_type_counts"].get(settlement_type, 0) + 1
        # 价格隐含结算(盘口未 closed、靠实时价≈1.0 提前结)的审计标记,聚合到整场。
        if signal.get("settled_by_price"):
            bucket["settled_by_price_count"] += 1
        if status == "open":
            outcome_index = int(signal.get("outcome_index") or 0)
            for leg in legs:
                if isinstance(leg, dict):
                    if _leg_actual_stake(leg) > 0:
                        bucket["funded_open_leg_count"] += 1
                    elif str(leg.get("funding_status") or "") == "insufficient_balance":
                        bucket["insufficient_balance_leg_count"] += 1
                    bucket["open_pnl_legs"].append(
                        {
                            "outcome_index": outcome_index,
                            "stake": _leg_actual_stake(leg),
                            "our_entry_price": _to_float(leg.get("our_entry_price")),
                            "would_follow": leg.get("would_follow", True),
                        }
                    )
        bucket["our_realized_pnl"] += _signal_our_pnl(signal)
        bucket["wallet_basis_realized_pnl"] += _signal_wallet_pnl(signal)
        _exit_price = _signal_follow_exit_price(signal)
        _exit_stake = sum(
            to_float(record.get("sold_stake"))
            for record in (signal.get("partial_exits") or [])
            if isinstance(record, dict)
        )
        if _exit_price is not None and _exit_stake > 0:
            bucket["_exit_stake"] += _exit_stake
            bucket["_weighted_exit"] += _exit_price * _exit_stake
        if signal.get("wallet_clv") is not None:
            bucket["clv_sum"] += _to_float(signal.get("wallet_clv"))
            bucket["clv_count"] += 1
        bucket["last_activity_at"] = max(
            int(bucket["last_activity_at"] or 0),
            int(signal.get("updated_at") or signal.get("settled_at") or signal.get("exit_at") or signal.get("created_at") or 0),
        )
        bucket["last_follow_action_at"] = max(
            int(bucket["last_follow_action_at"] or 0),
            _signal_follow_action_at(signal),
        )
    for bucket in groups.values():
        bucket["wallet_count"] = len(bucket["wallets"])
        bucket["wallets"] = sorted(bucket["wallets"])
        if bucket["status_counts"].get("open"):
            bucket["status"] = (
                "insufficient_balance"
                if bucket.get("funded_open_leg_count", 0) <= 0 and bucket.get("insufficient_balance_leg_count", 0) > 0
                else "open"
            )
        else:
            bucket["status"] = "settled"
        # 一个 bucket(同一 conditionId,多钱包/双边)可能混多种结算口径:stop_loss(止损)/
        # manual_exit(镜像提前卖)/auto_settlement(持到结算)。纯一种 → 该种;混合 → 部分卖出。
        settlement_types = set(bucket.get("settlement_type_counts") or {}) - {""}
        if len(settlement_types) == 1:
            bucket["settlement_type"] = next(iter(settlement_types))
        elif settlement_types:
            bucket["settlement_type"] = "auto_and_manual"
        else:
            bucket["settlement_type"] = ""
        bucket["settled_by_price"] = bool(bucket.pop("settled_by_price_count", 0))
        _exit_stake = to_float(bucket.pop("_exit_stake", 0.0))
        _weighted_exit = to_float(bucket.pop("_weighted_exit", 0.0))
        bucket["follow_exit_price"] = round(_weighted_exit / _exit_stake, 8) if _exit_stake > 0 else None
        bucket["roi"] = bucket["our_realized_pnl"] / bucket["stake"] if bucket["stake"] else None
        bucket["avg_wallet_clv"] = bucket["clv_sum"] / bucket["clv_count"] if bucket["clv_count"] else None
        quality_flags = _signal_quality_flags(bucket.get("quality_signals") or [])
        bucket["quality_two_sided"] = quality_flags["two_sided"]
        bucket["quality_disagreement"] = quality_flags["disagreement"]
        bucket["quality_label"] = _signal_quality_label(quality_flags)
        signal_count = len(bucket.get("quality_signals") or [])
        bucket["two_sided_signal_count"] = signal_count if quality_flags["two_sided"] else 0
        bucket["disagreement_signal_count"] = signal_count if quality_flags["disagreement"] else 0
        bucket["contested_signal_count"] = bucket["disagreement_signal_count"]
        signal_stakes = [value for value in bucket.get("signal_stakes") or [] if value > 0]
        bucket["signal_stake_min"] = min(signal_stakes) if signal_stakes else None
        bucket["signal_stake_max"] = max(signal_stakes) if signal_stakes else None
        bucket["sides"] = sorted(
            bucket.pop("side_counts").values(), key=lambda s: s.get("outcome_index", 0)
        )
        bucket.pop("signal_stakes", None)
        bucket.pop("quality_signals", None)
        bucket.pop("funded_open_leg_count", None)
        bucket.pop("insufficient_balance_leg_count", None)
    return groups


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _file_mtime(path: Path) -> int:
    try:
        return int(path.stat().st_mtime)
    except OSError:
        return 0


def _latest_scoring_version(data_dir: Path) -> int | None:
    versions: list[int] = []
    for category, category_dir in category_data_dirs(data_dir).items():
        version = LeaderboardStore(_leaderboard_db_file(category_dir)).latest_scoring_version(category=category)
        if version:
            versions.append(int(version))
    legacy_version = LeaderboardStore(_leaderboard_db_file(data_dir)).latest_scoring_version(category="esports")
    if legacy_version:
        versions.append(int(legacy_version))
    if versions:
        return max(versions)
    leaderboard = [row for _category, _dir, rows, _mtime in _category_leaderboards(data_dir) for row in rows]
    versions = [int(row.get("scoring_version") or 0) for row in leaderboard if isinstance(row, dict)]
    version = max(versions or [0])
    return version or None


def _signals_contested(signals: list[dict[str, Any]]) -> bool:
    return _signal_quality_flags(signals)["disagreement"]


def _signal_side_counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for signal in signals:
        side = _signal_side(signal) or "unknown"
        counts[side] = counts.get(side, 0) + 1
    return counts


def _signal_side(signal: dict[str, Any]) -> str:
    outcome = signal.get("outcome")
    if outcome not in (None, ""):
        return str(outcome)
    outcome_index = signal.get("outcome_index")
    if outcome_index not in (None, ""):
        return str(outcome_index)
    return ""


def _default_log_dir(data_dir: Path) -> Path:
    data_dir = Path(data_dir)
    if data_dir.name == "data":
        return data_dir.parent / "logs"
    return data_dir / "logs"


def _follow_log_dir(data_dir: Path, *, log_dir: Path | None = None) -> Path:
    return (Path(log_dir) if log_dir else _default_log_dir(data_dir)) / "follow"


def _cookie_value(raw: str, key: str) -> str:
    for part in raw.split(";"):
        if "=" not in part:
            continue
        name, value = part.strip().split("=", 1)
        if name == key:
            return value
    return ""


def _int_param(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _leg_actual_stake(leg: dict[str, Any]) -> float:
    if not isinstance(leg, dict):
        return 0.0
    if leg.get("funded_stake") is not None:
        return max(0.0, _to_float(leg.get("funded_stake")))
    if leg.get("would_follow") is False:
        return 0.0
    return max(0.0, _to_float(leg.get("stake")))


def _leg_hypothetical_stake(leg: dict[str, Any]) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return max(0.0, _to_float(leg.get("stake")))


def _parse_timestamp(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    parsed = parse_dt(str(value))
    return int(parsed.timestamp()) if parsed else 0


def _trade_condition_id(trade: dict[str, Any]) -> str:
    for key in ("conditionId", "condition_id", "market", "marketConditionId"):
        value = trade.get(key)
        if value:
            return str(value).lower()
    return ""


def _result_win(row: dict[str, Any]) -> bool:
    if row.get("outcome_won") is not None:
        return bool(row.get("outcome_won"))
    if row.get("won") is not None:
        return bool(row.get("won"))
    if row.get("our_realized_pnl") is not None or row.get("our_paper_pnl") is not None:
        return _signal_our_pnl(row) > 0
    return False


def _signal_our_pnl(row: dict[str, Any]) -> float:
    status = str(row.get("status") or "")
    if status == "settled":
        if row.get("our_paper_pnl") is not None:
            return _to_float(row.get("our_paper_pnl"))
        return _to_float(row.get("our_realized_pnl"))
    if status == "exited":
        if row.get("our_realized_pnl") is not None:
            return _to_float(row.get("our_realized_pnl"))
        return _to_float(row.get("our_paper_pnl"))
    if row.get("our_realized_pnl") is not None:
        return _to_float(row.get("our_realized_pnl"))
    return _to_float(row.get("our_paper_pnl"))


def _signal_hypothetical_pnl(row: dict[str, Any]) -> float:
    if row.get("hypothetical_pnl") is not None:
        return _to_float(row.get("hypothetical_pnl"))
    return _signal_our_pnl(row)


def _signal_wallet_pnl(row: dict[str, Any]) -> float:
    if row.get("wallet_basis_realized_pnl") is not None:
        return _to_float(row.get("wallet_basis_realized_pnl"))
    by_wallet = row.get("wallet_paper_pnl_by_wallet")
    if isinstance(by_wallet, dict):
        return sum(_to_float(value) for value in by_wallet.values())
    return _to_float(row.get("wallet_realized_pnl"))


def _behavior_counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for signal in signals:
        behavior = signal.get("wallet_behavior") or {}
        for key in ("exited", "hedged", "held_to_resolution"):
            if behavior.get(key):
                counts[key] = counts.get(key, 0) + 1
        for event in signal.get("behavior_events") or []:
            kind = str(event.get("kind") or "")
            if kind:
                counts[kind] = counts.get(kind, 0) + 1
    return counts
