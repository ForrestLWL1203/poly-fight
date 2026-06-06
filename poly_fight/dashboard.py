from __future__ import annotations

import base64
import hashlib
import hmac
import json
import mimetypes
import os
import re
import signal
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

from .control import read_follow_control, update_wallet_refresh_status, write_follow_control
from .storage import FollowStore


COOKIE_NAME = "poly_fight_session"
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
FAILED_LOGINS: dict[str, list[float]] = {}
_TRADES_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_TRADES_CACHE_LOCK = threading.Lock()


@dataclass(frozen=True)
class DashboardConfig:
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 8787
    username: str = "admin"
    password: str = ""
    cookie_secret: str = ""
    session_ttl_seconds: int = 12 * 3600
    cookie_secure: bool = False
    static_dir: Path | None = None
    client: Any = None
    trades_cache_ttl_seconds: int = 30
    observe_window_hours: float = 24.0
    wallet_refresh_runner: Any = None
    wallet_refresh_timeout_seconds: int = 7200
    runner_stake_usdc: float = 1.0
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
    static_dir = config.static_dir or Path(__file__).with_name("dashboard").joinpath("static")
    resolved = DashboardConfig(
        data_dir=config.data_dir,
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        cookie_secret=config.cookie_secret,
        session_ttl_seconds=config.session_ttl_seconds,
        cookie_secure=config.cookie_secure,
        static_dir=static_dir,
        client=config.client,
        trades_cache_ttl_seconds=config.trades_cache_ttl_seconds,
        observe_window_hours=config.observe_window_hours,
        wallet_refresh_runner=config.wallet_refresh_runner,
        wallet_refresh_timeout_seconds=config.wallet_refresh_timeout_seconds,
        runner_stake_usdc=config.runner_stake_usdc,
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
            if parsed.path == "/api/runner/start":
                self._runner_start()
                return
            if parsed.path == "/api/runner/stop":
                self._runner_stop()
                return
        self._error("not_found", status=HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/api/stream":
            self._serve_stream()
            return
        if parsed.path == "/api/health":
            self._ok(build_health(self.dashboard_config.data_dir, started_at=self.started_at))
            return
        if parsed.path == "/api/overview":
            self._ok(build_overview(self.dashboard_config.data_dir))
            return
        if parsed.path == "/api/wallets":
            self._ok(build_wallets(self.dashboard_config.data_dir))
            return
        if parsed.path == "/api/follows":
            query = urllib.parse.parse_qs(parsed.query)
            page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
            size = _int_param(query.get("size", ["25"])[0], default=25, minimum=1, maximum=200)
            self._ok(build_follows(self.dashboard_config.data_dir, page=page, size=size))
            return
        if parsed.path.startswith("/api/follows/"):
            condition_id = urllib.parse.unquote(parsed.path.rsplit("/", 1)[-1]).lower()
            self._ok(build_follow_detail(self.dashboard_config.data_dir, condition_id))
            return
        if parsed.path == "/api/events":
            self._ok(build_events(self.dashboard_config.data_dir, observe_window_hours=self.dashboard_config.observe_window_hours))
            return
        if parsed.path == "/api/wallet-refresh":
            self._ok(build_wallet_refresh_status(self.dashboard_config.data_dir))
            return
        if parsed.path == "/api/runner":
            self._ok(build_runner_status(self.dashboard_config))
            return
        match = re.match(r"^/api/wallets/([^/]+)/trades$", parsed.path)
        if match:
            self._wallet_trades(match.group(1), urllib.parse.parse_qs(parsed.query))
            return
        self._error("not_found", status=HTTPStatus.NOT_FOUND)

    def _wallet_refresh(self) -> None:
        try:
            status = start_wallet_refresh(
                self.dashboard_config.data_dir,
                runner=self.dashboard_config.wallet_refresh_runner,
                timeout_seconds=self.dashboard_config.wallet_refresh_timeout_seconds,
            )
        except WalletRefreshAlreadyRunning as exc:
            self._json({"ok": False, "error": "wallet_refresh_running", "data": exc.status}, status=HTTPStatus.CONFLICT)
            return
        self._json({"ok": True, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED)

    def _runner_start(self) -> None:
        try:
            status = start_runner(self.dashboard_config)
        except RunnerAlreadyRunning as exc:
            self._json({"ok": False, "error": "runner_already_running", "data": exc.status}, status=HTTPStatus.CONFLICT)
            return
        self._json({"ok": True, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED)

    def _runner_stop(self) -> None:
        status = stop_runner(self.dashboard_config)
        accepted = status.get("status") in {"stopping", "stopped"}
        self._json({"ok": accepted, "data": status, "generated_at": int(time.time())}, status=HTTPStatus.ACCEPTED if accepted else HTTPStatus.CONFLICT)

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
        watched = build_events(self.dashboard_config.data_dir, observe_window_hours=self.dashboard_config.observe_window_hours)
        watched_ids = {str(row.get("condition_id") or "").lower() for row in watched.get("events", [])}
        open_snapshot = FollowStore(self.dashboard_config.data_dir / "follow" / "follow.db").load_dashboard_open_signals()
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
        body = self.rfile.read(_int_param(self.headers.get("Content-Length"), default=0, minimum=0, maximum=16384))
        content_type = self.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                form = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                form = {}
        else:
            form = {key: values[0] for key, values in urllib.parse.parse_qs(body.decode()).items()}
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
        cookie = (
            f"{COOKIE_NAME}={token}; Path=/; Max-Age={config.session_ttl_seconds}; "
            "HttpOnly; SameSite=Lax"
        )
        if config.cookie_secure:
            cookie += "; Secure"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Set-Cookie", cookie)
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

    def _serve_static(self, path: str) -> None:
        static_dir = self.dashboard_config.static_dir or Path(__file__).with_name("dashboard").joinpath("static")
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
            store = FollowStore(config.data_dir / "follow" / "follow.db")
            previous: StreamSignal | None = None
            last_heartbeat = 0.0
            while True:
                if conn is None:
                    conn = store.connect_readonly()
                signal = read_stream_signal(config.data_dir, store=store, conn=conn)
                if conn is not None and signal.snapshot_updated_at == 0 and not (config.data_dir / "follow" / "follow.db").exists():
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
        self.end_headers()
        self.wfile.write(body)


def build_health(data_dir: Path, *, started_at: float) -> dict[str, Any]:
    rows = _read_jsonl(data_dir / "follow" / "follow_run_log.jsonl")
    db_ready = FollowStore(data_dir / "follow" / "follow.db").dashboard_db_ready()
    last_tick = rows[-1] if rows else {}
    build_summary = _read_json(data_dir / "build_summary.json", {})
    leaderboard_path = data_dir / "smart_wallet_leaderboard.json"
    leaderboard_updated_at = int(leaderboard_path.stat().st_mtime) if leaderboard_path.exists() else 0
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
        "uptime_seconds": int(time.time() - started_at),
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
    store: FollowStore | None = None,
    conn: sqlite3.Connection | None = None,
) -> StreamSignal:
    store = store or FollowStore(data_dir / "follow" / "follow.db")
    return StreamSignal(
        snapshot_updated_at=store.read_meta_int("follow_snapshot_updated_at", conn=conn),
        run_log_mtime=_file_mtime(data_dir / "follow" / "follow_run_log.jsonl"),
        control_mtime=_file_mtime(data_dir / "follow" / "follow_control.json"),
        leaderboard_mtime=_file_mtime(data_dir / "smart_wallet_leaderboard.json"),
    )


def stream_dirty_flags(previous: StreamSignal | None, current: StreamSignal) -> dict[str, bool]:
    if previous is None:
        return {"follows_dirty": True, "events_dirty": True, "wallets_dirty": True}
    snapshot_dirty = current.snapshot_updated_at != previous.snapshot_updated_at
    return {
        "follows_dirty": snapshot_dirty,
        "events_dirty": snapshot_dirty,
        "wallets_dirty": current.leaderboard_mtime != previous.leaderboard_mtime,
    }


def build_stream_header(config: DashboardConfig, *, started_at: float) -> dict[str, Any]:
    control = read_follow_control(config.data_dir)
    return {
        "health": build_health(config.data_dir, started_at=started_at),
        "overview": build_overview(config.data_dir),
        "runner": build_runner_status(config),
        "refresh": build_wallet_refresh_status(config.data_dir).get("status") or {"status": "idle"},
        "pause_follow": control.get("pause_follow") if isinstance(control, dict) else None,
        "live": {
            "status": "connected",
            "generated_at": int(time.time()),
        },
    }


def build_overview(data_dir: Path) -> dict[str, Any]:
    snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()
    open_signals = snapshot.get("open_signals", [])
    results = snapshot.get("results", [])
    all_signals = [*open_signals, *results]
    settled = [row for row in results if row.get("status") == "settled"]
    exited = [row for row in results if row.get("status") == "exited"]
    wins = [row for row in settled if _result_win(row)]
    legs = [leg for signal in all_signals for leg in signal.get("legs") or []]
    would_follow = [leg for leg in legs if leg.get("would_follow", True)]
    contested = [signal for signal in all_signals if signal.get("contested")]
    clv_values = [_to_float(signal.get("wallet_clv")) for signal in all_signals if signal.get("wallet_clv") is not None]
    our_pnl = sum(_signal_our_pnl(row) for row in results)
    wallet_basis_pnl = sum(_signal_wallet_pnl(row) for row in results)
    behavior = _behavior_counts(all_signals)
    return {
        "db_ready": bool(snapshot.get("db_ready")),
        "open_signal_count": len(open_signals),
        "result_count": len(results),
        "settled_count": len(settled),
        "exited_count": len(exited),
        "win_rate": (len(wins) / len(settled)) if settled else None,
        "our_realized_pnl": our_pnl,
        "wallet_basis_realized_pnl": wallet_basis_pnl,
        "delay_cost": wallet_basis_pnl - our_pnl,
        "would_follow_capture_rate": (len(would_follow) / len(legs)) if legs else None,
        "contested_signal_count": len(contested),
        "clean_signal_count": len(all_signals) - len(contested),
        "avg_wallet_clv": (sum(clv_values) / len(clv_values)) if clv_values else None,
        "open_exposure": sum(sum(_to_float(leg.get("stake")) for leg in signal.get("legs") or []) for signal in open_signals),
        "behavior_counts": behavior,
        "performance": snapshot.get("performance") or {},
    }


def build_wallets(data_dir: Path) -> dict[str, Any]:
    leaderboard = _read_json(data_dir / "smart_wallet_leaderboard.json", [])
    store = FollowStore(data_dir / "follow" / "follow.db")
    perf_snapshot = store.load_dashboard_performance()
    open_snapshot = store.load_dashboard_open_signals()
    quarantine_snapshot = store.load_dashboard_wallet_quarantine()
    performance = (perf_snapshot.get("performance") or {}).get("wallets") or {}
    quarantine = quarantine_snapshot.get("wallet_quarantine") or {}
    open_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for signal in open_snapshot.get("open_signals", []):
        wallet = str(signal.get("wallet") or "").lower()
        open_by_wallet.setdefault(wallet, []).append(signal)
    rows = []
    for row in leaderboard if isinstance(leaderboard, list) else []:
        wallet = str(row.get("wallet") or "").lower()
        rows.append(
            {
                "wallet": wallet,
                "short_addr": short_addr(wallet),
                "grade": row.get("grade"),
                "last_esports_trade_at": row.get("last_esports_trade_at"),
                "wilson_win_rate_lower_bound": row.get("wilson_win_rate_lower_bound"),
                "entry_edge": row.get("entry_edge"),
                "esports_roi": row.get("esports_roi"),
                "median_entry_price": row.get("median_entry_price"),
                "reasons": row.get("reasons") or [],
                "scoring_version": row.get("scoring_version"),
                "esports_win_count": row.get("esports_win_count"),
                "esports_loss_count": row.get("esports_loss_count"),
                "esports_closed_count": row.get("esports_closed_count"),
                "positive_market_rate": row.get("positive_market_rate"),
                "sold_before_resolution_market_rate": row.get("sold_before_resolution_market_rate"),
                "two_sided_trade_market_rate": row.get("two_sided_trade_market_rate"),
                "quarantined": wallet in quarantine,
                "quarantine": quarantine.get(wallet),
                "performance": performance.get(wallet, {}),
                "open_signals": open_by_wallet.get(wallet, []),
            }
        )
    return {
        "wallets": rows,
        "count": len(rows),
        "quarantined_count": sum(1 for row in rows if row.get("quarantined")),
        "leaderboard_updated_at": int((data_dir / "smart_wallet_leaderboard.json").stat().st_mtime)
        if (data_dir / "smart_wallet_leaderboard.json").exists()
        else 0,
        "scoring_version": max([int(row.get("scoring_version") or 0) for row in rows] or [0]) or None,
        "db_ready": bool(perf_snapshot.get("db_ready") or open_snapshot.get("db_ready") or quarantine_snapshot.get("db_ready")),
    }


def build_follows(data_dir: Path, *, page: int = 1, size: int = 25) -> dict[str, Any]:
    store = FollowStore(data_dir / "follow" / "follow.db")
    result = store.load_dashboard_follow_rows(page=page, size=size)
    groups = _follow_groups_from_signals(result.get("signals", []))
    rows = sorted(groups.values(), key=lambda row: row.get("last_activity_at") or 0, reverse=True)
    return {"page": page, "size": size, "total": int(result.get("total") or 0), "follows": rows, "db_ready": bool(result.get("db_ready"))}


def build_follow_detail(data_dir: Path, condition_id: str) -> dict[str, Any]:
    condition_id = condition_id.lower()
    result = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_follow_detail(condition_id)
    signals = result.get("signals", [])
    by_wallet: dict[str, dict[str, Any]] = {}
    for signal in signals:
        wallet = str(signal.get("wallet") or "").lower()
        bucket = by_wallet.setdefault(wallet, {"wallet": wallet, "short_addr": short_addr(wallet), "signals": [], "leg_count": 0})
        bucket["signals"].append(signal)
        bucket["leg_count"] += len(signal.get("legs") or [])
    return {
        "condition_id": condition_id,
        "wallets": list(by_wallet.values()),
        "signal_count": len(signals),
        "db_ready": bool(result.get("db_ready")),
    }


def build_events(data_dir: Path, *, observe_window_hours: float = 24.0) -> dict[str, Any]:
    cache_path = data_dir / "follow" / "active_market_cache.json"
    cached = _read_json(cache_path, {})
    updated_at = int(cached.get("updated_at") or 0) if isinstance(cached, dict) else 0
    markets = cached.get("markets") if isinstance(cached, dict) else []
    if isinstance(markets, dict):
        rows = list(markets.values())
    elif isinstance(markets, list):
        rows = markets
    else:
        rows = []
    now_ts = int(time.time())
    window_end = now_ts + int(observe_window_hours * 3600)
    events = []
    open_snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_open_signals()
    open_by_condition: dict[str, list[dict[str, Any]]] = {}
    for signal in open_snapshot.get("open_signals", []):
        open_by_condition.setdefault(str(signal.get("condition_id") or "").lower(), []).append(signal)
    for market in rows:
        if not isinstance(market, dict):
            continue
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        start_ts = _parse_timestamp(market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"))
        if start_ts and now_ts <= start_ts <= window_end:
            open_signals = open_by_condition.get(condition_id, [])
            events.append(
                {
                    "condition_id": condition_id,
                    "title": market.get("title"),
                    "question": market.get("question"),
                    "match_start_time": market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"),
                    "outcomes": market.get("outcomes"),
                    "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
                    "open_signals": open_signals,
                    "contested": _signals_contested(open_signals),
                    "side_counts": _signal_side_counts(open_signals),
                }
            )
    return {
        "events": events,
        "count": len(events),
        "cache_updated_at": updated_at,
        "cache_stale": bool(not updated_at or now_ts - updated_at > 15 * 60),
    }


class WalletRefreshAlreadyRunning(RuntimeError):
    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__("wallet refresh already running")
        self.status = status


class RunnerAlreadyRunning(RuntimeError):
    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__("runner already running")
        self.status = status


def build_runner_status(config: DashboardConfig) -> dict[str, Any]:
    control = read_follow_control(config.data_dir)
    recorded = control.get("runner") if isinstance(control.get("runner"), dict) else {}
    processes = _find_runner_processes(config)
    recorded_pid = int(recorded.get("pid") or 0) if isinstance(recorded, dict) else 0
    matched = next((row for row in processes if int(row.get("pid") or 0) == recorded_pid), None)
    if matched is None and processes:
        matched = processes[0]
    if matched:
        source = "dashboard" if int(matched.get("pid") or 0) == recorded_pid else "external"
        return {
            "status": "running",
            "pid": int(matched.get("pid") or 0),
            "pgid": int(matched.get("pgid") or 0),
            "source": source,
            "command": matched.get("command") or recorded.get("command"),
            "started_at": recorded.get("started_at") if source == "dashboard" else None,
            "log_path": recorded.get("log_path") if source == "dashboard" else None,
            "data_dir": str(config.data_dir),
        }
    if recorded:
        return {
            **recorded,
            "status": "stopped",
            "pid": recorded_pid or None,
            "data_dir": str(config.data_dir),
        }
    return {"status": "stopped", "data_dir": str(config.data_dir)}


def start_runner(config: DashboardConfig) -> dict[str, Any]:
    current = build_runner_status(config)
    if current.get("status") == "running":
        raise RunnerAlreadyRunning(current)
    now_ts = int(time.time())
    follow_dir = config.data_dir / "follow"
    follow_dir.mkdir(parents=True, exist_ok=True)
    log_path = follow_dir / f"dashboard-runner-{now_ts}.out"
    command = [
        sys.executable,
        "-u",
        "-m",
        "poly_fight.cli",
        "--data-dir",
        str(config.data_dir),
        "run",
        "--stake-usdc",
        str(config.runner_stake_usdc),
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
    status = {
        "status": "running",
        "source": "dashboard",
        "pid": pid,
        "pgid": pgid,
        "started_at": now_ts,
        "command": command,
        "log_path": str(log_path),
        "data_dir": str(config.data_dir),
    }
    _update_runner_control(config.data_dir, status)
    return status


def stop_runner(config: DashboardConfig) -> dict[str, Any]:
    current = build_runner_status(config)
    pid = int(current.get("pid") or 0)
    if not pid:
        return {"status": "stopped", "data_dir": str(config.data_dir)}
    if config.runner_process_stopper is not None:
        config.runner_process_stopper(current)
    else:
        _terminate_runner_process(current)
    status = {
        **current,
        "status": "stopping",
        "stop_requested_at": int(time.time()),
    }
    _update_runner_control(config.data_dir, status)
    return status


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


def _system_processes() -> list[dict[str, Any]]:
    for command in (["ps", "-axo", "pid=,ppid=,pgid=,command="], ["ps", "-eo", "pid=,ppid=,pgid=,command="]):
        try:
            result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        except OSError:
            continue
        if result.returncode == 0:
            return [_parse_ps_line(line) for line in result.stdout.splitlines()]
    return []


def _parse_ps_line(line: str) -> dict[str, Any]:
    parts = line.strip().split(None, 3)
    if len(parts) < 4:
        return {}
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
        "command": str(row.get("command") or ""),
    }


def _process_matches_runner(row: dict[str, Any], data_dir: Path) -> bool:
    command = str(row.get("command") or "")
    pid = int(row.get("pid") or 0)
    tokens = command.split()
    if pid <= 0 or "poly_fight.cli" not in tokens or "run" not in tokens:
        return False
    if "--execution-mode live" in command:
        return False
    data_dir_values = _data_dir_values_from_command(tokens)
    if not data_dir_values:
        return False
    expected = {str(data_dir), str(data_dir.resolve())}
    return any(value in expected or str(Path(value).resolve()) in expected for value in data_dir_values)


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


def build_wallet_refresh_status(data_dir: Path) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    status = control.get("wallet_refresh") if isinstance(control.get("wallet_refresh"), dict) else {}
    return {
        "status": status or {"status": "idle"},
    }


def start_wallet_refresh(
    data_dir: Path,
    *,
    runner: Any = None,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    now_ts = int(time.time())
    control = read_follow_control(data_dir)
    existing = control.get("wallet_refresh")
    if isinstance(existing, dict) and existing.get("status") == "running":
        started_at = int(existing.get("started_at") or 0)
        if not started_at or now_ts - started_at < timeout_seconds:
            raise WalletRefreshAlreadyRunning(existing)

    follow_dir = data_dir / "follow"
    follow_dir.mkdir(parents=True, exist_ok=True)
    log_path = follow_dir / f"wallet-refresh-{now_ts}.out"
    command = [
        sys.executable,
        "-u",
        "-m",
        "poly_fight.cli",
        "--data-dir",
        str(data_dir),
        "collect",
        "--max-profiles-per-run",
        "1000",
    ]
    status = {
        "status": "running",
        "started_at": now_ts,
        "command": command,
        "log_path": str(log_path),
    }
    update_wallet_refresh_status(data_dir, status)

    def worker() -> None:
        finished_at = int(time.time())
        try:
            if runner is not None:
                returncode = int(runner(data_dir, log_path) or 0)
            else:
                with log_path.open("ab") as log_file:
                    result = subprocess.run(command, cwd=Path(__file__).resolve().parents[1], stdout=log_file, stderr=subprocess.STDOUT, check=False)
                    returncode = int(result.returncode)
            finished_at = int(time.time())
            update_wallet_refresh_status(
                data_dir,
                {
                    **status,
                    "status": "succeeded" if returncode == 0 else "failed",
                    "finished_at": finished_at,
                    "returncode": returncode,
                },
            )
        except Exception as exc:
            finished_at = int(time.time())
            update_wallet_refresh_status(
                data_dir,
                {
                    **status,
                    "status": "failed",
                    "finished_at": finished_at,
                    "error": str(exc),
                },
            )

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
                "title": signal.get("title") or signal.get("market_title"),
                "question": signal.get("question"),
                "wallets": set(),
                "leg_count": 0,
                "stake": 0.0,
                "status_counts": {},
                "our_realized_pnl": 0.0,
                "wallet_basis_realized_pnl": 0.0,
                "last_activity_at": 0,
                "contested_signal_count": 0,
                "clv_sum": 0.0,
                "clv_count": 0,
            },
        )
        wallet = str(signal.get("wallet") or "").lower()
        if wallet:
            bucket["wallets"].add(wallet)
        legs = signal.get("legs") or []
        bucket["leg_count"] += len(legs)
        bucket["stake"] += sum(_to_float(leg.get("stake")) for leg in legs)
        status = str(signal.get("status") or "open")
        bucket["status_counts"][status] = bucket["status_counts"].get(status, 0) + 1
        bucket["our_realized_pnl"] += _signal_our_pnl(signal)
        bucket["wallet_basis_realized_pnl"] += _signal_wallet_pnl(signal)
        if signal.get("contested"):
            bucket["contested_signal_count"] += 1
        if signal.get("wallet_clv") is not None:
            bucket["clv_sum"] += _to_float(signal.get("wallet_clv"))
            bucket["clv_count"] += 1
        bucket["last_activity_at"] = max(
            int(bucket["last_activity_at"] or 0),
            int(signal.get("updated_at") or signal.get("settled_at") or signal.get("exit_at") or signal.get("created_at") or 0),
        )
    for bucket in groups.values():
        bucket["wallet_count"] = len(bucket["wallets"])
        bucket["wallets"] = sorted(bucket["wallets"])
        if bucket["status_counts"].get("open"):
            bucket["status"] = "open"
        elif len(bucket["status_counts"]) == 1:
            bucket["status"] = next(iter(bucket["status_counts"]))
        else:
            bucket["status"] = "mixed"
        bucket["roi"] = bucket["our_realized_pnl"] / bucket["stake"] if bucket["stake"] else None
        bucket["avg_wallet_clv"] = bucket["clv_sum"] / bucket["clv_count"] if bucket["clv_count"] else None
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
    leaderboard = _read_json(data_dir / "smart_wallet_leaderboard.json", [])
    versions = [int(row.get("scoring_version") or 0) for row in leaderboard if isinstance(row, dict)]
    version = max(versions or [0])
    return version or None


def _signals_contested(signals: list[dict[str, Any]]) -> bool:
    outcomes = {_signal_side(signal) for signal in signals}
    outcomes.discard("")
    return len(outcomes) > 1 or any(bool(signal.get("contested")) for signal in signals)


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


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


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


def _parse_timestamp(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace("Z", "+00:00")
    try:
        from datetime import datetime, timezone

        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return 0


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
    if row.get("our_realized_pnl") is not None:
        return _to_float(row.get("our_realized_pnl"))
    return _to_float(row.get("our_paper_pnl"))


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
