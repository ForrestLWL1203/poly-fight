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
from .core import MIN_A_POSITIVE_MARKET_RATE, parse_jsonish, to_float
from .storage import FollowStore


COOKIE_NAME = "poly_fight_session"
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
FAILED_LOGINS: dict[str, list[float]] = {}
_TRADES_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_TRADES_CACHE_LOCK = threading.Lock()
_MATCH_TITLE_RE = re.compile(r"^([^:]+):\s+(.+?)\s+vs\s+(.+?)(\s+\([^)]+\))?\s+-\s+(.+)$", re.IGNORECASE)


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
        if parsed.path == "/api/wallet-follows":
            query = urllib.parse.parse_qs(parsed.query)
            wallet = str(query.get("wallet", [""])[0] or "").lower()
            status = str(query.get("status", [""])[0] or "").lower()
            self._ok(build_wallet_follow_detail(self.dashboard_config.data_dir, wallet, status=status))
            return
        match = re.match(r"^/api/wallets/([^/]+)/follows$", parsed.path)
        if match:
            wallet = urllib.parse.unquote(match.group(1)).lower()
            query = urllib.parse.parse_qs(parsed.query)
            status = str(query.get("status", [""])[0] or "").lower()
            self._ok(build_wallet_follow_detail(self.dashboard_config.data_dir, wallet, status=status))
            return
        if parsed.path == "/api/follows":
            query = urllib.parse.parse_qs(parsed.query)
            page = _int_param(query.get("page", ["1"])[0], default=1, minimum=1, maximum=10_000)
            size = _int_param(query.get("size", ["25"])[0], default=25, minimum=1, maximum=200)
            status = str(query.get("status", [""])[0] or "").lower()
            self._ok(build_follows(self.dashboard_config.data_dir, page=page, size=size, status=status))
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
        match = re.match(r"^/api/markets/([^/]+)/prices$", parsed.path)
        if match:
            condition_id = urllib.parse.unquote(match.group(1)).lower()
            try:
                self._ok(fetch_market_prices(self.dashboard_config.data_dir, self.dashboard_config.client, condition_id))
            except Exception as exc:
                self._error(str(exc) or "price_refresh_failed", status=HTTPStatus.BAD_GATEWAY)
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
        "wallets_dirty": snapshot_dirty or current.leaderboard_mtime != previous.leaderboard_mtime,
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
    result_legs = [leg for signal in results for leg in signal.get("legs") or []]
    total_stake = sum(_to_float(leg.get("stake")) for leg in legs)
    resolved_stake = sum(_to_float(leg.get("stake")) for leg in result_legs)
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
        "total_stake": total_stake,
        "resolved_stake": resolved_stake,
        "realized_roi": (our_pnl / resolved_stake) if resolved_stake else None,
        "wallet_basis_realized_roi": (wallet_basis_pnl / resolved_stake) if resolved_stake else None,
        "delay_cost": wallet_basis_pnl - our_pnl,
        "would_follow_capture_rate": (len(would_follow) / len(legs)) if legs else None,
        "contested_signal_count": len(contested),
        "clean_signal_count": len(all_signals) - len(contested),
        "avg_wallet_clv": (sum(clv_values) / len(clv_values)) if clv_values else None,
        "open_exposure": sum(sum(_to_float(leg.get("stake")) for leg in signal.get("legs") or []) for signal in open_signals),
        "behavior_counts": behavior,
        "performance": snapshot.get("performance") or {},
    }


def _eligible_display_metrics(row: dict[str, Any]) -> dict[str, Any]:
    """Stats to show for a leaderboard row.

    When a wallet qualifies via per-type buckets, return the eligible bucket with the
    most evidence (largest sample) so the displayed win rate / ROI / edge align with the
    grade and market-type label. Falls back to the overall row for legacy profiles that
    predate per-type grading.
    """
    eligible = row.get("eligible_market_types") or []
    per_type = row.get("per_type_grades") or {}
    buckets = [per_type[market_type] for market_type in eligible if isinstance(per_type.get(market_type), dict)]
    if not buckets:
        return row
    return max(buckets, key=lambda bucket: int(bucket.get("esports_closed_count") or 0))


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
        # Grading is per market_type, so a wallet may qualify only on a sub-bucket
        # (e.g. game_winner) while its blended overall record looks weak. Display the
        # eligible bucket's own stats so the shown numbers match the grade + type label.
        metrics = _eligible_display_metrics(row)
        if "positive_market_rate" in metrics and to_float(metrics.get("positive_market_rate")) < MIN_A_POSITIVE_MARKET_RATE:
            continue
        wallet = str(row.get("wallet") or "").lower()
        observed = wallet_observed_performance(performance.get(wallet, {}), open_count=len(open_by_wallet.get(wallet, [])))
        observed_market_types = _observed_market_types(row)
        rows.append(
            {
                "wallet": wallet,
                "short_addr": short_addr(wallet),
                "grade": row.get("grade"),
                "last_esports_trade_at": row.get("last_esports_trade_at"),
                "wilson_win_rate_lower_bound": metrics.get("wilson_win_rate_lower_bound"),
                "entry_edge": metrics.get("entry_edge"),
                "esports_roi": metrics.get("esports_roi"),
                "median_market_roi": metrics.get("median_market_roi"),
                "median_entry_price": metrics.get("median_entry_price"),
                "reasons": metrics.get("reasons") or row.get("reasons") or [],
                "scoring_version": row.get("scoring_version"),
                "esports_win_count": metrics.get("esports_win_count"),
                "esports_loss_count": metrics.get("esports_loss_count"),
                "esports_closed_count": metrics.get("esports_closed_count"),
                "positive_market_rate": metrics.get("positive_market_rate"),
                "sold_before_resolution_market_rate": row.get("sold_before_resolution_market_rate"),
                "two_sided_trade_market_rate": row.get("two_sided_trade_market_rate"),
                "eligible_market_types": row.get("eligible_market_types") or [],
                "eligible_market_type_labels": row.get("eligible_market_type_labels") or [],
                "observed_market_types": observed_market_types,
                "observed_market_type_labels": row.get("observed_market_type_labels") or _market_type_labels(observed_market_types),
                "per_type_grades": row.get("per_type_grades") or {},
                "quarantined": wallet in quarantine,
                "quarantine": quarantine.get(wallet),
                "performance": performance.get(wallet, {}),
                "observed": observed,
                "open_signals": open_by_wallet.get(wallet, []),
            }
        )
    rows.sort(key=wallet_leaderboard_rank_key)
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
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


def wallet_leaderboard_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    loss_count = int(row.get("esports_loss_count") or 0)
    observed = row.get("observed") or {}
    observed_has_loss = bool(observed.get("has_loss"))
    observed_pnl = to_float(observed.get("our_pnl"))
    return (
        bool(row.get("quarantined")),
        observed_has_loss,
        -observed_pnl,
        loss_count > 0,
        loss_count,
        -to_float(row.get("positive_market_rate")),
        -to_float(row.get("wilson_win_rate_lower_bound")),
        -to_float(row.get("entry_edge")),
        -to_float(row.get("median_market_roi") or row.get("esports_roi")),
        str(row.get("wallet") or ""),
    )


def build_follows(data_dir: Path, *, page: int = 1, size: int = 25, status: str = "") -> dict[str, Any]:
    store = FollowStore(data_dir / "follow" / "follow.db")
    result = store.load_dashboard_follow_rows(page=1, size=10_000)
    logo_cache = _load_team_logo_cache(data_dir)
    allowed_statuses = {"open", "settled", "exited", "mixed"}
    status_filter = status if status in allowed_statuses else ""
    groups = _follow_groups_from_signals(result.get("signals", []))
    rows = sorted(groups.values(), key=lambda row: row.get("last_activity_at") or 0, reverse=True)
    if status_filter:
        rows = [row for row in rows if str(row.get("status") or "") == status_filter]
    total = len(rows)
    page = max(1, int(page or 1))
    size = max(1, int(size or 25))
    start = (page - 1) * size
    rows = rows[start : start + size]
    for row in rows:
        row["team_logos"] = _team_logos_for_title(str(row.get("title") or row.get("question") or ""), logo_cache)
    return {
        "page": page,
        "size": size,
        "total": total,
        "status": status_filter,
        "follows": rows,
        "db_ready": bool(result.get("db_ready")),
    }


def build_follow_detail(data_dir: Path, condition_id: str) -> dict[str, Any]:
    condition_id = condition_id.lower()
    result = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_follow_detail(condition_id)
    signals = result.get("signals", [])
    market = _active_market_by_condition(data_dir, condition_id)
    logo_cache = _load_team_logo_cache(data_dir)
    leaderboard_ranks = _leaderboard_rank_by_wallet(data_dir)
    by_wallet: dict[str, dict[str, Any]] = {}
    title = ""
    question = ""
    match_start_time = None
    end_date = None
    event_slug = ""
    market_type = ""
    market_type_label = ""
    for signal in signals:
        title = title or str(signal.get("event_title") or signal.get("title") or signal.get("market_title") or "")
        question = question or str(signal.get("market_question") or signal.get("question") or "")
        match_start_time = match_start_time or signal.get("match_start_time") or signal.get("market_start_time")
        end_date = end_date or signal.get("end_date")
        event_slug = event_slug or str(signal.get("event_slug") or "")
        market_type = market_type or str(signal.get("market_type") or "")
        market_type_label = market_type_label or str(signal.get("market_type_label") or "")
        wallet = str(signal.get("wallet") or "").lower()
        bucket = by_wallet.setdefault(
            wallet,
            {
                "wallet": wallet,
                "short_addr": short_addr(wallet),
                "leaderboard_rank": leaderboard_ranks.get(wallet),
                "signals": [],
                "leg_count": 0,
            },
        )
        bucket["signals"].append(signal)
        bucket["leg_count"] += len(signal.get("legs") or [])
    title = title or str(market.get("title") or "")
    question = question or str(market.get("question") or "")
    match_start_time = match_start_time or market.get("match_start_time") or market.get("market_start_time")
    end_date = end_date or market.get("end_date")
    event_slug = event_slug or str(market.get("event_slug") or "")
    market_type = market_type or str(market.get("market_type") or "")
    market_type_label = market_type_label or str(market.get("market_type_label") or "")
    return {
        "condition_id": condition_id,
        "title": title,
        "question": question,
        "match_start_time": match_start_time,
        "end_date": end_date,
        "event_slug": event_slug,
        "event_url": f"https://polymarket.com/event/{event_slug}" if event_slug else "",
        "market_type": market_type,
        "market_type_label": market_type_label,
        "team_logos": _team_logos_for_title(title or question, logo_cache),
        "outcomes": market.get("outcomes"),
        "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
        "wallets": list(by_wallet.values()),
        "signal_count": len(signals),
        "db_ready": bool(result.get("db_ready")),
    }


def _leaderboard_rank_by_wallet(data_dir: Path) -> dict[str, int]:
    leaderboard = _read_json(data_dir / "smart_wallet_leaderboard.json", [])
    rows: list[dict[str, Any]] = []
    for row in leaderboard if isinstance(leaderboard, list) else []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        metrics = _eligible_display_metrics(row)
        merged = {**row, **metrics, "wallet": wallet}
        rows.append(merged)
    rows.sort(key=wallet_leaderboard_rank_key)
    return {str(row.get("wallet") or "").lower(): index for index, row in enumerate(rows, start=1)}


def build_wallet_follow_detail(data_dir: Path, wallet: str, *, status: str = "") -> dict[str, Any]:
    wallet = wallet.lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", wallet):
        return {"wallet": wallet, "short_addr": short_addr(wallet), "signals": [], "count": 0, "db_ready": False}
    allowed_statuses = {"open", "settled", "exited"}
    statuses = {status} if status in allowed_statuses else set()
    result = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_wallet_follow_detail(wallet, statuses=statuses)
    signals = result.get("signals", [])
    signals = sorted(
        [signal for signal in signals if isinstance(signal, dict)],
        key=lambda signal: _signal_activity_at(signal),
        reverse=True,
    )
    return {
        "wallet": wallet,
        "short_addr": short_addr(wallet),
        "status": status if status in allowed_statuses else "",
        "signals": signals,
        "count": len(signals),
        "db_ready": bool(result.get("db_ready")),
    }


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


def build_events(data_dir: Path, *, observe_window_hours: float = 24.0) -> dict[str, Any]:
    cache_path = data_dir / "follow" / "active_market_cache.json"
    cached = _read_json(cache_path, {})
    logo_cache = _load_team_logo_cache(data_dir)
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
    follow_snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()
    open_by_condition: dict[str, list[dict[str, Any]]] = {}
    for signal in follow_snapshot.get("open_signals", []):
        open_by_condition.setdefault(str(signal.get("condition_id") or "").lower(), []).append(signal)
    results_by_condition: dict[str, list[dict[str, Any]]] = {}
    for result in follow_snapshot.get("results", []):
        condition_id = str(result.get("condition_id") or "").lower()
        if condition_id:
            results_by_condition.setdefault(condition_id, []).append(result)
    for market in rows:
        if not isinstance(market, dict):
            continue
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        start_ts = _parse_timestamp(market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"))
        open_signals = open_by_condition.get(condition_id, [])
        results = results_by_condition.get(condition_id, [])
        if (start_ts and now_ts <= start_ts <= window_end) or open_signals:
            open_signals = open_by_condition.get(condition_id, [])
            results = results_by_condition.get(condition_id, [])
            events.append(
                {
                    "condition_id": condition_id,
                    "title": market.get("title"),
                    "question": market.get("question"),
                    "team_logos": _team_logos_for_title(str(market.get("title") or market.get("question") or ""), logo_cache),
                    "match_start_time": market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"),
                    "outcomes": market.get("outcomes"),
                    "outcome_prices": market.get("outcome_prices") or market.get("outcomePrices"),
                    "market_type": market.get("market_type"),
                    "market_type_label": market.get("market_type_label"),
                    "open_signals": open_signals,
                    "results": results,
                    "settled_count": sum(1 for result in results if result.get("status") == "settled"),
                    "exited_count": sum(1 for result in results if result.get("status") == "exited"),
                    "contested": _signals_contested([*open_signals, *results]),
                    "side_counts": _signal_side_counts([*open_signals, *results]),
                }
            )
    events.sort(
        key=lambda row: (
            0 if row.get("open_signals") else 1 if row.get("results") else 2,
            _parse_timestamp(row.get("match_start_time")) or 0,
        )
    )
    return {
        "events": events,
        "count": len(events),
        "cache_updated_at": updated_at,
        "cache_stale": bool(not updated_at or now_ts - updated_at > 15 * 60),
    }


def _load_team_logo_cache(data_dir: Path) -> dict[str, str]:
    static_path = Path(__file__).with_name("dashboard") / "static" / "team_logos" / "team_logos.json"
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


def _team_logos_for_title(title: str, logo_cache: dict[str, str]) -> dict[str, str]:
    if not title or not logo_cache:
        return {}
    parts = _match_title_parts(title)
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


def _normalize_logo_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _active_market_by_condition(data_dir: Path, condition_id: str) -> dict[str, Any]:
    cached = _read_json(data_dir / "follow" / "active_market_cache.json", {})
    markets = cached.get("markets") if isinstance(cached, dict) else []
    if isinstance(markets, dict):
        rows = markets.values()
    elif isinstance(markets, list):
        rows = markets
    else:
        rows = []
    for market in rows:
        if not isinstance(market, dict):
            continue
        current = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        if current == condition_id:
            return market
    return {}


def fetch_market_prices(data_dir: Path, client: Any, condition_id: str) -> dict[str, Any]:
    if client is None:
        raise RuntimeError("polymarket client unavailable")
    condition_id = condition_id.lower()
    markets = client.gamma("/markets", condition_ids=condition_id, limit=1)
    if not isinstance(markets, list) or not markets:
        raise RuntimeError("market_not_found")
    market = markets[0]
    record = _market_price_record(market)
    if not record.get("outcomes") or not record.get("outcome_prices"):
        raise RuntimeError("market_prices_unavailable")
    _update_active_market_price_cache(data_dir, condition_id, record)
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


def _update_active_market_price_cache(data_dir: Path, condition_id: str, record: dict[str, Any]) -> None:
    cache_path = data_dir / "follow" / "active_market_cache.json"
    cached = _read_json(cache_path, {})
    markets = cached.get("markets") if isinstance(cached, dict) else []
    if isinstance(markets, dict):
        rows = list(markets.values())
    elif isinstance(markets, list):
        rows = list(markets)
    else:
        rows = []
    updated = False
    for market in rows:
        if not isinstance(market, dict):
            continue
        current = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        if current != condition_id:
            continue
        market["outcomes"] = record.get("outcomes")
        market["outcome_prices"] = record.get("outcome_prices")
        market["price_refreshed_at"] = record.get("updated_at")
        updated = True
        break
    if not updated:
        rows.append(
            {
                "condition_id": condition_id,
                "title": record.get("title"),
                "question": record.get("question"),
                "event_slug": record.get("event_slug"),
                "outcomes": record.get("outcomes"),
                "outcome_prices": record.get("outcome_prices"),
                "price_refreshed_at": record.get("updated_at"),
            }
        )
    _write_json(cache_path, {"updated_at": int(cached.get("updated_at") or time.time()) if isinstance(cached, dict) else int(time.time()), "markets": rows})


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
                "title": signal.get("event_title") or signal.get("title") or signal.get("market_title"),
                "question": signal.get("market_question") or signal.get("question"),
                "match_start_time": signal.get("match_start_time") or signal.get("market_start_time"),
                "market_type": signal.get("market_type"),
                "market_type_label": signal.get("market_type_label"),
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


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


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
