from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


class LeaderboardStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def connect_readonly(self) -> sqlite3.Connection | None:
        if not self.path.exists():
            return None
        try:
            conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=1")
            conn.execute("PRAGMA busy_timeout=5000")
            return conn
        except sqlite3.Error:
            return None

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS leaderboard_wallets (
                    category TEXT NOT NULL,
                    wallet TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (category, wallet)
                );
                CREATE INDEX IF NOT EXISTS idx_leaderboard_wallets_category_rank
                    ON leaderboard_wallets(category, rank);
                """
            )

    def replace_leaderboard(self, rows: list[dict[str, Any]], *, category: str, updated_at: int) -> None:
        category = str(category or "esports").lower()
        updated_at = int(updated_at or time.time())
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM leaderboard_wallets WHERE category = ?", (category,))
            for index, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    continue
                wallet = str(row.get("wallet") or "").lower()
                if not wallet:
                    continue
                payload = dict(row)
                payload.setdefault("category", category)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO leaderboard_wallets(category, wallet, rank, updated_at, raw_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (category, wallet, index, updated_at, _dumps(payload)),
                )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (f"{category}:updated_at", str(updated_at)),
            )
            conn.execute("COMMIT")

    def load_leaderboard(self, *, category: str | None = None) -> tuple[list[dict[str, Any]], dict[str, int]]:
        conn = self.connect_readonly()
        if conn is None:
            return [], {}
        try:
            params: tuple[Any, ...] = ()
            where = ""
            if category:
                where = "WHERE category = ?"
                params = (str(category).lower(),)
            rows = conn.execute(
                f"SELECT category, raw_json FROM leaderboard_wallets {where} ORDER BY category, rank",
                params,
            ).fetchall()
            values = []
            mtimes: dict[str, int] = {}
            for row in rows:
                row_category = str(row["category"] or "").lower()
                payload = _loads(row["raw_json"], {})
                if isinstance(payload, dict):
                    payload.setdefault("category", row_category)
                    values.append(payload)
            meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
            for row in meta_rows:
                key = str(row["key"] or "")
                if key.endswith(":updated_at"):
                    mtimes[key.split(":", 1)[0]] = int(row["value"] or 0)
            return values, mtimes
        finally:
            conn.close()


class FollowStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._initialized = False

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def connect_readonly(self) -> sqlite3.Connection | None:
        if not self.path.exists():
            return None
        try:
            conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=1")
            conn.execute("PRAGMA busy_timeout=5000")
            return conn
        except sqlite3.Error:
            return None

    def init_db(self) -> None:
        if self._initialized:
            return
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_cursors (
                    wallet TEXT PRIMARY KEY,
                    last_trade_timestamp INTEGER,
                    last_trade_id TEXT,
                    last_seen_at INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS market_cache (
                    condition_id TEXT PRIMARY KEY,
                    cache_kind TEXT NOT NULL DEFAULT 'active',
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS follow_signals (
                    signal_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    wallet TEXT,
                    condition_id TEXT,
                    outcome_index INTEGER,
                    created_at INTEGER,
                    updated_at INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS follow_legs (
                    signal_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    wallet TEXT,
                    condition_id TEXT,
                    leg_at INTEGER,
                    stake REAL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (signal_id, trade_id)
                );
                CREATE TABLE IF NOT EXISTS follow_behavior_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    kind TEXT,
                    timestamp INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS follow_results (
                    signal_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    wallet TEXT,
                    condition_id TEXT,
                    resolved_at INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_performance (
                    wallet TEXT PRIMARY KEY,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS performance_total (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_quarantine (
                    wallet TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    quarantined_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_favorites (
                    wallet_key TEXT PRIMARY KEY,
                    wallet TEXT NOT NULL,
                    category TEXT NOT NULL,
                    favorited_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS account_balance (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    balance_usdc REAL NOT NULL,
                    source TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS account_balance_ledger (
                    ledger_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    amount_usdc REAL NOT NULL,
                    balance_after_usdc REAL NOT NULL,
                    created_at INTEGER NOT NULL,
                    signal_id TEXT,
                    trade_id TEXT,
                    raw_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_follow_signals_status ON follow_signals(status);
                CREATE INDEX IF NOT EXISTS idx_follow_signals_wallet ON follow_signals(wallet);
                CREATE INDEX IF NOT EXISTS idx_follow_signals_condition_id ON follow_signals(condition_id);
                CREATE INDEX IF NOT EXISTS idx_follow_legs_signal_id ON follow_legs(signal_id);
                CREATE INDEX IF NOT EXISTS idx_follow_behavior_events_signal_id ON follow_behavior_events(signal_id);
                CREATE INDEX IF NOT EXISTS idx_follow_behavior_events_kind_ts ON follow_behavior_events(kind, timestamp);
                CREATE INDEX IF NOT EXISTS idx_follow_results_status_resolved_at ON follow_results(status, resolved_at);
                CREATE INDEX IF NOT EXISTS idx_market_cache_kind ON market_cache(cache_kind);
                CREATE INDEX IF NOT EXISTS idx_wallet_quarantine_ts ON wallet_quarantine(quarantined_at);
                CREATE INDEX IF NOT EXISTS idx_wallet_favorites_category_ts ON wallet_favorites(category, favorited_at);
                CREATE INDEX IF NOT EXISTS idx_account_balance_ledger_created_at ON account_balance_ledger(created_at);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        self._initialized = True

    def index_names(self) -> set[str]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        return {str(row["name"]) for row in rows}

    def load_wallet_trade_state(self) -> dict[str, dict[str, Any]]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT wallet, raw_json FROM wallet_cursors").fetchall()
        return {str(row["wallet"]): _loads(row["raw_json"], {}) for row in rows}

    def load_open_signals(self) -> list[dict[str, Any]]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT raw_json FROM follow_signals WHERE status = 'open' ORDER BY created_at, signal_id").fetchall()
        return [_loads(row["raw_json"], {}) for row in rows]

    def load_results(self) -> list[dict[str, Any]]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT raw_json FROM follow_results ORDER BY resolved_at, signal_id").fetchall()
        return [_loads(row["raw_json"], {}) for row in rows]

    def set_account_balance(self, balance_usdc: float, *, ts: int | None = None, source: str = "manual") -> dict[str, Any]:
        self.init_db()
        balance = round(float(balance_usdc), 8)
        updated_at = int(ts or time.time())
        row = {
            "configured": True,
            "balance_usdc": balance,
            "source": str(source or "manual"),
            "updated_at": updated_at,
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO account_balance(id, balance_usdc, source, updated_at, raw_json)
                VALUES (1, ?, ?, ?, ?)
                """,
                (balance, row["source"], updated_at, _dumps(row)),
            )
        return row

    def load_account_balance(self, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
        def read_from(active: sqlite3.Connection) -> dict[str, Any]:
            try:
                tables = {
                    str(row["name"])
                    for row in active.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                if "account_balance" not in tables:
                    return {"configured": False, "balance_usdc": None}
                row = active.execute("SELECT raw_json FROM account_balance WHERE id = 1").fetchone()
            except sqlite3.Error:
                return {"configured": False, "balance_usdc": None}
            if not row:
                return {"configured": False, "balance_usdc": None}
            payload = _loads(row["raw_json"], {})
            if not isinstance(payload, dict):
                return {"configured": False, "balance_usdc": None}
            payload["configured"] = True
            return payload

        if conn is not None:
            return read_from(conn)
        self.init_db()
        with self.connect() as active:
            return read_from(active)

    def load_account_balance_readonly(self) -> dict[str, Any]:
        readonly = self.connect_readonly()
        if readonly is None:
            return {"configured": False, "balance_usdc": None}
        try:
            return self.load_account_balance(readonly)
        finally:
            readonly.close()

    def apply_account_ledger(self, entries: list[dict[str, Any]]) -> dict[str, Any]:
        self.init_db()
        applied_count = 0
        applied_amount = 0.0
        with self.connect() as conn:
            state = self.load_account_balance(conn)
            if not state.get("configured"):
                return {"configured": False, "applied_count": 0, "applied_amount_usdc": 0.0}
            balance = float(state.get("balance_usdc") or 0.0)
            for entry in entries:
                ledger_id = str(entry.get("ledger_id") or "").strip()
                if not ledger_id:
                    continue
                exists = conn.execute(
                    "SELECT 1 FROM account_balance_ledger WHERE ledger_id = ?",
                    (ledger_id,),
                ).fetchone()
                if exists:
                    continue
                amount = round(float(entry.get("amount_usdc") or 0.0), 8)
                balance = round(balance + amount, 8)
                payload = dict(entry)
                payload["amount_usdc"] = amount
                payload["balance_after_usdc"] = balance
                conn.execute(
                    """
                    INSERT INTO account_balance_ledger
                    (ledger_id, kind, amount_usdc, balance_after_usdc, created_at, signal_id, trade_id, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ledger_id,
                        str(entry.get("kind") or ""),
                        amount,
                        balance,
                        int(entry.get("created_at") or time.time()),
                        str(entry.get("signal_id") or ""),
                        str(entry.get("trade_id") or ""),
                        _dumps(payload),
                    ),
                )
                applied_count += 1
                applied_amount = round(applied_amount + amount, 8)
            if applied_count:
                updated_at = int(time.time())
                updated = {
                    **state,
                    "configured": True,
                    "balance_usdc": balance,
                    "updated_at": updated_at,
                }
                conn.execute(
                    """
                    INSERT OR REPLACE INTO account_balance(id, balance_usdc, source, updated_at, raw_json)
                    VALUES (1, ?, ?, ?, ?)
                    """,
                    (balance, str(updated.get("source") or "manual"), updated_at, _dumps(updated)),
                )
        return {
            "configured": True,
            "applied_count": applied_count,
            "applied_amount_usdc": round(applied_amount, 8),
        }

    def upsert_wallet_quarantine(
        self,
        wallet: str,
        *,
        reason: str,
        ts: int,
        category: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.init_db()
        wallet = str(wallet or "").lower()
        if not wallet:
            return
        category = str(category or "").lower()
        key = f"{category}:{wallet}" if category else wallet
        row = {"wallet": wallet, "reason": reason, "quarantined_at": int(ts)}
        if category:
            row["category"] = category
        if details:
            row["details"] = details
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wallet_quarantine(wallet, reason, quarantined_at, raw_json)
                VALUES (?, ?, ?, ?)
                """,
                (key, reason, int(ts), _dumps(row)),
            )

    def load_wallet_quarantine(self, *, category: str | None = None) -> dict[str, dict[str, Any]]:
        self.init_db()
        category = str(category or "").lower()
        with self.connect() as conn:
            rows = conn.execute("SELECT wallet, raw_json FROM wallet_quarantine ORDER BY quarantined_at DESC").fetchall()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row["wallet"]).lower()
            value = _loads(row["raw_json"], {})
            value_category = str(value.get("category") or "").lower()
            if category and value_category and value_category != category:
                continue
            if category and not value_category and category != "esports":
                continue
            result_key = str(value.get("wallet") or key).lower() if category else key
            result[result_key] = value
        return result

    def clear_wallet_quarantine_reasons(self, reasons: set[str]) -> None:
        self.init_db()
        clean = {str(reason).strip() for reason in reasons if str(reason).strip()}
        if not clean:
            return
        placeholders = ",".join("?" for _ in clean)
        with self.connect() as conn:
            conn.execute(f"DELETE FROM wallet_quarantine WHERE reason IN ({placeholders})", tuple(sorted(clean)))

    def clear_wallet_quarantine_except(self, wallets: set[str]) -> None:
        self.init_db()
        wallets = {str(wallet).lower() for wallet in wallets if wallet}
        with self.connect() as conn:
            if not wallets:
                conn.execute("DELETE FROM wallet_quarantine")
                return
            placeholders = ",".join("?" for _ in wallets)
            conn.execute(f"DELETE FROM wallet_quarantine WHERE wallet NOT IN ({placeholders})", tuple(sorted(wallets)))

    def clear_wallet_quarantine_wallets(self, wallets: set[str]) -> None:
        self.init_db()
        clean = {str(wallet).lower() for wallet in wallets if str(wallet).strip()}
        if not clean:
            return
        expanded = set(clean)
        for value in list(clean):
            if ":" in value:
                expanded.add(value.split(":", 1)[1])
            else:
                expanded.add(f"esports:{value}")
        placeholders = ",".join("?" for _ in expanded)
        with self.connect() as conn:
            conn.execute(f"DELETE FROM wallet_quarantine WHERE wallet IN ({placeholders})", tuple(sorted(expanded)))

    def clear_revalidated_quarantine(self, wallets: set[str], *, validated_at: int) -> None:
        self.init_db()
        wallets = {str(wallet).lower() for wallet in wallets if wallet}
        if not wallets or validated_at <= 0:
            return
        expanded_wallets = set(wallets)
        expanded_wallets.update(f"esports:{wallet}" for wallet in wallets)
        placeholders = ",".join("?" for _ in expanded_wallets)
        with self.connect() as conn:
            conn.execute(
                f"""
                DELETE FROM wallet_quarantine
                WHERE wallet IN ({placeholders})
                  AND quarantined_at < ?
                  AND reason != ?
                """,
                    (*tuple(sorted(expanded_wallets)), int(validated_at), "manual_dashboard_quarantine"),
            )

    def upsert_wallet_favorite(
        self,
        wallet: str,
        *,
        category: str = "esports",
        favorite: bool = True,
        ts: int | None = None,
        snapshot: dict[str, Any] | None = None,
    ) -> None:
        self.init_db()
        wallet = str(wallet or "").lower()
        category = str(category or "esports").lower()
        if not wallet or not category:
            return
        key = f"{category}:{wallet}"
        with self.connect() as conn:
            if not favorite:
                conn.execute("DELETE FROM wallet_favorites WHERE wallet_key = ?", (key,))
                return
            favorited_at = int(ts or time.time())
            compact_snapshot = dict(snapshot or {})
            if compact_snapshot:
                compact_snapshot["wallet"] = str(compact_snapshot.get("wallet") or wallet).lower()
                compact_snapshot.setdefault("category", category)
            row = {
                "wallet": wallet,
                "category": category,
                "favorited_at": favorited_at,
                "snapshot": compact_snapshot,
            }
            conn.execute(
                """
                INSERT OR REPLACE INTO wallet_favorites(wallet_key, wallet, category, favorited_at, raw_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, wallet, category, favorited_at, _dumps(row)),
            )

    def load_wallet_favorites(self, *, category: str | None = None) -> dict[str, dict[str, Any]]:
        self.init_db()
        category = str(category or "").lower()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT wallet_key, wallet, category, raw_json FROM wallet_favorites ORDER BY favorited_at DESC"
            ).fetchall()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            row_category = str(row["category"] or "").lower()
            wallet = str(row["wallet"] or "").lower()
            if category and row_category != category:
                continue
            value = _loads(row["raw_json"], {})
            if not isinstance(value, dict):
                value = {}
            value.setdefault("wallet", wallet)
            value.setdefault("category", row_category)
            result_key = wallet if category else str(row["wallet_key"]).lower()
            result[result_key] = value
        return result

    def is_wallet_favorite(self, wallet: str, *, category: str = "esports") -> bool:
        wallet = str(wallet or "").lower()
        category = str(category or "esports").lower()
        if not wallet or not category:
            return False
        return f"{category}:{wallet}" in self.load_wallet_favorites()

    def save_market_cache(self, markets: dict[str, dict[str, Any]], *, cache_kind: str, updated_at: int) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute("DELETE FROM market_cache WHERE cache_kind = ?", (cache_kind,))
            for condition_id, market in sorted(markets.items()):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO market_cache(condition_id, cache_kind, updated_at, raw_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (condition_id, cache_kind, updated_at, _dumps(market)),
                )

    def load_market_cache(
        self,
        *,
        cache_kind: str,
        now_ts: int,
        ttl_seconds: int,
    ) -> tuple[dict[str, dict[str, Any]], int, bool]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT condition_id, updated_at, raw_json FROM market_cache WHERE cache_kind = ?",
                (cache_kind,),
            ).fetchall()
        if not rows:
            return {}, 0, False
        updated_at = max(int(row["updated_at"] or 0) for row in rows)
        fresh = now_ts - updated_at < ttl_seconds
        markets = {str(row["condition_id"]): _loads(row["raw_json"], {}) for row in rows}
        return markets, updated_at, fresh

    def load_performance(self) -> dict[str, Any]:
        self.init_db()
        with self.connect() as conn:
            total = conn.execute("SELECT raw_json FROM performance_total WHERE id = 1").fetchone()
            wallet_rows = conn.execute("SELECT wallet, raw_json FROM wallet_performance").fetchall()
            updated = conn.execute("SELECT value FROM meta WHERE key = 'performance_updated_at'").fetchone()
        if not total and not wallet_rows:
            return {}
        performance = {
            "wallets": {str(row["wallet"]): _loads(row["raw_json"], {}) for row in wallet_rows},
            "total": _loads(total["raw_json"], {}) if total else {},
        }
        if updated:
            performance["updated_at"] = int(updated["value"] or 0)
        return performance

    def read_meta_int(self, key: str, conn: sqlite3.Connection | None = None) -> int:
        def read_from(active: sqlite3.Connection) -> int:
            try:
                row = active.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            except sqlite3.Error:
                return 0
            if not row:
                return 0
            try:
                return int(row["value"] or 0)
            except (TypeError, ValueError):
                return 0

        if conn is not None:
            return read_from(conn)
        readonly = self.connect_readonly()
        if readonly is None:
            return 0
        try:
            return read_from(readonly)
        finally:
            readonly.close()

    def load_dashboard_snapshot(self) -> dict[str, Any]:
        snapshot = {
            "db_ready": False,
            "wallet_trade_state": {},
            "open_signals": [],
            "results": [],
            "performance": {},
        }
        conn = self.connect_readonly()
        if conn is None:
            return snapshot
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            required = {"wallet_cursors", "follow_signals", "follow_results", "wallet_performance", "performance_total"}
            if not required.issubset(tables):
                return snapshot
            cursor_rows = conn.execute("SELECT wallet, raw_json FROM wallet_cursors").fetchall()
            open_rows = conn.execute(
                "SELECT raw_json FROM follow_signals WHERE status = 'open' ORDER BY created_at, signal_id"
            ).fetchall()
            result_rows = conn.execute("SELECT raw_json FROM follow_results ORDER BY resolved_at, signal_id").fetchall()
            total = conn.execute("SELECT raw_json FROM performance_total WHERE id = 1").fetchone()
            wallet_rows = conn.execute("SELECT wallet, raw_json FROM wallet_performance").fetchall()
            updated = conn.execute("SELECT value FROM meta WHERE key = 'performance_updated_at'").fetchone() if "meta" in tables else None
        except sqlite3.Error:
            return snapshot
        finally:
            conn.close()
        performance = {}
        if total or wallet_rows:
            performance = {
                "wallets": {str(row["wallet"]): _loads(row["raw_json"], {}) for row in wallet_rows},
                "total": _loads(total["raw_json"], {}) if total else {},
            }
            if updated:
                performance["updated_at"] = int(updated["value"] or 0)
        return {
            "db_ready": True,
            "wallet_trade_state": {str(row["wallet"]): _loads(row["raw_json"], {}) for row in cursor_rows},
            "open_signals": [_loads(row["raw_json"], {}) for row in open_rows],
            "results": [_loads(row["raw_json"], {}) for row in result_rows],
            "performance": performance,
        }

    def dashboard_db_ready(self) -> bool:
        conn = self.connect_readonly()
        if conn is None:
            return False
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            return {"follow_signals", "follow_results"}.issubset(tables)
        except sqlite3.Error:
            return False
        finally:
            conn.close()

    def load_dashboard_open_signals(self) -> dict[str, Any]:
        empty = {"db_ready": False, "open_signals": []}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if "follow_signals" not in tables:
                return empty
            rows = conn.execute(
                "SELECT raw_json FROM follow_signals WHERE status = 'open' ORDER BY created_at, signal_id"
            ).fetchall()
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        return {"db_ready": True, "open_signals": [_loads(row["raw_json"], {}) for row in rows]}

    def load_dashboard_performance(self) -> dict[str, Any]:
        empty = {"db_ready": False, "performance": {}}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if not {"wallet_performance", "performance_total"}.issubset(tables):
                return empty
            total = conn.execute("SELECT raw_json FROM performance_total WHERE id = 1").fetchone()
            wallet_rows = conn.execute("SELECT wallet, raw_json FROM wallet_performance").fetchall()
            updated = conn.execute("SELECT value FROM meta WHERE key = 'performance_updated_at'").fetchone() if "meta" in tables else None
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        performance = {}
        if total or wallet_rows:
            performance = {
                "wallets": {str(row["wallet"]): _loads(row["raw_json"], {}) for row in wallet_rows},
                "total": _loads(total["raw_json"], {}) if total else {},
            }
            if updated:
                performance["updated_at"] = int(updated["value"] or 0)
        return {"db_ready": True, "performance": performance}

    def load_dashboard_wallet_quarantine(self) -> dict[str, Any]:
        empty = {"db_ready": False, "wallet_quarantine": {}}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if "wallet_quarantine" not in tables:
                return empty
            rows = conn.execute("SELECT wallet, raw_json FROM wallet_quarantine ORDER BY quarantined_at DESC").fetchall()
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        return {
            "db_ready": True,
            "wallet_quarantine": {str(row["wallet"]).lower(): _loads(row["raw_json"], {}) for row in rows},
        }

    def load_dashboard_wallet_favorites(self) -> dict[str, Any]:
        empty = {"db_ready": False, "wallet_favorites": {}}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if "wallet_favorites" not in tables:
                return empty
            rows = conn.execute("SELECT wallet_key, raw_json FROM wallet_favorites ORDER BY favorited_at DESC").fetchall()
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        return {
            "db_ready": True,
            "wallet_favorites": {str(row["wallet_key"]).lower(): _loads(row["raw_json"], {}) for row in rows},
        }

    def load_dashboard_follow_rows(
        self,
        *,
        page: int,
        size: int,
    ) -> dict[str, Any]:
        empty = {"db_ready": False, "total": 0, "signals": []}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        offset = max(0, (page - 1) * size)
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if not {"follow_signals", "follow_results"}.issubset(tables):
                return empty
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM (
                    SELECT condition_id FROM follow_signals WHERE condition_id IS NOT NULL AND condition_id != ''
                    UNION
                    SELECT condition_id FROM follow_results WHERE condition_id IS NOT NULL AND condition_id != ''
                )
                """
            ).fetchone()
            condition_rows = conn.execute(
                """
                SELECT condition_id, MAX(last_activity_at) AS last_activity_at
                FROM (
                    SELECT condition_id, MAX(COALESCE(updated_at, created_at, 0)) AS last_activity_at
                    FROM follow_signals
                    WHERE condition_id IS NOT NULL AND condition_id != ''
                    GROUP BY condition_id
                    UNION ALL
                    SELECT condition_id, MAX(COALESCE(resolved_at, 0)) AS last_activity_at
                    FROM follow_results
                    WHERE condition_id IS NOT NULL AND condition_id != ''
                    GROUP BY condition_id
                )
                GROUP BY condition_id
                ORDER BY last_activity_at DESC, condition_id ASC
                LIMIT ? OFFSET ?
                """,
                (size, offset),
            ).fetchall()
            condition_ids = [str(row["condition_id"]).lower() for row in condition_rows]
            signals = self._dashboard_signals_for_conditions(conn, condition_ids)
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        return {
            "db_ready": True,
            "total": int(total_row["count"] or 0) if total_row else 0,
            "signals": signals,
        }

    def load_dashboard_follow_detail(self, condition_id: str) -> dict[str, Any]:
        empty = {"db_ready": False, "signals": []}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        condition_id = str(condition_id or "").lower()
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if not {"follow_signals", "follow_results"}.issubset(tables):
                return empty
            signals = self._dashboard_signals_for_conditions(conn, [condition_id])
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        return {"db_ready": True, "signals": signals}

    def load_dashboard_wallet_follow_detail(self, wallet: str, *, statuses: set[str] | None = None) -> dict[str, Any]:
        empty = {"db_ready": False, "signals": []}
        conn = self.connect_readonly()
        if conn is None:
            return empty
        wallet = str(wallet or "").lower()
        statuses = {str(status).lower() for status in (statuses or set()) if status}
        try:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if not {"follow_signals", "follow_results"}.issubset(tables):
                return empty
            rows = conn.execute(
                """
                SELECT raw_json, COALESCE(updated_at, created_at, 0) AS sort_at, signal_id
                FROM follow_signals
                WHERE lower(wallet) = ?
                UNION ALL
                SELECT raw_json, COALESCE(resolved_at, 0) AS sort_at, signal_id
                FROM follow_results
                WHERE lower(wallet) = ?
                ORDER BY sort_at DESC, signal_id ASC
                """,
                (wallet, wallet),
            ).fetchall()
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        by_id = {}
        for row in rows:
            signal = _loads(row["raw_json"], {})
            status = str(signal.get("status") or "").lower()
            if statuses and status not in statuses:
                continue
            signal_id = str(signal.get("signal_id") or row["signal_id"] or "")
            if signal_id and signal_id not in by_id:
                by_id[signal_id] = signal
        return {"db_ready": True, "signals": list(by_id.values())}

    def _dashboard_signals_for_conditions(
        self,
        conn: sqlite3.Connection,
        condition_ids: list[str],
    ) -> list[dict[str, Any]]:
        if not condition_ids:
            return []
        placeholders = ",".join("?" for _ in condition_ids)
        rows = conn.execute(
            f"""
            SELECT raw_json, COALESCE(updated_at, created_at, 0) AS sort_at, signal_id
            FROM follow_signals
            WHERE lower(condition_id) IN ({placeholders})
            UNION ALL
            SELECT raw_json, COALESCE(resolved_at, 0) AS sort_at, signal_id
            FROM follow_results
            WHERE lower(condition_id) IN ({placeholders})
            ORDER BY sort_at DESC, signal_id ASC
            """,
            (*condition_ids, *condition_ids),
        ).fetchall()
        by_id = {}
        for row in rows:
            signal = _loads(row["raw_json"], {})
            signal_id = str(signal.get("signal_id") or row["signal_id"] or "")
            if signal_id and signal_id not in by_id:
                by_id[signal_id] = signal
        return list(by_id.values())

    def save_follow_snapshot(
        self,
        *,
        wallet_trade_state: dict[str, dict[str, Any]],
        open_signals: list[dict[str, Any]],
        result_events: list[dict[str, Any]],
        performance: dict[str, Any],
    ) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM wallet_cursors")
            for wallet, row in sorted(wallet_trade_state.items()):
                cursor = row.get("last_trade_cursor") or {}
                conn.execute(
                    """
                    INSERT OR REPLACE INTO wallet_cursors
                    (wallet, last_trade_timestamp, last_trade_id, last_seen_at, raw_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        wallet,
                        int(cursor.get("timestamp") or 0),
                        str(cursor.get("id") or ""),
                        int(row.get("last_seen_at") or 0),
                        _dumps(row),
                    ),
                )

            conn.execute("DELETE FROM follow_signals WHERE status = 'open'")
            for signal in open_signals:
                self._upsert_signal(conn, signal)

            for event in result_events:
                self._upsert_result(conn, event)

            self._save_performance(conn, performance)
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('follow_snapshot_updated_at', ?)",
                (str(int(time.time())),),
            )
            conn.commit()

    def _upsert_signal(self, conn: sqlite3.Connection, signal: dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id") or "")
        if not signal_id:
            return
        wallet = str(signal.get("wallet") or "")
        condition_id = str(signal.get("condition_id") or "")
        conn.execute(
            """
            INSERT OR REPLACE INTO follow_signals
            (signal_id, status, wallet, condition_id, outcome_index, created_at, updated_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                str(signal.get("status") or "open"),
                wallet,
                condition_id,
                int(signal.get("outcome_index") or 0),
                int(signal.get("created_at") or 0),
                int(signal.get("updated_at") or 0),
                _dumps(signal),
            ),
        )
        conn.execute("DELETE FROM follow_legs WHERE signal_id = ?", (signal_id,))
        for leg in signal.get("legs") or []:
            trade_id = str(leg.get("trade_id") or f"{signal_id}:{leg.get('leg_at')}")
            conn.execute(
                """
                INSERT OR REPLACE INTO follow_legs
                (signal_id, trade_id, wallet, condition_id, leg_at, stake, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    trade_id,
                    wallet,
                    condition_id,
                    int(leg.get("leg_at") or 0),
                    float(leg.get("stake") or 0),
                    _dumps(leg),
                ),
            )
        conn.execute("DELETE FROM follow_behavior_events WHERE signal_id = ?", (signal_id,))
        for event in signal.get("behavior_events") or []:
            conn.execute(
                "INSERT INTO follow_behavior_events(signal_id, kind, timestamp, raw_json) VALUES (?, ?, ?, ?)",
                (signal_id, str(event.get("kind") or ""), int(event.get("timestamp") or 0), _dumps(event)),
            )

    def _upsert_result(self, conn: sqlite3.Connection, result: dict[str, Any]) -> None:
        signal_id = str(result.get("signal_id") or "")
        if not signal_id:
            return
        self._upsert_signal(conn, result)
        resolved_at = int(result.get("settled_at") or result.get("exit_at") or result.get("updated_at") or 0)
        conn.execute(
            """
            INSERT OR REPLACE INTO follow_results
            (signal_id, status, wallet, condition_id, resolved_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                str(result.get("status") or ""),
                str(result.get("wallet") or ""),
                str(result.get("condition_id") or ""),
                resolved_at,
                _dumps(result),
            ),
        )

    def _save_performance(self, conn: sqlite3.Connection, performance: dict[str, Any]) -> None:
        conn.execute("DELETE FROM wallet_performance")
        for wallet, row in sorted((performance.get("wallets") or {}).items()):
            conn.execute(
                "INSERT OR REPLACE INTO wallet_performance(wallet, raw_json) VALUES (?, ?)",
                (wallet, _dumps(row)),
            )
        conn.execute(
            "INSERT OR REPLACE INTO performance_total(id, raw_json) VALUES (1, ?)",
            (_dumps(performance.get("total") or {}),),
        )
        if performance.get("updated_at") is not None:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('performance_updated_at', ?)",
                (str(int(performance.get("updated_at") or 0)),),
            )

    def import_legacy_json(
        self,
        *,
        state_path: Path,
        open_path: Path,
        results_path: Path,
        perf_path: Path,
    ) -> bool:
        self.init_db()
        with self.connect() as conn:
            existing = conn.execute("SELECT COUNT(*) AS count FROM meta WHERE key = 'legacy_imported'").fetchone()
            if existing and int(existing["count"] or 0):
                return False
            existing_rows = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM wallet_cursors)
                + (SELECT COUNT(*) FROM follow_signals)
                + (SELECT COUNT(*) FROM follow_results) AS count
                """
            ).fetchone()
            if existing_rows and int(existing_rows["count"] or 0):
                conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('legacy_imported', '1')")
                return False
        state = _read_json(state_path, {})
        open_signals = _read_json(open_path, [])
        results = _read_jsonl(results_path)
        performance = _read_json(perf_path, {})
        if results:
            from .follow import aggregate_follow_performance

            performance = aggregate_follow_performance({}, results)
        self.save_follow_snapshot(
            wallet_trade_state=state.get("wallet_trade_state") or {},
            open_signals=open_signals if isinstance(open_signals, list) else [],
            result_events=results,
            performance=performance if isinstance(performance, dict) else {},
        )
        with self.connect() as conn:
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('legacy_imported', '1')")
        return True
