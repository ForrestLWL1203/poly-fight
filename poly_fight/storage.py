from __future__ import annotations

import json
import math
import secrets
import sqlite3
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .follow_strategy import ACTIVE_FOLLOW_STRATEGY_ID, normalize_follow_strategy, validate_follow_strategy

class _AutoCloseConnection(sqlite3.Connection):
    """`with self.connect() as conn:` 退出时提交/回滚后**关闭连接**。

    原生 sqlite3 的 connection-as-context-manager 只 commit/rollback、**不 close** →
    连接靠引用计数/GC 回收,长驻 runner 的热路径里每 tick 开多个连接,GC 滞后时 fd
    越堆越多 → 撞进程 fd 上限(1024)→ EMFILE → 连 SQLite 都 "unable to open database
    file" → runner 连错自停(实测 ~25min 撞顶、多次拖垮生产)。此处用完即关,根治。"""

    def __exit__(self, *exc: Any) -> Any:
        try:
            return super().__exit__(*exc)
        finally:
            self.close()


LEADERBOARD_SCHEMA_VERSION = 2
FOLLOW_SCHEMA_VERSION = 3

# run_ticks 是 runner 心跳日志(每行含完整 stats blob),dashboard 只读最近若干条。
# 保留最近 RUN_TICKS_RETENTION 条,写入时裁掉更旧的,防止该表无界增长。
RUN_TICKS_RETENTION = 5000


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _timestamp(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }


def _first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _best_bucket(row: dict[str, Any]) -> str:
    direct = str(row.get("best_bucket") or "").strip()
    if direct:
        return direct
    buckets = row.get("eligible_buckets")
    if isinstance(buckets, list) and buckets:
        return str(buckets[0] or "")
    per_game = row.get("per_game_type") or row.get("per_game_type_grades")
    if isinstance(per_game, dict) and per_game:
        return str(next(iter(per_game)))
    return str(row.get("best_market_type") or row.get("market_type") or "")


def _best_bucket_score(row: dict[str, Any]) -> float:
    direct = row.get("best_bucket_score")
    if direct not in (None, ""):
        return _to_float(direct)
    bucket = _best_bucket(row)
    for key in ("per_game_type", "per_game_type_grades", "per_type", "per_type_grades"):
        values = row.get(key)
        if isinstance(values, dict) and isinstance(values.get(bucket), dict):
            bucket_row = values[bucket]
            return _to_float(
                _first_value(
                    bucket_row,
                    ("score", "quality_score", "bucket_score", "wilson_win_rate_lower_bound", "positive_market_rate"),
                )
            )
    return _to_float(_first_value(row, ("score", "quality_score", "wilson_win_rate_lower_bound", "positive_market_rate")))


def _last_trade_at(row: dict[str, Any]) -> int:
    return _timestamp(
        _first_value(
            row,
            (
                "last_trade_at",
                "best_bucket_last_trade_at",
                "last_esports_trade_at",
                "last_sports_trade_at",
                "last_category_trade_at",
                "profiled_at",
            ),
        )
    )


def _market_count(row: dict[str, Any]) -> int:
    return _to_int(
        _first_value(
            row,
            (
                "participated_market_count",
                "historical_trade_behavior_market_count",
                "esports_closed_count",
                "sports_closed_count",
                "closed_market_count",
                "market_count",
            ),
        )
    )


def _cash_volume(row: dict[str, Any]) -> float:
    return _to_float(
        _first_value(
            row,
            (
                "total_cash_volume",
                "cash_volume",
                "esports_total_cash_volume",
                "sports_total_cash_volume",
                "esports_total_cost",
                "sports_total_cost",
                "total_cost",
                "total_holder_usd",
            ),
        )
    )


def _avg_market_cash(row: dict[str, Any]) -> float:
    direct = _to_float(_first_value(row, ("avg_market_cash", "avg_market_usd")))
    if direct:
        return direct
    count = _market_count(row)
    total = _cash_volume(row)
    return round(total / count, 8) if count > 0 and total else 0.0


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
        conn = sqlite3.connect(self.path, factory=_AutoCloseConnection)
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
                    grade TEXT,
                    league TEXT,
                    best_bucket TEXT,
                    best_bucket_score REAL,
                    scoring_version INTEGER,
                    last_trade_at INTEGER,
                    positive_market_rate REAL,
                    avg_market_cash REAL,
                    participated_market_count INTEGER,
                    total_cash_volume REAL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (category, wallet)
                );
                CREATE TABLE IF NOT EXISTS wallet_profiles (
                    category TEXT NOT NULL,
                    wallet TEXT NOT NULL,
                    grade TEXT,
                    profile_state TEXT,
                    profiled_at INTEGER,
                    scoring_version INTEGER,
                    last_trade_at INTEGER,
                    profile_lookback_days INTEGER,
                    best_bucket TEXT,
                    esports_roi REAL,
                    positive_market_rate REAL,
                    avg_market_cash REAL,
                    participated_market_count INTEGER,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (category, wallet)
                );
                CREATE TABLE IF NOT EXISTS collection_runs (
                    run_id TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    collector TEXT,
                    created_at INTEGER,
                    published_at INTEGER NOT NULL,
                    classification_market_count INTEGER,
                    target_market_count INTEGER,
                    seed_wallet_count INTEGER,
                    profile_wallet_count INTEGER,
                    profiled_wallet_count INTEGER,
                    leaderboard_wallet_count INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS observe_analyzed (
                    condition_id TEXT PRIMARY KEY,
                    analyzed_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_observe_analyzed_at
                    ON observe_analyzed(analyzed_at);
                CREATE INDEX IF NOT EXISTS idx_leaderboard_wallets_category_rank
                    ON leaderboard_wallets(category, rank);
                CREATE INDEX IF NOT EXISTS idx_wallet_profiles_category_wallet
                    ON wallet_profiles(category, wallet);
                CREATE INDEX IF NOT EXISTS idx_collection_runs_category_published
                    ON collection_runs(category, published_at);
                """
            )
            _ensure_columns(
                conn,
                "leaderboard_wallets",
                {
                    "grade": "TEXT",
                    "league": "TEXT",
                    "best_bucket": "TEXT",
                    "best_bucket_score": "REAL",
                    "scoring_version": "INTEGER",
                    "last_trade_at": "INTEGER",
                    "positive_market_rate": "REAL",
                    "avg_market_cash": "REAL",
                    "participated_market_count": "INTEGER",
                    "total_cash_volume": "REAL",
                },
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(LEADERBOARD_SCHEMA_VERSION),),
            )

    def _insert_leaderboard_rows(
        self,
        conn: sqlite3.Connection,
        rows: list[dict[str, Any]],
        *,
        category: str,
        updated_at: int,
    ) -> None:
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
                INSERT OR REPLACE INTO leaderboard_wallets(
                    category, wallet, rank, updated_at, grade, league, best_bucket,
                    best_bucket_score, scoring_version, last_trade_at,
                    positive_market_rate, avg_market_cash, participated_market_count,
                    total_cash_volume, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    category,
                    wallet,
                    index,
                    updated_at,
                    str(payload.get("grade") or ""),
                    str(payload.get("league") or ""),
                    _best_bucket(payload),
                    _best_bucket_score(payload),
                    _to_int(payload.get("scoring_version")),
                    _last_trade_at(payload),
                    _to_float(payload.get("positive_market_rate")),
                    _avg_market_cash(payload),
                    _market_count(payload),
                    _cash_volume(payload),
                    _dumps(payload),
                ),
            )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
            (f"{category}:updated_at", str(updated_at)),
        )

    def _insert_wallet_profiles(
        self,
        conn: sqlite3.Connection,
        profiles: list[dict[str, Any]],
        *,
        category: str,
    ) -> None:
        conn.execute("DELETE FROM wallet_profiles WHERE category = ?", (category,))
        for row in profiles:
            if not isinstance(row, dict):
                continue
            wallet = str(row.get("wallet") or "").lower()
            if not wallet:
                continue
            payload = dict(row)
            payload.setdefault("category", category)
            conn.execute(
                """
                INSERT OR REPLACE INTO wallet_profiles(
                    category, wallet, grade, profile_state, profiled_at,
                    scoring_version, last_trade_at, profile_lookback_days,
                    best_bucket, esports_roi, positive_market_rate, avg_market_cash,
                    participated_market_count, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    category,
                    wallet,
                    str(payload.get("grade") or ""),
                    str(payload.get("profile_state") or payload.get("state") or ""),
                    _timestamp(payload.get("profiled_at")),
                    _to_int(payload.get("scoring_version")),
                    _last_trade_at(payload),
                    _to_int(payload.get("profile_lookback_days") or payload.get("lookback_days")),
                    _best_bucket(payload),
                    _to_float(payload.get("esports_roi")),
                    _to_float(payload.get("positive_market_rate")),
                    _avg_market_cash(payload),
                    _market_count(payload),
                    _dumps(payload),
                ),
            )

    def replace_leaderboard(self, rows: list[dict[str, Any]], *, category: str, updated_at: int) -> None:
        category = str(category or "esports").lower()
        updated_at = int(updated_at or time.time())
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN")
            self._insert_leaderboard_rows(conn, rows, category=category, updated_at=updated_at)
            conn.execute("COMMIT")

    def publish_collection(
        self,
        *,
        category: str,
        leaderboard: list[dict[str, Any]],
        profiles: list[dict[str, Any]] | dict[str, dict[str, Any]],
        summary: dict[str, Any] | None,
        updated_at: int,
    ) -> dict[str, Any]:
        category = str(category or "esports").lower()
        updated_at = int(updated_at or time.time())
        profile_rows = list(profiles.values()) if isinstance(profiles, dict) else list(profiles or [])
        summary_payload = dict(summary) if isinstance(summary, dict) else {}
        summary_payload.setdefault("category", category)
        summary_payload.setdefault("published_at", updated_at)
        summary_payload.setdefault("updated_at", updated_at)
        summary_payload.setdefault("profile_wallet_count", len(profile_rows))
        summary_payload.setdefault("profiled_wallet_count", len(profile_rows))
        summary_payload.setdefault("leaderboard_wallet_count", len(leaderboard or []))
        created_at = _timestamp(summary_payload.get("created_at")) or updated_at
        digest = hashlib.sha1(_dumps(summary_payload).encode("utf-8")).hexdigest()[:12]
        run_id = str(summary_payload.get("run_id") or f"{category}:{updated_at}:{digest}")
        summary_payload["run_id"] = run_id
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN")
            self._insert_leaderboard_rows(conn, leaderboard or [], category=category, updated_at=updated_at)
            self._insert_wallet_profiles(conn, profile_rows, category=category)
            conn.execute(
                """
                INSERT OR REPLACE INTO collection_runs(
                    run_id, category, collector, created_at, published_at,
                    classification_market_count, target_market_count, seed_wallet_count,
                    profile_wallet_count, profiled_wallet_count, leaderboard_wallet_count,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    category,
                    str(summary_payload.get("collector") or ""),
                    created_at,
                    updated_at,
                    _to_int(summary_payload.get("classification_market_count")),
                    _to_int(summary_payload.get("target_market_count") or summary_payload.get("discovery_market_count")),
                    _to_int(summary_payload.get("seed_wallet_count") or summary_payload.get("candidate_wallet_count")),
                    _to_int(summary_payload.get("profile_wallet_count")),
                    _to_int(summary_payload.get("profiled_wallet_count")),
                    _to_int(summary_payload.get("leaderboard_wallet_count")),
                    _dumps(summary_payload),
                ),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (f"{category}:collection_run_updated_at", str(updated_at)),
            )
            conn.execute("COMMIT")
        return summary_payload

    def load_observe_analyzed(self, *, now_ts: int, retain_days: int = 7) -> dict[str, int]:
        """M4 已分析结算盘 condition_id → analyzed_at(只返回 retain_days 内的;只读)。"""
        conn = self.connect_readonly()
        if conn is None:
            return {}
        try:
            cutoff = int(now_ts) - max(1, int(retain_days)) * 86400
            rows = conn.execute(
                "SELECT condition_id, analyzed_at FROM observe_analyzed WHERE analyzed_at >= ?",
                (cutoff,),
            ).fetchall()
            return {str(row["condition_id"]).lower(): int(row["analyzed_at"]) for row in rows}
        except sqlite3.Error:
            return {}
        finally:
            conn.close()

    def record_observe_analyzed(self, condition_ids, *, now_ts: int, retain_days: int = 7) -> None:
        """记录这批已分析结算盘 + 顺手剪枝超 retain_days 的旧项(防膨胀)。"""
        self.init_db()
        cutoff = int(now_ts) - max(1, int(retain_days)) * 86400
        with self.connect() as conn:
            conn.execute("BEGIN")
            for cid in condition_ids:
                cid = str(cid or "").lower()
                if cid:
                    conn.execute(
                        "INSERT OR REPLACE INTO observe_analyzed(condition_id, analyzed_at) VALUES(?, ?)",
                        (cid, int(now_ts)),
                    )
            conn.execute("DELETE FROM observe_analyzed WHERE analyzed_at < ?", (cutoff,))
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

    def load_wallet_profiles(self, *, category: str | None = None) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "wallet_profiles" not in _table_names(conn):
                return []
            params: tuple[Any, ...] = ()
            where = ""
            if category:
                where = "WHERE category = ?"
                params = (str(category).lower(),)
            rows = conn.execute(
                f"SELECT category, wallet, raw_json FROM wallet_profiles {where} ORDER BY category, wallet",
                params,
            ).fetchall()
            values = []
            for row in rows:
                payload = _loads(row["raw_json"], {})
                if isinstance(payload, dict):
                    payload.setdefault("category", str(row["category"] or "").lower())
                    payload.setdefault("wallet", str(row["wallet"] or "").lower())
                    values.append(payload)
            return values
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def load_latest_collection_run(self, *, category: str | None = None) -> dict[str, Any]:
        conn = self.connect_readonly()
        if conn is None:
            return {}
        try:
            if "collection_runs" not in _table_names(conn):
                return {}
            params: tuple[Any, ...] = ()
            where = ""
            if category:
                where = "WHERE category = ?"
                params = (str(category).lower(),)
            row = conn.execute(
                f"""
                SELECT *
                FROM collection_runs
                {where}
                ORDER BY published_at DESC, run_id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
            if not row:
                return {}
            payload = _loads(row["raw_json"], {})
            if not isinstance(payload, dict):
                payload = {}
            for key in (
                "run_id",
                "category",
                "collector",
                "created_at",
                "published_at",
                "classification_market_count",
                "target_market_count",
                "seed_wallet_count",
                "profile_wallet_count",
                "profiled_wallet_count",
                "leaderboard_wallet_count",
            ):
                payload.setdefault(key, row[key])
            return payload
        except sqlite3.Error:
            return {}
        finally:
            conn.close()

    def latest_scoring_version(self, *, category: str | None = None) -> int | None:
        conn = self.connect_readonly()
        if conn is None:
            return None
        try:
            tables = _table_names(conn)
            params: tuple[Any, ...] = ()
            where = ""
            if category:
                where = "WHERE category = ?"
                params = (str(category).lower(),)
            versions: list[int] = []
            if "leaderboard_wallets" in tables:
                row = conn.execute(
                    f"SELECT MAX(COALESCE(scoring_version, 0)) AS version FROM leaderboard_wallets {where}",
                    params,
                ).fetchone()
                if row:
                    versions.append(_to_int(row["version"]))
            if "wallet_profiles" in tables:
                row = conn.execute(
                    f"SELECT MAX(COALESCE(scoring_version, 0)) AS version FROM wallet_profiles {where}",
                    params,
                ).fetchone()
                if row:
                    versions.append(_to_int(row["version"]))
            version = max(versions or [0])
            return version or None
        except sqlite3.Error:
            return None
        finally:
            conn.close()


class FollowStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._initialized = False

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, factory=_AutoCloseConnection)
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
                    cache_kind TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    category TEXT,
                    league TEXT,
                    market_type TEXT,
                    event_slug TEXT,
                    title TEXT,
                    question TEXT,
                    match_start_ts INTEGER,
                    end_ts INTEGER,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (cache_kind, condition_id)
                );
                CREATE TABLE IF NOT EXISTS run_ticks (
                    tick_id TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL,
                    status TEXT,
                    gate_open INTEGER,
                    watched_market_count INTEGER,
                    open_signal_count INTEGER,
                    new_signal_count INTEGER,
                    tick_runtime_seconds REAL,
                    desired_next_interval_seconds INTEGER,
                    error TEXT,
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
                CREATE TABLE IF NOT EXISTS m5_demote_cooldown (
                    wallet TEXT PRIMARY KEY,
                    demoted_at INTEGER NOT NULL,
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
                CREATE TABLE IF NOT EXISTS follow_strategy (
                    id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS follow_strategy_library (
                    slug TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_match_assessments (
                    condition_id TEXT PRIMARY KEY,
                    input_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_follow_intents (
                    intent_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    wallet TEXT,
                    outcome_index INTEGER,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_shadow_positions (
                    shadow_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    wallet TEXT,
                    outcome_index INTEGER,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    shadow_kind TEXT NOT NULL DEFAULT 'wallet_original',
                    realized_pnl REAL NOT NULL DEFAULT 0,
                    ai_net_effect REAL,
                    comparison_pnl REAL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_data_cache (
                    cache_key TEXT PRIMARY KEY,
                    cache_kind TEXT NOT NULL,
                    game TEXT,
                    team_id TEXT,
                    fetched_at INTEGER NOT NULL,
                    last_used_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_backtest_cases (
                    case_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ai_proprietary_positions (
                    condition_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    outcome_index INTEGER,
                    evidence_score INTEGER NOT NULL DEFAULT 0,
                    stake_usdc REAL NOT NULL DEFAULT 0,
                    entry_price REAL,
                    realized_pnl REAL NOT NULL DEFAULT 0,
                    match_start_ts INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_follow_strategy_library_name
                    ON follow_strategy_library(name COLLATE NOCASE);
                CREATE INDEX IF NOT EXISTS idx_follow_signals_status ON follow_signals(status);
                CREATE INDEX IF NOT EXISTS idx_follow_signals_wallet ON follow_signals(wallet);
                CREATE INDEX IF NOT EXISTS idx_follow_signals_condition_id ON follow_signals(condition_id);
                CREATE INDEX IF NOT EXISTS idx_follow_legs_signal_id ON follow_legs(signal_id);
                CREATE INDEX IF NOT EXISTS idx_follow_behavior_events_signal_id ON follow_behavior_events(signal_id);
                CREATE INDEX IF NOT EXISTS idx_follow_behavior_events_kind_ts ON follow_behavior_events(kind, timestamp);
                CREATE INDEX IF NOT EXISTS idx_follow_results_status_resolved_at ON follow_results(status, resolved_at);
                CREATE INDEX IF NOT EXISTS idx_run_ticks_created_at ON run_ticks(created_at);
                CREATE INDEX IF NOT EXISTS idx_wallet_quarantine_ts ON wallet_quarantine(quarantined_at);
                CREATE INDEX IF NOT EXISTS idx_wallet_favorites_category_ts ON wallet_favorites(category, favorited_at);
                CREATE INDEX IF NOT EXISTS idx_account_balance_ledger_created_at ON account_balance_ledger(created_at);
                CREATE INDEX IF NOT EXISTS idx_ai_assessments_updated_at ON ai_match_assessments(updated_at);
                CREATE INDEX IF NOT EXISTS idx_ai_intents_condition_at ON ai_follow_intents(condition_id, updated_at);
                CREATE INDEX IF NOT EXISTS idx_ai_intents_action_at ON ai_follow_intents(action, updated_at);
                CREATE INDEX IF NOT EXISTS idx_ai_shadow_status_condition ON ai_shadow_positions(status, condition_id);
                CREATE INDEX IF NOT EXISTS idx_ai_data_cache_kind_used ON ai_data_cache(cache_kind, last_used_at);
                CREATE INDEX IF NOT EXISTS idx_ai_data_cache_team ON ai_data_cache(game, team_id);
                CREATE INDEX IF NOT EXISTS idx_ai_backtest_condition ON ai_backtest_cases(condition_id, updated_at);
                CREATE INDEX IF NOT EXISTS idx_ai_proprietary_status_at ON ai_proprietary_positions(status, updated_at);
                """
            )
            self._migrate_market_cache_schema(conn)
            _ensure_columns(
                conn,
                "ai_shadow_positions",
                {
                    "realized_pnl": "REAL NOT NULL DEFAULT 0",
                    "ai_net_effect": "REAL",
                    "shadow_kind": "TEXT NOT NULL DEFAULT 'wallet_original'",
                    "comparison_pnl": "REAL",
                },
            )
            _ensure_columns(
                conn,
                "ai_proprietary_positions",
                {"match_start_ts": "INTEGER NOT NULL DEFAULT 0"},
            )
            stale_start_rows = conn.execute(
                "SELECT condition_id,raw_json FROM ai_proprietary_positions WHERE match_start_ts=0"
            ).fetchall()
            for stale in stale_start_rows:
                value = _loads(stale["raw_json"], {})
                start_ts = _timestamp(value.get("match_start_time") or value.get("market_start_time"))
                if start_ts:
                    conn.execute(
                        "UPDATE ai_proprietary_positions SET match_start_ts=? WHERE condition_id=?",
                        (start_ts, stale["condition_id"]),
                    )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_shadow_kind_status "
                "ON ai_shadow_positions(shadow_kind, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ai_proprietary_start "
                "ON ai_proprietary_positions(match_start_ts, updated_at)"
            )
            _ensure_columns(
                conn,
                "market_cache",
                {
                    "category": "TEXT",
                    "league": "TEXT",
                    "market_type": "TEXT",
                    "event_slug": "TEXT",
                    "title": "TEXT",
                    "question": "TEXT",
                    "match_start_ts": "INTEGER",
                    "end_ts": "INTEGER",
                },
            )
            # M5 自动降级:终态结果是否已被纳入一轮重评(防重启后重复处理)。默认 0=未处理;
            # 现有历史结果迁移后 = 0,会被下一轮 M5 补处理一次(本就该评而从未评过)。
            _ensure_columns(
                conn,
                "follow_results",
                {"m5_processed": "INTEGER NOT NULL DEFAULT 0"},
            )
            conn.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_market_cache_kind ON market_cache(cache_kind);
                CREATE INDEX IF NOT EXISTS idx_market_cache_kind_start ON market_cache(cache_kind, match_start_ts);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(FOLLOW_SCHEMA_VERSION),),
            )
        self._initialized = True

    def _create_market_cache_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_cache (
                cache_kind TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                category TEXT,
                league TEXT,
                market_type TEXT,
                event_slug TEXT,
                title TEXT,
                question TEXT,
                match_start_ts INTEGER,
                end_ts INTEGER,
                updated_at INTEGER NOT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (cache_kind, condition_id)
            )
            """
        )

    def _migrate_market_cache_schema(self, conn: sqlite3.Connection) -> None:
        if "market_cache" not in _table_names(conn):
            self._create_market_cache_table(conn)
            return
        info = conn.execute("PRAGMA table_info(market_cache)").fetchall()
        pk_columns = [
            str(row["name"])
            for row in sorted((row for row in info if int(row["pk"] or 0)), key=lambda row: int(row["pk"] or 0))
        ]
        if pk_columns == ["cache_kind", "condition_id"]:
            return
        columns = {str(row["name"]) for row in info}
        select_columns = [
            "condition_id" if "condition_id" in columns else "'' AS condition_id",
            "cache_kind" if "cache_kind" in columns else "'active' AS cache_kind",
            "updated_at" if "updated_at" in columns else "0 AS updated_at",
            "raw_json" if "raw_json" in columns else "'{}' AS raw_json",
        ]
        rows = conn.execute(f"SELECT {', '.join(select_columns)} FROM market_cache").fetchall()
        conn.execute("DROP TABLE IF EXISTS market_cache_old")
        conn.execute("ALTER TABLE market_cache RENAME TO market_cache_old")
        self._create_market_cache_table(conn)
        for row in rows:
            condition_id = str(row["condition_id"] or "").lower()
            if not condition_id:
                continue
            cache_kind = str(row["cache_kind"] or "active").lower()
            updated_at = _to_int(row["updated_at"])
            payload = _loads(row["raw_json"], {})
            if not isinstance(payload, dict):
                payload = {}
            payload.setdefault("condition_id", condition_id)
            self._upsert_market_cache_item(conn, cache_kind, condition_id, payload, updated_at=updated_at)
        conn.execute("DROP TABLE IF EXISTS market_cache_old")

    def _market_cache_values(
        self,
        cache_kind: str,
        condition_id: str,
        market: dict[str, Any],
        *,
        updated_at: int,
    ) -> tuple[Any, ...] | None:
        condition_id = str(condition_id or market.get("condition_id") or market.get("conditionId") or "").lower()
        if not condition_id:
            return None
        payload = dict(market)
        payload["condition_id"] = condition_id
        payload.setdefault("cache_kind", cache_kind)
        payload.setdefault("updated_at", updated_at)
        return (
            str(cache_kind or "active").lower(),
            condition_id,
            str(payload.get("category") or "").lower(),
            str(payload.get("league") or "").lower(),
            str(payload.get("market_type") or "").lower(),
            str(payload.get("event_slug") or payload.get("eventSlug") or payload.get("slug") or ""),
            str(payload.get("title") or ""),
            str(payload.get("question") or ""),
            _timestamp(
                _first_value(
                    payload,
                    ("match_start_ts", "match_start_time", "market_start_time", "startTime", "eventStartTime", "gameStartTime"),
                )
            ),
            _timestamp(_first_value(payload, ("end_ts", "end_date", "endDate", "endTime"))),
            int(updated_at or time.time()),
            _dumps(payload),
        )

    def _upsert_market_cache_item(
        self,
        conn: sqlite3.Connection,
        cache_kind: str,
        condition_id: str,
        market: dict[str, Any],
        *,
        updated_at: int,
    ) -> None:
        values = self._market_cache_values(cache_kind, condition_id, market, updated_at=updated_at)
        if values is None:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO market_cache(
                cache_kind, condition_id, category, league, market_type, event_slug,
                title, question, match_start_ts, end_ts, updated_at, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

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

    def load_wallet_follow_activity_readonly(self, *, category: str | None = "esports") -> dict[str, int]:
        """Latest funded follow-leg time by target wallet, without mutating the DB.

        A persisted follow leg proves that the target wallet was active in a market we
        actually followed.  Collector activity gates use this operational timestamp in
        addition to the closed-market history timestamp, which can lag live positions by
        several days.  ``leg_at`` deliberately anchors the refresh to when the follow was
        created, matching the user's observable event even for position backfills whose
        original target fill time is unavailable.
        """
        conn = self.connect_readonly()
        if conn is None:
            return {}
        try:
            if "follow_legs" not in _table_names(conn):
                return {}
            rows = conn.execute(
                "SELECT wallet, leg_at, raw_json FROM follow_legs "
                "WHERE wallet IS NOT NULL AND wallet != '' AND COALESCE(leg_at, 0) > 0"
            ).fetchall()
            wanted_category = str(category or "").lower()
            latest: dict[str, int] = {}
            for row in rows:
                payload = _loads(row["raw_json"], {})
                if not isinstance(payload, dict):
                    payload = {}
                row_category = str(payload.get("category") or "esports").lower()
                if wanted_category and row_category != wanted_category:
                    continue
                # Only funded legs are written by the current follower.  Keep the
                # explicit check for defensive compatibility with older snapshots.
                funded_stake = _to_float(
                    payload.get("funded_stake")
                    if payload.get("funded_stake") is not None
                    else payload.get("stake")
                )
                if funded_stake <= 0:
                    continue
                wallet = str(row["wallet"] or "").lower()
                activity_at = _to_int(row["leg_at"])
                if wallet and activity_at > latest.get(wallet, 0):
                    latest[wallet] = activity_at
            return latest
        except (sqlite3.Error, json.JSONDecodeError):
            return {}
        finally:
            conn.close()

    def load_open_signals(self) -> list[dict[str, Any]]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT raw_json FROM follow_signals WHERE status = 'open' ORDER BY created_at, signal_id").fetchall()
        return [_loads(row["raw_json"], {}) for row in rows]

    def load_pool_refresh_at_readonly(self, *, category: str = "esports") -> int:
        """Load the last successful full collector publish without mutating the DB."""
        conn = self.connect_readonly()
        if conn is None:
            return 0
        try:
            if "meta" not in _table_names(conn):
                return 0
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?",
                (f"pool_refresh:{str(category or 'esports').lower()}:updated_at",),
            ).fetchone()
            return _to_int(row["value"]) if row else 0
        except sqlite3.Error:
            return 0
        finally:
            conn.close()

    def record_pool_refresh_at(self, *, category: str = "esports", ts: int | None = None) -> int:
        """Persist a successful full collector publish for the runner's 12h clock."""
        refreshed_at = int(ts or time.time())
        self.init_db()
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (
                    f"pool_refresh:{str(category or 'esports').lower()}:updated_at",
                    str(refreshed_at),
                ),
            )
        return refreshed_at

    def load_pending_small_buys(self) -> dict[str, dict[str, Any]]:
        """小单累加器:键 "wallet|condition_id|outcome_index" -> {cash,size,wsum,first_ts,last_ts}。
        目标钱包在可跟桶上的 BUY 未达最小下单额时缓存累加,凑够即跟、清零;卖出/结算亦清。
        存 meta JSON blob(仅 runner 读写,跨 tick 持久;每 tick load→更新→save)。"""
        self.init_db()
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'pending_small_buys'").fetchone()
        out = _loads(row["value"], {}) if row else {}
        return out if isinstance(out, dict) else {}

    def save_pending_small_buys(self, pending: dict[str, Any]) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('pending_small_buys', ?)",
                (_dumps(pending or {}),),
            )

    def load_held_pending_price(self) -> dict[str, dict[str, Any]]:
        """价格 held 暂存器:键 "wallet|condition_id|outcome_index" -> {trade, wallet_entry_price, held_since}。
        目标钱包在【现价低于我们下限】时建仓 → 不跟、暂存;独立的 held 刷新(默认每 5min,见 cli)拉
        实时价,价**上穿**进 [下限,上限] 即补跟并清缓存;跟单方现价<0.2/冲破上限/盘结束/出 watchlist
        则放弃清缓存。存 meta JSON blob(仅 runner 读写,跨 tick/重启持久)。"""
        self.init_db()
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'held_pending_price'").fetchone()
        out = _loads(row["value"], {}) if row else {}
        return out if isinstance(out, dict) else {}

    def save_held_pending_price(self, pending: dict[str, Any]) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('held_pending_price', ?)",
                (_dumps(pending or {}),),
            )

    def load_stop_loss_blocked(self) -> dict[str, Any]:
        """止损黑名单:键 "wallet|condition_id|outcome_index" -> {stopped_at,...}。被主盘止损平过的
        持仓不再复跟(钱包仍持有/加仓/重启补单都会再触发买单 → 一律拦,避免反复挨割)。盘结算/出
        watchlist 后清键。存 meta JSON blob(仅 runner 读写,跨 tick/重启持久)。"""
        self.init_db()
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'stop_loss_blocked'").fetchone()
        out = _loads(row["value"], {}) if row else {}
        return out if isinstance(out, dict) else {}

    def save_stop_loss_blocked(self, blocked: dict[str, Any]) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('stop_loss_blocked', ?)",
                (_dumps(blocked or {}),),
            )

    def load_results(self) -> list[dict[str, Any]]:
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT raw_json FROM follow_results ORDER BY resolved_at, signal_id").fetchall()
        return [_loads(row["raw_json"], {}) for row in rows]

    def load_unprocessed_m5_results(self) -> list[dict[str, Any]]:
        """M5 自动降级:尚未纳入重评的终态结果(settled + exited,均有真实盈亏)。
        计数与批次钱包从此派生 → 持久化、跨重启累加、含提前卖出。返回 [{signal_id,wallet}]。"""
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT signal_id, wallet FROM follow_results "
                "WHERE status IN ('settled', 'exited') AND COALESCE(m5_processed, 0) = 0 "
                "ORDER BY resolved_at, signal_id"
            ).fetchall()
        return [{"signal_id": str(r["signal_id"]), "wallet": str(r["wallet"] or "")} for r in rows]

    def mark_m5_results_processed(self, signal_ids: list[str] | set[str]) -> int:
        """把这批终态结果标记为已纳入一轮 M5 重评(防下次重启重复计数/处理)。"""
        ids = [str(s) for s in signal_ids if s]
        if not ids:
            return 0
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN")
            placeholders = ",".join("?" for _ in ids)
            cur = conn.execute(
                f"UPDATE follow_results SET m5_processed = 1 WHERE signal_id IN ({placeholders})",
                ids,
            )
            conn.commit()
        return cur.rowcount

    def save_follow_strategy(self, strategy: dict[str, Any], *, ts: int | None = None) -> dict[str, Any]:
        self.init_db()
        updated_at = int(ts or time.time())
        normalized = normalize_follow_strategy(strategy, updated_at=updated_at)
        normalized["configured"] = True
        valid, errors = validate_follow_strategy(normalized)
        if not valid:
            raise ValueError(",".join(errors))
        balance = round(_to_float((normalized.get("balance") or {}).get("usable_balance_usdc")), 8)
        normalized["balance"]["usable_balance_usdc"] = balance
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO follow_strategy(id, schema_version, updated_at, raw_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ACTIVE_FOLLOW_STRATEGY_ID,
                    int(normalized.get("schema_version") or FOLLOW_SCHEMA_VERSION),
                    updated_at,
                    _dumps(normalized),
                ),
            )
            # 运行余额是"买入即扣"的现金账本(account_balance_ledger 驱动),保存策略**不能**
            # re-pin 它:否则改任何参数都会把现金重置回静态预设、抹掉当时未结算持仓的扣款,
            # 之后这些持仓结算又贷记回来 → 现金单向虚高、注码被吹大(实测 5000 起跑变 6000 权益)。
            # 仅在**首次尚未配置**时,用策略里的 balance 作为初始本金 seed 一次;之后改余额走
            # 专门入口 set_account_balance(source=manual)。balance<=0 不再删除已有余额。
            existing = self.load_account_balance(conn)
            if balance > 0 and not existing.get("configured"):
                balance_row = {
                    "configured": True,
                    "balance_usdc": balance,
                    "source": "strategy",
                    "updated_at": updated_at,
                }
                conn.execute(
                    """
                    INSERT OR REPLACE INTO account_balance(id, balance_usdc, source, updated_at, raw_json)
                    VALUES (1, ?, ?, ?, ?)
                    """,
                    (balance, "strategy", updated_at, _dumps(balance_row)),
                )
        return normalized

    def load_follow_strategy(self, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
        def read_from(active: sqlite3.Connection) -> dict[str, Any]:
            try:
                if "follow_strategy" not in _table_names(active):
                    return normalize_follow_strategy(None)
                row = active.execute(
                    "SELECT raw_json FROM follow_strategy WHERE id = ?",
                    (ACTIVE_FOLLOW_STRATEGY_ID,),
                ).fetchone()
            except sqlite3.Error:
                return normalize_follow_strategy(None)
            if not row:
                return normalize_follow_strategy(None)
            payload = _loads(row["raw_json"], {})
            if not isinstance(payload, dict):
                return normalize_follow_strategy(None)
            return normalize_follow_strategy(payload)

        if conn is not None:
            return read_from(conn)
        self.init_db()
        with self.connect() as active:
            return read_from(active)

    def load_follow_strategy_readonly(self) -> dict[str, Any]:
        readonly = self.connect_readonly()
        if readonly is None:
            return normalize_follow_strategy(None)
        try:
            return self.load_follow_strategy(readonly)
        finally:
            readonly.close()

    # ---- named strategy library (multiple saved strategies, one active) ----

    @staticmethod
    def _strategy_entry(slug: str, name: str, *, active: bool, updated_at: int, strategy: dict[str, Any]) -> dict[str, Any]:
        return {
            "slug": str(slug),
            "name": str(name),
            "active": bool(active),
            "updated_at": int(updated_at),
            "strategy": strategy,
        }

    @staticmethod
    def _normalize_library_strategy(payload: Any, *, updated_at: int | None = None) -> dict[str, Any]:
        normalized = normalize_follow_strategy(payload if isinstance(payload, dict) else None, updated_at=updated_at)
        normalized["configured"] = True
        return normalized

    def _new_strategy_slug(self, conn: sqlite3.Connection) -> str:
        for _ in range(32):
            slug = "s" + secrets.token_hex(5)
            hit = conn.execute("SELECT 1 FROM follow_strategy_library WHERE slug = ?", (slug,)).fetchone()
            if not hit:
                return slug
        raise ValueError("slug_generation_failed")

    def list_follow_strategies(self) -> dict[str, Any]:
        self.init_db()
        with self.connect() as conn:
            return self._list_follow_strategies(conn)

    def list_follow_strategies_readonly(self) -> dict[str, Any]:
        conn = self.connect_readonly()
        if conn is None:
            return {"strategies": [], "active_slug": None}
        try:
            if "follow_strategy_library" not in _table_names(conn):
                return {"strategies": [], "active_slug": None}
            return self._list_follow_strategies(conn)
        finally:
            conn.close()

    def _list_follow_strategies(self, conn: sqlite3.Connection) -> dict[str, Any]:
        rows = conn.execute(
                "SELECT slug, name, is_active, updated_at, raw_json FROM follow_strategy_library "
                "ORDER BY updated_at ASC, slug ASC"
        ).fetchall()
        strategies: list[dict[str, Any]] = []
        active_slug: str | None = None
        for row in rows:
            payload = _loads(row["raw_json"], {})
            strategy = self._normalize_library_strategy(payload)
            active = bool(row["is_active"])
            if active:
                active_slug = str(row["slug"])
            strategies.append(
                self._strategy_entry(
                    str(row["slug"]),
                    str(row["name"]),
                    active=active,
                    updated_at=int(row["updated_at"]),
                    strategy=strategy,
                )
            )
        return {"strategies": strategies, "active_slug": active_slug}

    def create_follow_strategy(self, name: str, strategy: dict[str, Any], *, ts: int | None = None) -> dict[str, Any]:
        self.init_db()
        updated_at = int(ts or time.time())
        name = str(name or "").strip()
        if not name:
            raise ValueError("name_required")
        normalized = self._normalize_library_strategy(strategy, updated_at=updated_at)
        valid, errors = validate_follow_strategy(normalized)
        if not valid:
            raise ValueError("invalid_follow_strategy:" + ",".join(errors))
        make_active = False
        with self.connect() as conn:
            dup = conn.execute(
                "SELECT 1 FROM follow_strategy_library WHERE name = ? COLLATE NOCASE", (name,)
            ).fetchone()
            if dup:
                raise ValueError("duplicate_name")
            count_row = conn.execute("SELECT COUNT(*) AS n FROM follow_strategy_library").fetchone()
            make_active = int(count_row["n"]) == 0 if count_row else True
            slug = self._new_strategy_slug(conn)
            if make_active:
                conn.execute("UPDATE follow_strategy_library SET is_active = 0")
            conn.execute(
                "INSERT INTO follow_strategy_library(slug, name, is_active, updated_at, raw_json) VALUES (?, ?, ?, ?, ?)",
                (slug, name, 1 if make_active else 0, updated_at, _dumps(normalized)),
            )
        if make_active:
            self.save_follow_strategy(normalized, ts=updated_at)
        return self._strategy_entry(slug, name, active=make_active, updated_at=updated_at, strategy=normalized)

    def update_follow_strategy_entry(
        self, slug: str, name: str, strategy: dict[str, Any], *, ts: int | None = None
    ) -> dict[str, Any]:
        self.init_db()
        updated_at = int(ts or time.time())
        name = str(name or "").strip()
        if not name:
            raise ValueError("name_required")
        normalized = self._normalize_library_strategy(strategy, updated_at=updated_at)
        valid, errors = validate_follow_strategy(normalized)
        if not valid:
            raise ValueError("invalid_follow_strategy:" + ",".join(errors))
        with self.connect() as conn:
            row = conn.execute(
                "SELECT is_active FROM follow_strategy_library WHERE slug = ?", (slug,)
            ).fetchone()
            if not row:
                raise ValueError("strategy_not_found")
            dup = conn.execute(
                "SELECT 1 FROM follow_strategy_library WHERE name = ? COLLATE NOCASE AND slug != ?",
                (name, slug),
            ).fetchone()
            if dup:
                raise ValueError("duplicate_name")
            was_active = bool(row["is_active"])
            conn.execute(
                "UPDATE follow_strategy_library SET name = ?, updated_at = ?, raw_json = ? WHERE slug = ?",
                (name, updated_at, _dumps(normalized), slug),
            )
        if was_active:
            self.save_follow_strategy(normalized, ts=updated_at)
        return self._strategy_entry(slug, name, active=was_active, updated_at=updated_at, strategy=normalized)

    def activate_follow_strategy(self, slug: str, *, ts: int | None = None) -> dict[str, Any]:
        self.init_db()
        updated_at = int(ts or time.time())
        with self.connect() as conn:
            row = conn.execute(
                "SELECT raw_json FROM follow_strategy_library WHERE slug = ?", (slug,)
            ).fetchone()
            if not row:
                raise ValueError("strategy_not_found")
            conn.execute("UPDATE follow_strategy_library SET is_active = 0")
            conn.execute("UPDATE follow_strategy_library SET is_active = 1 WHERE slug = ?", (slug,))
            payload = _loads(row["raw_json"], {})
        normalized = self._normalize_library_strategy(payload, updated_at=updated_at)
        self.save_follow_strategy(normalized, ts=updated_at)
        return self.list_follow_strategies()

    def delete_follow_strategy_entry(self, slug: str, *, ts: int | None = None) -> dict[str, Any]:
        self.init_db()
        updated_at = int(ts or time.time())
        promote: sqlite3.Row | None = None
        was_active = False
        with self.connect() as conn:
            row = conn.execute(
                "SELECT is_active FROM follow_strategy_library WHERE slug = ?", (slug,)
            ).fetchone()
            if not row:
                raise ValueError("strategy_not_found")
            was_active = bool(row["is_active"])
            conn.execute("DELETE FROM follow_strategy_library WHERE slug = ?", (slug,))
            if was_active:
                remaining = conn.execute(
                    "SELECT slug, raw_json FROM follow_strategy_library ORDER BY updated_at ASC, slug ASC"
                ).fetchall()
                if len(remaining) == 1:
                    promote = remaining[0]
                    conn.execute(
                        "UPDATE follow_strategy_library SET is_active = 1 WHERE slug = ?",
                        (promote["slug"],),
                    )
        if was_active:
            if promote is not None:
                normalized = self._normalize_library_strategy(_loads(promote["raw_json"], {}), updated_at=updated_at)
                self.save_follow_strategy(normalized, ts=updated_at)
            else:
                self.clear_active_follow_strategy()
        return self.list_follow_strategies()

    def clear_active_follow_strategy(self) -> None:
        self.init_db()
        with self.connect() as conn:
            conn.execute("DELETE FROM follow_strategy WHERE id = ?", (ACTIVE_FOLLOW_STRATEGY_ID,))

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
        with self.connect() as conn:
            return self._apply_account_ledger(conn, entries)

    def _apply_account_ledger(
        self, conn: sqlite3.Connection, entries: list[dict[str, Any]]
    ) -> dict[str, Any]:
        applied_count = 0
        applied_amount = 0.0
        state = self.load_account_balance(conn)
        if not state.get("configured"):
            return {"configured": False, "applied_count": 0, "applied_amount_usdc": 0.0}
        balance = float(state.get("balance_usdc") or 0.0)
        for entry in entries:
            ledger_id = str(entry.get("ledger_id") or "").strip()
            if not ledger_id:
                continue
            exists = conn.execute(
                "SELECT 1 FROM account_balance_ledger WHERE ledger_id = ?", (ledger_id,)
            ).fetchone()
            if exists:
                continue
            amount = round(float(entry.get("amount_usdc") or 0.0), 8)
            balance = round(balance + amount, 8)
            payload = {**entry, "amount_usdc": amount, "balance_after_usdc": balance}
            conn.execute(
                """
                INSERT INTO account_balance_ledger
                (ledger_id, kind, amount_usdc, balance_after_usdc, created_at, signal_id, trade_id, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ledger_id, str(entry.get("kind") or ""), amount, balance,
                    int(entry.get("created_at") or time.time()), str(entry.get("signal_id") or ""),
                    str(entry.get("trade_id") or ""), _dumps(payload),
                ),
            )
            applied_count += 1
            applied_amount = round(applied_amount + amount, 8)
        if applied_count:
            updated_at = int(time.time())
            updated = {**state, "configured": True, "balance_usdc": balance, "updated_at": updated_at}
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
            "balance_usdc": round(balance, 8),
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

    def record_m5_demote_cooldown(self, wallets: Any, *, ts: int) -> None:
        """M5 自动降级删除的钱包记一个冷却时间戳;冷却窗口内 observer(observe-live)
        不把它当新候选重新发现加回(防边界钱包 demote↔readd ping-pong)。"""
        self.init_db()
        clean = sorted({str(w or "").lower() for w in (wallets or []) if str(w or "").strip()})
        if not clean:
            return
        with self.connect() as conn:
            for wallet in clean:
                conn.execute(
                    "INSERT OR REPLACE INTO m5_demote_cooldown(wallet, demoted_at, raw_json) VALUES (?, ?, ?)",
                    (wallet, int(ts), _dumps({"wallet": wallet, "demoted_at": int(ts)})),
                )

    def load_m5_demote_cooldown_wallets(self, *, now_ts: int, cooldown_seconds: int) -> set[str]:
        """仍在冷却窗口(now − demoted_at < cooldown_seconds)内的钱包集合。过期行留库不影响判断
        (按时间窗过滤),下次同一钱包再降级时 INSERT OR REPLACE 覆盖时间戳。"""
        self.init_db()
        cutoff = int(now_ts) - max(0, int(cooldown_seconds))
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT wallet FROM m5_demote_cooldown WHERE demoted_at >= ?", (cutoff,)
            ).fetchall()
        return {str(row["wallet"]).lower() for row in rows if row["wallet"]}

    def load_m5_demote_history(self) -> dict[str, int]:
        """所有 M5 淘汰时间戳(不按冷却窗裁切)。

        实跟表现熔断只统计上次淘汰之后的新结果；否则钱包冷却后被 observer 重新发现，
        会被同一批旧亏损永久、反复删除，失去用新质量重新证明自己的机会。
        """
        self.init_db()
        with self.connect() as conn:
            rows = conn.execute("SELECT wallet, demoted_at FROM m5_demote_cooldown").fetchall()
        return {
            str(row["wallet"]).lower(): int(row["demoted_at"] or 0)
            for row in rows
            if row["wallet"]
        }

    def clear_wallet_quarantine_reasons(self, reasons: set[str]) -> None:
        self.init_db()
        clean = {str(reason).strip() for reason in reasons if str(reason).strip()}
        if not clean:
            return
        placeholders = ",".join("?" for _ in clean)
        with self.connect() as conn:
            conn.execute(f"DELETE FROM wallet_quarantine WHERE reason IN ({placeholders})", tuple(sorted(clean)))

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

    def clear_revalidated_quarantine(
        self,
        wallets: set[str],
        *,
        validated_at: int,
        protected_reasons: set[str] | None = None,
    ) -> None:
        """历史复审清隔离;protected_reasons 里的原因(手动 + M5 实跟/重评类)不自动清除。"""
        self.init_db()
        wallets = {str(wallet).lower() for wallet in wallets if wallet}
        if not wallets or validated_at <= 0:
            return
        protected = {str(r).lower() for r in (protected_reasons or {"manual_dashboard_quarantine"}) if r}
        expanded_wallets = set(wallets)
        expanded_wallets.update(f"esports:{wallet}" for wallet in wallets)
        wallet_ph = ",".join("?" for _ in expanded_wallets)
        sql = f"DELETE FROM wallet_quarantine WHERE wallet IN ({wallet_ph}) AND quarantined_at < ?"
        params: list[Any] = [*sorted(expanded_wallets), int(validated_at)]
        if protected:
            sql += f" AND reason NOT IN ({','.join('?' for _ in protected)})"
            params.extend(sorted(protected))
        with self.connect() as conn:
            conn.execute(sql, tuple(params))

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

    def save_market_cache(self, markets: dict[str, dict[str, Any]], *, cache_kind: str, updated_at: int) -> None:
        cache_kind = str(cache_kind or "active").lower()
        updated_at = int(updated_at or time.time())
        self.init_db()
        with self.connect() as conn:
            conn.execute("DELETE FROM market_cache WHERE cache_kind = ?", (cache_kind,))
            for condition_id, market in sorted(markets.items()):
                if isinstance(market, dict):
                    self._upsert_market_cache_item(conn, cache_kind, str(condition_id), market, updated_at=updated_at)
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (f"market_cache:{cache_kind}:updated_at", str(updated_at)),
            )

    def upsert_market_cache_item(
        self,
        cache_kind: str,
        condition_id: str,
        market: dict[str, Any],
        *,
        updated_at: int | None = None,
    ) -> None:
        cache_kind = str(cache_kind or "active").lower()
        updated_at = int(updated_at or time.time())
        self.init_db()
        with self.connect() as conn:
            self._upsert_market_cache_item(conn, cache_kind, condition_id, market, updated_at=updated_at)
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                (f"market_cache:{cache_kind}:updated_at", str(updated_at)),
            )

    def _load_market_cache_from_conn(
        self,
        conn: sqlite3.Connection,
        *,
        cache_kind: str,
        now_ts: int,
        ttl_seconds: int,
    ) -> tuple[dict[str, dict[str, Any]], int, bool]:
        try:
            if "market_cache" not in _table_names(conn):
                return {}, 0, False
            rows = conn.execute(
                "SELECT condition_id, updated_at, raw_json FROM market_cache WHERE cache_kind = ?",
                (str(cache_kind or "active").lower(),),
            ).fetchall()
        except sqlite3.Error:
            return {}, 0, False
        if not rows:
            return {}, 0, False
        updated_at = max(int(row["updated_at"] or 0) for row in rows)
        fresh = int(now_ts or time.time()) - updated_at < int(ttl_seconds or 0)
        markets = {}
        for row in rows:
            payload = _loads(row["raw_json"], {})
            if isinstance(payload, dict):
                payload.setdefault("condition_id", str(row["condition_id"]).lower())
                markets[str(row["condition_id"]).lower()] = payload
        return markets, updated_at, fresh

    def load_market_cache(
        self,
        *,
        cache_kind: str,
        now_ts: int,
        ttl_seconds: int,
    ) -> tuple[dict[str, dict[str, Any]], int, bool]:
        self.init_db()
        with self.connect() as conn:
            return self._load_market_cache_from_conn(
                conn,
                cache_kind=cache_kind,
                now_ts=now_ts,
                ttl_seconds=ttl_seconds,
            )

    def load_market_cache_readonly(
        self,
        *,
        cache_kind: str,
        now_ts: int,
        ttl_seconds: int,
    ) -> tuple[dict[str, dict[str, Any]], int, bool]:
        conn = self.connect_readonly()
        if conn is None:
            return {}, 0, False
        try:
            return self._load_market_cache_from_conn(
                conn,
                cache_kind=cache_kind,
                now_ts=now_ts,
                ttl_seconds=ttl_seconds,
            )
        finally:
            conn.close()

    def get_market_cache_item(self, cache_kind: str, condition_id: str) -> dict[str, Any]:
        self.init_db()
        with self.connect() as conn:
            return self._get_market_cache_item_from_conn(conn, cache_kind, condition_id)

    def get_market_cache_item_readonly(self, cache_kind: str, condition_id: str) -> dict[str, Any]:
        conn = self.connect_readonly()
        if conn is None:
            return {}
        try:
            return self._get_market_cache_item_from_conn(conn, cache_kind, condition_id)
        finally:
            conn.close()

    def _get_market_cache_item_from_conn(
        self,
        conn: sqlite3.Connection,
        cache_kind: str,
        condition_id: str,
    ) -> dict[str, Any]:
        try:
            if "market_cache" not in _table_names(conn):
                return {}
            row = conn.execute(
                "SELECT raw_json FROM market_cache WHERE cache_kind = ? AND condition_id = ?",
                (str(cache_kind or "active").lower(), str(condition_id or "").lower()),
            ).fetchone()
        except sqlite3.Error:
            return {}
        if not row:
            return {}
        payload = _loads(row["raw_json"], {})
        return payload if isinstance(payload, dict) else {}

    def save_run_tick(self, row: dict[str, Any]) -> dict[str, Any]:
        self.init_db()
        with self.connect() as conn:
            return self._save_run_tick(conn, row)

    def _save_run_tick(self, conn: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row or {})
        created_at = _timestamp(payload.get("created_at")) or int(time.time())
        payload["created_at"] = created_at
        payload["status"] = str(payload.get("status") or ("run_iteration_error" if payload.get("error") else "ok"))
        digest = hashlib.sha1(_dumps(payload).encode("utf-8")).hexdigest()[:12]
        tick_id = str(payload.get("tick_id") or f"{created_at}:{digest}")
        payload["tick_id"] = tick_id
        conn.execute(
            """
            INSERT OR REPLACE INTO run_ticks(
                tick_id, created_at, status, gate_open, watched_market_count,
                open_signal_count, new_signal_count, tick_runtime_seconds,
                desired_next_interval_seconds, error, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tick_id, created_at, str(payload.get("status") or "ok"),
                1 if payload.get("gate_open") else 0, _to_int(payload.get("watched_market_count")),
                _to_int(payload.get("open_signal_count")), _to_int(payload.get("new_signal_count")),
                _to_float(payload.get("tick_runtime_seconds")),
                _to_int(payload.get("desired_next_interval_seconds")), str(payload.get("error") or ""),
                _dumps(payload),
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('run_ticks_updated_at', ?)",
            (str(max(created_at, int(time.time()))),),
        )
        conn.execute(
            """
            DELETE FROM run_ticks WHERE tick_id NOT IN (
                SELECT tick_id FROM run_ticks ORDER BY created_at DESC, tick_id DESC LIMIT ?
            )
            """,
            (RUN_TICKS_RETENTION,),
        )
        return payload

    def load_run_ticks(self, *, limit: int = 100) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "run_ticks" not in _table_names(conn):
                return []
            rows = conn.execute(
                "SELECT raw_json, gate_open FROM run_ticks ORDER BY created_at DESC, tick_id DESC LIMIT ?",
                (max(1, int(limit or 100)),),
            ).fetchall()
            ticks = []
            for row in rows:
                payload = _loads(row["raw_json"], {})
                if isinstance(payload, dict):
                    payload["gate_open"] = bool(payload.get("gate_open") or row["gate_open"])
                    ticks.append(payload)
            return list(reversed(ticks))
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def latest_run_tick(self) -> dict[str, Any]:
        rows = self.load_run_ticks(limit=1)
        return rows[-1] if rows else {}

    def load_performance(self) -> dict[str, Any]:
        self.init_db()
        with self.connect() as conn:
            total = conn.execute("SELECT raw_json FROM performance_total WHERE id = 1").fetchone()
            wallet_rows = conn.execute("SELECT wallet, raw_json FROM wallet_performance").fetchall()
            updated = conn.execute("SELECT value FROM meta WHERE key = 'performance_updated_at'").fetchone()
            extra = conn.execute("SELECT value FROM meta WHERE key = 'performance_extra'").fetchone()
        if not total and not wallet_rows:
            return {}
        performance = {
            "wallets": {str(row["wallet"]): _loads(row["raw_json"], {}) for row in wallet_rows},
            "total": _loads(total["raw_json"], {}) if total else {},
        }
        extra_payload = _loads(extra["value"], {}) if extra else {}
        if isinstance(extra_payload, dict):
            performance.update(extra_payload)
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
            extra = conn.execute("SELECT value FROM meta WHERE key = 'performance_extra'").fetchone() if "meta" in tables else None
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
            extra_payload = _loads(extra["value"], {}) if extra else {}
            if isinstance(extra_payload, dict):
                performance.update(extra_payload)
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
            extra = conn.execute("SELECT value FROM meta WHERE key = 'performance_extra'").fetchone() if "meta" in tables else None
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
            extra_payload = _loads(extra["value"], {}) if extra else {}
            if isinstance(extra_payload, dict):
                performance.update(extra_payload)
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

    # ---- Evidence-grounded pre-match AI risk audit ----------------------

    def load_ai_assessment(self, condition_id: str) -> dict[str, Any] | None:
        conn = self.connect_readonly()
        if conn is None:
            return None
        try:
            if "ai_match_assessments" not in _table_names(conn):
                return None
            row = conn.execute(
                "SELECT raw_json FROM ai_match_assessments WHERE condition_id = ?",
                (str(condition_id or "").lower(),),
            ).fetchone()
            return _loads(row["raw_json"], {}) if row else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def save_ai_assessment(self, assessment: dict[str, Any]) -> None:
        condition_id = str(assessment.get("condition_id") or "").lower()
        if not condition_id:
            raise ValueError("AI assessment requires condition_id")
        self.init_db()
        now = _to_int(assessment.get("updated_at") or assessment.get("created_at") or time.time())
        created = _to_int(assessment.get("created_at") or now)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_match_assessments
                (condition_id, input_hash, status, created_at, updated_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    input_hash=excluded.input_hash,
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    raw_json=excluded.raw_json
                """,
                (
                    condition_id,
                    str(assessment.get("input_hash") or ""),
                    str(assessment.get("status") or "unavailable"),
                    created,
                    now,
                    _dumps({**assessment, "condition_id": condition_id, "created_at": created, "updated_at": now}),
                ),
            )

    def save_ai_intent(self, intent: dict[str, Any]) -> None:
        intent_id = str(intent.get("intent_id") or "")
        condition_id = str(intent.get("condition_id") or "").lower()
        if not intent_id or not condition_id:
            raise ValueError("AI intent requires intent_id and condition_id")
        self.init_db()
        now = _to_int(intent.get("updated_at") or intent.get("created_at") or time.time())
        created = _to_int(intent.get("created_at") or now)
        normalized = {**intent, "condition_id": condition_id, "created_at": created, "updated_at": now}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_follow_intents
                (intent_id, condition_id, wallet, outcome_index, action, status, created_at, updated_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(intent_id) DO UPDATE SET
                    action=excluded.action,
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    raw_json=excluded.raw_json
                """,
                (
                    intent_id,
                    condition_id,
                    str(intent.get("wallet") or "").lower(),
                    _to_int(intent.get("outcome_index"), -1),
                    str(intent.get("action") or "unavailable"),
                    str(intent.get("status") or "open"),
                    created,
                    now,
                    _dumps(normalized),
                ),
            )

    def update_ai_intent_status(self, intent_id: str, status: str, *, updated_at: int) -> None:
        self.init_db()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT raw_json FROM ai_follow_intents WHERE intent_id = ?", (str(intent_id),)
            ).fetchone()
            if not row:
                return
            payload = _loads(row["raw_json"], {})
            payload["status"] = str(status)
            payload["updated_at"] = int(updated_at)
            conn.execute(
                "UPDATE ai_follow_intents SET status=?, updated_at=?, raw_json=? WHERE intent_id=?",
                (str(status), int(updated_at), _dumps(payload), str(intent_id)),
            )

    def save_ai_shadow(self, shadow: dict[str, Any]) -> None:
        shadow_id = str(shadow.get("shadow_id") or "")
        condition_id = str(shadow.get("condition_id") or "").lower()
        if not shadow_id or not condition_id:
            raise ValueError("AI shadow requires shadow_id and condition_id")
        self.init_db()
        now = _to_int(shadow.get("updated_at") or shadow.get("created_at") or time.time())
        created = _to_int(shadow.get("created_at") or now)
        normalized = {**shadow, "condition_id": condition_id, "created_at": created, "updated_at": now}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_shadow_positions
                (shadow_id, intent_id, condition_id, wallet, outcome_index, status, created_at, updated_at,
                 shadow_kind, realized_pnl, ai_net_effect, comparison_pnl, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(shadow_id) DO UPDATE SET
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    shadow_kind=excluded.shadow_kind,
                    realized_pnl=excluded.realized_pnl,
                    ai_net_effect=excluded.ai_net_effect,
                    comparison_pnl=excluded.comparison_pnl,
                    raw_json=excluded.raw_json
                """,
                (
                    shadow_id,
                    str(shadow.get("intent_id") or ""),
                    condition_id,
                    str(shadow.get("wallet") or "").lower(),
                    _to_int(shadow.get("outcome_index"), -1),
                    str(shadow.get("status") or "open"),
                    created,
                    now,
                    str(shadow.get("shadow_kind") or "wallet_original"),
                    _to_float(shadow.get("realized_pnl")),
                    (_to_float(shadow.get("ai_net_effect")) if shadow.get("ai_net_effect") is not None else None),
                    (_to_float(shadow.get("comparison_pnl")) if shadow.get("comparison_pnl") is not None else None),
                    _dumps(normalized),
                ),
            )

    def load_ai_shadows_for_intent(self, intent_id: str) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "ai_shadow_positions" not in _table_names(conn):
                return []
            rows = conn.execute(
                "SELECT raw_json FROM ai_shadow_positions WHERE intent_id=? ORDER BY shadow_id",
                (str(intent_id or ""),),
            ).fetchall()
            return [_loads(row["raw_json"], {}) for row in rows]
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def save_ai_data_cache(self, row: dict[str, Any]) -> None:
        cache_key = str(row.get("cache_key") or "")
        if not cache_key:
            raise ValueError("AI data cache requires cache_key")
        self.init_db()
        fetched_at = _to_int(row.get("fetched_at") or time.time())
        last_used_at = _to_int(row.get("last_used_at") or fetched_at)
        expires_at = _to_int(row.get("expires_at") or fetched_at)
        normalized = {
            **row,
            "cache_key": cache_key,
            "fetched_at": fetched_at,
            "last_used_at": last_used_at,
            "expires_at": expires_at,
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_data_cache
                (cache_key,cache_kind,game,team_id,fetched_at,last_used_at,expires_at,raw_json)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    cache_kind=excluded.cache_kind,game=excluded.game,team_id=excluded.team_id,
                    fetched_at=excluded.fetched_at,last_used_at=excluded.last_used_at,
                    expires_at=excluded.expires_at,raw_json=excluded.raw_json
                """,
                (
                    cache_key, str(row.get("cache_kind") or ""), str(row.get("game") or ""),
                    str(row.get("team_id") or ""), fetched_at, last_used_at, expires_at,
                    _dumps(normalized),
                ),
            )

    def load_ai_data_cache(self, cache_key: str, *, now_ts: int, touch: bool = True) -> dict[str, Any] | None:
        self.init_db()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT expires_at,raw_json FROM ai_data_cache WHERE cache_key=?",
                (str(cache_key or ""),),
            ).fetchone()
            if not row or _to_int(row["expires_at"]) < int(now_ts):
                if row:
                    conn.execute("DELETE FROM ai_data_cache WHERE cache_key=?", (str(cache_key or ""),))
                return None
            payload = _loads(row["raw_json"], {})
            if touch:
                payload["last_used_at"] = int(now_ts)
                conn.execute(
                    "UPDATE ai_data_cache SET last_used_at=?,raw_json=? WHERE cache_key=?",
                    (int(now_ts), _dumps(payload), str(cache_key or "")),
                )
            return payload if isinstance(payload, dict) else None

    def delete_ai_data_cache(self, cache_keys: list[str]) -> int:
        keys = [str(value) for value in cache_keys if str(value)]
        if not keys:
            return 0
        self.init_db()
        placeholders = ",".join("?" for _ in keys)
        with self.connect() as conn:
            return int(conn.execute(
                f"DELETE FROM ai_data_cache WHERE cache_key IN ({placeholders})", keys
            ).rowcount or 0)

    def prune_ai_data_cache(self, *, now_ts: int, idle_seconds: int = 24 * 3600) -> int:
        self.init_db()
        cutoff = int(now_ts) - max(1, int(idle_seconds))
        with self.connect() as conn:
            return int(conn.execute(
                "DELETE FROM ai_data_cache WHERE expires_at < ? OR last_used_at < ?",
                (int(now_ts), cutoff),
            ).rowcount or 0)

    def save_ai_backtest_case(self, row: dict[str, Any]) -> None:
        case_id = str(row.get("case_id") or "")
        condition_id = str(row.get("condition_id") or "").lower()
        if not case_id or not condition_id:
            raise ValueError("AI backtest case requires ids")
        self.init_db()
        now = _to_int(row.get("updated_at") or row.get("created_at") or time.time())
        created = _to_int(row.get("created_at") or now)
        normalized = {**row, "case_id": case_id, "condition_id": condition_id,
                      "created_at": created, "updated_at": now}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_backtest_cases(case_id,condition_id,status,created_at,updated_at,raw_json)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(case_id) DO UPDATE SET status=excluded.status,
                    updated_at=excluded.updated_at,raw_json=excluded.raw_json
                """,
                (case_id, condition_id, str(row.get("status") or "unknown"), created, now, _dumps(normalized)),
            )

    def load_ai_backtest_cases(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "ai_backtest_cases" not in _table_names(conn):
                return []
            rows = conn.execute(
                "SELECT raw_json FROM ai_backtest_cases ORDER BY updated_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            return [_loads(row["raw_json"], {}) for row in rows]
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def save_ai_proprietary_position(self, row: dict[str, Any]) -> None:
        condition_id = str(row.get("condition_id") or "").lower()
        if not condition_id:
            raise ValueError("AI proprietary position requires condition_id")
        self.init_db()
        now = _to_int(row.get("updated_at") or row.get("created_at") or time.time())
        created = _to_int(row.get("created_at") or now)
        normalized = {**row, "condition_id": condition_id, "created_at": created, "updated_at": now}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ai_proprietary_positions
                (condition_id,status,decision,outcome_index,evidence_score,stake_usdc,entry_price,
                 realized_pnl,match_start_ts,created_at,updated_at,raw_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    status=excluded.status,decision=excluded.decision,outcome_index=excluded.outcome_index,
                    evidence_score=excluded.evidence_score,stake_usdc=excluded.stake_usdc,
                    entry_price=excluded.entry_price,realized_pnl=excluded.realized_pnl,
                    match_start_ts=excluded.match_start_ts,updated_at=excluded.updated_at,raw_json=excluded.raw_json
                """,
                (
                    condition_id, str(row.get("status") or "screened"), str(row.get("decision") or "unknown"),
                    _to_int(row.get("outcome_index"), -1), _to_int(row.get("evidence_score")),
                    _to_float(row.get("stake_usdc")),
                    (_to_float(row.get("entry_price")) if row.get("entry_price") is not None else None),
                    _to_float(row.get("realized_pnl")),
                    _timestamp(row.get("match_start_time") or row.get("market_start_time")),
                    created, now, _dumps(normalized),
                ),
            )

    def load_ai_proprietary_positions(
        self,
        *,
        open_only: bool = False,
        limit: int = 1000,
        offset: int = 0,
        order_by_start: bool = False,
        now_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "ai_proprietary_positions" not in _table_names(conn):
                return []
            sql = "SELECT raw_json FROM ai_proprietary_positions"
            params: list[Any] = []
            if open_only:
                sql += " WHERE status='open'"
            if order_by_start:
                current = int(now_ts if now_ts is not None else time.time())
                sql += (
                    " ORDER BY CASE WHEN match_start_ts>=? THEN 0 ELSE 1 END,"
                    " CASE WHEN match_start_ts>=? THEN match_start_ts ELSE -match_start_ts END ASC,"
                    " updated_at DESC"
                )
                params.extend((current, current))
            else:
                sql += " ORDER BY updated_at DESC"
            sql += " LIMIT ? OFFSET ?"
            params.extend((max(1, int(limit)), max(0, int(offset))))
            return [_loads(row["raw_json"], {}) for row in conn.execute(sql, params).fetchall()]
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def load_ai_provider_health(self) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "ai_data_cache" not in _table_names(conn):
                return []
            rows = conn.execute(
                "SELECT raw_json FROM ai_data_cache WHERE cache_kind='provider_health' ORDER BY cache_key"
            ).fetchall()
            return [_loads(row["raw_json"], {}) for row in rows]
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def load_ai_shadows(self, *, open_only: bool = False) -> list[dict[str, Any]]:
        conn = self.connect_readonly()
        if conn is None:
            return []
        try:
            if "ai_shadow_positions" not in _table_names(conn):
                return []
            sql = "SELECT raw_json FROM ai_shadow_positions"
            params: tuple[Any, ...] = ()
            if open_only:
                sql += " WHERE status = ?"
                params = ("open",)
            sql += " ORDER BY updated_at DESC"
            rows = conn.execute(sql, params).fetchall()
            return [_loads(row["raw_json"], {}) for row in rows]
        except sqlite3.Error:
            return []
        finally:
            conn.close()

    def load_ai_audit(self, *, condition_id: str = "", limit: int = 500) -> dict[str, Any]:
        conn = self.connect_readonly()
        if conn is None:
            return {"db_ready": False, "assessments": [], "intents": [], "shadows": []}
        try:
            tables = _table_names(conn)
            if not {"ai_match_assessments", "ai_follow_intents", "ai_shadow_positions"}.issubset(tables):
                return {"db_ready": False, "assessments": [], "intents": [], "shadows": []}
            cid = str(condition_id or "").lower()
            where = " WHERE condition_id = ?" if cid else ""
            params: tuple[Any, ...] = (cid,) if cid else ()
            assessment_rows = conn.execute(
                "SELECT raw_json FROM ai_match_assessments" + where + " ORDER BY updated_at DESC LIMIT ?",
                (*params, max(1, int(limit))),
            ).fetchall()
            intent_rows = conn.execute(
                "SELECT raw_json FROM ai_follow_intents" + where + " ORDER BY updated_at DESC LIMIT ?",
                (*params, max(1, int(limit))),
            ).fetchall()
            shadow_rows = conn.execute(
                "SELECT raw_json FROM ai_shadow_positions" + where + " ORDER BY updated_at DESC LIMIT ?",
                (*params, max(1, int(limit))),
            ).fetchall()
        except sqlite3.Error:
            return {"db_ready": False, "assessments": [], "intents": [], "shadows": []}
        finally:
            conn.close()
        return {
            "db_ready": True,
            "assessments": [_loads(row["raw_json"], {}) for row in assessment_rows],
            "intents": [_loads(row["raw_json"], {}) for row in intent_rows],
            "shadows": [_loads(row["raw_json"], {}) for row in shadow_rows],
        }

    def load_ai_intent_condition_ids(self) -> set[str]:
        conn = self.connect_readonly()
        if conn is None:
            return set()
        try:
            if "ai_follow_intents" not in _table_names(conn):
                return set()
            return {
                str(row["condition_id"] or "").lower()
                for row in conn.execute(
                    "SELECT DISTINCT condition_id FROM ai_follow_intents WHERE condition_id<>''"
                ).fetchall()
            }
        except sqlite3.Error:
            return set()
        finally:
            conn.close()

    def load_ai_summary(self) -> dict[str, Any]:
        """Aggregate the complete AI audit history without loading every JSON row."""
        empty = {
            "assessment_count": 0,
            "intent_count": 0,
            "blocked_count": 0,
            "agree_count": 0,
            "insufficient_count": 0,
            "unavailable_count": 0,
            "resolved_blocked_count": 0,
            "avoided_loss_usdc": 0.0,
            "missed_profit_usdc": 0.0,
            "net_effect_usdc": 0.0,
            "ai_inverse_resolved_count": 0,
            "ai_inverse_win_count": 0,
            "ai_inverse_pnl_usdc": 0.0,
            "ai_vs_wallet_usdc": 0.0,
            "proprietary_screened_count": 0,
            "proprietary_open_count": 0,
            "proprietary_settled_count": 0,
            "proprietary_win_count": 0,
            "proprietary_pnl_usdc": 0.0,
            "proprietary_roi": 0.0,
            "proprietary_bankroll_usdc": 5000.0,
            "proprietary_brier_score": None,
            "proprietary_market_edge": None,
            "proprietary_wallet_same_count": 0,
            "proprietary_wallet_opposite_count": 0,
        }
        conn = self.connect_readonly()
        if conn is None:
            return empty
        try:
            tables = _table_names(conn)
            if not {"ai_match_assessments", "ai_follow_intents", "ai_shadow_positions"}.issubset(tables):
                return empty
            assessment = conn.execute("SELECT COUNT(*) AS n FROM ai_match_assessments").fetchone()
            intents = conn.execute(
                """
                SELECT COUNT(*) AS n,
                       SUM(CASE WHEN action='blocked' THEN 1 ELSE 0 END) AS blocked,
                       SUM(CASE WHEN action='agree' THEN 1 ELSE 0 END) AS agree,
                       SUM(CASE WHEN action='insufficient' THEN 1 ELSE 0 END) AS insufficient,
                       SUM(CASE WHEN action='unavailable' THEN 1 ELSE 0 END) AS unavailable
                FROM ai_follow_intents
                """
            ).fetchone()
            shadows = conn.execute(
                """
                SELECT COUNT(*) AS n,
                       SUM(CASE WHEN ai_net_effect > 0 THEN ai_net_effect ELSE 0 END) AS avoided,
                       SUM(CASE WHEN ai_net_effect < 0 THEN -ai_net_effect ELSE 0 END) AS missed,
                       SUM(COALESCE(ai_net_effect, 0)) AS net
                FROM ai_shadow_positions
                WHERE status IN ('settled','exited')
                  AND shadow_kind = 'wallet_original'
                """
            ).fetchone()
            inverse = conn.execute(
                """
                SELECT COUNT(*) AS n,
                       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                       SUM(COALESCE(realized_pnl, 0)) AS pnl,
                       SUM(COALESCE(comparison_pnl, 0)) AS comparison
                FROM ai_shadow_positions
                WHERE status IN ('settled','exited')
                  AND shadow_kind = 'ai_prediction'
                """
            ).fetchone()
            proprietary = (
                conn.execute(
                    """
                    SELECT COUNT(*) AS screened,
                           SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) AS open_count,
                           SUM(CASE WHEN status='settled' THEN 1 ELSE 0 END) AS settled_count,
                           SUM(CASE WHEN status='settled' AND realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN status='settled' THEN COALESCE(realized_pnl,0) ELSE 0 END) AS pnl,
                           SUM(CASE WHEN status='settled' THEN COALESCE(stake_usdc,0) ELSE 0 END) AS settled_stake
                    FROM ai_proprietary_positions
                    """
                ).fetchone()
                if "ai_proprietary_positions" in tables else None
            )
            proprietary_rows = (
                [_loads(row["raw_json"], {}) for row in conn.execute(
                    "SELECT raw_json FROM ai_proprietary_positions WHERE status='settled'"
                ).fetchall()]
                if "ai_proprietary_positions" in tables else []
            )
            direction_rows = (
                conn.execute(
                    """
                    SELECT p.condition_id,p.outcome_index AS self_outcome,i.outcome_index AS wallet_outcome
                    FROM ai_proprietary_positions p
                    JOIN ai_follow_intents i ON i.condition_id=p.condition_id
                    WHERE p.outcome_index >= 0 AND i.outcome_index >= 0
                    """
                ).fetchall()
                if "ai_proprietary_positions" in tables else []
            )
        except sqlite3.Error:
            return empty
        finally:
            conn.close()
        proprietary_pnl = _to_float(proprietary["pnl"]) if proprietary else 0.0
        proprietary_stake = _to_float(proprietary["settled_stake"]) if proprietary else 0.0
        briers = [_to_float(row.get("brier_score"), float("nan")) for row in proprietary_rows]
        briers = [value for value in briers if math.isfinite(value)]
        market_edges = [
            _to_float(row.get("ai_probability")) / 100.0 - _to_float(row.get("market_implied_probability"))
            for row in proprietary_rows if row.get("ai_probability") is not None and row.get("market_implied_probability") is not None
        ]
        wallet_same = sum(_to_int(row["self_outcome"], -1) == _to_int(row["wallet_outcome"], -2) for row in direction_rows)
        wallet_opposite = len(direction_rows) - wallet_same
        return {
            "assessment_count": _to_int(assessment["n"]),
            "intent_count": _to_int(intents["n"]),
            "blocked_count": _to_int(intents["blocked"]),
            "agree_count": _to_int(intents["agree"]),
            "insufficient_count": _to_int(intents["insufficient"]),
            "unavailable_count": _to_int(intents["unavailable"]),
            "resolved_blocked_count": _to_int(shadows["n"]),
            "avoided_loss_usdc": round(_to_float(shadows["avoided"]), 8),
            "missed_profit_usdc": round(_to_float(shadows["missed"]), 8),
            "net_effect_usdc": round(_to_float(shadows["net"]), 8),
            "ai_inverse_resolved_count": _to_int(inverse["n"]),
            "ai_inverse_win_count": _to_int(inverse["wins"]),
            "ai_inverse_pnl_usdc": round(_to_float(inverse["pnl"]), 8),
            "ai_vs_wallet_usdc": round(_to_float(inverse["comparison"]), 8),
            "proprietary_screened_count": _to_int(proprietary["screened"]) if proprietary else 0,
            "proprietary_open_count": _to_int(proprietary["open_count"]) if proprietary else 0,
            "proprietary_settled_count": _to_int(proprietary["settled_count"]) if proprietary else 0,
            "proprietary_win_count": _to_int(proprietary["wins"]) if proprietary else 0,
            "proprietary_pnl_usdc": round(proprietary_pnl, 8),
            "proprietary_roi": round(proprietary_pnl / proprietary_stake, 8) if proprietary_stake > 0 else 0.0,
            "proprietary_bankroll_usdc": round(5000.0 + proprietary_pnl, 8),
            "proprietary_brier_score": round(sum(briers) / len(briers), 8) if briers else None,
            "proprietary_market_edge": round(sum(market_edges) / len(market_edges), 8) if market_edges else None,
            "proprietary_wallet_same_count": wallet_same,
            "proprietary_wallet_opposite_count": wallet_opposite,
        }

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
            self._save_follow_snapshot(
                conn,
                wallet_trade_state=wallet_trade_state,
                open_signals=open_signals,
                result_events=result_events,
                performance=performance,
            )
            conn.commit()

    def _save_follow_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        wallet_trade_state: dict[str, dict[str, Any]],
        open_signals: list[dict[str, Any]],
        result_events: list[dict[str, Any]],
        performance: dict[str, Any],
    ) -> None:
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
                    wallet, int(cursor.get("timestamp") or 0), str(cursor.get("id") or ""),
                    int(row.get("last_seen_at") or 0), _dumps(row),
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

    def commit_follow_tick(
        self,
        *,
        ledger_entries: list[dict[str, Any]],
        pending_small_buys: dict[str, Any],
        held_pending_price: dict[str, Any],
        stop_loss_blocked: dict[str, Any],
        wallet_trade_state: dict[str, dict[str, Any]],
        open_signals: list[dict[str, Any]],
        result_events: list[dict[str, Any]],
        performance: dict[str, Any],
        run_tick: dict[str, Any],
    ) -> dict[str, Any]:
        """Atomically persist all business state produced by one follow tick."""
        self.init_db()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger_result = self._apply_account_ledger(conn, ledger_entries)
            for key, value in (
                ("pending_small_buys", pending_small_buys),
                ("held_pending_price", held_pending_price),
                ("stop_loss_blocked", stop_loss_blocked),
            ):
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                    (key, _dumps(value or {})),
                )
            self._save_follow_snapshot(
                conn,
                wallet_trade_state=wallet_trade_state,
                open_signals=open_signals,
                result_events=result_events,
                performance=performance,
            )
            persisted_tick = dict(run_tick or {})
            persisted_tick["balance_ledger_applied_count"] = ledger_result.get("applied_count", 0)
            persisted_tick["balance_ledger_applied_amount_usdc"] = ledger_result.get("applied_amount_usdc", 0.0)
            if ledger_result.get("configured"):
                persisted_tick["account_balance_usdc"] = ledger_result.get("balance_usdc")
            persisted_tick = self._save_run_tick(conn, persisted_tick)
            conn.commit()
        return {
            "ledger": ledger_result,
            "account_balance": self.load_account_balance(),
            "run_tick": persisted_tick,
        }

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
            INSERT INTO follow_results
            (signal_id, status, wallet, condition_id, resolved_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_id) DO UPDATE SET
                status = excluded.status,
                wallet = excluded.wallet,
                condition_id = excluded.condition_id,
                resolved_at = excluded.resolved_at,
                raw_json = excluded.raw_json
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
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('performance_extra', ?)",
            (_dumps({key: performance.get(key) or {} for key in ("groups", "by_category") if key in performance}),),
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
