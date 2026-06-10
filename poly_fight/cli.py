from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import html as html_lib
import math
import json
import os
import re
import signal
import shutil
import sqlite3
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from .api import PolymarketClient, RateLimiter
from .core import (
    ALLOWED_GAME_FAMILIES,
    GAME_FAMILY_LABELS,
    LEAGUE_LABELS,
    MARKET_TYPE_LABELS,
    MAX_HIGH_CHURN_MARKET_RATE,
    MIN_A_POSITIVE_MARKET_RATE,
    SCORING_VERSION,
    SWING_DEPENDENT_RATE,
    TRADE_BEHAVIOR_EXCLUDE_RATE,
    TRADE_BEHAVIOR_MIN_MARKETS,
    GAME_WINNER,
    MAIN_MATCH,
    MAP_WINNER,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    bucket_key,
    bucket_label,
    classify_market_type,
    ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS,
    classify_wallet,
    event_to_market_record,
    event_to_market_records,
    split_bucket_key,
    is_settled_binary_prices,
    normalize_market_text,
    normalize_wallet,
    parse_dt,
    parse_jsonish,
    profile_candidate_wallet,
    summarize_closed_positions,
    summarize_trade_reconstructed_positions,
    to_float,
    to_int,
    winning_outcome_index,
)
from .follow import (
    aggregate_follow_performance,
    apply_closing_line_snapshots,
    apply_contested_flags,
    bootstrap_position_trades,
    contested_markets,
    desired_tick_interval,
    detect_new_positions,
    eligible_follow_wallets,
    esports_match_imminent,
    evaluate_slippage,
    market_current_price,
    process_follow_trades,
    qualify_follow,
    select_new_trades,
    settle_open_signals,
    should_retry_unqualified_position,
    summarize_wallet_fills,
    trade_condition_id,
    winner_outcome_index,
)
from .storage import FollowStore, LeaderboardStore
from .control import read_follow_control, set_pause_new_signals

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix fallback
    fcntl = None


class BuildLockUnavailable(RuntimeError):
    pass


ESPORTS_LEADERBOARD_MIN_TYPE_ROI = 0.12
ESPORTS_LEADERBOARD_MIN_TYPE_CAPITAL_EDGE = 0.0
ESPORTS_DEFAULT_LEADERBOARD_MAX_INACTIVE_DAYS = 3
ESPORTS_RECENT_BUCKET_MIN_MARKETS = 3
ESPORTS_RECENT_BUCKET_MIN_POSITIVE_RATE = 0.50
SPORTS_LEADERBOARD_MIN_CAPITAL_EDGE = 0.0
SPORTS_FLAT_FOLLOW_MIN_SAMPLE = 8
SPORTS_FLAT_FOLLOW_MIN_POSITIVE_MARKET_RATE = 0.60
SPORTS_FLAT_FOLLOW_MIN_WILSON = 0.50
SPORTS_FLAT_FOLLOW_MIN_MEDIAN_MARKET_ROI = 0.10
SPORTS_FLAT_FOLLOW_MAX_MEDIAN_ENTRY = 0.68
CATEGORY_REFRESH_OUTPUT_FILES = {
    "leaderboard.db",
    "smart_wallet_leaderboard.json",
    "wallet_profiles.json",
    "candidate_wallets.json",
    "profile_candidate_wallets.json",
    "discovery_slate.json",
    "leaderboard_wallet_overlap.json",
    "build_summary.json",
    "last_event_analysis.json",
}
CATEGORY_REFRESH_CACHE_DIRS = {
    "raw_market_trades",
    "raw_user_trades",
    "clob_market_metadata",
}
DEFAULT_REFRESH_CACHE_RETENTION_DAYS = {
    "raw_market_trades": 7,
    "raw_user_trades": 1,
    "clob_market_metadata": 30,
}
LEGACY_COLLECTOR_CACHE_ROOTS = (
    Path("collector_v2") / "esports",
    Path("collector_v3") / "esports",
)


@contextmanager
def acquire_build_lock(data_dir: Path, *, blocking: bool = True):
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path = data_dir / ".build-leaderboard.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        if fcntl is None:
            yield
            return
        flags = fcntl.LOCK_EX
        if not blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(handle.fileno(), flags)
        except BlockingIOError as exc:
            raise BuildLockUnavailable(str(lock_path)) from exc
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def prepare_category_refresh_dir(
    category_dir: Path,
    *,
    max_lookback_days: int = 30,
    now_ts: int | None = None,
    cache_retention_days_by_dir: dict[str, int] | None = None,
) -> None:
    """Clear rebuild outputs while keeping reusable API caches bounded."""
    category_dir.mkdir(parents=True, exist_ok=True)
    for name in CATEGORY_REFRESH_OUTPUT_FILES:
        path = category_dir / name
        if path.exists() or path.is_symlink():
            path.unlink()
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    retention_days = {
        dirname: min(max(0, int(max_lookback_days)), int(DEFAULT_REFRESH_CACHE_RETENTION_DAYS.get(dirname, max_lookback_days)))
        for dirname in CATEGORY_REFRESH_CACHE_DIRS
    }
    for dirname, days in (cache_retention_days_by_dir or {}).items():
        if dirname in CATEGORY_REFRESH_CACHE_DIRS:
            retention_days[dirname] = min(max(0, int(max_lookback_days)), max(0, int(days)))

    cache_roots = [category_dir, *(category_dir / relative for relative in LEGACY_COLLECTOR_CACHE_ROOTS)]
    for cache_root in cache_roots:
        for dirname in CATEGORY_REFRESH_CACHE_DIRS:
            cache_dir = cache_root / dirname
            if not cache_dir.exists():
                continue
            cutoff_ts = now_ts - retention_days[dirname] * 86400
            for path in cache_dir.rglob("*"):
                if (path.is_file() or path.is_symlink()) and int(path.stat().st_mtime) < cutoff_ts:
                    path.unlink()
            for path in sorted((p for p in cache_dir.rglob("*") if p.is_dir()), key=lambda value: len(value.parts), reverse=True):
                try:
                    path.rmdir()
                except OSError:
                    pass


def category_refresh_cache_retention_days(args: argparse.Namespace) -> int:
    effective_defaults = effective_build_defaults(args)
    return max(
        1,
        effective_defaults["classification_lookback_days"],
        int(getattr(args, "market_trades_cache_ttl_days", 0) or 0),
        int(getattr(args, "user_trades_cache_ttl_days", 0) or 0),
        30,  # CLOB condition metadata cache default.
    )


def category_refresh_cache_retention_days_by_dir(args: argparse.Namespace) -> dict[str, int]:
    return {
        "raw_market_trades": max(1, int(getattr(args, "market_trades_cache_ttl_days", 7) or 7)),
        "raw_user_trades": max(1, int(getattr(args, "user_trades_cache_ttl_days", 1) or 1)),
        "clob_market_metadata": 30,
    }


def build_client(args: argparse.Namespace) -> PolymarketClient:
    rate_per_second = getattr(args, "max_requests_per_second", 10)
    rate_limiter = (
        RateLimiter(rate_per_second=rate_per_second, burst=getattr(args, "request_burst", 5))
        if rate_per_second and rate_per_second > 0
        else None
    )
    return PolymarketClient(
        timeout=args.timeout,
        rate_limiter=rate_limiter,
        max_retry_after_seconds=getattr(args, "max_retry_after_seconds", 60),
    )


DATA_DIR = Path("data")
CATEGORY_TAG_SLUGS = {
    "esports": ("counter-strike-2", "league-of-legends", "dota-2"),
    "sports": ("nba", "ufc"),
}
CATEGORY_MARKET_SCOPES = {
    "esports": "cs-dota-lol-main-game-map-v1",
    "sports": "nba-ufc-moneyline-v1",
}
FOLLOW_SIGNAL_CATEGORIES = ("esports",)
V2_STRICT_MIN_ROI = 0.05
V2_STRICT_MIN_POSITIVE_MARKET_RATE = 0.55
V2_STRICT_MIN_WILSON = 0.50
V2_STRICT_MIN_CAPITAL_WEIGHTED_EDGE = 0.05
V3_CORE_MIN_RECENT_14D_MARKETS = 3
V3_CORE_MIN_RECENT_POSITIVE_RATE = 0.50
V3_CORE_MIN_RECENT_ROI = 0.0
V3_MAX_INACTIVE_SECONDS = 5 * 24 * 60 * 60
V3_MIN_COPYABLE_BUCKET_ROI = 0.15
V3_MAX_COPYABLE_TWO_SIDED_COUNT = 1
V3_MAX_COPYABLE_TWO_SIDED_RATE = 0.05
V3_MAX_COPYABLE_TAIL_ENTRY_RATE = 0.25
V3_MOMENTUM_MIN_7D_MARKETS = 10
V3_MOMENTUM_MIN_7D_POSITIVE_RATE = 0.70
V3_MOMENTUM_MIN_7D_ROI = 0.10
V3_MOMENTUM_MIN_14D_MARKETS = 20
V3_MOMENTUM_MIN_14D_POSITIVE_RATE = 0.65
V3_MOMENTUM_MIN_14D_ROI = 0.08
V3_MOMENTUM_MIN_OVERALL_ROI = 0.02
V3_MOMENTUM_MIN_CAPITAL_WEIGHTED_EDGE = 0.04
V3_MOMENTUM_MAX_MEDIAN_ENTRY_PRICE = 0.75
V3_MOMENTUM_MAX_TWO_SIDED_RATE = 0.05
V3_FAMILY_SUPPLEMENT_ORDER = ("lol",)
V3_FAMILY_SUPPLEMENT_CONFIG = {
    "lol": {
        "limit": 4,
        "min_closed_count": 16,
        "min_bucket_roi": V3_MIN_COPYABLE_BUCKET_ROI,
        "min_positive_rate": 0.65,
        "min_wilson": 0.58,
        "min_capital_edge": 0.08,
        "max_median_entry_price": 0.78,
        "min_recent_14d_markets": 10,
        "min_recent_14d_roi": 0.08,
        "min_recent_14d_positive_rate": 0.65,
        "max_two_sided_rate": 0.0,
        "max_tail_entry_rate": V3_MAX_COPYABLE_TAIL_ENTRY_RATE,
        "max_high_churn_rate": 0.35,
    },
}
COLLECTOR_V2_BUCKETS = (
    ("lol", MAIN_MATCH),
    ("lol", GAME_WINNER),
    ("dota2", MAIN_MATCH),
    ("dota2", GAME_WINNER),
    ("cs2", MAIN_MATCH),
    ("cs2", MAP_WINNER),
)
ESPORTS_DEFAULT_CLASSIFICATION_LOOKBACK_DAYS = 60
ESPORTS_DEFAULT_MIN_PROFILE_PARTICIPATED_MARKETS = 6
ESPORTS_DEFAULT_LEADERBOARD_MIN_PARTICIPATED_MARKETS = 6
ESPORTS_DEFAULT_TARGET_MARKETS = sum(
    limit for key, limit in ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS.items() if key.endswith(":main_match")
)
ESPORTS_DEFAULT_SUBMARKET_TARGET_MARKETS = 150
ESPORTS_DEFAULT_GAME_WINNER_TARGET_MARKETS = sum(
    limit for key, limit in ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS.items() if key.endswith(":game_winner")
)
ESPORTS_DEFAULT_MAP_WINNER_TARGET_MARKETS = sum(
    limit for key, limit in ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS.items() if key.endswith(":map_winner")
)
ESPORTS_DEFAULT_MAX_MARKETS_PER_RUN = ESPORTS_DEFAULT_TARGET_MARKETS
ESPORTS_DEFAULT_SUBMARKET_MAX_MARKETS_PER_RUN = 150
ESPORTS_DEFAULT_GAME_WINNER_MAX_MARKETS_PER_RUN = ESPORTS_DEFAULT_GAME_WINNER_TARGET_MARKETS
ESPORTS_DEFAULT_MAP_WINNER_MAX_MARKETS_PER_RUN = ESPORTS_DEFAULT_MAP_WINNER_TARGET_MARKETS
ESPORTS_DEFAULT_CANDIDATE_WALLETS_PER_MARKET_TYPE = 1_000
ESPORTS_CANDIDATE_MARKET_TYPE_THRESHOLDS = {
    "main_match": {"min_participated_markets": 11, "min_avg_market_cash": 800},
    "game_winner": {"min_participated_markets": 11, "min_avg_market_cash": 800},
    "map_winner": {"min_participated_markets": 11, "min_avg_market_cash": 500},
}
ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS = {}
SPORTS_DEFAULT_CLASSIFICATION_LOOKBACK_DAYS = 90
SPORTS_DEFAULT_MIN_PROFILE_PARTICIPATED_MARKETS = 3
SPORTS_DEFAULT_LEADERBOARD_MIN_PARTICIPATED_MARKETS = 3
SPORTS_DEFAULT_TARGET_MARKETS = 20
SPORTS_DEFAULT_SUBMARKET_TARGET_MARKETS = 60
SPORTS_DEFAULT_MAX_MARKETS_PER_RUN = 100
SPORTS_DEFAULT_SUBMARKET_MAX_MARKETS_PER_RUN = 60
ESPORTS_DEFAULT_MAX_PROFILES_PER_RUN = 300
ESPORTS_DEFAULT_MAX_ESPORTS_CLOSED_POSITIONS_PER_WALLET = 100
ESPORTS_DEFAULT_USER_HISTORY_TRADES_MAX_PAGES = 3
SPORTS_DEFAULT_MAX_PROFILES_PER_RUN = 200
SPORTS_DEFAULT_MAX_ESPORTS_CLOSED_POSITIONS_PER_WALLET = 150
SPORTS_DEFAULT_USER_HISTORY_TRADES_MAX_PAGES = 8


def effective_build_defaults(args: argparse.Namespace) -> dict[str, int]:
    category = getattr(args, "category", "esports")
    if category == "sports":
        return {
            "classification_lookback_days": int(
                args.classification_lookback_days
                if args.classification_lookback_days is not None
                else SPORTS_DEFAULT_CLASSIFICATION_LOOKBACK_DAYS
            ),
            "min_profile_participated_markets": int(
                args.min_profile_participated_markets
                if args.min_profile_participated_markets is not None
                else SPORTS_DEFAULT_MIN_PROFILE_PARTICIPATED_MARKETS
            ),
            "leaderboard_min_participated_markets": int(
                args.leaderboard_min_participated_markets
                if args.leaderboard_min_participated_markets is not None
                else SPORTS_DEFAULT_LEADERBOARD_MIN_PARTICIPATED_MARKETS
            ),
        }
    return {
        "classification_lookback_days": int(
            args.classification_lookback_days
            if args.classification_lookback_days is not None
            else ESPORTS_DEFAULT_CLASSIFICATION_LOOKBACK_DAYS
        ),
        "min_profile_participated_markets": int(
            args.min_profile_participated_markets
            if args.min_profile_participated_markets is not None
            else ESPORTS_DEFAULT_MIN_PROFILE_PARTICIPATED_MARKETS
        ),
        "leaderboard_min_participated_markets": int(
            args.leaderboard_min_participated_markets
            if args.leaderboard_min_participated_markets is not None
            else ESPORTS_DEFAULT_LEADERBOARD_MIN_PARTICIPATED_MARKETS
        ),
    }


def effective_discovery_defaults(args: argparse.Namespace) -> dict[str, int]:
    category = getattr(args, "category", "esports")
    if category == "sports":
        target_markets = int(args.target_markets if args.target_markets is not None else SPORTS_DEFAULT_TARGET_MARKETS)
        submarket_target_markets = int(
            args.submarket_target_markets
            if args.submarket_target_markets is not None
            else SPORTS_DEFAULT_SUBMARKET_TARGET_MARKETS
        )
        max_markets_per_run = int(
            args.max_markets_per_run if args.max_markets_per_run is not None else SPORTS_DEFAULT_MAX_MARKETS_PER_RUN
        )
        submarket_max_markets_per_run = int(
            args.submarket_max_markets_per_run
            if args.submarket_max_markets_per_run is not None
            else SPORTS_DEFAULT_SUBMARKET_MAX_MARKETS_PER_RUN
        )
    else:
        target_markets = int(args.target_markets if args.target_markets is not None else ESPORTS_DEFAULT_TARGET_MARKETS)
        submarket_target_markets = int(
            args.submarket_target_markets
            if args.submarket_target_markets is not None
            else ESPORTS_DEFAULT_SUBMARKET_TARGET_MARKETS
        )
        game_winner_target_markets = int(
            args.game_winner_target_markets
            if args.game_winner_target_markets is not None
            else ESPORTS_DEFAULT_GAME_WINNER_TARGET_MARKETS
        )
        map_winner_target_markets = int(
            args.map_winner_target_markets
            if args.map_winner_target_markets is not None
            else ESPORTS_DEFAULT_MAP_WINNER_TARGET_MARKETS
        )
        max_markets_per_run = int(
            args.max_markets_per_run if args.max_markets_per_run is not None else ESPORTS_DEFAULT_MAX_MARKETS_PER_RUN
        )
        submarket_max_markets_per_run = int(
            args.submarket_max_markets_per_run
            if args.submarket_max_markets_per_run is not None
            else ESPORTS_DEFAULT_SUBMARKET_MAX_MARKETS_PER_RUN
        )
        game_winner_max_markets_per_run = int(
            args.game_winner_max_markets_per_run
            if args.game_winner_max_markets_per_run is not None
            else ESPORTS_DEFAULT_GAME_WINNER_MAX_MARKETS_PER_RUN
        )
        map_winner_max_markets_per_run = int(
            args.map_winner_max_markets_per_run
            if args.map_winner_max_markets_per_run is not None
            else ESPORTS_DEFAULT_MAP_WINNER_MAX_MARKETS_PER_RUN
        )
    if category == "sports":
        game_winner_target_markets = int(
            args.game_winner_target_markets
            if args.game_winner_target_markets is not None
            else submarket_target_markets
        )
        map_winner_target_markets = int(
            args.map_winner_target_markets
            if args.map_winner_target_markets is not None
            else submarket_target_markets
        )
        game_winner_max_markets_per_run = int(
            args.game_winner_max_markets_per_run
            if args.game_winner_max_markets_per_run is not None
            else submarket_max_markets_per_run
        )
        map_winner_max_markets_per_run = int(
            args.map_winner_max_markets_per_run
            if args.map_winner_max_markets_per_run is not None
            else submarket_max_markets_per_run
        )
    return {
        "target_markets": target_markets,
        "submarket_target_markets": submarket_target_markets,
        "game_winner_target_markets": game_winner_target_markets,
        "map_winner_target_markets": map_winner_target_markets,
        "max_markets_per_run": max_markets_per_run,
        "submarket_max_markets_per_run": submarket_max_markets_per_run,
        "game_winner_max_markets_per_run": game_winner_max_markets_per_run,
        "map_winner_max_markets_per_run": map_winner_max_markets_per_run,
    }


def effective_build_limits(args: argparse.Namespace) -> dict[str, int]:
    if getattr(args, "category", "esports") != "sports":
        return {}
    limits = {
        "sports_nba_target_markets": int(getattr(args, "sports_nba_target_markets", 80) or 80),
        "sports_ufc_target_markets": int(getattr(args, "sports_ufc_target_markets", 80) or 80),
        "max_profiles_per_run": int(getattr(args, "max_profiles_per_run", SPORTS_DEFAULT_MAX_PROFILES_PER_RUN) or 0),
        "user_history_trades_max_pages": int(
            getattr(args, "user_history_trades_max_pages", SPORTS_DEFAULT_USER_HISTORY_TRADES_MAX_PAGES) or 0
        ),
        "max_esports_closed_positions_per_wallet": int(
            getattr(
                args,
                "max_esports_closed_positions_per_wallet",
                SPORTS_DEFAULT_MAX_ESPORTS_CLOSED_POSITIONS_PER_WALLET,
            )
            or 0
        ),
    }
    if limits["max_profiles_per_run"] == ESPORTS_DEFAULT_MAX_PROFILES_PER_RUN:
        limits["max_profiles_per_run"] = SPORTS_DEFAULT_MAX_PROFILES_PER_RUN
    if limits["user_history_trades_max_pages"] == ESPORTS_DEFAULT_USER_HISTORY_TRADES_MAX_PAGES:
        limits["user_history_trades_max_pages"] = SPORTS_DEFAULT_USER_HISTORY_TRADES_MAX_PAGES
    if limits["max_esports_closed_positions_per_wallet"] == ESPORTS_DEFAULT_MAX_ESPORTS_CLOSED_POSITIONS_PER_WALLET:
        limits["max_esports_closed_positions_per_wallet"] = SPORTS_DEFAULT_MAX_ESPORTS_CLOSED_POSITIONS_PER_WALLET
    return limits


def resolve_data_dir(args: argparse.Namespace) -> Path:
    # Each category lives under its own subdir (data/esports, data/sports) so the two
    # lines can be integrated later without colliding. Explicit --data-dir always wins.
    explicit = getattr(args, "data_dir", None)
    if explicit:
        return Path(explicit)
    if getattr(args, "category", None) == "sports":
        return DATA_DIR / "sports"
    return DATA_DIR / "esports"


def resolve_dashboard_root(args: argparse.Namespace) -> Path:
    explicit = getattr(args, "data_dir", None)
    return Path(explicit) if explicit else DATA_DIR


def resolve_follow_dir(args: argparse.Namespace, root: Path | None = None) -> Path:
    explicit = getattr(args, "follow_dir", None)
    if explicit:
        return Path(explicit)
    return (root or DATA_DIR) / "follow"


def category_data_dirs(root: Path) -> dict[str, Path]:
    root = Path(root)
    return {"esports": root / "esports", "sports": root / "sports"}


def read_category_leaderboards(root: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    mtimes: dict[str, int] = {}
    for category, data_dir in category_data_dirs(root).items():
        db_rows, db_mtimes = LeaderboardStore(data_dir / "leaderboard.db").load_leaderboard(category=category)
        if db_rows:
            rows.extend({**row, "category": category} for row in db_rows if isinstance(row, dict))
            mtimes[category] = int(db_mtimes.get(category) or 0)
            continue
        path = data_dir / "smart_wallet_leaderboard.json"
        leaderboard = read_json(path, [])
        mtimes[category] = int(path.stat().st_mtime) if path.exists() else 0
        for row in leaderboard if isinstance(leaderboard, list) else []:
            if isinstance(row, dict):
                rows.append({**row, "category": category})
    legacy_path = root / "smart_wallet_leaderboard.json"
    legacy_db_rows, legacy_db_mtimes = LeaderboardStore(root / "leaderboard.db").load_leaderboard(category="esports")
    if not rows and legacy_db_rows:
        rows.extend({**row, "category": "esports"} for row in legacy_db_rows if isinstance(row, dict))
        mtimes["esports"] = int(legacy_db_mtimes.get("esports") or 0)
    if not rows and legacy_path.exists():
        legacy = read_json(legacy_path, [])
        mtimes["esports"] = int(legacy_path.stat().st_mtime)
        rows.extend({**row, "category": "esports"} for row in legacy if isinstance(row, dict))
    return rows, mtimes


def migrate_category_follow_dbs(root: Path, follow_dir: Path, *, now_ts: int | None = None) -> dict[str, Any]:
    now_ts = int(now_ts or time.time())
    follow_dir.mkdir(parents=True, exist_ok=True)
    target_path = follow_dir / "follow.db"
    imported: dict[str, int] = {"esports": 0, "sports": 0}
    sources = {
        category: data_dir / "follow" / "follow.db"
        for category, data_dir in category_data_dirs(root).items()
        if (data_dir / "follow" / "follow.db").exists()
    }
    if not sources:
        return {"migrated": False, "imported": imported, "target": str(target_path)}
    marker_path = follow_dir / "follow_migration_state.json"
    source_signature = _follow_migration_source_signature(sources)
    marker = read_json(marker_path, {})
    if target_path.exists() and marker.get("sources") == source_signature:
        return {"migrated": False, "imported": imported, "target": str(target_path), "source": "migration_guard"}
    target_store = FollowStore(target_path)
    target_store.init_db()
    backup_dir = follow_dir / "migration_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        backup = backup_dir / f"follow-{now_ts}.db"
        if not backup.exists():
            shutil.copy2(target_path, backup)
    for category, source_path in sources.items():
        if source_path.resolve() == target_path.resolve():
            continue
        backup = backup_dir / f"{category}-follow-{now_ts}.db"
        if not backup.exists():
            shutil.copy2(source_path, backup)
        imported[category] += _import_follow_db(source_path, target_path, category=category)
    write_json(marker_path, {"updated_at": now_ts, "sources": source_signature, "imported": imported, "target": str(target_path)})
    return {"migrated": any(imported.values()), "imported": imported, "target": str(target_path)}


def _follow_migration_source_signature(sources: dict[str, Path]) -> dict[str, dict[str, Any]]:
    signature: dict[str, dict[str, Any]] = {}
    for category, path in sorted(sources.items()):
        stat = path.stat()
        signature[category] = {
            "path": str(path.resolve()),
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return signature


def _json_with_category(raw: str | None, category: str, *, wallet: str | None = None) -> str:
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError:
        value = {}
    if not isinstance(value, dict):
        value = {}
    value.setdefault("category", category)
    if wallet:
        value.setdefault("wallet", wallet)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _import_follow_db(source_path: Path, target_path: Path, *, category: str) -> int:
    imported = 0
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(target_path)
    target.row_factory = sqlite3.Row
    try:
        source_tables = {str(row["name"]) for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        target.execute("BEGIN")
        def count_changes() -> int:
            nonlocal imported
            changes = int(target.execute("SELECT changes()").fetchone()[0] or 0)
            imported += changes
            return changes

        if "wallet_cursors" in source_tables:
            for row in source.execute("SELECT wallet, last_trade_timestamp, last_trade_id, last_seen_at, raw_json FROM wallet_cursors"):
                wallet = str(row["wallet"] or "").lower()
                key = f"{category}:{wallet}"
                target.execute(
                    """
                    INSERT OR IGNORE INTO wallet_cursors(wallet, last_trade_timestamp, last_trade_id, last_seen_at, raw_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (key, row["last_trade_timestamp"], row["last_trade_id"], row["last_seen_at"], _json_with_category(row["raw_json"], category, wallet=wallet)),
                )
                count_changes()
        if "follow_signals" in source_tables:
            for row in source.execute("SELECT signal_id, status, wallet, condition_id, outcome_index, created_at, updated_at, raw_json FROM follow_signals"):
                target.execute(
                    """
                    INSERT OR IGNORE INTO follow_signals(signal_id, status, wallet, condition_id, outcome_index, created_at, updated_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (row["signal_id"], row["status"], row["wallet"], row["condition_id"], row["outcome_index"], row["created_at"], row["updated_at"], _json_with_category(row["raw_json"], category)),
                )
                count_changes()
        if "follow_legs" in source_tables:
            for row in source.execute("SELECT signal_id, trade_id, wallet, condition_id, leg_at, stake, raw_json FROM follow_legs"):
                target.execute(
                    """
                    INSERT OR IGNORE INTO follow_legs(signal_id, trade_id, wallet, condition_id, leg_at, stake, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (row["signal_id"], row["trade_id"], row["wallet"], row["condition_id"], row["leg_at"], row["stake"], _json_with_category(row["raw_json"], category)),
                )
                count_changes()
        if "follow_behavior_events" in source_tables:
            for row in source.execute("SELECT signal_id, kind, timestamp, raw_json FROM follow_behavior_events"):
                raw_json = _json_with_category(row["raw_json"], category)
                target.execute(
                    """
                    INSERT INTO follow_behavior_events(signal_id, kind, timestamp, raw_json)
                    SELECT ?, ?, ?, ?
                    WHERE NOT EXISTS (
                      SELECT 1 FROM follow_behavior_events
                      WHERE signal_id = ? AND kind = ? AND timestamp = ? AND raw_json = ?
                    )
                    """,
                    (row["signal_id"], row["kind"], row["timestamp"], raw_json, row["signal_id"], row["kind"], row["timestamp"], raw_json),
                )
                count_changes()
        if "follow_results" in source_tables:
            for row in source.execute("SELECT signal_id, status, wallet, condition_id, resolved_at, raw_json FROM follow_results"):
                target.execute(
                    """
                    INSERT OR IGNORE INTO follow_results(signal_id, status, wallet, condition_id, resolved_at, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (row["signal_id"], row["status"], row["wallet"], row["condition_id"], row["resolved_at"], _json_with_category(row["raw_json"], category)),
                )
                count_changes()
        if "wallet_quarantine" in source_tables:
            for row in source.execute("SELECT wallet, reason, quarantined_at, raw_json FROM wallet_quarantine"):
                wallet = str(row["wallet"] or "").lower()
                key_wallet = wallet if ":" in wallet else f"{category}:{wallet}"
                target.execute(
                    """
                    INSERT OR IGNORE INTO wallet_quarantine(wallet, reason, quarantined_at, raw_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (key_wallet, row["reason"], row["quarantined_at"], _json_with_category(row["raw_json"], category, wallet=wallet.split(":", 1)[-1])),
                )
                count_changes()
        target.execute("COMMIT")
    except Exception:
        target.execute("ROLLBACK")
        raise
    finally:
        source.close()
        target.close()
    return imported


def default_log_dir(data_dir: Path) -> Path:
    data_dir = Path(data_dir)
    if data_dir.name == "data":
        return data_dir.parent / "logs"
    return data_dir / "logs"


def follow_run_log_path(data_dir: Path, log_dir: str | Path | None = None) -> Path:
    base = Path(log_dir) if log_dir else default_log_dir(data_dir)
    return base / "follow" / "follow_run_log.jsonl"


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def fetch_market_trades(
    client: PolymarketClient,
    condition_id: str,
    *,
    page_limit: int,
    max_pages: int,
    min_trade_cash: float,
) -> tuple[list[dict], bool, bool]:
    trades: list[dict] = []
    partial = False
    for page in range(max_pages):
        try:
            batch = client.trades_for_market(
                condition_id,
                limit=page_limit,
                offset=page * page_limit,
                min_trade_cash=min_trade_cash,
            )
        except RuntimeError:
            return trades, True, True
        trades.extend(batch)
        if len(batch) < page_limit:
            return trades, partial, False
    return trades, True, False


def market_trades_cache_path(data_dir: Path, condition_id: str) -> Path:
    safe_condition_id = "".join(ch for ch in condition_id.lower() if ch.isalnum() or ch in {"-", "_"})
    return data_dir / "raw_market_trades" / f"{safe_condition_id}.json"


def user_trades_cache_path(data_dir: Path, wallet: str) -> Path:
    safe_wallet = "".join(ch for ch in wallet.lower() if ch.isalnum() or ch in {"-", "_"})
    return data_dir / "raw_user_trades" / f"{safe_wallet}.json"


def condition_market_cache_path(data_dir: Path, condition_id: str) -> Path:
    safe_condition_id = "".join(ch for ch in condition_id.lower() if ch.isalnum() or ch in {"-", "_"})
    return data_dir / "clob_market_metadata" / f"{safe_condition_id}.json"


def fetch_market_trades_cached(
    client: PolymarketClient,
    condition_id: str,
    *,
    data_dir: Path,
    now_ts: int,
    page_limit: int,
    max_pages: int,
    min_trade_cash: float,
    cache_ttl_days: int,
    force_refresh: bool = False,
    use_cache: bool = True,
) -> tuple[list[dict], bool, str]:
    cache_path = market_trades_cache_path(data_dir, condition_id)
    expected_meta = {
        "condition_id": condition_id.lower(),
        "page_limit": page_limit,
        "max_pages": max_pages,
        "min_trade_cash": min_trade_cash,
    }
    if use_cache and cache_path.exists() and not should_refresh_file_cache(
        cache_path.stat().st_mtime,
        now_ts=now_ts,
        ttl_hours=cache_ttl_days * 24,
        force_refresh=force_refresh,
    ):
        cached = read_json(cache_path, {})
        if cached.get("meta") == expected_meta:
            return cached.get("trades") or [], bool(cached.get("partial")), "cache"

    trades, partial, errored = fetch_market_trades(
        client,
        condition_id,
        page_limit=page_limit,
        max_pages=max_pages,
        min_trade_cash=min_trade_cash,
    )
    if use_cache and not errored:
        write_json(
            cache_path,
            {
                "meta": expected_meta,
                "partial": partial,
                "trades": trades,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return trades, partial, "error" if errored else "api"


def fetch_recent_esports_closed_positions_for_wallet(
    client: PolymarketClient,
    wallet: str,
    esports_condition_ids: set[str],
    *,
    max_esports_closed_positions: int,
    page_limit: int = 50,
    market_chunk_size: int = 50,
) -> list[dict]:
    condition_ids = sorted({str(value).lower() for value in esports_condition_ids if value})
    if not condition_ids:
        return []
    chunk_size = max(1, int(market_chunk_size))
    limit = max(1, min(int(page_limit), int(max_esports_closed_positions)))
    positions_by_key: dict[str, dict] = {}
    for index in range(0, len(condition_ids), chunk_size):
        chunk = condition_ids[index : index + chunk_size]
        batch = client.closed_positions(
            wallet,
            limit=limit,
            offset=0,
            market=chunk,
            sort_direction="DESC",
        )
        for position in batch or []:
            condition_id = str(position.get("conditionId") or position.get("condition_id") or "").lower()
            if condition_id not in esports_condition_ids:
                continue
            key = ":".join(
                [
                    condition_id,
                    str(position.get("outcomeIndex") or position.get("outcome_index") or ""),
                    str(position.get("asset") or ""),
                ]
            )
            positions_by_key[key] = position
    positions = sorted(
        positions_by_key.values(),
        key=lambda row: int(row.get("timestamp") or 0),
        reverse=True,
    )
    return positions[:max_esports_closed_positions]


def fetch_recent_esports_user_trades_for_wallet(
    client: PolymarketClient,
    wallet: str,
    esports_condition_ids: set[str],
    *,
    page_limit: int = 500,
    max_pages: int = 6,
    max_esports_markets: int = 50,
    data_dir: Path | None = None,
    now_ts: int | None = None,
    cache_ttl_days: int = 1,
    force_refresh: bool = False,
    use_cache: bool = True,
) -> list[dict]:
    condition_ids = {str(value).lower() for value in esports_condition_ids if value}
    if not condition_ids:
        return []
    wallet = normalize_wallet(wallet)
    expected_meta = {
        "wallet": wallet,
        "page_limit": int(page_limit),
        "max_pages": int(max_pages),
        "max_esports_markets": int(max_esports_markets),
    }
    cache_path = user_trades_cache_path(data_dir, wallet) if data_dir else None
    if (
        use_cache
        and cache_path
        and cache_path.exists()
        and not should_refresh_file_cache(
            cache_path.stat().st_mtime,
            now_ts=now_ts or int(time.time()),
            ttl_hours=cache_ttl_days * 24,
            force_refresh=force_refresh,
        )
    ):
        cached = read_json(cache_path, {})
        if cached.get("meta") == expected_meta:
            raw_trades = cached.get("trades") or []
            return _filter_esports_user_trades(raw_trades, condition_ids, max_esports_markets=max_esports_markets)

    trades = []
    limit = max(1, int(page_limit))
    for page in range(max(1, int(max_pages))):
        offset = page * limit
        try:
            batch = client.trades_for_user(wallet, limit=limit, offset=offset)
        except RuntimeError as exc:
            if offset > 0 and "HTTP Error 400" in str(exc):
                break
            raise
        if not batch:
            break
        trades.extend(batch)
        filtered = _filter_esports_user_trades(trades, condition_ids, max_esports_markets=max_esports_markets)
        seen_markets = {
            str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
            for trade in filtered
        }
        if len(seen_markets) >= max_esports_markets:
            break
        if len(batch) < limit:
            break
    if use_cache and cache_path:
        filtered_trades = _filter_esports_user_trades(trades, condition_ids, max_esports_markets=max_esports_markets)
        write_json(
            cache_path,
            {
                "meta": expected_meta,
                "trades": filtered_trades,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return _filter_esports_user_trades(trades, condition_ids, max_esports_markets=max_esports_markets)


def _filter_esports_user_trades(
    trades: list[dict],
    condition_ids: set[str],
    *,
    max_esports_markets: int,
) -> list[dict]:
    filtered = []
    seen_markets: set[str] = set()
    for trade in trades:
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
        if condition_id not in condition_ids:
            continue
        filtered.append(trade)
        seen_markets.add(condition_id)
    allowed_markets = set(
        sorted(
            seen_markets,
            key=lambda condition_id: max(
                int(trade.get("timestamp") or 0)
                for trade in filtered
                if str(trade.get("conditionId") or trade.get("condition_id") or "").lower() == condition_id
            ),
            reverse=True,
        )[:max_esports_markets]
    )
    return [
        trade
        for trade in filtered
        if str(trade.get("conditionId") or trade.get("condition_id") or "").lower() in allowed_markets
    ]


def fetch_recent_user_trades_for_wallet(
    client: PolymarketClient,
    wallet: str,
    *,
    page_limit: int = 500,
    max_pages: int = 3,
    data_dir: Path | None = None,
    now_ts: int | None = None,
    cache_ttl_days: int = 1,
    force_refresh: bool = False,
    use_cache: bool = True,
) -> list[dict]:
    wallet = normalize_wallet(wallet)
    expected_meta = {
        "wallet": wallet,
        "page_limit": int(page_limit),
        "max_pages": int(max_pages),
        "raw_user_trades": True,
    }

    def cache_meta_compatible(meta: dict[str, Any]) -> bool:
        if not isinstance(meta, dict):
            return False
        if meta.get("wallet") != wallet:
            return False
        if int(meta.get("page_limit") or 0) != int(page_limit):
            return False
        if not meta.get("raw_user_trades"):
            return False
        return int(meta.get("max_pages") or 0) >= int(max_pages)

    cache_path = user_trades_cache_path(data_dir, wallet) if data_dir else None
    if (
        use_cache
        and cache_path
        and cache_path.exists()
        and not should_refresh_file_cache(
            cache_path.stat().st_mtime,
            now_ts=now_ts or int(time.time()),
            ttl_hours=cache_ttl_days * 24,
            force_refresh=force_refresh,
        )
    ):
        cached = read_json(cache_path, {})
        if cache_meta_compatible(cached.get("meta") or {}):
            return cached.get("trades") or []

    trades = []
    limit = max(1, int(page_limit))
    for page in range(max(1, int(max_pages))):
        offset = page * limit
        try:
            batch = client.trades_for_user(wallet, limit=limit, offset=offset)
        except RuntimeError as exc:
            if offset > 0 and "HTTP Error 400" in str(exc):
                break
            raise
        if not batch:
            break
        trades.extend(batch)
        if len(batch) < limit:
            break
    if use_cache and cache_path:
        write_json(
            cache_path,
            {
                "meta": expected_meta,
                "trades": trades,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return trades


def user_trade_backfill_candidate_condition_ids(
    raw_trades_by_wallet: dict[str, list[dict]],
    known_condition_ids: set[str],
) -> set[str]:
    known = {str(value).lower() for value in known_condition_ids if value}
    candidates: set[str] = set()
    for trades in raw_trades_by_wallet.values():
        for trade in trades or []:
            condition_id = str(trade.get("conditionId") or trade.get("condition_id") or trade.get("market") or "").lower()
            if not condition_id or condition_id in known:
                continue
            text = normalize_market_text(
                " ".join(
                    str(trade.get(key) or "")
                    for key in ("title", "eventTitle", "event_title", "question", "marketQuestion", "market_question", "marketName")
                )
            )
            if not text:
                continue
            allowed_family = (
                "dota 2" in text
                or text.startswith("lol ")
                or "league of legends" in text
                or "counter strike" in text
                or "cs2" in text
            )
            numbered_winner = re.search(r"\b(game|map)\s+[1-5]\b.*\bwinner\b", text) or re.search(
                r"\bwinner\b.*\b(game|map)\s+[1-5]\b",
                text,
            )
            if allowed_family and numbered_winner:
                candidates.add(condition_id)
    return candidates


def _normalize_backfill_market(market: dict[str, Any]) -> dict[str, Any]:
    condition_id = str(market.get("conditionId") or market.get("condition_id") or "").lower()
    if market.get("conditionId") and market.get("outcomePrices") is not None:
        return {**market, "conditionId": condition_id}
    tokens = market.get("tokens") or []
    outcomes = [token.get("outcome") for token in tokens if token.get("outcome") is not None]
    prices = [to_float(token.get("price")) for token in tokens if token.get("outcome") is not None]
    return {
        **market,
        "conditionId": condition_id,
        "question": market.get("question"),
        "outcomes": outcomes,
        "outcomePrices": prices,
        "endDate": market.get("end_date_iso") or market.get("endDate"),
        "gameStartTime": market.get("game_start_time") or market.get("gameStartTime"),
        "eventStartTime": market.get("game_start_time") or market.get("eventStartTime"),
        "closed": bool(market.get("closed")),
        "active": bool(market.get("active")),
    }


def _infer_backfill_event_tags(*values: Any) -> list[dict[str, str]]:
    text = normalize_market_text(" ".join(str(value or "") for value in values))
    slugs: list[str] = []
    if "dota 2" in text:
        slugs.extend(["esports", "dota-2"])
    elif "counter strike" in text or "cs2" in text:
        slugs.extend(["esports", "counter-strike-2"])
    elif text.startswith("lol ") or " league of legends " in f" {text} ":
        slugs.extend(["esports", "league-of-legends"])
    return [{"slug": slug} for slug in dict.fromkeys(slugs)]


def _market_event_for_backfill(market: dict[str, Any]) -> dict[str, Any] | None:
    events = market.get("events") or market.get("event") or []
    if isinstance(events, dict):
        event = dict(events)
    elif isinstance(events, list) and events:
        event = dict(events[0])
    else:
        tags = market.get("tags") or []
        question = str(market.get("question") or "")
        event_title = re.sub(r"\s+-\s+(game|map)\s+[1-5]\s+winner\s*$", "", question, flags=re.IGNORECASE)
        event = {
            "id": str(market.get("event_id") or market.get("market_slug") or market.get("conditionId") or ""),
            "slug": market.get("event_slug") or market.get("market_slug"),
            "title": market.get("event_title") or event_title or question,
            "tags": [{"slug": str(tag).lower().replace(" ", "-")} for tag in tags],
            "closed": bool(market.get("closed")),
            "endDate": market.get("endDate"),
            "startTime": market.get("gameStartTime") or market.get("eventStartTime"),
        }
    if not event.get("tags"):
        event["tags"] = _infer_backfill_event_tags(event.get("title"), market.get("question"))
    if not event.get("endDate"):
        event["endDate"] = market.get("endDate")
    if not event.get("startTime"):
        event["startTime"] = market.get("gameStartTime") or market.get("eventStartTime")
    if not event.get("closed") and market.get("closed") is not None:
        event["closed"] = bool(market.get("closed"))
    event["markets"] = [market]
    return event


def _read_cached_condition_market(
    condition_id: str,
    *,
    data_dir: Path | None,
    now_ts: int | None,
    cache_ttl_days: int,
    force_refresh: bool,
    use_cache: bool,
) -> tuple[dict[str, Any] | None, bool]:
    if not data_dir or not use_cache:
        return None, False
    cache_path = condition_market_cache_path(data_dir, condition_id)
    if not cache_path.exists() or should_refresh_file_cache(
        cache_path.stat().st_mtime,
        now_ts=now_ts or int(time.time()),
        ttl_hours=cache_ttl_days * 24,
        force_refresh=force_refresh,
    ):
        return None, False
    cached = read_json(cache_path, {})
    if cached.get("condition_id") != condition_id:
        return None, False
    return cached.get("market"), True


def _write_cached_condition_market(
    condition_id: str,
    market: dict[str, Any] | None,
    *,
    data_dir: Path | None,
    use_cache: bool,
) -> None:
    if not data_dir or not use_cache:
        return
    write_json(
        condition_market_cache_path(data_dir, condition_id),
        {
            "condition_id": condition_id,
            "market": market,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def backfill_user_trade_submarkets(
    client: PolymarketClient,
    raw_trades_by_wallet: dict[str, list[dict]],
    known_market_records_by_id: dict[str, dict[str, Any]],
    *,
    batch_size: int = 50,
    data_dir: Path | None = None,
    now_ts: int | None = None,
    cache_ttl_days: int = 30,
    force_refresh: bool = False,
    use_cache: bool = True,
    max_workers: int = 8,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    known_ids = {str(value).lower() for value in known_market_records_by_id}
    candidate_ids = sorted(user_trade_backfill_candidate_condition_ids(raw_trades_by_wallet, known_ids))
    backfilled: dict[str, dict[str, Any]] = {}
    cache_hits = 0
    api_fetches = 0
    api_error_count = 0
    api_error_condition_count = 0

    raw_markets_by_id: dict[str, dict[str, Any] | None] = {}
    uncached_ids: list[str] = []
    for condition_id in candidate_ids:
        cached_market, hit = _read_cached_condition_market(
            condition_id,
            data_dir=data_dir,
            now_ts=now_ts,
            cache_ttl_days=cache_ttl_days,
            force_refresh=force_refresh,
            use_cache=use_cache,
        )
        if hit:
            cache_hits += 1
            raw_markets_by_id[condition_id] = cached_market
        else:
            uncached_ids.append(condition_id)

    def fetch_chunk(chunk: list[str]) -> tuple[list[str], list[dict[str, Any]], bool]:
        try:
            return chunk, client.markets_by_condition_ids(chunk, limit=len(chunk)), False
        except Exception:
            return chunk, [], True

    chunks = [uncached_ids[index : index + max(1, int(batch_size))] for index in range(0, len(uncached_ids), max(1, int(batch_size)))]
    lookup_results = run_ordered_io_tasks(chunks, fetch_chunk, max_workers=max_workers) if chunks else []
    for result in lookup_results:
        if isinstance(result, Exception):
            continue
        chunk, markets, errored = result
        if errored:
            api_error_count += 1
            api_error_condition_count += len(chunk)
            continue
        api_fetches += len(chunk)
        markets_by_condition = {
            str(market.get("conditionId") or market.get("condition_id") or "").lower(): market
            for market in markets or []
        }
        for condition_id in chunk:
            market = markets_by_condition.get(condition_id)
            raw_markets_by_id[condition_id] = market
            _write_cached_condition_market(condition_id, market, data_dir=data_dir, use_cache=use_cache)

    for raw_market in raw_markets_by_id.values():
        if not raw_market:
            continue
        market = _normalize_backfill_market(raw_market)
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").lower()
        if not condition_id or condition_id in known_ids:
            continue
        event = _market_event_for_backfill(market)
        if not event:
            continue
        market_type = classify_market_type(event, market)
        prices = [
            to_float(value)
            for value in parse_jsonish(market.get("outcomePrices") or market.get("outcome_prices"), [])
        ]
        if market_type not in {GAME_WINNER, MAP_WINNER} or not is_settled_binary_prices(prices):
            continue
        records = event_to_market_records(event, allowed_market_types={GAME_WINNER, MAP_WINNER})
        record = next((row for row in records if row.get("condition_id") == condition_id), None)
        if not record:
            continue
        backfilled[condition_id] = record
    by_type: dict[str, int] = {}
    for record in backfilled.values():
        market_type = str(record.get("market_type") or "")
        by_type[market_type] = by_type.get(market_type, 0) + 1
    return backfilled, {
        "user_trade_backfill_candidate_count": len(candidate_ids),
        "user_trade_backfilled_market_count": len(backfilled),
        "user_trade_backfilled_by_market_type": dict(sorted(by_type.items())),
        "user_trade_backfill_cache_hits": cache_hits,
        "user_trade_backfill_api_fetches": api_fetches,
        "user_trade_backfill_api_error_count": api_error_count,
        "user_trade_backfill_api_error_condition_count": api_error_condition_count,
    }


def empty_user_trade_backfill_summary() -> dict[str, Any]:
    return {
        "user_trade_backfill_candidate_count": 0,
        "user_trade_backfilled_market_count": 0,
        "user_trade_backfilled_by_market_type": {},
        "user_trade_backfill_cache_hits": 0,
        "user_trade_backfill_api_fetches": 0,
        "user_trade_backfill_api_error_count": 0,
        "user_trade_backfill_api_error_condition_count": 0,
    }


def observed_performance_quarantine_events(
    performance: dict[str, Any],
    *,
    now_ts: int,
    min_signals: int = 10,
) -> list[dict[str, Any]]:
    events = []
    for wallet, row in sorted((performance.get("wallets") or {}).items()):
        wallet = normalize_wallet(wallet)
        if not wallet:
            continue
        signals = int(row.get("signals") or 0)
        wins = int(row.get("wins") or 0)
        if signals < min_signals:
            continue
        win_rate = wins / signals if signals else 0.0
        if win_rate < 0.5 or to_float(row.get("our_pnl")) < 0:
            events.append(
                {
                    "wallet": wallet,
                    "reason": "observed_paper_underperformance",
                    "timestamp": now_ts,
                }
            )
    return events


def market_records_from_events(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    records = {}
    for event in events:
        for record in event_to_market_records(event):
            records[record["condition_id"]] = record
    return records


def active_cache_market_rows(cached: dict[str, Any]) -> list[dict[str, Any]]:
    markets = cached.get("markets") if isinstance(cached, dict) else []
    if isinstance(markets, dict):
        rows = list(markets.values())
    elif isinstance(markets, list):
        rows = markets
    else:
        rows = []
    return [row for row in rows if isinstance(row, dict)]


def active_cache_categories(cached: dict[str, Any]) -> set[str]:
    explicit = {
        str(value).lower()
        for value in (cached.get("categories") or [])
        if value
    }
    rows = active_cache_market_rows(cached)
    return explicit | {str(row.get("category") or "").lower() for row in rows if isinstance(row, dict) and row.get("category")}


def active_cache_has_required_categories(cached: dict[str, Any]) -> bool:
    categories = active_cache_categories(cached)
    return set(FOLLOW_SIGNAL_CATEGORIES).issubset(categories)


def active_market_cache_in_follow_scope(
    market: dict[str, Any],
    *,
    now_ts: int,
    observe_window_hours: float,
    post_start_grace_seconds: int,
    allowed_categories: set[str],
    preserve_condition_ids: set[str] | None = None,
) -> bool:
    condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
    if preserve_condition_ids and condition_id in preserve_condition_ids:
        return True
    category = str(market.get("category") or "esports").lower()
    if category not in allowed_categories:
        return False
    start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"))
    if not start_dt:
        return False
    start_ts = int(start_dt.timestamp())
    window_end = now_ts + int(max(1.0, float(observe_window_hours)) * 3600)
    grace_start = now_ts - max(0, int(post_start_grace_seconds))
    return grace_start <= start_ts <= window_end


def scoped_active_market_cache_rows(
    rows: list[dict[str, Any]],
    *,
    now_ts: int,
    observe_window_hours: float,
    post_start_grace_seconds: int,
    allowed_categories: set[str] | None = None,
    preserve_condition_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    allowed = {str(value).lower() for value in (allowed_categories or set(FOLLOW_SIGNAL_CATEGORIES))}
    return [
        row
        for row in rows
        if isinstance(row, dict)
        and active_market_cache_in_follow_scope(
            row,
            now_ts=now_ts,
            observe_window_hours=observe_window_hours,
            post_start_grace_seconds=post_start_grace_seconds,
            allowed_categories=allowed,
            preserve_condition_ids=preserve_condition_ids,
        )
    ]


def load_active_market_cache(
    client: PolymarketClient,
    state: dict[str, Any],
    *,
    cache_path: Path | None = None,
    now_ts: int,
    gamma_pages: int,
    ttl_seconds: int,
    observe_window_hours: float = 24.0,
    post_start_grace_seconds: int = 0,
    allowed_categories: set[str] | None = None,
    preserve_condition_ids: set[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], str]:
    allowed = {str(value).lower() for value in (allowed_categories or set(FOLLOW_SIGNAL_CATEGORIES))}
    preserve_ids = {str(value).lower() for value in (preserve_condition_ids or set()) if value}

    def rows_to_markets(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        scoped_rows = scoped_active_market_cache_rows(
            rows,
            now_ts=now_ts,
            observe_window_hours=observe_window_hours,
            post_start_grace_seconds=post_start_grace_seconds,
            allowed_categories=allowed,
            preserve_condition_ids=preserve_ids,
        )
        return {
            str(row.get("condition_id") or row.get("conditionId") or "").lower(): row
            for row in scoped_rows
            if row.get("condition_id") or row.get("conditionId")
        }

    legacy_cached = state.pop("active_market_cache", None) or {}
    if cache_path and legacy_cached:
        if now_ts - int(legacy_cached.get("updated_at") or 0) < ttl_seconds and active_cache_has_required_categories(legacy_cached):
            markets = rows_to_markets(active_cache_market_rows(legacy_cached))
            write_json(
                cache_path,
                {"updated_at": legacy_cached.get("updated_at") or now_ts, "categories": sorted(allowed), "markets": list(markets.values())},
            )
            return markets, state, "legacy_state_cache"
        write_json(cache_path, legacy_cached)
    cached = read_json(cache_path, {}) if cache_path else {}
    if cached and now_ts - int(cached.get("updated_at") or 0) < ttl_seconds and active_cache_has_required_categories(cached):
        markets = rows_to_markets(active_cache_market_rows(cached))
        if cache_path and len(markets) != len(active_cache_market_rows(cached)):
            write_json(
                cache_path,
                {"updated_at": cached.get("updated_at") or now_ts, "categories": sorted(allowed), "markets": list(markets.values())},
            )
        return markets, state, "cache"
    markets: dict[str, dict[str, Any]] = {}
    fetched_categories: list[str] = []
    for category, tag_slugs in CATEGORY_TAG_SLUGS.items():
        if category not in allowed:
            continue
        events = client.list_events_paginated(
            closed=False,
            active=True,
            max_pages=gamma_pages,
            order="startTime",
            tag_slugs=tag_slugs,
        )
        markets.update(market_records_from_events(events))
        fetched_categories.append(category)
    scoped_markets = rows_to_markets(list(markets.values()))
    cache_value = {
        "updated_at": now_ts,
        "categories": fetched_categories,
        "markets": list(scoped_markets.values()),
    }
    if cache_path:
        write_json(cache_path, cache_value)
    else:
        state["active_market_cache"] = cache_value
    return scoped_markets, state, "api"


TEAM_LOGO_URL_RE = re.compile(
    r"(?:url=)?(https%3A%2F%2Fpolymarket-upload\.s3\.us-east-2\.amazonaws\.com%2F[^&\"<>\s]+?\.(?:png|jpg|jpeg|webp)|https://polymarket-upload\.s3\.us-east-2\.amazonaws\.com/[^\"<>\s]+?\.(?:png|jpg|jpeg|webp))",
    re.IGNORECASE,
)
MATCH_TITLE_TEAMS_RE = re.compile(r"^([^:]+):\s+(.+?)\s+vs\s+(.+?)(\s+\([^)]+\))?\s+-\s+(.+)$", re.IGNORECASE)
SPORTS_TITLE_TEAMS_RE = re.compile(r"^(.+?)\s+vs\.?\s+(.+?)(?:\s+-\s+(.+))?$", re.IGNORECASE)


def refresh_team_logo_cache_from_active_markets(
    data_dir: Path,
    *,
    timeout_seconds: int = 8,
    max_workers: int = 8,
    max_events: int = 0,
    observe_window_hours: float = 24.0,
    now_ts: int | None = None,
    fetch_html: Any = None,
    fetch_logo_bytes: Any = None,
) -> dict[str, Any]:
    active_cache = read_json(data_dir / "follow" / "active_market_cache.json", {})
    markets = active_cache.get("markets") if isinstance(active_cache, dict) else []
    if not isinstance(markets, list):
        markets = list(markets.values()) if isinstance(markets, dict) else []
    by_slug: dict[str, dict[str, Any]] = {}
    current_ts = int(now_ts if now_ts is not None else time.time())
    window_seconds = int(observe_window_hours * 3600)
    for market in markets:
        if not isinstance(market, dict):
            continue
        start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time") or market.get("startTime"))
        if start_dt:
            start_ts = int(start_dt.timestamp())
            if start_ts < current_ts - window_seconds or start_ts > current_ts + window_seconds:
                continue
        slug = str(market.get("event_slug") or "").strip()
        if slug:
            by_slug.setdefault(slug, market)
    logo_dir = Path(__file__).with_name("dashboard") / "static" / "team_logos"
    logo_path = logo_dir / "team_logos.json"
    cache = read_json(logo_path, {})
    if not isinstance(cache, dict):
        cache = {}
    teams = cache.get("teams") if isinstance(cache.get("teams"), dict) else {}
    teams = dict(teams)

    def cached_logo_exists(value: str) -> bool:
        url = str(value or "")
        if not url.startswith("/team_logos/"):
            return False
        return (logo_dir / url.rsplit("/", 1)[-1]).exists()

    slugs = []
    for slug, market in by_slug.items():
        fallback_game = str(market.get("league_label") or LEAGUE_LABELS.get(str(market.get("league") or "").lower(), "") or "")
        game, team_a, team_b = _match_title_teams(str(market.get("title") or market.get("question") or ""), fallback_game=fallback_game)
        game_key = _normalize_team_logo_key(game)
        keys = []
        for team in (team_a, team_b):
            team_key = _normalize_team_logo_key(team)
            keys.append(team_key)
            if game_key:
                keys.append(_normalize_team_logo_key(f"{game_key}:{team_key}"))
        if not keys or not all(any(cached_logo_exists(teams.get(key, "")) for key in pair) for pair in (keys[0:2], keys[2:4])):
            slugs.append(slug)
    if max_events and max_events > 0:
        slugs = slugs[:max_events]

    def default_fetch_html(slug: str) -> str:
        url = f"https://polymarket.com/zh/event/{slug}"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        return urlopen(request, timeout=timeout_seconds).read().decode("utf-8", "ignore")

    fetcher = fetch_html or default_fetch_html
    logo_fetcher = fetch_logo_bytes or _fetch_team_logo_bytes

    def local_logo_url(remote_url: str) -> str:
        suffix = Path(remote_url.split("?", 1)[0]).suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
            suffix = ".png"
        digest = hashlib.sha1(remote_url.encode("utf-8")).hexdigest()[:20]
        return f"/team_logos/{digest}{suffix}"

    def worker(slug: str) -> tuple[int, list[tuple[str, str, str, bytes]]]:
        market = by_slug[slug]
        html = fetcher(slug)
        fallback_game = str(market.get("league_label") or LEAGUE_LABELS.get(str(market.get("league") or "").lower(), "") or "")
        game, team_a, team_b = _match_title_teams(str(market.get("title") or market.get("question") or ""), fallback_game=fallback_game)
        title_teams = [team_a, team_b]
        rows: list[tuple[str, str, str, bytes]] = []
        seen_urls: set[str] = set()
        for match in TEAM_LOGO_URL_RE.finditer(html):
            encoded_url = match.group(1)
            logo_url = unquote(html_lib.unescape(encoded_url))
            if logo_url in seen_urls:
                continue
            seen_urls.add(logo_url)
            team_key = _team_logo_key_from_url(logo_url, game=game, title_teams=title_teams)
            if not _team_logo_key_matches_title(team_key, title_teams):
                img_start = html.rfind("<img", 0, match.start())
                context_start = img_start if img_start >= 0 else max(0, match.start() - 600)
                context = html[context_start : match.end() + 200]
                alt_match = re.search(r'alt=["\']([^"\']+)["\']', context, re.IGNORECASE)
                team_key = _team_logo_key_from_alt(alt_match.group(1) if alt_match else "", title_teams=title_teams)
            if not _team_logo_key_matches_title(team_key, title_teams):
                continue
            local_url = local_logo_url(logo_url)
            logo_bytes = b""
            if not (logo_dir / local_url.rsplit("/", 1)[-1]).exists():
                try:
                    logo_bytes = logo_fetcher(logo_url, timeout_seconds)
                except Exception:
                    continue
            rows.append((team_key, logo_url, local_url, logo_bytes))
            game_key = _normalize_team_logo_key(game)
            if game_key:
                rows.append((_normalize_team_logo_key(f"{game_key}:{team_key}"), logo_url, local_url, logo_bytes))
        return len(rows), rows

    fetched = 0
    failed = 0
    updated = 0
    if slugs:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
            futures = [pool.submit(worker, slug) for slug in slugs]
            for future in as_completed(futures):
                try:
                    count, rows = future.result()
                except Exception:
                    failed += 1
                    continue
                if count:
                    fetched += 1
                for key, _remote_url, local_url, logo_bytes in rows:
                    if logo_bytes:
                        logo_dir.mkdir(parents=True, exist_ok=True)
                        (logo_dir / local_url.rsplit("/", 1)[-1]).write_bytes(logo_bytes)
                    if key and teams.get(key) != local_url:
                        teams[key] = local_url
                        updated += 1

    if updated or not logo_path.exists():
        payload = {
            "updated_at": int(time.time()),
            "source": "polymarket watched event preload images",
            "teams": teams,
        }
        write_json(logo_path, payload)
    return {
        "watched_event_count": len(slugs),
        "fetched_event_count": fetched,
        "failed_event_count": failed,
        "updated_logo_key_count": updated,
        "total_logo_key_count": len(teams),
        "path": str(logo_path),
    }


def _fetch_team_logo_bytes(url: str, timeout_seconds: int) -> bytes:
    parts = urlsplit(url)
    safe_url = urlunsplit((parts.scheme, parts.netloc, quote(parts.path), parts.query, parts.fragment))
    request = Request(safe_url, headers={"User-Agent": "Mozilla/5.0"})
    return urlopen(request, timeout=timeout_seconds).read()


def _match_title_teams(title: str, *, fallback_game: str = "Sports") -> tuple[str, str, str]:
    match = MATCH_TITLE_TEAMS_RE.match(title)
    if match:
        return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
    sports_match = SPORTS_TITLE_TEAMS_RE.match(title)
    if sports_match:
        return fallback_game, sports_match.group(1).strip(), sports_match.group(2).strip()
    return "", "", ""


def _team_logo_key_from_url(logo_url: str, *, game: str, title_teams: list[str]) -> str:
    base = unquote(logo_url).split("/")[-1]
    base = re.sub(r"\.png(?:\?.*)?$", "", base)
    base = re.sub(r"_\d+$", "", base)
    for prefix in [
        game,
        game.replace(" ", "-"),
        game.replace(" ", "_"),
        "counter-strike",
        "cs2",
        "cs-go",
        "league-of-legends",
        "dota-2",
    ]:
        prefix = prefix.strip()
        if prefix and base.lower().startswith(prefix.lower() + "_"):
            base = base[len(prefix) + 1 :]
            break
    candidate = _normalize_team_logo_key(base)
    for team in title_teams:
        title_key = _normalize_team_logo_key(team)
        if title_key and (candidate == title_key or candidate.endswith(f" {title_key}") or title_key in candidate.split()):
            return title_key
    if "-" in base:
        first, rest = base.split("-", 1)
        if re.search(r"\d", rest) or len(rest) >= 6:
            base = first
    candidate = _normalize_team_logo_key(base)
    for team in title_teams:
        title_key = _normalize_team_logo_key(team)
        if title_key and (candidate == title_key or candidate.endswith(f" {title_key}") or title_key in candidate.split()):
            return title_key
    return candidate


def _team_logo_key_from_alt(alt: str, *, title_teams: list[str]) -> str:
    candidate = _normalize_team_logo_key(html_lib.unescape(str(alt or "")))
    if not candidate:
        return ""
    for team in title_teams:
        title_key = _normalize_team_logo_key(team)
        if title_key and (candidate == title_key or title_key.endswith(f" {candidate}") or candidate in title_key.split()):
            return title_key
    return ""


def _team_logo_key_matches_title(team_key: str, title_teams: list[str]) -> bool:
    normalized = _normalize_team_logo_key(team_key)
    if not normalized:
        return False
    return any(normalized == _normalize_team_logo_key(team) for team in title_teams)


def _normalize_team_logo_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def command_refresh_team_logos(args: argparse.Namespace) -> int:
    summary = refresh_team_logo_cache_from_active_markets(
        resolve_data_dir(args),
        timeout_seconds=args.logo_timeout_seconds,
        max_workers=args.max_workers,
        max_events=args.max_events,
        observe_window_hours=args.observe_window_hours,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def watched_markets(
    active_markets: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    observe_window_hours: float,
    post_start_grace_seconds: int = 0,
) -> dict[str, dict[str, Any]]:
    window_end = now_ts + int(observe_window_hours * 3600)
    grace_start = now_ts - max(0, int(post_start_grace_seconds))
    watched = {}
    for condition_id, market in active_markets.items():
        start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
        if not start_dt:
            continue
        start_ts = int(start_dt.timestamp())
        if grace_start <= start_ts <= window_end and (now_ts < start_ts or post_start_grace_seconds > 0):
            watched[condition_id] = market
    return watched


def fetch_resolutions_for_open_signals(
    client: PolymarketClient,
    open_signals: list[dict[str, Any]],
    *,
    state: dict[str, Any],
    store: FollowStore | None = None,
    now_ts: int,
    gamma_pages: int,
    ttl_seconds: int,
) -> dict[str, int]:
    eligible_signals = []
    for signal in open_signals:
        start_dt = parse_dt(signal.get("match_start_time"))
        if start_dt and now_ts < int(start_dt.timestamp()):
            continue
        eligible_signals.append(signal)
    if not eligible_signals:
        return {}
    needed = {str(signal.get("condition_id") or "").lower() for signal in eligible_signals}
    cached = state.pop("closed_market_cache", None) or {}
    if store:
        closed_markets, _updated_at, fresh = store.load_market_cache(
            cache_kind="closed",
            now_ts=now_ts,
            ttl_seconds=ttl_seconds,
        )
        if fresh:
            cached = {}
        elif cached:
            store.save_market_cache(
                {row["condition_id"]: row for row in cached.get("markets") or []},
                cache_kind="closed",
                updated_at=int(cached.get("updated_at") or 0),
            )
    if not store and cached and now_ts - int(cached.get("updated_at") or 0) < ttl_seconds:
        closed_markets = {row["condition_id"]: row for row in cached.get("markets") or []}
    elif store and closed_markets and fresh:
        pass
    else:
        events = client.list_events_paginated(closed=True, active=None, max_pages=gamma_pages, order="endDate")
        closed_markets = market_records_from_events(events)
        if store:
            store.save_market_cache(closed_markets, cache_kind="closed", updated_at=now_ts)
        else:
            state["closed_market_cache"] = {
                "updated_at": now_ts,
                "markets": list(closed_markets.values()),
            }
    resolutions = {}
    for condition_id, market in closed_markets.items():
        if condition_id in needed:
            winner = winner_outcome_index(market)
            if winner is not None:
                resolutions[condition_id] = winner
    return resolutions


def fetch_user_trades_until_cursor(
    client: PolymarketClient,
    wallet: str,
    *,
    previous_cursor: dict[str, Any] | None,
    limit: int,
    max_pages: int,
) -> list[dict]:
    trades: list[dict] = []
    if max_pages <= 0:
        return trades
    cursor_ts = int((previous_cursor or {}).get("timestamp") or 0)
    cursor_id = str((previous_cursor or {}).get("id") or "")
    for page in range(max_pages):
        offset = page * limit
        batch = client.trades_for_user(wallet, limit=limit, offset=offset)
        trades.extend(batch)
        if previous_cursor is None:
            break
        found_cursor = False
        for trade in batch:
            ts = int(trade.get("timestamp") or 0)
            tid = str(trade.get("id") or trade.get("transactionHash") or "")
            if ts < cursor_ts or (ts == cursor_ts and (not cursor_id or tid == cursor_id)):
                found_cursor = True
                break
        if found_cursor or len(batch) < limit:
            break
    return trades


def should_use_cached_profile(cached: dict[str, Any] | None, *, now_ts: int, ttl_seconds: int) -> bool:
    if not cached:
        return False
    if cached.get("profile_state") == "failed_retryable":
        return False
    if not isinstance(cached.get("esports_condition_ids"), list):
        return False
    if int(cached.get("scoring_version") or 0) != SCORING_VERSION:
        return False
    return now_ts - int(cached.get("profiled_at", 0)) < ttl_seconds


def should_refresh_file_cache(
    cache_mtime: float | None,
    *,
    now_ts: int,
    ttl_hours: int,
    force_refresh: bool = False,
) -> bool:
    if force_refresh or cache_mtime is None:
        return True
    return now_ts - int(cache_mtime) >= ttl_hours * 3600


def candidate_two_sided_within_limits(
    metrics: dict[str, Any],
    *,
    max_count: int = 0,
    max_rate: float = 0.0,
) -> bool:
    two_sided_count = int(metrics.get("two_sided_market_count") or 0)
    if two_sided_count <= 0:
        return True
    participated = int(metrics.get("participated_market_count") or metrics.get("historical_trade_behavior_market_count") or 0)
    two_sided_rate = two_sided_count / participated if participated > 0 else 1.0
    return two_sided_count <= max_count and two_sided_rate <= max_rate


def filter_profile_candidates(
    candidates: list[dict[str, Any]],
    *,
    min_participated_markets: int = 1,
    min_avg_market_cash: float = 1_500,
    require_clean_discovery: bool = True,
    max_tail_entry_rate: float = 0.34,
    market_type_thresholds: dict[str, dict[str, float]] | None = None,
    game_family_thresholds: dict[str, dict[str, float]] | None = None,
) -> list[dict[str, Any]]:
    rows = []
    for candidate in candidates:
        if market_type_thresholds or game_family_thresholds:
            qualified_market_types = []
            qualified_game_families = []
            qualified_buckets = []
            per_type = candidate.get("per_type_candidate") if isinstance(candidate.get("per_type_candidate"), dict) else {}
            per_game_family = (
                candidate.get("per_game_family_candidate")
                if isinstance(candidate.get("per_game_family_candidate"), dict)
                else {}
            )
            per_game_type = (
                candidate.get("per_game_type_candidate")
                if isinstance(candidate.get("per_game_type_candidate"), dict)
                else {}
            )

            def qualifies_metrics(metrics: dict[str, Any], thresholds: dict[str, float]) -> bool:
                participated = int(metrics.get("participated_market_count") or 0)
                if participated < int(thresholds.get("min_participated_markets") or 0):
                    return False
                avg_market_cash = to_float(metrics.get("avg_market_cash"))
                if avg_market_cash < to_float(thresholds.get("min_avg_market_cash")):
                    return False
                if require_clean_discovery:
                    if not candidate_two_sided_within_limits(metrics):
                        return False
                    tail_entry_count = int(metrics.get("tail_entry_market_count") or 0)
                    if participated > 0 and tail_entry_count / participated > max_tail_entry_rate:
                        return False
                return True

            for market_type, thresholds in (market_type_thresholds or {}).items():
                metrics = per_type.get(market_type)
                if not isinstance(metrics, dict):
                    continue
                if qualifies_metrics(metrics, thresholds):
                    qualified_market_types.append(market_type)

            for game_family, thresholds in (game_family_thresholds or {}).items():
                family_bucket_qualified = False
                for key, metrics in per_game_type.items():
                    bucket_game_family, market_type = split_bucket_key(key)
                    if bucket_game_family != game_family or not isinstance(metrics, dict):
                        continue
                    if not qualifies_metrics(metrics, thresholds):
                        continue
                    family_bucket_qualified = True
                    if key not in qualified_buckets:
                        qualified_buckets.append(key)
                    if market_type not in qualified_market_types:
                        qualified_market_types.append(market_type)
                if not family_bucket_qualified:
                    metrics = per_game_family.get(game_family)
                    if not isinstance(metrics, dict) or not qualifies_metrics(metrics, thresholds):
                        continue
                    family_bucket_qualified = True
                    for market_type, type_metrics in per_type.items():
                        if not isinstance(type_metrics, dict):
                            continue
                        family_market_ids = set(str(value) for value in metrics.get("participated_market_ids") or [])
                        type_market_ids = set(str(value) for value in type_metrics.get("participated_market_ids") or [])
                        if family_market_ids & type_market_ids and market_type not in qualified_market_types:
                            qualified_market_types.append(market_type)
                if family_bucket_qualified:
                    qualified_game_families.append(game_family)
            if qualified_market_types or qualified_buckets:
                payload = {**candidate, "qualified_market_types": qualified_market_types}
                if qualified_game_families:
                    payload["qualified_game_families"] = qualified_game_families
                if qualified_buckets:
                    payload["qualified_buckets"] = qualified_buckets
                    payload["qualified_bucket_labels"] = [bucket_label(value) for value in qualified_buckets]
                rows.append(payload)
            continue
        participated = int(candidate.get("participated_market_count") or 0)
        if participated < min_participated_markets:
            continue
        avg_market_cash = to_float(candidate.get("avg_market_cash") or candidate.get("avg_market_usd"))
        if avg_market_cash < min_avg_market_cash:
            continue
        if require_clean_discovery:
            if not candidate_two_sided_within_limits(candidate):
                continue
            # Gate on tail-entry rate, not any-occurrence, so a single tail entry among
            # many clean markets doesn't block an otherwise elite wallet pre-profiling.
            tail_entry_count = int(candidate.get("tail_entry_market_count") or 0)
            if participated > 0 and tail_entry_count / participated > max_tail_entry_rate:
                continue
        rows.append(candidate)
    return rows


def league_event_counts_from_classification_set(classification_set: list[dict[str, Any]]) -> dict[str, int]:
    event_keys_by_league: dict[str, set[str]] = {}
    for row in classification_set:
        if str(row.get("category") or "").lower() != "sports":
            continue
        league = str(row.get("league") or "").lower()
        if not league:
            continue
        event_key = str(row.get("event_id") or row.get("event_slug") or row.get("condition_id") or "").lower()
        if not event_key:
            continue
        event_keys_by_league.setdefault(league, set()).add(event_key)
    return {league: len(event_keys) for league, event_keys in sorted(event_keys_by_league.items())}


def augment_profile_sports_league_fields(profile: dict[str, Any], market_records_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if str(profile.get("category") or "").lower() != "sports":
        return profile
    condition_ids = [str(value).lower() for value in profile.get("esports_condition_ids") or [] if value]
    condition_ids_by_league: dict[str, set[str]] = {}
    for condition_id in condition_ids:
        record = market_records_by_id.get(condition_id) or {}
        league = str(record.get("league") or "").lower()
        if league:
            condition_ids_by_league.setdefault(league, set()).add(condition_id)
    if not condition_ids_by_league and profile.get("league"):
        league = str(profile.get("league") or "").lower()
        condition_ids_by_league[league] = set(condition_ids)
    if not condition_ids_by_league:
        return profile
    primary_league = max(
        sorted(condition_ids_by_league),
        key=lambda league: len(condition_ids_by_league.get(league) or set()),
    )
    augmented = dict(profile)
    augmented["league"] = primary_league
    augmented["league_label"] = LEAGUE_LABELS.get(primary_league, primary_league.upper())
    augmented["league_condition_ids_by_league"] = {
        league: sorted(ids)
        for league, ids in sorted(condition_ids_by_league.items())
    }
    return augmented


def sports_wallet_participation(profile: dict[str, Any], league_event_counts: dict[str, int]) -> dict[str, Any] | None:
    if not league_event_counts:
        return None
    raw_by_league = profile.get("league_condition_ids_by_league")
    condition_ids_by_league: dict[str, set[str]] = {}
    if isinstance(raw_by_league, dict):
        for league, values in raw_by_league.items():
            normalized_league = str(league or "").lower()
            ids = {str(value).lower() for value in values or [] if value}
            if normalized_league and ids:
                condition_ids_by_league[normalized_league] = ids
    if not condition_ids_by_league:
        league = str(profile.get("league") or "").lower()
        ids = {str(value).lower() for value in profile.get("esports_condition_ids") or [] if value}
        if league and ids:
            condition_ids_by_league[league] = ids
    if not condition_ids_by_league:
        return None
    best: dict[str, Any] | None = None
    for league, ids in sorted(condition_ids_by_league.items()):
        denominator = int(league_event_counts.get(league) or 0)
        participated = len(ids)
        rate = participated / denominator if denominator > 0 else 0.0
        candidate = {
            "league": league,
            "league_label": LEAGUE_LABELS.get(league, league.upper()),
            "participated_events": participated,
            "eligible_event_count": denominator,
            "participation_rate": round(rate, 8),
        }
        if best is None or (candidate["participated_events"], candidate["participation_rate"]) > (
            best["participated_events"],
            best["participation_rate"],
        ):
            best = candidate
    return best


def build_leaderboard_from_profiles(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int | None = None,
    max_inactive_days: int = ESPORTS_DEFAULT_LEADERBOARD_MAX_INACTIVE_DAYS,
    min_participated_markets: int = 1,
    min_avg_market_cash: float = 1_500,
    require_tail_entry_field: bool = False,
    require_current_scoring_version: bool = False,
    max_leaderboard_wallets: int = 30,
    max_tail_entry_rate: float = 0.34,
    max_two_sided_market_count: int = 0,
    max_two_sided_market_rate: float = 0.0,
    min_pre_match_entry_rate: float = 0.0,
    league_event_counts: dict[str, int] | None = None,
    min_sports_participation_rate: float = 0.0,
    min_sports_participated_events: int = 5,
) -> list[dict[str, Any]]:
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    max_inactive_seconds = max_inactive_days * 86400
    leaderboard = []
    for profile in profiles_by_wallet.values():
        profile = dict(profile)
        if require_current_scoring_version and int(profile.get("scoring_version") or 0) != SCORING_VERSION:
            continue
        # Optional followability filter: drop wallets that mostly buy in-game (their alpha
        # isn't pre-match copyable). Off by default; only applied when a known rate exists.
        if min_pre_match_entry_rate > 0:
            pre_match_rate = profile.get("pre_match_entry_rate")
            if pre_match_rate is not None and to_float(pre_match_rate) < min_pre_match_entry_rate:
                continue
        eligible_market_types = profile.get("eligible_market_types") or []
        if profile.get("per_type_grades") is not None and not eligible_market_types:
            continue
        if profile.get("grade") != "A" and not eligible_market_types:
            continue
        if not eligible_market_types and to_float(profile.get("esports_roi")) < 0.30:
            continue
        # Grading is per market_type: an eligible bucket already cleared the positive-rate
        # floor on its own type, so only apply the blended-overall floor to legacy/overall
        # grades. Otherwise a real sub-specialist (strong on its type, weak overall) is
        # wrongly dropped here even though it qualified.
        if (
            not eligible_market_types
            and "positive_market_rate" in profile
            and to_float(profile.get("positive_market_rate")) < MIN_A_POSITIVE_MARKET_RATE
        ):
            continue
        behavior_market_count = int(profile.get("historical_trade_behavior_market_count") or 0)
        # Do NOT cut on sold_before_resolution rate: selling winners at ~0.99 to free capital
        # is smart-money behavior, not short-term noise. The copyability concern is captured by
        # actual_minus_hold (swing_dependent) — a wallet whose profit leans on in-game selling
        # (actual PnL >> hold-to-settlement PnL) can't be followed on entry alone, so exclude
        # it instead. Pure profit-takers have actual ≈ hold, so they pass.
        if to_float(profile.get("actual_minus_hold_pnl_rate")) > SWING_DEPENDENT_RATE:
            continue
        two_sided_trade_rate = to_float(profile.get("two_sided_trade_market_rate"))
        if (
            int(profile.get("two_sided_trade_market_count") or 0) > 0
            and behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS
            and two_sided_trade_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
        ):
            continue
        candidate = profile.get("candidate") or {}
        is_sports_profile = str(profile.get("category") or "").lower() == "sports"
        if is_sports_profile:
            profile = _with_sports_followable_market_types(profile)
            if profile is None:
                continue
            eligible_market_types = profile.get("eligible_market_types") or []
            participation = sports_wallet_participation(profile, league_event_counts or {})
            if participation is None:
                continue
            if int(participation.get("participated_events") or 0) < min_sports_participated_events:
                continue
            if to_float(participation.get("participation_rate")) < min_sports_participation_rate:
                continue
            profile = {**profile, **participation}
        else:
            profile = _with_esports_followable_market_types(profile)
            if profile is None:
                continue
            eligible_market_types = profile.get("eligible_market_types") or []
            candidate = profile.get("candidate") or {}
            eligible_buckets = [str(value) for value in profile.get("eligible_buckets") or [] if value]
            qualified_buckets = [str(value) for value in candidate.get("qualified_buckets") or [] if value]
            if qualified_buckets and eligible_buckets:
                followable_buckets = [key for key in eligible_buckets if key in set(qualified_buckets)]
                if not followable_buckets:
                    continue
                profile["eligible_buckets"] = followable_buckets
                profile["eligible_bucket_labels"] = [bucket_label(value) for value in followable_buckets]
                eligible_market_types = sorted(
                    {split_bucket_key(value)[1] for value in followable_buckets},
                    key=lambda value: {"main_match": 0, "game_winner": 1, "map_winner": 2}.get(value, 99),
                )
                profile["eligible_market_types"] = eligible_market_types
                profile["eligible_market_type_labels"] = [
                    MARKET_TYPE_LABELS.get(value, value)
                    for value in eligible_market_types
                ]
                profile["eligible_game_families"] = sorted(
                    {split_bucket_key(value)[0] for value in followable_buckets if split_bucket_key(value)[0]}
                )
            qualified_market_types = [
                str(value) for value in candidate.get("qualified_market_types") or [] if value
            ]
            if qualified_market_types and eligible_market_types and not qualified_buckets:
                followable_types = [
                    market_type
                    for market_type in eligible_market_types
                    if market_type in set(qualified_market_types)
                ]
                if not followable_types:
                    continue
                profile["eligible_market_types"] = followable_types
                eligible_market_types = followable_types
        effective_min_participated = 1 if is_sports_profile else min_participated_markets
        per_type = candidate.get("per_type_candidate") if isinstance(candidate.get("per_type_candidate"), dict) else {}
        qualified_market_types = [str(value) for value in candidate.get("qualified_market_types") or [] if value]
        qualified_buckets = [str(value) for value in candidate.get("qualified_buckets") or [] if value]
        eligible_buckets = [str(value) for value in profile.get("eligible_buckets") or [] if value]
        if not is_sports_profile and eligible_buckets and (
            qualified_buckets or isinstance(candidate.get("per_game_type_candidate"), dict)
        ):
            per_game_type = (
                candidate.get("per_game_type_candidate")
                if isinstance(candidate.get("per_game_type_candidate"), dict)
                else {}
            )
            behavior_buckets = [
                key
                for key in eligible_buckets
                if (not qualified_buckets or key in set(qualified_buckets)) and isinstance(per_game_type.get(key), dict)
            ]
            if not behavior_buckets:
                continue
            behavior_ok = False
            for key in behavior_buckets:
                metrics = per_game_type.get(key)
                if not isinstance(metrics, dict):
                    continue
                participated_count = int(metrics.get("participated_market_count") or 0)
                if not candidate_two_sided_within_limits(
                    metrics,
                    max_count=max_two_sided_market_count,
                    max_rate=max_two_sided_market_rate,
                ):
                    continue
                if require_tail_entry_field and "tail_entry_market_count" not in metrics:
                    continue
                tail_entry_count = int(metrics.get("tail_entry_market_count") or 0)
                if participated_count > 0 and tail_entry_count / participated_count > max_tail_entry_rate:
                    continue
                behavior_ok = True
                break
            if not behavior_ok:
                continue
        elif not is_sports_profile and qualified_market_types and eligible_market_types:
            behavior_types = [market_type for market_type in eligible_market_types if market_type in qualified_market_types]
            if not behavior_types:
                continue
            behavior_ok = False
            for market_type in behavior_types:
                metrics = per_type.get(market_type)
                if not isinstance(metrics, dict):
                    continue
                participated_count = int(metrics.get("participated_market_count") or 0)
                if not candidate_two_sided_within_limits(
                    metrics,
                    max_count=max_two_sided_market_count,
                    max_rate=max_two_sided_market_rate,
                ):
                    continue
                if require_tail_entry_field and "tail_entry_market_count" not in metrics:
                    continue
                tail_entry_count = int(metrics.get("tail_entry_market_count") or 0)
                if participated_count > 0 and tail_entry_count / participated_count > max_tail_entry_rate:
                    continue
                behavior_ok = True
                break
            if not behavior_ok:
                continue
        else:
            if int(candidate.get("participated_market_count") or 0) < effective_min_participated:
                continue
            avg_market_cash = to_float(candidate.get("avg_market_cash") or candidate.get("avg_market_usd"))
            effective_min_avg_market_cash = min(min_avg_market_cash, 3_000) if is_sports_profile else min_avg_market_cash
            if avg_market_cash < effective_min_avg_market_cash:
                continue
            if not candidate_two_sided_within_limits(
                candidate,
                max_count=max_two_sided_market_count,
                max_rate=max_two_sided_market_rate,
            ):
                continue
            if require_tail_entry_field and "tail_entry_market_count" not in candidate:
                continue
            # A single tail entry among many markets shouldn't disqualify an otherwise elite
            # wallet; gate on the rate of tail entries instead of any-occurrence.
            tail_entry_count = int(candidate.get("tail_entry_market_count") or 0)
            participated_count = int(candidate.get("participated_market_count") or 0)
            if participated_count > 0 and tail_entry_count / participated_count > max_tail_entry_rate:
                continue
        if not is_sports_profile:
            profile = enrich_esports_bucket_scores(profile, now_ts=now_ts)
            best_bucket_last_trade = int(profile.get("best_bucket_last_trade_at") or profile.get("last_esports_trade_at") or 0)
            if not best_bucket_last_trade or now_ts - best_bucket_last_trade > max_inactive_seconds:
                continue
            if _best_bucket_recent_performance_is_bad(profile):
                continue
        leaderboard.append(profile)
    ranked = sorted(leaderboard, key=leaderboard_rank_key)
    if max_leaderboard_wallets > 0:
        return ranked[:max_leaderboard_wallets]
    return ranked


def _with_esports_followable_market_types(profile: dict[str, Any]) -> dict[str, Any] | None:
    eligible_market_types = [str(value) for value in profile.get("eligible_market_types") or [] if value]
    eligible_buckets = [str(value) for value in profile.get("eligible_buckets") or [] if value]
    if eligible_buckets:
        per_game_type = profile.get("per_game_type") if isinstance(profile.get("per_game_type"), dict) else {}
        per_game_type_grades = (
            profile.get("per_game_type_grades")
            if isinstance(profile.get("per_game_type_grades"), dict)
            else {}
        )
        followable_buckets = []
        for key in eligible_buckets:
            game_family, _market_type = split_bucket_key(key)
            if game_family and game_family not in ALLOWED_GAME_FAMILIES:
                continue
            metrics = dict(profile)
            if isinstance(per_game_type.get(key), dict):
                metrics.update(per_game_type[key])
            if isinstance(per_game_type_grades.get(key), dict):
                metrics.update(per_game_type_grades[key])
            if _esports_followable_roi(metrics) < ESPORTS_LEADERBOARD_MIN_TYPE_ROI:
                continue
            if _followable_capital_edge(metrics) < ESPORTS_LEADERBOARD_MIN_TYPE_CAPITAL_EDGE:
                continue
            followable_buckets.append(key)
        if not followable_buckets:
            return None
        filtered = dict(profile)
        filtered["eligible_buckets"] = followable_buckets
        filtered["eligible_bucket_labels"] = [bucket_label(value) for value in followable_buckets]
        filtered["eligible_market_types"] = sorted(
            {split_bucket_key(value)[1] for value in followable_buckets},
            key=lambda value: {"main_match": 0, "game_winner": 1, "map_winner": 2}.get(value, 99),
        )
        filtered["eligible_market_type_labels"] = [
            MARKET_TYPE_LABELS.get(value, value)
            for value in filtered["eligible_market_types"]
        ]
        filtered["eligible_game_families"] = sorted(
            {split_bucket_key(value)[0] for value in followable_buckets if split_bucket_key(value)[0]}
        )
        filtered["eligible_game_family_labels"] = [
            GAME_FAMILY_LABELS.get(value, value.upper())
            for value in filtered["eligible_game_families"]
        ]
        if isinstance(per_game_type_grades, dict):
            filtered["per_game_type_grades"] = {
                key: value for key, value in per_game_type_grades.items() if key in followable_buckets
            }
        return filtered
    if not eligible_market_types:
        if _esports_followable_roi(profile) < ESPORTS_LEADERBOARD_MIN_TYPE_ROI:
            return None
        if _followable_capital_edge(profile) < ESPORTS_LEADERBOARD_MIN_TYPE_CAPITAL_EDGE:
            return None
        return profile

    per_type = profile.get("per_type") if isinstance(profile.get("per_type"), dict) else {}
    per_type_grades = profile.get("per_type_grades") if isinstance(profile.get("per_type_grades"), dict) else {}
    followable_types = []
    for market_type in eligible_market_types:
        metrics = dict(profile)
        if isinstance(per_type.get(market_type), dict):
            metrics.update(per_type[market_type])
        if isinstance(per_type_grades.get(market_type), dict):
            metrics.update(per_type_grades[market_type])
        if _esports_followable_roi(metrics) < ESPORTS_LEADERBOARD_MIN_TYPE_ROI:
            continue
        if _followable_capital_edge(metrics) < ESPORTS_LEADERBOARD_MIN_TYPE_CAPITAL_EDGE:
            continue
        followable_types.append(market_type)
    if not followable_types:
        return None
    if followable_types == eligible_market_types:
        return profile
    filtered = dict(profile)
    filtered["eligible_market_types"] = followable_types
    filtered["eligible_market_type_labels"] = [MARKET_TYPE_LABELS.get(value, value) for value in followable_types]
    if isinstance(per_type_grades, dict):
        filtered["per_type_grades"] = {key: value for key, value in per_type_grades.items() if key in followable_types}
    return filtered


def _with_sports_followable_market_types(profile: dict[str, Any]) -> dict[str, Any] | None:
    eligible_market_types = [str(value) for value in profile.get("eligible_market_types") or [] if value]
    if not eligible_market_types:
        if _followable_capital_edge(profile) < SPORTS_LEADERBOARD_MIN_CAPITAL_EDGE:
            return None
        followability = sports_flat_followability(profile)
        if not followability["flat_followable"]:
            return None
        return {**profile, **followability}

    per_type = profile.get("per_type") if isinstance(profile.get("per_type"), dict) else {}
    per_type_grades = profile.get("per_type_grades") if isinstance(profile.get("per_type_grades"), dict) else {}
    followable_types = []
    for market_type in eligible_market_types:
        metrics = dict(profile)
        if isinstance(per_type.get(market_type), dict):
            metrics.update(per_type[market_type])
        if isinstance(per_type_grades.get(market_type), dict):
            metrics.update(per_type_grades[market_type])
        if _followable_capital_edge(metrics) < SPORTS_LEADERBOARD_MIN_CAPITAL_EDGE:
            continue
        if not sports_flat_followability(metrics)["flat_followable"]:
            continue
        followable_types.append(market_type)
    if not followable_types:
        return None
    followability = {"flat_followable": True, "sports_follow_mode": "flat", "sports_flat_follow_reasons": []}
    if followable_types == eligible_market_types:
        return {**profile, **followability}
    filtered = dict(profile)
    filtered["eligible_market_types"] = followable_types
    filtered["eligible_market_type_labels"] = [MARKET_TYPE_LABELS.get(value, value) for value in followable_types]
    filtered.update(followability)
    if isinstance(per_type_grades, dict):
        filtered["per_type_grades"] = {key: value for key, value in per_type_grades.items() if key in followable_types}
    return filtered


def sports_flat_followability(metrics: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    closed_count = int(metrics.get("esports_closed_count") or 0)
    if closed_count < SPORTS_FLAT_FOLLOW_MIN_SAMPLE:
        reasons.append("flat_thin_sample")
    if to_float(metrics.get("positive_market_rate")) < SPORTS_FLAT_FOLLOW_MIN_POSITIVE_MARKET_RATE:
        reasons.append("flat_low_positive_rate")
    if to_float(metrics.get("wilson_win_rate_lower_bound")) < SPORTS_FLAT_FOLLOW_MIN_WILSON:
        reasons.append("flat_low_wilson")
    if to_float(metrics.get("median_market_roi")) < SPORTS_FLAT_FOLLOW_MIN_MEDIAN_MARKET_ROI:
        reasons.append("flat_low_median_market_roi")
    median_entry = to_float(metrics.get("median_entry_price"))
    if median_entry <= 0 or median_entry > SPORTS_FLAT_FOLLOW_MAX_MEDIAN_ENTRY:
        reasons.append("flat_high_entry_price")
    if _followable_capital_edge(metrics) < SPORTS_LEADERBOARD_MIN_CAPITAL_EDGE:
        reasons.append("flat_low_capital_edge")
    return {
        "flat_followable": not reasons,
        "sports_follow_mode": "flat" if not reasons else "watch",
        "sports_flat_follow_reasons": reasons,
    }


def _esports_followable_roi(metrics: dict[str, Any]) -> float:
    if metrics.get("esports_roi") is not None:
        return to_float(metrics.get("esports_roi"))
    return to_float(metrics.get("median_market_roi"))


def _followable_capital_edge(metrics: dict[str, Any]) -> float:
    if metrics.get("capital_weighted_edge") is not None:
        return to_float(metrics.get("capital_weighted_edge"))
    return to_float(metrics.get("entry_edge"))


def _clamp_float(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _candidate_type_metrics(row: dict[str, Any], market_type: str) -> dict[str, Any]:
    candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    per_type_candidate = (
        candidate.get("per_type_candidate")
        if isinstance(candidate.get("per_type_candidate"), dict)
        else {}
    )
    metrics = per_type_candidate.get(market_type)
    return metrics if isinstance(metrics, dict) else {}


def _candidate_bucket_metrics(row: dict[str, Any], bucket_key: str) -> dict[str, Any]:
    candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    game_family, market_type = split_bucket_key(bucket_key)
    per_game_type = (
        candidate.get("per_game_type_candidate")
        if isinstance(candidate.get("per_game_type_candidate"), dict)
        else {}
    )
    metrics = per_game_type.get(bucket_key)
    if isinstance(metrics, dict):
        return metrics
    per_game_family = (
        candidate.get("per_game_family_candidate")
        if isinstance(candidate.get("per_game_family_candidate"), dict)
        else {}
    )
    metrics = per_game_family.get(game_family)
    if isinstance(metrics, dict):
        return metrics
    return _candidate_type_metrics(row, market_type)


def esports_bucket_score(
    row: dict[str, Any],
    market_type: str,
    *,
    now_ts: int | None = None,
    bucket_key: str | None = None,
) -> dict[str, Any] | None:
    per_type = row.get("per_type") if isinstance(row.get("per_type"), dict) else {}
    per_type_grades = row.get("per_type_grades") if isinstance(row.get("per_type_grades"), dict) else {}
    per_game_type = row.get("per_game_type") if isinstance(row.get("per_game_type"), dict) else {}
    per_game_type_grades = row.get("per_game_type_grades") if isinstance(row.get("per_game_type_grades"), dict) else {}
    if bucket_key:
        if not isinstance(per_game_type.get(bucket_key), dict) and not isinstance(per_game_type_grades.get(bucket_key), dict):
            return None
    elif not isinstance(per_type.get(market_type), dict) and not isinstance(per_type_grades.get(market_type), dict):
        return None

    metrics = dict(row)
    if bucket_key and isinstance(per_game_type.get(bucket_key), dict):
        metrics.update(per_game_type[bucket_key])
    elif isinstance(per_type.get(market_type), dict):
        metrics.update(per_type[market_type])
    if bucket_key and isinstance(per_game_type_grades.get(bucket_key), dict):
        metrics.update(per_game_type_grades[bucket_key])
    elif isinstance(per_type_grades.get(market_type), dict):
        metrics.update(per_type_grades[market_type])
    candidate_metrics = _candidate_bucket_metrics(row, bucket_key) if bucket_key else _candidate_type_metrics(row, market_type)
    game_family = metrics.get("game_family")

    participated = int(candidate_metrics.get("participated_market_count") or 0)
    avg_market_cash = to_float(candidate_metrics.get("avg_market_cash") or candidate_metrics.get("avg_market_usd"))
    tail_entry_count = int(candidate_metrics.get("tail_entry_market_count") or 0)
    high_churn_count = int(candidate_metrics.get("high_churn_market_count") or 0)
    tail_rate = (tail_entry_count / participated) if participated > 0 else 0.0
    high_churn_rate = (high_churn_count / participated) if participated > 0 else 0.0
    closed_count = int(metrics.get("esports_closed_count") or 0)
    median_roi = to_float(metrics.get("median_market_roi"))
    realized_roi = to_float(metrics.get("esports_roi"))

    wilson = to_float(metrics.get("wilson_win_rate_lower_bound"))
    edge = _followable_capital_edge(metrics)
    positive_rate = to_float(metrics.get("positive_market_rate"))
    edge_norm = _clamp_float(edge / 0.25)
    realized_roi_norm = _clamp_float(realized_roi / 0.50)
    median_roi_norm = _clamp_float(median_roi / 0.50)
    sample_conf = _clamp_float(closed_count / 50)
    participation_norm = _clamp_float(participated / 30)
    avg_cash_norm = _clamp_float(avg_market_cash / 5_000)
    median_entry_price = to_float(metrics.get("median_entry_price"))
    price_safety_norm = _clamp_float((0.75 - median_entry_price) / 0.35) if median_entry_price > 0 else 0.0
    recency_norm = 0.0
    last_trade_at = int(metrics.get("last_esports_trade_at") or row.get("last_esports_trade_at") or 0)
    if now_ts is not None and last_trade_at > 0:
        recency_norm = _clamp_float(1 - max(0, int(now_ts) - last_trade_at) / (3 * 24 * 60 * 60))
    score = 100 * (
        0.30 * wilson
        + 0.12 * positive_rate
        + 0.05 * sample_conf
        + 0.13 * realized_roi_norm
        + 0.10 * median_roi_norm
        + 0.06 * participation_norm
        + 0.04 * recency_norm
        + 0.02 * avg_cash_norm
        + 0.05 * edge_norm
        + 0.03 * price_safety_norm
        - 0.10 * tail_rate
        - 0.05 * high_churn_rate
    )
    return {
        "score": round(score, 6),
        **({"bucket_key": bucket_key, "bucket_label": bucket_label(bucket_key)} if bucket_key else {}),
        **({"game_family": game_family, "game_family_label": metrics.get("game_family_label")} if game_family else {}),
        "market_type": market_type,
        "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
        "wilson_win_rate_lower_bound": metrics.get("wilson_win_rate_lower_bound"),
        "capital_weighted_edge": metrics.get("capital_weighted_edge"),
        "entry_edge": metrics.get("entry_edge"),
        "positive_market_rate": metrics.get("positive_market_rate"),
        "median_market_roi": metrics.get("median_market_roi"),
        "esports_roi": metrics.get("esports_roi"),
        "esports_win_count": metrics.get("esports_win_count"),
        "esports_loss_count": metrics.get("esports_loss_count"),
        "esports_closed_count": metrics.get("esports_closed_count"),
        "median_entry_price": metrics.get("median_entry_price"),
        "last_esports_trade_at": last_trade_at,
        "recent_bucket_market_count": metrics.get("recent_bucket_market_count"),
        "recent_bucket_window_days": metrics.get("recent_bucket_window_days"),
        "recent_bucket_roi": metrics.get("recent_bucket_roi"),
        "recent_bucket_positive_rate": metrics.get("recent_bucket_positive_rate"),
        "recent_bucket_pnl": metrics.get("recent_bucket_pnl"),
        "recent_7d_market_count": metrics.get("recent_7d_market_count"),
        "recent_7d_roi": metrics.get("recent_7d_roi"),
        "recent_7d_positive_rate": metrics.get("recent_7d_positive_rate"),
        "recent_14d_market_count": metrics.get("recent_14d_market_count"),
        "recent_14d_roi": metrics.get("recent_14d_roi"),
        "recent_14d_positive_rate": metrics.get("recent_14d_positive_rate"),
        "avg_market_cash": avg_market_cash,
        "participated_market_count": participated,
        "total_cash_volume": to_float(candidate_metrics.get("total_cash_volume")),
        "max_single_market_cash": to_float(candidate_metrics.get("max_single_market_cash")),
        "tail_entry_rate": round(tail_rate, 8),
        "high_churn_rate": round(high_churn_rate, 8),
    }


def enrich_esports_bucket_scores(row: dict[str, Any], *, now_ts: int | None = None) -> dict[str, Any]:
    eligible_market_types = [str(value) for value in row.get("eligible_market_types") or [] if value]
    eligible_buckets = [str(value) for value in row.get("eligible_buckets") or [] if value]
    if not eligible_market_types and not eligible_buckets:
        return row
    candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    qualified_buckets = [str(value) for value in candidate.get("qualified_buckets") or [] if value]
    qualified_market_types = [str(value) for value in candidate.get("qualified_market_types") or [] if value]
    qualified_game_families = [str(value) for value in candidate.get("qualified_game_families") or [] if value]
    if eligible_buckets:
        allowed_buckets = []
        for key in eligible_buckets:
            game_family, market_type = split_bucket_key(key)
            if qualified_buckets and key not in set(qualified_buckets):
                continue
            if not qualified_buckets:
                if qualified_market_types and market_type not in set(qualified_market_types):
                    continue
                if qualified_game_families and game_family not in set(qualified_game_families):
                    continue
            allowed_buckets.append(key)
        bucket_scores = {
            key: score
            for key in allowed_buckets
            if (score := esports_bucket_score(row, split_bucket_key(key)[1], now_ts=now_ts, bucket_key=key)) is not None
        }
    else:
        allowed_types = [
            market_type
            for market_type in eligible_market_types
            if not qualified_market_types or market_type in set(qualified_market_types)
        ]
        bucket_scores = {
            market_type: score
            for market_type in allowed_types
            if (score := esports_bucket_score(row, market_type, now_ts=now_ts)) is not None
        }
    if not bucket_scores:
        return row
    order = {"main_match": 0, "game_winner": 1, "map_winner": 2}
    best_bucket, best_score = max(
        bucket_scores.items(),
        key=lambda item: (
            to_float(item[1].get("score")),
            to_float(item[1].get("wilson_win_rate_lower_bound")),
            to_float(item[1].get("capital_weighted_edge") or item[1].get("entry_edge")),
            to_float(item[1].get("positive_market_rate")),
            int(item[1].get("esports_closed_count") or 0),
            -order.get(split_bucket_key(item[0])[1], 99),
        ),
    )
    best_game_family, best_market_type = split_bucket_key(best_bucket) if ":" in best_bucket else ("", best_bucket)
    return {
        **row,
        "overall_esports_roi": row.get("esports_roi"),
        "overall_wilson_win_rate_lower_bound": row.get("wilson_win_rate_lower_bound"),
        "overall_positive_market_rate": row.get("positive_market_rate"),
        "best_bucket": best_bucket,
        "best_bucket_label": bucket_label(best_bucket) if ":" in best_bucket else MARKET_TYPE_LABELS.get(best_bucket, best_bucket),
        "best_game_family": best_game_family,
        "best_market_type": best_market_type,
        "best_market_type_label": MARKET_TYPE_LABELS.get(best_market_type, best_market_type),
        "best_bucket_score": round(to_float(best_score.get("score")), 2),
        "best_bucket_last_trade_at": int(best_score.get("last_esports_trade_at") or row.get("last_esports_trade_at") or 0),
        "recent_bucket_market_count": best_score.get("recent_bucket_market_count"),
        "recent_bucket_window_days": best_score.get("recent_bucket_window_days"),
        "recent_bucket_roi": best_score.get("recent_bucket_roi"),
        "recent_bucket_positive_rate": best_score.get("recent_bucket_positive_rate"),
        "recent_bucket_pnl": best_score.get("recent_bucket_pnl"),
        "recent_7d_market_count": best_score.get("recent_7d_market_count"),
        "recent_7d_roi": best_score.get("recent_7d_roi"),
        "recent_7d_positive_rate": best_score.get("recent_7d_positive_rate"),
        "recent_14d_market_count": best_score.get("recent_14d_market_count"),
        "recent_14d_roi": best_score.get("recent_14d_roi"),
        "recent_14d_positive_rate": best_score.get("recent_14d_positive_rate"),
        "bucket_scores": bucket_scores,
    }


def leaderboard_rank_metrics(row: dict[str, Any]) -> dict[str, Any]:
    best_bucket = str(row.get("best_bucket") or "")
    best_market_type = str(row.get("best_market_type") or "")
    bucket_scores = row.get("bucket_scores") if isinstance(row.get("bucket_scores"), dict) else {}
    if best_bucket and isinstance(bucket_scores.get(best_bucket), dict):
        return bucket_scores[best_bucket]
    if best_market_type and isinstance(bucket_scores.get(best_market_type), dict):
        return bucket_scores[best_market_type]
    eligible_buckets = [str(value) for value in row.get("eligible_buckets") or [] if value]
    per_game_type_grades = row.get("per_game_type_grades") if isinstance(row.get("per_game_type_grades"), dict) else {}
    bucket_options = [
        per_game_type_grades[key]
        for key in eligible_buckets
        if isinstance(per_game_type_grades.get(key), dict)
    ]
    if bucket_options:
        return min(
            bucket_options,
            key=lambda metrics: (
                int(metrics.get("esports_loss_count") or 0) > 0,
                int(metrics.get("esports_loss_count") or 0),
                -to_float(metrics.get("positive_market_rate")),
                -to_float(metrics.get("wilson_win_rate_lower_bound")),
                -to_float(metrics.get("entry_edge")),
                -to_float(metrics.get("median_market_roi") or metrics.get("esports_roi")),
            ),
        )
    eligible_market_types = [str(value) for value in row.get("eligible_market_types") or [] if value]
    per_type_grades = row.get("per_type_grades") if isinstance(row.get("per_type_grades"), dict) else {}
    options = [
        per_type_grades[market_type]
        for market_type in eligible_market_types
        if isinstance(per_type_grades.get(market_type), dict)
    ]
    if not options:
        return row
    return min(
        options,
        key=lambda metrics: (
            int(metrics.get("esports_loss_count") or 0) > 0,
            int(metrics.get("esports_loss_count") or 0),
            -to_float(metrics.get("positive_market_rate")),
            -to_float(metrics.get("wilson_win_rate_lower_bound")),
            -to_float(metrics.get("entry_edge")),
            -to_float(metrics.get("median_market_roi") or metrics.get("esports_roi")),
        ),
    )


def leaderboard_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = leaderboard_rank_metrics(row)
    score = row.get("best_bucket_score")
    if score is not None:
        return (
            0 if row.get("grade") == "A" or row.get("eligible_market_types") else 1,
            -to_float(score),
            -to_float(metrics.get("wilson_win_rate_lower_bound")),
            -to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")),
            -to_float(metrics.get("positive_market_rate")),
            -int(metrics.get("esports_closed_count") or 0),
            normalize_wallet(row.get("wallet")),
        )
    loss_count = int(metrics.get("esports_loss_count") or 0)
    return (
        0 if row.get("grade") == "A" or row.get("eligible_market_types") else 1,
        loss_count > 0,
        loss_count,
        -to_float(metrics.get("positive_market_rate")),
        -to_float(metrics.get("wilson_win_rate_lower_bound")),
        -to_float(metrics.get("entry_edge")),
        -to_float(metrics.get("median_market_roi") or metrics.get("esports_roi")),
        -to_float(candidate_profile_priority(row.get("candidate") or {})[2]),
        normalize_wallet(row.get("wallet")),
    )


def _best_bucket_recent_performance_is_bad(row: dict[str, Any]) -> bool:
    market_count = int(row.get("recent_bucket_market_count") or 0)
    if market_count < ESPORTS_RECENT_BUCKET_MIN_MARKETS:
        return False
    return (
        to_float(row.get("recent_bucket_roi")) < 0
        or to_float(row.get("recent_bucket_positive_rate")) < ESPORTS_RECENT_BUCKET_MIN_POSITIVE_RATE
    )


def build_wallet_overlap_report(wallets: list[dict[str, Any]]) -> dict[str, Any]:
    market_sets: dict[str, set[str]] = {}
    for wallet in wallets:
        address = normalize_wallet(wallet.get("wallet"))
        condition_ids = wallet.get("esports_condition_ids") or (wallet.get("candidate") or {}).get("participated_market_ids") or []
        ids = {str(condition_id).lower() for condition_id in condition_ids if condition_id}
        if address and ids:
            market_sets[address] = ids

    union_ids = sorted(set().union(*market_sets.values())) if market_sets else []
    shared_all = sorted(set.intersection(*market_sets.values())) if market_sets else []
    pairs = []
    addresses = sorted(market_sets)
    for index, left in enumerate(addresses):
        for right in addresses[index + 1 :]:
            intersection = sorted(market_sets[left] & market_sets[right])
            if not intersection:
                continue
            pairs.append(
                {
                    "wallets": [left, right],
                    "shared_market_count": len(intersection),
                    "shared_market_ids": intersection,
                    "union_market_count": len(market_sets[left] | market_sets[right]),
                }
            )
    pairs.sort(key=lambda row: row["shared_market_count"], reverse=True)
    return {
        "wallet_count": len(market_sets),
        "union_market_count": len(union_ids),
        "union_market_ids": union_ids,
        "shared_by_all_count": len(shared_all),
        "shared_by_all_market_ids": shared_all,
        "pair_overlaps": pairs,
    }


def merge_cached_profile_with_candidate(cached: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {**cached, "candidate": candidate}


RECENT_BUCKET_METRIC_KEYS = (
    "last_esports_trade_at",
    "recent_7d_market_count",
    "recent_7d_roi",
    "recent_7d_positive_rate",
    "recent_7d_pnl",
    "recent_14d_market_count",
    "recent_14d_roi",
    "recent_14d_positive_rate",
    "recent_14d_pnl",
    "recent_bucket_market_count",
    "recent_bucket_window_days",
    "recent_bucket_roi",
    "recent_bucket_positive_rate",
    "recent_bucket_pnl",
)


def merge_recent_trade_metrics_into_profile(
    profile: dict[str, Any],
    raw_trades: list[dict],
    market_records_by_id: dict[str, dict[str, Any]],
    *,
    now_ts: int,
) -> dict[str, Any]:
    if not raw_trades or not market_records_by_id:
        return profile
    try:
        summary = summarize_trade_reconstructed_positions(
            raw_trades,
            market_records_by_id,
            now_ts=now_ts,
        )
    except Exception:
        return profile
    per_type_recent = summary.get("per_type") if isinstance(summary.get("per_type"), dict) else {}
    per_game_type_recent = summary.get("per_game_type") if isinstance(summary.get("per_game_type"), dict) else {}
    if not per_type_recent and not per_game_type_recent:
        return profile
    updated = dict(profile)
    per_type_grades = (
        dict(updated.get("per_type_grades"))
        if isinstance(updated.get("per_type_grades"), dict)
        else {}
    )
    for market_type, recent_metrics in per_type_recent.items():
        if not isinstance(recent_metrics, dict):
            continue
        existing = per_type_grades.get(market_type)
        if isinstance(existing, dict):
            per_type_grades[market_type] = {
                **existing,
                **{key: recent_metrics.get(key) for key in RECENT_BUCKET_METRIC_KEYS if key in recent_metrics},
            }
    if per_type_grades:
        updated["per_type_grades"] = per_type_grades
    per_game_type_grades = (
        dict(updated.get("per_game_type_grades"))
        if isinstance(updated.get("per_game_type_grades"), dict)
        else {}
    )
    for key, recent_metrics in per_game_type_recent.items():
        if not isinstance(recent_metrics, dict):
            continue
        existing = per_game_type_grades.get(key)
        if isinstance(existing, dict):
            per_game_type_grades[key] = {
                **existing,
                **{metric_key: recent_metrics.get(metric_key) for metric_key in RECENT_BUCKET_METRIC_KEYS if metric_key in recent_metrics},
            }
    if per_game_type_grades:
        updated["per_game_type_grades"] = per_game_type_grades
    if not isinstance(updated.get("per_type_grades"), dict):
        updated.update({key: summary.get(key) for key in RECENT_BUCKET_METRIC_KEYS if key in summary})
    if summary.get("last_esports_trade_at"):
        updated["last_esports_trade_at"] = max(
            int(updated.get("last_esports_trade_at") or 0),
            int(summary.get("last_esports_trade_at") or 0),
        )
    return updated


def prune_profile_store(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    max_age_days: int,
) -> dict[str, dict[str, Any]]:
    """Drop only low-value, current-version, long-inactive profiles.

    Keeps A/B (the asset), any stale-schema profile (migration still needs them),
    and failed_retryable (still owed a retry). Prunes only grade C/excluded that
    are current scoring version and have not traded esports within max_age_days.
    """
    if max_age_days <= 0:
        return profiles_by_wallet
    cutoff = now_ts - max_age_days * 86400
    kept: dict[str, dict[str, Any]] = {}
    for wallet, profile in profiles_by_wallet.items():
        current_version = int(profile.get("scoring_version") or 0) == SCORING_VERSION
        prunable = (
            current_version
            and profile.get("grade") in {"C", "excluded"}
            and profile.get("profile_state") != "failed_retryable"
            and int(profile.get("last_esports_trade_at") or 0) < cutoff
        )
        if not prunable:
            kept[wallet] = profile
    return kept


def merge_profiles_with_candidates(
    profiles_by_wallet: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {normalize_wallet(wallet): dict(profile) for wallet, profile in profiles_by_wallet.items()}
    for candidate in candidates:
        wallet = normalize_wallet(candidate.get("wallet"))
        if wallet and wallet in merged:
            merged[wallet] = merge_cached_profile_with_candidate(merged[wallet], candidate)
    return merged


def profile_needs_schema_migration(profile: dict[str, Any] | None) -> bool:
    if not profile:
        return False
    if int(profile.get("scoring_version") or 0) != SCORING_VERSION:
        return True
    if not isinstance(profile.get("esports_condition_ids"), list):
        return True
    if profile.get("grade") in {"A", "B"} and "entry_edge" not in profile:
        return True
    return False


def candidate_profile_priority(candidate: dict[str, Any]) -> tuple[Any, ...]:
    qualified = [str(value) for value in candidate.get("qualified_market_types") or [] if value]
    per_type = candidate.get("per_type_candidate") if isinstance(candidate.get("per_type_candidate"), dict) else {}
    qualified_metrics = [per_type.get(market_type) for market_type in qualified if isinstance(per_type.get(market_type), dict)]
    if not qualified_metrics:
        return (
            0,
            to_float(candidate.get("avg_market_cash") or candidate.get("avg_market_usd")),
            to_float(candidate.get("total_cash_volume") or candidate.get("total_holder_usd")),
            int(candidate.get("participated_market_count") or 0),
        )
    return (
        len(qualified),
        max(to_float(metrics.get("avg_market_cash") or metrics.get("avg_market_usd")) for metrics in qualified_metrics),
        sum(to_float(metrics.get("total_cash_volume") or metrics.get("total_holder_usd")) for metrics in qualified_metrics),
        sum(int(metrics.get("participated_market_count") or 0) for metrics in qualified_metrics),
    )


def build_profile_fetch_plan(
    profile_candidates: list[dict[str, Any]],
    existing_profiles: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    ttl_seconds: int,
    max_profiles: int,
) -> list[dict[str, Any]]:
    if max_profiles <= 0:
        return []

    candidate_items: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    ordered_profile_candidates = list(profile_candidates)
    if any(candidate.get("qualified_market_types") for candidate in ordered_profile_candidates):
        ordered_profile_candidates = sorted(
            enumerate(ordered_profile_candidates),
            key=lambda item: (*candidate_profile_priority(item[1]), -item[0]),
            reverse=True,
        )
        ordered_profile_candidates = [candidate for _index, candidate in ordered_profile_candidates]
    for candidate in ordered_profile_candidates:
        wallet = normalize_wallet(candidate.get("wallet"))
        if not wallet or wallet in seen_candidates:
            continue
        seen_candidates.add(wallet)
        normalized = {**candidate, "wallet": wallet}
        cached = existing_profiles.get(wallet)
        if (
            not should_use_cached_profile(cached, now_ts=now_ts, ttl_seconds=ttl_seconds)
            or profile_needs_schema_migration(cached)
        ):
            candidate_items.append(normalized)

    migration_items: list[dict[str, Any]] = []
    seen_migration: set[str] = set()
    for wallet, profile in existing_profiles.items():
        wallet = normalize_wallet(wallet or profile.get("wallet"))
        if not wallet or wallet in seen_candidates or wallet in seen_migration:
            continue
        if not profile_needs_schema_migration(profile):
            continue
        seen_migration.add(wallet)
        candidate = dict(profile.get("candidate") or {})
        candidate["wallet"] = wallet
        migration_items.append(candidate)

    candidate_budget = min(len(candidate_items), math.ceil(max_profiles * 0.7))
    selected_candidates = candidate_items[:candidate_budget]
    selected_wallets = {row["wallet"] for row in selected_candidates}

    remaining = max_profiles - len(selected_candidates)
    selected_migrations = [row for row in migration_items if row["wallet"] not in selected_wallets][:remaining]
    selected_wallets.update(row["wallet"] for row in selected_migrations)

    remaining = max_profiles - len(selected_candidates) - len(selected_migrations)
    if remaining > 0:
        selected_candidates.extend(
            row for row in candidate_items[candidate_budget:] if row["wallet"] not in selected_wallets
        )
        selected_candidates = selected_candidates[: candidate_budget + remaining]

    return [*selected_candidates, *selected_migrations]


def build_profile_budget_summary(
    *,
    profile_candidate_wallet_count: int,
    profile_fetch_plan_count: int,
    max_profiles_per_run_effective: int,
) -> dict[str, int]:
    return {
        "profile_fetch_plan_count": int(profile_fetch_plan_count),
        "unprofiled_profile_candidate_count": max(
            0,
            int(profile_candidate_wallet_count) - int(profile_fetch_plan_count),
        ),
        "max_profiles_per_run_effective": int(max_profiles_per_run_effective),
    }


def _increment_count(counter: dict[str, int], key: str, amount: int = 1) -> None:
    counter[key] = counter.get(key, 0) + amount


def _sorted_count_dict(counter: dict[str, int]) -> dict[str, int]:
    return {
        key: counter[key]
        for key in sorted(counter, key=lambda value: (-counter[value], value))
    }


def _market_record_volume(row: dict[str, Any]) -> float:
    for key in ("volume", "volume_num", "volumeNum", "volume_24hr", "volume24hr"):
        if row.get(key) is not None:
            return to_float(row.get(key))
    return 0.0


def _median_float(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _market_record_days_ago(row: dict[str, Any], *, now_ts: int) -> float | None:
    end = parse_dt(row.get("end_date"))
    if not end:
        return None
    return max(0.0, (now_ts - int(end.timestamp())) / 86400)


def _leaderboard_reject_reasons_for_profile(
    profile: dict[str, Any],
    *,
    now_ts: int,
    max_inactive_days: int = ESPORTS_DEFAULT_LEADERBOARD_MAX_INACTIVE_DAYS,
    max_tail_entry_rate: float = 0.34,
    max_two_sided_market_count: int = 0,
    max_two_sided_market_rate: float = 0.0,
) -> list[str]:
    reasons: list[str] = []
    eligible_market_types = [str(value) for value in profile.get("eligible_market_types") or [] if value]
    if int(profile.get("scoring_version") or 0) != SCORING_VERSION:
        reasons.append("old_scoring_version")
    if profile.get("per_type_grades") is not None and not eligible_market_types:
        reasons.append("no_eligible_per_type")
    if profile.get("grade") != "A" and not eligible_market_types:
        reasons.append("not_A_no_eligible_type")
    if not eligible_market_types and to_float(profile.get("esports_roi")) < 0.30:
        reasons.append("legacy_low_roi")
    if (
        not eligible_market_types
        and "positive_market_rate" in profile
        and to_float(profile.get("positive_market_rate")) < MIN_A_POSITIVE_MARKET_RATE
    ):
        reasons.append("legacy_low_positive_rate")
    if to_float(profile.get("actual_minus_hold_pnl_rate")) > SWING_DEPENDENT_RATE:
        reasons.append("swing_dependent")
    behavior_market_count = int(profile.get("historical_trade_behavior_market_count") or 0)
    two_sided_trade_rate = to_float(profile.get("two_sided_trade_market_rate"))
    if (
        int(profile.get("two_sided_trade_market_count") or 0) > 0
        and behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS
        and two_sided_trade_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
    ):
        reasons.append("systemic_two_sided_profile")
    followable_profile = _with_esports_followable_market_types(dict(profile))
    if followable_profile is None:
        reasons.append("type_roi_or_edge_gate")
        return reasons

    followable_profile = enrich_esports_bucket_scores(followable_profile, now_ts=now_ts)
    best_bucket_last_trade = int(
        followable_profile.get("best_bucket_last_trade_at") or followable_profile.get("last_esports_trade_at") or 0
    )
    if not best_bucket_last_trade or now_ts - best_bucket_last_trade > max_inactive_days * 86400:
        reasons.append("best_bucket_inactive_gt3d")
    if _best_bucket_recent_performance_is_bad(followable_profile):
        reasons.append("recent_bucket_bad_performance")

    followable_types = [str(value) for value in followable_profile.get("eligible_market_types") or [] if value]
    followable_buckets = [str(value) for value in followable_profile.get("eligible_buckets") or [] if value]
    candidate = followable_profile.get("candidate") if isinstance(followable_profile.get("candidate"), dict) else {}
    qualified_market_types = [str(value) for value in candidate.get("qualified_market_types") or [] if value]
    qualified_buckets = [str(value) for value in candidate.get("qualified_buckets") or [] if value]
    per_game_type = (
        candidate.get("per_game_type_candidate")
        if isinstance(candidate.get("per_game_type_candidate"), dict)
        else {}
    )
    if followable_buckets and (qualified_buckets or per_game_type):
        behavior_buckets = [
            key
            for key in followable_buckets
            if (not qualified_buckets or key in set(qualified_buckets)) and isinstance(per_game_type.get(key), dict)
        ]
        if not behavior_buckets:
            reasons.append("candidate_behavior_gate")
            return reasons
        behavior_ok = False
        for key in behavior_buckets:
            metrics = per_game_type.get(key)
            participated = int(metrics.get("participated_market_count") or 0)
            if not candidate_two_sided_within_limits(
                metrics,
                max_count=max_two_sided_market_count,
                max_rate=max_two_sided_market_rate,
            ):
                continue
            if "tail_entry_market_count" not in metrics:
                continue
            if participated > 0 and int(metrics.get("tail_entry_market_count") or 0) / participated > max_tail_entry_rate:
                continue
            behavior_ok = True
            break
        if not behavior_ok:
            reasons.append("candidate_behavior_gate")
        return reasons
    if qualified_market_types and followable_types:
        behavior_types = [market_type for market_type in followable_types if market_type in set(qualified_market_types)]
        if not behavior_types:
            reasons.append("eligible_not_qualified")
            return reasons
        per_type = candidate.get("per_type_candidate") if isinstance(candidate.get("per_type_candidate"), dict) else {}
        behavior_ok = False
        for market_type in behavior_types:
            metrics = per_type.get(market_type)
            if not isinstance(metrics, dict):
                continue
            participated = int(metrics.get("participated_market_count") or 0)
            if not candidate_two_sided_within_limits(
                metrics,
                max_count=max_two_sided_market_count,
                max_rate=max_two_sided_market_rate,
            ):
                continue
            if "tail_entry_market_count" not in metrics:
                continue
            if participated > 0 and int(metrics.get("tail_entry_market_count") or 0) / participated > max_tail_entry_rate:
                continue
            behavior_ok = True
            break
        if not behavior_ok:
            reasons.append("candidate_behavior_gate")
    return reasons


def build_collection_diagnostics(
    *,
    discovery_slate: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    profile_candidates: list[dict[str, Any]],
    profiles_by_wallet: dict[str, dict[str, Any]],
    leaderboard: list[dict[str, Any]],
    now_ts: int,
    stage_timings: dict[str, float] | None = None,
) -> dict[str, Any]:
    market_types = [MAIN_MATCH, GAME_WINNER, MAP_WINNER]
    market_type_slate: dict[str, dict[str, Any]] = {}
    for market_type in market_types:
        rows = [row for row in discovery_slate if str(row.get("market_type") or MAIN_MATCH) == market_type]
        volumes = sorted((_market_record_volume(row) for row in rows), reverse=True)
        days_ago_values = [
            days_ago
            for row in rows
            if (days_ago := _market_record_days_ago(row, now_ts=now_ts)) is not None
        ]
        market_type_slate[market_type] = {
            "market_count": len(rows),
            "max_volume": round(volumes[0], 6) if volumes else 0.0,
            "min_volume": round(volumes[-1], 6) if volumes else 0.0,
            "median_volume": round(_median_float(volumes), 6),
            "max_days_ago": round(max(days_ago_values), 6) if days_ago_values else 0.0,
            "min_days_ago": round(min(days_ago_values), 6) if days_ago_values else 0.0,
            "median_days_ago": round(_median_float(days_ago_values), 6),
            "sort_mode": "volume_recency_score_70_30",
        }

    candidate_funnel: dict[str, dict[str, Any]] = {}
    for market_type in market_types:
        candidate_rows = [
            row
            for row in candidates
            if isinstance((row.get("per_type_candidate") or {}).get(market_type), dict)
        ]
        profile_rows = [
            row
            for row in profile_candidates
            if isinstance((row.get("per_type_candidate") or {}).get(market_type), dict)
        ]
        qualified_rows = [
            row for row in profile_candidates if market_type in [str(value) for value in row.get("qualified_market_types") or []]
        ]
        candidate_funnel[market_type] = {
            "candidate_wallets": len(candidate_rows),
            "profile_candidate_wallets": len(profile_rows),
            "qualified_profile_candidates": len(qualified_rows),
        }

    profile_grade_counts: dict[str, int] = {}
    eligible_market_type_counts: dict[str, int] = {}
    reject_reasons: dict[str, int] = {}
    leaderboard_wallets = {normalize_wallet(row.get("wallet")) for row in leaderboard}
    for wallet, profile in profiles_by_wallet.items():
        _increment_count(profile_grade_counts, str(profile.get("grade") or "unknown"))
        for market_type in profile.get("eligible_market_types") or []:
            _increment_count(eligible_market_type_counts, str(market_type))
        normalized_wallet = normalize_wallet(wallet or profile.get("wallet"))
        if normalized_wallet in leaderboard_wallets:
            continue
        for reason in set(_leaderboard_reject_reasons_for_profile(profile, now_ts=now_ts)):
            _increment_count(reject_reasons, reason)

    leaderboard_best_counts: dict[str, int] = {}
    for row in leaderboard:
        market_type = str(row.get("best_market_type") or "unknown")
        _increment_count(leaderboard_best_counts, market_type)

    return {
        "market_type_slate": market_type_slate,
        "candidate_funnel": candidate_funnel,
        "profile_grade_counts": _sorted_count_dict(profile_grade_counts),
        "eligible_market_type_counts": _sorted_count_dict(eligible_market_type_counts),
        "leaderboard_best_market_type_counts": _sorted_count_dict(leaderboard_best_counts),
        "leaderboard_reject_reasons": _sorted_count_dict(reject_reasons),
        "stage_timings": {
            key: round(float(value), 6)
            for key, value in sorted((stage_timings or {}).items())
        },
    }


def select_v2_target_markets(
    classification_set: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_days: int = 30,
    bucket_market_limit: int = 50,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    cutoff_ts = int(now.timestamp()) - max(0, int(lookback_days)) * 86400
    bucket_counts: dict[str, int] = {}
    bucket_shortfalls: dict[str, int] = {}
    selected: list[dict[str, Any]] = []
    for game_family, market_type in COLLECTOR_V2_BUCKETS:
        key = bucket_key(game_family, market_type)
        bucket_rows = []
        for market in classification_set:
            if str(market.get("category") or "").lower() != "esports":
                continue
            if str(market.get("game_family") or "").lower() != game_family:
                continue
            if str(market.get("market_type") or MAIN_MATCH) != market_type:
                continue
            end_dt = parse_dt(market.get("end_date"))
            if not end_dt or int(end_dt.timestamp()) < cutoff_ts or end_dt > now:
                continue
            if winning_outcome_index(market) is None:
                continue
            bucket_rows.append({**market, "bucket_key": key, "bucket_label": bucket_label(key)})
        bucket_rows.sort(
            key=lambda row: (
                to_float(row.get("volume")),
                parse_dt(row.get("end_date")).timestamp() if parse_dt(row.get("end_date")) else 0.0,
                str(row.get("condition_id") or ""),
            ),
            reverse=True,
        )
        slice_rows = bucket_rows[: max(0, int(bucket_market_limit))]
        selected.extend(slice_rows)
        bucket_counts[key] = len(slice_rows)
        shortfall = max(0, int(bucket_market_limit) - len(slice_rows))
        if shortfall:
            bucket_shortfalls[key] = shortfall
    return selected, {
        "lookback_days": int(lookback_days),
        "bucket_market_limit": int(bucket_market_limit),
        "bucket_counts": bucket_counts,
        "bucket_shortfalls": bucket_shortfalls,
        "target_market_count": len(selected),
        "target_market_capacity": int(bucket_market_limit) * len(COLLECTOR_V2_BUCKETS),
    }


def collect_v2_seed_positions(
    market: dict[str, Any],
    market_positions_response: list[dict[str, Any]],
    *,
    positions_per_market: int = 20,
) -> list[dict[str, Any]]:
    winner_index = winning_outcome_index(market)
    if winner_index is None:
        return []
    winning_positions: list[dict[str, Any]] = []
    for token_index, token_block in enumerate(market_positions_response or []):
        for position in token_block.get("positions") or []:
            outcome_index = to_int(position.get("outcomeIndex"), token_index)
            if outcome_index != winner_index:
                continue
            wallet = normalize_wallet(position.get("proxyWallet") or position.get("wallet"))
            if not wallet:
                continue
            total_bought = to_float(position.get("totalBought"))
            avg_price = to_float(position.get("avgPrice"))
            seed_pnl = to_float(position.get("totalPnl"))
            if seed_pnl == 0:
                seed_pnl = to_float(position.get("realizedPnl"))
            seed_cost = total_bought * avg_price
            if seed_pnl <= 0 or total_bought <= 0 or avg_price <= 0 or seed_cost <= 0:
                continue
            winning_positions.append(
                {
                    "wallet": wallet,
                    "condition_id": str(market.get("condition_id") or "").lower(),
                    "question": market.get("question") or market.get("title") or "",
                    "game_family": str(market.get("game_family") or "").lower(),
                    "market_type": str(market.get("market_type") or MAIN_MATCH),
                    "bucket_key": str(market.get("bucket_key") or bucket_key(market.get("game_family"), market.get("market_type"))),
                    "outcome_index": outcome_index,
                    "outcome": position.get("outcome"),
                    "avg_price": round(avg_price, 8),
                    "total_bought": round(total_bought, 8),
                    "seed_cost": round(seed_cost, 8),
                    "seed_pnl": round(seed_pnl, 8),
                    "seed_roi": round(seed_pnl / seed_cost, 8),
                    "seed_edge": round(max(0.0, 1.0 - avg_price), 8),
                    "market_volume": to_float(market.get("volume")),
                    "timestamp": int(parse_dt(market.get("end_date")).timestamp()) if parse_dt(market.get("end_date")) else 0,
                }
            )
    rows = winning_positions[: max(0, int(positions_per_market))]
    for rank, row in enumerate(rows, start=1):
        row["seed_rank"] = rank
    return rows


def aggregate_v2_seed_wallets(seed_positions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in seed_positions:
        wallet = normalize_wallet(row.get("wallet"))
        if wallet:
            grouped.setdefault(wallet, []).append({**row, "wallet": wallet})
    wallets: dict[str, dict[str, Any]] = {}
    for wallet, raw_rows in grouped.items():
        deduped_by_market: dict[str, dict[str, Any]] = {}
        for row in raw_rows:
            condition_id = str(row.get("condition_id") or "").lower()
            if not condition_id:
                continue
            existing = deduped_by_market.get(condition_id)
            if existing is None:
                deduped_by_market[condition_id] = row
                continue
            current_key = (
                -to_int(row.get("seed_rank"), 999999),
                to_float(row.get("seed_pnl")),
                to_float(row.get("seed_cost")),
                to_int(row.get("timestamp")),
            )
            existing_key = (
                -to_int(existing.get("seed_rank"), 999999),
                to_float(existing.get("seed_pnl")),
                to_float(existing.get("seed_cost")),
                to_int(existing.get("timestamp")),
            )
            if current_key > existing_key:
                deduped_by_market[condition_id] = row
        rows = list(deduped_by_market.values())
        condition_ids = {str(row.get("condition_id") or "").lower() for row in rows if row.get("condition_id")}
        bucket_counts: dict[str, int] = {}
        game_family_counts: dict[str, int] = {}
        market_type_counts: dict[str, int] = {}
        for row in rows:
            _increment_count(bucket_counts, str(row.get("bucket_key") or "unknown"))
            _increment_count(game_family_counts, str(row.get("game_family") or "unknown"))
            _increment_count(market_type_counts, str(row.get("market_type") or MAIN_MATCH))
        seed_cost_total = sum(to_float(row.get("seed_cost")) for row in rows)
        seed_pnl_total = sum(to_float(row.get("seed_pnl")) for row in rows)
        avg_prices = [to_float(row.get("avg_price")) for row in rows if to_float(row.get("avg_price")) > 0]
        ranks = [to_float(row.get("seed_rank")) for row in rows if to_float(row.get("seed_rank")) > 0]
        wallet_row = {
            "wallet": wallet,
            "seed_position_row_count": len(raw_rows),
            "seed_win_count": len(condition_ids),
            "seed_market_count": len(condition_ids),
            "seed_condition_ids": sorted(condition_ids),
            "seed_bucket_count": len(bucket_counts),
            "seed_bucket_counts": _sorted_count_dict(bucket_counts),
            "seed_game_family_counts": _sorted_count_dict(game_family_counts),
            "seed_market_type_counts": _sorted_count_dict(market_type_counts),
            "seed_pnl_total": round(seed_pnl_total, 8),
            "seed_cost_total": round(seed_cost_total, 8),
            "seed_weighted_roi": round(seed_pnl_total / seed_cost_total, 8) if seed_cost_total > 0 else 0.0,
            "median_avg_price": round(_median_float(avg_prices), 8),
            "avg_seed_rank": round(sum(ranks) / len(ranks), 8) if ranks else 0.0,
            "last_seed_at": max((to_int(row.get("timestamp")) for row in rows), default=0),
            "seed_positions": sorted(
                rows,
                key=lambda row: (
                    -to_int(row.get("timestamp")),
                    str(row.get("condition_id") or ""),
                    to_int(row.get("seed_rank")),
                ),
            ),
        }
        wallet_row["seed_score"] = round(seed_score_v2_wallet(wallet_row), 8)
        wallet_row["candidate"] = {
            "wallet": wallet,
            "participated_market_count": wallet_row["seed_market_count"],
            "participated_market_ids": wallet_row["seed_condition_ids"],
            "total_cash_volume": wallet_row["seed_cost_total"],
            "max_single_market_cash": round(max((to_float(row.get("seed_cost")) for row in rows), default=0.0), 8),
            "avg_market_cash": round(seed_cost_total / len(condition_ids), 8) if condition_ids else 0.0,
            "seed_tail_entry_market_count": sum(1 for price in avg_prices if price > 0.85),
            "candidate_reasons": ["v2_profitable_winner_seed"],
            "source": "collector_v2_market_positions",
        }
        wallets[wallet] = wallet_row
    return wallets


def seed_score_v2_wallet(row: dict[str, Any]) -> float:
    frequency_score = min(to_int(row.get("seed_win_count")), 10) * 4.0
    pnl_score = min(math.log10(max(to_float(row.get("seed_pnl_total")), 0.0) + 1.0), 4.0) * 1.5
    roi_score = min(max(to_float(row.get("seed_weighted_roi")), 0.0), 1.0) * 4.0
    median_avg = to_float(row.get("median_avg_price"))
    entry_edge_score = min(max(1.0 - median_avg, 0.0), 0.7) * 5.0
    cross_bucket_score = min(to_int(row.get("seed_bucket_count")), 4) * 1.0
    avg_rank = to_float(row.get("avg_seed_rank"))
    rank_score = max(0.0, (21.0 - avg_rank) / 20.0) * 2.0 if avg_rank > 0 else 0.0
    return frequency_score + pnl_score + roi_score + entry_edge_score + cross_bucket_score + rank_score


def filter_v2_profile_wallets(
    seed_wallets: dict[str, dict[str, Any]],
    *,
    max_wallets: int = 500,
    min_seed_win_count: int = 2,
    min_seed_cost_total: float = 250,
    max_median_avg_price: float = 0.85,
) -> list[dict[str, Any]]:
    rows = []
    for row in seed_wallets.values():
        if to_int(row.get("seed_win_count")) < min_seed_win_count:
            continue
        if to_float(row.get("seed_cost_total")) < min_seed_cost_total:
            continue
        if to_float(row.get("seed_weighted_roi")) <= 0:
            continue
        if to_float(row.get("median_avg_price")) > max_median_avg_price:
            continue
        updated = dict(row)
        updated["seed_score"] = round(seed_score_v2_wallet(updated), 8)
        rows.append(updated)
    rows.sort(
        key=lambda row: (
            to_float(row.get("seed_score")),
            to_int(row.get("seed_win_count")),
            to_float(row.get("seed_pnl_total")),
            -to_float(row.get("median_avg_price")),
            normalize_wallet(row.get("wallet")),
        ),
        reverse=True,
    )
    return rows[: max(0, int(max_wallets))]


def v2_seed_filter_reject_reasons(
    seed_wallets: dict[str, dict[str, Any]],
    profile_wallets: list[dict[str, Any]],
    *,
    min_seed_win_count: int = 2,
    min_seed_cost_total: float = 250,
    max_median_avg_price: float = 0.85,
) -> dict[str, int]:
    selected_wallets = {normalize_wallet(row.get("wallet")) for row in profile_wallets}
    counts: dict[str, int] = {}
    for row in seed_wallets.values():
        wallet = normalize_wallet(row.get("wallet"))
        reasons: list[str] = []
        if to_int(row.get("seed_win_count")) < min_seed_win_count:
            reasons.append("seed_win_count_lt_min")
        if to_float(row.get("seed_cost_total")) < min_seed_cost_total:
            reasons.append("seed_cost_total_lt_min")
        if to_float(row.get("seed_weighted_roi")) <= 0:
            reasons.append("seed_weighted_roi_not_positive")
        if to_float(row.get("median_avg_price")) > max_median_avg_price:
            reasons.append("median_avg_price_gt_max")
        if not reasons and wallet not in selected_wallets:
            reasons.append("profile_wallet_cap")
        for reason in set(reasons):
            _increment_count(counts, reason)
    return _sorted_count_dict(counts)


def v2_bucket_wallet_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        bucket_counts = row.get("seed_bucket_counts") if isinstance(row.get("seed_bucket_counts"), dict) else {}
        for bucket, count in bucket_counts.items():
            if to_int(count) > 0:
                _increment_count(counts, str(bucket))
    return _sorted_count_dict(counts)


def build_v2_collection_diagnostics(
    *,
    seed_wallets: dict[str, dict[str, Any]],
    profile_wallets: list[dict[str, Any]],
    profiles_by_wallet: dict[str, dict[str, Any]],
    leaderboard: list[dict[str, Any]],
    now_ts: int,
) -> dict[str, Any]:
    profile_grade_counts: dict[str, int] = {}
    eligible_bucket_counts: dict[str, int] = {}
    eligible_market_type_counts: dict[str, int] = {}
    leaderboard_wallets = {normalize_wallet(row.get("wallet")) for row in leaderboard}
    leaderboard_reject_reasons: dict[str, int] = {}
    for wallet, profile in profiles_by_wallet.items():
        _increment_count(profile_grade_counts, str(profile.get("grade") or "unknown"))
        for bucket in profile.get("eligible_buckets") or []:
            _increment_count(eligible_bucket_counts, str(bucket))
        for market_type in profile.get("eligible_market_types") or []:
            _increment_count(eligible_market_type_counts, str(market_type))
        if normalize_wallet(wallet or profile.get("wallet")) in leaderboard_wallets:
            continue
        reasons = set(_leaderboard_reject_reasons_for_profile(profile, now_ts=now_ts))
        if not reasons and not v2_strict_final_quality_ok(profile):
            reasons.add("v2_strict_quality_gate")
        for reason in reasons:
            _increment_count(leaderboard_reject_reasons, reason)
    return {
        "seed_filter_reject_reasons": v2_seed_filter_reject_reasons(seed_wallets, profile_wallets),
        "bucket_seed_wallet_counts": v2_bucket_wallet_counts(list(seed_wallets.values())),
        "bucket_profile_wallet_counts": v2_bucket_wallet_counts(profile_wallets),
        "profile_grade_counts": _sorted_count_dict(profile_grade_counts),
        "eligible_bucket_counts": _sorted_count_dict(eligible_bucket_counts),
        "eligible_market_type_counts": _sorted_count_dict(eligible_market_type_counts),
        "leaderboard_reject_reasons": _sorted_count_dict(leaderboard_reject_reasons),
    }


def _record_timestamp(record: dict[str, Any], *keys: str) -> int:
    for key in keys:
        dt = parse_dt(record.get(key))
        if dt:
            return int(dt.timestamp())
    return 0


def resolve_collect_v2_output_dir(args: argparse.Namespace) -> Path:
    return resolve_collect_experimental_output_dir(args, collector_version="v2")


def resolve_collect_v3_output_dir(args: argparse.Namespace) -> Path:
    return resolve_collect_experimental_output_dir(args, collector_version="v3")


def resolve_collect_experimental_output_dir(args: argparse.Namespace, *, collector_version: str) -> Path:
    explicit = getattr(args, "output_dir", None)
    if explicit:
        return Path(explicit)
    if collector_version == "v3":
        return resolve_data_dir(args)
    root = Path(getattr(args, "data_dir", None) or DATA_DIR)
    return root / f"collector_{collector_version}" / "esports"


def resolve_collect_experimental_profile_wallet_limit(
    args: argparse.Namespace,
    *,
    collector_version: str,
) -> int:
    explicit = getattr(args, "max_profile_wallets", None)
    if explicit is None:
        explicit = getattr(args, "max_profiles_per_run", None)
    if explicit is None:
        explicit = 700 if collector_version == "v3" else 500
    return int(explicit or 0)


def build_v2_profile_candidate_from_trades(
    seed_candidate: dict[str, Any],
    raw_trades: list[dict[str, Any]],
    market_records_by_id: dict[str, dict[str, Any]],
    *,
    tail_entry_price_threshold: float = 0.75,
) -> dict[str, Any]:
    wallet = normalize_wallet(seed_candidate.get("wallet"))
    markets = {str(key).lower(): value for key, value in market_records_by_id.items()}
    trades_by_market: dict[str, list[dict[str, Any]]] = {}
    for trade in raw_trades or []:
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
        if not condition_id or condition_id not in markets:
            continue
        normalized = dict(trade)
        if wallet and not normalize_wallet(normalized.get("proxyWallet") or normalized.get("wallet")):
            normalized["proxyWallet"] = wallet
        if "outcome" not in normalized and (
            normalized.get("outcomeIndex") is not None or normalized.get("outcome_index") is not None
        ):
            normalized["outcome"] = str(normalized.get("outcomeIndex", normalized.get("outcome_index")))
        trades_by_market.setdefault(condition_id, []).append(normalized)
    if not trades_by_market:
        return {
            **seed_candidate,
            "wallet": wallet,
            "candidate_reasons": sorted(set(seed_candidate.get("candidate_reasons") or []) | {"v2_profitable_winner_seed"}),
        }
    market_type_by_id = {
        condition_id: str(record.get("market_type") or MAIN_MATCH)
        for condition_id, record in markets.items()
    }
    market_game_family_by_id = {
        condition_id: str(record.get("game_family") or "")
        for condition_id, record in markets.items()
        if record.get("game_family")
    }
    market_end_times = {
        condition_id: _record_timestamp(record, "end_date", "endDate")
        for condition_id, record in markets.items()
    }
    market_start_times = {
        condition_id: _record_timestamp(record, "match_start_time", "market_start_time", "eventStartTime", "startTime", "gameStartTime")
        for condition_id, record in markets.items()
    }
    candidates = build_candidate_wallets(
        trades_by_market,
        market_type_by_id=market_type_by_id,
        market_game_family_by_id=market_game_family_by_id,
        market_end_times=market_end_times,
        market_start_times=market_start_times,
        min_trade_cash=0,
        participation_threshold=1,
        top_participation_count=1,
        total_cash_threshold=0,
        single_market_cash_threshold=0,
        max_candidate_wallets=10,
        tail_entry_price_threshold=tail_entry_price_threshold,
    )
    deep_candidate = next((row for row in candidates if normalize_wallet(row.get("wallet")) == wallet), None)
    if not deep_candidate:
        return {
            **seed_candidate,
            "wallet": wallet,
            "candidate_reasons": sorted(set(seed_candidate.get("candidate_reasons") or []) | {"v2_profitable_winner_seed"}),
        }
    merged = {**seed_candidate, **{key: value for key, value in deep_candidate.items() if key != "candidate_reasons"}}
    merged["wallet"] = wallet
    merged["candidate_reasons"] = sorted(
        set(seed_candidate.get("candidate_reasons") or []) | {"v2_profitable_winner_seed", "v2_deep_trade_behavior"}
    )
    merged["source"] = "collector_v2_market_positions+deep_user_trades"
    return merged


def build_v2_leaderboard(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    max_leaderboard_wallets: int = 30,
    require_strict_quality_gate: bool = True,
    max_two_sided_market_count: int = 0,
    max_two_sided_market_rate: float = 0.0,
) -> list[dict[str, Any]]:
    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        max_inactive_days=30,
        min_participated_markets=2,
        min_avg_market_cash=250,
        require_tail_entry_field=True,
        require_current_scoring_version=True,
        max_leaderboard_wallets=0 if require_strict_quality_gate else max_leaderboard_wallets,
        max_two_sided_market_count=max_two_sided_market_count,
        max_two_sided_market_rate=max_two_sided_market_rate,
    )
    rows = [{**row, "collector_version": "v2"} for row in leaderboard]
    if require_strict_quality_gate:
        rows = [row for row in rows if v2_strict_final_quality_ok(row)]
        if max_leaderboard_wallets > 0:
            rows = rows[:max_leaderboard_wallets]
    return rows


def v2_strict_final_quality_ok(row: dict[str, Any]) -> bool:
    if to_float(row.get("esports_roi")) <= V2_STRICT_MIN_ROI:
        return False
    if to_float(row.get("positive_market_rate")) < V2_STRICT_MIN_POSITIVE_MARKET_RATE:
        return False
    if to_float(row.get("wilson_win_rate_lower_bound")) < V2_STRICT_MIN_WILSON:
        return False
    if to_float(row.get("capital_weighted_edge")) < V2_STRICT_MIN_CAPITAL_WEIGHTED_EDGE:
        return False
    return True


def v3_recent_health_ok(row: dict[str, Any]) -> bool:
    recent_14d_markets = int(row.get("recent_14d_market_count") or 0)
    if recent_14d_markets < V3_CORE_MIN_RECENT_14D_MARKETS:
        return False
    if to_float(row.get("recent_14d_roi")) <= V3_CORE_MIN_RECENT_ROI:
        return False
    if to_float(row.get("recent_14d_positive_rate")) < V3_CORE_MIN_RECENT_POSITIVE_RATE:
        return False
    recent_7d_markets = int(row.get("recent_7d_market_count") or 0)
    if recent_7d_markets > 0:
        if to_float(row.get("recent_7d_roi")) <= V3_CORE_MIN_RECENT_ROI:
            return False
        if to_float(row.get("recent_7d_positive_rate")) < V3_CORE_MIN_RECENT_POSITIVE_RATE:
            return False
    return True


def v3_core_quality_ok(row: dict[str, Any]) -> bool:
    return v2_strict_final_quality_ok(row) and v3_recent_health_ok(row)


def v3_activity_fresh_ok(row: dict[str, Any], *, now_ts: int) -> bool:
    last_trade_at = int(row.get("best_bucket_last_trade_at") or row.get("last_esports_trade_at") or 0)
    if last_trade_at <= 0:
        return False
    return int(now_ts) - last_trade_at <= V3_MAX_INACTIVE_SECONDS


def v3_copyable_final_gate_ok(row: dict[str, Any]) -> bool:
    metrics = leaderboard_rank_metrics(row)
    if to_float(metrics.get("esports_roi")) < V3_MIN_COPYABLE_BUCKET_ROI:
        return False
    bucket_key = str(row.get("best_bucket") or "")
    if not bucket_key:
        return False
    candidate_metrics = _candidate_bucket_metrics(row, bucket_key)
    if not candidate_two_sided_within_limits(
        candidate_metrics,
        max_count=V3_MAX_COPYABLE_TWO_SIDED_COUNT,
        max_rate=V3_MAX_COPYABLE_TWO_SIDED_RATE,
    ):
        return False
    participated = int(candidate_metrics.get("participated_market_count") or 0)
    if participated > 0:
        tail_rate = int(candidate_metrics.get("tail_entry_market_count") or 0) / participated
        if tail_rate > V3_MAX_COPYABLE_TAIL_ENTRY_RATE:
            return False
    return True


def v3_momentum_recent_ok(row: dict[str, Any]) -> bool:
    recent_7d_ok = (
        int(row.get("recent_7d_market_count") or 0) >= V3_MOMENTUM_MIN_7D_MARKETS
        and to_float(row.get("recent_7d_positive_rate")) >= V3_MOMENTUM_MIN_7D_POSITIVE_RATE
        and to_float(row.get("recent_7d_roi")) >= V3_MOMENTUM_MIN_7D_ROI
    )
    recent_14d_ok = (
        int(row.get("recent_14d_market_count") or 0) >= V3_MOMENTUM_MIN_14D_MARKETS
        and to_float(row.get("recent_14d_positive_rate")) >= V3_MOMENTUM_MIN_14D_POSITIVE_RATE
        and to_float(row.get("recent_14d_roi")) >= V3_MOMENTUM_MIN_14D_ROI
    )
    return recent_7d_ok or recent_14d_ok


def v3_momentum_quality_ok(row: dict[str, Any]) -> bool:
    if not v3_momentum_recent_ok(row):
        return False
    metrics = leaderboard_rank_metrics(row)
    best_bucket = str(row.get("best_bucket") or "")
    candidate_metrics = _candidate_bucket_metrics(row, best_bucket) if best_bucket else {}
    if to_float(row.get("esports_roi")) < V3_MOMENTUM_MIN_OVERALL_ROI:
        return False
    if to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")) < V3_MOMENTUM_MIN_CAPITAL_WEIGHTED_EDGE:
        return False
    median_entry = to_float(metrics.get("median_entry_price") or row.get("median_entry_price"))
    if median_entry <= 0 or median_entry > V3_MOMENTUM_MAX_MEDIAN_ENTRY_PRICE:
        return False
    if to_float(row.get("actual_minus_hold_pnl_rate")) > SWING_DEPENDENT_RATE:
        return False
    behavior_market_count = int(row.get("historical_trade_behavior_market_count") or 0)
    two_sided_rate = to_float(row.get("two_sided_trade_market_rate"))
    if behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS and two_sided_rate > V3_MOMENTUM_MAX_TWO_SIDED_RATE:
        return False
    if not candidate_two_sided_within_limits(
        candidate_metrics,
        max_count=V3_MAX_COPYABLE_TWO_SIDED_COUNT,
        max_rate=V3_MAX_COPYABLE_TWO_SIDED_RATE,
    ):
        return False
    participated = int(candidate_metrics.get("participated_market_count") or 0)
    if participated > 0:
        tail_rate = int(candidate_metrics.get("tail_entry_market_count") or 0) / participated
        if tail_rate > 0.34:
            return False
    if not row.get("eligible_buckets") and not row.get("eligible_market_types"):
        return False
    return True


def v3_momentum_score(row: dict[str, Any]) -> float:
    metrics = leaderboard_rank_metrics(row)
    recent_7d_score = 0.0
    if int(row.get("recent_7d_market_count") or 0) > 0:
        recent_7d_score = (
            45.0 * _clamp_float(to_float(row.get("recent_7d_positive_rate")))
            + 30.0 * _clamp_float(to_float(row.get("recent_7d_roi")) / 0.75)
            + 10.0 * _clamp_float(int(row.get("recent_7d_market_count") or 0) / 50)
        )
    recent_14d_score = (
        35.0 * _clamp_float(to_float(row.get("recent_14d_positive_rate")))
        + 25.0 * _clamp_float(to_float(row.get("recent_14d_roi")) / 0.60)
        + 10.0 * _clamp_float(int(row.get("recent_14d_market_count") or 0) / 100)
    )
    score = max(recent_7d_score, recent_14d_score)
    score += 10.0 * _clamp_float(to_float(row.get("esports_roi")) / 0.30)
    score += 10.0 * _clamp_float(to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")) / 0.20)
    score += 5.0 * _clamp_float((0.75 - to_float(metrics.get("median_entry_price") or row.get("median_entry_price"))) / 0.35)
    return round(score, 6)


def _v3_focus_profile_on_family(row: dict[str, Any], game_family: str, *, now_ts: int) -> dict[str, Any] | None:
    if str(row.get("category") or "esports").lower() != "esports":
        return None
    if int(row.get("scoring_version") or 0) != SCORING_VERSION:
        return None
    family_buckets = [
        str(value)
        for value in row.get("eligible_buckets") or []
        if value and split_bucket_key(str(value))[0] == game_family
    ]
    if not family_buckets:
        return None
    filtered = dict(row)
    filtered["eligible_buckets"] = family_buckets
    filtered["eligible_bucket_labels"] = [bucket_label(value) for value in family_buckets]
    filtered["eligible_market_types"] = sorted(
        {split_bucket_key(value)[1] for value in family_buckets},
        key=lambda value: {"main_match": 0, "game_winner": 1, "map_winner": 2}.get(value, 99),
    )
    filtered["eligible_market_type_labels"] = [
        MARKET_TYPE_LABELS.get(value, value)
        for value in filtered["eligible_market_types"]
    ]
    filtered["eligible_game_families"] = [game_family]
    filtered["eligible_game_family_labels"] = [
        {
            "dota2": "Dota2",
            "lol": "LoL",
            "cs2": "CS2",
        }.get(game_family, game_family.upper())
    ]
    enriched = enrich_esports_bucket_scores(filtered, now_ts=now_ts)
    if str(enriched.get("best_game_family") or "") != game_family:
        return None
    return enriched


def _v3_candidate_bucket_rates(row: dict[str, Any], bucket_key: str) -> dict[str, float]:
    candidate_metrics = _candidate_bucket_metrics(row, bucket_key)
    participated = int(candidate_metrics.get("participated_market_count") or 0)
    if participated <= 0:
        return {"two_sided": 0.0, "tail_entry": 0.0, "high_churn": 0.0}
    return {
        "two_sided": int(candidate_metrics.get("two_sided_market_count") or 0) / participated,
        "tail_entry": int(candidate_metrics.get("tail_entry_market_count") or 0) / participated,
        "high_churn": int(candidate_metrics.get("high_churn_market_count") or 0) / participated,
    }


def v3_family_supplement_quality_ok(row: dict[str, Any], game_family: str) -> bool:
    config = V3_FAMILY_SUPPLEMENT_CONFIG.get(game_family)
    if not config:
        return False
    if str(row.get("best_game_family") or "") != game_family:
        return False
    metrics = leaderboard_rank_metrics(row)
    closed_count = int(metrics.get("esports_closed_count") or 0)
    if closed_count < int(config["min_closed_count"]):
        return False
    if to_float(metrics.get("esports_roi")) < to_float(config["min_bucket_roi"]):
        return False
    if to_float(metrics.get("positive_market_rate")) < to_float(config["min_positive_rate"]):
        return False
    if to_float(metrics.get("wilson_win_rate_lower_bound")) < to_float(config["min_wilson"]):
        return False
    if to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")) < to_float(config["min_capital_edge"]):
        return False
    median_entry = to_float(metrics.get("median_entry_price") or row.get("median_entry_price"))
    if median_entry <= 0 or median_entry > to_float(config["max_median_entry_price"]):
        return False
    if int(row.get("recent_14d_market_count") or 0) < int(config["min_recent_14d_markets"]):
        return False
    if to_float(row.get("recent_14d_roi")) < to_float(config["min_recent_14d_roi"]):
        return False
    if to_float(row.get("recent_14d_positive_rate")) < to_float(config["min_recent_14d_positive_rate"]):
        return False
    if int(row.get("recent_7d_market_count") or 0) > 0 and to_float(row.get("recent_7d_roi")) < -0.05:
        return False
    if to_float(row.get("actual_minus_hold_pnl_rate")) > SWING_DEPENDENT_RATE:
        return False
    behavior_market_count = int(row.get("historical_trade_behavior_market_count") or 0)
    if behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS:
        if to_float(row.get("two_sided_trade_market_rate")) > to_float(config["max_two_sided_rate"]):
            return False
    bucket_key = str(row.get("best_bucket") or "")
    rates = _v3_candidate_bucket_rates(row, bucket_key)
    if rates["two_sided"] > to_float(config["max_two_sided_rate"]):
        return False
    if rates["tail_entry"] > to_float(config["max_tail_entry_rate"]):
        return False
    return True


def v3_family_supplement_score(row: dict[str, Any], game_family: str) -> float:
    metrics = leaderboard_rank_metrics(row)
    bucket_key = str(row.get("best_bucket") or "")
    rates = _v3_candidate_bucket_rates(row, bucket_key)
    score = (
        34.0 * _clamp_float(to_float(metrics.get("positive_market_rate")))
        + 22.0 * _clamp_float(to_float(metrics.get("esports_roi")) / 0.50)
        + 16.0 * _clamp_float(to_float(row.get("recent_14d_roi")) / 0.50)
        + 10.0 * _clamp_float(to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")) / 0.25)
        + 8.0 * _clamp_float(int(metrics.get("esports_closed_count") or 0) / 50)
        + 6.0 * _clamp_float(to_float(row.get("recent_14d_positive_rate")))
        + 4.0 * _clamp_float((0.75 - to_float(metrics.get("median_entry_price") or row.get("median_entry_price"))) / 0.35)
        - 8.0 * rates["tail_entry"]
        - 8.0 * rates["two_sided"]
        - 4.0 * rates["high_churn"]
    )
    return round(score, 6)


def _v3_prepare_followable_profile(row: dict[str, Any], *, now_ts: int) -> dict[str, Any] | None:
    if str(row.get("category") or "esports").lower() != "esports":
        return None
    if int(row.get("scoring_version") or 0) != SCORING_VERSION:
        return None
    profile = _with_esports_followable_market_types(dict(row))
    if profile is None:
        return None
    return enrich_esports_bucket_scores(profile, now_ts=now_ts)


def _v3_watch_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    metrics = leaderboard_rank_metrics(row)
    if not v3_momentum_recent_ok(row):
        reasons.append("not_recent_momentum")
    if to_float(row.get("esports_roi")) < V3_MOMENTUM_MIN_OVERALL_ROI:
        reasons.append("low_overall_roi")
    if to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")) < V3_MOMENTUM_MIN_CAPITAL_WEIGHTED_EDGE:
        reasons.append("weak_capital_edge")
    median_entry = to_float(metrics.get("median_entry_price") or row.get("median_entry_price"))
    if median_entry <= 0 or median_entry > V3_MOMENTUM_MAX_MEDIAN_ENTRY_PRICE:
        reasons.append("high_entry_price")
    if to_float(row.get("actual_minus_hold_pnl_rate")) > SWING_DEPENDENT_RATE:
        reasons.append("swing_dependent")
    behavior_market_count = int(row.get("historical_trade_behavior_market_count") or 0)
    if behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS and to_float(row.get("two_sided_trade_market_rate")) > V3_MOMENTUM_MAX_TWO_SIDED_RATE:
        reasons.append("two_sided_behavior")
    if not row.get("eligible_buckets") and not row.get("eligible_market_types"):
        reasons.append("no_followable_scope")
    if not reasons and not v3_recent_health_ok(row):
        reasons.append("core_recent_health_gate")
    return sorted(set(reasons))


def build_v3_leaderboard(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    max_leaderboard_wallets: int = 30,
    max_core_wallets: int = 20,
    max_momentum_wallets: int = 10,
    max_watchlist_wallets: int = 50,
) -> dict[str, Any]:
    core_candidates = [
        {**row, "collector_version": "v3", "v3_lane": "core"}
        for row in build_v2_leaderboard(
            profiles_by_wallet,
            now_ts=now_ts,
            max_leaderboard_wallets=0,
            require_strict_quality_gate=True,
            max_two_sided_market_count=V3_MAX_COPYABLE_TWO_SIDED_COUNT,
            max_two_sided_market_rate=V3_MAX_COPYABLE_TWO_SIDED_RATE,
        )
        if v3_core_quality_ok(row)
        and v3_activity_fresh_ok(row, now_ts=now_ts)
        and v3_copyable_final_gate_ok(row)
    ]
    core = sorted(core_candidates, key=leaderboard_rank_key)[: max(0, int(max_core_wallets))]
    core_wallets = {normalize_wallet(row.get("wallet")) for row in core}

    family_candidates_by_family: dict[str, list[dict[str, Any]]] = {
        family: [] for family in V3_FAMILY_SUPPLEMENT_ORDER
    }
    momentum_candidates: list[dict[str, Any]] = []
    watch_candidates: list[dict[str, Any]] = []
    for profile in profiles_by_wallet.values():
        wallet = normalize_wallet(profile.get("wallet"))
        if not wallet or wallet in core_wallets:
            continue
        for game_family in V3_FAMILY_SUPPLEMENT_ORDER:
            focused = _v3_focus_profile_on_family(profile, game_family, now_ts=now_ts)
            if focused is None:
                continue
            if not v3_activity_fresh_ok(focused, now_ts=now_ts):
                continue
            if not v3_family_supplement_quality_ok(focused, game_family):
                continue
            if not v3_copyable_final_gate_ok(focused):
                continue
            score = v3_family_supplement_score(focused, game_family)
            family_candidates_by_family[game_family].append(
                {
                    **focused,
                    "collector_version": "v3",
                    "v3_lane": "family_supplement",
                    "v3_family": game_family,
                    "v3_family_score": score,
                }
            )
        prepared = _v3_prepare_followable_profile(profile, now_ts=now_ts)
        if prepared is None:
            continue
        prepared = {**prepared, "collector_version": "v3"}
        if not v3_activity_fresh_ok(prepared, now_ts=now_ts):
            watch_candidates.append(
                {
                    **prepared,
                    "v3_lane": "watch",
                    "v3_watch_reasons": sorted(set(_v3_watch_reasons(prepared)) | {"inactive_gt5d"}),
                    "v3_momentum_score": v3_momentum_score(prepared),
                }
            )
            continue
        if not v3_copyable_final_gate_ok(prepared):
            watch_candidates.append(
                {
                    **prepared,
                    "v3_lane": "watch",
                    "v3_watch_reasons": sorted(set(_v3_watch_reasons(prepared)) | {"copyability_gate"}),
                    "v3_momentum_score": v3_momentum_score(prepared),
                }
            )
            continue
        if v3_momentum_quality_ok(prepared):
            score = v3_momentum_score(prepared)
            momentum_candidates.append({**prepared, "v3_lane": "momentum", "v3_momentum_score": score})
        elif v3_momentum_recent_ok(prepared) or not v3_recent_health_ok(prepared):
            watch_candidates.append(
                {
                    **prepared,
                    "v3_lane": "watch",
                    "v3_watch_reasons": _v3_watch_reasons(prepared),
                    "v3_momentum_score": v3_momentum_score(prepared),
                }
            )

    momentum_candidates.sort(
        key=lambda row: (
            -to_float(row.get("v3_momentum_score")),
            -to_float(row.get("recent_7d_roi")),
            -to_float(row.get("recent_14d_roi")),
            -to_float(row.get("positive_market_rate")),
            normalize_wallet(row.get("wallet")),
        )
    )
    watch_candidates.sort(
        key=lambda row: (
            -to_float(row.get("v3_momentum_score")),
            -to_float(row.get("recent_7d_positive_rate")),
            -to_float(row.get("recent_14d_positive_rate")),
            normalize_wallet(row.get("wallet")),
        )
    )
    family_supplements: list[dict[str, Any]] = []
    family_wallets: set[str] = set()
    family_slots = (
        max(0, int(max_leaderboard_wallets) - len(core))
        if max_leaderboard_wallets > 0
        else sum(int(V3_FAMILY_SUPPLEMENT_CONFIG[family]["limit"]) for family in V3_FAMILY_SUPPLEMENT_ORDER)
    )
    for game_family in V3_FAMILY_SUPPLEMENT_ORDER:
        candidates = family_candidates_by_family.get(game_family, [])
        candidates.sort(
            key=lambda row: (
                -to_float(row.get("v3_family_score")),
                -to_float(row.get("recent_14d_roi")),
                -to_float(row.get("recent_14d_positive_rate")),
                -int(leaderboard_rank_metrics(row).get("esports_closed_count") or 0),
                normalize_wallet(row.get("wallet")),
            )
        )
        family_limit = int(V3_FAMILY_SUPPLEMENT_CONFIG[game_family]["limit"])
        selected_for_family = 0
        for candidate in candidates:
            wallet = normalize_wallet(candidate.get("wallet"))
            if not wallet or wallet in family_wallets:
                continue
            if max_leaderboard_wallets > 0 and len(family_supplements) >= family_slots:
                break
            if selected_for_family >= family_limit:
                break
            family_supplements.append(candidate)
            family_wallets.add(wallet)
            selected_for_family += 1

    occupied_wallets = core_wallets | family_wallets
    momentum_candidates = [
        row for row in momentum_candidates if normalize_wallet(row.get("wallet")) not in occupied_wallets
    ]
    remaining_slots = (
        max(0, int(max_leaderboard_wallets) - len(core) - len(family_supplements))
        if max_leaderboard_wallets > 0
        else int(max_momentum_wallets)
    )
    momentum_limit = min(max(0, int(max_momentum_wallets)), remaining_slots) if max_leaderboard_wallets > 0 else max(0, int(max_momentum_wallets))
    momentum = momentum_candidates[:momentum_limit]
    leaderboard = core + family_supplements + momentum
    if max_leaderboard_wallets > 0:
        leaderboard = leaderboard[:max_leaderboard_wallets]
    watch = watch_candidates[: max(0, int(max_watchlist_wallets))]
    return {
        "leaderboard": leaderboard,
        "core": core,
        "momentum": momentum,
        "family_supplements": family_supplements,
        "watch": watch,
        "lane_counts": {
            "core": len(core),
            "momentum": len(momentum),
            "family_supplement": len(family_supplements),
            "watch": len(watch),
        },
    }


def run_ordered_io_tasks(items: list[Any], worker, *, max_workers: int) -> list[Any]:
    if len(items) <= 1 or max_workers <= 1:
        results = []
        for item in items:
            try:
                results.append(worker(item))
            except Exception as exc:
                results.append(exc)
        return results
    results: list[Any] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, item): index for index, item in enumerate(items)}
        for future in as_completed(futures):
            index = futures[future]
            try:
                results[index] = future.result()
            except Exception as exc:
                results[index] = exc
    return results


def make_retryable_profile(candidate: dict[str, Any], exc: Exception, *, now_ts: int) -> dict[str, Any]:
    wallet = normalize_wallet(candidate.get("wallet"))
    return {
        "wallet": wallet,
        "candidate": {**candidate, "wallet": wallet},
        "grade": "unknown",
        "profile_state": "failed_retryable",
        "reasons": ["profile_failed"],
        "error": str(exc),
        "profiled_at": now_ts,
        "scoring_version": SCORING_VERSION,
    }


def _wallets_from_rows(rows: list[dict[str, Any]]) -> set[str]:
    return {wallet for row in rows if (wallet := normalize_wallet(row.get("wallet")))}


def _grade_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        _increment_count(counts, str(row.get("grade") or "unknown"))
    return _sorted_count_dict(counts)


def build_v2_compare_report(v1_dir: Path, v2_dir: Path) -> dict[str, Any]:
    v1_leaderboard = read_json(v1_dir / "smart_wallet_leaderboard.json", [])
    v1_profiles = read_json(v1_dir / "wallet_profiles.json", [])
    v2_leaderboard = read_json(v2_dir / "v2_leaderboard.json", [])
    v2_profiles = read_json(v2_dir / "v2_wallet_rawdata.json", [])
    v2_seed_wallets = read_json(v2_dir / "v2_seed_wallets.json", [])
    v2_profile_wallets = read_json(v2_dir / "v2_profile_wallets.json", [])
    if not isinstance(v1_leaderboard, list):
        v1_leaderboard = []
    if not isinstance(v1_profiles, list):
        v1_profiles = []
    if not isinstance(v2_leaderboard, list):
        v2_leaderboard = []
    if not isinstance(v2_profiles, list):
        v2_profiles = []
    if not isinstance(v2_seed_wallets, list):
        v2_seed_wallets = []
    if not isinstance(v2_profile_wallets, list):
        v2_profile_wallets = []
    v1_leaderboard_wallets = _wallets_from_rows(v1_leaderboard)
    v2_leaderboard_wallets = _wallets_from_rows(v2_leaderboard)
    v1_profile_wallets = _wallets_from_rows(v1_profiles)
    v2_profile_raw_wallets = _wallets_from_rows(v2_profiles)
    v2_seed_wallet_set = _wallets_from_rows(v2_seed_wallets)
    v2_profile_wallet_set = _wallets_from_rows(v2_profile_wallets)
    leaderboard_overlap = sorted(v1_leaderboard_wallets & v2_leaderboard_wallets)
    v1_only = sorted(v1_leaderboard_wallets - v2_leaderboard_wallets)
    v2_only = sorted(v2_leaderboard_wallets - v1_leaderboard_wallets)
    v1_leaderboard_count = len(v1_leaderboard_wallets)
    v2_leaderboard_count = len(v2_leaderboard_wallets)
    return {
        "v1_dir": str(v1_dir),
        "v2_dir": str(v2_dir),
        "v1_leaderboard_wallet_count": v1_leaderboard_count,
        "v2_leaderboard_wallet_count": v2_leaderboard_count,
        "leaderboard_overlap_count": len(leaderboard_overlap),
        "leaderboard_overlap_rate_vs_v1": round(len(leaderboard_overlap) / v1_leaderboard_count, 8)
        if v1_leaderboard_count
        else 0.0,
        "leaderboard_overlap_rate_vs_v2": round(len(leaderboard_overlap) / v2_leaderboard_count, 8)
        if v2_leaderboard_count
        else 0.0,
        "leaderboard_overlap_wallets": leaderboard_overlap,
        "v1_only_leaderboard_wallets": v1_only,
        "v2_only_leaderboard_wallets": v2_only,
        "v1_profile_wallet_count": len(v1_profile_wallets),
        "v2_seed_wallet_count": len(v2_seed_wallet_set),
        "v2_profile_wallet_count": len(v2_profile_wallet_set),
        "v2_raw_profile_wallet_count": len(v2_profile_raw_wallets),
        "v1_leaderboard_missing_from_v2_seed_count": len(v1_leaderboard_wallets - v2_seed_wallet_set),
        "v1_leaderboard_missing_from_v2_seed_wallets": sorted(v1_leaderboard_wallets - v2_seed_wallet_set),
        "v1_leaderboard_missing_from_v2_profile_count": len(v1_leaderboard_wallets - v2_profile_wallet_set),
        "v1_leaderboard_missing_from_v2_profile_wallets": sorted(v1_leaderboard_wallets - v2_profile_wallet_set),
        "v2_raw_profile_grade_counts": _grade_counts(v2_profiles),
        "v2_leaderboard_grade_counts": _grade_counts(v2_leaderboard),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def command_collect_v2_compare(args: argparse.Namespace) -> int:
    root = Path(getattr(args, "data_dir", None) or DATA_DIR)
    v1_dir = Path(getattr(args, "v1_dir", None) or (root / "esports"))
    v2_dir = Path(getattr(args, "v2_dir", None) or (root / "collector_v2" / "esports"))
    output_file = Path(getattr(args, "output_file", None) or (v2_dir / "v2_compare_report.json"))
    output_file.parent.mkdir(parents=True, exist_ok=True)
    report = build_v2_compare_report(v1_dir, v2_dir)
    write_json(output_file, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _position_pnl(position: dict[str, Any]) -> float:
    total_pnl = to_float(position.get("totalPnl"))
    if total_pnl != 0:
        return total_pnl
    return to_float(position.get("realizedPnl"))


def summarize_v2_market_positions_probe(
    market: dict[str, Any],
    response: list[dict[str, Any]],
) -> dict[str, Any]:
    winner_index = winning_outcome_index(market)
    outcome_stats: dict[str, dict[str, Any]] = {}
    for token_index, token_block in enumerate(response or []):
        for position in token_block.get("positions") or []:
            outcome_index = to_int(position.get("outcomeIndex"), token_index)
            key = str(outcome_index)
            stats = outcome_stats.setdefault(
                key,
                {
                    "outcome_index": outcome_index,
                    "is_winning_outcome": outcome_index == winner_index,
                    "position_count": 0,
                    "positive_pnl_positions": 0,
                    "zero_pnl_positions": 0,
                    "negative_pnl_positions": 0,
                    "total_pnl_sum": 0.0,
                    "realized_pnl_sum": 0.0,
                },
            )
            pnl = _position_pnl(position)
            stats["position_count"] += 1
            stats["total_pnl_sum"] = round(to_float(stats.get("total_pnl_sum")) + to_float(position.get("totalPnl")), 8)
            stats["realized_pnl_sum"] = round(
                to_float(stats.get("realized_pnl_sum")) + to_float(position.get("realizedPnl")),
                8,
            )
            if pnl > 0:
                stats["positive_pnl_positions"] += 1
            elif pnl < 0:
                stats["negative_pnl_positions"] += 1
            else:
                stats["zero_pnl_positions"] += 1
    winning_stats = [row for row in outcome_stats.values() if row.get("is_winning_outcome")]
    losing_stats = [row for row in outcome_stats.values() if not row.get("is_winning_outcome")]
    return {
        "condition_id": str(market.get("condition_id") or "").lower(),
        "bucket_key": market.get("bucket_key") or bucket_key(market.get("game_family"), market.get("market_type")),
        "winning_outcome_index": winner_index,
        "outcome_stats": {
            key: outcome_stats[key]
            for key in sorted(outcome_stats, key=lambda value: to_int(value))
        },
        "winning_position_count": sum(to_int(row.get("position_count")) for row in winning_stats),
        "losing_position_count": sum(to_int(row.get("position_count")) for row in losing_stats),
        "winning_positive_positions": sum(to_int(row.get("positive_pnl_positions")) for row in winning_stats),
        "losing_positive_positions": sum(to_int(row.get("positive_pnl_positions")) for row in losing_stats),
        "losing_zero_positions": sum(to_int(row.get("zero_pnl_positions")) for row in losing_stats),
        "losing_negative_positions": sum(to_int(row.get("negative_pnl_positions")) for row in losing_stats),
    }


def build_v2_api_probe_report(
    target_markets: list[dict[str, Any]],
    client: PolymarketClient,
    *,
    sample_markets: int = 20,
    positions_limit: int = 20,
    sort_by: str = "TOTAL_PNL",
) -> dict[str, Any]:
    sampled = [
        market
        for market in target_markets
        if isinstance(market, dict) and market.get("condition_id") and winning_outcome_index(market) is not None
    ][: max(0, int(sample_markets))]
    markets: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    totals = {
        "winning_position_count": 0,
        "losing_position_count": 0,
        "winning_positive_positions": 0,
        "losing_positive_positions": 0,
        "losing_zero_positions": 0,
        "losing_negative_positions": 0,
    }
    for market in sampled:
        condition_id = str(market.get("condition_id") or "").lower()
        try:
            response = client.market_positions(
                condition_id,
                limit=positions_limit,
                sort_by=sort_by,
                sort_direction="DESC",
            )
        except Exception as exc:
            errors[condition_id] = str(exc)
            continue
        summary = summarize_v2_market_positions_probe(market, response or [])
        markets.append(summary)
        for key in totals:
            totals[key] += to_int(summary.get(key))
    if totals["losing_positive_positions"] > 0:
        conclusion = "losing_outcome_can_show_positive_pnl"
    elif totals["losing_position_count"] > 0:
        conclusion = "losing_outcome_returned_without_positive_pnl"
    else:
        conclusion = "losing_outcome_absent_in_sample"
    return {
        "sampled_market_count": len(sampled),
        "successful_market_count": len(markets),
        "error_market_count": len(errors),
        "errors": errors,
        "positions_limit": int(positions_limit),
        "sort_by": sort_by,
        "totals": totals,
        "conclusion": conclusion,
        "markets": markets,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def command_collect_v2_probe(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    client = client or build_client(args)
    root = Path(getattr(args, "data_dir", None) or DATA_DIR)
    v2_dir = Path(getattr(args, "v2_dir", None) or (root / "collector_v2" / "esports"))
    target_markets = read_json(v2_dir / "v2_target_markets.json", [])
    if not isinstance(target_markets, list):
        target_markets = []
    report = build_v2_api_probe_report(
        target_markets,
        client,
        sample_markets=getattr(args, "sample_markets", 20),
        positions_limit=getattr(args, "positions_limit", 20),
        sort_by=getattr(args, "seed_sort_by", "TOTAL_PNL"),
    )
    output_file = Path(getattr(args, "output_file", None) or (v2_dir / "v2_api_probe.json"))
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_json(output_file, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _command_collect_experimental(
    args: argparse.Namespace,
    client: PolymarketClient | None = None,
    *,
    collector_version: str = "v2",
) -> int:
    client = client or build_client(args)
    output_dir = resolve_collect_experimental_output_dir(args, collector_version=collector_version)
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = collector_version
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    lookback_days = int(getattr(args, "lookback_days", 30) or 30)
    min_end_date = now_dt - timedelta(days=lookback_days)
    stage_timings: dict[str, float] = {}
    stage_started_at = time.monotonic()

    def mark_stage(name: str) -> None:
        nonlocal stage_started_at
        current = time.monotonic()
        stage_timings[f"{name}_seconds"] = round(current - stage_started_at, 6)
        stage_started_at = current

    closed_events = client.list_events_paginated(
        closed=True,
        active=None,
        max_pages=getattr(args, "gamma_pages", 10),
        min_end_date=min_end_date,
        max_end_date=now_dt,
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
    )
    classification_set = build_classification_set(
        closed_events,
        now=now_dt,
        lookback_days=lookback_days,
    )
    write_json(output_dir / f"{prefix}_classification_set.json", classification_set)
    mark_stage("classification")

    target_markets, target_meta = select_v2_target_markets(
        classification_set,
        now=now_dt,
        lookback_days=lookback_days,
        bucket_market_limit=getattr(args, "bucket_market_limit", 50),
    )
    write_json(output_dir / f"{prefix}_target_markets.json", target_markets)
    mark_stage("target_markets")

    def fetch_market_positions(market: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str | None]:
        condition_id = str(market.get("condition_id") or "").lower()
        try:
            response = client.market_positions(
                condition_id,
                limit=getattr(args, "positions_per_market", 20),
                sort_by=getattr(args, "seed_sort_by", "TOTAL_PNL"),
                sort_direction="DESC",
            )
            return condition_id, response or [], None
        except Exception as exc:
            return condition_id, [], str(exc)

    market_position_results = run_ordered_io_tasks(
        target_markets,
        fetch_market_positions,
        max_workers=getattr(args, "max_workers", 8),
    )
    market_by_id = {str(market.get("condition_id") or "").lower(): market for market in target_markets}
    seed_positions: list[dict[str, Any]] = []
    market_position_errors: dict[str, str] = {}
    market_position_api_fetches = 0
    for result in market_position_results:
        if isinstance(result, Exception):
            continue
        condition_id, response, error = result
        if error:
            market_position_errors[condition_id] = error
            continue
        market_position_api_fetches += 1
        seed_positions.extend(
            collect_v2_seed_positions(
                market_by_id.get(condition_id) or {"condition_id": condition_id},
                response,
                positions_per_market=getattr(args, "positions_per_market", 20),
            )
        )
    write_json(output_dir / f"{prefix}_seed_positions.json", seed_positions)
    mark_stage("seed_positions")

    seed_wallets = aggregate_v2_seed_wallets(seed_positions)
    write_json(output_dir / f"{prefix}_seed_wallets.json", list(seed_wallets.values()))
    max_profile_wallets = resolve_collect_experimental_profile_wallet_limit(
        args,
        collector_version=collector_version,
    )
    profile_wallets = filter_v2_profile_wallets(
        seed_wallets,
        max_wallets=max_profile_wallets,
    )
    write_json(output_dir / f"{prefix}_profile_wallets.json", profile_wallets)
    mark_stage("seed_wallet_filter")

    condition_ids = {
        str(row.get("condition_id") or "").lower()
        for row in classification_set
        if row.get("condition_id")
    }
    market_records_by_id = {
        str(row.get("condition_id") or "").lower(): row
        for row in classification_set
        if row.get("condition_id")
    }
    condition_type_by_id = {
        condition_id: str(row.get("market_type") or MAIN_MATCH)
        for condition_id, row in market_records_by_id.items()
    }
    condition_game_family_by_id = {
        condition_id: str(row.get("game_family") or "unknown")
        for condition_id, row in market_records_by_id.items()
    }

    def fetch_raw_user_trades(seed_wallet: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
        wallet = normalize_wallet(seed_wallet.get("wallet"))
        if collector_version == "v3":
            trades = fetch_recent_esports_user_trades_for_wallet(
                client,
                wallet,
                condition_ids,
                page_limit=getattr(args, "user_history_trades_limit", 500),
                max_pages=getattr(args, "user_history_trades_max_pages", 3),
                max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
                data_dir=output_dir,
                now_ts=now_ts,
                cache_ttl_days=getattr(args, "user_trades_cache_ttl_days", 1),
                force_refresh=getattr(args, "refresh_user_trades", False),
                use_cache=not getattr(args, "no_user_trades_cache", False),
            )
        else:
            trades = fetch_recent_user_trades_for_wallet(
                client,
                wallet,
                page_limit=getattr(args, "user_history_trades_limit", 500),
                max_pages=getattr(args, "user_history_trades_max_pages", 8),
                data_dir=output_dir,
                now_ts=now_ts,
                cache_ttl_days=getattr(args, "user_trades_cache_ttl_days", 1),
                force_refresh=getattr(args, "refresh_user_trades", False),
                use_cache=not getattr(args, "no_user_trades_cache", False),
            )
        return wallet, trades

    raw_trade_results = run_ordered_io_tasks(
        profile_wallets,
        fetch_raw_user_trades,
        max_workers=getattr(args, "max_workers", 8),
    )
    raw_user_trades_by_wallet: dict[str, list[dict[str, Any]]] = {}
    raw_user_trade_errors: dict[str, str] = {}
    for index, result in enumerate(raw_trade_results):
        fallback_wallet = normalize_wallet(profile_wallets[index].get("wallet")) if index < len(profile_wallets) else ""
        if isinstance(result, Exception):
            if fallback_wallet:
                raw_user_trade_errors[fallback_wallet] = str(result)
                raw_user_trades_by_wallet[fallback_wallet] = []
            continue
        wallet, trades = result
        raw_user_trades_by_wallet[normalize_wallet(wallet)] = trades
    mark_stage("raw_user_trades")

    def profile_one(seed_wallet: dict[str, Any]) -> dict[str, Any]:
        seed_candidate = dict(seed_wallet.get("candidate") or {})
        seed_candidate.update(
            {
                "wallet": normalize_wallet(seed_wallet.get("wallet")),
                "v2_seed_score": seed_wallet.get("seed_score"),
                "v2_seed_win_count": seed_wallet.get("seed_win_count"),
                "v2_seed_weighted_roi": seed_wallet.get("seed_weighted_roi"),
                "v2_seed_bucket_counts": seed_wallet.get("seed_bucket_counts"),
                "v2_seed_market_type_counts": seed_wallet.get("seed_market_type_counts"),
            }
        )
        wallet = normalize_wallet(seed_candidate.get("wallet"))
        seed_candidate = build_v2_profile_candidate_from_trades(
            seed_candidate,
            raw_user_trades_by_wallet.get(wallet, []),
            market_records_by_id,
        )
        profile = profile_candidate_wallet(
            seed_candidate,
            condition_ids,
            market_records_by_id=market_records_by_id,
            condition_type_by_id=condition_type_by_id,
            condition_game_family_by_id=condition_game_family_by_id,
            user_trades_loader=lambda _wallet: raw_user_trades_by_wallet.get(wallet, []),
            current_positions_loader=lambda _wallet: [],
            now_ts=now_ts,
        )
        return {**profile, "v2_seed": {key: value for key, value in seed_wallet.items() if key != "seed_positions"}}

    profiles = [
        make_retryable_profile(profile_wallets[index].get("candidate") or profile_wallets[index], result, now_ts=now_ts)
        if isinstance(result, Exception)
        else result
        for index, result in enumerate(
            run_ordered_io_tasks(profile_wallets, profile_one, max_workers=getattr(args, "max_workers", 8))
        )
    ]
    profiles_by_wallet = {
        normalize_wallet(row.get("wallet")): row
        for row in profiles
        if normalize_wallet(row.get("wallet"))
    }
    write_json(output_dir / f"{prefix}_wallet_rawdata.json", list(profiles_by_wallet.values()))
    mark_stage("wallet_profiles")

    v3_result: dict[str, Any] | None = None
    if collector_version == "v3":
        v3_result = build_v3_leaderboard(
            profiles_by_wallet,
            now_ts=now_ts,
            max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", 30),
            max_core_wallets=getattr(args, "max_core_wallets", 20),
            max_momentum_wallets=getattr(args, "max_momentum_wallets", 10),
            max_watchlist_wallets=getattr(args, "max_watchlist_wallets", 50),
        )
        leaderboard = v3_result["leaderboard"]
        write_json(output_dir / "v3_core_leaderboard.json", v3_result["core"])
        write_json(output_dir / "v3_momentum_leaderboard.json", v3_result["momentum"])
        write_json(output_dir / "v3_family_leaderboard.json", v3_result["family_supplements"])
        write_json(output_dir / "v3_watchlist.json", v3_result["watch"])
    else:
        leaderboard = build_v2_leaderboard(
            profiles_by_wallet,
            now_ts=now_ts,
            max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", 30),
        )
    write_json(output_dir / f"{prefix}_leaderboard.json", leaderboard)
    mark_stage("leaderboard")

    summary = {
        "collector_version": collector_version,
        "category": "esports",
        "lookback_days": lookback_days,
        "classification_market_count": len(classification_set),
        "target_market_count": len(target_markets),
        "seed_position_count": len(seed_positions),
        "seed_wallet_count": len(seed_wallets),
        "profile_wallet_count": len(profile_wallets),
        "max_profile_wallets": max_profile_wallets,
        "profiled_wallet_count": len(profiles_by_wallet),
        "leaderboard_wallet_count": len(leaderboard),
        "market_position_api_fetches": market_position_api_fetches,
        "market_position_errors": len(market_position_errors),
        "market_position_error_markets": market_position_errors,
        "raw_user_trade_errors": raw_user_trade_errors,
        "target_meta": target_meta,
        **build_v2_collection_diagnostics(
            seed_wallets=seed_wallets,
            profile_wallets=profile_wallets,
            profiles_by_wallet=profiles_by_wallet,
            leaderboard=leaderboard,
            now_ts=now_ts,
        ),
        "stage_timings": {key: round(float(value), 6) for key, value in sorted(stage_timings.items())},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if v3_result is not None:
        summary.update(
            {
                "v3_lane_counts": v3_result["lane_counts"],
                "core_leaderboard_wallet_count": len(v3_result["core"]),
                "momentum_leaderboard_wallet_count": len(v3_result["momentum"]),
                "family_leaderboard_wallet_count": len(v3_result["family_supplements"]),
                "watchlist_wallet_count": len(v3_result["watch"]),
                "max_inactive_hours": round(V3_MAX_INACTIVE_SECONDS / 3600, 2),
                "min_copyable_bucket_roi": V3_MIN_COPYABLE_BUCKET_ROI,
                "max_copyable_two_sided_market_count": V3_MAX_COPYABLE_TWO_SIDED_COUNT,
                "max_copyable_two_sided_market_rate": V3_MAX_COPYABLE_TWO_SIDED_RATE,
                "max_copyable_tail_entry_rate": V3_MAX_COPYABLE_TAIL_ENTRY_RATE,
                "high_churn_hard_excluded": False,
            }
        )
    if collector_version == "v3" and not getattr(args, "no_dashboard_publish", False):
        dashboard_publish = publish_collector_v3_dashboard_outputs(
            output_dir,
            resolve_data_dir(args),
            summary=summary,
            now_ts=now_ts,
        )
        summary["dashboard_publish"] = dashboard_publish
        write_json(
            resolve_data_dir(args) / "build_summary.json",
            {**summary, "dashboard_publish": dashboard_publish},
        )
    write_json(output_dir / f"{prefix}_build_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def command_collect_v2(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    return _command_collect_experimental(args, client=client, collector_version="v2")


def publish_collector_v3_dashboard_outputs(
    collector_output_dir: Path,
    data_dir: Path,
    *,
    summary: dict[str, Any] | None = None,
    now_ts: int | None = None,
) -> dict[str, Any]:
    collector_output_dir = Path(collector_output_dir)
    data_dir = Path(data_dir)
    now_ts = int(now_ts or time.time())
    leaderboard_value = read_json(collector_output_dir / "v3_leaderboard.json", [])
    profiles_value = read_json(collector_output_dir / "v3_wallet_rawdata.json", [])
    leaderboard = [row for row in leaderboard_value if isinstance(row, dict)] if isinstance(leaderboard_value, list) else []
    profiles = [row for row in profiles_value if isinstance(row, dict)] if isinstance(profiles_value, list) else []

    data_dir.mkdir(parents=True, exist_ok=True)
    write_json(data_dir / "smart_wallet_leaderboard.json", leaderboard)
    write_json(data_dir / "wallet_profiles.json", profiles)
    LeaderboardStore(data_dir / "leaderboard.db").replace_leaderboard(
        leaderboard,
        category="esports",
        updated_at=now_ts,
    )
    publish_summary = {
        "published": True,
        "collector_version": "v3",
        "category": "esports",
        "collector_output_dir": str(collector_output_dir),
        "data_dir": str(data_dir),
        "leaderboard_wallet_count": len(leaderboard),
        "profile_wallet_count": len(profiles),
        "updated_at": now_ts,
    }
    dashboard_summary = dict(summary) if isinstance(summary, dict) else {}
    dashboard_summary.update(
        {
            "collector_version": "v3",
            "category": "esports",
            "leaderboard_wallet_count": len(leaderboard),
            "profiled_wallet_count": len(profiles),
            "dashboard_publish": publish_summary,
        }
    )
    write_json(data_dir / "build_summary.json", dashboard_summary)
    return publish_summary


def command_collect_v3(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    return _command_collect_experimental(args, client=client, collector_version="v3")


def command_collect(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    category = str(getattr(args, "category", "esports") or "esports").lower()
    if category == "esports":
        if client is None:
            return command_collect_v3(args)
        return command_collect_v3(args, client=client)
    if client is None:
        return command_build_leaderboard(args)
    return command_build_leaderboard(args, client=client)


def command_build_leaderboard(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    data_dir = resolve_data_dir(args)
    with acquire_build_lock(data_dir):
        return _command_build_leaderboard_unlocked(args, client=client)


def _command_build_leaderboard_unlocked(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    client = client or build_client(args)
    data_dir = resolve_data_dir(args)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    stage_timings: dict[str, float] = {}
    stage_started_at = time.monotonic()

    def mark_stage(name: str) -> None:
        nonlocal stage_started_at
        now = time.monotonic()
        stage_timings[f"{name}_seconds"] = round(now - stage_started_at, 6)
        stage_started_at = now

    category = getattr(args, "category", "esports")
    effective_limits = effective_build_limits(args)
    effective_defaults = effective_build_defaults(args)
    classification_lookback_days = effective_defaults["classification_lookback_days"]
    lookback_steps = (classification_lookback_days,) if classification_lookback_days else (7, 14, 30)
    classification_path = data_dir / "esports_classification_set.json"
    classification_meta_path = data_dir / "esports_classification_set.meta.json"
    tag_slugs = CATEGORY_TAG_SLUGS.get(category, CATEGORY_TAG_SLUGS["esports"])
    market_scope = CATEGORY_MARKET_SCOPES.get(category, CATEGORY_MARKET_SCOPES["esports"])
    classification_meta = {
        "category": category,
        "gamma_pages": args.gamma_pages,
        "classification_lookback_days": classification_lookback_days,
        "market_scope": market_scope,
        "tag_slugs": list(tag_slugs),
        "sports_event_min_volume": getattr(args, "sports_event_min_volume", 0.0) if category == "sports" else 0.0,
    }
    classification_source = "api"
    if (
        classification_path.exists()
        and read_json(classification_meta_path, None) == classification_meta
        and not should_refresh_file_cache(
            classification_path.stat().st_mtime,
            now_ts=now_ts,
            ttl_hours=args.classification_cache_ttl_hours,
            force_refresh=args.refresh_classification,
        )
    ):
        classification_set = read_json(classification_path, [])
        classification_source = "cache"
    else:
        now_dt = datetime.fromtimestamp(now_ts, timezone.utc)
        min_end_date = (
            now_dt - timedelta(days=classification_lookback_days)
            if classification_lookback_days and classification_lookback_days > 0
            else None
        )
        closed_events = client.list_events_paginated(
            closed=True,
            active=None,
            max_pages=args.gamma_pages,
            min_end_date=min_end_date,
            max_end_date=None if category == "sports" else now_dt,
            tag_slugs=tag_slugs,
        )
        classification_set = build_classification_set(
            closed_events,
            now=now_dt,
            lookback_days=classification_lookback_days if classification_lookback_days > 0 else None,
            sports_event_min_volume=getattr(args, "sports_event_min_volume", 0.0) if category == "sports" else 0.0,
        )
        write_json(classification_path, classification_set)
        write_json(classification_meta_path, classification_meta)
    league_event_counts = league_event_counts_from_classification_set(classification_set)
    mark_stage("classification")

    effective_discovery = effective_discovery_defaults(args)
    market_batch_size = args.market_batch_size or 50
    market_count = effective_discovery["max_markets_per_run"]
    market_offset = args.market_offset
    if args.market_batch_index is not None:
        market_offset = args.market_batch_index * market_batch_size
        market_count = market_batch_size
    discovery_slate, slate_meta = build_discovery_slate(
        classification_set,
        lookback_steps=lookback_steps,
        min_market_volume=args.min_market_volume,
        fallback_min_market_volume=args.fallback_min_market_volume,
        submarket_min_market_volume=args.submarket_min_market_volume,
        submarket_fallback_min_market_volume=args.submarket_fallback_min_market_volume,
        target_markets=effective_discovery["target_markets"],
        submarket_target_markets=effective_discovery["submarket_target_markets"],
        game_winner_target_markets=effective_discovery["game_winner_target_markets"],
        map_winner_target_markets=effective_discovery["map_winner_target_markets"],
        max_markets_per_run=market_count,
        submarket_max_markets_per_run=effective_discovery["submarket_max_markets_per_run"],
        game_winner_max_markets_per_run=effective_discovery["game_winner_max_markets_per_run"],
        map_winner_max_markets_per_run=effective_discovery["map_winner_max_markets_per_run"],
        market_offset=market_offset,
        league_target_markets=(
            {
                "nba": effective_limits.get("sports_nba_target_markets", args.sports_nba_target_markets),
                "ufc": effective_limits.get("sports_ufc_target_markets", args.sports_ufc_target_markets),
            }
            if category == "sports"
            else None
        ),
        league_min_market_volumes=(
            {"nba": args.sports_nba_min_market_volume, "ufc": args.sports_ufc_min_market_volume}
            if category == "sports"
            else None
        ),
        league_fallback_min_market_volumes=(
            {"nba": args.sports_nba_fallback_min_market_volume, "ufc": args.sports_ufc_fallback_min_market_volume}
            if category == "sports"
            else None
        ),
    )
    write_json(data_dir / "discovery_slate.json", discovery_slate)
    mark_stage("discovery_slate")

    trades_by_market: dict[str, list[dict]] = {}
    holders_by_market: dict[str, list[dict]] = {}
    prices_by_market: dict[str, list[float]] = {}
    partial_markets = []
    market_trades_cache_hits = 0
    market_trades_api_fetches = 0
    if args.discovery_source == "holders":
        def fetch_holders_for_market(market: dict[str, Any]) -> tuple[str, list[dict], list[float], bool]:
            condition_id = market["condition_id"]
            try:
                return condition_id, client.holders(condition_id, limit=args.holders_limit), market.get("outcome_prices") or [], False
            except Exception:
                return condition_id, [], market.get("outcome_prices") or [], True

        holder_results = run_ordered_io_tasks(
            discovery_slate,
            fetch_holders_for_market,
            max_workers=args.max_workers,
        )
        for result in holder_results:
            if isinstance(result, Exception):
                continue
            condition_id, holders, prices, partial = result
            holders_by_market[condition_id] = holders
            prices_by_market[condition_id] = prices
            if partial:
                partial_markets.append(condition_id)
        candidates = build_candidate_wallets_from_holders(
            holders_by_market,
            prices_by_market,
            participation_threshold=args.participation_threshold,
            top_participation_count=args.top_participation_count,
            total_usd_threshold=args.total_cash_threshold,
            single_market_usd_threshold=args.single_market_cash_threshold,
            max_candidate_wallets=args.max_candidate_wallets,
        )
    else:
        def fetch_trades_for_market(market: dict[str, Any]) -> tuple[str, list[dict], bool, str]:
            condition_id = market["condition_id"]
            try:
                trades, partial, trades_source = fetch_market_trades_cached(
                    client,
                    condition_id,
                    data_dir=data_dir,
                    now_ts=now_ts,
                    page_limit=args.trades_page_limit,
                    max_pages=args.max_pages_per_market,
                    min_trade_cash=args.min_trade_cash,
                    cache_ttl_days=args.market_trades_cache_ttl_days,
                    force_refresh=args.refresh_market_trades,
                    use_cache=not args.no_market_trades_cache,
                )
                return condition_id, trades, partial, trades_source
            except Exception:
                return condition_id, [], True, "error"

        trade_results = run_ordered_io_tasks(
            discovery_slate,
            fetch_trades_for_market,
            max_workers=args.max_workers,
        )
        for result in trade_results:
            if isinstance(result, Exception):
                continue
            condition_id, trades, partial, trades_source = result
            trades_by_market[condition_id] = trades
            if partial:
                partial_markets.append(condition_id)
            if trades_source == "cache":
                market_trades_cache_hits += 1
            else:
                market_trades_api_fetches += 1
        market_end_times = {}
        market_start_times = {}
        for market in discovery_slate:
            end_dt = parse_dt(market.get("end_date"))
            if end_dt:
                market_end_times[market["condition_id"]] = int(end_dt.timestamp())
            start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
            if start_dt:
                market_start_times[market["condition_id"]] = int(start_dt.timestamp())
        candidates = build_candidate_wallets(
            trades_by_market,
            market_type_by_id={
                str(market.get("condition_id") or "").lower(): str(market.get("market_type") or "main_match")
                for market in discovery_slate
                if market.get("condition_id")
            },
            market_game_family_by_id={
                str(market.get("condition_id") or "").lower(): str(market.get("game_family") or "")
                for market in discovery_slate
                if market.get("condition_id") and market.get("game_family")
            },
            market_end_times=market_end_times,
            market_start_times=market_start_times,
            min_trade_cash=args.min_trade_cash,
            participation_threshold=args.participation_threshold,
            top_participation_count=args.top_participation_count,
            total_cash_threshold=args.total_cash_threshold,
            single_market_cash_threshold=args.single_market_cash_threshold,
            max_candidate_wallets=args.max_candidate_wallets,
            candidate_wallets_per_market_type=(
                ESPORTS_DEFAULT_CANDIDATE_WALLETS_PER_MARKET_TYPE if category == "esports" else None
            ),
            candidate_wallets_per_game_family=(
                ESPORTS_DEFAULT_CANDIDATE_WALLETS_PER_MARKET_TYPE if category == "esports" else None
            ),
            candidate_game_family_thresholds=(
                ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS if category == "esports" else None
            ),
        )
    mark_stage("market_trades_fetch")
    profile_candidates = filter_profile_candidates(
        candidates,
        min_participated_markets=effective_defaults["min_profile_participated_markets"],
        min_avg_market_cash=args.min_profile_avg_market_cash,
        require_clean_discovery=not args.allow_dirty_profile_candidates,
        market_type_thresholds=ESPORTS_CANDIDATE_MARKET_TYPE_THRESHOLDS if category == "esports" else None,
        game_family_thresholds=ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS if category == "esports" else None,
    )
    write_json(data_dir / "candidate_wallets.json", candidates)
    write_json(data_dir / "profile_candidate_wallets.json", profile_candidates)
    mark_stage("candidate_filtering")

    existing_profiles = {
        normalize_wallet(row.get("wallet")): row for row in read_json(data_dir / "wallet_profiles.json", [])
    }
    condition_ids = {row["condition_id"] for row in classification_set}
    market_records_by_id = {
        str(row.get("condition_id") or "").lower(): row
        for row in classification_set
        if row.get("condition_id")
    }
    condition_type_by_id = {
        str(row.get("condition_id") or "").lower(): str(row.get("market_type") or "main_match")
        for row in classification_set
        if row.get("condition_id")
    }
    condition_game_family_by_id = {
        str(row.get("condition_id") or "").lower(): str(row.get("game_family") or "unknown")
        for row in classification_set
        if row.get("condition_id")
    }
    max_profiles_per_run_effective = effective_limits.get("max_profiles_per_run", args.max_profiles_per_run)
    profile_fetch_plan = build_profile_fetch_plan(
        profile_candidates,
        existing_profiles,
        now_ts=now_ts,
        ttl_seconds=args.profile_refresh_ttl_days * 86400,
        max_profiles=max_profiles_per_run_effective,
    )

    def fetch_raw_user_trades_for_candidate(candidate: dict[str, Any]) -> tuple[str, list[dict]]:
        wallet = normalize_wallet(candidate.get("wallet"))
        trades = fetch_recent_user_trades_for_wallet(
            client,
            wallet,
            page_limit=args.user_history_trades_limit,
            max_pages=effective_limits.get("user_history_trades_max_pages", args.user_history_trades_max_pages),
            data_dir=data_dir,
            now_ts=now_ts,
            cache_ttl_days=args.market_trades_cache_ttl_days,
            force_refresh=args.refresh_market_trades,
            use_cache=not args.no_market_trades_cache,
        )
        return wallet, trades

    raw_user_trade_results = run_ordered_io_tasks(
        profile_fetch_plan,
        fetch_raw_user_trades_for_candidate,
        max_workers=args.max_workers,
    )
    raw_user_trades_by_wallet: dict[str, list[dict]] = {}
    for index, result in enumerate(raw_user_trade_results):
        fallback_wallet = normalize_wallet(profile_fetch_plan[index].get("wallet"))
        if isinstance(result, Exception):
            raw_user_trades_by_wallet[fallback_wallet] = []
            continue
        wallet, trades = result
        raw_user_trades_by_wallet[normalize_wallet(wallet)] = trades
    mark_stage("raw_user_trades_fetch")

    if category == "esports":
        backfilled_market_records, backfill_summary = backfill_user_trade_submarkets(
            client,
            raw_user_trades_by_wallet,
            market_records_by_id,
            data_dir=data_dir,
            now_ts=now_ts,
            cache_ttl_days=args.market_trades_cache_ttl_days,
            force_refresh=args.refresh_market_trades,
            use_cache=not args.no_market_trades_cache,
            max_workers=args.max_workers,
        )
    else:
        backfilled_market_records = {}
        backfill_summary = empty_user_trade_backfill_summary()
    if backfilled_market_records:
        market_records_by_id = {**market_records_by_id, **backfilled_market_records}
        condition_type_by_id = {
            **condition_type_by_id,
            **{
                condition_id: str(record.get("market_type") or "main_match")
                for condition_id, record in backfilled_market_records.items()
            },
        }
        condition_game_family_by_id = {
            **condition_game_family_by_id,
            **{
                condition_id: str(record.get("game_family") or "unknown")
                for condition_id, record in backfilled_market_records.items()
            },
        }
        condition_ids = set(market_records_by_id)
    mark_stage("submarket_backfill")

    def load_user_trades(wallet: str) -> list[dict]:
        # Per-user history is the accurate source: a per-market pool over the discovery
        # slate only covers markets we fetched and is taker-side only, which materially
        # undercounts a wallet's resolved markets (verified: 0xe16 80→39 markets, A→B).
        return _filter_esports_user_trades(
            raw_user_trades_by_wallet.get(normalize_wallet(wallet), []),
            condition_ids,
            max_esports_markets=effective_limits.get(
                "max_esports_closed_positions_per_wallet",
                args.max_esports_closed_positions_per_wallet,
            ),
        )

    def load_closed_positions(wallet: str) -> list[dict]:
        return fetch_recent_esports_closed_positions_for_wallet(
            client,
            wallet,
            condition_ids,
            max_esports_closed_positions=effective_limits.get(
                "max_esports_closed_positions_per_wallet",
                args.max_esports_closed_positions_per_wallet,
            ),
            market_chunk_size=args.closed_position_market_chunk_size,
        )

    def profile_one_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            return profile_candidate_wallet(
                candidate,
                condition_ids,
                market_records_by_id=market_records_by_id,
                condition_type_by_id=condition_type_by_id,
                condition_game_family_by_id=condition_game_family_by_id,
                user_trades_loader=load_user_trades,
                closed_positions_loader=load_closed_positions,
                current_positions_loader=(
                    (lambda w: client.positions(w, limit=100))
                    if args.check_current_positions
                    else (lambda w: [])
                ),
                now_ts=now_ts,
            )
        except Exception as exc:
            return make_retryable_profile(candidate, exc, now_ts=now_ts)

    profile_results = run_ordered_io_tasks(
        profile_fetch_plan,
        profile_one_candidate,
        max_workers=args.max_workers,
    )
    profiles = [
        make_retryable_profile(profile_fetch_plan[index], result, now_ts=now_ts)
        if isinstance(result, Exception)
        else result
        for index, result in enumerate(profile_results)
    ]
    profiled_count = len(profile_fetch_plan)
    mark_stage("wallet_profiling")

    profiles_by_wallet = {
        normalize_wallet(row.get("wallet")): row
        for row in [*existing_profiles.values(), *profiles]
        if normalize_wallet(row.get("wallet"))
    }
    if category == "sports":
        profiles_by_wallet = {
            wallet: augment_profile_sports_league_fields(profile, market_records_by_id)
            for wallet, profile in profiles_by_wallet.items()
        }
    profiles_by_wallet = merge_profiles_with_candidates(profiles_by_wallet, profile_candidates)
    if category == "esports":
        refreshed_profiles: dict[str, dict[str, Any]] = {}
        for wallet, profile in profiles_by_wallet.items():
            raw_trades = raw_user_trades_by_wallet.get(wallet)
            if raw_trades is None:
                cached = read_json(user_trades_cache_path(data_dir, wallet), {})
                raw_trades = cached.get("trades") if isinstance(cached, dict) else []
            refreshed_profiles[wallet] = merge_recent_trade_metrics_into_profile(
                profile,
                raw_trades or [],
                market_records_by_id,
                now_ts=now_ts,
            )
        profiles_by_wallet = refreshed_profiles
    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        min_participated_markets=effective_defaults["leaderboard_min_participated_markets"],
        min_avg_market_cash=args.leaderboard_min_avg_market_cash,
        require_tail_entry_field=True,
        require_current_scoring_version=True,
        max_leaderboard_wallets=args.max_leaderboard_wallets,
        min_pre_match_entry_rate=getattr(args, "min_pre_match_entry_rate", 0.0),
        league_event_counts=league_event_counts if category == "sports" else None,
    )
    overlap_report = build_wallet_overlap_report(leaderboard)
    diagnostics = build_collection_diagnostics(
        discovery_slate=discovery_slate,
        candidates=candidates,
        profile_candidates=profile_candidates,
        profiles_by_wallet=profiles_by_wallet,
        leaderboard=leaderboard,
        now_ts=now_ts,
        stage_timings=stage_timings,
    )
    mark_stage("leaderboard_build")
    diagnostics["stage_timings"] = {
        key: round(float(value), 6)
        for key, value in sorted(stage_timings.items())
    }
    profiles_by_wallet = prune_profile_store(
        profiles_by_wallet,
        now_ts=now_ts,
        max_age_days=args.profile_store_max_age_days,
    )
    write_json(data_dir / "wallet_profiles.json", list(profiles_by_wallet.values()))
    LeaderboardStore(data_dir / "leaderboard.db").replace_leaderboard(
        leaderboard,
        category=category,
        updated_at=now_ts,
    )
    write_json(data_dir / "leaderboard_wallet_overlap.json", overlap_report)

    summary = {
        "category": category,
        "market_scope": market_scope,
        "classification_market_count": len(classification_set),
        "eligible_event_count_by_league": league_event_counts if category == "sports" else {},
        "classification_source": classification_source,
        "discovery_market_count": len(discovery_slate),
        "candidate_wallet_count": len(candidates),
        "profile_candidate_wallet_count": len(profile_candidates),
        "profiled_wallet_count": profiled_count,
        **build_profile_budget_summary(
            profile_candidate_wallet_count=len(profile_candidates),
            profile_fetch_plan_count=len(profile_fetch_plan),
            max_profiles_per_run_effective=max_profiles_per_run_effective,
        ),
        **backfill_summary,
        "leaderboard_wallet_count": len(leaderboard),
        "leaderboard_union_market_count": overlap_report["union_market_count"],
        "leaderboard_pair_overlap_count": len(overlap_report["pair_overlaps"]),
        "market_trades_cache_hits": market_trades_cache_hits,
        "market_trades_api_fetches": market_trades_api_fetches,
        "partial_market_trades": partial_markets,
        "discovery_source": args.discovery_source,
        "slate": slate_meta,
        "diagnostics": diagnostics,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(data_dir / "build_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    print()
    category_label = "sports" if category == "sports" else "esports"
    print("搜集完成")
    print(f"- 历史 {category_label} 胜负市场: {summary['classification_market_count']}")
    print(f"- 本轮发现市场: {summary['discovery_market_count']}")
    print(f"- 候选钱包: {summary['candidate_wallet_count']}")
    print(f"- 通过快速过滤的钱包: {summary['profile_candidate_wallet_count']}")
    print(f"- 本轮深度分析钱包: {summary['profiled_wallet_count']}")
    print(f"- A/B 优质钱包: {summary['leaderboard_wallet_count']}")
    print(f"- 输出目录: {data_dir}")
    return 0


def find_active_market(client: PolymarketClient, args: argparse.Namespace) -> dict[str, Any] | None:
    active_events = client.list_events_paginated(
        closed=False,
        active=True,
        max_pages=args.gamma_pages,
        order="volume24hr",
    )
    records = []
    for event in active_events:
        record = event_to_market_record(event)
        if not record:
            continue
        if args.event_slug and record.get("event_slug") != args.event_slug:
            continue
        if args.condition_id and record.get("condition_id") != args.condition_id.lower():
            continue
        end = record.get("end_date")
        end_dt = parse_dt(end) if end else None
        if not args.event_slug and not args.condition_id:
            if not end_dt:
                continue
            hours = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if not (1 <= hours <= 24):
                continue
        records.append(record)
    if not records:
        return None
    return max(records, key=lambda row: to_float(row.get("volume24hr")) * 0.7 + to_float(row.get("liquidity")) * 0.3)


def command_analyze_event(args: argparse.Namespace) -> int:
    client = build_client(args)
    data_dir = resolve_data_dir(args)
    leaderboard_rows, _mtimes = read_category_leaderboards(data_dir)
    leaderboard = {normalize_wallet(row.get("wallet")): row for row in leaderboard_rows}

    market = find_active_market(client, args)
    if not market:
        raise SystemExit("No matching active esports market found.")

    holders = client.holders(market["condition_id"], limit=args.holders_limit)
    outcomes = market.get("outcomes") or []
    prices = market.get("outcome_prices") or [0.0 for _ in outcomes]
    result = analyze_holders(holders, leaderboard, outcomes=outcomes, outcome_prices=prices)
    output = {
        "event_title": market.get("title"),
        "market_question": market.get("question"),
        "condition_id": market.get("condition_id"),
        "hours_to_end": None,
        "outcomes": outcomes,
        "current_prices": prices,
        **result,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    end_dt = parse_dt(market.get("end_date"))
    if end_dt:
        output["hours_to_end"] = round((end_dt - datetime.now(timezone.utc)).total_seconds() / 3600, 2)
    write_json(data_dir / "last_event_analysis.json", output)
    print(json.dumps(output, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def command_follow(
    args: argparse.Namespace,
    client: PolymarketClient | None = None,
    *,
    emit: bool = True,
) -> dict[str, Any]:
    if args.execution_mode != "paper":
        raise SystemExit("Only paper execution mode is implemented.")
    client = client or build_client(args)
    data_dir = resolve_dashboard_root(args)
    follow_dir = resolve_follow_dir(args, data_dir)
    now_ts = int(datetime.now(timezone.utc).timestamp())

    state_path = follow_dir / "follow_state.json"
    open_path = follow_dir / "follow_signals_open.json"
    perf_path = follow_dir / "follow_performance.json"
    results_path = follow_dir / "follow_results.jsonl"
    run_log_path = follow_run_log_path(data_dir, getattr(args, "log_dir", None))
    active_cache_path = follow_dir / "active_market_cache.json"
    migration_summary = migrate_category_follow_dbs(data_dir, follow_dir, now_ts=now_ts)
    store = FollowStore(follow_dir / "follow.db")
    store.import_legacy_json(
        state_path=state_path,
        open_path=open_path,
        results_path=results_path,
        perf_path=perf_path,
    )
    leaderboard_rows, leaderboard_mtimes = read_category_leaderboards(data_dir)
    leaderboard_wallets = {
        f"{str(row.get('category') or 'esports').lower()}:{normalize_wallet(row.get('wallet'))}"
        for row in leaderboard_rows
        if normalize_wallet(row.get("wallet"))
    }
    leaderboard_validated_at = max(leaderboard_mtimes.values() or [0])
    store.clear_revalidated_quarantine(leaderboard_wallets, validated_at=leaderboard_validated_at)
    quarantine_rows = store.load_wallet_quarantine()
    quarantined_wallets = set(quarantine_rows)
    eligible_wallet_rows = eligible_follow_wallets(
        leaderboard_rows,
        now_ts=now_ts,
        recency_days=args.follow_recency_days,
        quarantined_wallets=quarantined_wallets,
        allowed_categories=set(FOLLOW_SIGNAL_CATEGORIES),
    )

    state = read_json(state_path, {"wallet_trade_state": {}})
    eligible_market_types_by_wallet = {
        f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}": {str(value) for value in (row.get("eligible_market_types") or []) if value}
        for row in eligible_wallet_rows
    }
    eligible_buckets_by_wallet = {
        f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}": {str(value) for value in (row.get("eligible_buckets") or []) if value}
        for row in eligible_wallet_rows
        if row.get("eligible_buckets")
    }
    eligible_leagues_by_wallet = {
        f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}": {str(row.get("league") or "").lower()}
        for row in eligible_wallet_rows
        if str(row.get("category") or "esports").lower() == "sports" and str(row.get("league") or "").strip()
    }
    wallet_trade_state = store.load_wallet_trade_state()
    open_signals = store.load_open_signals()
    performance = store.load_performance()
    control = read_follow_control(follow_dir)
    pause_new_signals = control.get("pause_new_signals") if isinstance(control.get("pause_new_signals"), dict) else {}
    paused_new_signal_categories = {
        str(category).lower()
        for category, status in pause_new_signals.items()
        if isinstance(status, dict) and status.get("status") == "paused"
    }
    open_condition_ids_by_wallet: dict[str, set[str]] = {}
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        wallet = normalize_wallet(signal.get("wallet"))
        category = str(signal.get("category") or "esports").lower()
        if category not in FOLLOW_SIGNAL_CATEGORIES:
            continue
        scope_key = f"{category}:{wallet}"
        condition_id = str(signal.get("condition_id") or "").lower()
        if wallet and condition_id:
            open_condition_ids_by_wallet.setdefault(scope_key, set()).add(condition_id)
    eligible_wallet_set = {f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}" for row in eligible_wallet_rows}
    lifecycle_wallets = sorted(set(open_condition_ids_by_wallet) - eligible_wallet_set)
    follow_wallets = [
        *({**row, "scope_key": f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}"} for row in eligible_wallet_rows),
        *(
            {
                "wallet": wallet.split(":", 1)[1],
                "category": wallet.split(":", 1)[0],
                "scope_key": wallet,
                "follow_scope": "open_signals",
            }
            for wallet in lifecycle_wallets
        ),
    ]
    open_condition_ids = {
        condition_id
        for condition_ids in open_condition_ids_by_wallet.values()
        for condition_id in condition_ids
    }

    active_markets, state, active_source = load_active_market_cache(
        client,
        state,
        cache_path=active_cache_path,
        now_ts=now_ts,
        gamma_pages=args.gamma_pages,
        ttl_seconds=args.event_cache_ttl_minutes * 60,
        observe_window_hours=args.observe_window_hours,
        post_start_grace_seconds=args.post_start_trade_grace_seconds,
        allowed_categories=set(FOLLOW_SIGNAL_CATEGORIES),
        preserve_condition_ids=set(open_condition_ids),
    )
    active_markets_for_follow = {
        condition_id: market
        for condition_id, market in active_markets.items()
        if str(market.get("category") or "esports").lower() in FOLLOW_SIGNAL_CATEGORIES
    }
    try:
        refresh_team_logo_cache_from_active_markets(
            data_dir,
            timeout_seconds=4,
            max_workers=min(max(1, int(args.max_workers)), 4),
            max_events=40,
            observe_window_hours=args.observe_window_hours,
            now_ts=now_ts,
        )
    except Exception:
        pass
    watched = watched_markets(
        active_markets_for_follow,
        now_ts=now_ts,
        observe_window_hours=args.observe_window_hours,
        post_start_grace_seconds=args.post_start_trade_grace_seconds,
    )
    gate_open = bool(watched or open_signals)
    next_interval = desired_tick_interval(
        list(watched.values()),
        open_signals,
        now_ts=now_ts,
        observe_window_hours=args.observe_window_hours,
        min_tick_seconds=args.min_tick_seconds,
        max_tick_seconds=args.max_tick_seconds,
        fixed_tick_seconds=getattr(args, "tick_seconds", 0),
    )

    wallet_trade_state = dict(wallet_trade_state or state.get("wallet_trade_state") or {})
    total_new_trade_count = 0
    watched_new_trade_count = 0
    ignored_trade_count = 0
    new_signal_count = 0
    exited_signal_count = 0
    hedge_event_count = 0
    quarantine_event_count = 0
    market_type_not_eligible_count = 0
    opposite_blocked_count = 0
    contested_signal_count = 0
    closing_line_snapshot_count = 0
    cold_start_wallet_count = 0
    bootstrap_position_count = 0
    bootstrap_position_request_count = 0
    trade_request_count = 0
    insufficient_balance_count = 0
    low_entry_price_blocked_count = 0

    tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
    tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
    markets_for_follow = {
        condition_id: market
        for condition_id, market in active_markets_for_follow.items()
        if condition_id in tracked_condition_ids or condition_id in watched
    }

    if gate_open and follow_wallets:
        def fetch_trades_for_wallet(row: dict[str, Any]) -> tuple[str, list[dict], list[dict]]:
            wallet = normalize_wallet(row.get("wallet"))
            scope_key = str(row.get("scope_key") or f"{str(row.get('category') or 'esports').lower()}:{wallet}")
            previous_cursor = (wallet_trade_state.get(scope_key) or wallet_trade_state.get(wallet) or {}).get("last_trade_cursor")
            try:
                trades = fetch_user_trades_until_cursor(
                    client,
                    wallet,
                    previous_cursor=previous_cursor,
                    limit=args.user_trades_limit,
                    max_pages=args.user_trades_max_pages,
                )
                positions = []
                if scope_key in eligible_wallet_set and previous_cursor is None and args.bootstrap_current_positions:
                    positions = client.positions(wallet, limit=args.positions_limit)
                return scope_key, trades, positions
            except Exception:
                return scope_key, [], []

        trade_results = run_ordered_io_tasks(
            follow_wallets,
            fetch_trades_for_wallet,
            max_workers=args.max_workers,
        )
        trade_request_count = len(follow_wallets)
        for result in trade_results:
            if isinstance(result, Exception):
                continue
            scope_key, trades, positions = result
            category, wallet = scope_key.split(":", 1)
            wallet_can_open_new = scope_key in eligible_wallet_set and category not in paused_new_signal_categories
            previous_cursor = (wallet_trade_state.get(scope_key) or wallet_trade_state.get(wallet) or {}).get("last_trade_cursor")
            new_trades, next_cursor, cold_start = select_new_trades(trades, previous_cursor)
            if cold_start:
                cold_start_wallet_count += 1
                if wallet_can_open_new and args.bootstrap_current_positions:
                    bootstrap_position_request_count += 1
                if positions:
                    bootstrap_trades = bootstrap_position_trades(
                        positions,
                        wallet=wallet,
                        markets_by_condition=watched,
                        now_ts=now_ts,
                        max_slippage=args.max_slippage_over_entry,
                        min_wallet_entry_price=args.min_wallet_entry_price,
                        eligible_market_types=eligible_market_types_by_wallet.get(scope_key),
                        eligible_buckets=eligible_buckets_by_wallet.get(scope_key),
                        eligible_category=category,
                        eligible_leagues=eligible_leagues_by_wallet.get(scope_key),
                        require_pre_match=args.require_pre_match,
                    )
                    before_ids = {signal.get("signal_id") for signal in open_signals}
                    open_signals, stats = process_follow_trades(
                        open_signals,
                        wallet=wallet,
                        trades=bootstrap_trades,
                        markets_by_condition=markets_for_follow or watched,
                        now_ts=now_ts,
                        stake_usdc=args.stake_usdc,
                        max_follow_legs=args.max_follow_legs,
                        max_slippage=args.max_slippage_over_entry,
                        min_wallet_entry_price=args.min_wallet_entry_price,
                        stake_ratio_percent=args.stake_ratio_percent,
                        require_pre_match=args.require_pre_match,
                        post_start_grace_seconds=args.post_start_trade_grace_seconds,
                        quarantine_sell_frac=args.quarantine_sell_frac,
                        eligible_market_types=eligible_market_types_by_wallet.get(scope_key),
                        eligible_buckets=eligible_buckets_by_wallet.get(scope_key),
                        eligible_category=category,
                        eligible_leagues=eligible_leagues_by_wallet.get(scope_key),
                        conflict_policy=args.conflict_policy,
                        bankroll_usdc=args.bankroll_usdc,
                    )
                    after_ids = {signal.get("signal_id") for signal in open_signals}
                    new_signal_count += len(after_ids - before_ids)
                    bootstrap_position_count += len(bootstrap_trades)
                    for event in stats.get("quarantine_events") or []:
                        store.upsert_wallet_quarantine(event.get("wallet"), reason=str(event.get("reason") or ""), ts=int(event.get("timestamp") or now_ts), category=str(event.get("category") or category))
                        quarantine_event_count += 1
                    market_type_not_eligible_count += stats.get("market_type_not_eligible_count", 0)
                    ignored_trade_count += stats.get("ignored_trade_count", 0)
                    insufficient_balance_count += stats.get("insufficient_balance_count", 0)
                    low_entry_price_blocked_count += stats.get("low_entry_price_blocked_count", 0)
                    exited_signal_count += stats.get("exited_signal_count", 0)
                    hedge_event_count += stats.get("hedge_event_count", 0)
                    opposite_blocked_count += stats.get("opposite_blocked_count", 0)
                wallet_trade_state[scope_key] = {
                    "last_trade_cursor": next_cursor,
                    "last_seen_at": now_ts,
                    "wallet": wallet,
                    "category": category,
                }
                continue
            tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
            tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
            markets_for_follow = {
                condition_id: market
                for condition_id, market in active_markets_for_follow.items()
                if condition_id in tracked_condition_ids or condition_id in watched
            }
            if wallet_can_open_new:
                wallet_tracked_condition_ids = tracked_condition_ids
            else:
                wallet_tracked_condition_ids = open_condition_ids_by_wallet.get(scope_key, set())
            watched_trades = [trade for trade in new_trades if trade_condition_id(trade) in wallet_tracked_condition_ids]
            before_ids = {signal.get("signal_id") for signal in open_signals}
            open_signals, stats = process_follow_trades(
                open_signals,
                wallet=wallet,
                trades=watched_trades,
                markets_by_condition=markets_for_follow or watched,
                now_ts=now_ts,
                stake_usdc=args.stake_usdc,
                max_follow_legs=args.max_follow_legs,
                max_slippage=args.max_slippage_over_entry,
                min_wallet_entry_price=args.min_wallet_entry_price,
                stake_ratio_percent=args.stake_ratio_percent,
                require_pre_match=args.require_pre_match,
                post_start_grace_seconds=args.post_start_trade_grace_seconds,
                quarantine_sell_frac=args.quarantine_sell_frac,
                eligible_market_types=eligible_market_types_by_wallet.get(scope_key) if wallet_can_open_new else None,
                eligible_buckets=eligible_buckets_by_wallet.get(scope_key) if wallet_can_open_new else None,
                eligible_category=category if wallet_can_open_new else None,
                eligible_leagues=eligible_leagues_by_wallet.get(scope_key) if wallet_can_open_new else None,
                conflict_policy=args.conflict_policy,
                bankroll_usdc=args.bankroll_usdc,
            )
            after_ids = {signal.get("signal_id") for signal in open_signals}
            new_signal_count += len(after_ids - before_ids)
            total_new_trade_count += len(new_trades)
            watched_new_trade_count += len(watched_trades)
            ignored_trade_count += stats.get("ignored_trade_count", 0) + (len(new_trades) - len(watched_trades))
            insufficient_balance_count += stats.get("insufficient_balance_count", 0)
            market_type_not_eligible_count += stats.get("market_type_not_eligible_count", 0)
            low_entry_price_blocked_count += stats.get("low_entry_price_blocked_count", 0)
            exited_signal_count += stats.get("exited_signal_count", 0)
            hedge_event_count += stats.get("hedge_event_count", 0)
            opposite_blocked_count += stats.get("opposite_blocked_count", 0)
            for event in stats.get("quarantine_events") or []:
                store.upsert_wallet_quarantine(event.get("wallet"), reason=str(event.get("reason") or ""), ts=int(event.get("timestamp") or now_ts), category=str(event.get("category") or category))
                quarantine_event_count += 1
            wallet_trade_state[scope_key] = {
                "last_trade_cursor": next_cursor,
                "last_seen_at": now_ts,
                "wallet": wallet,
                "category": category,
            }

    state["wallet_trade_state"] = wallet_trade_state
    state["updated_at"] = now_ts

    open_signals, clv_stats = apply_closing_line_snapshots(open_signals, active_markets, now_ts=now_ts)
    closing_line_snapshot_count += clv_stats.get("closing_line_snapshot_count", 0)
    if args.consensus_block_opposite:
        contested_condition_ids = contested_markets(open_signals, now_ts=now_ts)
        open_signals, contested_stats = apply_contested_flags(open_signals, contested_condition_ids, now_ts=now_ts)
        contested_signal_count += contested_stats.get("contested_signal_count", 0)

    exited_signals = [signal for signal in open_signals if signal.get("status") == "exited"]
    open_signals = [signal for signal in open_signals if signal.get("status") != "exited"]
    resolutions = fetch_resolutions_for_open_signals(
        client,
        open_signals,
        state=state,
        store=store,
        now_ts=now_ts,
        gamma_pages=args.resolution_gamma_pages,
        ttl_seconds=args.resolution_cache_ttl_seconds,
    )
    open_signals, settled = settle_open_signals(open_signals, resolutions, now_ts=now_ts)
    result_events = [*exited_signals, *settled]
    if result_events:
        performance = aggregate_follow_performance(performance, result_events)
    else:
        performance = aggregate_follow_performance(performance, [])
    existing_quarantine = set(store.load_wallet_quarantine())
    for event in observed_performance_quarantine_events(performance, now_ts=now_ts):
        category = str(event.get("category") or "esports").lower()
        key = f"{category}:{event['wallet']}"
        if key in existing_quarantine:
            continue
        store.upsert_wallet_quarantine(event["wallet"], reason=event["reason"], ts=int(event["timestamp"]), category=category)
        existing_quarantine.add(key)
        quarantine_event_count += 1

    run_log_row = {
        "created_at": now_ts,
        "follow_wallet_count": len(follow_wallets),
        "eligible_follow_wallet_count": len(eligible_wallet_rows),
        "lifecycle_follow_wallet_count": len(lifecycle_wallets),
        "gate_open": gate_open,
        "active_market_source": active_source,
        "watched_market_count": len(watched),
        "trade_request_count": trade_request_count,
        "bootstrap_position_request_count": bootstrap_position_request_count,
        "cold_start_wallet_count": cold_start_wallet_count,
        "bootstrap_position_count": bootstrap_position_count,
        "total_new_trade_count": total_new_trade_count,
        "watched_new_trade_count": watched_new_trade_count,
        "new_trade_count": watched_new_trade_count,
        "ignored_trade_count": ignored_trade_count,
        "insufficient_balance_count": insufficient_balance_count,
        "market_type_not_eligible_count": market_type_not_eligible_count,
        "low_entry_price_blocked_count": low_entry_price_blocked_count,
        "opposite_blocked_count": opposite_blocked_count,
        "new_signal_count": new_signal_count,
        "exited_signal_count": exited_signal_count,
        "hedge_event_count": hedge_event_count,
        "quarantine_event_count": quarantine_event_count,
        "contested_signal_count": contested_signal_count,
        "closing_line_snapshot_count": closing_line_snapshot_count,
        "open_signal_count": len(open_signals),
        "settled_signal_count": len(settled),
        "desired_next_interval_seconds": next_interval,
        "migration": migration_summary,
        "by_category": {
            category: {
                "eligible_follow_wallet_count": sum(1 for row in eligible_wallet_rows if str(row.get("category") or "esports").lower() == category),
                "watched_market_count": sum(1 for market in watched.values() if str(market.get("category") or "esports").lower() == category),
                "open_signal_count": sum(1 for signal in open_signals if str(signal.get("category") or "esports").lower() == category),
            }
            for category in ("esports", "sports")
        },
    }
    run_log_rows = read_jsonl(run_log_path)
    from .follow import prune_jsonl

    run_log_rows = prune_jsonl([*run_log_rows, run_log_row], now_ts=now_ts, retention_days=args.run_log_retention_days)
    store.save_follow_snapshot(
        wallet_trade_state=wallet_trade_state,
        open_signals=open_signals,
        result_events=result_events,
        performance=performance,
    )
    state = {
        "updated_at": now_ts,
        "db_path": str(follow_dir / "follow.db"),
        "active_market_cache_path": str(active_cache_path),
        "schema_version": 1,
    }
    write_json(state_path, state)
    write_jsonl(run_log_path, run_log_rows)

    summary = {
        **run_log_row,
        "settled_result_count_total": len(store.load_results()),
        "output_dir": str(follow_dir),
    }
    if emit:
        print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
    return summary


def command_run(args: argparse.Namespace) -> int:
    if args.execution_mode != "paper":
        raise SystemExit("Only paper execution mode is implemented.")
    client = build_client(args)
    now_init = int(datetime.now(timezone.utc).timestamp())
    last_build_at = now_init if args.skip_initial_build else 0
    tick_count = 0
    first_error_at: float | None = None
    stop_requested = {"value": False, "reason": ""}
    collection_root = resolve_dashboard_root(args)
    follow_dir = resolve_follow_dir(args, collection_root)

    def category_args(category: str) -> argparse.Namespace:
        return argparse.Namespace(
            **{
                **vars(args),
                "category": category,
                "data_dir": str(category_data_dirs(collection_root)[category]),
            }
        )

    def request_stop(signum, _frame) -> None:
        stop_requested["value"] = True
        stop_requested["reason"] = f"signal_{signum}"

    previous_sigterm = None
    try:
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, request_stop)
    except (AttributeError, ValueError):
        previous_sigterm = None

    def sleep_or_stop(seconds: int) -> None:
        if not stop_requested["value"]:
            time.sleep(max(1, int(seconds)))

    def maybe_build(force: bool = False) -> bool:
        nonlocal last_build_at
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if force or now_ts - last_build_at >= int(args.pool_refresh_hours * 3600):
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(
                    follow_dir,
                    category,
                    {"status": "paused", "reason": "pool_refresh", "started_at": now_ts},
                )
                try:
                    category_dir = category_data_dirs(collection_root)[category]
                    prepare_category_refresh_dir(
                        category_dir,
                        max_lookback_days=category_refresh_cache_retention_days(args),
                        now_ts=now_ts,
                        cache_retention_days_by_dir=category_refresh_cache_retention_days_by_dir(args),
                    )
                    command_collect(category_args(category), client=client)
                finally:
                    set_pause_new_signals(follow_dir, category, None)
            now_done = int(datetime.now(timezone.utc).timestamp())
            last_build_at = now_done
            return True
        return False

    try:
        if not args.skip_initial_build:
            try:
                maybe_build(force=True)
            except Exception as exc:
                first_error_at = time.time()
                print(
                    json.dumps(
                        {
                            "status": "run_iteration_error",
                            "phase": "initial_build",
                            "error": str(exc),
                            "sleep_seconds": int(args.error_retry_seconds),
                        },
                        ensure_ascii=False,
                    )
                )
                sleep_or_stop(int(args.error_retry_seconds))
        while not stop_requested["value"]:
            try:
                maybe_build(force=False)
                summary = command_follow(args, client=client, emit=True)
            except Exception as exc:
                now = time.time()
                if first_error_at is None:
                    first_error_at = now
                elapsed = now - first_error_at
                if args.max_consecutive_error_seconds > 0 and elapsed >= args.max_consecutive_error_seconds:
                    print(
                        json.dumps(
                            {
                                "status": "stopped",
                                "reason": "consecutive_errors",
                                "error": str(exc),
                                "elapsed_error_seconds": round(elapsed, 2),
                                "ticks": tick_count,
                            },
                            ensure_ascii=False,
                        )
                    )
                    return 2
                print(
                    json.dumps(
                        {
                            "status": "run_iteration_error",
                            "phase": "loop",
                            "error": str(exc),
                            "elapsed_error_seconds": round(elapsed, 2),
                            "sleep_seconds": int(args.error_retry_seconds),
                        },
                        ensure_ascii=False,
                    )
                )
                sleep_or_stop(int(args.error_retry_seconds))
                continue
            first_error_at = None
            tick_count += 1
            if args.max_run_ticks and tick_count >= args.max_run_ticks:
                break
            sleep_seconds = int(summary.get("desired_next_interval_seconds") or args.max_tick_seconds)
            sleep_or_stop(sleep_seconds)
    except KeyboardInterrupt:
        print(json.dumps({"status": "stopped", "reason": "keyboard_interrupt", "ticks": tick_count}, ensure_ascii=False))
    finally:
        if stop_requested["value"]:
            print(json.dumps({"status": "stopped", "reason": stop_requested["reason"], "ticks": tick_count}, ensure_ascii=False))
        if previous_sigterm is not None:
            try:
                signal.signal(signal.SIGTERM, previous_sigterm)
            except (AttributeError, ValueError):
                pass
    return 0


def command_serve(args: argparse.Namespace) -> int:
    from .dashboard import DashboardConfig, create_server

    password = os.environ.get("POLY_FIGHT_DASH_PASSWORD", "")
    cookie_secret = os.environ.get("POLY_FIGHT_DASH_COOKIE_SECRET", "")
    config = DashboardConfig(
        data_dir=resolve_dashboard_root(args),
        follow_dir=resolve_follow_dir(args, resolve_dashboard_root(args)),
        log_dir=Path(args.log_dir) if args.log_dir else None,
        host=args.host,
        port=args.port,
        username=args.user,
        password=password,
        cookie_secret=cookie_secret,
        session_ttl_seconds=args.session_ttl_seconds,
        cookie_secure=args.cookie_secure,
        client=build_client(args),
        observe_window_hours=args.observe_window_hours,
        runner_stake_usdc=args.runner_stake_usdc,
        runner_stake_ratio_percent=args.runner_stake_ratio_percent,
        stream_poll_seconds=args.stream_poll_seconds,
        stream_heartbeat_seconds=args.stream_heartbeat_seconds,
        max_stream_clients=args.max_stream_clients,
    )
    server = create_server(config)
    host, port = server.server_address[:2]
    print(f"dashboard listening on http://{host}:{port} data_dir={config.data_dir}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("dashboard stopped", flush=True)
    finally:
        server.server_close()
    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket esports smart-wallet analysis")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--log-dir")
    parser.add_argument("--timeout", type=int, default=30)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_build_arguments(subparser: argparse.ArgumentParser, *, include_category: bool = False) -> None:
        if include_category:
            subparser.add_argument("--category", choices=["esports", "sports"], default="esports")
        subparser.add_argument("--gamma-pages", type=int, default=10)
        subparser.add_argument("--refresh-classification", action="store_true")
        subparser.add_argument("--classification-cache-ttl-hours", type=int, default=24)
        subparser.add_argument("--classification-lookback-days", type=int, default=None)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)
        subparser.add_argument("--min-market-volume", type=float, default=25_000)
        subparser.add_argument("--sports-event-min-volume", type=float, default=50_000)
        subparser.add_argument("--sports-nba-target-markets", type=int, default=80)
        subparser.add_argument("--sports-ufc-target-markets", type=int, default=80)
        subparser.add_argument("--sports-nba-min-market-volume", type=float, default=250_000)
        subparser.add_argument("--sports-nba-fallback-min-market-volume", type=float, default=100_000)
        subparser.add_argument("--sports-ufc-min-market-volume", type=float, default=25_000)
        subparser.add_argument("--sports-ufc-fallback-min-market-volume", type=float, default=10_000)
        subparser.add_argument("--fallback-min-market-volume", type=float, default=10_000)
        subparser.add_argument("--submarket-min-market-volume", type=float, default=5_000)
        subparser.add_argument("--submarket-fallback-min-market-volume", type=float, default=1_000)
        subparser.add_argument("--target-markets", type=int, default=None)
        subparser.add_argument("--submarket-target-markets", type=int, default=None)
        subparser.add_argument("--game-winner-target-markets", type=int, default=None)
        subparser.add_argument("--map-winner-target-markets", type=int, default=None)
        subparser.add_argument("--max-markets-per-run", type=int)
        subparser.add_argument("--submarket-max-markets-per-run", type=int, default=None)
        subparser.add_argument("--game-winner-max-markets-per-run", type=int, default=None)
        subparser.add_argument("--map-winner-max-markets-per-run", type=int, default=None)
        subparser.add_argument("--market-batch-size", type=int, default=50)
        subparser.add_argument("--market-batch-count", type=int, default=2)
        subparser.add_argument("--market-batch-index", type=int)
        subparser.add_argument("--market-offset", type=int, default=0)
        subparser.add_argument("--trades-page-limit", type=int, default=1000)
        subparser.add_argument("--max-pages-per-market", type=int, default=3)
        subparser.add_argument("--market-trades-cache-ttl-days", type=int, default=7)
        subparser.add_argument("--refresh-market-trades", action="store_true")
        subparser.add_argument("--no-market-trades-cache", action="store_true")
        subparser.add_argument("--discovery-source", choices=["trades", "holders"], default="trades")
        subparser.add_argument("--holders-limit", type=int, default=20)
        subparser.add_argument("--min-trade-cash", type=float, default=50)
        subparser.add_argument("--participation-threshold", type=int, default=8)
        subparser.add_argument("--top-participation-count", type=int, default=100)
        subparser.add_argument("--total-cash-threshold", type=float, default=5_000)
        subparser.add_argument("--single-market-cash-threshold", type=float, default=1_000)
        subparser.add_argument("--max-candidate-wallets", type=int, default=1000)
        subparser.add_argument("--max-profiles-per-run", type=int, default=ESPORTS_DEFAULT_MAX_PROFILES_PER_RUN)
        subparser.add_argument("--max-esports-closed-positions-per-wallet", type=int, default=100)
        subparser.add_argument("--closed-position-market-chunk-size", type=int, default=50)
        subparser.add_argument("--user-history-trades-limit", type=int, default=500)
        # Fallback-only path (wallets not in the per-market pool); kept small on purpose —
        # 3 pages ≈ 1500 trades is plenty to cover a wallet's recent esports markets.
        subparser.add_argument("--user-history-trades-max-pages", type=int, default=3)
        subparser.add_argument("--min-profile-participated-markets", type=int, default=None)
        subparser.add_argument("--min-profile-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--leaderboard-min-participated-markets", type=int, default=None)
        subparser.add_argument("--leaderboard-min-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--max-leaderboard-wallets", type=int, default=30)
        # Soft followability filter: require this fraction of pre-match (before kickoff)
        # entries. 0 = off (default). Useful for sports where some alpha is in-game.
        subparser.add_argument("--min-pre-match-entry-rate", type=float, default=0.0)
        subparser.add_argument("--allow-dirty-profile-candidates", action="store_true")
        subparser.add_argument("--check-current-positions", action="store_true")
        subparser.add_argument("--profile-refresh-ttl-days", type=int, default=7)
        subparser.add_argument("--profile-store-max-age-days", type=int, default=180)
        subparser.set_defaults(func=command_build_leaderboard)

    def add_follow_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--follow-dir")
        subparser.add_argument("--stake-usdc", type=float, required=True, help="minimum paper stake per followed BUY leg")
        subparser.add_argument("--stake-ratio-percent", type=float, default=10.0, help="target-wallet cash replication ratio per BUY leg")
        subparser.add_argument("--bankroll-usdc", type=float, default=1000.0, help="paper bankroll cap for total open exposure")
        subparser.add_argument("--follow-recency-days", type=int, default=30)
        subparser.add_argument("--observe-window-hours", type=float, default=24)
        subparser.add_argument("--event-cache-ttl-minutes", type=int, default=10)
        subparser.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
        subparser.add_argument("--resolution-gamma-pages", type=int, default=2)
        subparser.add_argument("--max-slippage-over-entry", type=float, default=0.05)
        subparser.add_argument("--min-wallet-entry-price", type=float, default=0.4)
        subparser.add_argument("--post-start-trade-grace-seconds", type=int, default=900)
        subparser.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=False)
        subparser.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
        subparser.add_argument("--execution-mode", choices=["paper", "live"], default="paper")
        subparser.add_argument("--run-log-retention-days", type=int, default=7)
        subparser.add_argument("--gamma-pages", type=int, default=3)
        subparser.add_argument("--positions-limit", type=int, default=100)
        subparser.add_argument("--user-trades-limit", type=int, default=100)
        subparser.add_argument("--user-trades-max-pages", type=int, default=3)
        subparser.add_argument("--bootstrap-current-positions", dest="bootstrap_current_positions", action="store_true", default=True)
        subparser.add_argument("--no-bootstrap-current-positions", dest="bootstrap_current_positions", action="store_false")
        subparser.add_argument("--max-follow-legs", type=int, default=10)
        subparser.add_argument("--min-tick-seconds", type=int, default=180)
        subparser.add_argument("--max-tick-seconds", type=int, default=900)
        # Fixed polling cadence (seconds). >0 overrides the adaptive min/max curve so every
        # wallet is checked on one steady interval; 0 restores the start-time-aware backoff.
        subparser.add_argument("--tick-seconds", type=int, default=120)
        subparser.add_argument("--consensus-block-opposite", dest="consensus_block_opposite", action="store_true", default=True)
        subparser.add_argument("--no-consensus-block-opposite", dest="consensus_block_opposite", action="store_false")
        subparser.add_argument("--conflict-policy", choices=["dual_follow", "exit_on_opposite"], default="dual_follow")
        subparser.add_argument("--quarantine-sell-frac", type=float, default=0.2)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)

    def add_latest_collector_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--output-dir", default=None)
        subparser.add_argument("--lookback-days", type=int, default=30)
        subparser.add_argument("--bucket-market-limit", type=int, default=50)
        subparser.add_argument("--positions-per-market", type=int, default=20)
        subparser.add_argument("--seed-sort-by", choices=["TOTAL_PNL", "REALIZED_PNL", "CASH_PNL", "TOKENS"], default="TOTAL_PNL")
        subparser.add_argument("--max-profile-wallets", type=int, default=None)
        subparser.add_argument("--max-core-wallets", type=int, default=20)
        subparser.add_argument("--max-momentum-wallets", type=int, default=10)
        subparser.add_argument("--max-watchlist-wallets", type=int, default=50)
        subparser.add_argument("--max-esports-markets-per-wallet", type=int, default=100)
        subparser.add_argument("--user-trades-cache-ttl-days", type=int, default=1)
        subparser.add_argument("--refresh-user-trades", action="store_true")
        subparser.add_argument("--no-user-trades-cache", action="store_true")
        subparser.add_argument("--no-dashboard-publish", action="store_true")

    build = subparsers.add_parser("build-leaderboard")
    add_build_arguments(build, include_category=True)
    add_latest_collector_arguments(build)
    build.set_defaults(func=command_collect)

    collect = subparsers.add_parser("collect", help="one-shot wallet collection and leaderboard build")
    add_build_arguments(collect, include_category=True)
    add_latest_collector_arguments(collect)
    collect.set_defaults(func=command_collect)

    collect_v2 = subparsers.add_parser("collect-v2", help="experimental market-position wallet collector")
    collect_v2.add_argument("--category", choices=["esports"], default="esports")
    collect_v2.add_argument("--output-dir", default=None)
    collect_v2.add_argument("--gamma-pages", type=int, default=10)
    collect_v2.add_argument("--lookback-days", type=int, default=30)
    collect_v2.add_argument("--bucket-market-limit", type=int, default=50)
    collect_v2.add_argument("--positions-per-market", type=int, default=20)
    collect_v2.add_argument("--seed-sort-by", choices=["TOTAL_PNL", "REALIZED_PNL", "CASH_PNL", "TOKENS"], default="TOTAL_PNL")
    collect_v2.add_argument("--max-profile-wallets", type=int, default=500)
    collect_v2.add_argument("--max-leaderboard-wallets", type=int, default=30)
    collect_v2.add_argument("--user-history-trades-limit", type=int, default=500)
    collect_v2.add_argument("--user-history-trades-max-pages", type=int, default=8)
    collect_v2.add_argument("--user-trades-cache-ttl-days", type=int, default=1)
    collect_v2.add_argument("--refresh-user-trades", action="store_true")
    collect_v2.add_argument("--no-user-trades-cache", action="store_true")
    collect_v2.add_argument("--max-workers", type=int, default=8)
    collect_v2.add_argument("--max-requests-per-second", type=float, default=10)
    collect_v2.add_argument("--request-burst", type=int, default=5)
    collect_v2.add_argument("--max-retry-after-seconds", type=float, default=60)
    collect_v2.set_defaults(func=command_collect_v2)

    collect_v3 = subparsers.add_parser("collect-v3", help="experimental V1/V2 hybrid wallet collector")
    collect_v3.add_argument("--category", choices=["esports"], default="esports")
    collect_v3.add_argument("--output-dir", default=None)
    collect_v3.add_argument("--gamma-pages", type=int, default=10)
    collect_v3.add_argument("--lookback-days", type=int, default=30)
    collect_v3.add_argument("--bucket-market-limit", type=int, default=50)
    collect_v3.add_argument("--positions-per-market", type=int, default=20)
    collect_v3.add_argument("--seed-sort-by", choices=["TOTAL_PNL", "REALIZED_PNL", "CASH_PNL", "TOKENS"], default="TOTAL_PNL")
    collect_v3.add_argument("--max-profile-wallets", type=int, default=700)
    collect_v3.add_argument("--max-leaderboard-wallets", type=int, default=30)
    collect_v3.add_argument("--max-core-wallets", type=int, default=20)
    collect_v3.add_argument("--max-momentum-wallets", type=int, default=10)
    collect_v3.add_argument("--max-watchlist-wallets", type=int, default=50)
    collect_v3.add_argument("--user-history-trades-limit", type=int, default=500)
    collect_v3.add_argument("--user-history-trades-max-pages", type=int, default=3)
    collect_v3.add_argument("--max-esports-markets-per-wallet", type=int, default=100)
    collect_v3.add_argument("--user-trades-cache-ttl-days", type=int, default=1)
    collect_v3.add_argument("--refresh-user-trades", action="store_true")
    collect_v3.add_argument("--no-user-trades-cache", action="store_true")
    collect_v3.add_argument("--no-dashboard-publish", action="store_true")
    collect_v3.add_argument("--max-workers", type=int, default=8)
    collect_v3.add_argument("--max-requests-per-second", type=float, default=10)
    collect_v3.add_argument("--request-burst", type=int, default=5)
    collect_v3.add_argument("--max-retry-after-seconds", type=float, default=60)
    collect_v3.set_defaults(func=command_collect_v3)

    collect_v2_compare = subparsers.add_parser("collect-v2-compare", help="compare V1 and V2 leaderboard overlap")
    collect_v2_compare.add_argument("--v1-dir")
    collect_v2_compare.add_argument("--v2-dir")
    collect_v2_compare.add_argument("--output-file")
    collect_v2_compare.set_defaults(func=command_collect_v2_compare)

    collect_v2_probe = subparsers.add_parser("collect-v2-probe", help="probe settled market-position outcome semantics")
    collect_v2_probe.add_argument("--v2-dir")
    collect_v2_probe.add_argument("--output-file")
    collect_v2_probe.add_argument("--sample-markets", type=int, default=20)
    collect_v2_probe.add_argument("--positions-limit", type=int, default=20)
    collect_v2_probe.add_argument("--seed-sort-by", choices=["TOTAL_PNL", "REALIZED_PNL", "CASH_PNL", "TOKENS"], default="TOTAL_PNL")
    collect_v2_probe.add_argument("--max-workers", type=int, default=8)
    collect_v2_probe.add_argument("--max-requests-per-second", type=float, default=10)
    collect_v2_probe.add_argument("--request-burst", type=int, default=5)
    collect_v2_probe.add_argument("--max-retry-after-seconds", type=float, default=60)
    collect_v2_probe.set_defaults(func=command_collect_v2_probe)

    analyze = subparsers.add_parser("analyze-event")
    analyze.add_argument("--gamma-pages", type=int, default=3)
    analyze.add_argument("--event-slug")
    analyze.add_argument("--condition-id")
    analyze.add_argument("--holders-limit", type=int, default=10)
    analyze.set_defaults(func=command_analyze_event)

    logos = subparsers.add_parser("refresh-team-logos", help="refresh cached team logos for currently watched events")
    logos.add_argument("--max-workers", type=int, default=8)
    logos.add_argument("--logo-timeout-seconds", type=int, default=8)
    logos.add_argument("--max-events", type=int, default=0)
    logos.add_argument("--observe-window-hours", type=float, default=24)
    logos.set_defaults(func=command_refresh_team_logos)

    follow = subparsers.add_parser("follow", help="run one paper follow tick")
    add_follow_arguments(follow)
    follow.set_defaults(func=command_follow)

    run = subparsers.add_parser("run", help="run paper follow loop with scheduled pool refresh")
    add_build_arguments(run)
    run.add_argument("--follow-dir")
    run.add_argument("--stake-usdc", type=float, required=True, help="minimum paper stake per followed BUY leg")
    run.add_argument("--stake-ratio-percent", type=float, default=10.0, help="target-wallet cash replication ratio per BUY leg")
    run.add_argument("--bankroll-usdc", type=float, default=1000.0, help="paper bankroll cap for total open exposure")
    run.add_argument("--follow-recency-days", type=int, default=30)
    run.add_argument("--observe-window-hours", type=float, default=24)
    run.add_argument("--event-cache-ttl-minutes", type=int, default=10)
    run.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
    run.add_argument("--resolution-gamma-pages", type=int, default=2)
    run.add_argument("--max-slippage-over-entry", type=float, default=0.05)
    run.add_argument("--min-wallet-entry-price", type=float, default=0.4)
    run.add_argument("--post-start-trade-grace-seconds", type=int, default=900)
    run.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=False)
    run.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
    run.add_argument("--execution-mode", choices=["paper", "live"], default="paper")
    run.add_argument("--run-log-retention-days", type=int, default=7)
    run.add_argument("--positions-limit", type=int, default=100)
    run.add_argument("--user-trades-limit", type=int, default=100)
    run.add_argument("--user-trades-max-pages", type=int, default=3)
    run.add_argument("--bootstrap-current-positions", dest="bootstrap_current_positions", action="store_true", default=True)
    run.add_argument("--no-bootstrap-current-positions", dest="bootstrap_current_positions", action="store_false")
    run.add_argument("--max-follow-legs", type=int, default=10)
    run.add_argument("--min-tick-seconds", type=int, default=180)
    run.add_argument("--max-tick-seconds", type=int, default=900)
    # Fixed polling cadence (seconds). >0 overrides the adaptive min/max curve; 0 = adaptive.
    run.add_argument("--tick-seconds", type=int, default=120)
    run.add_argument("--consensus-block-opposite", dest="consensus_block_opposite", action="store_true", default=True)
    run.add_argument("--no-consensus-block-opposite", dest="consensus_block_opposite", action="store_false")
    run.add_argument("--conflict-policy", choices=["dual_follow", "exit_on_opposite"], default="dual_follow")
    run.add_argument("--quarantine-sell-frac", type=float, default=0.2)
    run.add_argument("--error-retry-seconds", type=int, default=180)
    run.add_argument("--max-consecutive-error-seconds", type=int, default=600)
    run.add_argument("--pool-refresh-hours", type=float, default=24)
    run.add_argument("--skip-initial-build", action="store_true")
    run.add_argument("--max-run-ticks", type=int, default=0)
    run.set_defaults(func=command_run)

    serve = subparsers.add_parser("serve", help="run read-only dashboard API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--user", default="admin")
    serve.add_argument("--session-ttl-seconds", type=int, default=12 * 3600)
    serve.add_argument("--cookie-secure", action="store_true")
    serve.add_argument("--observe-window-hours", type=float, default=24)
    serve.add_argument("--max-requests-per-second", type=float, default=10)
    serve.add_argument("--request-burst", type=int, default=5)
    serve.add_argument("--max-retry-after-seconds", type=float, default=60)
    serve.add_argument("--runner-stake-usdc", type=float, default=1.0)
    serve.add_argument("--runner-stake-ratio-percent", type=float, default=10.0)
    serve.add_argument("--stream-poll-seconds", type=float, default=2.0)
    serve.add_argument("--stream-heartbeat-seconds", type=float, default=15.0)
    serve.add_argument("--max-stream-clients", type=int, default=8)
    serve.add_argument("--follow-dir")
    serve.set_defaults(func=command_serve)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
