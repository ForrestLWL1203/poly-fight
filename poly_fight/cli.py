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
import threading
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
    FOLLOWABLE_PRICE_CEILING,
    GAME_FAMILY_LABELS,
    LEAGUE_LABELS,
    MARKET_TYPE_LABELS,
    SCORING_VERSION,
    SECONDS_PER_DAY,
    SWING_DEPENDENT_RATE,
    TRADE_BEHAVIOR_EXCLUDE_RATE,
    wallet_is_followable,
    TRADE_BEHAVIOR_MIN_MARKETS,
    GAME_WINNER,
    MAIN_MATCH,
    MAP_WINNER,
    analyze_holders,
    build_candidate_wallets,
    build_classification_set,
    build_discovery_slate,
    bucket_key,
    bucket_label,
    choose_main_market,
    classify_edge_type,
    classify_market_type,
    CALIBRATION_WINDOW_DAYS,
    derive_scope_params,
    match_day_gaps,
    ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS,
    event_to_market_record,
    event_to_market_records,
    split_bucket_key,
    is_settled_binary_prices,
    normalize_market_text,
    normalize_wallet,
    parse_dt,
    parse_jsonish,
    profile_candidate_wallet,
    summarize_trade_reconstructed_positions,
    to_float,
    to_int,
    winning_outcome_index,
)
from .follow import (
    aggregate_follow_performance,
    apply_closing_line_snapshots,
    apply_contested_flags,
    contested_markets,
    desired_tick_interval,
    eligible_follow_wallets,
    follow_signal_id,
    leg_actual_stake,
    market_current_price,
    paper_exit_pnl,
    paper_pnl,
    process_follow_trades,
    prune_unfollowed_signals,
    select_new_trades,
    settle_open_signals,
    trade_condition_id,
    trade_id,
    trade_timestamp,
    winner_outcome_index,
)
from .follow_strategy import strategy_from_legacy_args, strategy_summary, validate_follow_strategy
from .onchain import (
    OnchainFollowCollector,
    build_asset_map,
    clob_price,
    fill_to_trade,
    load_rpc_endpoints,
)
from .storage import FollowStore, LeaderboardStore
from .control import read_follow_control, reconcile_pause_new_signals, set_pause_new_signals

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
# M5 动态淘汰旧实现:跌出 grade-A 曾隔离进 quarantine(reason=rescore_below_grade_a)。现已改为
# 直接删除淘汰(见 rescore_demote_wallets),不再写该 reason。历史遗留的该类隔离行 = 当初表现差被
# 淘汰的钱包,启动时按新策略**直接删除**(下榜+删profile+删缓存),**绝不解禁放回跟单集**——见
# purge_legacy_demote_quarantine。常量保留供该迁移识别这批行。
RESCORE_QUARANTINE_REASON = "rescore_below_grade_a"
# 历史复审清除的已废弃 trade 隔离 reason(恒 None 的死逻辑残留;非淘汰语义,清掉无妨)。
LEGACY_TRADE_QUARANTINE_REASONS = {"material_sell", "two_sided_switch"}
# 隔离入口收束为人工按钮一种(不被历史复审自动清除)。自动淘汰已改直接删除,无 quarantine 中间态。
STICKY_QUARANTINE_REASONS = {
    "manual_dashboard_quarantine",
    "manual_quarantine",
}

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

    for dirname in CATEGORY_REFRESH_CACHE_DIRS:
        cache_dir = category_dir / dirname
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
    "esports": ("counter-strike-2", "league-of-legends", "dota-2", "valorant"),
}
# 每 scope(game_family)的 Gamma tag slug。校准器(及 P2 的 per-game 窗口/发现)按此逐游戏拉取。
# 加新游戏在此登记即可被校准器自动测密度。valorant 已可测密度(分类注册是另一步)。
ESPORTS_GAME_TAGS = {
    "cs2": "counter-strike-2",
    "lol": "league-of-legends",
    "dota2": "dota-2",
    "valorant": "valorant",
}
FOLLOW_SIGNAL_CATEGORIES = ("esports",)
COLLECTOR_NAME = "wallet_collector"
STRICT_FINAL_MIN_ROI = 0.05
STRICT_FINAL_MIN_POSITIVE_MARKET_RATE = 0.55
STRICT_FINAL_MIN_WILSON = 0.50
STRICT_FINAL_MIN_CAPITAL_WEIGHTED_EDGE = 0.05
CORE_MIN_RECENT_14D_MARKETS = 3
CORE_MIN_RECENT_POSITIVE_RATE = 0.50
CORE_MIN_RECENT_ROI = 0.0
COLLECTOR_MAX_INACTIVE_SECONDS = 5 * 24 * 60 * 60
MIN_COPYABLE_BUCKET_ROI = 0.15
MAX_COPYABLE_TWO_SIDED_COUNT = 1
MAX_COPYABLE_TWO_SIDED_RATE = 0.05
COPYABLE_TWO_SIDED_MAX_RATE = 0.30
COPYABLE_TWO_SIDED_MIN_BUCKET_ROI = 0.35
COPYABLE_TWO_SIDED_MIN_FIRST_DIRECTION_WIN_RATE = 0.70
COPYABLE_TWO_SIDED_MIN_CLOSED_COUNT = 10
COPYABLE_TWO_SIDED_PREFILTER_MAX_COUNT = 1_000_000
MAX_COPYABLE_TAIL_ENTRY_RATE = 0.25
MOMENTUM_MIN_7D_MARKETS = 10
MOMENTUM_MIN_7D_POSITIVE_RATE = 0.70
MOMENTUM_MIN_7D_ROI = 0.10
MOMENTUM_MIN_14D_MARKETS = 20
MOMENTUM_MIN_14D_POSITIVE_RATE = 0.65
MOMENTUM_MIN_14D_ROI = 0.08
MOMENTUM_MIN_OVERALL_ROI = 0.02
MOMENTUM_MIN_CAPITAL_WEIGHTED_EDGE = 0.04
MOMENTUM_MAX_MEDIAN_ENTRY_PRICE = 0.75
MOMENTUM_MAX_TWO_SIDED_RATE = 0.05
# --- collect-v2 全新 actual 口径选择门(不继承 V1 老门;见 review/collector-v2-plan.md 附录 C/D)---
# V2 跟两类钱包:单向(押对+持有到结算)与技术型(低买弱势方、盘中短涨清仓),两类都要求场均 ROI 足够高。
# 评分基准为 actual(实际进出场),故这些阈值读到的 pnl/roi/胜率均为 actual 口径。
# 钱包级硬排除门(逐桶质量门已统一到 classify_wallet_bucket 的新三条,见 core.py;
# 原 v2_bucket_gate 的 Wilson/ROI/PnL/positive_rate/entry 门已随该函数删除)。
V2_MAX_TWO_SIDED_RATE = 0.20       # material 双边市场率 ≤ 0.20(实证空谷,降噪;edge 无关)
V2_MAX_BOT_SCORE = 70              # bot 评分硬排除阈
# v21:board 级 tail 门已删(原 V2_MAX_TAIL_ENTRY_RATE=0.25)。其"整盘 avg≥0.75"定义把分批
# 建仓误判成追高,且与 edge_lb 评级门 + 0.85 跟单执行上限三重冗余。discovery 种子预筛仍有独立
# 的 tail 软门(默认 0.34,见下方 filter_profile_seed_wallets_*),那是另一层、阈值更松。
# 钱包级硬门:最后一笔(scoped)交易超过这么多小时 → 直接不入榜(比逐桶 14 天门紧得多)。
# 跟单要钱包当下活跃;沉寂 >72h 的不再上榜(collector 发现 + observer 重评共用)。
V2_MAX_LEADERBOARD_IDLE_HOURS = 336  # 14d:打分窗口(14-30d)已管"是否活跃",72h 过紧会误杀低频高手
V2_PER_GAME_QUOTA = 0              # 0 = 不设每游戏上限(榜单=全部够格);>0 时才强制每游戏封顶
V2_MAX_LEADERBOARD_WALLETS = 200   # 仅作安全上限(质量门已把关,大小不重要)
# 技术型(低买高卖/靠出场)默认不纳入:占比极低 + 我们 follow 延迟高跟不准卖点,风险大。
# 路径保留(--v2-include-technical 可一键开回)。scoring_basis 用 hold(方向正确性):
# 技术型方向常错 → hold 口径下算亏损,自然出局,与"复制方向"一致。
V2_INCLUDE_TECHNICAL = False
# 榜单只发 grade-A(= 有 ≥1 个 A 级桶 = follow 实际跟单的集合)。B 档不跟单 → 不上榜、不展示,
# 但仍保留在内部 profiles 池里(observe-v2 去重必需 + 保留 B→A 晋升路径)。
V2_LEADERBOARD_MIN_GRADE = "A"
# 统一单一 15 天窗口:发现=打分=近期,全部用最近 15 天。esports 比赛密集,实测 15 天
# 即可把 6 个桶全部装满 100 场(瓶颈是 bucket_market_limit,不是窗口),故发现层无损;
# 而窗口短 → 赛事新鲜 → 捞出的钱包原生活跃。近期复检自然内建(整个打分窗口就是 15 天)。
V2_DEFAULT_LOOKBACK_DAYS = 15          # 历史赛事发现范围(--lookback-days)
V2_DEFAULT_PROFILE_LOOKBACK_DAYS = 15  # 打分窗口(--profile-lookback-days)
V2_COLLECTOR_NAME = "wallet_collector_v2"

SEED_BUCKET_MIN_HIT_RATE = 0.10
SEED_BUCKET_MIN_WIN_RATE = SEED_BUCKET_MIN_HIT_RATE
SEED_BUCKET_MIN_WINS_FLOOR = 0
SEED_SINGLE_BUCKET_MIN_WINS = 5
SEED_MULTI_BUCKET_MIN_WINS = 8
# 唯一的 seed 现金门:单场均额(seed_cost_total/seed_market_count)≥ 此值才做数。
# v2 profiling 预筛(filter_profile_seed_wallets_v2)与诊断拒因统计共用此单一来源。
SEED_MIN_AVG_CASH = 100.0
# observe-live:活跃盘种子发现的市场活跃度门(volume 达此值才扫,控成本);读 follow
# runner 维护的 active 市场缓存,缓存可稍陈(best-effort,只读)。
LIVE_SEED_MIN_VOLUME = 5000.0
LIVE_SEED_CACHE_TTL_SECONDS = 24 * 3600
SEED_MAIN_MATCH_MIN_AVG_CASH = SEED_MIN_AVG_CASH  # 旧 per-bucket 名保留为别名,统一指向单一门
SEED_GAME_WINNER_MIN_AVG_CASH = SEED_MIN_AVG_CASH
SEED_MAP_WINNER_MIN_AVG_CASH = SEED_MIN_AVG_CASH
SEED_MIN_WEIGHTED_ROI = 0.30  # v16:seed ROI 门已删(美元口径,质量交给下游 Wilson);此常量仅留作 CLI/记录兼容
SEED_MIN_MEDIAN_AVG_PRICE = 0.35  # 入场价下限(<0.35 多为安全垫/赌爆冷,胜率低);floor,不参与上限收口
SEED_MAX_MEDIAN_AVG_PRICE = FOLLOWABLE_PRICE_CEILING  # 高价上限 = 全系统唯一分水岭(0.85)
COLLECTOR_PROFILE_LOOKBACK_DAYS = 14
COLLECTOR_BUCKETS = (
    ("lol", MAIN_MATCH),
    ("lol", GAME_WINNER),
    ("dota2", MAIN_MATCH),
    ("dota2", GAME_WINNER),
    ("cs2", MAIN_MATCH),
    ("cs2", MAP_WINNER),
    ("valorant", MAIN_MATCH),
    ("valorant", MAP_WINNER),
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
ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS = {
    "lol": {"min_participated_markets": 6, "min_avg_market_cash": 800},
    "dota2": {"min_participated_markets": 5, "min_avg_market_cash": 800},
}
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
    return {"esports": root / "esports"}


def leaderboard_db_path(data_dir: Path) -> Path:
    """优先 collect-v2 的 leaderboard_v2.db;不存在则回退 v1 leaderboard.db。"""
    v2 = Path(data_dir) / "leaderboard_v2.db"
    return v2 if v2.exists() else Path(data_dir) / "leaderboard.db"


def read_category_leaderboards(root: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    mtimes: dict[str, int] = {}
    for category, data_dir in category_data_dirs(root).items():
        db_rows, db_mtimes = LeaderboardStore(leaderboard_db_path(data_dir)).load_leaderboard(category=category)
        if db_rows:
            rows.extend({**row, "category": category} for row in db_rows if isinstance(row, dict))
            mtimes[category] = int(db_mtimes.get(category) or 0)
    legacy_db_rows, legacy_db_mtimes = LeaderboardStore(leaderboard_db_path(root)).load_leaderboard(category="esports")
    if not rows and legacy_db_rows:
        rows.extend({**row, "category": "esports"} for row in legacy_db_rows if isinstance(row, dict))
        mtimes["esports"] = int(legacy_db_mtimes.get("esports") or 0)
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


USER_TRADES_CACHE_SCHEMA = 2


def _user_trade_dedup_key(trade: dict[str, Any]) -> tuple[Any, ...]:
    tid = trade.get("id") or trade.get("transactionHash")
    if tid:
        return ("id", str(tid))
    return (
        "composite",
        str(trade.get("conditionId") or trade.get("condition_id") or "").lower(),
        int(trade.get("timestamp") or 0),
        str(trade.get("side") or ""),
        str(trade.get("size") or ""),
        str(trade.get("price") or ""),
        str(trade.get("outcomeIndex") if trade.get("outcomeIndex") is not None else trade.get("outcome") or ""),
    )


def _sort_user_trades_desc(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(trades, key=lambda trade: int(trade.get("timestamp") or 0), reverse=True)


def _load_raw_user_trade_cache(cache_path: Path) -> tuple[list[dict[str, Any]], bool]:
    """载入 schema=2 的原始交易缓存;旧格式/缺失返回 ([], False) → 当首采处理。"""
    data = read_json(cache_path, {})
    if not isinstance(data, dict) or int(data.get("schema") or 0) != USER_TRADES_CACHE_SCHEMA:
        return [], False
    trades = data.get("trades")
    if not isinstance(trades, list):
        return [], False
    return trades, True


def _fetch_user_trade_pages(
    client: PolymarketClient,
    wallet: str,
    *,
    page_limit: int,
    max_pages: int,
    stop_cursor: dict[str, Any] | None = None,
    scope_condition_ids: set[str] | None = None,
    max_scoped_markets: int = 0,
) -> list[dict[str, Any]]:
    """从 offset 0 翻页拉取钱包交易。
    - stop_cursor 给定(增量):遇到 ≤cursor 的交易即停(通常 1 页)。
    - 否则(首采):翻至短页 / max_pages / 深度命中 max_scoped_markets 个 scoped 市场。
    深翻 400 容错沿用旧逻辑。"""
    collected: list[dict[str, Any]] = []
    limit = max(1, int(page_limit))
    cursor_ts = int((stop_cursor or {}).get("timestamp") or 0) if stop_cursor else None
    cursor_id = str((stop_cursor or {}).get("id") or "") if stop_cursor else ""
    seen_scoped: set[str] = set()
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
        reached_cursor = False
        for trade in batch:
            if cursor_ts is not None:
                ts = int(trade.get("timestamp") or 0)
                tid = str(trade.get("id") or trade.get("transactionHash") or "")
                if ts < cursor_ts or (ts == cursor_ts and (not cursor_id or tid == cursor_id)):
                    reached_cursor = True
                    break
            collected.append(trade)
            if scope_condition_ids is not None and max_scoped_markets:
                cid = str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
                if cid in scope_condition_ids:
                    seen_scoped.add(cid)
        if reached_cursor:
            break
        if scope_condition_ids is not None and max_scoped_markets and len(seen_scoped) >= max_scoped_markets:
            break
        if len(batch) < limit:
            break
    return collected


# 交易缓存(raw_user_trades)瘦身:删每条交易里打分/增量游标都不读的展示字段
# (头像/昵称/标题/slug/asset 等)。打分只用 conditionId/outcomeIndex/outcome/price/side/size/
# timestamp/transactionHash/proxyWallet(已逐字段核对 summarize_trade_reconstructed_positions)。
# 用黑名单(只删已验证无用的大字段、保留其余)更安全。约省 ~55% 体积。
_USER_TRADE_DROP_KEYS = frozenset({
    "asset", "bio", "icon", "name", "profileImage", "profileImageOptimized",
    "pseudonym", "slug", "eventSlug", "title",
})


def _slim_user_trade(trade: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(trade, dict):
        return trade
    return {key: value for key, value in trade.items() if key not in _USER_TRADE_DROP_KEYS}


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
    include_source: bool = False,
    retention_days: int | None = None,
) -> list[dict] | tuple[list[dict], str]:
    """逐钱包**原始**交易缓存 + 增量拉取。缓存按钱包做键、存原始近期交易(不按 scope 过滤);
    已拉过的历史不重复拉,只增量拉游标之后的新交易;打分时再按当前 scope 过滤(窗口裁剪正确)。"""
    condition_ids = {str(value).lower() for value in esports_condition_ids if value}
    if not condition_ids:
        return ([], "empty") if include_source else []
    wallet = normalize_wallet(wallet)
    now_ts = int(now_ts if now_ts is not None else time.time())
    cache_path = user_trades_cache_path(data_dir, wallet) if data_dir else None

    cached_trades: list[dict[str, Any]] = []
    cache_ok = False
    fetch_needed = True
    if use_cache and cache_path and cache_path.exists():
        cached_trades, cache_ok = _load_raw_user_trade_cache(cache_path)
        if cache_ok and not should_refresh_file_cache(
            cache_path.stat().st_mtime,
            now_ts=now_ts,
            ttl_hours=cache_ttl_days * 24,
            force_refresh=force_refresh,
        ):
            fetch_needed = False

    source = "cache"
    if fetch_needed:
        if cache_ok and cached_trades and not force_refresh:
            # 增量:仅拉最新缓存交易之后的新交易。
            newest = _sort_user_trades_desc(cached_trades)[0]
            stop_cursor = {
                "timestamp": int(newest.get("timestamp") or 0),
                "id": str(newest.get("id") or newest.get("transactionHash") or ""),
            }
            new_trades = _fetch_user_trade_pages(
                client, wallet, page_limit=page_limit, max_pages=max_pages, stop_cursor=stop_cursor
            )
        else:
            # 首采(或 force):全采,深度沿用旧界(scoped-market 上限)。
            new_trades = _fetch_user_trade_pages(
                client, wallet, page_limit=page_limit, max_pages=max_pages,
                scope_condition_ids=condition_ids, max_scoped_markets=max_esports_markets,
            )
            cached_trades = []
        merged: dict[tuple[Any, ...], dict[str, Any]] = {}
        for trade in [*new_trades, *cached_trades]:   # 新交易优先覆盖
            merged.setdefault(_user_trade_dedup_key(trade), trade)
        cached_trades = _sort_user_trades_desc(list(merged.values()))
        if retention_days is not None and int(retention_days) > 0:
            cutoff = now_ts - int(retention_days) * 86400
            cached_trades = [t for t in cached_trades if int(t.get("timestamp") or 0) >= cutoff]
        cached_trades = [_slim_user_trade(t) for t in cached_trades]  # 写盘前删无用展示字段(~55%)
        source = "api"
        if use_cache and cache_path:
            write_json(
                cache_path,
                {
                    "schema": USER_TRADES_CACHE_SCHEMA,
                    "wallet": wallet,
                    "trades": cached_trades,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

    filtered = _filter_esports_user_trades(cached_trades, condition_ids, max_esports_markets=max_esports_markets)
    return (filtered, source) if include_source else filtered


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


def _signal_result_at(signal: dict[str, Any]) -> int:
    return to_int(signal.get("exit_at") or signal.get("settled_at") or signal.get("updated_at") or signal.get("created_at"))


def market_records_from_events(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    records = {}
    for event in events:
        for record in event_to_market_records(event):
            records[record["condition_id"]] = record
    return records


def resolution_market_records_from_markets(markets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    records = {}
    for market in markets:
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").lower()
        if not condition_id:
            continue
        prices = [
            to_float(value)
            for value in parse_jsonish(market.get("outcomePrices") or market.get("outcome_prices"), [])
        ]
        records[condition_id] = {
            "condition_id": condition_id,
            "outcome_prices": prices,
            "closed": bool(market.get("closed")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    return records


def backfill_market_resolutions(
    client: PolymarketClient,
    classification_set: list[dict[str, Any]],
    *,
    now_ts: int,
    batch_size: int = 50,
) -> dict[str, int]:
    """打分前补拉结算结果:对分类集里**已过结束时间、但记录还没结算结果**的在册市场,
    用 Gamma 定向批量查(markets_by_condition_ids)拉到 outcome_prices 并就地回填。

    关闭"市场已在 Polymarket 结算、但本地分类记录还没刷到 → reconstruct 因无 winner 跳过该场
    → 钱包清仓亏损被漏计"的时序窗口。复用跟单侧结算轮询同一条定向查询;原地改 classification_set,
    使紧随其后的打分(及 observer/M5 重评)拿到最新结算结果。返回 {checked, filled}。"""
    pending: dict[str, dict[str, Any]] = {}
    for record in classification_set:
        if not isinstance(record, dict):
            continue
        if winning_outcome_index(record) is not None:
            continue  # 已有结算结果
        end_dt = parse_dt(record.get("end_date"))
        if not end_dt or int(end_dt.timestamp()) > now_ts:
            continue  # 还没到结束时间,不该有结果
        condition_id = str(record.get("condition_id") or "").lower()
        if condition_id and condition_id not in pending:
            pending[condition_id] = record
    if not pending:
        return {"checked": 0, "filled": 0}
    filled = 0
    cids = list(pending)
    for index in range(0, len(cids), max(1, batch_size)):
        chunk = cids[index : index + batch_size]
        try:
            markets = client.markets_by_condition_ids(chunk, limit=len(chunk))
        except Exception:
            continue
        for condition_id, fetched in resolution_market_records_from_markets(markets or []).items():
            prices = fetched.get("outcome_prices") or []
            if winning_outcome_index({"outcome_prices": prices}) is None:
                continue  # Gamma 侧也还没结算
            record = pending.get(condition_id)
            if record is None:
                continue
            record["outcome_prices"] = list(prices)
            record["updated_at"] = datetime.now(timezone.utc).isoformat()
            filled += 1
    return {"checked": len(pending), "filled": filled}


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
    if start_ts > window_end:
        return False                              # 太远的未来,暂不纳入
    # 与 watched_markets 一致:未结算的盘(含已开赛的 in-play 子盘)一律保留,只剔除已结算的。
    # 不再用 post_start_grace_seconds 按开始时间下界截断 in-play 盘——否则同一系列赛里我们只能
    # 跟"开赛前就建过仓"的那个子盘(靠 preserve_condition_ids 豁免),Game2-5 / Match Winner /
    # 后续 map 全部被剪掉,目标钱包盘中加注的子盘就永远进不了 watchlist。grace 仅保留签名兼容。
    return winning_outcome_index(market) is None


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
    store: FollowStore | None = None,
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
    if store and legacy_cached:
        markets = rows_to_markets(active_cache_market_rows(legacy_cached))
        if markets:
            store.save_market_cache(markets, cache_kind="active", updated_at=int(legacy_cached.get("updated_at") or now_ts))
        if (
            markets
            and now_ts - int(legacy_cached.get("updated_at") or 0) < ttl_seconds
            and active_cache_has_required_categories({"updated_at": legacy_cached.get("updated_at") or now_ts, "categories": sorted(allowed), "markets": list(markets.values())})
        ):
            return markets, state, "legacy_state_cache"
    elif cache_path and legacy_cached:
        if now_ts - int(legacy_cached.get("updated_at") or 0) < ttl_seconds and active_cache_has_required_categories(legacy_cached):
            markets = rows_to_markets(active_cache_market_rows(legacy_cached))
            write_json(
                cache_path,
                {"updated_at": legacy_cached.get("updated_at") or now_ts, "categories": sorted(allowed), "markets": list(markets.values())},
            )
            return markets, state, "legacy_state_cache"
        write_json(cache_path, legacy_cached)
    if store:
        db_markets, db_updated_at, db_fresh = store.load_market_cache(
            cache_kind="active",
            now_ts=now_ts,
            ttl_seconds=ttl_seconds,
        )
        db_markets = rows_to_markets(list(db_markets.values()))
        if db_markets and db_fresh and active_cache_has_required_categories(
            {"updated_at": db_updated_at, "categories": sorted(allowed), "markets": list(db_markets.values())}
        ):
            return db_markets, state, "db_cache"
        if cache_path and not db_markets:
            cached = read_json(cache_path, {})
            if cached:
                markets = rows_to_markets(active_cache_market_rows(cached))
                if markets:
                    store.save_market_cache(markets, cache_kind="active", updated_at=int(cached.get("updated_at") or now_ts))
                if (
                    markets
                    and now_ts - int(cached.get("updated_at") or 0) < ttl_seconds
                    and active_cache_has_required_categories({"updated_at": cached.get("updated_at") or now_ts, "categories": sorted(allowed), "markets": list(markets.values())})
                ):
                    return markets, state, "legacy_file_cache"
    cached = read_json(cache_path, {}) if cache_path else {}
    if not store and cached and now_ts - int(cached.get("updated_at") or 0) < ttl_seconds and active_cache_has_required_categories(cached):
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
    if store:
        store.save_market_cache(scoped_markets, cache_kind="active", updated_at=now_ts)
    elif cache_path:
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
    active_markets: Any = None,
    store: FollowStore | None = None,
    timeout_seconds: int = 8,
    max_workers: int = 8,
    max_events: int = 0,
    observe_window_hours: float = 24.0,
    now_ts: int | None = None,
    fetch_html: Any = None,
    fetch_logo_bytes: Any = None,
) -> dict[str, Any]:
    if active_markets is None and store is not None:
        loaded, _updated_at, _fresh = store.load_market_cache(
            cache_kind="active",
            now_ts=int(now_ts if now_ts is not None else time.time()),
            ttl_seconds=24 * 3600,
        )
        markets = list(loaded.values())
    elif active_markets is None:
        active_cache = read_json(data_dir / "follow" / "active_market_cache.json", {})
        markets = active_cache.get("markets") if isinstance(active_cache, dict) else []
        if not isinstance(markets, list):
            markets = list(markets.values()) if isinstance(markets, dict) else []
    elif isinstance(active_markets, dict):
        markets = list(active_markets.values())
    else:
        markets = list(active_markets or [])
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
    logo_dir = Path(__file__).with_name("dashboardV2") / "logo"
    logo_path = logo_dir / "team_logos.json"
    cache = read_json(logo_path, {})
    if not isinstance(cache, dict):
        cache = {}
    teams = cache.get("teams") if isinstance(cache.get("teams"), dict) else {}
    teams = dict(teams)

    def cached_logo_exists(value: str) -> bool:
        url = str(value or "")
        if not url.startswith("/logo/"):
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
        return f"/logo/{digest}{suffix}"

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
    # Watchlist:未来 observe_window 内将开赛的盘 + 已开赛但**尚未结算**的盘。盘中持续 watch,
    # 让目标钱包的盘中新成交也能进入跟单评估;要不要跟、跟多少由策略的 edge 闸决定
    # (Kelly:edge_lb = 被跟桶 wilson_lb − 现价;≤0 即 no_live_edge 不跟 → 天然挡住"追已涨过把握的局")。
    # 已结算的盘剔除。post_start_grace_seconds 保留仅为签名兼容,不再用于截断 in-play 盘。
    window_end = now_ts + int(observe_window_hours * 3600)
    watched = {}
    for condition_id, market in active_markets.items():
        start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
        if not start_dt:
            continue
        start_ts = int(start_dt.timestamp())
        if start_ts > window_end:
            continue                                  # 太远的未来,暂不纳入
        if winning_outcome_index(market) is not None:
            continue                                  # 已结算 → 不再跟
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
    needed = sorted(
        {
            str(signal.get("condition_id") or "").lower()
            for signal in eligible_signals
            if signal.get("condition_id")
        }
    )
    if not needed:
        return {}
    # Resolve exactly the started open signals' markets via a targeted batch query.
    # No broad closed-events pull and no scratch cache: winners are persisted into
    # signals/results by the caller, so there is nothing here worth retaining. With a
    # min tick interval of 180s, a per-tick targeted lookup is strictly cheaper than
    # the old 60s-TTL broad pull (which expired every tick anyway).
    direct_markets = client.markets_by_condition_ids(needed, limit=len(needed))
    direct_records = resolution_market_records_from_markets(direct_markets)
    resolutions: dict[str, int] = {}
    for condition_id in needed:
        market = direct_records.get(condition_id)
        if not market:
            continue
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


def effective_bankroll_usdc(value: Any) -> float:
    bankroll = to_float(value)
    if not math.isfinite(bankroll) or bankroll <= 0:
        return float("inf")
    return bankroll


def funded_open_exposure(signals: list[dict[str, Any]]) -> float:
    return round(
        sum(
            leg_actual_stake(leg)
            for signal in signals
            if (signal.get("status") or "open") == "open"
            for leg in signal.get("legs") or []
        ),
        8,
    )


def account_buy_ledger_entries(signals: list[dict[str, Any]], *, created_at: int) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for signal in signals:
        signal_id = str(signal.get("signal_id") or "")
        if not signal_id:
            continue
        wallet = normalize_wallet(signal.get("wallet"))
        condition_id = str(signal.get("condition_id") or "").lower()
        for leg in signal.get("legs") or []:
            if not isinstance(leg, dict) or leg.get("funded_stake") is None:
                continue
            funded_stake = to_float(leg.get("funded_stake"))
            if funded_stake <= 0:
                continue
            trade_id = str(leg.get("trade_id") or leg.get("leg_at") or "")
            ledger_id = f"buy:{signal_id}:{trade_id}"
            entries.append(
                {
                    "ledger_id": ledger_id,
                    "kind": "buy",
                    "amount_usdc": -round(funded_stake, 8),
                    "created_at": int(leg.get("leg_at") or created_at),
                    "signal_id": signal_id,
                    "trade_id": trade_id,
                    "wallet": wallet,
                    "condition_id": condition_id,
                }
            )
    return entries


def account_result_ledger_entries(results: list[dict[str, Any]], *, created_at: int) -> list[dict[str, Any]]:
    """退出/结算时把每条已注资的腿按 funded_stake + 盈亏贷记回余额。

    ledger_id 与买入**对称**(`result:{signal_id}:{trade_id}`,每条腿一条),而不是按
    signal_id 一次性。signal_id = 钱包:市场:方向 是稳定复用的:一个信号"进→出→再进
    →再出"时,旧的 `exit:{signal_id}` 只能落一条,第二段的回笼会撞主键被 apply_account_ledger
    当重复吞掉 → 第二段本金永远不回笼(实测 CHAOS 丢 $200,见 review)。统一 `result:` 前缀
    确保一条腿无论走 exit 还是 settle 都只贷记一次,再入场的新腿是新 trade_id、不撞键。
    """
    entries: list[dict[str, Any]] = []
    for result in results:
        signal_id = str(result.get("signal_id") or "")
        if not signal_id:
            continue
        status = str(result.get("status") or "")
        if status not in ("exited", "settled"):
            continue
        ledger_kind = "exit" if status == "exited" else "settle"
        ts = int(result.get("settled_at") or result.get("exit_at") or created_at)
        wallet = normalize_wallet(result.get("wallet"))
        condition_id = str(result.get("condition_id") or "").lower()
        exit_price = to_float(result.get("exit_price"))
        outcome_won = bool(result.get("outcome_won"))
        for leg in result.get("legs") or []:
            if not isinstance(leg, dict) or leg.get("funded_stake") is None:
                continue
            funded_stake = to_float(leg.get("funded_stake"))
            if funded_stake <= 0:
                continue
            entry_price = to_float(leg.get("our_entry_price"))
            if status == "exited":
                payout = funded_stake + paper_exit_pnl(entry_price, exit_price, funded_stake)
            else:
                payout = funded_stake + paper_pnl(entry_price, outcome_won, funded_stake)
            if payout <= 0:
                continue
            trade_id = str(leg.get("trade_id") or leg.get("leg_at") or "")
            entries.append(
                {
                    "ledger_id": f"result:{signal_id}:{trade_id}",
                    "kind": ledger_kind,
                    "amount_usdc": round(payout, 8),
                    "created_at": ts,
                    "signal_id": signal_id,
                    "trade_id": trade_id,
                    "wallet": wallet,
                    "condition_id": condition_id,
                }
            )
    return entries


# 画像里钱包买过、但当时尚未结算(无 outcome_prices)的在册市场数 → 画像"不完整":
# 这些市场结算后会改变评分(可能新增清仓亏损)。把这类画像的复用 TTL 收紧到 1h,确保
# 市场结算后(esports 通常数小时内)的下一轮采集会重评、补计该亏损 —— 修复"钱包在结算
# 落库前一刻被打分、提前止损卖出的亏损被长期(默认 24h TTL)漏计"的时间竞态。
PENDING_PROFILE_REUSE_TTL_SECONDS = 3600


def should_use_cached_profile(cached: dict[str, Any] | None, *, now_ts: int, ttl_seconds: int) -> bool:
    if not cached:
        return False
    if cached.get("profile_state") == "failed_retryable":
        return False
    if not isinstance(cached.get("esports_condition_ids"), list):
        return False
    if int(cached.get("scoring_version") or 0) != SCORING_VERSION:
        return False
    age = now_ts - int(cached.get("profiled_at", 0))
    if int(cached.get("pending_resolution_market_count") or 0) > 0:
        return age < min(ttl_seconds, PENDING_PROFILE_REUSE_TTL_SECONDS)
    return age < ttl_seconds


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


def copyable_two_sided_reject_reasons(
    candidate_metrics: dict[str, Any],
    quality_metrics: dict[str, Any],
) -> list[str]:
    two_sided_count = int(candidate_metrics.get("two_sided_market_count") or 0)
    if two_sided_count <= 0:
        return []
    participated = int(
        candidate_metrics.get("participated_market_count")
        or candidate_metrics.get("historical_trade_behavior_market_count")
        or 0
    )
    two_sided_rate = two_sided_count / participated if participated > 0 else 1.0
    reasons: list[str] = []
    if two_sided_rate >= COPYABLE_TWO_SIDED_MAX_RATE:
        reasons.append("two_sided_rate_gte_max")
    if int(quality_metrics.get("esports_closed_count") or 0) < COPYABLE_TWO_SIDED_MIN_CLOSED_COUNT:
        reasons.append("two_sided_closed_count_lt_min")
    if (
        two_sided_rate >= MAX_COPYABLE_TWO_SIDED_RATE
        and to_float(quality_metrics.get("esports_roi")) < COPYABLE_TWO_SIDED_MIN_BUCKET_ROI
    ):
        reasons.append("two_sided_bucket_roi_lt_min")
    first_direction_rate = quality_metrics.get("first_direction_win_rate")
    if first_direction_rate is None:
        reasons.append("first_direction_missing")
    elif to_float(first_direction_rate) <= COPYABLE_TWO_SIDED_MIN_FIRST_DIRECTION_WIN_RATE:
        reasons.append("first_direction_win_rate_lte_min")
    return sorted(set(reasons))


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
        if not wallet_is_followable(profile):
            continue
        # v16:legacy ROI / positive-rate 门已删(美元/拉通整体口径,与分桶均仓跟单无关;
        # 质量由 Wilson 双下界 + 分桶 eligible 把关)。
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
                if isinstance(profile.get("eligible_bucket_modes"), dict):
                    profile["eligible_bucket_modes"] = {
                        key: profile["eligible_bucket_modes"][key]
                        for key in followable_buckets
                        if key in profile["eligible_bucket_modes"]
                    }
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
            # 不再用美元 ROI / 资金加权边际二次过滤:评分层的胜率(θ̂≥0.58)+ edge(θ̂−价格≥0.06)
            # 已是质量护栏。ROI 门槛会把"高胜率但因 sizing 亏/买热门 ROI 低"的钱包再次误杀。
            followable_buckets.append(key)
        if not followable_buckets:
            return None
        filtered = dict(profile)
        filtered["eligible_buckets"] = followable_buckets
        if isinstance(profile.get("eligible_bucket_modes"), dict):
            filtered["eligible_bucket_modes"] = {
                key: profile["eligible_bucket_modes"][key]
                for key in followable_buckets
                if key in profile["eligible_bucket_modes"]
            }
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
        return profile

    # legacy per_type 路径:同样不再用美元轴二次过滤(评分层已守住)。
    per_type_grades = profile.get("per_type_grades") if isinstance(profile.get("per_type_grades"), dict) else {}
    followable_types = list(eligible_market_types)
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
    # 展示分走新轴 v2_bucket_display_score(近期加权胜率 θ̂ + copy-edge + n_eff + 活跃度),
    # 与采集时存入 best_bucket_score 的口径一致;旧 Wilson/ROI 复合分已废弃(ROI 已不是评分依据)。
    score = v2_bucket_display_score(metrics, now_ts=now_ts)
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
        "first_direction_market_count": metrics.get("first_direction_market_count"),
        "first_direction_win_count": metrics.get("first_direction_win_count"),
        "first_direction_win_rate": metrics.get("first_direction_win_rate"),
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
        "eligible_mode": metrics.get("eligible_mode"),
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
            0 if wallet_is_followable(row) else 1,
            -to_float(score),
            -to_float(metrics.get("wilson_win_rate_lower_bound")),
            -to_float(metrics.get("capital_weighted_edge") or metrics.get("entry_edge")),
            -to_float(metrics.get("positive_market_rate")),
            -int(metrics.get("esports_closed_count") or 0),
            normalize_wallet(row.get("wallet")),
        )
    loss_count = int(metrics.get("esports_loss_count") or 0)
    return (
        0 if wallet_is_followable(row) else 1,
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
    force_refresh_wallets: set[str] | None = None,
) -> list[dict[str, Any]]:
    force_refresh_wallets = {normalize_wallet(wallet) for wallet in (force_refresh_wallets or set()) if normalize_wallet(wallet)}
    forced_items: list[dict[str, Any]] = []
    seen_forced: set[str] = set()
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
        if wallet in force_refresh_wallets:
            forced_items.append(normalized)
            seen_forced.add(wallet)
            continue
        if (
            not should_use_cached_profile(cached, now_ts=now_ts, ttl_seconds=ttl_seconds)
            or profile_needs_schema_migration(cached)
        ):
            candidate_items.append(normalized)

    for wallet in sorted(force_refresh_wallets - seen_forced):
        profile = existing_profiles.get(wallet)
        if not profile:
            continue
        candidate = dict(profile.get("candidate") or {})
        candidate["wallet"] = wallet
        forced_items.append(candidate)
        seen_candidates.add(wallet)

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

    if max_profiles <= 0:
        return forced_items

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

    normal_items = [*selected_candidates, *selected_migrations]
    forced_wallets = {row["wallet"] for row in forced_items}
    return [*forced_items, *(row for row in normal_items if row["wallet"] not in forced_wallets)]


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


def _sorted_nested_count_dict(counter: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    return {
        key: _sorted_count_dict(counter[key])
        for key in sorted(counter)
    }


def default_seed_bucket_min_avg_cash() -> dict[str, float]:
    return {
        MAIN_MATCH: SEED_MAIN_MATCH_MIN_AVG_CASH,
        GAME_WINNER: SEED_GAME_WINNER_MIN_AVG_CASH,
        MAP_WINNER: SEED_MAP_WINNER_MIN_AVG_CASH,
    }


def seed_bucket_market_type(bucket: str, rows: list[dict[str, Any]] | None = None) -> str:
    for row in rows or []:
        market_type = str(row.get("market_type") or "")
        if market_type:
            return market_type
    return split_bucket_key(bucket)[1]


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
    max_two_sided_market_count: int = COPYABLE_TWO_SIDED_PREFILTER_MAX_COUNT,
    max_two_sided_market_rate: float = COPYABLE_TWO_SIDED_MAX_RATE,
) -> list[str]:
    reasons: list[str] = []
    eligible_market_types = [str(value) for value in profile.get("eligible_market_types") or [] if value]
    if int(profile.get("scoring_version") or 0) != SCORING_VERSION:
        reasons.append("old_scoring_version")
    if profile.get("per_type_grades") is not None and not eligible_market_types:
        reasons.append("no_eligible_per_type")
    if not wallet_is_followable(profile):
        reasons.append("not_A_no_eligible_type")
    # v16:legacy_low_roi / legacy_low_positive_rate 已删(美元/拉通整体口径,且只在"无合格桶"时触发、
    # 与 no_eligible_per_type 重叠,删后该类钱包仍被拒)。质量由 Wilson 双下界 + 分桶 eligible 把关。
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


def select_collector_target_markets(
    classification_set: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_days: int = 30,
    bucket_market_limit: int = 100,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    cutoff_ts = int(now.timestamp()) - max(0, int(lookback_days)) * 86400
    bucket_counts: dict[str, int] = {}
    bucket_shortfalls: dict[str, int] = {}
    selected: list[dict[str, Any]] = []
    for game_family, market_type in COLLECTOR_BUCKETS:
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
        "target_market_capacity": int(bucket_market_limit) * len(COLLECTOR_BUCKETS),
    }


def calculate_seed_bucket_min_wins(
    bucket_counts: dict[str, Any],
    *,
    min_rate: float = SEED_BUCKET_MIN_WIN_RATE,
    floor: int = SEED_BUCKET_MIN_WINS_FLOOR,
) -> dict[str, int]:
    thresholds: dict[str, int] = {}
    for bucket, count in (bucket_counts or {}).items():
        bucket_count = to_int(count)
        if bucket_count <= 0:
            continue
        thresholds[str(bucket)] = max(int(floor), int(math.ceil(bucket_count * float(min_rate))))
    return thresholds


def effective_seed_bucket_min_wins(
    seed_bucket_min_wins: dict[str, int] | None,
    *,
    seed_single_bucket_min_wins: int = SEED_SINGLE_BUCKET_MIN_WINS,
) -> dict[str, int]:
    single_bucket_cap = to_int(seed_single_bucket_min_wins)
    thresholds: dict[str, int] = {}
    for bucket, min_wins in (seed_bucket_min_wins or {}).items():
        raw_min_wins = to_int(min_wins)
        if raw_min_wins <= 0:
            continue
        thresholds[str(bucket)] = min(raw_min_wins, single_bucket_cap) if single_bucket_cap > 0 else raw_min_wins
    return thresholds


def collect_seed_positions(
    market: dict[str, Any],
    market_positions_response: list[dict[str, Any]],
    *,
    positions_per_market: int = 20,
    include_losing_side: bool = False,
) -> list[dict[str, Any]]:
    """从 market-positions 抽取盈利持仓作为种子。

    v1(默认)只取最终胜方持仓 —— 但实证(review/collector-v2-plan.md 附录 A)显示负方
    也有大量盈利钱包,它们靠"低价买入 + 结算前卖出"赚钱(technical 型),winners-only 会
    把这些高水平钱包全部丢掉。`include_losing_side=True`(collect-v2)同时采集双侧盈利持仓,
    把"押对/押错"留给钱包级 profiling 用真实历史判定,降噪下沉到钱包级。
    """
    winner_index = winning_outcome_index(market)
    if winner_index is None:
        return []
    seed_rows: list[dict[str, Any]] = []
    for token_index, token_block in enumerate(market_positions_response or []):
        for position in token_block.get("positions") or []:
            outcome_index = to_int(position.get("outcomeIndex"), token_index)
            outcome_won = outcome_index == winner_index
            if not include_losing_side and not outcome_won:
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
            # 仍只保留盈利持仓(质量信号);负方盈利者必然已在结算前卖出 → technical 型。
            if seed_pnl <= 0 or total_bought <= 0 or avg_price <= 0 or seed_cost <= 0:
                continue
            seed_rows.append(
                {
                    "wallet": wallet,
                    "condition_id": str(market.get("condition_id") or "").lower(),
                    "question": market.get("question") or market.get("title") or "",
                    "game_family": str(market.get("game_family") or "").lower(),
                    "market_type": str(market.get("market_type") or MAIN_MATCH),
                    "bucket_key": str(market.get("bucket_key") or bucket_key(market.get("game_family"), market.get("market_type"))),
                    "outcome_index": outcome_index,
                    "outcome": position.get("outcome"),
                    "seed_outcome_won": outcome_won,
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
    cap = max(0, int(positions_per_market))
    if not include_losing_side:
        rows = seed_rows[:cap]
        for rank, row in enumerate(rows, start=1):
            row["seed_rank"] = rank
        return rows
    # 双侧:按每一侧分别截断并各自排名,避免胜方占满名额把负方整段挤掉。
    rows: list[dict[str, Any]] = []
    for won in (True, False):
        side_rows = [row for row in seed_rows if bool(row.get("seed_outcome_won")) is won][:cap]
        for rank, row in enumerate(side_rows, start=1):
            row["seed_rank"] = rank
        rows.extend(side_rows)
    return rows


def collect_live_seed_positions(
    market: dict[str, Any],
    market_positions_response: list[dict[str, Any]],
    *,
    positions_per_market: int = 20,
) -> list[dict[str, Any]]:
    """活跃(**未结算**)盘的种子抽取 —— collect_seed_positions 的 live 对称版。

    与闭市版的关键差异(observe-live 用):
    - **无 winner**:未结算盘没有 winning_outcome_index,双侧全取,`seed_outcome_won=None`。
    - **不按当前盈亏硬筛**:totalPnl 是未实现盈亏,只作软排序/诊断(seed_in_profit),
      不当准入闸 —— 押对/押错交给钱包级历史 profiling 判定(用户决策:软排序非硬闸)。
    - **按持仓名义额(seed_cost)排序**,每侧(outcome_index)各取 top-K;名义额相同时
      ITM(seed_in_profit)优先打破平局。
    """
    cid = str(market.get("condition_id") or "").lower()
    by_outcome: dict[int, list[dict[str, Any]]] = {}
    for token_index, token_block in enumerate(market_positions_response or []):
        for position in token_block.get("positions") or []:
            wallet = normalize_wallet(position.get("proxyWallet") or position.get("wallet"))
            if not wallet:
                continue
            outcome_index = to_int(position.get("outcomeIndex"), token_index)
            total_bought = to_float(position.get("totalBought"))
            avg_price = to_float(position.get("avgPrice"))
            seed_cost = total_bought * avg_price
            if total_bought <= 0 or avg_price <= 0 or seed_cost <= 0:   # 仅要求有效持仓,不筛盈亏
                continue
            seed_pnl = to_float(position.get("totalPnl"))
            if seed_pnl == 0:
                seed_pnl = to_float(position.get("realizedPnl"))
            by_outcome.setdefault(outcome_index, []).append({
                "wallet": wallet,
                "condition_id": cid,
                "question": market.get("question") or market.get("title") or "",
                "game_family": str(market.get("game_family") or "").lower(),
                "market_type": str(market.get("market_type") or MAIN_MATCH),
                "bucket_key": str(market.get("bucket_key") or bucket_key(market.get("game_family"), market.get("market_type"))),
                "outcome_index": outcome_index,
                "outcome": position.get("outcome"),
                "seed_outcome_won": None,                # 未结算,未知
                "avg_price": round(avg_price, 8),
                "total_bought": round(total_bought, 8),
                "seed_cost": round(seed_cost, 8),
                "seed_pnl": round(seed_pnl, 8),
                "seed_roi": round(seed_pnl / seed_cost, 8) if seed_cost > 0 else 0.0,
                "seed_edge": round(max(0.0, 1.0 - avg_price), 8),
                "seed_in_profit": seed_pnl > 0,          # 软排序信号
                "market_volume": to_float(market.get("volume")),
                "timestamp": 0,                          # 未结算,无 end_date
            })
    cap = max(0, int(positions_per_market))
    rows: list[dict[str, Any]] = []
    for outcome_index in sorted(by_outcome):
        side_rows = by_outcome[outcome_index]
        side_rows.sort(key=lambda r: (r["seed_cost"], r["seed_in_profit"]), reverse=True)
        side_rows = side_rows[:cap]
        for rank, row in enumerate(side_rows, start=1):
            row["seed_rank"] = rank
        rows.extend(side_rows)
    return rows


def aggregate_seed_wallets(seed_positions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
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
        bucket_rows: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            bucket = str(row.get("bucket_key") or "unknown")
            _increment_count(bucket_counts, bucket)
            bucket_rows.setdefault(bucket, []).append(row)
            _increment_count(game_family_counts, str(row.get("game_family") or "unknown"))
            _increment_count(market_type_counts, str(row.get("market_type") or MAIN_MATCH))
        seed_cost_total = sum(to_float(row.get("seed_cost")) for row in rows)
        seed_pnl_total = sum(to_float(row.get("seed_pnl")) for row in rows)
        avg_prices = [to_float(row.get("avg_price")) for row in rows if to_float(row.get("avg_price")) > 0]
        ranks = [to_float(row.get("seed_rank")) for row in rows if to_float(row.get("seed_rank")) > 0]
        seed_bucket_stats: dict[str, dict[str, Any]] = {}
        for bucket, bucket_group in sorted(bucket_rows.items()):
            bucket_cost = sum(to_float(row.get("seed_cost")) for row in bucket_group)
            bucket_pnl = sum(to_float(row.get("seed_pnl")) for row in bucket_group)
            bucket_avg_prices = [
                to_float(row.get("avg_price"))
                for row in bucket_group
                if to_float(row.get("avg_price")) > 0
            ]
            game_family, fallback_market_type = split_bucket_key(bucket)
            market_type = seed_bucket_market_type(bucket, bucket_group) or fallback_market_type
            seed_bucket_stats[bucket] = {
                "bucket_key": bucket,
                "bucket_label": bucket_label(bucket),
                "game_family": game_family,
                "market_type": market_type,
                "seed_market_count": len(bucket_group),
                "seed_cost_total": round(bucket_cost, 8),
                "seed_pnl_total": round(bucket_pnl, 8),
                "seed_weighted_roi": round(bucket_pnl / bucket_cost, 8) if bucket_cost > 0 else 0.0,
                "avg_seed_cash": round(bucket_cost / len(bucket_group), 8) if bucket_group else 0.0,
                "median_avg_price": round(_median_float(bucket_avg_prices), 8),
            }
        wallet_row = {
            "wallet": wallet,
            "seed_position_row_count": len(raw_rows),
            "seed_win_count": len(condition_ids),
            "seed_market_count": len(condition_ids),
            "seed_condition_ids": sorted(condition_ids),
            "seed_bucket_count": len(bucket_counts),
            "seed_bucket_counts": _sorted_count_dict(bucket_counts),
            "seed_bucket_stats": seed_bucket_stats,
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
        wallet_row["seed_score"] = round(seed_wallet_score(wallet_row), 8)
        wallet_row["candidate"] = {
            "wallet": wallet,
            "participated_market_count": wallet_row["seed_market_count"],
            "participated_market_ids": wallet_row["seed_condition_ids"],
            "total_cash_volume": wallet_row["seed_cost_total"],
            "max_single_market_cash": round(max((to_float(row.get("seed_cost")) for row in rows), default=0.0), 8),
            "avg_market_cash": round(seed_cost_total / len(condition_ids), 8) if condition_ids else 0.0,
            "seed_tail_entry_market_count": sum(1 for price in avg_prices if price > 0.85),
            "candidate_reasons": ["profitable_winner_seed"],
            "source": "collector_market_positions",
        }
        wallets[wallet] = wallet_row
    return wallets


def seed_wallet_score(row: dict[str, Any]) -> float:
    frequency_score = min(to_int(row.get("seed_win_count")), 10) * 4.0
    pnl_score = min(math.log10(max(to_float(row.get("seed_pnl_total")), 0.0) + 1.0), 4.0) * 1.5
    roi_score = min(max(to_float(row.get("seed_weighted_roi")), 0.0), 1.0) * 4.0
    median_avg = to_float(row.get("median_avg_price"))
    entry_edge_score = min(max(1.0 - median_avg, 0.0), 0.7) * 5.0
    cross_bucket_score = min(to_int(row.get("seed_bucket_count")), 4) * 1.0
    avg_rank = to_float(row.get("avg_seed_rank"))
    rank_score = max(0.0, (21.0 - avg_rank) / 20.0) * 2.0 if avg_rank > 0 else 0.0
    return frequency_score + pnl_score + roi_score + entry_edge_score + cross_bucket_score + rank_score


def seed_bucket_stats_for_row(row: dict[str, Any], bucket: str) -> dict[str, Any]:
    seed_bucket_stats = row.get("seed_bucket_stats") if isinstance(row.get("seed_bucket_stats"), dict) else {}
    if isinstance(seed_bucket_stats.get(bucket), dict):
        return dict(seed_bucket_stats[bucket])
    seed_bucket_counts = row.get("seed_bucket_counts") if isinstance(row.get("seed_bucket_counts"), dict) else {}
    count = to_int(seed_bucket_counts.get(bucket))
    return {
        "bucket_key": bucket,
        "bucket_label": bucket_label(bucket),
        "game_family": split_bucket_key(bucket)[0],
        "market_type": split_bucket_key(bucket)[1],
        "seed_market_count": count,
        "seed_cost_total": 0.0,
        "seed_pnl_total": 0.0,
        "seed_weighted_roi": 0.0,
        "avg_seed_cash": 0.0,
        "median_avg_price": 0.0,
    }


def seed_bucket_quality_reject_reasons(
    bucket: str,
    stats: dict[str, Any],
    *,
    min_wins: int,
    min_avg_cash_by_market_type: dict[str, float] | None,
    min_weighted_roi: float,
    max_median_avg_price: float,
) -> list[str]:
    reasons: list[str] = []
    market_type = str(stats.get("market_type") or split_bucket_key(bucket)[1])
    min_avg_cash = to_float((min_avg_cash_by_market_type or default_seed_bucket_min_avg_cash()).get(market_type))
    if to_int(stats.get("seed_market_count")) < int(min_wins):
        reasons.append("seed_bucket_win_count_lt_min")
    if to_float(stats.get("avg_seed_cash")) < min_avg_cash:
        reasons.append("seed_bucket_avg_cash_lt_min")
    # v16:seed ROI 门已删(美元口径,与均仓跟单无关,质量交给下游 Wilson)。min_weighted_roi 入参保留仅作兼容。
    median_avg_price = to_float(stats.get("median_avg_price"))
    if median_avg_price < SEED_MIN_MEDIAN_AVG_PRICE or median_avg_price > float(max_median_avg_price):
        reasons.append("seed_bucket_median_avg_price_gt_max")
    return reasons


def evaluate_seed_bucket_qualification(
    row: dict[str, Any],
    *,
    seed_bucket_min_wins: dict[str, int] | None,
    seed_single_bucket_min_wins: int,
    seed_multi_bucket_min_wins: int,
    seed_bucket_min_avg_cash: dict[str, float] | None,
    seed_min_weighted_roi: float,
    seed_max_median_avg_price: float,
) -> dict[str, Any]:
    bucket_thresholds = effective_seed_bucket_min_wins(
        seed_bucket_min_wins,
        seed_single_bucket_min_wins=seed_single_bucket_min_wins,
    )
    single_bucket_qualified: list[str] = []
    single_bucket_stats: dict[str, dict[str, Any]] = {}
    multi_bucket_candidates: list[str] = []
    multi_bucket_stats: dict[str, dict[str, Any]] = {}
    bucket_reject_reasons: dict[str, list[str]] = {}
    wallet_reject_reasons: list[str] = []
    for bucket, min_wins in bucket_thresholds.items():
        stats = seed_bucket_stats_for_row(row, bucket)
        single_reasons = seed_bucket_quality_reject_reasons(
            bucket,
            stats,
            min_wins=min_wins,
            min_avg_cash_by_market_type=seed_bucket_min_avg_cash,
            min_weighted_roi=seed_min_weighted_roi,
            max_median_avg_price=seed_max_median_avg_price,
        )
        if single_reasons:
            bucket_reject_reasons[bucket] = single_reasons
        else:
            single_bucket_qualified.append(bucket)
            single_bucket_stats[bucket] = stats

        if to_int(stats.get("seed_market_count")) <= 0:
            continue
        quality_reasons = seed_bucket_quality_reject_reasons(
            bucket,
            stats,
            min_wins=1,
            min_avg_cash_by_market_type=seed_bucket_min_avg_cash,
            min_weighted_roi=seed_min_weighted_roi,
            max_median_avg_price=seed_max_median_avg_price,
        )
        if not quality_reasons:
            multi_bucket_candidates.append(bucket)
            multi_bucket_stats[bucket] = stats

    multi_bucket_min_wins = to_int(seed_multi_bucket_min_wins)
    multi_bucket_win_count = sum(
        to_int(multi_bucket_stats[bucket].get("seed_market_count"))
        for bucket in multi_bucket_candidates
    )
    multi_bucket_ok = (
        multi_bucket_min_wins > 0
        and len(multi_bucket_candidates) >= 2
        and multi_bucket_win_count >= multi_bucket_min_wins
    )
    if single_bucket_qualified:
        qualified_buckets = list(single_bucket_qualified)
        qualified_stats = dict(single_bucket_stats)
        qualification_mode = "single_bucket"
    elif multi_bucket_ok:
        qualified_buckets = list(multi_bucket_candidates)
        qualified_stats = dict(multi_bucket_stats)
        qualification_mode = "multi_bucket"
    else:
        qualified_buckets = []
        qualified_stats = {}
        qualification_mode = ""
        if bucket_thresholds:
            wallet_reject_reasons.append("seed_bucket_win_count_lt_min")
            if multi_bucket_min_wins > 0:
                wallet_reject_reasons.append("seed_multi_bucket_win_count_lt_min")

    qualified_buckets.sort(
        key=lambda value: (
            {"lol": 0, "dota2": 1, "cs2": 2}.get(split_bucket_key(value)[0], 99),
            {"main_match": 0, "game_winner": 1, "map_winner": 2}.get(split_bucket_key(value)[1], 99),
            value,
        )
    )
    return {
        "seed_bucket_min_wins": bucket_thresholds,
        "seed_multi_bucket_min_wins": multi_bucket_min_wins,
        "multi_bucket_seed_win_count": multi_bucket_win_count,
        "qualified_seed_buckets": qualified_buckets,
        "qualified_seed_bucket_stats": {
            bucket: qualified_stats[bucket]
            for bucket in qualified_buckets
        },
        "seed_bucket_reject_reasons": bucket_reject_reasons,
        "seed_qualification_mode": qualification_mode,
        "wallet_reject_reasons": sorted(set(wallet_reject_reasons)),
    }




def filter_profile_seed_wallets_v2(
    seed_wallets: dict[str, dict[str, Any]],
    *,
    max_wallets: int,
    min_seed_markets: int = 1,
    min_avg_seed_cash: float = SEED_MIN_AVG_CASH,
) -> list[dict[str, Any]]:
    """v2 种子预筛:廉价召回 + per-game round-robin 分配 profiling 预算。

    不套 v1 严格桶门(min_wins/weighted_roi/median_price)——质量决策已下沉到 v2 导出门
    (actual ROI + 降噪)。这里只:① 去 dust(至少 1 个种子市场、均额 ≥ 低底);
    ② 按 seed_score 在每个 game_family 内排序后 round-robin 取,保证低交易游戏的候选也能
    拿到 profiling 名额(从源头解决偏科),高交易游戏用剩余预算补足。
    """
    candidates: list[dict[str, Any]] = []
    for row in seed_wallets.values():
        market_count = to_int(row.get("seed_market_count"))
        if market_count < min_seed_markets:
            continue
        avg_cash = to_float(row.get("seed_cost_total")) / market_count if market_count else 0.0
        if avg_cash < min_avg_seed_cash:
            continue
        game_counts = row.get("seed_game_family_counts") if isinstance(row.get("seed_game_family_counts"), dict) else {}
        primary_game = max(game_counts, key=lambda g: game_counts[g]) if game_counts else "unknown"
        updated = dict(row)
        updated["seed_primary_game"] = primary_game
        updated["seed_avg_cash"] = round(avg_cash, 4)
        candidates.append(updated)
    by_game: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        by_game.setdefault(row["seed_primary_game"], []).append(row)
    for rows in by_game.values():
        rows.sort(
            key=lambda row: (
                to_float(row.get("seed_score")),
                to_int(row.get("seed_win_count")),
                to_float(row.get("seed_pnl_total")),
                normalize_wallet(row.get("wallet")),
            ),
            reverse=True,
        )
    budget = max(0, int(max_wallets))
    games = sorted(by_game)
    pointers = {game: 0 for game in games}
    selected: list[dict[str, Any]] = []
    # round-robin:每轮各游戏取 1 个(按各自 seed_score 序),低交易游戏先取尽,高交易游戏补足预算。
    while len(selected) < budget and any(pointers[game] < len(by_game[game]) for game in games):
        for game in games:
            if pointers[game] < len(by_game[game]):
                selected.append(by_game[game][pointers[game]])
                pointers[game] += 1
                if len(selected) >= budget:
                    break
    return selected


def seed_filter_reject_reasons(
    seed_wallets: dict[str, dict[str, Any]],
    profile_wallets: list[dict[str, Any]],
    *,
    min_seed_win_count: int = 2,
    min_seed_cost_total: float = 0,
    max_median_avg_price: float = SEED_MAX_MEDIAN_AVG_PRICE,
    seed_bucket_min_wins: dict[str, int] | None = None,
    seed_bucket_min_avg_cash: dict[str, float] | None = None,
    seed_min_weighted_roi: float = SEED_MIN_WEIGHTED_ROI,
    seed_max_median_avg_price: float | None = None,
    seed_single_bucket_min_wins: int = SEED_SINGLE_BUCKET_MIN_WINS,
    seed_multi_bucket_min_wins: int = SEED_MULTI_BUCKET_MIN_WINS,
) -> dict[str, int]:
    selected_wallets = {normalize_wallet(row.get("wallet")) for row in profile_wallets}
    counts: dict[str, int] = {}
    bucket_thresholds = effective_seed_bucket_min_wins(
        seed_bucket_min_wins,
        seed_single_bucket_min_wins=seed_single_bucket_min_wins,
    )
    max_avg_price = float(seed_max_median_avg_price if seed_max_median_avg_price is not None else max_median_avg_price)
    for row in seed_wallets.values():
        wallet = normalize_wallet(row.get("wallet"))
        reasons: list[str] = []
        if bucket_thresholds:
            qualification = evaluate_seed_bucket_qualification(
                row,
                seed_bucket_min_wins=seed_bucket_min_wins,
                seed_single_bucket_min_wins=seed_single_bucket_min_wins,
                seed_multi_bucket_min_wins=seed_multi_bucket_min_wins,
                seed_bucket_min_avg_cash=seed_bucket_min_avg_cash,
                seed_min_weighted_roi=seed_min_weighted_roi,
                seed_max_median_avg_price=max_avg_price,
            )
            if not qualification.get("qualified_seed_buckets"):
                reasons.extend(qualification.get("wallet_reject_reasons") or [])
                for bucket_reasons in (qualification.get("seed_bucket_reject_reasons") or {}).values():
                    reasons.extend(bucket_reasons)
        elif to_int(row.get("seed_win_count")) < min_seed_win_count:
            reasons.append("seed_win_count_lt_min")
        if not bucket_thresholds:
            if to_float(row.get("seed_cost_total")) < min_seed_cost_total:
                reasons.append("seed_cost_total_lt_min")
            # v16:ROI 门已删。
            if to_float(row.get("median_avg_price")) > max_avg_price:
                reasons.append("median_avg_price_gt_max")
        if not reasons and wallet not in selected_wallets:
            reasons.append("profile_wallet_cap")
        for reason in set(reasons):
            _increment_count(counts, reason)
    return _sorted_count_dict(counts)


def bucket_seed_wallet_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        bucket_counts = row.get("seed_bucket_counts") if isinstance(row.get("seed_bucket_counts"), dict) else {}
        for bucket, count in bucket_counts.items():
            if to_int(count) > 0:
                _increment_count(counts, str(bucket))
    return _sorted_count_dict(counts)


def build_collector_diagnostics(
    *,
    seed_wallets: dict[str, dict[str, Any]],
    profile_wallets: list[dict[str, Any]],
    profiles_by_wallet: dict[str, dict[str, Any]],
    leaderboard: list[dict[str, Any]],
    now_ts: int,
    seed_bucket_min_wins: dict[str, int] | None = None,
    seed_bucket_min_avg_cash: dict[str, float] | None = None,
    seed_min_weighted_roi: float = SEED_MIN_WEIGHTED_ROI,
    seed_max_median_avg_price: float = SEED_MAX_MEDIAN_AVG_PRICE,
    seed_single_bucket_min_wins: int = SEED_SINGLE_BUCKET_MIN_WINS,
    seed_multi_bucket_min_wins: int = SEED_MULTI_BUCKET_MIN_WINS,
) -> dict[str, Any]:
    profile_grade_counts: dict[str, int] = {}
    eligible_bucket_counts: dict[str, int] = {}
    eligible_bucket_mode_counts: dict[str, int] = {}
    eligible_market_type_counts: dict[str, int] = {}
    qualified_seed_bucket_counts: dict[str, int] = {}
    leaderboard_best_bucket_counts: dict[str, int] = {}
    leaderboard_best_game_family_counts: dict[str, int] = {}
    leaderboard_wallets = {normalize_wallet(row.get("wallet")) for row in leaderboard}
    leaderboard_reject_reasons: dict[str, int] = {}
    eligible_bucket_reject_reasons: dict[str, dict[str, int]] = {}
    copyable_reject_reasons_by_bucket: dict[str, dict[str, int]] = {}
    for row in profile_wallets:
        for bucket in row.get("qualified_seed_buckets") or []:
            _increment_count(qualified_seed_bucket_counts, str(bucket))
    for row in leaderboard:
        best_bucket = str(row.get("best_bucket") or "unknown")
        _increment_count(leaderboard_best_bucket_counts, best_bucket)
        game_family = str(row.get("best_game_family") or "")
        if not game_family and ":" in best_bucket:
            game_family = split_bucket_key(best_bucket)[0]
        _increment_count(leaderboard_best_game_family_counts, game_family or "unknown")

    def focus_bucket(profile: dict[str, Any], bucket: str) -> dict[str, Any]:
        game_family, market_type = split_bucket_key(bucket)
        focused = dict(profile)
        focused["eligible_buckets"] = [bucket]
        focused["eligible_bucket_labels"] = [bucket_label(bucket)]
        focused["eligible_market_types"] = [market_type]
        focused["eligible_market_type_labels"] = [MARKET_TYPE_LABELS.get(market_type, market_type)]
        focused["eligible_game_families"] = [game_family]
        focused["eligible_game_family_labels"] = [GAME_FAMILY_LABELS.get(game_family, game_family.upper())]
        return enrich_esports_bucket_scores(focused, now_ts=now_ts)

    for wallet, profile in profiles_by_wallet.items():
        _increment_count(profile_grade_counts, str(profile.get("grade") or "unknown"))
        for bucket in profile.get("eligible_buckets") or []:
            bucket_key_value = str(bucket)
            _increment_count(eligible_bucket_counts, bucket_key_value)
            mode = ""
            if isinstance(profile.get("eligible_bucket_modes"), dict):
                mode = str(profile["eligible_bucket_modes"].get(bucket_key_value) or "")
            per_game_type_grades = (
                profile.get("per_game_type_grades")
                if isinstance(profile.get("per_game_type_grades"), dict)
                else {}
            )
            if not mode and isinstance(per_game_type_grades.get(bucket_key_value), dict):
                mode = str(per_game_type_grades[bucket_key_value].get("eligible_mode") or "")
            _increment_count(eligible_bucket_mode_counts, mode or "unknown")
        for market_type in profile.get("eligible_market_types") or []:
            _increment_count(eligible_market_type_counts, str(market_type))
        if normalize_wallet(wallet or profile.get("wallet")) in leaderboard_wallets:
            continue
        reasons = set(_leaderboard_reject_reasons_for_profile(profile, now_ts=now_ts))
        if not reasons and not strict_final_quality_ok(profile):
            reasons.add("strict_quality_gate")
        for reason in reasons:
            _increment_count(leaderboard_reject_reasons, reason)
        for bucket in profile.get("eligible_buckets") or []:
            bucket_key_value = str(bucket)
            focused = focus_bucket(profile, bucket_key_value)
            bucket_reasons = set(_leaderboard_reject_reasons_for_profile(focused, now_ts=now_ts))
            if not bucket_reasons and not strict_final_quality_ok(focused):
                bucket_reasons.add("strict_quality_gate")
            for reason in bucket_reasons:
                eligible_bucket_reject_reasons.setdefault(bucket_key_value, {})
                _increment_count(eligible_bucket_reject_reasons[bucket_key_value], reason)
            copyable_reasons = _copyable_reject_reasons(focused)
            if copyable_reasons:
                copyable_reject_reasons_by_bucket.setdefault(bucket_key_value, {})
                for reason in copyable_reasons:
                    _increment_count(copyable_reject_reasons_by_bucket[bucket_key_value], reason)
    return {
        "seed_bucket_min_wins": dict(seed_bucket_min_wins or {}),
        "seed_single_bucket_min_wins": int(seed_single_bucket_min_wins),
        "seed_multi_bucket_min_wins": int(seed_multi_bucket_min_wins),
        "seed_bucket_min_avg_cash": dict(seed_bucket_min_avg_cash or default_seed_bucket_min_avg_cash()),
        "seed_min_weighted_roi": float(seed_min_weighted_roi),
        "seed_max_median_avg_price": float(seed_max_median_avg_price),
        "seed_filter_reject_reasons": seed_filter_reject_reasons(
            seed_wallets,
            profile_wallets,
            seed_bucket_min_wins=seed_bucket_min_wins,
            seed_bucket_min_avg_cash=seed_bucket_min_avg_cash,
            seed_min_weighted_roi=seed_min_weighted_roi,
            seed_max_median_avg_price=seed_max_median_avg_price,
            seed_single_bucket_min_wins=seed_single_bucket_min_wins,
            seed_multi_bucket_min_wins=seed_multi_bucket_min_wins,
        ),
        "bucket_seed_wallet_counts": bucket_seed_wallet_counts(list(seed_wallets.values())),
        "bucket_profile_wallet_counts": bucket_seed_wallet_counts(profile_wallets),
        "qualified_seed_bucket_counts": _sorted_count_dict(qualified_seed_bucket_counts),
        "profile_grade_counts": _sorted_count_dict(profile_grade_counts),
        "eligible_bucket_counts": _sorted_count_dict(eligible_bucket_counts),
        "eligible_bucket_mode_counts": _sorted_count_dict(eligible_bucket_mode_counts),
        "eligible_market_type_counts": _sorted_count_dict(eligible_market_type_counts),
        "leaderboard_best_bucket_counts": _sorted_count_dict(leaderboard_best_bucket_counts),
        "leaderboard_best_game_family_counts": _sorted_count_dict(leaderboard_best_game_family_counts),
        "eligible_bucket_reject_reasons": _sorted_nested_count_dict(eligible_bucket_reject_reasons),
        "copyable_reject_reasons_by_bucket": _sorted_nested_count_dict(copyable_reject_reasons_by_bucket),
        "leaderboard_reject_reasons": _sorted_count_dict(leaderboard_reject_reasons),
    }


def seed_age_buckets(profile_wallets: list[dict[str, Any]], *, now_ts: int) -> dict[str, int]:
    counts = {
        "lt24h": 0,
        "1d_3d": 0,
        "3d_5d": 0,
        "gt5d": 0,
        "unknown": 0,
    }
    for row in profile_wallets:
        last_seed_at = to_int(row.get("last_seed_at"))
        if last_seed_at <= 0:
            counts["unknown"] += 1
            continue
        age_seconds = max(0, int(now_ts) - last_seed_at)
        if age_seconds < 86400:
            counts["lt24h"] += 1
        elif age_seconds < 3 * 86400:
            counts["1d_3d"] += 1
        elif age_seconds <= 5 * 86400:
            counts["3d_5d"] += 1
        else:
            counts["gt5d"] += 1
    return {key: value for key, value in counts.items() if value}


def collector_cached_profile_usable(
    profile: dict[str, Any] | None,
    *,
    now_ts: int,
    ttl_seconds: int,
    profile_condition_ids: set[str] | None = None,
    profile_lookback_days: int | None = None,
) -> bool:
    if ttl_seconds <= 0:
        return False
    if not should_use_cached_profile(profile, now_ts=now_ts, ttl_seconds=ttl_seconds):
        return False
    if profile_needs_schema_migration(profile):
        return False
    if profile_lookback_days is not None and to_int(profile.get("profile_lookback_days")) != int(profile_lookback_days):
        return False
    if profile_condition_ids is not None:
        cached_condition_ids = {
            str(value).lower()
            for value in (profile.get("esports_condition_ids") or [])
            if value
        }
        current_scope = {str(value).lower() for value in profile_condition_ids if value}
        # 复用条件:钱包**已交易过的市场**仍全部落在当前 scope 内。若它打过的某个盘滑出了 15 天
        # 窗口(不在 scope 了)→ 画像会变 → 必须重算。
        # 原先这里用 `!= 全 scope` 等式比较是 bug:profile 存的是钱包打过的少数盘(如 15/1642),
        # 与整 scope 永不相等 → 复用永远失效 → 每次全量重 profile(142s)。改成子集判断。
        if not cached_condition_ids.issubset(current_scope):
            return False
    return True


def collector_seed_candidate(seed_wallet: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(seed_wallet.get("candidate") or {})
    candidate.update(
        {
            "wallet": normalize_wallet(seed_wallet.get("wallet")),
            "seed_score": seed_wallet.get("seed_score"),
            "seed_win_count": seed_wallet.get("seed_win_count"),
            "seed_weighted_roi": seed_wallet.get("seed_weighted_roi"),
            "seed_bucket_counts": seed_wallet.get("seed_bucket_counts"),
            "seed_bucket_stats": seed_wallet.get("seed_bucket_stats"),
            "seed_market_type_counts": seed_wallet.get("seed_market_type_counts"),
            "qualified_seed_buckets": seed_wallet.get("qualified_seed_buckets"),
            "qualified_seed_bucket_labels": seed_wallet.get("qualified_seed_bucket_labels"),
            "qualified_seed_bucket_stats": seed_wallet.get("qualified_seed_bucket_stats"),
            "seed_bucket_min_wins": seed_wallet.get("seed_bucket_min_wins"),
            "seed_multi_bucket_min_wins": seed_wallet.get("seed_multi_bucket_min_wins"),
            "multi_bucket_seed_win_count": seed_wallet.get("multi_bucket_seed_win_count"),
            "seed_qualification_mode": seed_wallet.get("seed_qualification_mode"),
            "seed_bucket_min_avg_cash": seed_wallet.get("seed_bucket_min_avg_cash"),
            "seed_min_weighted_roi": seed_wallet.get("seed_min_weighted_roi"),
            "seed_max_median_avg_price": seed_wallet.get("seed_max_median_avg_price"),
        }
    )
    return candidate


def collector_seed_payload(seed_wallet: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in seed_wallet.items() if key != "seed_positions"}


def merge_collector_cached_profile_with_seed(cached: dict[str, Any], seed_wallet: dict[str, Any]) -> dict[str, Any]:
    wallet = normalize_wallet(seed_wallet.get("wallet") or cached.get("wallet"))
    merged = dict(cached)
    merged["wallet"] = wallet
    merged["candidate"] = collector_seed_candidate(seed_wallet)
    merged["seed"] = collector_seed_payload(seed_wallet)
    return merged


def collector_refresh_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        to_int(row.get("last_seed_at")),
        to_float(row.get("seed_score")),
        to_int(row.get("seed_win_count")),
        to_float(row.get("seed_cost_total")),
        normalize_wallet(row.get("wallet")),
    )


def build_collector_profile_refresh_plan(
    profile_wallets: list[dict[str, Any]],
    existing_profiles: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    ttl_seconds: int,
    max_refresh_profiles: int,
    profile_condition_ids: set[str] | None = None,
    profile_lookback_days: int | None = None,
) -> dict[str, Any]:
    reused_profiles_by_wallet: dict[str, dict[str, Any]] = {}
    refresh_candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for seed_wallet in profile_wallets:
        wallet = normalize_wallet(seed_wallet.get("wallet"))
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        normalized_seed = {**seed_wallet, "wallet": wallet}
        cached = existing_profiles.get(wallet)
        if collector_cached_profile_usable(
            cached,
            now_ts=now_ts,
            ttl_seconds=ttl_seconds,
            profile_condition_ids=profile_condition_ids,
            profile_lookback_days=profile_lookback_days,
        ):
            reused_profiles_by_wallet[wallet] = merge_collector_cached_profile_with_seed(cached or {}, normalized_seed)
        else:
            refresh_candidates.append(normalized_seed)
    refresh_candidates.sort(key=collector_refresh_priority_key, reverse=True)
    refresh_limit = max(0, int(max_refresh_profiles))
    refresh_plan = refresh_candidates[:refresh_limit]
    skipped_due_budget = refresh_candidates[refresh_limit:]
    return {
        "reused_profiles_by_wallet": reused_profiles_by_wallet,
        "refresh_plan": refresh_plan,
        "skipped_due_budget": skipped_due_budget,
        "stats": {
            "profile_cache_hits": len(reused_profiles_by_wallet),
            "profile_cache_misses": len(refresh_candidates),
            "profile_refresh_plan_count": len(refresh_plan),
            "profile_reused_count": len(reused_profiles_by_wallet),
            "profile_skipped_due_budget": len(skipped_due_budget),
        },
    }


def load_collector_existing_profiles(
    output_dir: Path, data_dir: Path | None = None, *, prefix: str = "collector"
) -> dict[str, dict[str, Any]]:
    del data_dir
    paths = [
        Path(output_dir) / f"{prefix}_wallet_profiles.json",
        Path(output_dir) / "wallet_profiles.json",
    ]
    for path in paths:
        rows = read_json(path, [])
        if isinstance(rows, list) and rows:
            return {
                normalize_wallet(row.get("wallet")): row
                for row in rows
                if isinstance(row, dict) and normalize_wallet(row.get("wallet"))
            }
    return {}


def _json_list_count(path: Path) -> int:
    value = read_json(path, [])
    return len(value) if isinstance(value, list) else 0


def _first_json_list_count(paths: list[Path]) -> int:
    for path in paths:
        if path.exists():
            count = _json_list_count(path)
            if count:
                return count
    return 0


def _raw_user_trade_cache_summary(raw_dir: Path, *, now_ts: int) -> dict[str, Any]:
    files = sorted(path for path in raw_dir.glob("*.json") if path.is_file()) if raw_dir.exists() else []
    sizes = [path.stat().st_size for path in files]
    mtimes = [int(path.stat().st_mtime) for path in files]
    age_buckets = {"lt24h": 0, "1d_7d": 0, "gt7d": 0}
    for mtime in mtimes:
        age_seconds = max(0, int(now_ts) - mtime)
        if age_seconds < 86400:
            age_buckets["lt24h"] += 1
        elif age_seconds < 7 * 86400:
            age_buckets["1d_7d"] += 1
        else:
            age_buckets["gt7d"] += 1
    return {
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "min_bytes": min(sizes) if sizes else 0,
        "max_bytes": max(sizes) if sizes else 0,
        "min_mtime": min(mtimes) if mtimes else 0,
        "max_mtime": max(mtimes) if mtimes else 0,
        "age_buckets": {key: value for key, value in age_buckets.items() if value},
    }


def build_collector_snapshot_diagnostics(snapshot_dir: Path, *, now_ts: int | None = None) -> dict[str, Any]:
    snapshot_dir = Path(snapshot_dir)
    now_ts = int(now_ts or time.time())
    summary_path = (
        snapshot_dir / "collector_build_summary.json"
        if (snapshot_dir / "collector_build_summary.json").exists()
        else snapshot_dir / "build_summary.json"
    )
    summary = read_json(summary_path, {}) if summary_path.exists() else {}
    return {
        "snapshot_dir": str(snapshot_dir),
        "summary_file": summary_path.name if summary_path.exists() else "",
        "stage_timings": summary.get("stage_timings") or summary.get("diagnostics", {}).get("stage_timings") or {},
        "summary_counts": {
            key: summary.get(key)
            for key in (
                "target_market_count",
                "seed_wallet_count",
                "profile_wallet_count",
                "profiled_wallet_count",
                "leaderboard_wallet_count",
                "market_position_api_fetches",
                "raw_user_trade_api_fetches",
                "raw_user_trade_cache_hits",
            )
            if key in summary
        },
        "profile_wallet_count": _first_json_list_count(
            [
                snapshot_dir / "collector_profile_wallets.json",
                snapshot_dir / "v3_profile_wallets.json",
            ]
        ),
        "wallet_profile_count": _first_json_list_count(
            [
                snapshot_dir / "collector_wallet_profiles.json",
                snapshot_dir / "v3_wallet_rawdata.json",
                snapshot_dir / "wallet_profiles.json",
            ]
        ),
        "seed_wallet_count": _first_json_list_count(
            [
                snapshot_dir / "collector_seed_wallets.json",
                snapshot_dir / "v3_seed_wallets.json",
            ]
        ),
        "raw_user_trades": _raw_user_trade_cache_summary(snapshot_dir / "raw_user_trades", now_ts=now_ts),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _record_timestamp(record: dict[str, Any], *keys: str) -> int:
    for key in keys:
        dt = parse_dt(record.get(key))
        if dt:
            return int(dt.timestamp())
    return 0


def filter_classification_set_by_lookback(
    classification_set: list[dict[str, Any]],
    *,
    now: datetime,
    lookback_days: int,
) -> list[dict[str, Any]]:
    if int(lookback_days) <= 0:
        return list(classification_set)
    cutoff = now - timedelta(days=int(lookback_days))
    rows: list[dict[str, Any]] = []
    for row in classification_set:
        end = parse_dt(row.get("end_date"))
        if not end or end > now or end < cutoff:
            continue
        rows.append(row)
    return rows


def resolve_collector_output_dir(args: argparse.Namespace) -> Path:
    explicit = getattr(args, "output_dir", None)
    if explicit:
        return Path(explicit)
    return resolve_data_dir(args)


def resolve_collector_profile_wallet_limit(
    args: argparse.Namespace,
) -> int:
    explicit = getattr(args, "max_profile_wallets", None)
    if explicit is None:
        explicit = getattr(args, "max_profiles_per_run", None)
    if explicit is None:
        explicit = 2000  # v16:扩漏斗,深采上限 700→2000
    return int(explicit or 0)


def measure_scope_density(
    client: PolymarketClient,
    tag_slug: str,
    *,
    window_days: int = CALIBRATION_WINDOW_DAYS,
    min_volume: float = 10_000.0,
    now: datetime | None = None,
    max_pages: int = 24,
) -> dict[str, Any]:
    """量一个 scope 的供给侧密度:校准窗内"已结算、够流动性的主盘"市场数 + 其结算日(算间隙)。

    只数主盘成交量 ≥ min_volume 的市场 —— 与 discovery 的流动性门对齐,反映**可打分**供给,
    而非 Gamma tag 下成千上万的微型/无人盘(否则稀疏游戏会被噪声盘虚高、间隙恒为 1 天)。
    game-agnostic(choose_main_market,不经 ALLOWED_GAME_FAMILIES 门),未注册的新游戏也能测;
    只读 Gamma、不拉交易。
    """
    now = now or datetime.now(timezone.utc)
    events = client.list_events_paginated(
        closed=True, active=None, max_pages=max_pages,
        min_end_date=now - timedelta(days=window_days), max_end_date=now,
        tag_slugs=(tag_slug,),
    )
    # closed=True ⇒ 已结算;winning_outcome_index 在 list 端 stringified 价上不可靠,不依赖它。
    end_timestamps: list[int] = []
    for event in events:
        market = choose_main_market(event)
        if not market:
            continue
        volume = to_float(market.get("volume") or market.get("volumeNum") or event.get("volume"))
        if volume < min_volume:
            continue
        end_dt = parse_dt(event.get("endDate") or event.get("end_date") or market.get("endDate"))
        if end_dt and end_dt <= now:
            end_timestamps.append(int(end_dt.timestamp()))
    return {
        "event_count": len(events),
        "markets": len(end_timestamps),
        "window_days": int(window_days),
        "min_volume": float(min_volume),
        "end_timestamps": end_timestamps,
    }


SCOPE_CALIBRATION_FILENAME = "scope_calibration.json"
SCOPE_CALIBRATION_MAX_AGE_SECONDS = 24 * 3600


def compute_scope_calibration(
    client: PolymarketClient,
    *,
    window_days: int = CALIBRATION_WINDOW_DAYS,
    min_volume: float = 10_000.0,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    """量各 game 密度 → 推每 scope 的 {lookback / n_eff / idle}。供 calibrate-scopes 命令与
    collect 起步刷新共用(单一真相源)。"""
    now = now or datetime.now(timezone.utc)
    scopes: dict[str, dict[str, Any]] = {}
    for game_family, tag_slug in ESPORTS_GAME_TAGS.items():
        density = measure_scope_density(client, tag_slug, window_days=window_days, min_volume=min_volume, now=now)
        gaps = match_day_gaps(density["end_timestamps"])
        params = derive_scope_params(markets=density["markets"], window_days=density["window_days"], gaps=gaps)
        params["event_count"] = density["event_count"]
        params["tag_slug"] = tag_slug
        scopes[game_family] = params
    return scopes


def load_scope_params(
    data_dir: Path | str,
    *,
    category: str = "esports",
    client: PolymarketClient | None = None,
    now: datetime | None = None,
    refresh: bool = False,
    window_days: int = CALIBRATION_WINDOW_DAYS,
    min_volume: float = 10_000.0,
) -> dict[str, dict[str, Any]]:
    """读 scope_calibration.json 的 per-game 参数(单一真相源,collect/observe/rescore 共用)。
    过期/缺失且给了 client → 重算并落盘;无 client 且无缓存 → {}(消费方回退全局默认,不报错)。"""
    now = now or datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    path = Path(data_dir) / SCOPE_CALIBRATION_FILENAME
    cached = read_json(path, {}) if path.exists() else {}
    fresh = bool(cached.get("scopes")) and (now_ts - int(cached.get("calibrated_at") or 0)) < SCOPE_CALIBRATION_MAX_AGE_SECONDS
    if client is not None and (refresh or not fresh):
        try:
            scopes = compute_scope_calibration(client, window_days=window_days, min_volume=min_volume, now=now)
        except Exception as exc:
            # 校准失败(API 故障 / 分页异常等)不致命:回退到磁盘缓存校准(无则 {} → 全局默认),
            # 且**不覆盖**磁盘旧校准。否则一次 API 抖动会让整个 collect 崩 —— 尤其 launcher 全量
            # 采集已先清空 data/,崩在校准 = 库被清空却没重建。宁可用旧/默认参数把 collect 跑完。
            print(json.dumps({
                "event": "scope_calibration_failed", "error": str(exc),
                "fallback": "cached" if cached.get("scopes") else "global_default",
            }), file=sys.stderr)
            return cached.get("scopes") or {}
        payload = {"category": category, "calibration_window_days": window_days, "calibrated_at": now_ts, "scopes": scopes}
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        write_json(path, payload)
        return scopes
    return cached.get("scopes") or {}


def scope_n_eff_floors(scopes: dict[str, Any]) -> dict[str, int]:
    """{game_family: n_eff_floor} —— 传给 classify_wallet/profile_candidate_wallet。"""
    return {g: int(p["n_eff_floor"]) for g, p in (scopes or {}).items() if isinstance(p, dict) and p.get("n_eff_floor")}


def scope_n_eff_anchors(scopes: dict[str, Any]) -> dict[str, int]:
    """{game_family: n_eff_floor_full} —— 满严格锚点,启用薄样本附加门(v21)。
    只返回显式带锚点的游戏;旧校准文件无此字段 → 空 map → classify 不加薄门(安全退化)。"""
    return {
        g: int(p["n_eff_floor_full"])
        for g, p in (scopes or {}).items()
        if isinstance(p, dict) and p.get("n_eff_floor_full")
    }


def scope_lookback_by_game(scopes: dict[str, Any]) -> dict[str, int]:
    """{game_family: lookback_days} —— 用于 per-game 打分窗口截断。"""
    return {g: int(p["lookback_days"]) for g, p in (scopes or {}).items() if isinstance(p, dict) and p.get("lookback_days")}


def scope_max_lookback_days(scopes: dict[str, Any], default_days: int) -> int:
    """所有 game 的最长 lookback —— discovery 一次拉取用它(再按 per-game 窗口在打分 scope 收口)。"""
    values = [int(p["lookback_days"]) for p in (scopes or {}).values() if isinstance(p, dict) and p.get("lookback_days")]
    return max([default_days, *values]) if values else int(default_days)


def filter_classification_set_by_game_window(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    lookback_by_game: dict[str, int],
    default_days: int,
) -> list[dict[str, Any]]:
    """按每个市场所属 game 的打分窗口截断(per-game profile lookback)。无 per-game 配置的游戏用 default。"""
    out: list[dict[str, Any]] = []
    for row in rows:
        game = str(row.get("game_family") or "").lower()
        days = int(lookback_by_game.get(game, default_days))
        end_dt = parse_dt(row.get("end_date"))
        if end_dt and end_dt >= now - timedelta(days=days):
            out.append(row)
    return out


def command_calibrate_scopes(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    """[只读] 量 esports 各 game 的赛事密度 → 推导自适应 lookback/n_eff/idle,打印并落 json。"""
    client = client or build_client(args)
    now = datetime.now(timezone.utc)
    window_days = int(getattr(args, "calibration_window_days", CALIBRATION_WINDOW_DAYS) or CALIBRATION_WINDOW_DAYS)
    min_volume = float(getattr(args, "calibration_min_volume", 10_000.0) or 0.0)
    scopes = compute_scope_calibration(client, window_days=window_days, min_volume=min_volume, now=now)
    payload = {
        "category": "esports",
        "calibration_window_days": window_days,
        "calibrated_at": int(now.timestamp()),
        "scopes": scopes,
    }
    out_dir = resolve_data_dir(args)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(out_dir / SCOPE_CALIBRATION_FILENAME, payload)
    # 人读表
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print("\n=== 推导参数(按密度自适应) ===")
    print(f"{'game':10s} {'mkts':>5s} {'λ/day':>6s} {'gapP50':>6s} {'gapP90':>6s} {'gapMax':>6s} "
          f"{'lookbk':>6s} {'n_eff':>5s} {'idle_h':>6s} {'idle_d':>6s}")
    for game, p in scopes.items():
        print(f"{game:10s} {p['markets']:>5d} {p['lambda_per_day']:>6.2f} {p['gap_p50_days']:>6.2f} "
              f"{p['gap_p90_days']:>6.2f} {p['gap_max_days']:>6.2f} {p['lookback_days']:>6d} "
              f"{p['n_eff_floor']:>5d} {p['idle_ceiling_hours']:>6d} {p['idle_ceiling_hours']/24:>6.1f}")
    return 0


def build_profile_candidate_from_trades(
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
            "candidate_reasons": sorted(set(seed_candidate.get("candidate_reasons") or []) | {"profitable_winner_seed"}),
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
            "candidate_reasons": sorted(set(seed_candidate.get("candidate_reasons") or []) | {"profitable_winner_seed"}),
        }
    merged = {**seed_candidate, **{key: value for key, value in deep_candidate.items() if key != "candidate_reasons"}}
    merged["wallet"] = wallet
    merged["candidate_reasons"] = sorted(
        set(seed_candidate.get("candidate_reasons") or []) | {"profitable_winner_seed", "scoped_trade_behavior"}
    )
    merged["source"] = "collector_market_positions+scoped_user_trades"
    return merged


def build_seeded_leaderboard(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    max_leaderboard_wallets: int = 60,
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
    rows = [{**row, "collector": COLLECTOR_NAME} for row in leaderboard]
    if require_strict_quality_gate:
        rows = [row for row in rows if strict_final_quality_ok(row)]
        if max_leaderboard_wallets > 0:
            rows = rows[:max_leaderboard_wallets]
    return rows


def strict_final_quality_metrics_ok(metrics: dict[str, Any]) -> bool:
    if to_float(metrics.get("esports_roi")) <= STRICT_FINAL_MIN_ROI:
        return False
    if to_float(metrics.get("positive_market_rate")) < STRICT_FINAL_MIN_POSITIVE_MARKET_RATE:
        return False
    if to_float(metrics.get("wilson_win_rate_lower_bound")) < STRICT_FINAL_MIN_WILSON:
        return False
    if to_float(metrics.get("capital_weighted_edge")) < STRICT_FINAL_MIN_CAPITAL_WEIGHTED_EDGE:
        return False
    return True


def strict_final_quality_ok(row: dict[str, Any]) -> bool:
    metrics = (
        leaderboard_rank_metrics(row)
        if row.get("best_bucket") or row.get("eligible_buckets") or row.get("eligible_market_types")
        else row
    )
    return strict_final_quality_metrics_ok(metrics)


def _copyable_reject_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    metrics = leaderboard_rank_metrics(row)
    if to_float(metrics.get("esports_roi")) < MIN_COPYABLE_BUCKET_ROI:
        reasons.append("bucket_roi_lt_min")
    bucket_key_value = str(row.get("best_bucket") or "")
    if not bucket_key_value:
        reasons.append("missing_best_bucket")
        return sorted(set(reasons))
    candidate_metrics = _candidate_bucket_metrics(row, bucket_key_value)
    if not candidate_metrics:
        reasons.append("missing_candidate_bucket")
        return sorted(set(reasons))
    two_sided_reasons = copyable_two_sided_reject_reasons(candidate_metrics, metrics)
    if two_sided_reasons:
        reasons.append("two_sided_over_limit")
        reasons.extend(two_sided_reasons)
    participated = int(candidate_metrics.get("participated_market_count") or 0)
    if participated > 0:
        tail_rate = int(candidate_metrics.get("tail_entry_market_count") or 0) / participated
        if tail_rate > MAX_COPYABLE_TAIL_ENTRY_RATE:
            reasons.append("tail_entry_over_limit")
    return sorted(set(reasons))




def _v2_candidate_metric(profile: dict[str, Any], key: str) -> Any:
    """读取在 profile 顶层或嵌套 candidate 里的发现层指标(avg_market_cash/tail/participated)。"""
    if key in profile:
        return profile.get(key)
    candidate = profile.get("candidate") if isinstance(profile.get("candidate"), dict) else {}
    return candidate.get(key)


# 写盘前 profile 瘦身:删掉只在采集/打分中间步骤用、后续 build/dashboard/复用都不读的重字段。
#   - 原始 per_type / per_game_type:display 消费方全是 `_grades or 原始`,优先读 `_grades`,
#     原始永远兜底不到;评分产出 *_grades 保留。
#   - candidate 里 4 个大嵌套块(逐桶/逐游戏 candidate 镜像 + 全市场 id 列表):V2 上榜只经
#     _v2_candidate_metric 读 candidate 的几个标量;这些大块仅 V1 build_leaderboard_from_profiles
#     用(不在 v2 collect/observe 路径)。overlap 报表用的 participated_market_ids 有
#     esports_condition_ids 作等价兜底(且后者保留)。
# 重评(改 SCORING_VERSION)从交易缓存重算 → 不读这些块;复用判定要的 scoring_version /
# esports_condition_ids / profile_lookback_days + 所有扁平标量全保留。幂等。约省 54% 体积。
_PROFILE_STORAGE_DROP_KEYS = ("per_type", "per_game_type")
_CANDIDATE_STORAGE_DROP_KEYS = (
    "per_type_candidate",
    "per_game_type_candidate",
    "per_game_family_candidate",
    "participated_market_ids",
)


def slim_profile_for_storage(profile: dict[str, Any]) -> dict[str, Any]:
    """投影出写盘用的瘦身 profile(不改评分逻辑,只去冗余;见上方常量注释)。"""
    if not isinstance(profile, dict):
        return profile
    slim = {key: value for key, value in profile.items() if key not in _PROFILE_STORAGE_DROP_KEYS}
    candidate = slim.get("candidate")
    if isinstance(candidate, dict):
        slim["candidate"] = {
            key: value for key, value in candidate.items() if key not in _CANDIDATE_STORAGE_DROP_KEYS
        }
    return slim


_V2_GRADE_RANK = {"a": 4, "b": 3, "stale": 2, "c": 1}


def v2_bucket_display_score(metrics: dict[str, Any], *, now_ts: int | None = None) -> float:
    """新轴 0-100 展示分(与上榜门同口径):近期加权胜率 θ̂ + copy-edge + 有效样本 + 活跃度。
    归一化天花板按"顶级实测水平"标定,使最强桶接近 100;刚够格的桶约 30+。
    """
    win_rate = to_float(metrics.get("bucket_win_rate"))
    copy_edge = to_float(metrics.get("bucket_copy_edge"))
    eff_sample = to_float(metrics.get("bucket_eff_sample"))
    last_trade = to_int(metrics.get("last_esports_trade_at"))
    n_wr = _clamp_float((win_rate - 0.50) / 0.28)     # 胜率 0.78 → 1.0
    n_edge = _clamp_float(copy_edge / 0.22)           # copy-edge 0.22 → 1.0
    n_eff = _clamp_float(eff_sample / 22.0)           # n_eff 22 → 1.0
    n_rec = 0.0
    if now_ts and last_trade > 0:
        n_rec = _clamp_float(1.0 - max(0, int(now_ts) - last_trade) / (14 * 86400))  # 当天 → 1.0
    return round(100.0 * (0.45 * n_wr + 0.30 * n_edge + 0.15 * n_eff + 0.10 * n_rec), 1)


def _v2_grade_rank(grade: Any) -> int:
    return _V2_GRADE_RANK.get(str(grade or "").lower(), 0)


def build_collector_leaderboard_v2(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    per_game_quota: int = V2_PER_GAME_QUOTA,
    max_leaderboard_wallets: int = V2_MAX_LEADERBOARD_WALLETS,
    include_technical: bool = V2_INCLUDE_TECHNICAL,
    min_grade: str = V2_LEADERBOARD_MIN_GRADE,
    gate_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """V2 导出:逐 game×market_type 桶的专精评估 + edge_type 标签 +(可选)每游戏配额。

    钱包在任一盘口桶够格即入榜(eligible_buckets 记录够格的盘口),不被它在别处的平庸表现
    拖累。bot/系统性双边是钱包级硬排除。不走 V1 的 classify_wallet 等级门 / strict_final /
    copyable / recent_health。
    """
    gate_kwargs = gate_kwargs or {}
    # 钱包级硬排除:bot / 系统性 material 双边 / 沉寂超时 / 尾盘进场过多(跟单跟不准)。
    # 逐桶质量门已统一到 classify_wallet_bucket 的逐桶 A(新三条),不再有独立的 v2_bucket_gate。
    max_two_sided_rate = float(gate_kwargs.get("max_two_sided_rate", V2_MAX_TWO_SIDED_RATE))
    max_bot_score = int(gate_kwargs.get("max_bot_score", V2_MAX_BOT_SCORE))
    max_idle_hours = int(gate_kwargs.get("max_idle_hours", V2_MAX_LEADERBOARD_IDLE_HOURS))
    qualified: list[dict[str, Any]] = []
    rejected_counts: dict[str, int] = {}
    for wallet, profile in profiles_by_wallet.items():
        wallet = normalize_wallet(wallet or profile.get("wallet"))
        if not wallet:
            continue
        # wallet 级硬排除
        if to_float(profile.get("two_sided_trade_market_rate")) > max_two_sided_rate:
            rejected_counts["two_sided_over_limit"] = rejected_counts.get("two_sided_over_limit", 0) + 1
            continue
        if to_int(profile.get("bot_like_score")) >= max_bot_score:
            rejected_counts["bot_like"] = rejected_counts.get("bot_like", 0) + 1
            continue
        # 钱包级沉寂硬门:最后一笔 scoped 交易超过 max_idle_hours → 不入榜(无最近交易记录同样排除)。
        if max_idle_hours > 0:
            last_trade_at = to_int(profile.get("last_esports_trade_at"))
            if not last_trade_at or (now_ts - last_trade_at) > max_idle_hours * 3600:
                rejected_counts["idle_over_limit"] = rejected_counts.get("idle_over_limit", 0) + 1
                continue
        # 榜单只发 grade ≥ 下限(默认 A):B 档不跟单 → 不上榜(profiles 池仍保留它)。
        if _v2_grade_rank(profile.get("grade")) < _v2_grade_rank(min_grade):
            rejected_counts["grade_below_floor"] = rejected_counts.get("grade_below_floor", 0) + 1
            continue
        # v21:board 级 tail 门已删。它用"整盘 avg 买入价 ≥0.75"判定追高,会把分批建仓
        # (median 入场低、avg 被晚加仓拉高)的高手误判;且与 edge_lb 评级门 + 0.85 跟单执行
        # 现价上限三重冗余 —— 复制不了的高价买单本就被 edge_lb 刷掉 + 执行层不跟。交给那两道把关。
        # 逐桶 = classify_wallet_bucket 已算好的逐桶 A(θ̂≥0.58 + n_eff≥10 + edge,非 esports/别桶已隔离)。
        per_game_type_grades = profile.get("per_game_type_grades") if isinstance(profile.get("per_game_type_grades"), dict) else {}
        eligible: list[dict[str, Any]] = []
        for bucket_key, metrics in per_game_type_grades.items():
            if not isinstance(metrics, dict) or _v2_grade_rank(metrics.get("grade")) < _v2_grade_rank(min_grade):
                continue
            game_family, market_type = split_bucket_key(bucket_key)
            bucket_edge = classify_edge_type(metrics)
            # 技术型默认不纳入(占比低 + follow 延迟跟不准卖点风险大);路径保留可一键开回。
            if not include_technical and bucket_edge == "technical":
                rejected_counts["technical_excluded"] = rejected_counts.get("technical_excluded", 0) + 1
                continue
            eligible.append(
                {
                    "bucket_key": bucket_key,
                    "game_family": str(game_family or "unknown"),
                    "market_type": str(market_type),
                    "edge_type": bucket_edge,
                    "win_rate": round(to_float(metrics.get("bucket_win_rate")), 6),
                    "copy_edge": round(to_float(metrics.get("bucket_copy_edge")), 6),
                    "eff_sample": round(to_float(metrics.get("bucket_eff_sample")), 4),
                    "median_entry_price": round(to_float(metrics.get("median_entry_price")), 6),
                    "positive_market_rate": round(to_float(metrics.get("positive_market_rate")), 6),
                    "esports_closed_count": to_int(metrics.get("esports_closed_count")),
                    "score": v2_bucket_display_score(metrics, now_ts=now_ts),
                    # 主桶排序 = 新轴展示分(高分=更准/更能赚/更活跃),并列时用 eff/edge 兜底确定性。
                    "rank_score": (
                        v2_bucket_display_score(metrics, now_ts=now_ts),
                        to_float(metrics.get("bucket_eff_sample")),
                        to_float(metrics.get("bucket_copy_edge")),
                    ),
                }
            )
        # Fallback:无任何 per-game-type 够格桶,但有 per-type(跨游戏盘口)够格桶 → 也上榜。
        # 这类钱包在单个游戏样本不够,但同一盘口跨游戏合并后过了同一道 edge_lb+n_eff 门(经 Wilson
        # 确认),是"跨游戏盘口专家"。follow 本就按 eligible_market_types 跟它们,这一步把上榜逻辑
        # 与跟单逻辑对齐(此前能跟却不上榜)。质量门完全相同,只是专精维度从"游戏×盘口"放宽到"盘口"。
        if not eligible:
            per_type_grades = profile.get("per_type_grades") if isinstance(profile.get("per_type_grades"), dict) else {}
            for market_type, metrics in per_type_grades.items():
                if not isinstance(metrics, dict) or _v2_grade_rank(metrics.get("grade")) < _v2_grade_rank(min_grade):
                    continue
                bucket_edge = classify_edge_type(metrics)
                if not include_technical and bucket_edge == "technical":
                    rejected_counts["technical_excluded"] = rejected_counts.get("technical_excluded", 0) + 1
                    continue
                eligible.append(
                    {
                        "bucket_key": f"multi:{market_type}",
                        "game_family": "multi",
                        "market_type": str(market_type),
                        "edge_type": bucket_edge,
                        "cross_game": True,
                        "win_rate": round(to_float(metrics.get("bucket_win_rate")), 6),
                        "copy_edge": round(to_float(metrics.get("bucket_copy_edge")), 6),
                        "eff_sample": round(to_float(metrics.get("bucket_eff_sample")), 4),
                        "median_entry_price": round(to_float(metrics.get("median_entry_price")), 6),
                        "positive_market_rate": round(to_float(metrics.get("positive_market_rate")), 6),
                        "esports_closed_count": to_int(metrics.get("esports_closed_count")),
                        "score": v2_bucket_display_score(metrics, now_ts=now_ts),
                        "rank_score": (
                            v2_bucket_display_score(metrics, now_ts=now_ts),
                            to_float(metrics.get("bucket_eff_sample")),
                            to_float(metrics.get("bucket_copy_edge")),
                        ),
                    }
                )
        if not eligible:
            continue
        best = max(eligible, key=lambda item: item["rank_score"])
        qualified.append(
            {
                **profile,
                "wallet": wallet,
                "collector": V2_COLLECTOR_NAME,
                "primary_game": best["game_family"],
                "best_bucket": best["bucket_key"],
                "best_market_type": best["market_type"],
                "best_bucket_score": best["score"],   # 0-100 新轴展示分(dashboard 显示)
                # 顶层提升 avg_market_cash(dashboard 读顶层;它原只在 candidate 里 → 显示 0)。
                "avg_market_cash": _v2_candidate_metric(profile, "avg_market_cash"),
                # 进榜桶内胜率 θ̂(dashboard 显示用 —— 比整体 positive_market_rate 更准、且与胜率门同口径)。
                "best_bucket_win_rate": best["win_rate"],
                "edge_type": best["edge_type"],
                "eligible_buckets": [item["bucket_key"] for item in eligible],
                # follow 循环按 eligible_market_types 判定可跟盘口;从够格桶派生,否则 v2 钱包会被跳过。
                "eligible_market_types": sorted({item["market_type"] for item in eligible}),
                "eligible_game_families": sorted({item["game_family"] for item in eligible}),
                "eligible_bucket_details": [
                    {key: value for key, value in item.items() if key != "rank_score"} for item in eligible
                ],
                "v2_rank_score": best["rank_score"],
            }
        )
    # 默认不设每游戏上限(per_game_quota<=0):榜单 = 全部够格钱包,不砍。质量由门把控,
    # 偏科由"全部纳入"自然化解(少数游戏的够格钱包也全在);需要强均衡时才设 quota>0。
    by_game: dict[str, list[dict[str, Any]]] = {}
    for row in qualified:
        by_game.setdefault(row["primary_game"], []).append(row)
    cap_per_game = int(per_game_quota) if per_game_quota and int(per_game_quota) > 0 else None
    selected: list[dict[str, Any]] = []
    game_counts: dict[str, int] = {}
    for game, rows in by_game.items():
        rows.sort(key=lambda r: r["v2_rank_score"], reverse=True)
        kept = rows[:cap_per_game] if cap_per_game is not None else rows
        game_counts[game] = len(kept)
        selected.extend(kept)
    selected.sort(key=lambda r: r["v2_rank_score"], reverse=True)
    leaderboard = selected[: max(0, int(max_leaderboard_wallets))] if max_leaderboard_wallets and max_leaderboard_wallets > 0 else selected
    for row in leaderboard:
        row.pop("v2_rank_score", None)
    edge_counts: dict[str, int] = {}
    for row in leaderboard:
        edge_counts[row.get("edge_type", "unknown")] = edge_counts.get(row.get("edge_type", "unknown"), 0) + 1
    return {
        "leaderboard": leaderboard,
        "qualified_count": len(qualified),
        "per_game_counts": game_counts,
        "edge_type_counts": edge_counts,
        "rejected_counts": rejected_counts,
    }


def _v2_gate_kwargs_from_args(args: argparse.Namespace) -> dict[str, Any]:
    # 仅钱包级硬排除门(逐桶质量统一走 classify_wallet_bucket)。
    return {
        "max_two_sided_rate": float(getattr(args, "v2_max_two_sided_rate", V2_MAX_TWO_SIDED_RATE)),
        "max_bot_score": int(getattr(args, "v2_max_bot_score", V2_MAX_BOT_SCORE)),
        "max_idle_hours": int(getattr(args, "max_leaderboard_idle_hours", V2_MAX_LEADERBOARD_IDLE_HOURS)),
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


def _command_collect_wallets(
    args: argparse.Namespace,
    client: PolymarketClient | None = None,
    *,
    variant: str = "v2",
) -> int:
    # collect-v2 是唯一管线 —— 双侧发现 + hold 口径打分 + V2 导出门 + 每游戏配额,
    # 产出隔离到 collector_v2_* / leaderboard_v2.db。
    # hold 口径 = 按"方向是否猜对(持有到结算)"算胜率/ROI:技术型(靠出场盈利、方向常错)
    # 自然被算成亏损而出局,与我们"复制方向、跟随卖出"的策略一致。提前卖出是执行层的事。
    scoring_basis = "hold"
    include_losing_side = True
    client = client or build_client(args)
    output_dir = resolve_collector_output_dir(args) / "collector_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = "collector_v2"
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    default_lookback_days = int(getattr(args, "lookback_days", V2_DEFAULT_LOOKBACK_DAYS) or V2_DEFAULT_LOOKBACK_DAYS)
    # scope 自适应:collect 起步刷新校准(单一真相源),per-game lookback/n_eff 由此派生。
    scope_params = load_scope_params(resolve_data_dir(args), client=client, now=now_dt, refresh=True)
    n_eff_floors = scope_n_eff_floors(scope_params)
    n_eff_anchors = scope_n_eff_anchors(scope_params)
    lookback_by_game = scope_lookback_by_game(scope_params)
    # discovery 一次按"最长 per-game 窗口"拉取;打分 scope 再按 per-game 窗口收口(见下方 filter)。
    lookback_days = scope_max_lookback_days(scope_params, default_lookback_days)
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
    # 打分前补拉结算:已结束但记录还没结算结果的在册市场,定向补拉 outcome_prices,
    # 关闭"已在 Polymarket 结算但本地未刷到 → 打分跳过该场漏计亏损"的时序窗口。
    resolution_backfill = backfill_market_resolutions(client, classification_set, now_ts=int(now_dt.timestamp()))
    write_json(output_dir / f"{prefix}_classification_set.json", classification_set)
    mark_stage("classification")

    target_markets, target_meta = select_collector_target_markets(
        classification_set,
        now=now_dt,
        lookback_days=lookback_days,
        bucket_market_limit=getattr(args, "bucket_market_limit", 100),
    )
    seed_bucket_min_hit_rate = float(getattr(args, "seed_bucket_min_hit_rate", SEED_BUCKET_MIN_HIT_RATE))
    raw_seed_bucket_min_wins = calculate_seed_bucket_min_wins(
        target_meta.get("bucket_counts") or {},
        min_rate=seed_bucket_min_hit_rate,
    )
    seed_single_bucket_min_wins = int(getattr(args, "seed_single_bucket_min_wins", SEED_SINGLE_BUCKET_MIN_WINS) or 0)
    seed_multi_bucket_min_wins = int(getattr(args, "seed_multi_bucket_min_wins", SEED_MULTI_BUCKET_MIN_WINS) or 0)
    seed_bucket_min_wins = effective_seed_bucket_min_wins(
        raw_seed_bucket_min_wins,
        seed_single_bucket_min_wins=seed_single_bucket_min_wins,
    )
    seed_bucket_min_avg_cash = {
        MAIN_MATCH: float(getattr(args, "seed_main_match_min_avg_cash", SEED_MAIN_MATCH_MIN_AVG_CASH)),
        GAME_WINNER: float(getattr(args, "seed_game_winner_min_avg_cash", SEED_GAME_WINNER_MIN_AVG_CASH)),
        MAP_WINNER: float(getattr(args, "seed_map_winner_min_avg_cash", SEED_MAP_WINNER_MIN_AVG_CASH)),
    }
    seed_min_weighted_roi = float(getattr(args, "seed_min_weighted_roi", SEED_MIN_WEIGHTED_ROI))
    seed_max_median_avg_price = float(getattr(args, "seed_max_median_avg_price", SEED_MAX_MEDIAN_AVG_PRICE))
    write_json(output_dir / f"{prefix}_target_markets.json", target_markets)
    mark_stage("target_markets")

    def fetch_market_positions(market: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str | None]:
        condition_id = str(market.get("condition_id") or "").lower()
        try:
            response = client.market_positions(
                condition_id,
                limit=getattr(args, "positions_per_market", 20),
                sort_by="TOTAL_PNL",
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
            collect_seed_positions(
                market_by_id.get(condition_id) or {"condition_id": condition_id},
                response,
                positions_per_market=getattr(args, "positions_per_market", 20),
                include_losing_side=include_losing_side,
            )
        )
    write_json(output_dir / f"{prefix}_seed_positions.json", seed_positions)
    mark_stage("seed_positions")

    seed_wallets = aggregate_seed_wallets(seed_positions)
    write_json(output_dir / f"{prefix}_seed_wallets.json", list(seed_wallets.values()))
    max_profile_wallets = resolve_collector_profile_wallet_limit(args)
    # v2:廉价召回 + per-game round-robin 填满 profiling 预算(破 v1 严格种子门的 411 天花板)。
    profile_wallets = filter_profile_seed_wallets_v2(
        seed_wallets,
        max_wallets=max_profile_wallets,
        min_seed_markets=getattr(args, "v2_min_seed_markets", 1),
        min_avg_seed_cash=getattr(args, "v2_min_seed_avg_cash", SEED_MIN_AVG_CASH),
    )
    write_json(output_dir / f"{prefix}_profile_wallets.json", profile_wallets)
    mark_stage("seed_wallet_filter")

    classification_condition_ids = {
        str(row.get("condition_id") or "").lower()
        for row in classification_set
        if row.get("condition_id")
    }
    profile_lookback_days = int(getattr(args, "profile_lookback_days", V2_DEFAULT_PROFILE_LOOKBACK_DAYS) or V2_DEFAULT_PROFILE_LOOKBACK_DAYS)
    # 打分 scope 按 per-game 窗口收口(valorant 30d / cs2 14d…),无 per-game 配置回退全局默认。
    profile_classification_set = filter_classification_set_by_game_window(
        classification_set,
        now=now_dt,
        lookback_by_game=lookback_by_game,
        default_days=profile_lookback_days,
    )
    condition_ids = {
        str(row.get("condition_id") or "").lower()
        for row in profile_classification_set
        if row.get("condition_id")
    }
    market_records_by_id = {
        str(row.get("condition_id") or "").lower(): row
        for row in profile_classification_set
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
    existing_profiles = load_collector_existing_profiles(output_dir, resolve_data_dir(args), prefix=prefix)
    profile_cache_ttl_hours = float(getattr(args, "collector_profile_cache_ttl_hours", 24) or 0)
    profile_refresh = build_collector_profile_refresh_plan(
        profile_wallets,
        existing_profiles,
        now_ts=now_ts,
        ttl_seconds=max(0, int(profile_cache_ttl_hours * 3600)),
        max_refresh_profiles=max_profile_wallets,
        profile_condition_ids=condition_ids,
        profile_lookback_days=profile_lookback_days,
    )
    reused_profiles_by_wallet = profile_refresh["reused_profiles_by_wallet"]
    refresh_profile_wallets = profile_refresh["refresh_plan"]
    skipped_due_budget = profile_refresh["skipped_due_budget"]

    # 拉取阶段只负责把逐钱包原始交易**落盘**(缓存),不在内存里保留 trades。
    # 旧实现把全部钱包的交易攒进 raw_user_trades_by_wallet 大 dict、贯穿整个评分阶段,
    # 内存 = O(钱包数 × 各自交易),2k+ 钱包就把小内存机顶爆(还会用 swap)。改成流式:
    # 盘是唯一数据源,评分时 profile_one 按需读单个钱包、用完即弃,峰值降到 ~max_workers 份。
    def fetch_raw_user_trades(seed_wallet: dict[str, Any]) -> tuple[str, str]:
        wallet = normalize_wallet(seed_wallet.get("wallet"))
        _trades, source = fetch_recent_esports_user_trades_for_wallet(
            client,
            wallet,
            condition_ids,
            page_limit=getattr(args, "user_history_trades_limit", 500),
            max_pages=getattr(args, "user_history_trades_max_pages", 3),
            max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
            data_dir=output_dir,
            now_ts=now_ts,
            cache_ttl_days=getattr(args, "user_trades_cache_ttl_days", 1),
            force_refresh=False,
            use_cache=True,
            include_source=True,
            retention_days=lookback_days,  # 保留到最长 per-game 窗口,打分 scope 再按 per-game 收口
        )
        del _trades  # 已落盘,不保留在内存(流式削峰的关键)
        return wallet, source

    raw_trade_results = run_ordered_io_tasks(
        refresh_profile_wallets,
        fetch_raw_user_trades,
        max_workers=getattr(args, "max_workers", 8),
    )
    raw_user_trade_errors: dict[str, str] = {}
    raw_user_trade_cache_hits = 0
    raw_user_trade_api_fetches = 0
    for index, result in enumerate(raw_trade_results):
        fallback_wallet = normalize_wallet(refresh_profile_wallets[index].get("wallet")) if index < len(refresh_profile_wallets) else ""
        if isinstance(result, Exception):
            if fallback_wallet:
                raw_user_trade_errors[fallback_wallet] = str(result)
            continue
        wallet, source = result
        if source == "cache":
            raw_user_trade_cache_hits += 1
        elif source == "api":
            raw_user_trade_api_fetches += 1
    mark_stage("raw_user_trades")

    def load_cached_user_trades(wallet: str) -> list[dict[str, Any]]:
        """评分阶段逐钱包从盘读 scope 过滤后的交易(缓存已由拉取阶段写好),不走 API。
        复用 fetch 路径同一套 _filter_esports_user_trades(同 max_esports_markets),
        产出与旧的内存 dict 完全一致。"""
        cache_path = user_trades_cache_path(output_dir, wallet)
        if not cache_path.exists():
            return []
        trades, ok = _load_raw_user_trade_cache(cache_path)
        if not ok:
            return []
        return _filter_esports_user_trades(
            trades,
            condition_ids,
            max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
        )

    def profile_one(seed_wallet: dict[str, Any]) -> dict[str, Any]:
        seed_candidate = collector_seed_candidate(seed_wallet)
        wallet = normalize_wallet(seed_candidate.get("wallet"))
        wallet_trades = load_cached_user_trades(wallet)  # 单钱包从盘读,profile_one 返回即释放
        seed_candidate = build_profile_candidate_from_trades(
            seed_candidate,
            wallet_trades,
            market_records_by_id,
        )
        profile = profile_candidate_wallet(
            seed_candidate,
            condition_ids,
            market_records_by_id=market_records_by_id,
            condition_type_by_id=condition_type_by_id,
            condition_game_family_by_id=condition_game_family_by_id,
            user_trades_loader=lambda _wallet: wallet_trades,
            current_positions_loader=lambda _wallet: [],
            now_ts=now_ts,
            scoring_basis=scoring_basis,
            n_eff_floors=n_eff_floors,
            n_eff_anchors=n_eff_anchors,
        )
        return {
            **profile,
            "profile_lookback_days": profile_lookback_days,
            "seed": collector_seed_payload(seed_wallet),
        }

    refreshed_profiles = [
        make_retryable_profile(refresh_profile_wallets[index].get("candidate") or refresh_profile_wallets[index], result, now_ts=now_ts)
        if isinstance(result, Exception)
        else result
        for index, result in enumerate(
            run_ordered_io_tasks(refresh_profile_wallets, profile_one, max_workers=getattr(args, "max_workers", 8))
        )
    ]
    profiles_by_wallet = {
        normalize_wallet(row.get("wallet")): row
        for row in [*reused_profiles_by_wallet.values(), *refreshed_profiles]
        if normalize_wallet(row.get("wallet"))
    }
    # 协调:保留 observe-v2(M4)累积发现的钱包,避免被全量重建丢掉;按打分窗口剪枝防膨胀。
    prune_cutoff = now_ts - max(1, profile_lookback_days) * 86400
    for wallet_key, cached in existing_profiles.items():
        wallet_key = normalize_wallet(wallet_key or cached.get("wallet"))
        if not wallet_key or wallet_key in profiles_by_wallet:
            continue
        last_trade = to_int(cached.get("last_esports_trade_at"))
        if last_trade and last_trade >= prune_cutoff:
            profiles_by_wallet[wallet_key] = cached
    write_json(
        output_dir / f"{prefix}_wallet_profiles.json",
        [slim_profile_for_storage(row) for row in profiles_by_wallet.values()],
    )
    mark_stage("wallet_profiles")

    collector_result = build_collector_leaderboard_v2(
        profiles_by_wallet,
        now_ts=now_ts,
        per_game_quota=getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
        max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", V2_MAX_LEADERBOARD_WALLETS),
        include_technical=bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
        gate_kwargs=_v2_gate_kwargs_from_args(args),
    )
    leaderboard = collector_result["leaderboard"]
    write_json(
        output_dir / f"{prefix}_leaderboard.json",
        [slim_profile_for_storage(row) for row in leaderboard],
    )
    mark_stage("leaderboard")

    summary = {
        "collector": COLLECTOR_NAME,
        "category": "esports",
        "lookback_days": lookback_days,
        "profile_lookback_days": profile_lookback_days,
        "classification_market_count": len(classification_set),
        "classification_condition_id_count": len(classification_condition_ids),
        "profile_scoring_market_count": len(profile_classification_set),
        "profile_condition_id_count": len(condition_ids),
        "target_market_count": len(target_markets),
        "seed_position_count": len(seed_positions),
        "seed_wallet_count": len(seed_wallets),
        "profile_wallet_count": len(profile_wallets),
        "max_profile_wallets": max_profile_wallets,
        "seed_bucket_min_hit_rate": seed_bucket_min_hit_rate,
        "seed_raw_bucket_min_wins": raw_seed_bucket_min_wins,
        "seed_bucket_min_wins": seed_bucket_min_wins,
        "seed_single_bucket_min_wins": seed_single_bucket_min_wins,
        "seed_multi_bucket_min_wins": seed_multi_bucket_min_wins,
        "seed_bucket_min_avg_cash": seed_bucket_min_avg_cash,
        "seed_min_weighted_roi": seed_min_weighted_roi,
        "seed_max_median_avg_price": seed_max_median_avg_price,
        "profiled_wallet_count": len(profiles_by_wallet),
        "collector_profile_cache_ttl_hours": profile_cache_ttl_hours,
        **profile_refresh["stats"],
        "leaderboard_wallet_count": len(leaderboard),
        "market_position_api_fetches": market_position_api_fetches,
        "market_position_errors": len(market_position_errors),
        "market_position_error_markets": market_position_errors,
        "raw_user_trade_errors": raw_user_trade_errors,
        "raw_user_trade_cache_hits": raw_user_trade_cache_hits,
        "raw_user_trade_api_fetches": raw_user_trade_api_fetches,
        "raw_user_trade_error_count": len(raw_user_trade_errors),
        "seed_age_buckets": seed_age_buckets(profile_wallets, now_ts=now_ts),
        "target_meta": target_meta,
        **build_collector_diagnostics(
            seed_wallets=seed_wallets,
            profile_wallets=profile_wallets,
            profiles_by_wallet=profiles_by_wallet,
            leaderboard=leaderboard,
            now_ts=now_ts,
            seed_bucket_min_wins=seed_bucket_min_wins,
            seed_bucket_min_avg_cash=seed_bucket_min_avg_cash,
            seed_min_weighted_roi=seed_min_weighted_roi,
            seed_max_median_avg_price=seed_max_median_avg_price,
            seed_single_bucket_min_wins=seed_single_bucket_min_wins,
            seed_multi_bucket_min_wins=seed_multi_bucket_min_wins,
        ),
        "stage_timings": {key: round(float(value), 6) for key, value in sorted(stage_timings.items())},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    summary["collector"] = V2_COLLECTOR_NAME
    summary.update(
        {
            "scoring_basis": scoring_basis,
            "include_losing_side": include_losing_side,
            "v2_include_technical": bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
            "v2_qualified_count": collector_result.get("qualified_count", 0),
            "v2_per_game_counts": collector_result.get("per_game_counts", {}),
            "v2_edge_type_counts": collector_result.get("edge_type_counts", {}),
            "v2_rejected_counts": collector_result.get("rejected_counts", {}),
            "v2_gates": _v2_gate_kwargs_from_args(args),
            "v2_per_game_quota": getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
        }
    )
    # 注:collect-v2 跑在 run-loop maybe_build 里(已 pause follow)、频率以小时计,且每次是
    #     从共享 profiles 全量重建 → 与 sidecar(observe-v2/observe-live)的 publish 竞争窗口极小
    #     且自愈;故此处暂未包 build lock(两个 sidecar 已互相串行化)。TODO 若未来观测到丢更新再补。
    dashboard_publish = publish_collector_dashboard_outputs(
        output_dir,
        resolve_data_dir(args),
        summary=summary,
        now_ts=now_ts,
        prefix=prefix,
        db_filename="leaderboard_v2.db",
        profiles_publish_name="wallet_profiles_v2.json",
        collector_name=V2_COLLECTOR_NAME,
    )
    summary["dashboard_publish"] = dashboard_publish
    write_json(output_dir / f"{prefix}_build_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def publish_collector_dashboard_outputs(
    collector_output_dir: Path,
    data_dir: Path,
    *,
    summary: dict[str, Any] | None = None,
    now_ts: int | None = None,
    prefix: str = "collector",
    db_filename: str = "leaderboard.db",
    profiles_publish_name: str = "wallet_profiles.json",
    collector_name: str = COLLECTOR_NAME,
) -> dict[str, Any]:
    collector_output_dir = Path(collector_output_dir)
    data_dir = Path(data_dir)
    now_ts = int(now_ts or time.time())
    leaderboard_value = read_json(collector_output_dir / f"{prefix}_leaderboard.json", [])
    profiles_value = read_json(collector_output_dir / f"{prefix}_wallet_profiles.json", [])
    leaderboard = [row for row in leaderboard_value if isinstance(row, dict)] if isinstance(leaderboard_value, list) else []
    profiles = [row for row in profiles_value if isinstance(row, dict)] if isinstance(profiles_value, list) else []

    data_dir.mkdir(parents=True, exist_ok=True)
    # v2 用独立的 profiles 落地名(wallet_profiles_v2.json),不覆盖 v1 dashboard 源。
    write_json(data_dir / profiles_publish_name, profiles)
    publish_summary = {
        "published": True,
        "collector": collector_name,
        "category": "esports",
        "collector_output_dir": str(collector_output_dir),
        "data_dir": str(data_dir),
        "leaderboard_db": db_filename,
        "leaderboard_wallet_count": len(leaderboard),
        "profile_wallet_count": len(profiles),
        "updated_at": now_ts,
    }
    dashboard_summary = dict(summary) if isinstance(summary, dict) else {}
    dashboard_summary.update(
        {
            "collector": collector_name,
            "category": "esports",
            "leaderboard_wallet_count": len(leaderboard),
            "profiled_wallet_count": len(profiles),
            "dashboard_publish": publish_summary,
        }
    )
    LeaderboardStore(data_dir / db_filename).publish_collection(
        category="esports",
        leaderboard=leaderboard,
        profiles=profiles,
        summary=dashboard_summary,
        updated_at=now_ts,
    )
    return publish_summary


def command_analyze_collector_snapshot(args: argparse.Namespace) -> int:
    snapshot_dir = Path(getattr(args, "snapshot_dir", "data_vps/esports") or "data_vps/esports")
    output_file = Path(getattr(args, "output_file", "") or snapshot_dir / "collector_snapshot_diagnostics.json")
    diagnostics = build_collector_snapshot_diagnostics(snapshot_dir)
    write_json(output_file, diagnostics)
    print(json.dumps(diagnostics, indent=2, sort_keys=True))
    return 0


def run_collect_v2_loop(
    args: argparse.Namespace,
    *,
    client: PolymarketClient,
    sleeper=time.sleep,
) -> int:
    """定时自动重采:每 --loop-hours 跑一次 collect-v2 刷新 leaderboard_v2.db。

    不依赖 follow 循环(M3 的 L2 单独形态)。单次失败不崩溃,退避后继续。
    --loop-max-iterations>0 时跑够即停(测试/有限轮用)。
    """
    interval = max(60, int(float(getattr(args, "loop_hours", 0) or 0) * 3600))
    retry = max(30, int(getattr(args, "loop_error_retry_seconds", 300) or 300))
    max_iter = int(getattr(args, "loop_max_iterations", 0) or 0)
    # 暂停门写到 follow 循环读的同一个 follow_dir;采集期间 follow 不开新单,DB 落库后恢复。
    follow_dir = resolve_follow_dir(args, resolve_data_dir(args))
    iterations = 0
    while True:
        now_ts = int(time.time())
        for category in FOLLOW_SIGNAL_CATEGORIES:
            set_pause_new_signals(follow_dir, category, {"status": "paused", "reason": "auto_refresh", "started_at": now_ts})
        try:
            _command_collect_wallets(args, client=client, variant="v2")
            wait = interval
        except KeyboardInterrupt:
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(follow_dir, category, None)
            return 0
        except Exception as exc:
            print(json.dumps({"event": "collect_v2_loop_error", "error": str(exc)}))
            wait = min(interval, retry)
        finally:
            # DB 已落库(或失败保持旧榜)→ 解除暂停,follow 在新榜上继续。
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(follow_dir, category, None)
        iterations += 1
        if max_iter and iterations >= max_iter:
            return 0
        sleeper(wait)


def command_collect_v2(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    # collect-v2:全新 actual 口径管线,esports-only(与现有新信号 esports-only 政策一致)。
    if float(getattr(args, "loop_hours", 0) or 0) <= 0:
        return _command_collect_wallets(args, client=client, variant="v2")
    return run_collect_v2_loop(args, client=client or build_client(args))


# ============================================================
# M4 observe-v2 —— 事件驱动增量发现(从新结算赛事;见 review/m4-observe-design.md)
# ============================================================
def detect_newly_settled_markets(
    client: PolymarketClient,
    *,
    analyzed_ids: set[str],
    now: datetime | None = None,
    lookback_hours: float = 4.0,
    gamma_pages: int = 2,
) -> list[dict[str, Any]]:
    """检测最近结算的 in-scope esports 盘口(已分析的除外)。

    窗口 lookback_hours = tick 间隔 + buffer(默认 4h 配 2h tick = 2 倍覆盖,靠去重维护 delta;
    buffer 兼顾 closed→UMA 解析延迟与 tick 抖动)。只返回已结算
    (winning_outcome 非空)、end_date 在窗口内、且未分析过的盘口。
    """
    now = now or datetime.now(timezone.utc)
    min_end = now - timedelta(hours=max(1.0, float(lookback_hours)))
    events = client.list_events_paginated(
        closed=True,
        active=None,
        max_pages=gamma_pages,
        min_end_date=min_end,
        max_end_date=now,
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
    )
    classification = build_classification_set(events, now=now, lookback_days=1)
    new_markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for market in classification:
        cid = str(market.get("condition_id") or "").lower()
        if not cid or cid in analyzed_ids or cid in seen:
            continue
        if winning_outcome_index(market) is None:  # 未结算
            continue
        end_dt = parse_dt(market.get("end_date"))
        if not end_dt or end_dt < min_end or end_dt > now:
            continue
        seen.add(cid)
        new_markets.append(market)
    return new_markets


def _delete_wallets_from_leaderboard(
    args: argparse.Namespace,
    *,
    wallets: set[str],
    follow_dir: Path,
    now_ts: int,
    source: str,
) -> list[str]:
    """淘汰的统一执行路径:从 A 榜**直接删除**给定钱包 —— 从打分池 profiles 剔除 + 删该钱包
    原始交易缓存 + 重建并发布榜(即时下榜停跟)。build lock 与 observe 进程串行化;发布窗口
    暂停开新信号,避免 runner 主循环读到半写榜。**不**碰 follow.db 跟单研究记录。返回实删钱包。"""
    targets = sorted({normalize_wallet(w) for w in wallets if normalize_wallet(w)})
    if not targets:
        return []
    data_dir = resolve_data_dir(args)
    output_dir = resolve_collector_output_dir(args) / "collector_v2"
    with acquire_build_lock(data_dir, blocking=True):
        profiles_by_wallet = dict(load_collector_existing_profiles(output_dir, data_dir, prefix="collector_v2"))
        for wallet in targets:
            profiles_by_wallet.pop(wallet, None)
            try:
                user_trades_cache_path(output_dir, wallet).unlink()
            except FileNotFoundError:
                pass
        write_json(
            output_dir / "collector_v2_wallet_profiles.json",
            [slim_profile_for_storage(row) for row in profiles_by_wallet.values()],
        )
        leaderboard = build_collector_leaderboard_v2(
            profiles_by_wallet, now_ts=now_ts,
            per_game_quota=getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
            max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", V2_MAX_LEADERBOARD_WALLETS),
            include_technical=bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
            gate_kwargs=_v2_gate_kwargs_from_args(args),
        )["leaderboard"]
        write_json(
            output_dir / "collector_v2_leaderboard.json",
            [slim_profile_for_storage(row) for row in leaderboard],
        )
        for category in FOLLOW_SIGNAL_CATEGORIES:
            set_pause_new_signals(follow_dir, category, {"status": "paused", "reason": source, "started_at": now_ts})
        try:
            publish_collector_dashboard_outputs(
                output_dir, data_dir,
                summary={"collector": V2_COLLECTOR_NAME, "category": "esports", "source": source},
                now_ts=now_ts, prefix="collector_v2", db_filename="leaderboard_v2.db",
                profiles_publish_name="wallet_profiles_v2.json", collector_name=V2_COLLECTOR_NAME,
            )
        finally:
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(follow_dir, category, None)
    return targets


def purge_legacy_demote_quarantine(
    args: argparse.Namespace,
    *,
    follow_dir: Path | str,
    now_ts: int,
) -> dict[str, Any]:
    """一次性迁移(runner 启动跑一次):历史自动降级隔离(reason=rescore_below_grade_a)的钱包
    = 当初表现差被淘汰,按新策略**直接删除**(下榜+删profile+删缓存)+ 清掉其隔离行。
    **绝不**解禁放回跟单集。人工隔离(manual_*)不动。库里没有这类行时为 no-op。"""
    follow_dir = Path(follow_dir)
    store = FollowStore(follow_dir / "follow.db")
    legacy = {
        normalize_wallet((info or {}).get("wallet") or key)
        for key, info in store.load_wallet_quarantine(category="esports").items()
        if str((info or {}).get("reason") or "") == RESCORE_QUARANTINE_REASON
    }
    legacy.discard("")
    if not legacy:
        return {"deleted": 0, "wallets": []}
    removed = _delete_wallets_from_leaderboard(
        args, wallets=legacy, follow_dir=follow_dir, now_ts=now_ts, source="legacy_demote_purge")
    store.clear_wallet_quarantine_wallets(set(legacy))
    return {"deleted": len(removed), "wallets": sorted(legacy)}


def rescore_demote_wallets(
    client: PolymarketClient,
    args: argparse.Namespace,
    *,
    wallets: set[str],
    follow_dir: Path | str | None = None,
    now_ts: int | None = None,
) -> dict[str, Any]:
    """M5 降级重评(独立于 observe-v2 周期发现,由 follow runner 按结算笔数事件触发)。

    对给定 batch 钱包用当前 scope 重 profile,跌出 grade-A 榜的 → **直接删除淘汰**(无 quarantine
    中间态):从打分池 profiles 剔除 + 删该钱包原始交易缓存 + 重建并发布 A 榜 → 即时下榜停跟。
    保留 follow.db 的跟单研究记录(信号/腿/结算/CLV),未结算仓位继续结算到底。日后若重新达标,
    observer 周期发现会把它作为新候选自然加回 A 榜。favorite(人工置顶、越过 grade-A 强制跟单)
    不被自动淘汰。删除走 build lock 与 observe 进程串行化;发布窗口暂停开新信号避免读到半写榜。
    """
    now_dt = datetime.now(timezone.utc) if now_ts is None else datetime.fromtimestamp(now_ts, tz=timezone.utc)
    now_ts = int(now_dt.timestamp())
    targets = {normalize_wallet(w) for w in wallets if normalize_wallet(w)}
    if not targets:
        return {"rescored": 0, "demoted": 0, "demoted_wallets": []}

    # data_dir/output_dir target the esports category dir (leaderboard_v2.db +
    # collector_v2 outputs); follow_dir is the SHARED follow dir (follow.db) —
    # the runner passes it explicitly since it differs from the category dir.
    data_dir = resolve_data_dir(args)
    follow_dir = Path(follow_dir) if follow_dir else resolve_follow_dir(args, data_dir)
    output_dir = resolve_collector_output_dir(args) / "collector_v2"
    follow_store = FollowStore(follow_dir / "follow.db")

    # 只重评在榜且未隔离的目标(下榜的本就不跟,已隔离的已淘汰)。
    observe_store = LeaderboardStore(data_dir / "leaderboard_v2.db")
    board_rows, _meta = observe_store.load_leaderboard(category="esports")
    board_wallets = {normalize_wallet(r.get("wallet")) for r in board_rows if normalize_wallet(r.get("wallet"))}
    quarantined = {
        normalize_wallet((info or {}).get("wallet") or key)
        for key, info in follow_store.load_wallet_quarantine(category="esports").items()
    }
    targets = (targets & board_wallets) - quarantined
    if not targets:
        return {"rescored": 0, "demoted": 0, "demoted_wallets": []}

    # scope 分类集(与 collect/observe 共用同一 scope 校准 → per-game lookback/n_eff 一致,不各自为政)。
    # rescore 读**缓存**校准(refresh=False):用上一次 collect 定的同一套参数,避免被跟钱包入榜/重评窗口错配。
    default_profile_lookback = int(getattr(args, "profile_lookback_days", V2_DEFAULT_PROFILE_LOOKBACK_DAYS) or V2_DEFAULT_PROFILE_LOOKBACK_DAYS)
    default_lookback_days = int(getattr(args, "lookback_days", V2_DEFAULT_LOOKBACK_DAYS) or V2_DEFAULT_LOOKBACK_DAYS)
    scope_params = load_scope_params(data_dir, client=None, now=now_dt, refresh=False)
    n_eff_floors = scope_n_eff_floors(scope_params)
    n_eff_anchors = scope_n_eff_anchors(scope_params)
    lookback_by_game = scope_lookback_by_game(scope_params)
    lookback_days = scope_max_lookback_days(scope_params, default_lookback_days)
    closed_events = client.list_events_paginated(
        closed=True, active=None, max_pages=getattr(args, "gamma_pages", 10),
        min_end_date=now_dt - timedelta(days=lookback_days), max_end_date=now_dt,
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
    )
    classification_set = build_classification_set(closed_events, now=now_dt, lookback_days=lookback_days)
    # 降级重评前同样补拉结算:确保刚结算的赛事被计入,自动降级判定不被陈旧结算结果误判。
    backfill_market_resolutions(client, classification_set, now_ts=now_ts)
    profile_cls = filter_classification_set_by_game_window(
        classification_set, now=now_dt, lookback_by_game=lookback_by_game, default_days=default_profile_lookback)
    condition_ids = {str(r.get("condition_id") or "").lower() for r in profile_cls if r.get("condition_id")}
    market_records_by_id = {str(r.get("condition_id") or "").lower(): r for r in profile_cls if r.get("condition_id")}
    condition_type_by_id = {cid: str(r.get("market_type") or MAIN_MATCH) for cid, r in market_records_by_id.items()}
    condition_game_family_by_id = {cid: str(r.get("game_family") or "unknown") for cid, r in market_records_by_id.items()}

    def profile_one(wallet: str) -> dict[str, Any] | None:
        wallet = normalize_wallet(wallet)
        try:
            trades, _src = fetch_recent_esports_user_trades_for_wallet(
                client, wallet, condition_ids,
                page_limit=getattr(args, "user_history_trades_limit", 500),
                max_pages=getattr(args, "user_history_trades_max_pages", 3),
                max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
                data_dir=output_dir, now_ts=now_ts,
                cache_ttl_days=0, force_refresh=True, use_cache=False, include_source=True,
                retention_days=lookback_days,
            )
        except Exception:
            return None
        seed_candidate = build_profile_candidate_from_trades(
            collector_seed_candidate({"wallet": wallet}), trades, market_records_by_id)
        profile = profile_candidate_wallet(
            seed_candidate, condition_ids,
            market_records_by_id=market_records_by_id,
            condition_type_by_id=condition_type_by_id,
            condition_game_family_by_id=condition_game_family_by_id,
            user_trades_loader=lambda _w: trades,
            current_positions_loader=lambda _w: [],
            now_ts=now_ts, scoring_basis="hold", n_eff_floors=n_eff_floors, n_eff_anchors=n_eff_anchors,
        )
        return {**profile, "profile_lookback_days": default_profile_lookback, "observed_at": now_ts}

    reprofiled: dict[str, dict[str, Any]] = {}
    for result in run_ordered_io_tasks(sorted(targets), profile_one, max_workers=getattr(args, "max_workers", 8)):
        if isinstance(result, Exception) or not result:
            continue
        wallet = normalize_wallet(result.get("wallet"))
        if wallet:
            reprofiled[wallet] = result

    def _rebuild_board(profiles: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return build_collector_leaderboard_v2(
            profiles, now_ts=now_ts,
            per_game_quota=getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
            max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", V2_MAX_LEADERBOARD_WALLETS),
            include_technical=bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
            gate_kwargs=_v2_gate_kwargs_from_args(args),
        )["leaderboard"]

    # 内存重建榜(现有 profiles 叠加重评结果)判断 batch 是否仍在 A 榜。favorite 是人工置顶、
    # 越过 grade-A 强制跟单的覆盖项 → 不自动淘汰。
    existing = load_collector_existing_profiles(output_dir, data_dir, prefix="collector_v2")
    new_board = {
        normalize_wallet(r.get("wallet"))
        for r in _rebuild_board({**existing, **reprofiled})
        if normalize_wallet(r.get("wallet"))
    }
    favorites = {
        normalize_wallet((info or {}).get("wallet") or key)
        for key, info in follow_store.load_wallet_favorites(category="esports").items()
    }
    demoted = sorted(
        wallet for wallet in targets
        if wallet in reprofiled and wallet not in new_board and wallet not in favorites
    )
    if not demoted:
        return {"rescored": len(reprofiled), "demoted": 0, "demoted_wallets": []}

    # 淘汰=直接删除(统一执行路径,见 _delete_wallets_from_leaderboard)。
    _delete_wallets_from_leaderboard(
        args, wallets=set(demoted), follow_dir=Path(follow_dir), now_ts=now_ts, source="rescore_demote")
    return {"rescored": len(reprofiled), "demoted": len(demoted), "demoted_wallets": demoted}


def _command_observe_v2(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    """M4 发现一次 tick:新结算盘 → top-PnL 双侧持仓者 → 按 scope 打分 → 合并重建 A-only 榜 → 发布。
    (降级/恢复已全部移出:降级由 follow runner 直接删除淘汰,无 quarantine 中间态、无恢复重评。)"""
    client = client or build_client(args)
    output_dir = resolve_collector_output_dir(args) / "collector_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    follow_dir = resolve_follow_dir(args, resolve_data_dir(args))
    positions_per_market = getattr(args, "positions_per_market", 20)
    observe_store = LeaderboardStore(resolve_data_dir(args) / "leaderboard_v2.db")

    new_markets = detect_newly_settled_markets(
        client,
        analyzed_ids=set(observe_store.load_observe_analyzed(now_ts=now_ts)),
        now=now_dt,
        lookback_hours=float(getattr(args, "observe_lookback_hours", 4.0)),
        gamma_pages=int(getattr(args, "observe_gamma_pages", 2)),
    )
    # M5 降级已拆出到 follow runner(按结算笔数事件触发 rescore_demote_wallets,直接删除淘汰
    # 钱包,无 quarantine 中间态、无恢复重评)。observe-v2 只负责 M4 发现:跌出的钱包若日后重新
    # 达标,会作为新候选被周期发现自然加回 A 榜。
    follow_store = FollowStore(follow_dir / "follow.db")
    profile_lookback_days = int(getattr(args, "profile_lookback_days", V2_DEFAULT_PROFILE_LOOKBACK_DAYS) or V2_DEFAULT_PROFILE_LOOKBACK_DAYS)

    # 无新结算盘 → 真没事做(省一次分类集 Gamma 调用)
    if not new_markets:
        print(json.dumps({"event": "observe_v2_tick", "new_settled_markets": 0, "rescored": 0}))
        return 0

    # 1) 发现:新结算盘 top-20 双侧持仓者(只盈利);无新盘则跳过发现层。
    seed_positions: list[dict[str, Any]] = []
    for market in new_markets:
        condition_id = str(market.get("condition_id") or "").lower()
        try:
            response = client.market_positions(condition_id, limit=positions_per_market, sort_by="TOTAL_PNL", sort_direction="DESC")
        except Exception:
            continue
        seed_positions.extend(collect_seed_positions(market, response, positions_per_market=positions_per_market, include_losing_side=True))
    seed_wallets = aggregate_seed_wallets(seed_positions)

    # 2) 分类集给 profiling 提供 scope(与 collect/rescore 共用同一 scope 校准 → per-game 一致)。
    #    observe 每 tick 读缓存校准;缓存 >24h 过期才用 client 重算(daily 保鲜,不每 tick 重算)。
    scope_params = load_scope_params(resolve_data_dir(args), client=client, now=now_dt, refresh=False)
    n_eff_floors = scope_n_eff_floors(scope_params)
    n_eff_anchors = scope_n_eff_anchors(scope_params)
    lookback_by_game = scope_lookback_by_game(scope_params)
    lookback_days = scope_max_lookback_days(scope_params, int(getattr(args, "lookback_days", V2_DEFAULT_LOOKBACK_DAYS) or V2_DEFAULT_LOOKBACK_DAYS))
    closed_events = client.list_events_paginated(
        closed=True, active=None, max_pages=getattr(args, "gamma_pages", 10),
        min_end_date=now_dt - timedelta(days=lookback_days), max_end_date=now_dt,
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
    )
    classification_set = build_classification_set(closed_events, now=now_dt, lookback_days=lookback_days)
    profile_classification_set = filter_classification_set_by_game_window(
        classification_set, now=now_dt, lookback_by_game=lookback_by_game, default_days=profile_lookback_days)
    condition_ids = {str(row.get("condition_id") or "").lower() for row in profile_classification_set if row.get("condition_id")}
    market_records_by_id = {str(row.get("condition_id") or "").lower(): row for row in profile_classification_set if row.get("condition_id")}
    condition_type_by_id = {cid: str(row.get("market_type") or MAIN_MATCH) for cid, row in market_records_by_id.items()}
    condition_game_family_by_id = {cid: str(row.get("game_family") or "unknown") for cid, row in market_records_by_id.items()}

    # 3) 合并累积 profiles;只 profile "新" 候选(已有的保留,含其 observed_at)
    existing = load_collector_existing_profiles(output_dir, resolve_data_dir(args), prefix="collector_v2")
    # 与 collect-v2 同口径:新种子先过 dust 现金门(min_avg_seed_cash,默认 100),否则小额交易者
    # 会从 observer 溜进榜(collect 有这道门、observer 没有 → 口径不一致)。
    new_seed_pool = {wallet: sw for wallet, sw in seed_wallets.items() if wallet not in existing}
    new_seed_wallets = filter_profile_seed_wallets_v2(
        new_seed_pool,
        max_wallets=resolve_collector_profile_wallet_limit(args),
        min_seed_markets=getattr(args, "v2_min_seed_markets", 1),
        min_avg_seed_cash=getattr(args, "v2_min_seed_avg_cash", SEED_MIN_AVG_CASH),
    )

    def profile_one(seed_wallet: dict[str, Any], *, cache_ttl_days: int | None = None) -> dict[str, Any]:
        wallet = normalize_wallet(seed_wallet.get("wallet"))
        ttl = getattr(args, "user_trades_cache_ttl_days", 1) if cache_ttl_days is None else cache_ttl_days
        trades, _source = fetch_recent_esports_user_trades_for_wallet(
            client, wallet, condition_ids,
            page_limit=getattr(args, "user_history_trades_limit", 500),
            max_pages=getattr(args, "user_history_trades_max_pages", 3),
            max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
            data_dir=output_dir, now_ts=now_ts,
            cache_ttl_days=ttl,
            force_refresh=False, use_cache=True, include_source=True,
            retention_days=lookback_days,
        )
        seed_candidate = build_profile_candidate_from_trades(collector_seed_candidate(seed_wallet), trades, market_records_by_id)
        profile = profile_candidate_wallet(
            seed_candidate, condition_ids,
            market_records_by_id=market_records_by_id,
            condition_type_by_id=condition_type_by_id,
            condition_game_family_by_id=condition_game_family_by_id,
            user_trades_loader=lambda _w: trades,
            current_positions_loader=lambda _w: [],
            now_ts=now_ts, scoring_basis="hold", n_eff_floors=n_eff_floors, n_eff_anchors=n_eff_anchors,
        )
        # observed_at:M4 发现并打分的时间 → dashboard 据此显示 2h "new" 标记
        return {**profile, "profile_lookback_days": profile_lookback_days, "seed": collector_seed_payload(seed_wallet), "observed_at": now_ts}

    new_profiles = [r for r in run_ordered_io_tasks(new_seed_wallets, profile_one, max_workers=getattr(args, "max_workers", 8)) if not isinstance(r, Exception)]

    # 4) 整体重建 + 发布。临界区(merge→写 profiles→build→publish)加 build lock 与
    #    collect-v2 / observe-live 串行化,避免并发 publish 的 lost-update。publish 函数本身
    #    不再取锁(避免同进程二次 flock 自死锁)。
    with acquire_build_lock(resolve_data_dir(args), blocking=True):
        profiles_by_wallet = dict(existing)
        for profile in new_profiles:   # 新候选 profile 覆盖旧的
            wallet = normalize_wallet(profile.get("wallet"))
            if wallet:
                profiles_by_wallet[wallet] = profile
        write_json(
            output_dir / "collector_v2_wallet_profiles.json",
            [slim_profile_for_storage(row) for row in profiles_by_wallet.values()],
        )
        collector_result = build_collector_leaderboard_v2(
            profiles_by_wallet, now_ts=now_ts,
            per_game_quota=getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
            max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", V2_MAX_LEADERBOARD_WALLETS),
            include_technical=bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
            gate_kwargs=_v2_gate_kwargs_from_args(args),
        )
        leaderboard = collector_result["leaderboard"]
        write_json(
            output_dir / "collector_v2_leaderboard.json",
            [slim_profile_for_storage(row) for row in leaderboard],
        )

        for category in FOLLOW_SIGNAL_CATEGORIES:
            set_pause_new_signals(follow_dir, category, {"status": "paused", "reason": "observe_v2", "started_at": now_ts})
        try:
            publish_collector_dashboard_outputs(
                output_dir, resolve_data_dir(args),
                summary={"collector": V2_COLLECTOR_NAME, "category": "esports", "source": "observe_v2"},
                now_ts=now_ts, prefix="collector_v2", db_filename="leaderboard_v2.db",
                profiles_publish_name="wallet_profiles_v2.json", collector_name=V2_COLLECTOR_NAME,
            )
        finally:
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(follow_dir, category, None)

    observe_store.record_observe_analyzed(
        [str(market.get("condition_id") or "").lower() for market in new_markets],
        now_ts=now_ts,
    )

    new_on_board = sum(1 for row in leaderboard if to_int(row.get("observed_at")) and now_ts - to_int(row.get("observed_at")) < 7200)
    print(json.dumps({
        "event": "observe_v2_tick", "new_settled_markets": len(new_markets),
        "new_candidates": len(new_seed_wallets), "profiled": len(new_profiles),
        "leaderboard": len(leaderboard), "new_on_board_2h": new_on_board,
        "rescored": 0, "demoted": 0, "recovered": 0,
    }))
    return 0


def command_observe_v2(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    if float(getattr(args, "loop_hours", 0) or 0) <= 0:
        return _command_observe_v2(args, client=client)
    cli_client = client or build_client(args)
    interval = max(60, int(float(getattr(args, "loop_hours", 0) or 0) * 3600))
    retry = max(30, int(getattr(args, "loop_error_retry_seconds", 300) or 300))
    max_iter = int(getattr(args, "loop_max_iterations", 0) or 0)
    iterations = 0
    # 作为 follow sidecar 启动时,榜单刚被手动采集刷新过,第一轮先睡满 interval 再跑,
    # 避免"刚采完就立即用另一套默认阈值重算覆盖"。
    if getattr(args, "defer_first_tick", False):
        print(json.dumps({"event": "observe_v2_deferred", "first_tick_in_seconds": interval}))
        time.sleep(interval)
    while True:
        try:
            _command_observe_v2(args, client=cli_client)
            wait = interval
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            print(json.dumps({"event": "observe_v2_loop_error", "error": str(exc)}))
            wait = min(interval, retry)
        iterations += 1
        if max_iter and iterations >= max_iter:
            return 0
        time.sleep(wait)


def _command_observe_live(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    """observe-live 一次 tick:**活跃(未结算)** watchlist 盘 → 双侧持仓者 → 用其历史
    交易评分 → grade-A **提前**写进 leaderboard(source=observe_live),让跟单侧增量补单
    看到其 live 持仓即补跟,不等结算。与 observe-v2 共用评分管线 + 同一 leaderboard.db;
    差异只在种子来源(live 盘,无 winner)。控量四道:volume 门 + top-K + 钱包级去重
    early-exit + profiles 持久化天然负缓存。发布临界区与 observe-v2/collect 用 build lock
    串行化(非阻塞,占用即跳过本轮,分钟级下轮再来)。"""
    client = client or build_client(args)
    output_dir = resolve_collector_output_dir(args) / "collector_v2"
    output_dir.mkdir(parents=True, exist_ok=True)
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    data_dir = resolve_data_dir(args)
    follow_dir = resolve_follow_dir(args, data_dir)
    positions_per_market = getattr(args, "positions_per_market", 20)
    min_volume = to_float(getattr(args, "min_market_volume", LIVE_SEED_MIN_VOLUME) or 0.0)

    # 1) 只读 follow runner 维护的 active 市场缓存(不重复 Gamma 拉取、不写 follow.db);
    #    只留未结算(winning_outcome_index None)且 volume 达门的盘。
    store = FollowStore(follow_dir / "follow.db")
    active_markets, _updated_at, _fresh = store.load_market_cache_readonly(
        cache_kind="active", now_ts=now_ts, ttl_seconds=LIVE_SEED_CACHE_TTL_SECONDS,
    )
    live_markets = [
        market for market in active_markets.values()
        if isinstance(market, dict)
        and winning_outcome_index(market) is None
        and to_float(market.get("volume")) >= min_volume
    ]
    if not live_markets:
        print(json.dumps({"event": "observe_live_tick", "live_markets": 0, "reason": "no_active_market_over_volume"}))
        return 0

    # 2) 每场双侧持仓者 → 种子(无 winner;当前盈亏只软排序不硬筛)
    # sort_by 必须用 data-api 合法枚举 TOTAL_PNL(与 collect-v2/observe-v2 一致);旧值 "CASHPNL"
    # 被 data-api 拒绝 → 每场抛异常被吞 → 0 seeds(observe-live 自上线起一直空转)。collect_live_seed_positions
    # 反正自己按 seed_cost 重排,API 排序值不影响结果。fetch 失败计数并随 tick 暴露,避免再次静默全失败。
    seed_positions: list[dict[str, Any]] = []
    position_fetch_error_count = 0
    for market in live_markets:
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        if not condition_id:
            continue
        try:
            response = client.market_positions(condition_id, limit=positions_per_market, sort_by="TOTAL_PNL", sort_direction="DESC")
        except Exception:
            position_fetch_error_count += 1
            continue
        seed_positions.extend(collect_live_seed_positions(market, response, positions_per_market=positions_per_market))
    seed_wallets = aggregate_seed_wallets(seed_positions)

    # 3) 钱包级去重:只 profile 既有 collector_v2 profiles 没有的新钱包(已打过分的进 profiles
    #    持久化 → 天然负缓存,后续轮次只是集合查找)。无新候选 → early-exit(常态,廉价)。
    existing = load_collector_existing_profiles(output_dir, data_dir, prefix="collector_v2")
    new_seed_pool = {wallet: sw for wallet, sw in seed_wallets.items() if wallet not in existing}
    new_seed_wallets = filter_profile_seed_wallets_v2(
        new_seed_pool,
        max_wallets=resolve_collector_profile_wallet_limit(args),
        min_seed_markets=getattr(args, "v2_min_seed_markets", 1),
        min_avg_seed_cash=getattr(args, "v2_min_seed_avg_cash", SEED_MIN_AVG_CASH),
    )
    if not new_seed_wallets:
        print(json.dumps({"event": "observe_live_tick", "live_markets": len(live_markets),
                          "seed_wallets": len(seed_wallets), "new_candidates": 0,
                          "position_fetch_errors": position_fetch_error_count}))
        return 0

    # 4) 评分(与 observe-v2 同口径 scope/profile —— 复用同样的模块级函数,保证 grade 一致)
    profile_lookback_days = int(getattr(args, "profile_lookback_days", V2_DEFAULT_PROFILE_LOOKBACK_DAYS) or V2_DEFAULT_PROFILE_LOOKBACK_DAYS)
    scope_params = load_scope_params(data_dir, client=client, now=now_dt, refresh=False)
    n_eff_floors = scope_n_eff_floors(scope_params)
    n_eff_anchors = scope_n_eff_anchors(scope_params)
    lookback_by_game = scope_lookback_by_game(scope_params)
    lookback_days = scope_max_lookback_days(scope_params, int(getattr(args, "lookback_days", V2_DEFAULT_LOOKBACK_DAYS) or V2_DEFAULT_LOOKBACK_DAYS))
    closed_events = client.list_events_paginated(
        closed=True, active=None, max_pages=getattr(args, "gamma_pages", 10),
        min_end_date=now_dt - timedelta(days=lookback_days), max_end_date=now_dt,
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
    )
    classification_set = build_classification_set(closed_events, now=now_dt, lookback_days=lookback_days)
    profile_classification_set = filter_classification_set_by_game_window(
        classification_set, now=now_dt, lookback_by_game=lookback_by_game, default_days=profile_lookback_days)
    condition_ids = {str(row.get("condition_id") or "").lower() for row in profile_classification_set if row.get("condition_id")}
    market_records_by_id = {str(row.get("condition_id") or "").lower(): row for row in profile_classification_set if row.get("condition_id")}
    condition_type_by_id = {cid: str(row.get("market_type") or MAIN_MATCH) for cid, row in market_records_by_id.items()}
    condition_game_family_by_id = {cid: str(row.get("game_family") or "unknown") for cid, row in market_records_by_id.items()}

    def profile_one(seed_wallet: dict[str, Any]) -> dict[str, Any]:
        wallet = normalize_wallet(seed_wallet.get("wallet"))
        trades, _source = fetch_recent_esports_user_trades_for_wallet(
            client, wallet, condition_ids,
            page_limit=getattr(args, "user_history_trades_limit", 500),
            max_pages=getattr(args, "user_history_trades_max_pages", 3),
            max_esports_markets=getattr(args, "max_esports_markets_per_wallet", 100),
            data_dir=output_dir, now_ts=now_ts,
            cache_ttl_days=getattr(args, "user_trades_cache_ttl_days", 1),
            force_refresh=False, use_cache=True, include_source=True,
            retention_days=lookback_days,
        )
        seed_candidate = build_profile_candidate_from_trades(collector_seed_candidate(seed_wallet), trades, market_records_by_id)
        profile = profile_candidate_wallet(
            seed_candidate, condition_ids,
            market_records_by_id=market_records_by_id,
            condition_type_by_id=condition_type_by_id,
            condition_game_family_by_id=condition_game_family_by_id,
            user_trades_loader=lambda _w: trades,
            current_positions_loader=lambda _w: [],
            now_ts=now_ts, scoring_basis="hold", n_eff_floors=n_eff_floors, n_eff_anchors=n_eff_anchors,
        )
        return {**profile, "profile_lookback_days": profile_lookback_days,
                "seed": collector_seed_payload(seed_wallet), "observed_at": now_ts, "seed_source": "observe_live"}

    new_profiles = [r for r in run_ordered_io_tasks(new_seed_wallets, profile_one, max_workers=getattr(args, "max_workers", 8)) if not isinstance(r, Exception)]

    # 5) 合并 + 重建 A-only 榜 + 发布;临界区加 build lock(非阻塞)与 observe-v2/collect 串行化。
    #    锁内重读 existing → 不丢其它 writer 期间新增的 profile(防 lost-update)。
    try:
        with acquire_build_lock(data_dir, blocking=False):
            profiles_by_wallet = dict(load_collector_existing_profiles(output_dir, data_dir, prefix="collector_v2"))
            for profile in new_profiles:
                wallet = normalize_wallet(profile.get("wallet"))
                if wallet:
                    profiles_by_wallet[wallet] = profile
            write_json(
                output_dir / "collector_v2_wallet_profiles.json",
                [slim_profile_for_storage(row) for row in profiles_by_wallet.values()],
            )
            collector_result = build_collector_leaderboard_v2(
                profiles_by_wallet, now_ts=now_ts,
                per_game_quota=getattr(args, "v2_per_game_quota", V2_PER_GAME_QUOTA),
                max_leaderboard_wallets=getattr(args, "max_leaderboard_wallets", V2_MAX_LEADERBOARD_WALLETS),
                include_technical=bool(getattr(args, "v2_include_technical", V2_INCLUDE_TECHNICAL)),
                gate_kwargs=_v2_gate_kwargs_from_args(args),
            )
            leaderboard = collector_result["leaderboard"]
            write_json(
                output_dir / "collector_v2_leaderboard.json",
                [slim_profile_for_storage(row) for row in leaderboard],
            )
            for category in FOLLOW_SIGNAL_CATEGORIES:
                set_pause_new_signals(follow_dir, category, {"status": "paused", "reason": "observe_live", "started_at": now_ts})
            try:
                publish_collector_dashboard_outputs(
                    output_dir, data_dir,
                    summary={"collector": V2_COLLECTOR_NAME, "category": "esports", "source": "observe_live"},
                    now_ts=now_ts, prefix="collector_v2", db_filename="leaderboard_v2.db",
                    profiles_publish_name="wallet_profiles_v2.json", collector_name=V2_COLLECTOR_NAME,
                )
            finally:
                for category in FOLLOW_SIGNAL_CATEGORIES:
                    set_pause_new_signals(follow_dir, category, None)
    except BuildLockUnavailable:
        print(json.dumps({"event": "observe_live_tick", "skipped": "build_lock_busy",
                          "new_candidates": len(new_seed_wallets), "profiled": len(new_profiles)}))
        return 0

    new_on_board = sum(1 for row in leaderboard if str(row.get("seed_source") or "") == "observe_live"
                       and to_int(row.get("observed_at")) >= now_ts)
    print(json.dumps({
        "event": "observe_live_tick", "live_markets": len(live_markets),
        "seed_wallets": len(seed_wallets), "new_candidates": len(new_seed_wallets),
        "profiled": len(new_profiles), "leaderboard": len(leaderboard), "new_live_on_board": new_on_board,
        "position_fetch_errors": position_fetch_error_count,
    }))
    return 0


def command_observe_live(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    if float(getattr(args, "loop_minutes", 0) or 0) <= 0:
        return _command_observe_live(args, client=client)
    cli_client = client or build_client(args)
    interval = max(60, int(float(getattr(args, "loop_minutes", 0) or 0) * 60))
    retry = max(30, int(getattr(args, "loop_error_retry_seconds", 120) or 120))
    max_iter = int(getattr(args, "loop_max_iterations", 0) or 0)
    iterations = 0
    if getattr(args, "defer_first_tick", False):
        print(json.dumps({"event": "observe_live_deferred", "first_tick_in_seconds": interval}))
        time.sleep(interval)
    while True:
        try:
            _command_observe_live(args, client=cli_client)
            wait = interval
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            print(json.dumps({"event": "observe_live_loop_error", "error": str(exc)}))
            wait = min(interval, retry)
        iterations += 1
        if max_iter and iterations >= max_iter:
            return 0
        time.sleep(wait)


def command_collect(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    # v2 是唯一管线;collect 走 collect-v2(esports-only)。
    return _command_collect_wallets(args, client=client, variant="v2")


def find_active_market(client: PolymarketClient, args: argparse.Namespace) -> dict[str, Any] | None:
    active_events = client.list_events_paginated(
        closed=False,
        active=True,
        max_pages=args.gamma_pages,
        order="volume24hr",
        tag_slugs=CATEGORY_TAG_SLUGS["esports"],
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


def build_position_backfill_trades(
    client: PolymarketClient,
    follow_wallets: list[dict[str, Any]],
    markets_by_condition: dict[str, dict[str, Any]],
    *,
    max_entry_price: float,
    now_ts: int,
    existing_signal_ids: set[str] | None = None,
    positions_limit: int = 200,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """启动补单(只跑一次):逐 leaderboard 钱包查当前持仓,对**已经持有**且落在
    watch scope 的仓位,合成一笔 BUY trade 走同一条 process_follow_trades 管线建 paper
    leg —— 弥补"WS 只看订阅后新成交、漏掉启动前存量持仓"。

    价格决策**完全交给** process_follow_trades / 策略(唯一现价门 = θ̂×0.95 edge 闸,
    + FOLLOWABLE_PRICE_CEILING 0.85 上限),补单不再自带"成本×1.15"闸(已删,曾把仍 +EV
    的补单因钱包买得更便宜而误杀)。trade.price=avgPrice(钱包成本);并把市场 outcome_prices[idx]
    设为现价 → process_follow_trades 以现价作为我们的 our_entry_price。
    返回 ({wallet:[trade,...]}, stats)。
    """
    asset_map = build_asset_map(markets_by_condition)
    existing_signal_ids = existing_signal_ids or set()
    by_wallet: dict[str, list[dict[str, Any]]] = {}
    stats: dict[str, int] = {
        "wallets_scanned": 0, "positions_in_scope": 0, "already_followed": 0,
        "price_ceiling_blocked": 0, "candidates": 0,
    }
    for row in follow_wallets:
        wallet = normalize_wallet(row.get("wallet"))
        if not wallet:
            continue
        try:
            positions = client.positions(wallet, limit=positions_limit)
        except Exception:
            continue
        stats["wallets_scanned"] += 1
        for pos in positions or []:
            cid = str(pos.get("conditionId") or "").lower()
            market = markets_by_condition.get(cid)
            if not market:
                continue
            token_id = str(pos.get("asset") or "")
            mapping = asset_map.get(token_id)
            if not mapping or mapping.get("conditionId") != cid:
                continue
            idx = int(mapping.get("outcomeIndex", -1))
            if idx < 0:
                continue
            # 幂等:已有该 wallet+condition+outcome 的开放信号 → 已在跟,不再补腿
            # (补单的合成 trade id 是确定性的,不挡的话每次重启都会给已有信号加重复腿)。
            if follow_signal_id(wallet, cid, idx) in existing_signal_ids:
                stats["already_followed"] += 1
                continue
            avg = to_float(pos.get("avgPrice"))
            size = to_float(pos.get("size"))
            if avg <= 0 or size <= 0:
                continue
            stats["positions_in_scope"] += 1
            try:
                quote = clob_price(token_id, "buy")
            except Exception:
                quote = None
            entry = to_float(quote) if quote is not None else 0.0
            if entry <= 0:
                entry = to_float(pos.get("curPrice"))
            if entry <= 0:
                continue
            if max_entry_price > 0 and entry > max_entry_price:   # 仅留现价上限(与 live 同),其余交策略 edge 闸
                stats["price_ceiling_blocked"] += 1
                continue
            prices = list(market.get("outcome_prices") or [])
            while len(prices) <= idx:
                prices.append(0.0)
            prices[idx] = round(entry, 8)
            market["outcome_prices"] = prices
            tid = f"backfill:{wallet}:{cid}:{idx}"
            by_wallet.setdefault(wallet, []).append({
                "conditionId": cid,
                "outcomeIndex": idx,
                "outcome_index": idx,
                "asset": token_id,
                "side": "BUY",
                "size": size,
                "price": round(avg, 8),
                "timestamp": int(now_ts),
                "id": tid,
                "transactionHash": tid,
                "source": "position_backfill",
            })
            stats["candidates"] += 1
    return by_wallet, stats


def _market_outcome_token(market: dict[str, Any], idx: int) -> str | None:
    raw = market.get("clobTokenIds") or market.get("clob_token_ids")
    if not raw:
        return None
    try:
        tokens = json.loads(raw) if isinstance(raw, str) else list(raw)
    except (ValueError, TypeError):
        return None
    return str(tokens[idx]) if 0 <= idx < len(tokens) else None


def build_position_exit_reconcile_trades(
    client: PolymarketClient,
    open_signals: list[dict[str, Any]],
    markets_by_condition: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    min_exit_price: float = 0.1,
    positions_limit: int = 500,
    recent_buy_grace_seconds: int = 300,
    price_loader=None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """运行期持仓对账(独立兜底,非每 tick):WS 可能漏抓目标钱包的卖出(重连窗口/订阅时序)。
    对传入的这批 open 跟单查目标钱包当前持仓,**目标已清仓而我们仍持有** → 查 CLOB 卖一价 →
    合成一笔全量 SELL 走 process_follow_trades → apply_follow_sell 镜像补平 → 该单结算。

    现价取 CLOB 卖一价(price_loader,默认 clob_price(...,"sell"),可注入便于测试),
    退化到市场快照 outcome_prices。安全闸:
      1. 持仓查询返回空 → 跳过该钱包(防 API 抖动把"全清仓"误判,导致错误全平);
      2. 现价 < min_exit_price(默认 0.1)→ 跳过,大概率已无盘口,留到结算认亏;
      3. 按 (condition_id, outcome 名) 匹配持仓,size>0 视为仍持有;
      4. 最近一笔买入/加仓在 recent_buy_grace_seconds 宽限期内 → 跳过(data-api positions
         对刚成交、尤其 maker 成交有索引延迟,开仓同 tick 查不到≠清仓;真实卖出靠链上 WS 兜);
      5. 该 (cid,outcome) 从没在 positions 里出现过(position_seen_at 未置)→ 跳过:
         区分"索引后消失=真清仓"与"还没被索引=延迟",杜绝开仓即被误平。
    返回 ({wallet:[sell_trade]}, stats)。是 build_position_backfill_trades 的对称操作;
    调用方负责按 60s/批量 节流,不放进每 5s 主循环。
    """
    fetch_price = price_loader or (lambda token: clob_price(token, "sell"))
    by_wallet_sigs: dict[str, list[dict[str, Any]]] = {}
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        wallet = normalize_wallet(signal.get("wallet"))
        if wallet:
            by_wallet_sigs.setdefault(wallet, []).append(signal)

    out: dict[str, list[dict[str, Any]]] = {}
    stats: dict[str, int] = {
        "wallets_checked": 0, "still_holding": 0, "exited_detected": 0,
        "synth_sells": 0, "low_price_skipped": 0, "no_price_skipped": 0, "empty_positions_skipped": 0,
        "recent_buy_skipped": 0, "unseen_skipped": 0,
    }
    for wallet, sigs in by_wallet_sigs.items():
        try:
            positions = client.positions(wallet, limit=positions_limit)
        except Exception:
            continue
        if not positions:  # 闸1:空响应不当作"全清仓",跳过避免误平
            stats["empty_positions_skipped"] += 1
            continue
        stats["wallets_checked"] += 1
        held: dict[tuple[str, str], float] = {}
        for pos in positions:
            cid = str(pos.get("conditionId") or pos.get("condition_id") or "").lower()
            name = str(pos.get("outcome") or "").strip().lower()
            held[(cid, name)] = held.get((cid, name), 0.0) + to_float(pos.get("size"))
        for signal in sigs:
            cid = str(signal.get("condition_id") or "").lower()
            name = str(signal.get("outcome") or "").strip().lower()
            idx = to_int(signal.get("outcome_index"), -1)
            if idx < 0:
                continue
            bought = sum(to_float(leg.get("wallet_trade_size")) for leg in signal.get("legs") or [])
            remaining = bought - to_float(signal.get("wallet_sell_size"))
            if held.get((cid, name), 0.0) > 1e-6:
                # 这一刻 positions 里确实查到该仓位 → 记一笔"见过",供闸5"见过才信失踪"判据(随信号落库)。
                signal["position_seen_at"] = int(now_ts)
                stats["still_holding"] += 1
                continue
            if remaining <= 1e-6:  # 我们记录里已全部卖出 → 无需补卖
                stats["still_holding"] += 1
                continue
            # positions 查不到该仓位、而我们仍持有 → 可能真清仓,也可能 data-api 尚未索引(maker 成交尤甚)。
            # 闸4:最近买入/加仓还在宽限期内 → 大概率没索引到 → 跳过(开仓同 tick 误平的直接根因)。
            last_buy_at = max((to_int(leg.get("wallet_trade_at")) for leg in signal.get("legs") or []), default=0)
            if last_buy_at and now_ts - last_buy_at < max(0, int(recent_buy_grace_seconds)):
                stats["recent_buy_skipped"] += 1
                continue
            # 闸5:从没在 positions 见过该仓位 → 是"还没被索引"而非"索引后消失" → 不当清仓。
            if not to_int(signal.get("position_seen_at")):
                stats["unseen_skipped"] += 1
                continue
            stats["exited_detected"] += 1
            market = markets_by_condition.get(cid)
            if not market:
                continue
            # 现价:优先 CLOB 卖一价(实盘要真实可成交价),退化到市场快照
            price = 0.0
            token = _market_outcome_token(market, idx)
            if token:
                try:
                    price = to_float(fetch_price(token))
                except Exception:
                    price = 0.0
            if price <= 0:
                prices = market.get("outcome_prices") or []
                price = to_float(prices[idx]) if idx < len(prices) else 0.0
            if price <= 0:  # 闸:无现价,本轮不补卖(下轮再说)
                stats["no_price_skipped"] += 1
                continue
            if price < min_exit_price:  # 闸2:价格极低,大概率无盘口 → 留到结算
                stats["low_price_skipped"] += 1
                continue
            out.setdefault(wallet, []).append({
                "conditionId": cid,
                "outcomeIndex": idx,
                "outcome_index": idx,
                "outcome": signal.get("outcome"),
                "side": "SELL",
                "size": round(max(remaining, 0.0), 8),
                "price": round(price, 8),
                "timestamp": int(now_ts),
                "id": f"reconcile-exit:{signal.get('signal_id')}:{int(now_ts)}",
                "transactionHash": f"reconcile-exit:{signal.get('signal_id')}:{int(now_ts)}",
                "source": "position_exit_reconcile",
            })
            stats["synth_sells"] += 1
    return out, stats


def command_follow(
    args: argparse.Namespace,
    client: PolymarketClient | None = None,
    *,
    emit: bool = True,
    collector: "OnchainFollowCollector | None" = None,
    backfill_positions: bool = False,
    backfilled_wallets: set[str] | None = None,
    refresh_logos: bool = True,
) -> dict[str, Any]:
    client = client or build_client(args)
    data_dir = resolve_dashboard_root(args)
    follow_dir = resolve_follow_dir(args, data_dir)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    follow_started_mono = time.monotonic()
    stage_seconds: dict[str, float] = {}

    state_path = follow_dir / "follow_state.json"
    open_path = follow_dir / "follow_signals_open.json"
    perf_path = follow_dir / "follow_performance.json"
    results_path = follow_dir / "follow_results.jsonl"
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
    # M5:实跟/重评/手动类隔离不被历史复审清除(恢复走 observe-v2 重评满冷却,或人工)。
    store.clear_revalidated_quarantine(
        leaderboard_wallets,
        validated_at=leaderboard_validated_at,
        protected_reasons=STICKY_QUARANTINE_REASONS,
    )
    store.clear_wallet_quarantine_reasons(LEGACY_TRADE_QUARANTINE_REASONS)
    favorite_rows = store.load_wallet_favorites()
    favorite_wallets = set(favorite_rows)
    quarantine_rows = store.load_wallet_quarantine()
    quarantined_wallets = set(quarantine_rows)
    eligible_wallet_rows = eligible_follow_wallets(
        leaderboard_rows,
        now_ts=now_ts,
        recency_days=args.follow_recency_days,
        quarantined_wallets=quarantined_wallets,
        favorite_wallets=favorite_wallets,
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
    # kelly 下注用:每钱包 桶→θ̂(近期加权点估胜率,与入榜轴一致)。键含 game:type(per_game_type)
    # 与 type(per_type),follow 端按信号的 market_bucket 取,取不到回退 market_type。
    # 现价门由策略内部 θ̂×0.95 实现(见 THETA_FOLLOW_DISCOUNT)。
    def _theta_map(row: dict[str, Any]) -> dict[str, float]:
        out: dict[str, float] = {}
        for src in ("per_game_type_grades", "per_type_grades"):
            for key, val in (row.get(src) or {}).items():
                if isinstance(val, dict) and val.get("bucket_win_rate") is not None:
                    out[str(key)] = to_float(val.get("bucket_win_rate"))
        return out
    bucket_theta_by_wallet = {
        f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}": _theta_map(row)
        for row in eligible_wallet_rows
    }
    eligible_leagues_by_wallet = {
        f"{str(row.get('category') or 'esports').lower()}:{row['wallet']}": {str(row.get("league") or "").lower()}
        for row in eligible_wallet_rows
        if str(row.get("category") or "esports").lower() == "sports" and str(row.get("league") or "").strip()
    }
    wallet_trade_state = store.load_wallet_trade_state()
    open_signals = prune_unfollowed_signals(store.load_open_signals())
    pending_small_buys = store.load_pending_small_buys()   # 小单累加器(跨 tick 持久)
    performance = store.load_performance()
    account_balance = store.load_account_balance()
    account_balance_configured = bool(account_balance.get("configured"))
    # 每 tick 先自愈孤儿暂停:设置 pause 的进程若已死(dashboard 重启 / collect 被杀),
    # 其 finally 不会清除 pause,这里按属主 pid 存活性当场清掉 → 跟单不会被永久静默挡住。
    pause_new_signals = reconcile_pause_new_signals(follow_dir)
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

    active_cache_started_mono = time.monotonic()
    active_markets, state, active_source = load_active_market_cache(
        client,
        state,
        cache_path=active_cache_path,
        store=store,
        now_ts=now_ts,
        gamma_pages=args.gamma_pages,
        ttl_seconds=args.event_cache_ttl_minutes * 60,
        observe_window_hours=args.observe_window_hours,
        post_start_grace_seconds=args.post_start_trade_grace_seconds,
        allowed_categories=set(FOLLOW_SIGNAL_CATEGORIES),
        preserve_condition_ids=set(open_condition_ids),
    )
    stage_seconds["active_market_cache"] = round(time.monotonic() - active_cache_started_mono, 3)
    active_markets_for_follow = {
        condition_id: market
        for condition_id, market in active_markets.items()
        if str(market.get("category") or "esports").lower() in FOLLOW_SIGNAL_CATEGORIES
    }
    # 队标抓取:Polymarket 对电竞对阵盘多数只给通用游戏图、无队标(已核实),所以未缓存的
    # 队每 tick 重抓也只会空手而归 → 由 command_run 节流到约 30min 一次(refresh_logos),
    # 避免每 tick 白花 ~1.3s。静默失败改为记一行日志,便于观测。
    if refresh_logos:
        logo_started_mono = time.monotonic()
        try:
            logo_stats = refresh_team_logo_cache_from_active_markets(
                data_dir,
                active_markets=list(active_markets.values()),
                store=store,
                timeout_seconds=4,
                max_workers=min(max(1, int(args.max_workers)), 4),
                max_events=40,
                observe_window_hours=args.observe_window_hours,
                now_ts=now_ts,
            )
            if emit and isinstance(logo_stats, dict) and to_int(logo_stats.get("updated_logo_key_count")):
                print(json.dumps({"status": "team_logo_refresh", **{
                    k: logo_stats.get(k) for k in ("watched_event_count", "fetched_event_count", "updated_logo_key_count", "total_logo_key_count")
                }}, ensure_ascii=False), flush=True)
        except Exception as exc:
            if emit:
                print(json.dumps({"status": "team_logo_refresh_error", "error": str(exc)[:200]}, ensure_ascii=False), flush=True)
        finally:
            stage_seconds["team_logo_refresh"] = round(time.monotonic() - logo_started_mono, 3)
    watched = watched_markets(
        active_markets_for_follow,
        now_ts=now_ts,
        observe_window_hours=args.observe_window_hours,
        post_start_grace_seconds=args.post_start_trade_grace_seconds,
    )
    gate_open = bool(watched or open_signals)
    # On-chain detection: keep the WS collector's subscription tracking the
    # current watched-market token ids + follow wallet set. Detection then comes
    # from the collector buffer (drained below) instead of data-api polling.
    onchain_asset_map: dict[str, dict] = {}
    if collector is not None:
        onchain_asset_map = build_asset_map(watched)
        collector.update_asset_map(onchain_asset_map)
        collector.update_wallets({normalize_wallet(row.get("wallet")) for row in follow_wallets})
    detection_source = "onchain" if (collector is not None and collector.healthy) else "data_api"
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
    market_type_not_eligible_count = 0
    opposite_blocked_count = 0
    contested_signal_count = 0
    closing_line_snapshot_count = 0
    cold_start_wallet_count = 0
    backfill_legs_opened = 0
    backfill_block_breakdown: dict[str, int] = {}
    backfill_ran = False
    backfill_stats: dict[str, int] = {}
    trade_request_count = 0
    insufficient_balance_count = 0
    low_entry_price_blocked_count = 0
    high_entry_price_blocked_count = 0
    small_wallet_trade_blocked_count = 0
    small_add_blocked_count = 0
    signal_cap_limited_count = 0
    signal_cap_blocked_count = 0
    strategy_invalid_count = 0
    stake_below_minimum_count = 0
    condition_order_cap_blocked_count = 0
    wallet_condition_order_cap_blocked_count = 0
    condition_stake_cap_blocked_count = 0
    wallet_fetch_error_count = 0
    wallet_fetch_seconds: list[float] = []
    observed_delay_values: list[int] = []
    index_lag_lower_bound_values: list[int] = []
    bankroll_usdc = effective_bankroll_usdc(args.bankroll_usdc)
    if account_balance_configured:
        bankroll_usdc = to_float(account_balance.get("balance_usdc")) + funded_open_exposure(open_signals)
    max_signal_stake_usdc = to_float(getattr(args, "max_signal_stake_usdc", 0.0))
    max_signal_stake_balance_percent = to_float(getattr(args, "max_signal_stake_balance_percent", 0.0))
    if max_signal_stake_usdc <= 0 and max_signal_stake_balance_percent > 0 and math.isfinite(bankroll_usdc):
        max_signal_stake_usdc = round(bankroll_usdc * max_signal_stake_balance_percent / 100.0, 8)
    strategy_source = str(getattr(args, "strategy_source", "auto") or "auto").lower()
    db_strategy = store.load_follow_strategy() if strategy_source in {"auto", "db"} else {"configured": False}
    follow_strategy = None
    legacy_follow_strategy = None
    strategy_loaded_from = "legacy"
    if strategy_source in {"auto", "db"} and db_strategy.get("configured"):
        valid_strategy, strategy_errors = validate_follow_strategy(db_strategy)
        if not valid_strategy and strategy_source == "db":
            raise RuntimeError(f"invalid_follow_strategy:{','.join(strategy_errors)}")
        if valid_strategy:
            follow_strategy = db_strategy
            strategy_loaded_from = "db"
    if follow_strategy is None:
        if strategy_source == "db":
            raise RuntimeError("follow_strategy_required")
        legacy_follow_strategy = strategy_from_legacy_args(
            stake_usdc=args.stake_usdc,
            stake_ratio_percent=args.stake_ratio_percent,
            max_stake_usdc=getattr(args, "max_stake_usdc", 0.0),
            max_signal_stake_usdc=max_signal_stake_usdc,
            min_wallet_trade_cash_usdc=10.0,
            balance_usdc=account_balance.get("balance_usdc") if account_balance_configured else None,
        )

    # 现价上限:策略里显式配了 max_follow_entry_price 就用它(含 0=不限),否则回退 CLI 默认
    # (--max-entry-price 0.85)。老策略没这字段 → 保持 CLI 默认不变。
    effective_max_entry_price = args.max_entry_price
    if follow_strategy and follow_strategy.get("configured"):
        strat_max_entry = (follow_strategy.get("prefilters") or {}).get("max_follow_entry_price")
        if strat_max_entry is not None:
            effective_max_entry_price = to_float(strat_max_entry)

    tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
    tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
    if gate_open and follow_wallets:
        # 增量补单:把钱包**进入跟单集合前已有**的、落在 watch scope 的持仓,按现价补成
        # paper leg(同一套策略过滤 + signal_id 判重,所以 WS 之后看到同一钱包加仓也不会重复开)。
        # 每个钱包只补一次——startup 全量补,之后 live-seed 中途晋升的新钱包随到随补
        # (其入榜前的存量持仓 WS 看不到)。已补集合(backfilled_wallets)跨 tick 由 command_run 持有。
        bf_seen = backfilled_wallets if backfilled_wallets is not None else set()
        wallets_to_backfill = [
            row for row in follow_wallets
            if normalize_wallet(row.get("wallet")) and normalize_wallet(row.get("wallet")) not in bf_seen
        ]
        if backfill_positions and wallets_to_backfill:
            markets_for_bf = active_markets_for_follow or watched
            bf_by_wallet, backfill_stats = build_position_backfill_trades(
                client, wallets_to_backfill, markets_for_bf,
                max_entry_price=effective_max_entry_price, now_ts=now_ts,
                existing_signal_ids={str(s.get("signal_id") or "") for s in open_signals},
            )
            for row in wallets_to_backfill:
                wallet = normalize_wallet(row.get("wallet"))
                bf_seen.add(wallet)   # 标记已补(无论有没有存量持仓),避免每 tick 重查 positions
                bf_trades = bf_by_wallet.get(wallet)
                if not bf_trades:
                    continue
                category = str(row.get("category") or "esports").lower()
                scope_key = str(row.get("scope_key") or f"{category}:{wallet}")
                if not (scope_key in eligible_wallet_set and category not in paused_new_signal_categories):
                    continue
                before_ids = {signal.get("signal_id") for signal in open_signals}
                open_signals, _bf_pft_stats = process_follow_trades(
                    open_signals,
                    wallet=wallet,
                    trades=bf_trades,
                    markets_by_condition=markets_for_bf,
                    now_ts=now_ts,
                    stake_usdc=args.stake_usdc,
                    max_follow_legs=args.max_follow_legs,
                    max_slippage=1.0,  # 15% 成本闸已在 helper 应用,放开绝对滑点闸避免二次拦
                    min_wallet_entry_price=args.min_wallet_entry_price,
                    max_entry_price=effective_max_entry_price,
                    stake_ratio_percent=args.stake_ratio_percent,
                    require_pre_match=args.require_pre_match,
                    post_start_grace_seconds=args.post_start_trade_grace_seconds,
                    quarantine_sell_frac=args.quarantine_sell_frac,
                    eligible_market_types=eligible_market_types_by_wallet.get(scope_key),
                    eligible_buckets=eligible_buckets_by_wallet.get(scope_key),
                    bucket_theta=bucket_theta_by_wallet.get(scope_key),
                    eligible_category=category,
                    eligible_leagues=eligible_leagues_by_wallet.get(scope_key),
                    conflict_policy="dual_follow",
                    bankroll_usdc=bankroll_usdc,
                    max_stake_usdc=getattr(args, "max_stake_usdc", 0.0),
                    max_signal_stake_usdc=max_signal_stake_usdc,
                    follow_strategy=follow_strategy,
                )
                backfill_legs_opened += len({signal.get("signal_id") for signal in open_signals} - before_ids)
                # 汇总各候选未开仓的拦截原因(eligibility / low_entry / small_wallet / no_live_edge …),
                # 便于诊断"candidates 多但 opened 少"卡在哪道闸。
                for stat_key, stat_val in (_bf_pft_stats or {}).items():
                    if (stat_key.endswith("_count") and stat_key not in ("funded_stake_usdc",)
                            and not stat_key.startswith("new_leg")) and to_int(stat_val) > 0:
                        backfill_block_breakdown[stat_key] = backfill_block_breakdown.get(stat_key, 0) + to_int(stat_val)
            backfill_ran = True
            if emit:
                print(json.dumps({"status": "position_backfill", "opened_legs": backfill_legs_opened,
                                  "block_breakdown": backfill_block_breakdown, **backfill_stats}, ensure_ascii=False), flush=True)

        def fetch_trades_for_wallet(row: dict[str, Any]) -> tuple[str, list[dict], dict[str, Any]]:
            wallet = normalize_wallet(row.get("wallet"))
            scope_key = str(row.get("scope_key") or f"{str(row.get('category') or 'esports').lower()}:{wallet}")
            previous_state = wallet_trade_state.get(scope_key) or wallet_trade_state.get(wallet) or {}
            previous_cursor = previous_state.get("last_trade_cursor")
            fetch_started_at = int(datetime.now(timezone.utc).timestamp())
            fetch_started_mono = time.monotonic()
            meta = {
                "fetch_started_at": fetch_started_at,
                "previous_poll_at": to_int(previous_state.get("last_seen_at")),
            }
            trades: list[dict] = []
            try:
                trades = fetch_user_trades_until_cursor(
                    client,
                    wallet,
                    previous_cursor=previous_cursor,
                    limit=args.user_trades_limit,
                    max_pages=args.user_trades_max_pages,
                )
            except Exception as exc:
                meta["fetch_completed_at"] = int(datetime.now(timezone.utc).timestamp())
                meta["fetch_seconds"] = round(time.monotonic() - fetch_started_mono, 3)
                meta["error"] = exc.__class__.__name__
                return scope_key, [], meta
            meta["fetch_completed_at"] = int(datetime.now(timezone.utc).timestamp())
            meta["fetch_seconds"] = round(time.monotonic() - fetch_started_mono, 3)
            return scope_key, trades, meta

        wallet_fetch_started_mono = time.monotonic()
        if detection_source == "onchain":
            # Near-real-time path: drain on-chain fills (OrderFilled, deduped,
            # maker=wallet). Each fill carries the EXACT on-chain fill price
            # (USDC/shares) — no clob_price proxy. Sub-fills of one order (operator
            # matched several makers) are aggregated per (tx, token, side) into one
            # trade with the cash-weighted avg price. Our paper entry MIRRORS that
            # real price (refresh the market snapshot so process_follow_trades reads it).
            drained = collector.drain()
            trade_results = []
            for row in follow_wallets:
                wallet = normalize_wallet(row.get("wallet"))
                scope_key = str(row.get("scope_key") or f"{str(row.get('category') or 'esports').lower()}:{wallet}")
                fills = drained.get(wallet) or []
                if not fills:
                    continue
                previous_state = wallet_trade_state.get(scope_key) or wallet_trade_state.get(wallet) or {}
                agg: dict[tuple[str, str, str], dict[str, Any]] = {}
                for fill in fills:
                    key = (fill["transactionHash"], fill["tokenId"], fill["side"])
                    bucket = agg.setdefault(key, {"fill": fill, "size": 0.0, "cash": 0.0})
                    bucket["size"] += to_float(fill.get("size"))
                    bucket["cash"] += to_float(fill.get("cash"))
                trades = []
                for bucket in agg.values():
                    base = bucket["fill"]
                    size = bucket["size"]
                    price = (bucket["cash"] / size) if size > 0 else to_float(base.get("price"))
                    merged = {**base, "size": round(size, 6), "price": round(price, 6), "cash": round(bucket["cash"], 6)}
                    market = active_markets_for_follow.get(base["conditionId"]) or watched.get(base["conditionId"]) or {}
                    if market and price > 0:
                        prices = list(market.get("outcome_prices") or [])
                        idx = base["outcomeIndex"]
                        while len(prices) <= idx:
                            prices.append(0.0)
                        prices[idx] = price
                        market["outcome_prices"] = prices
                    trades.append(fill_to_trade(merged))
                meta = {
                    "fetch_started_at": now_ts,
                    "fetch_completed_at": now_ts,
                    "previous_poll_at": to_int(previous_state.get("last_seen_at")),
                    "fetch_seconds": 0.0,
                    "source": "onchain",
                }
                trade_results.append((scope_key, trades, meta))
            trade_request_count = len(trade_results)
        else:
            trade_results = run_ordered_io_tasks(
                follow_wallets,
                fetch_trades_for_wallet,
                max_workers=args.max_workers,
            )
            trade_request_count = len(follow_wallets)
        stage_seconds["wallet_trade_fetch"] = round(time.monotonic() - wallet_fetch_started_mono, 3)
        wallet_process_started_mono = time.monotonic()

        def record_observed_delay(trades: list[dict[str, Any]], *, observed_at: int, previous_poll_at: int) -> None:
            for trade in trades:
                trade_ts = trade_timestamp(trade)
                if not trade_ts:
                    continue
                observed_delay_values.append(max(0, observed_at - trade_ts))
                if previous_poll_at > 0 and trade_ts <= previous_poll_at:
                    index_lag_lower_bound_values.append(max(0, previous_poll_at - trade_ts))

        for result in trade_results:
            if isinstance(result, Exception):
                continue
            scope_key, trades, fetch_meta = result
            fetch_seconds_value = to_float(fetch_meta.get("fetch_seconds")) if isinstance(fetch_meta, dict) else 0.0
            if fetch_seconds_value > 0:
                wallet_fetch_seconds.append(fetch_seconds_value)
            if isinstance(fetch_meta, dict) and fetch_meta.get("error"):
                wallet_fetch_error_count += 1
            category, wallet = scope_key.split(":", 1)
            wallet_can_open_new = scope_key in eligible_wallet_set and category not in paused_new_signal_categories
            previous_wallet_state = wallet_trade_state.get(scope_key) or wallet_trade_state.get(wallet) or {}
            next_wallet_state = dict(previous_wallet_state)
            previous_cursor = previous_wallet_state.get("last_trade_cursor")
            observed_at = to_int(fetch_meta.get("fetch_completed_at")) if isinstance(fetch_meta, dict) else now_ts
            if observed_at <= 0:
                observed_at = now_ts
            previous_poll_at = to_int(fetch_meta.get("previous_poll_at")) if isinstance(fetch_meta, dict) else 0
            current_tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
            current_tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
            current_markets_for_follow = {
                condition_id: market
                for condition_id, market in active_markets_for_follow.items()
                if condition_id in current_tracked_condition_ids or condition_id in watched
            }
            if wallet_can_open_new:
                wallet_tracked_condition_ids = current_tracked_condition_ids
            else:
                wallet_tracked_condition_ids = open_condition_ids_by_wallet.get(scope_key, set())
            if detection_source == "onchain":
                # The collector only emits fills observed AFTER subscribing and
                # dedups them, so every drained trade is genuinely new — no
                # cold-start swallow (that's a data-api "diff full history" notion).
                new_trades = sorted(trades, key=lambda row: (trade_timestamp(row), trade_id(row)))
                cold_start = False
                if new_trades:
                    last = new_trades[-1]
                    next_cursor = {"timestamp": trade_timestamp(last), "id": trade_id(last)}
                else:
                    next_cursor = previous_cursor
            else:
                new_trades, next_cursor, cold_start = select_new_trades(trades, previous_cursor)
            if cold_start:
                cold_start_wallet_count += 1
                next_wallet_state.update({
                    "last_trade_cursor": next_cursor,
                    "last_seen_at": now_ts,
                    "wallet": wallet,
                    "category": category,
                })
                wallet_trade_state[scope_key] = next_wallet_state
                continue
            watched_trades = [trade for trade in new_trades if trade_condition_id(trade) in wallet_tracked_condition_ids]
            record_observed_delay(watched_trades, observed_at=observed_at, previous_poll_at=previous_poll_at)
            before_ids = {signal.get("signal_id") for signal in open_signals}
            open_signals, stats = process_follow_trades(
                open_signals,
                wallet=wallet,
                trades=watched_trades,
                markets_by_condition=current_markets_for_follow or watched,
                now_ts=now_ts,
                observed_at=observed_at,
                previous_poll_at=previous_poll_at,
                stake_usdc=args.stake_usdc,
                max_follow_legs=args.max_follow_legs,
                max_slippage=args.max_slippage_over_entry,
                min_wallet_entry_price=args.min_wallet_entry_price,
                max_entry_price=effective_max_entry_price,
                stake_ratio_percent=args.stake_ratio_percent,
                require_pre_match=args.require_pre_match,
                post_start_grace_seconds=args.post_start_trade_grace_seconds,
                quarantine_sell_frac=args.quarantine_sell_frac,
                eligible_market_types=eligible_market_types_by_wallet.get(scope_key) if wallet_can_open_new else None,
                eligible_buckets=eligible_buckets_by_wallet.get(scope_key) if wallet_can_open_new else None,
                bucket_theta=bucket_theta_by_wallet.get(scope_key),
                eligible_category=category if wallet_can_open_new else None,
                eligible_leagues=eligible_leagues_by_wallet.get(scope_key) if wallet_can_open_new else None,
                conflict_policy="dual_follow",
                bankroll_usdc=bankroll_usdc,
                max_stake_usdc=getattr(args, "max_stake_usdc", 0.0),
                max_signal_stake_usdc=max_signal_stake_usdc,
                follow_strategy=follow_strategy,
                pending_small_buys=pending_small_buys,
            )
            after_ids = {signal.get("signal_id") for signal in open_signals}
            new_signal_count += len(after_ids - before_ids)
            total_new_trade_count += len(new_trades)
            watched_new_trade_count += len(watched_trades)
            ignored_trade_count += stats.get("ignored_trade_count", 0) + (len(new_trades) - len(watched_trades))
            insufficient_balance_count += stats.get("insufficient_balance_count", 0)
            market_type_not_eligible_count += stats.get("market_type_not_eligible_count", 0)
            low_entry_price_blocked_count += stats.get("low_entry_price_blocked_count", 0)
            high_entry_price_blocked_count += stats.get("high_entry_price_blocked_count", 0)
            small_wallet_trade_blocked_count += stats.get("small_wallet_trade_blocked_count", 0)
            small_add_blocked_count += stats.get("small_add_blocked_count", 0)
            signal_cap_limited_count += stats.get("signal_cap_limited_count", 0)
            signal_cap_blocked_count += stats.get("signal_cap_blocked_count", 0)
            strategy_invalid_count += stats.get("strategy_invalid_count", 0)
            stake_below_minimum_count += stats.get("stake_below_minimum_count", 0)
            condition_order_cap_blocked_count += stats.get("condition_order_cap_blocked_count", 0)
            wallet_condition_order_cap_blocked_count += stats.get("wallet_condition_order_cap_blocked_count", 0)
            condition_stake_cap_blocked_count += stats.get("condition_stake_cap_blocked_count", 0)
            exited_signal_count += stats.get("exited_signal_count", 0)
            hedge_event_count += stats.get("hedge_event_count", 0)
            opposite_blocked_count += stats.get("opposite_blocked_count", 0)
            next_wallet_state.update({
                "last_trade_cursor": next_cursor,
                "last_seen_at": now_ts,
                "wallet": wallet,
                "category": category,
            })
            wallet_trade_state[scope_key] = next_wallet_state
        stage_seconds["wallet_trade_process"] = round(time.monotonic() - wallet_process_started_mono, 3)
    else:
        stage_seconds["wallet_trade_fetch"] = 0.0
        stage_seconds["wallet_trade_process"] = 0.0

    state["wallet_trade_state"] = wallet_trade_state
    state["updated_at"] = now_ts

    # 独立持仓对账兜底(非每 5s 主循环):WS 可能漏抓目标钱包卖出。每 reconcile_interval(默认
    # 60s)只对**最久未核对的最多 reconcile_batch(默认 20)笔** open 跟单查目标持仓 —— 目标已清仓
    # → 查 CLOB 卖价镜像平仓、该单结算。跟很多单时分批轮转,避免把 data-api 打爆。
    # 节流游标用每信号 position_reconcile_at 持久化(随 open_signals 落库),全局闸 = max(stamp)。
    reconcile_interval = int(getattr(args, "reconcile_interval_seconds", 60) or 0)
    reconcile_batch = int(getattr(args, "reconcile_batch_size", 20) or 0)
    open_for_reconcile = [s for s in open_signals if (s.get("status") or "open") == "open"]
    if open_for_reconcile and reconcile_interval > 0 and reconcile_batch > 0 and (
        now_ts - max((to_int(s.get("position_reconcile_at")) for s in open_for_reconcile), default=0) >= reconcile_interval
    ):
        due = sorted(open_for_reconcile, key=lambda s: to_int(s.get("position_reconcile_at")))[:reconcile_batch]
        for signal in due:
            signal["position_reconcile_at"] = now_ts
        markets_for_reconcile = active_markets_for_follow or watched
        exit_by_wallet, exit_reconcile_stats = build_position_exit_reconcile_trades(
            client, due, markets_for_reconcile, now_ts=now_ts,
            recent_buy_grace_seconds=int(getattr(args, "reconcile_recent_buy_grace_seconds", 300) or 0),
        )
        for reconcile_wallet, exit_trades in exit_by_wallet.items():
            open_signals, _exit_pft_stats = process_follow_trades(
                open_signals,
                wallet=reconcile_wallet,
                trades=exit_trades,
                markets_by_condition=markets_for_reconcile,
                now_ts=now_ts,
                stake_usdc=args.stake_usdc,
                max_follow_legs=args.max_follow_legs,
                max_slippage=1.0,
                min_wallet_entry_price=args.min_wallet_entry_price,
                max_entry_price=effective_max_entry_price,
                stake_ratio_percent=args.stake_ratio_percent,
                require_pre_match=args.require_pre_match,
                post_start_grace_seconds=args.post_start_trade_grace_seconds,
                quarantine_sell_frac=args.quarantine_sell_frac,
                conflict_policy="dual_follow",
                bankroll_usdc=bankroll_usdc,
                max_stake_usdc=getattr(args, "max_stake_usdc", 0.0),
                max_signal_stake_usdc=max_signal_stake_usdc,
                follow_strategy=follow_strategy,
            )
            exited_signal_count += _exit_pft_stats.get("exited_signal_count", 0)
        if emit and exit_reconcile_stats.get("synth_sells"):
            print(json.dumps({"status": "position_exit_reconcile", **exit_reconcile_stats}, ensure_ascii=False), flush=True)

    settlement_started_mono = time.monotonic()
    open_signals, clv_stats = apply_closing_line_snapshots(open_signals, active_markets, now_ts=now_ts)
    closing_line_snapshot_count += clv_stats.get("closing_line_snapshot_count", 0)
    contested_condition_ids = contested_markets(open_signals, now_ts=now_ts)
    open_signals, contested_stats = apply_contested_flags(open_signals, contested_condition_ids, now_ts=now_ts)
    contested_signal_count += contested_stats.get("contested_signal_count", 0)

    exited_signals = [signal for signal in open_signals if signal.get("status") == "exited"]
    open_signals = [signal for signal in open_signals if signal.get("status") != "exited"]
    # 结算轮询降频:已跟进行中赛事的结算结果不敏感(打几小时;提前卖出已即时结算;自动结算也不急)。
    # 每 --resolution-poll-seconds(默认 300s)均匀拉一次所有在跟赛事的结算,而非随 5s drain 频繁查。
    # 游标用每信号 resolution_checked_at 持久化(随 open_signals 落库),全局闸 = max(stamp)。
    resolution_poll_interval = int(getattr(args, "resolution_poll_seconds", 300) or 0)
    resolutions: dict[str, int] = {}
    if open_signals and (
        resolution_poll_interval <= 0
        or now_ts - max((to_int(s.get("resolution_checked_at")) for s in open_signals), default=0) >= resolution_poll_interval
    ):
        for signal in open_signals:
            signal["resolution_checked_at"] = now_ts
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
    # 小单累加器清理:① 已结算/已离场的赛事 cid;② 已不在 watchlist 的赛事(结束/出范围,
    # 不会再有新 fill 触发)。凑够即跟时已就地清键,这里只兜底清理"凑不够就结束/卖光"的残留。
    if pending_small_buys:
        _settled_cids = {str(s.get("condition_id") or "").lower() for s in result_events}
        _watched_cids = {str(cid).lower() for cid in (watched or {})}
        for _key in list(pending_small_buys):
            _cid = _key.split("|", 2)[1] if "|" in _key else ""
            if _cid in _settled_cids or _cid not in _watched_cids:
                pending_small_buys.pop(_key, None)
    if result_events:
        performance = aggregate_follow_performance(performance, result_events)
    else:
        performance = aggregate_follow_performance(performance, [])
    # follow tick 本身不写隔离。M5 自动降级由 runner 按结算笔数事件触发(command_run →
    # rescore_demote_wallets,重评被跟钱包跌出 grade-A 即【直接删除】淘汰,无 quarantine 中间态)。
    # quarantine 入口现在只剩**人工按钮**一种。
    stage_seconds["settlement_and_quarantine"] = round(time.monotonic() - settlement_started_mono, 3)
    balance_ledger_result = {"configured": account_balance_configured, "applied_count": 0, "applied_amount_usdc": 0.0}
    if account_balance_configured:
        balance_ledger_entries = [
            *account_buy_ledger_entries([*open_signals, *result_events], created_at=now_ts),
            *account_result_ledger_entries(result_events, created_at=now_ts),
        ]
        balance_ledger_result = store.apply_account_ledger(balance_ledger_entries)
        account_balance = store.load_account_balance()

    def summarize_seconds(values: list[int] | list[float]) -> dict[str, Any]:
        clean = sorted(float(value) for value in values if value is not None and float(value) >= 0)
        if not clean:
            return {"count": 0}
        return {
            "count": len(clean),
            "p50": round(clean[int((len(clean) - 1) * 0.50)], 3),
            "p90": round(clean[int((len(clean) - 1) * 0.90)], 3),
            "max": round(clean[-1], 3),
        }

    run_log_row = {
        "created_at": now_ts,
        "follow_wallet_count": len(follow_wallets),
        "eligible_follow_wallet_count": len(eligible_wallet_rows),
        "lifecycle_follow_wallet_count": len(lifecycle_wallets),
        "gate_open": gate_open,
        "active_market_source": active_source,
        "watched_market_count": len(watched),
        "trade_request_count": trade_request_count,
        "wallet_trade_fetch_error_count": wallet_fetch_error_count,
        "wallet_trade_fetch_seconds": summarize_seconds(wallet_fetch_seconds),
        "observed_trade_delay_seconds": summarize_seconds(observed_delay_values),
        "index_lag_lower_bound_seconds": summarize_seconds(index_lag_lower_bound_values),
        "cold_start_wallet_count": cold_start_wallet_count,
        # 健康检查:链上检测来源(onchain WS 实时 / data_api 兜底)+ WS 是否健康。
        "detection_source": detection_source,
        "onchain_healthy": bool(collector is not None and getattr(collector, "healthy", False)),
        "onchain_configured": bool(collector is not None),
        "backfill_ran": backfill_ran,
        "backfill_legs_opened": backfill_legs_opened,
        "backfill_stats": backfill_stats,
        "total_new_trade_count": total_new_trade_count,
        "watched_new_trade_count": watched_new_trade_count,
        "new_trade_count": watched_new_trade_count,
        "ignored_trade_count": ignored_trade_count,
        "insufficient_balance_count": insufficient_balance_count,
        "market_type_not_eligible_count": market_type_not_eligible_count,
        "low_entry_price_blocked_count": low_entry_price_blocked_count,
        "high_entry_price_blocked_count": high_entry_price_blocked_count,
        "small_wallet_trade_blocked_count": small_wallet_trade_blocked_count,
        "small_add_blocked_count": small_add_blocked_count,
        "signal_cap_limited_count": signal_cap_limited_count,
        "signal_cap_blocked_count": signal_cap_blocked_count,
        "strategy_source": strategy_loaded_from,
        "strategy_configured": bool((follow_strategy or legacy_follow_strategy or {}).get("configured")),
        "strategy_summary": strategy_summary(follow_strategy or legacy_follow_strategy),
        "strategy_invalid_count": strategy_invalid_count,
        "stake_below_minimum_count": stake_below_minimum_count,
        "condition_order_cap_blocked_count": condition_order_cap_blocked_count,
        "wallet_condition_order_cap_blocked_count": wallet_condition_order_cap_blocked_count,
        "condition_stake_cap_blocked_count": condition_stake_cap_blocked_count,
        "max_signal_stake_usdc": max_signal_stake_usdc,
        "max_signal_stake_balance_percent": max_signal_stake_balance_percent,
        "opposite_blocked_count": opposite_blocked_count,
        "new_signal_count": new_signal_count,
        "exited_signal_count": exited_signal_count,
        "hedge_event_count": hedge_event_count,
        "contested_signal_count": contested_signal_count,
        "closing_line_snapshot_count": closing_line_snapshot_count,
        "account_balance_configured": account_balance_configured,
        "account_balance_usdc": account_balance.get("balance_usdc") if account_balance_configured else None,
        "balance_ledger_applied_count": balance_ledger_result.get("applied_count", 0),
        "balance_ledger_applied_amount_usdc": balance_ledger_result.get("applied_amount_usdc", 0.0),
        "open_signal_count": len(open_signals),
        "settled_signal_count": len(settled),
        # Wallets behind newly-settled follow signals this tick — the runner
        # accumulates these to event-trigger M5 demotion re-scoring.
        "newly_settled_wallets": sorted({normalize_wallet(s.get("wallet")) for s in settled if normalize_wallet(s.get("wallet"))}),
        "desired_next_interval_seconds": next_interval,
        "tick_runtime_seconds": round(time.monotonic() - follow_started_mono, 3),
        "stage_seconds": stage_seconds,
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
    store.save_pending_small_buys(pending_small_buys)
    store.save_follow_snapshot(
        wallet_trade_state=wallet_trade_state,
        open_signals=open_signals,
        result_events=result_events,
        performance=performance,
    )
    store.save_run_tick(run_log_row)
    state = {
        "updated_at": now_ts,
        "db_path": str(follow_dir / "follow.db"),
        "active_market_cache_path": str(active_cache_path),
        "schema_version": 1,
    }
    write_json(state_path, state)

    summary = {
        **run_log_row,
        "settled_result_count_total": len(store.load_results()),
        "output_dir": str(follow_dir),
    }
    if emit:
        # 单行紧凑 JSON:每拍一行而非 ~92 行 pretty-print,日志增速减半、grep 更快。
        # 完整结构化记录已经 save_run_tick 入库(follow.db),这里只为可读观测。
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def command_run(args: argparse.Namespace) -> int:
    client = build_client(args)
    now_init = int(datetime.now(timezone.utc).timestamp())
    last_build_at = now_init if args.skip_initial_build else 0
    tick_count = 0
    first_error_at: float | None = None
    # 增量补单:已补过持仓的钱包集合,跨 tick 持有。startup 全量补,之后 live-seed 中途
    # 晋升进 leaderboard 的新钱包随到随补(每钱包一次)。空集兜底使首 tick 因暂停/异常漏补时下 tick 重试。
    backfilled_wallets: set[str] = set()
    # 队标抓取节流:对阵盘多无队标,每 tick 重抓是空转 → 约 30min 才跑一次(WS 健康时 tick ~5s)。
    last_logo_refresh_at = 0.0
    LOGO_REFRESH_INTERVAL_SECONDS = 1800
    stop_event = threading.Event()   # 收到停止信号立即唤醒 sleep(避免 PEP 475 睡满长间隔才停)
    stop_reason = {"value": ""}
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

    # 一次性迁移:历史自动降级隔离的钱包(当初表现差被淘汰)按新策略直接删除,**不**解禁放回跟单集。
    try:
        purge = purge_legacy_demote_quarantine(category_args("esports"), follow_dir=follow_dir, now_ts=now_init)
        if purge["deleted"]:
            print(json.dumps({"status": "legacy_demote_purge", **purge}, ensure_ascii=False))
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"status": "legacy_demote_purge_error", "error": str(exc)}, ensure_ascii=False))

    def request_stop(signum, _frame) -> None:
        stop_reason["value"] = f"signal_{signum}"
        stop_event.set()

    previous_sigterm = None
    try:
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, request_stop)
    except (AttributeError, ValueError):
        previous_sigterm = None

    # On-chain follow detection: start a getLogs-polling collector if an RPC is
    # configured. command_follow drains it (when healthy) instead of polling
    # data-api; if getLogs polling fails repeatedly it falls back to data-api.
    # No RPC -> data-api. Only https_url is needed (WS is gone).
    collector: OnchainFollowCollector | None = None
    https_url, _wss_url = load_rpc_endpoints()
    onchain_poll_interval = float(getattr(args, "onchain_poll_interval", 30.0) or 30.0)
    if https_url:
        collector = OnchainFollowCollector(
            https_url=https_url,
            poll_interval=onchain_poll_interval,
            on_event=lambda kind, data: print(
                json.dumps({"status": "onchain", "event": kind, **{k: v for k, v in data.items() if k in ("error", "phase", "fills", "from", "to", "cold_start", "consecutive")}}, ensure_ascii=False),
                flush=True,
            ),
        )
        collector.start()
        print(json.dumps({"status": "onchain_collector_started", "mode": "getlogs_poll", "poll_interval": onchain_poll_interval, "rpc": https_url.split("/v2/")[0] + "/v2/***"}, ensure_ascii=False), flush=True)
    else:
        print(json.dumps({"status": "onchain_disabled", "reason": "no secret/rpc; using data-api polling"}, ensure_ascii=False), flush=True)

    # M5 动态降级:跨 tick 累计被跟钱包的新结算笔数,满阈值就对那批钱包后台重评/隔离
    # (事件驱动,替代旧的固定 2h observe-v2 降级扫描)。
    rescore_threshold = int(getattr(args, "rescore_settled_threshold", 10) or 0)
    rescore_thread: threading.Thread | None = None
    m5_store = FollowStore(follow_dir / "follow.db")   # M5 计数从 DB 派生(持久化、含 exited)

    def sleep_or_stop(seconds: int) -> None:
        # Event.wait 收到停止信号立即返回(不像 time.sleep 会睡满整段),所以 SIGTERM/停跟单秒停。
        if int(seconds) > 0:
            stop_event.wait(int(seconds))

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
                    # v2 是唯一管线:run 的周期性榜单刷新走 collect-v2(esports)。
                    _command_collect_wallets(category_args(category), client=client, variant="v2")
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
        while not stop_event.is_set():
            iteration_started_mono = time.monotonic()
            try:
                maybe_build(force=False)
                # 复用 tick 起点的 monotonic(不额外调用),节流队标抓取约 30min 一次。
                _do_logos = (iteration_started_mono - last_logo_refresh_at) >= LOGO_REFRESH_INTERVAL_SECONDS
                summary = command_follow(
                    args, client=client, emit=True, collector=collector,
                    backfill_positions=True,
                    backfilled_wallets=backfilled_wallets,
                    refresh_logos=_do_logos,
                )
                if _do_logos:
                    last_logo_refresh_at = iteration_started_mono
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

            # M5 计数从 DB 派生:未处理的终态结果(settled + exited,均有真实盈亏)。持久化
            # 跨重启累加、含提前卖出。达阈值即把这批标记已处理(防重启重复)+ off-thread
            # 重评其钱包(跌出 grade-A 即直接删除淘汰)。上一轮重评还在跑则跳过(这批留到下轮)。
            if rescore_threshold > 0 and (rescore_thread is None or not rescore_thread.is_alive()):
                pending = m5_store.load_unprocessed_m5_results()
                if len(pending) >= rescore_threshold:
                    batch = {normalize_wallet(r["wallet"]) for r in pending if normalize_wallet(r["wallet"])}
                    m5_store.mark_m5_results_processed([r["signal_id"] for r in pending])

                    def _run_rescore(wallets: set[str] = batch) -> None:
                        try:
                            # esports category dir for leaderboard_v2.db/collector outputs;
                            # shared follow_dir for the quarantine write.
                            result = rescore_demote_wallets(
                                client, category_args("esports"), wallets=wallets, follow_dir=follow_dir)
                            print(json.dumps({"status": "rescore_demote", **result}, ensure_ascii=False), flush=True)
                        except Exception as exc:  # noqa: BLE001
                            print(json.dumps({"status": "rescore_demote_error", "error": str(exc)}, ensure_ascii=False), flush=True)

                    if batch:
                        rescore_thread = threading.Thread(target=_run_rescore, name="m5-rescore-demote", daemon=True)
                        rescore_thread.start()

            if args.max_run_ticks and tick_count >= args.max_run_ticks:
                break
            # On-chain healthy -> drain on the short cadence (the collector polls
            # getLogs every ~30s; this bounds how soon we act on a buffered fill).
            # Otherwise (no RPC / getLogs failing, i.e. data-api fallback) use the
            # adaptive tick interval.
            if collector is not None and collector.healthy:
                target_interval = max(1, int(getattr(args, "ws_drain_seconds", 5)))
            else:
                target_interval = int(summary.get("desired_next_interval_seconds") or args.max_tick_seconds)
            iteration_seconds = time.monotonic() - iteration_started_mono
            sleep_seconds = max(0, int(round(target_interval - iteration_seconds)))
            sleep_or_stop(sleep_seconds)
    except KeyboardInterrupt:
        print(json.dumps({"status": "stopped", "reason": "keyboard_interrupt", "ticks": tick_count}, ensure_ascii=False))
    finally:
        if collector is not None:
            collector.stop()
        if stop_event.is_set():
            print(json.dumps({"status": "stopped", "reason": stop_reason["value"], "ticks": tick_count}, ensure_ascii=False))
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
        static_dir=(
            Path(args.static_dir).expanduser().resolve()
            if getattr(args, "static_dir", None)
            else Path(__file__).resolve().with_name("dashboardV2")
        ),
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
            subparser.add_argument("--category", choices=["esports"], default="esports")
        subparser.add_argument("--gamma-pages", type=int, default=10)
        subparser.add_argument("--refresh-classification", action="store_true")
        subparser.add_argument("--classification-cache-ttl-hours", type=int, default=24)
        subparser.add_argument("--classification-lookback-days", type=int, default=None)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)
        subparser.add_argument("--min-market-volume", type=float, default=25_000)
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
        subparser.add_argument("--max-leaderboard-wallets", type=int, default=60)
        subparser.add_argument("--profile-refresh-ttl-days", type=int, default=7)
        subparser.add_argument("--profile-store-max-age-days", type=int, default=180)
        subparser.set_defaults(func=command_collect)

    def add_follow_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--follow-dir")
        subparser.add_argument("--strategy-source", choices=("auto", "db", "legacy"), default="auto", help="follow strategy source: db requires saved follow.db strategy; legacy uses CLI stake flags")
        subparser.add_argument("--stake-usdc", type=float, default=1.0, help="legacy minimum paper stake per followed BUY leg")
        subparser.add_argument("--stake-ratio-percent", type=float, default=10.0, help="target-wallet cash replication ratio per BUY leg")
        subparser.add_argument("--max-stake-usdc", type=float, default=0.0, help="optional maximum paper stake per followed BUY leg; 0 disables the cap")
        subparser.add_argument("--max-signal-stake-usdc", type=float, default=0.0, help="optional maximum funded stake per wallet/market/outcome signal; 0 disables the cap")
        subparser.add_argument("--max-signal-stake-balance-percent", type=float, default=0.0, help="optional maximum funded stake per signal as a percent of current paper balance; 0 disables the cap")
        subparser.add_argument("--bankroll-usdc", type=float, default=0.0, help="optional paper bankroll cap for total open exposure; 0 disables the cap")
        subparser.add_argument("--follow-recency-days", type=int, default=30)
        subparser.add_argument("--observe-window-hours", type=float, default=24)
        subparser.add_argument("--event-cache-ttl-minutes", type=int, default=60)
        subparser.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
        subparser.add_argument("--resolution-poll-seconds", type=int, default=300, help="已跟进行中赛事结算结果的轮询周期(秒);300s 均匀拉一次")
        subparser.add_argument("--resolution-gamma-pages", type=int, default=2)
        subparser.add_argument("--max-slippage-over-entry", type=float, default=0.10)
        subparser.add_argument("--max-entry-price", type=float, default=0.85)
        subparser.add_argument("--min-wallet-entry-price", type=float, default=0.4)
        subparser.add_argument("--post-start-trade-grace-seconds", type=int, default=900)
        subparser.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=False)
        subparser.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
        subparser.add_argument("--run-log-retention-days", type=int, default=7)
        subparser.add_argument("--gamma-pages", type=int, default=3)
        subparser.add_argument("--user-trades-limit", type=int, default=50)
        subparser.add_argument("--user-trades-max-pages", type=int, default=1)
        subparser.add_argument("--max-follow-legs", type=int, default=10)
        subparser.add_argument("--min-tick-seconds", type=int, default=180)
        subparser.add_argument("--max-tick-seconds", type=int, default=900)
        # On-chain detection: getLogs poll cadence (seconds). Bounds blind-window if a
        # log subscription/RPC hiccups; ~30s keeps Alchemy free-tier CU comfortable.
        subparser.add_argument("--onchain-poll-interval", type=float, default=30.0)
        # Fixed polling cadence (seconds). >0 overrides the adaptive min/max curve so every
        # wallet is checked on one steady interval; 0 restores the start-time-aware backoff.
        subparser.add_argument("--tick-seconds", type=int, default=60)
        # When the on-chain WS collector is healthy, drain it on this short cadence
        # instead of the slow data-api tick (the WS already detected fills sub-second;
        # this bounds how soon we act on them). Ignored when WS is unavailable.
        subparser.add_argument("--ws-drain-seconds", type=int, default=5)
        # 独立持仓对账兜底:每 N 秒只核对最久未核对的 M 笔 open 跟单的目标持仓(分批轮转,
        # 不放进每 5s 主循环)。目标已清仓 → 镜像平仓。0 关闭。
        subparser.add_argument("--reconcile-interval-seconds", type=int, default=60)
        subparser.add_argument("--reconcile-batch-size", type=int, default=20)
        subparser.add_argument("--quarantine-sell-frac", type=float, default=0.2)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)

    def add_collector_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--output-dir", default=None)
        subparser.add_argument("--lookback-days", type=int, default=30)
        subparser.add_argument("--profile-lookback-days", type=int, default=COLLECTOR_PROFILE_LOOKBACK_DAYS)
        subparser.add_argument("--bucket-market-limit", type=int, default=100)
        subparser.add_argument("--positions-per-market", type=int, default=20)
        subparser.add_argument("--seed-bucket-min-hit-rate", type=float, default=SEED_BUCKET_MIN_HIT_RATE)
        subparser.add_argument("--seed-single-bucket-min-wins", type=int, default=SEED_SINGLE_BUCKET_MIN_WINS)
        subparser.add_argument("--seed-multi-bucket-min-wins", type=int, default=SEED_MULTI_BUCKET_MIN_WINS)
        subparser.add_argument("--seed-main-match-min-avg-cash", type=float, default=SEED_MAIN_MATCH_MIN_AVG_CASH)
        subparser.add_argument("--seed-game-winner-min-avg-cash", type=float, default=SEED_GAME_WINNER_MIN_AVG_CASH)
        subparser.add_argument("--seed-map-winner-min-avg-cash", type=float, default=SEED_MAP_WINNER_MIN_AVG_CASH)
        subparser.add_argument("--seed-min-weighted-roi", type=float, default=SEED_MIN_WEIGHTED_ROI)
        subparser.add_argument("--seed-max-median-avg-price", type=float, default=SEED_MAX_MEDIAN_AVG_PRICE)
        subparser.add_argument("--max-profile-wallets", type=int, default=None)
        subparser.add_argument("--max-core-wallets", type=int, default=20)
        subparser.add_argument("--max-momentum-wallets", type=int, default=10)
        subparser.add_argument("--max-watchlist-wallets", type=int, default=50)
        subparser.add_argument("--max-esports-markets-per-wallet", type=int, default=100)
        subparser.add_argument("--collector-profile-cache-ttl-hours", type=float, default=24)
        subparser.add_argument("--user-trades-cache-ttl-days", type=int, default=1)

    collect = subparsers.add_parser("collect", help="one-shot wallet collection and leaderboard build")
    add_build_arguments(collect, include_category=True)
    add_collector_arguments(collect)
    collect.set_defaults(func=command_collect)

    def add_collector_v2_arguments(subparser: argparse.ArgumentParser) -> None:
        # V2 钱包级硬排除门(逐桶质量统一走 classify_wallet_bucket,不再有逐桶 ROI/Wilson 参数)。
        subparser.add_argument("--v2-max-two-sided-rate", type=float, default=V2_MAX_TWO_SIDED_RATE)
        subparser.add_argument("--v2-max-bot-score", type=int, default=V2_MAX_BOT_SCORE)
        # 钱包级硬门:最后一笔交易超过这么多小时 → 不入榜(默认 72h;<=0 关闭)。
        subparser.add_argument("--max-leaderboard-idle-hours", type=int, default=V2_MAX_LEADERBOARD_IDLE_HOURS)
        subparser.add_argument("--v2-per-game-quota", type=int, default=V2_PER_GAME_QUOTA)
        # 技术型(低买高卖)默认不纳入;加此 flag 一键开回。
        subparser.add_argument("--v2-include-technical", dest="v2_include_technical",
                               action="store_true", default=V2_INCLUDE_TECHNICAL)
        # 定时自动重采(长驻进程):>0 时每 N 小时跑一次刷新 leaderboard_v2.db(0=一次性)。
        # 循环期间会暂停 follow 开新单;--follow-dir 须指向 follow 循环的同一个 follow 目录。
        subparser.add_argument("--loop-hours", type=float, default=0)
        subparser.add_argument("--loop-error-retry-seconds", type=int, default=300)
        subparser.add_argument("--loop-max-iterations", type=int, default=0)
        subparser.add_argument("--follow-dir")
        # v2 种子预筛(廉价召回;质量在导出门把控)
        subparser.add_argument("--v2-min-seed-markets", type=int, default=1)
        subparser.add_argument("--v2-min-seed-avg-cash", type=float, default=SEED_MIN_AVG_CASH)
        # M4 observe-v2:结算检测窗口(应 ≥ tick 间隔 + buffer)
        subparser.add_argument("--observe-lookback-hours", type=float, default=4.0)
        subparser.add_argument("--observe-gamma-pages", type=int, default=2)

    collect_v2 = subparsers.add_parser(
        "collect-v2",
        help="collect-v2: dual-side discovery + actual-PnL scoring + per-game quota (isolated leaderboard_v2.db)",
    )
    add_build_arguments(collect_v2, include_category=True)
    add_collector_arguments(collect_v2)
    add_collector_v2_arguments(collect_v2)
    # v2 默认更宽的发现/打分窗口(仍可用 --lookback-days / --profile-lookback-days 覆盖);v1 collect 不受影响。
    collect_v2.set_defaults(
        func=command_collect_v2,
        max_leaderboard_wallets=V2_MAX_LEADERBOARD_WALLETS,
        lookback_days=V2_DEFAULT_LOOKBACK_DAYS,
        profile_lookback_days=V2_DEFAULT_PROFILE_LOOKBACK_DAYS,
    )

    observe_v2 = subparsers.add_parser(
        "observe-v2",
        help="M4: event-driven incremental discovery from newly-settled matches (loop: --loop-hours 2)",
    )
    add_build_arguments(observe_v2, include_category=True)
    add_collector_arguments(observe_v2)
    add_collector_v2_arguments(observe_v2)
    observe_v2.add_argument("--defer-first-tick", action="store_true",
                            help="作为 follow sidecar 时先睡满一轮再跑(避免刚采集完立即重算覆盖)")
    observe_v2.set_defaults(
        func=command_observe_v2,
        max_leaderboard_wallets=V2_MAX_LEADERBOARD_WALLETS,
        lookback_days=V2_DEFAULT_LOOKBACK_DAYS,
        profile_lookback_days=V2_DEFAULT_PROFILE_LOOKBACK_DAYS,
    )

    observe_live = subparsers.add_parser(
        "observe-live",
        help="3.1: 活跃(未结算)watchlist 盘提前发现优质钱包并晋升(loop: --loop-minutes 10)",
    )
    add_build_arguments(observe_live, include_category=True)
    add_collector_arguments(observe_live)
    add_collector_v2_arguments(observe_live)
    # 分钟级快循环(与 observe-v2 的 2h 闭市深采解耦;0=一次性)。market_positions 持仓时效
    # 要新,故按分钟跑。--follow-dir 指向 follow 循环的同一 follow 目录(读其 active 市场缓存)。
    observe_live.add_argument("--loop-minutes", type=float, default=0)
    observe_live.add_argument("--defer-first-tick", action="store_true",
                              help="作为 follow sidecar 时先睡满一轮再跑")
    # --min-market-volume 已由 add_build_arguments 提供(活跃度门);observe-live 默认降到
    # LIVE_SEED_MIN_VOLUME(发现端比建榜的发现窗更宽松,要早抓活跃盘参与者)。
    observe_live.set_defaults(
        func=command_observe_live,
        min_market_volume=LIVE_SEED_MIN_VOLUME,
        max_leaderboard_wallets=V2_MAX_LEADERBOARD_WALLETS,
        lookback_days=V2_DEFAULT_LOOKBACK_DAYS,
        profile_lookback_days=V2_DEFAULT_PROFILE_LOOKBACK_DAYS,
    )

    snapshot = subparsers.add_parser("analyze-collector-snapshot", help="summarize a local collector data snapshot")
    snapshot.add_argument("--snapshot-dir", default="data_vps/esports")
    snapshot.add_argument("--output-file")
    snapshot.set_defaults(func=command_analyze_collector_snapshot)

    calibrate = subparsers.add_parser(
        "calibrate-scopes",
        help="[read-only] measure per-game event density and print derived adaptive params",
    )
    calibrate.add_argument("--calibration-window-days", type=int, default=CALIBRATION_WINDOW_DAYS)
    calibrate.add_argument("--calibration-min-volume", type=float, default=10_000.0)
    calibrate.set_defaults(func=command_calibrate_scopes)

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
    run.add_argument("--strategy-source", choices=("auto", "db", "legacy"), default="auto", help="follow strategy source: db requires saved follow.db strategy; legacy uses CLI stake flags")
    run.add_argument("--stake-usdc", type=float, default=1.0, help="legacy minimum paper stake per followed BUY leg")
    run.add_argument("--stake-ratio-percent", type=float, default=10.0, help="target-wallet cash replication ratio per BUY leg")
    run.add_argument("--max-stake-usdc", type=float, default=0.0, help="optional maximum paper stake per followed BUY leg; 0 disables the cap")
    run.add_argument("--max-signal-stake-usdc", type=float, default=0.0, help="optional maximum funded stake per wallet/market/outcome signal; 0 disables the cap")
    run.add_argument("--max-signal-stake-balance-percent", type=float, default=0.0, help="optional maximum funded stake per signal as a percent of current paper balance; 0 disables the cap")
    run.add_argument("--bankroll-usdc", type=float, default=0.0, help="optional paper bankroll cap for total open exposure; 0 disables the cap")
    run.add_argument("--follow-recency-days", type=int, default=30)
    run.add_argument("--observe-window-hours", type=float, default=24)
    run.add_argument("--event-cache-ttl-minutes", type=int, default=60)
    run.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
    run.add_argument("--resolution-poll-seconds", type=int, default=300, help="已跟进行中赛事结算结果的轮询周期(秒);300s 均匀拉一次,降低 Gamma 调用")
    run.add_argument("--reconcile-interval-seconds", type=int, default=60, help="持仓对账兜底周期(秒);每周期核对最久未核对的若干笔")
    run.add_argument("--reconcile-batch-size", type=int, default=20, help="每个对账周期核对的 open 跟单数上限(分批轮转)")
    run.add_argument("--resolution-gamma-pages", type=int, default=2)
    run.add_argument("--max-slippage-over-entry", type=float, default=0.10)
    run.add_argument("--max-entry-price", type=float, default=0.85)
    run.add_argument("--min-wallet-entry-price", type=float, default=0.4)
    run.add_argument("--post-start-trade-grace-seconds", type=int, default=900)
    run.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=False)
    run.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
    run.add_argument("--run-log-retention-days", type=int, default=7)
    run.add_argument("--user-trades-limit", type=int, default=50)
    run.add_argument("--user-trades-max-pages", type=int, default=1)
    run.add_argument("--max-follow-legs", type=int, default=10)
    run.add_argument("--min-tick-seconds", type=int, default=180)
    run.add_argument("--max-tick-seconds", type=int, default=900)
    # Fixed polling cadence (seconds). >0 overrides the adaptive min/max curve; 0 = adaptive.
    run.add_argument("--tick-seconds", type=int, default=60)
    run.add_argument("--ws-drain-seconds", type=int, default=5)
    # M5 动态降级:每累计这么多笔新结算跟单,就对那批被跟钱包重评一次,跌出 A 榜即隔离
    # (事件驱动,替代旧的固定 2h observe-v2 降级扫描)。<=0 关闭 runner 侧降级。
    run.add_argument("--rescore-settled-threshold", type=int, default=5)
    run.add_argument("--quarantine-sell-frac", type=float, default=0.2)
    run.add_argument("--error-retry-seconds", type=int, default=180)
    run.add_argument("--max-consecutive-error-seconds", type=int, default=600)
    run.add_argument("--pool-refresh-hours", type=float, default=12)
    # 与 pool-refresh 对齐:每次 12h 刷新就让全部已有钱包在滚动后的新窗口上重评一遍
    # (复用增量成交缓存、只重算分,不重下历史)。raw_user_trades 文件 1 天保留 > 12h 重评
    # 周期 → 增量缓存在两次重评间存活,不会被清后触发整窗重拉。
    run.add_argument("--collector-profile-cache-ttl-hours", type=float, default=12)
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
    serve.add_argument("--static-dir", help="override the directory of static dashboard assets (e.g. the dashboardV2 build)")
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
