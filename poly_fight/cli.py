from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import math
import json
import os
import re
import signal
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
    MIN_A_POSITIVE_MARKET_RATE,
    SCORING_VERSION,
    TRADE_BEHAVIOR_EXCLUDE_RATE,
    TRADE_BEHAVIOR_MIN_MARKETS,
    GAME_WINNER,
    MAP_WINNER,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    classify_market_type,
    classify_wallet,
    event_to_market_record,
    event_to_market_records,
    is_settled_binary_prices,
    normalize_market_text,
    normalize_wallet,
    parse_dt,
    parse_jsonish,
    profile_candidate_wallet,
    summarize_closed_positions,
    to_float,
)
from .follow import (
    aggregate_follow_performance,
    apply_closing_line_snapshots,
    apply_contested_flags,
    bootstrap_position_trades,
    contested_markets,
    current_position_keys,
    desired_tick_interval,
    detect_new_positions,
    eligible_follow_wallets,
    esports_match_imminent,
    evaluate_slippage,
    market_current_price,
    position_key,
    process_follow_trades,
    qualify_follow,
    select_new_trades,
    settle_open_signals,
    should_retry_unqualified_position,
    summarize_wallet_fills,
    trade_condition_id,
    upsert_follow_signal,
    winner_outcome_index,
)
from .storage import FollowStore

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix fallback
    fcntl = None


class BuildLockUnavailable(RuntimeError):
    pass


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
        write_json(
            cache_path,
            {
                "meta": expected_meta,
                "trades": trades,
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


def load_active_market_cache(
    client: PolymarketClient,
    state: dict[str, Any],
    *,
    cache_path: Path | None = None,
    now_ts: int,
    gamma_pages: int,
    ttl_seconds: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], str]:
    legacy_cached = state.pop("active_market_cache", None) or {}
    if cache_path and legacy_cached:
        write_json(cache_path, legacy_cached)
        if now_ts - int(legacy_cached.get("updated_at") or 0) < ttl_seconds:
            markets = {row["condition_id"]: row for row in legacy_cached.get("markets") or []}
            return markets, state, "legacy_state_cache"
    cached = read_json(cache_path, {}) if cache_path else {}
    if cached and now_ts - int(cached.get("updated_at") or 0) < ttl_seconds:
        markets = {row["condition_id"]: row for row in cached.get("markets") or []}
        return markets, state, "cache"
    events = client.list_events_paginated(closed=False, active=True, max_pages=gamma_pages, order="startTime")
    markets = market_records_from_events(events)
    cache_value = {
        "updated_at": now_ts,
        "markets": list(markets.values()),
    }
    if cache_path:
        write_json(cache_path, cache_value)
    else:
        state["active_market_cache"] = cache_value
    return markets, state, "api"


TEAM_LOGO_URL_RE = re.compile(
    r"url=(https%3A%2F%2Fpolymarket-upload\.s3\.us-east-2\.amazonaws\.com%2Fteam_logos%2F[^&\"<>]+?\.png)"
)
MATCH_TITLE_TEAMS_RE = re.compile(r"^([^:]+):\s+(.+?)\s+vs\s+(.+?)(\s+\([^)]+\))?\s+-\s+(.+)$", re.IGNORECASE)


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
        game, team_a, team_b = _match_title_teams(str(market.get("title") or market.get("question") or ""))
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
        game, team_a, team_b = _match_title_teams(str(market.get("title") or market.get("question") or ""))
        title_teams = [team_a, team_b]
        rows: list[tuple[str, str, str, bytes]] = []
        for encoded_url in sorted(set(TEAM_LOGO_URL_RE.findall(html))):
            logo_url = unquote(encoded_url)
            team_key = _team_logo_key_from_url(logo_url, game=game, title_teams=title_teams)
            if not team_key:
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


def _match_title_teams(title: str) -> tuple[str, str, str]:
    match = MATCH_TITLE_TEAMS_RE.match(title)
    if not match:
        return "", "", ""
    return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()


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
        "valorant",
    ]:
        prefix = prefix.strip()
        if prefix and base.lower().startswith(prefix.lower() + "_"):
            base = base[len(prefix) + 1 :]
            break
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


def _normalize_team_logo_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def command_refresh_team_logos(args: argparse.Namespace) -> int:
    summary = refresh_team_logo_cache_from_active_markets(
        Path(args.data_dir),
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
) -> dict[str, dict[str, Any]]:
    window_end = now_ts + int(observe_window_hours * 3600)
    watched = {}
    for condition_id, market in active_markets.items():
        start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
        if not start_dt:
            continue
        start_ts = int(start_dt.timestamp())
        if now_ts < start_ts <= window_end:
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


def fetch_wallet_market_trades(
    client: PolymarketClient,
    wallet: str,
    condition_id: str,
    *,
    page_limit: int = 500,
) -> list[dict]:
    try:
        return client.trades_for_user_market(wallet, condition_id, limit=page_limit)
    except RuntimeError:
        trades = client.trades_for_market(condition_id, limit=page_limit, min_trade_cash=0)
        wallet = normalize_wallet(wallet)
        return [
            trade
            for trade in trades
            if normalize_wallet(trade.get("proxyWallet") or trade.get("wallet") or trade.get("user")) == wallet
        ]


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


def refresh_open_signal_fills(
    client: PolymarketClient,
    open_signals: list[dict[str, Any]],
    active_markets: dict[str, dict[str, Any]],
    *,
    now_ts: int,
    max_slippage: float,
) -> tuple[list[dict[str, Any]], int]:
    refreshed = 0
    for signal in open_signals:
        condition_id = str(signal.get("condition_id") or "").lower()
        market = active_markets.get(condition_id)
        if not market:
            continue
        start_dt = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
        if start_dt and now_ts >= int(start_dt.timestamp()):
            continue
        outcome_index = int(signal.get("outcome_index") or 0)
        current_price = market_current_price(market, outcome_index)
        for trigger in signal.get("triggered_by") or []:
            wallet = normalize_wallet(trigger.get("wallet"))
            if not wallet:
                continue
            trades = fetch_wallet_market_trades(client, wallet, condition_id)
            fills_summary = summarize_wallet_fills(trades, wallet=wallet, outcome_index=outcome_index)
            wallet_avg_price = fills_summary.get("avg_price") or trigger.get("wallet_avg_price") or signal.get("wallet_avg_price")
            qualification = {
                "condition_id": condition_id,
                "outcome_index": outcome_index,
                "outcome": signal.get("outcome"),
                "wallet_avg_price": wallet_avg_price,
                "position_size": trigger.get("position_size") or 0,
            }
            open_signals, _created = upsert_follow_signal(
                open_signals,
                wallet=wallet,
                market=market,
                qualification=qualification,
                fills_summary=fills_summary,
                current_price=current_price,
                max_slippage=max_slippage,
                stake_usdc=to_float(signal.get("stake_usdc")),
                now_ts=now_ts,
            )
            refreshed += 1
    return open_signals, refreshed


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


def filter_profile_candidates(
    candidates: list[dict[str, Any]],
    *,
    min_participated_markets: int = 1,
    min_avg_market_cash: float = 1_500,
    require_clean_discovery: bool = True,
    max_tail_entry_rate: float = 0.34,
) -> list[dict[str, Any]]:
    rows = []
    for candidate in candidates:
        participated = int(candidate.get("participated_market_count") or 0)
        if participated < min_participated_markets:
            continue
        avg_market_cash = to_float(candidate.get("avg_market_cash") or candidate.get("avg_market_usd"))
        if avg_market_cash < min_avg_market_cash:
            continue
        if require_clean_discovery:
            if int(candidate.get("two_sided_market_count") or 0) > 0:
                continue
            # Gate on tail-entry rate, not any-occurrence, so a single tail entry among
            # many clean markets doesn't block an otherwise elite wallet pre-profiling.
            tail_entry_count = int(candidate.get("tail_entry_market_count") or 0)
            if participated > 0 and tail_entry_count / participated > max_tail_entry_rate:
                continue
        rows.append(candidate)
    return rows


def build_leaderboard_from_profiles(
    profiles_by_wallet: dict[str, dict[str, Any]],
    *,
    now_ts: int | None = None,
    max_inactive_days: int = 90,
    min_participated_markets: int = 1,
    min_avg_market_cash: float = 1_500,
    require_tail_entry_field: bool = False,
    require_current_scoring_version: bool = False,
    max_leaderboard_wallets: int = 30,
    max_tail_entry_rate: float = 0.34,
) -> list[dict[str, Any]]:
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    max_inactive_seconds = max_inactive_days * 86400
    leaderboard = []
    for profile in profiles_by_wallet.values():
        if require_current_scoring_version and int(profile.get("scoring_version") or 0) != SCORING_VERSION:
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
        sold_rate = to_float(profile.get("sold_before_resolution_market_rate"))
        if (
            int(profile.get("sold_before_resolution_market_count") or 0) > 0
            and behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS
            and sold_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
        ):
            continue
        two_sided_trade_rate = to_float(profile.get("two_sided_trade_market_rate"))
        if (
            int(profile.get("two_sided_trade_market_count") or 0) > 0
            and behavior_market_count >= TRADE_BEHAVIOR_MIN_MARKETS
            and two_sided_trade_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
        ):
            continue
        last_trade = int(profile.get("last_esports_trade_at") or 0)
        if not last_trade or now_ts - last_trade > max_inactive_seconds:
            continue
        candidate = profile.get("candidate") or {}
        if int(candidate.get("participated_market_count") or 0) < min_participated_markets:
            continue
        avg_market_cash = to_float(candidate.get("avg_market_cash") or candidate.get("avg_market_usd"))
        if avg_market_cash < min_avg_market_cash:
            continue
        if int(candidate.get("two_sided_market_count") or 0) > 0:
            continue
        if require_tail_entry_field and "tail_entry_market_count" not in candidate:
            continue
        # A single tail entry among many markets shouldn't disqualify an otherwise elite
        # wallet; gate on the rate of tail entries instead of any-occurrence.
        tail_entry_count = int(candidate.get("tail_entry_market_count") or 0)
        participated_count = int(candidate.get("participated_market_count") or 0)
        if participated_count > 0 and tail_entry_count / participated_count > max_tail_entry_rate:
            continue
        leaderboard.append(profile)
    ranked = sorted(leaderboard, key=leaderboard_rank_key)
    if max_leaderboard_wallets > 0:
        return ranked[:max_leaderboard_wallets]
    return ranked


def leaderboard_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    loss_count = int(row.get("esports_loss_count") or 0)
    return (
        0 if row.get("grade") == "A" or row.get("eligible_market_types") else 1,
        loss_count > 0,
        loss_count,
        -to_float(row.get("positive_market_rate")),
        -to_float(row.get("wilson_win_rate_lower_bound")),
        -to_float(row.get("entry_edge")),
        -to_float(row.get("median_market_roi") or row.get("esports_roi")),
        normalize_wallet(row.get("wallet")),
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
    for candidate in profile_candidates:
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


def command_build_leaderboard(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    data_dir = Path(args.data_dir)
    with acquire_build_lock(data_dir):
        return _command_build_leaderboard_unlocked(args, client=client)


def _command_build_leaderboard_unlocked(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    client = client or build_client(args)
    data_dir = Path(args.data_dir)
    now_ts = int(datetime.now(timezone.utc).timestamp())

    lookback_steps = (args.discovery_lookback_days,) if args.discovery_lookback_days else (7, 14, 30)
    classification_path = data_dir / "esports_classification_set.json"
    classification_meta_path = data_dir / "esports_classification_set.meta.json"
    classification_lookback_days = args.classification_lookback_days
    classification_meta = {
        "gamma_pages": args.gamma_pages,
        "classification_lookback_days": classification_lookback_days,
        "market_scope": "cs-dota-lol-main-game-map-winner-v1",
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
        )
        classification_set = build_classification_set(
            closed_events,
            now=now_dt,
            lookback_days=classification_lookback_days if classification_lookback_days > 0 else None,
        )
        write_json(classification_path, classification_set)
        write_json(classification_meta_path, classification_meta)

    market_batch_size = args.market_batch_size or 50
    market_count = args.max_markets_per_run or market_batch_size * args.market_batch_count
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
        target_markets=args.target_markets,
        submarket_target_markets=args.submarket_target_markets,
        game_winner_target_markets=args.game_winner_target_markets,
        map_winner_target_markets=args.map_winner_target_markets,
        max_markets_per_run=market_count,
        submarket_max_markets_per_run=args.submarket_max_markets_per_run,
        game_winner_max_markets_per_run=args.game_winner_max_markets_per_run,
        map_winner_max_markets_per_run=args.map_winner_max_markets_per_run,
        market_offset=market_offset,
    )
    write_json(data_dir / "discovery_slate.json", discovery_slate)

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
            market_end_times=market_end_times,
            market_start_times=market_start_times,
            min_trade_cash=args.min_trade_cash,
            participation_threshold=args.participation_threshold,
            top_participation_count=args.top_participation_count,
            total_cash_threshold=args.total_cash_threshold,
            single_market_cash_threshold=args.single_market_cash_threshold,
            max_candidate_wallets=args.max_candidate_wallets,
        )
    profile_candidates = filter_profile_candidates(
        candidates,
        min_participated_markets=args.min_profile_participated_markets,
        min_avg_market_cash=args.min_profile_avg_market_cash,
        require_clean_discovery=not args.allow_dirty_profile_candidates,
    )
    write_json(data_dir / "candidate_wallets.json", candidates)
    write_json(data_dir / "profile_candidate_wallets.json", profile_candidates)

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
    profile_fetch_plan = build_profile_fetch_plan(
        profile_candidates,
        existing_profiles,
        now_ts=now_ts,
        ttl_seconds=args.profile_refresh_ttl_days * 86400,
        max_profiles=args.max_profiles_per_run,
    )

    def fetch_raw_user_trades_for_candidate(candidate: dict[str, Any]) -> tuple[str, list[dict]]:
        wallet = normalize_wallet(candidate.get("wallet"))
        trades = fetch_recent_user_trades_for_wallet(
            client,
            wallet,
            page_limit=args.user_history_trades_limit,
            max_pages=args.user_history_trades_max_pages,
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
    if backfilled_market_records:
        market_records_by_id = {**market_records_by_id, **backfilled_market_records}
        condition_type_by_id = {
            **condition_type_by_id,
            **{
                condition_id: str(record.get("market_type") or "main_match")
                for condition_id, record in backfilled_market_records.items()
            },
        }
        condition_ids = set(market_records_by_id)

    def load_user_trades(wallet: str) -> list[dict]:
        # Per-user history is the accurate source: a per-market pool over the discovery
        # slate only covers markets we fetched and is taker-side only, which materially
        # undercounts a wallet's resolved markets (verified: 0xe16 80→39 markets, A→B).
        return _filter_esports_user_trades(
            raw_user_trades_by_wallet.get(normalize_wallet(wallet), []),
            condition_ids,
            max_esports_markets=args.max_esports_closed_positions_per_wallet,
        )

    def load_closed_positions(wallet: str) -> list[dict]:
        return fetch_recent_esports_closed_positions_for_wallet(
            client,
            wallet,
            condition_ids,
            max_esports_closed_positions=args.max_esports_closed_positions_per_wallet,
            market_chunk_size=args.closed_position_market_chunk_size,
        )

    def profile_one_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            return profile_candidate_wallet(
                candidate,
                condition_ids,
                market_records_by_id=market_records_by_id,
                condition_type_by_id=condition_type_by_id,
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

    profiles_by_wallet = {
        normalize_wallet(row.get("wallet")): row
        for row in [*existing_profiles.values(), *profiles]
        if normalize_wallet(row.get("wallet"))
    }
    profiles_by_wallet = merge_profiles_with_candidates(profiles_by_wallet, candidates)
    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        min_participated_markets=args.leaderboard_min_participated_markets,
        min_avg_market_cash=args.leaderboard_min_avg_market_cash,
        require_tail_entry_field=True,
        require_current_scoring_version=True,
        max_leaderboard_wallets=args.max_leaderboard_wallets,
    )
    overlap_report = build_wallet_overlap_report(leaderboard)
    profiles_by_wallet = prune_profile_store(
        profiles_by_wallet,
        now_ts=now_ts,
        max_age_days=args.profile_store_max_age_days,
    )
    write_json(data_dir / "wallet_profiles.json", list(profiles_by_wallet.values()))
    write_json(data_dir / "smart_wallet_leaderboard.json", leaderboard)
    write_json(data_dir / "leaderboard_wallet_overlap.json", overlap_report)

    summary = {
        "classification_market_count": len(classification_set),
        "classification_source": classification_source,
        "discovery_market_count": len(discovery_slate),
        "candidate_wallet_count": len(candidates),
        "profile_candidate_wallet_count": len(profile_candidates),
        "profiled_wallet_count": profiled_count,
        **backfill_summary,
        "leaderboard_wallet_count": len(leaderboard),
        "leaderboard_union_market_count": overlap_report["union_market_count"],
        "leaderboard_pair_overlap_count": len(overlap_report["pair_overlaps"]),
        "market_trades_cache_hits": market_trades_cache_hits,
        "market_trades_api_fetches": market_trades_api_fetches,
        "partial_market_trades": partial_markets,
        "discovery_source": args.discovery_source,
        "slate": slate_meta,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(data_dir / "build_summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    print()
    print("搜集完成")
    print(f"- 历史 esports 胜负市场: {summary['classification_market_count']}")
    print(f"- 本轮发现市场: {summary['discovery_market_count']}")
    print(f"- 候选钱包: {summary['candidate_wallet_count']}")
    print(f"- 通过快速过滤的钱包: {summary['profile_candidate_wallet_count']}")
    print(f"- 本轮深度分析钱包: {summary['profiled_wallet_count']}")
    print(f"- A/B 优质钱包: {summary['leaderboard_wallet_count']}")
    print(f"- 输出目录: {data_dir}")
    return 0


def command_rescore_wallets(args: argparse.Namespace, client: PolymarketClient | None = None) -> int:
    data_dir = Path(args.data_dir)
    with acquire_build_lock(data_dir):
        return _rescore_tracked_wallets_unlocked(args, client=client)


def _rescore_tracked_wallets_unlocked(args: argparse.Namespace, client: PolymarketClient | None = None) -> dict[str, Any]:
    """Lightweight incremental re-score of the wallets currently on the leaderboard.

    Skips the expensive discovery/candidate phase: re-fetches each tracked wallet's
    trades (force refresh), backfills newly-settled markets, re-reconstructs the hold
    summary, re-classifies, and rewrites the leaderboard. Used between the 24h full
    rebuilds so followed wallets' records stay near-real-time.
    """
    data_dir = Path(args.data_dir)
    now_ts = int(datetime.now(timezone.utc).timestamp())

    leaderboard_in = read_json(data_dir / "smart_wallet_leaderboard.json", [])
    existing_profiles = {
        normalize_wallet(row.get("wallet")): row for row in read_json(data_dir / "wallet_profiles.json", [])
    }
    tracked: list[str] = []
    seen: set[str] = set()
    for row in leaderboard_in if isinstance(leaderboard_in, list) else []:
        if row.get("grade") != "A":
            continue
        wallet = normalize_wallet(row.get("wallet"))
        if wallet and wallet not in seen:
            seen.add(wallet)
            tracked.append(wallet)

    summary = {"rescored_wallet_count": 0, "tracked_wallet_count": len(tracked), "leaderboard_wallet_count": len(leaderboard_in or [])}
    if not tracked:
        print(json.dumps({"status": "rescore_skipped", "reason": "empty_leaderboard", **summary}, ensure_ascii=False))
        return summary

    client = client or build_client(args)

    classification_set = read_json(data_dir / "esports_classification_set.json", [])
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
    condition_ids = set(market_records_by_id)

    rescore_candidates: list[dict[str, Any]] = []
    for wallet in tracked:
        candidate = dict((existing_profiles.get(wallet) or {}).get("candidate") or {})
        candidate["wallet"] = wallet
        rescore_candidates.append(candidate)

    def fetch_raw(candidate: dict[str, Any]) -> tuple[str, list[dict]]:
        wallet = normalize_wallet(candidate.get("wallet"))
        trades = fetch_recent_user_trades_for_wallet(
            client,
            wallet,
            page_limit=args.user_history_trades_limit,
            max_pages=args.user_history_trades_max_pages,
            data_dir=data_dir,
            now_ts=now_ts,
            cache_ttl_days=args.market_trades_cache_ttl_days,
            force_refresh=True,  # incremental pass must see new trades, not the cache
            use_cache=not args.no_market_trades_cache,
        )
        return wallet, trades

    raw_results = run_ordered_io_tasks(rescore_candidates, fetch_raw, max_workers=args.max_workers)
    raw_user_trades_by_wallet: dict[str, list[dict]] = {}
    for index, result in enumerate(raw_results):
        fallback_wallet = normalize_wallet(rescore_candidates[index].get("wallet"))
        if isinstance(result, Exception):
            raw_user_trades_by_wallet[fallback_wallet] = []
            continue
        wallet, trades = result
        raw_user_trades_by_wallet[normalize_wallet(wallet)] = trades

    backfilled_market_records, backfill_summary = backfill_user_trade_submarkets(
        client,
        raw_user_trades_by_wallet,
        market_records_by_id,
        data_dir=data_dir,
        now_ts=now_ts,
        cache_ttl_days=args.market_trades_cache_ttl_days,
        force_refresh=True,  # newly-settled markets won't be in the (possibly stale) cache
        use_cache=not args.no_market_trades_cache,
        max_workers=args.max_workers,
    )
    if backfilled_market_records:
        market_records_by_id = {**market_records_by_id, **backfilled_market_records}
        condition_type_by_id = {
            **condition_type_by_id,
            **{
                condition_id: str(record.get("market_type") or "main_match")
                for condition_id, record in backfilled_market_records.items()
            },
        }
        condition_ids = set(market_records_by_id)

    def load_user_trades(wallet: str) -> list[dict]:
        return _filter_esports_user_trades(
            raw_user_trades_by_wallet.get(normalize_wallet(wallet), []),
            condition_ids,
            max_esports_markets=args.max_esports_closed_positions_per_wallet,
        )

    def profile_one(candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            return profile_candidate_wallet(
                candidate,
                condition_ids,
                market_records_by_id=market_records_by_id,
                condition_type_by_id=condition_type_by_id,
                user_trades_loader=load_user_trades,
                current_positions_loader=(
                    (lambda w: client.positions(w, limit=100))
                    if args.check_current_positions
                    else (lambda w: [])
                ),
                now_ts=now_ts,
            )
        except Exception as exc:
            return make_retryable_profile(candidate, exc, now_ts=now_ts)

    profile_results = run_ordered_io_tasks(rescore_candidates, profile_one, max_workers=args.max_workers)
    rescored = [
        make_retryable_profile(rescore_candidates[index], result, now_ts=now_ts)
        if isinstance(result, Exception)
        else result
        for index, result in enumerate(profile_results)
    ]

    profiles_by_wallet = dict(existing_profiles)
    for profile in rescored:
        wallet = normalize_wallet(profile.get("wallet"))
        if wallet:
            profiles_by_wallet[wallet] = profile
    profiles_by_wallet = merge_profiles_with_candidates(profiles_by_wallet, rescore_candidates)

    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        min_participated_markets=args.leaderboard_min_participated_markets,
        min_avg_market_cash=args.leaderboard_min_avg_market_cash,
        require_tail_entry_field=True,
        require_current_scoring_version=True,
        max_leaderboard_wallets=args.max_leaderboard_wallets,
    )
    overlap_report = build_wallet_overlap_report(leaderboard)
    write_json(data_dir / "wallet_profiles.json", list(profiles_by_wallet.values()))
    write_json(data_dir / "smart_wallet_leaderboard.json", leaderboard)
    write_json(data_dir / "leaderboard_wallet_overlap.json", overlap_report)

    summary = {
        "rescored_wallet_count": len(rescored),
        "tracked_wallet_count": len(tracked),
        "leaderboard_wallet_count": len(leaderboard),
        **backfill_summary,
    }
    print(json.dumps({"status": "rescore_complete", **summary}, ensure_ascii=False))
    return summary


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
    data_dir = Path(args.data_dir)
    leaderboard_rows = read_json(data_dir / "smart_wallet_leaderboard.json", [])
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
    data_dir = Path(args.data_dir)
    follow_dir = data_dir / "follow"
    now_ts = int(datetime.now(timezone.utc).timestamp())

    state_path = follow_dir / "follow_state.json"
    open_path = follow_dir / "follow_signals_open.json"
    perf_path = follow_dir / "follow_performance.json"
    results_path = follow_dir / "follow_results.jsonl"
    run_log_path = follow_dir / "follow_run_log.jsonl"
    active_cache_path = follow_dir / "active_market_cache.json"
    store = FollowStore(follow_dir / "follow.db")
    store.import_legacy_json(
        state_path=state_path,
        open_path=open_path,
        results_path=results_path,
        perf_path=perf_path,
    )
    leaderboard_path = data_dir / "smart_wallet_leaderboard.json"
    leaderboard_rows = read_json(leaderboard_path, [])
    leaderboard_wallets = {normalize_wallet(row.get("wallet")) for row in leaderboard_rows if normalize_wallet(row.get("wallet"))}
    leaderboard_validated_at = int(leaderboard_path.stat().st_mtime) if leaderboard_path.exists() else 0
    store.clear_revalidated_quarantine(leaderboard_wallets, validated_at=leaderboard_validated_at)
    quarantined_wallets = set(store.load_wallet_quarantine())
    eligible_wallet_rows = eligible_follow_wallets(
        leaderboard_rows,
        now_ts=now_ts,
        recency_days=args.follow_recency_days,
        quarantined_wallets=quarantined_wallets,
    )

    state = read_json(state_path, {"wallet_trade_state": {}})
    eligible_market_types_by_wallet = {
        row["wallet"]: {str(value) for value in (row.get("eligible_market_types") or []) if value}
        for row in eligible_wallet_rows
    }
    wallet_trade_state = store.load_wallet_trade_state()
    open_signals = store.load_open_signals()
    performance = store.load_performance()
    open_condition_ids_by_wallet: dict[str, set[str]] = {}
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        wallet = normalize_wallet(signal.get("wallet"))
        condition_id = str(signal.get("condition_id") or "").lower()
        if wallet and condition_id:
            open_condition_ids_by_wallet.setdefault(wallet, set()).add(condition_id)
    eligible_wallet_set = {row["wallet"] for row in eligible_wallet_rows}
    lifecycle_wallets = sorted(set(open_condition_ids_by_wallet) - eligible_wallet_set)
    follow_wallets = [
        *eligible_wallet_rows,
        *({"wallet": wallet, "follow_scope": "open_signals"} for wallet in lifecycle_wallets),
    ]

    active_markets, state, active_source = load_active_market_cache(
        client,
        state,
        cache_path=active_cache_path,
        now_ts=now_ts,
        gamma_pages=args.gamma_pages,
        ttl_seconds=args.event_cache_ttl_minutes * 60,
    )
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
        active_markets,
        now_ts=now_ts,
        observe_window_hours=args.observe_window_hours,
    )
    gate_open = bool(watched or open_signals)
    next_interval = desired_tick_interval(
        list(watched.values()),
        open_signals,
        now_ts=now_ts,
        observe_window_hours=args.observe_window_hours,
        min_tick_seconds=args.min_tick_seconds,
        max_tick_seconds=args.max_tick_seconds,
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

    tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
    tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
    markets_for_follow = {
        condition_id: market
        for condition_id, market in active_markets.items()
        if condition_id in tracked_condition_ids or condition_id in watched
    }

    if gate_open and follow_wallets:
        def fetch_trades_for_wallet(row: dict[str, Any]) -> tuple[str, list[dict], list[dict]]:
            wallet = normalize_wallet(row.get("wallet"))
            previous_cursor = (wallet_trade_state.get(wallet) or {}).get("last_trade_cursor")
            try:
                trades = fetch_user_trades_until_cursor(
                    client,
                    wallet,
                    previous_cursor=previous_cursor,
                    limit=args.user_trades_limit,
                    max_pages=args.user_trades_max_pages,
                )
                positions = []
                if wallet in eligible_wallet_set and previous_cursor is None and args.bootstrap_current_positions:
                    positions = client.positions(wallet, limit=args.positions_limit)
                return wallet, trades, positions
            except Exception:
                return wallet, [], []

        trade_results = run_ordered_io_tasks(
            follow_wallets,
            fetch_trades_for_wallet,
            max_workers=args.max_workers,
        )
        trade_request_count = len(follow_wallets)
        for result in trade_results:
            if isinstance(result, Exception):
                continue
            wallet, trades, positions = result
            wallet_can_open_new = wallet in eligible_wallet_set
            previous_cursor = (wallet_trade_state.get(wallet) or {}).get("last_trade_cursor")
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
                        eligible_market_types=eligible_market_types_by_wallet.get(wallet),
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
                        require_pre_match=args.require_pre_match,
                        quarantine_sell_frac=args.quarantine_sell_frac,
                        eligible_market_types=eligible_market_types_by_wallet.get(wallet),
                        conflict_policy=args.conflict_policy,
                        mirror_wallet_sells=args.mirror_wallet_sells,
                    )
                    after_ids = {signal.get("signal_id") for signal in open_signals}
                    new_signal_count += len(after_ids - before_ids)
                    bootstrap_position_count += len(bootstrap_trades)
                    for event in stats.get("quarantine_events") or []:
                        store.upsert_wallet_quarantine(event.get("wallet"), reason=str(event.get("reason") or ""), ts=int(event.get("timestamp") or now_ts))
                        quarantine_event_count += 1
                    market_type_not_eligible_count += stats.get("market_type_not_eligible_count", 0)
                    ignored_trade_count += stats.get("ignored_trade_count", 0)
                    exited_signal_count += stats.get("exited_signal_count", 0)
                    hedge_event_count += stats.get("hedge_event_count", 0)
                    opposite_blocked_count += stats.get("opposite_blocked_count", 0)
                wallet_trade_state[wallet] = {
                    "last_trade_cursor": next_cursor,
                    "last_seen_at": now_ts,
                }
                continue
            tracked_condition_ids = {str(condition_id).lower() for condition_id in watched}
            tracked_condition_ids.update(str(signal.get("condition_id") or "").lower() for signal in open_signals)
            markets_for_follow = {
                condition_id: market
                for condition_id, market in active_markets.items()
                if condition_id in tracked_condition_ids or condition_id in watched
            }
            if wallet_can_open_new:
                wallet_tracked_condition_ids = tracked_condition_ids
            else:
                wallet_tracked_condition_ids = open_condition_ids_by_wallet.get(wallet, set())
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
                require_pre_match=args.require_pre_match,
                quarantine_sell_frac=args.quarantine_sell_frac,
                eligible_market_types=eligible_market_types_by_wallet.get(wallet) if wallet_can_open_new else None,
                conflict_policy=args.conflict_policy,
                mirror_wallet_sells=args.mirror_wallet_sells,
            )
            after_ids = {signal.get("signal_id") for signal in open_signals}
            new_signal_count += len(after_ids - before_ids)
            total_new_trade_count += len(new_trades)
            watched_new_trade_count += len(watched_trades)
            ignored_trade_count += stats.get("ignored_trade_count", 0) + (len(new_trades) - len(watched_trades))
            market_type_not_eligible_count += stats.get("market_type_not_eligible_count", 0)
            exited_signal_count += stats.get("exited_signal_count", 0)
            hedge_event_count += stats.get("hedge_event_count", 0)
            opposite_blocked_count += stats.get("opposite_blocked_count", 0)
            for event in stats.get("quarantine_events") or []:
                store.upsert_wallet_quarantine(event.get("wallet"), reason=str(event.get("reason") or ""), ts=int(event.get("timestamp") or now_ts))
                quarantine_event_count += 1
            wallet_trade_state[wallet] = {
                "last_trade_cursor": next_cursor,
                "last_seen_at": now_ts,
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
        if event["wallet"] in existing_quarantine:
            continue
        store.upsert_wallet_quarantine(event["wallet"], reason=event["reason"], ts=int(event["timestamp"]))
        existing_quarantine.add(event["wallet"])
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
        "market_type_not_eligible_count": market_type_not_eligible_count,
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
    last_rescore_at = now_init
    tick_count = 0
    first_error_at: float | None = None
    stop_requested = {"value": False, "reason": ""}

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
        nonlocal last_build_at, last_rescore_at
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if force or now_ts - last_build_at >= int(args.pool_refresh_hours * 3600):
            command_build_leaderboard(args, client=client)
            now_done = int(datetime.now(timezone.utc).timestamp())
            last_build_at = now_done
            last_rescore_at = now_done  # full build already re-scored everyone
            return True
        return False

    def maybe_rescore() -> None:
        nonlocal last_rescore_at
        interval = int(getattr(args, "wallet_rescore_hours", 0) * 3600)
        if interval <= 0:
            return
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if now_ts - last_rescore_at >= interval:
            command_rescore_wallets(args, client=client)
            last_rescore_at = int(datetime.now(timezone.utc).timestamp())

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
                maybe_rescore()  # no-op right after a full build (last_rescore_at was reset)
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
        data_dir=Path(args.data_dir),
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
        stream_poll_seconds=args.stream_poll_seconds,
        stream_heartbeat_seconds=args.stream_heartbeat_seconds,
        max_stream_clients=args.max_stream_clients,
    )
    server = create_server(config)
    host, port = server.server_address[:2]
    print(f"dashboard listening on http://{host}:{port} data_dir={args.data_dir}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("dashboard stopped", flush=True)
    finally:
        server.server_close()
    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket esports smart-wallet analysis")
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    parser.add_argument("--timeout", type=int, default=30)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_build_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--gamma-pages", type=int, default=10)
        subparser.add_argument("--refresh-classification", action="store_true")
        subparser.add_argument("--classification-cache-ttl-hours", type=int, default=24)
        subparser.add_argument("--classification-lookback-days", type=int, default=14)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)
        subparser.add_argument("--min-market-volume", type=float, default=25_000)
        subparser.add_argument("--fallback-min-market-volume", type=float, default=10_000)
        subparser.add_argument("--submarket-min-market-volume", type=float, default=5_000)
        subparser.add_argument("--submarket-fallback-min-market-volume", type=float, default=1_000)
        subparser.add_argument("--discovery-lookback-days", type=int, default=14)
        subparser.add_argument("--target-markets", type=int, default=20)
        subparser.add_argument("--submarket-target-markets", type=int, default=60)
        subparser.add_argument("--game-winner-target-markets", type=int, default=60)
        subparser.add_argument("--map-winner-target-markets", type=int, default=60)
        subparser.add_argument("--max-markets-per-run", type=int)
        subparser.add_argument("--submarket-max-markets-per-run", type=int, default=60)
        subparser.add_argument("--game-winner-max-markets-per-run", type=int, default=60)
        subparser.add_argument("--map-winner-max-markets-per-run", type=int, default=60)
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
        subparser.add_argument("--max-profiles-per-run", type=int, default=150)
        subparser.add_argument("--max-esports-closed-positions-per-wallet", type=int, default=100)
        subparser.add_argument("--closed-position-market-chunk-size", type=int, default=50)
        subparser.add_argument("--user-history-trades-limit", type=int, default=500)
        # Fallback-only path (wallets not in the per-market pool); kept small on purpose —
        # 3 pages ≈ 1500 trades is plenty to cover a wallet's recent esports markets.
        subparser.add_argument("--user-history-trades-max-pages", type=int, default=3)
        subparser.add_argument("--min-profile-participated-markets", type=int, default=3)
        subparser.add_argument("--min-profile-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--leaderboard-min-participated-markets", type=int, default=3)
        subparser.add_argument("--leaderboard-min-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--max-leaderboard-wallets", type=int, default=30)
        subparser.add_argument("--allow-dirty-profile-candidates", action="store_true")
        subparser.add_argument("--check-current-positions", action="store_true")
        subparser.add_argument("--profile-refresh-ttl-days", type=int, default=7)
        subparser.add_argument("--profile-store-max-age-days", type=int, default=180)
        subparser.set_defaults(func=command_build_leaderboard)

    def add_follow_arguments(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--stake-usdc", type=float, required=True)
        subparser.add_argument("--follow-recency-days", type=int, default=30)
        subparser.add_argument("--event-gate-horizon-hours", type=float, default=24)
        subparser.add_argument("--observe-window-hours", type=float, default=24)
        subparser.add_argument("--event-cache-ttl-minutes", type=int, default=15)
        subparser.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
        subparser.add_argument("--resolution-gamma-pages", type=int, default=2)
        subparser.add_argument("--max-slippage-over-entry", type=float, default=0.05)
        subparser.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=True)
        subparser.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
        subparser.add_argument("--execution-mode", choices=["paper", "live"], default="paper")
        subparser.add_argument("--run-log-retention-days", type=int, default=7)
        subparser.add_argument("--results-retention-days", type=int, default=0)
        subparser.add_argument("--gamma-pages", type=int, default=3)
        subparser.add_argument("--positions-limit", type=int, default=100)
        subparser.add_argument("--user-trades-limit", type=int, default=100)
        subparser.add_argument("--user-trades-max-pages", type=int, default=3)
        subparser.add_argument("--bootstrap-current-positions", dest="bootstrap_current_positions", action="store_true", default=True)
        subparser.add_argument("--no-bootstrap-current-positions", dest="bootstrap_current_positions", action="store_false")
        subparser.add_argument("--max-follow-legs", type=int, default=10)
        subparser.add_argument("--min-tick-seconds", type=int, default=180)
        subparser.add_argument("--max-tick-seconds", type=int, default=900)
        subparser.add_argument("--consensus-min-same-side", type=int, default=1)
        subparser.add_argument("--consensus-block-opposite", dest="consensus_block_opposite", action="store_true", default=True)
        subparser.add_argument("--no-consensus-block-opposite", dest="consensus_block_opposite", action="store_false")
        subparser.add_argument("--conflict-policy", choices=["dual_follow", "exit_on_opposite"], default="dual_follow")
        subparser.add_argument("--mirror-wallet-sells", action="store_true")
        subparser.add_argument("--quarantine-sell-frac", type=float, default=0.2)
        subparser.add_argument("--max-workers", type=int, default=8)
        subparser.add_argument("--max-requests-per-second", type=float, default=10)
        subparser.add_argument("--request-burst", type=int, default=5)
        subparser.add_argument("--max-retry-after-seconds", type=float, default=60)

    build = subparsers.add_parser("build-leaderboard")
    add_build_arguments(build)

    collect = subparsers.add_parser("collect", help="one-shot wallet collection and leaderboard build")
    add_build_arguments(collect)

    rescore = subparsers.add_parser(
        "rescore-wallets",
        help="lightweight re-score of current leaderboard wallets (no discovery)",
    )
    add_build_arguments(rescore)
    rescore.set_defaults(func=command_rescore_wallets)

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
    run.add_argument("--stake-usdc", type=float, required=True)
    run.add_argument("--follow-recency-days", type=int, default=30)
    run.add_argument("--event-gate-horizon-hours", type=float, default=24)
    run.add_argument("--observe-window-hours", type=float, default=24)
    run.add_argument("--event-cache-ttl-minutes", type=int, default=15)
    run.add_argument("--resolution-cache-ttl-seconds", type=int, default=60)
    run.add_argument("--resolution-gamma-pages", type=int, default=2)
    run.add_argument("--max-slippage-over-entry", type=float, default=0.05)
    run.add_argument("--require-pre-match", dest="require_pre_match", action="store_true", default=True)
    run.add_argument("--no-require-pre-match", dest="require_pre_match", action="store_false")
    run.add_argument("--execution-mode", choices=["paper", "live"], default="paper")
    run.add_argument("--run-log-retention-days", type=int, default=7)
    run.add_argument("--results-retention-days", type=int, default=0)
    run.add_argument("--positions-limit", type=int, default=100)
    run.add_argument("--user-trades-limit", type=int, default=100)
    run.add_argument("--user-trades-max-pages", type=int, default=3)
    run.add_argument("--bootstrap-current-positions", dest="bootstrap_current_positions", action="store_true", default=True)
    run.add_argument("--no-bootstrap-current-positions", dest="bootstrap_current_positions", action="store_false")
    run.add_argument("--max-follow-legs", type=int, default=10)
    run.add_argument("--min-tick-seconds", type=int, default=180)
    run.add_argument("--max-tick-seconds", type=int, default=900)
    run.add_argument("--consensus-min-same-side", type=int, default=1)
    run.add_argument("--consensus-block-opposite", dest="consensus_block_opposite", action="store_true", default=True)
    run.add_argument("--no-consensus-block-opposite", dest="consensus_block_opposite", action="store_false")
    run.add_argument("--conflict-policy", choices=["dual_follow", "exit_on_opposite"], default="dual_follow")
    run.add_argument("--mirror-wallet-sells", action="store_true")
    run.add_argument("--quarantine-sell-frac", type=float, default=0.2)
    run.add_argument("--error-retry-seconds", type=int, default=180)
    run.add_argument("--max-consecutive-error-seconds", type=int, default=600)
    run.add_argument("--pool-refresh-hours", type=float, default=24)
    run.add_argument("--wallet-rescore-hours", type=float, default=2)
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
    serve.add_argument("--stream-poll-seconds", type=float, default=2.0)
    serve.add_argument("--stream-heartbeat-seconds", type=float, default=15.0)
    serve.add_argument("--max-stream-clients", type=int, default=8)
    serve.set_defaults(func=command_serve)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
