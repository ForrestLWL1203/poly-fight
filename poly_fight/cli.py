from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .api import PolymarketClient
from .core import (
    SCORING_VERSION,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    classify_wallet,
    event_to_market_record,
    normalize_wallet,
    profile_candidate_wallet,
    summarize_closed_positions,
    to_float,
)


DATA_DIR = Path("data")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


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


def fetch_closed_positions_for_wallet(
    client: PolymarketClient,
    wallet: str,
    *,
    max_closed_positions: int,
    page_limit: int = 500,
) -> list[dict]:
    positions: list[dict] = []
    offset = 0
    while len(positions) < max_closed_positions:
        batch = client.closed_positions(wallet, limit=page_limit, offset=offset)
        if not batch:
            break
        positions.extend(batch)
        if len(batch) < page_limit:
            break
        offset += page_limit
    return positions[:max_closed_positions]


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
            tail_entry_count = int(candidate.get("tail_entry_market_count") or 0)
            if tail_entry_count > 0:
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
) -> list[dict[str, Any]]:
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    max_inactive_seconds = max_inactive_days * 86400
    leaderboard = []
    for profile in profiles_by_wallet.values():
        if profile.get("grade") != "A":
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
        if int(candidate.get("tail_entry_market_count") or 0) > 0:
            continue
        leaderboard.append(profile)
    return sorted(leaderboard, key=lambda row: (row.get("grade", ""), -to_float(row.get("esports_roi"))))


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


def command_build_leaderboard(args: argparse.Namespace) -> int:
    client = PolymarketClient(timeout=args.timeout)
    data_dir = Path(args.data_dir)
    now_ts = int(datetime.now(timezone.utc).timestamp())

    classification_path = data_dir / "esports_classification_set.json"
    classification_source = "api"
    if classification_path.exists() and not should_refresh_file_cache(
        classification_path.stat().st_mtime,
        now_ts=now_ts,
        ttl_hours=args.classification_cache_ttl_hours,
        force_refresh=args.refresh_classification,
    ):
        classification_set = read_json(classification_path, [])
        classification_source = "cache"
    else:
        closed_events = client.list_events_paginated(closed=True, active=None, max_pages=args.gamma_pages)
        classification_set = build_classification_set(closed_events)
        write_json(classification_path, classification_set)

    lookback_steps = (args.discovery_lookback_days,) if args.discovery_lookback_days else (7, 14, 30)
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
        target_markets=args.target_markets,
        max_markets_per_run=market_count,
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
        for market in discovery_slate:
            condition_id = market["condition_id"]
            try:
                holders_by_market[condition_id] = client.holders(condition_id, limit=args.holders_limit)
                prices_by_market[condition_id] = market.get("outcome_prices") or []
            except RuntimeError:
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
        for market in discovery_slate:
            condition_id = market["condition_id"]
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
            trades_by_market[condition_id] = trades
            if partial:
                partial_markets.append(condition_id)
            if trades_source == "cache":
                market_trades_cache_hits += 1
            else:
                market_trades_api_fetches += 1
        market_end_times = {}
        market_start_times = {}
        from .core import parse_dt

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
    profiles = []
    profiled_count = 0
    for candidate in profile_candidates:
        wallet = candidate["wallet"]
        cached = existing_profiles.get(wallet)
        if should_use_cached_profile(cached, now_ts=now_ts, ttl_seconds=args.profile_refresh_ttl_days * 86400):
            rated = merge_cached_profile_with_candidate(cached, candidate)
        elif profiled_count >= args.max_profiles_per_run:
            continue
        else:
            profiled_count += 1
            rated = profile_candidate_wallet(
                candidate,
                condition_ids,
                closed_positions_loader=lambda w: fetch_closed_positions_for_wallet(
                    client,
                    w,
                    max_closed_positions=args.max_closed_positions_per_wallet,
                ),
                current_positions_loader=(
                    (lambda w: client.positions(w, limit=100))
                    if args.check_current_positions
                    else (lambda w: [])
                ),
                now_ts=now_ts,
            )
        profiles.append(rated)

    profiles_by_wallet = {
        normalize_wallet(row.get("wallet")): row
        for row in [*existing_profiles.values(), *profiles]
        if int(row.get("scoring_version") or 0) == SCORING_VERSION
    }
    profiles_by_wallet = merge_profiles_with_candidates(profiles_by_wallet, candidates)
    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        min_participated_markets=args.leaderboard_min_participated_markets,
        min_avg_market_cash=args.leaderboard_min_avg_market_cash,
        require_tail_entry_field=True,
    )
    for row in [item for item in leaderboard if not item.get("esports_condition_ids")]:
        if profiled_count >= args.max_profiles_per_run:
            break
        candidate = row.get("candidate") or {"wallet": row.get("wallet")}
        profiled_count += 1
        refreshed = profile_candidate_wallet(
            candidate,
            condition_ids,
            closed_positions_loader=lambda w: fetch_closed_positions_for_wallet(
                client,
                w,
                max_closed_positions=args.max_closed_positions_per_wallet,
            ),
            current_positions_loader=(
                (lambda w: client.positions(w, limit=100))
                if args.check_current_positions
                else (lambda w: [])
            ),
            now_ts=now_ts,
        )
        profiles_by_wallet[normalize_wallet(refreshed.get("wallet"))] = refreshed
    profiles_by_wallet = merge_profiles_with_candidates(profiles_by_wallet, candidates)
    leaderboard = build_leaderboard_from_profiles(
        profiles_by_wallet,
        now_ts=now_ts,
        min_participated_markets=args.leaderboard_min_participated_markets,
        min_avg_market_cash=args.leaderboard_min_avg_market_cash,
        require_tail_entry_field=True,
    )
    overlap_report = build_wallet_overlap_report(leaderboard)
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
    print(f"- 历史 esports 主市场: {summary['classification_market_count']}")
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
        end_dt = None
        if end:
            from .core import parse_dt

            end_dt = parse_dt(end)
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
    client = PolymarketClient(timeout=args.timeout)
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
    from .core import parse_dt

    end_dt = parse_dt(market.get("end_date"))
    if end_dt:
        output["hours_to_end"] = round((end_dt - datetime.now(timezone.utc)).total_seconds() / 3600, 2)
    write_json(data_dir / "last_event_analysis.json", output)
    print(json.dumps(output, indent=2, ensure_ascii=False, sort_keys=True))
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
        subparser.add_argument("--min-market-volume", type=float, default=25_000)
        subparser.add_argument("--fallback-min-market-volume", type=float, default=10_000)
        subparser.add_argument("--discovery-lookback-days", type=int, default=14)
        subparser.add_argument("--target-markets", type=int, default=20)
        subparser.add_argument("--max-markets-per-run", type=int)
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
        subparser.add_argument("--max-closed-positions-per-wallet", type=int, default=500)
        subparser.add_argument("--min-profile-participated-markets", type=int, default=3)
        subparser.add_argument("--min-profile-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--leaderboard-min-positive-rate", type=float, default=0.95, help=argparse.SUPPRESS)
        subparser.add_argument("--leaderboard-max-median-entry-price", type=float, default=0.65, help=argparse.SUPPRESS)
        subparser.add_argument("--leaderboard-max-high-price-entry-rate", type=float, default=0.05, help=argparse.SUPPRESS)
        subparser.add_argument("--leaderboard-min-participated-markets", type=int, default=3)
        subparser.add_argument("--leaderboard-min-avg-market-cash", type=float, default=1_500)
        subparser.add_argument("--allow-dirty-profile-candidates", action="store_true")
        subparser.add_argument("--check-current-positions", action="store_true")
        subparser.add_argument("--profile-refresh-ttl-days", type=int, default=7)
        subparser.set_defaults(func=command_build_leaderboard)

    build = subparsers.add_parser("build-leaderboard")
    add_build_arguments(build)

    collect = subparsers.add_parser("collect", help="one-shot wallet collection and leaderboard build")
    add_build_arguments(collect)

    analyze = subparsers.add_parser("analyze-event")
    analyze.add_argument("--gamma-pages", type=int, default=3)
    analyze.add_argument("--event-slug")
    analyze.add_argument("--condition-id")
    analyze.add_argument("--holders-limit", type=int, default=10)
    analyze.set_defaults(func=command_analyze_event)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
