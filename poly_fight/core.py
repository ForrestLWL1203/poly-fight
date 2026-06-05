from __future__ import annotations

from datetime import datetime, timezone
from math import sqrt
from statistics import median
from typing import Any, Iterable


SECONDS_PER_DAY = 86400
SCORING_VERSION = 7
TRADE_BEHAVIOR_MIN_MARKETS = 4
TRADE_BEHAVIOR_EXCLUDE_RATE = 0.5
ESPORTS_TAGS = {
    "esports",
    "dota-2",
    "counter-strike-2",
    "cs2",
    "league-of-legends",
    "valorant",
}


def normalize_wallet(wallet: str | None) -> str:
    return (wallet or "").strip().lower()


def parse_jsonish(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if not isinstance(value, str):
        return value
    try:
        import json

        return json.loads(value)
    except Exception:
        return default


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def event_tags(event: dict[str, Any]) -> set[str]:
    tags = set()
    for tag in event.get("tags") or []:
        if isinstance(tag, dict):
            tag_value = tag.get("slug") or tag.get("label") or tag.get("name")
        else:
            tag_value = tag
        if tag_value:
            tags.add(str(tag_value).lower())
    return tags


def is_esports_event(event: dict[str, Any]) -> bool:
    return bool(event_tags(event) & ESPORTS_TAGS)


def game_family_from_event(event: dict[str, Any]) -> str:
    tags = event_tags(event)
    title = (event.get("title") or "").lower()
    if "counter-strike-2" in tags or "cs2" in tags or "counter-strike" in title:
        return "cs2"
    if "dota-2" in tags or title.startswith("dota 2:"):
        return "dota2"
    if "league-of-legends" in tags or title.startswith("lol:"):
        return "lol"
    if "valorant" in tags or title.startswith("valorant:"):
        return "valorant"
    return "other"


def is_binary_market(market: dict[str, Any]) -> bool:
    outcomes = parse_jsonish(market.get("outcomes"), [])
    return isinstance(outcomes, list) and len(outcomes) == 2


def is_settled_binary_prices(prices: list[float]) -> bool:
    if len(prices) != 2:
        return False
    return sorted(round(price, 6) for price in prices) == [0.0, 1.0]


def is_main_match_title(title: str) -> bool:
    text = title.lower()
    has_game_prefix = text.startswith(("dota 2:", "counter-strike:", "lol:", "valorant:"))
    if has_game_prefix and " vs " in text:
        return True
    return " vs " in text and any(
        marker in text
        for marker in (
            "bo1",
            "bo2",
            "bo3",
            "bo5",
            "best of",
            "match",
            "group",
            "playoff",
            "qualifier",
            "final",
        )
    )


def is_non_main_market(question: str) -> bool:
    q = question.lower()
    bad_fragments = [
        "game 1",
        "game 2",
        "game 3",
        "game 4",
        "game 5",
        "map 1",
        "map 2",
        "map 3",
        "map 4",
        "map 5",
        "total maps",
        "handicap",
        "spread",
        "correct score",
    ]
    return any(fragment in q for fragment in bad_fragments)


def choose_main_market(event: dict[str, Any]) -> dict[str, Any] | None:
    markets = [m for m in event.get("markets") or [] if m.get("conditionId") and is_binary_market(m)]
    if not markets:
        return None
    title = (event.get("title") or "").strip().lower()
    for market in markets:
        if (market.get("question") or "").strip().lower() == title:
            return market
    candidates = [m for m in markets if not is_non_main_market(m.get("question") or "")]
    if not candidates:
        return None
    return max(candidates, key=lambda m: to_float(m.get("volume")) + to_float(m.get("liquidity")))


def event_to_market_record(event: dict[str, Any]) -> dict[str, Any] | None:
    if not is_esports_event(event):
        return None
    market = choose_main_market(event)
    if not market:
        return None
    match_start_time = market.get("eventStartTime") or event.get("startTime") or market.get("gameStartTime")
    return {
        "condition_id": str(market.get("conditionId")).lower(),
        "event_id": str(event.get("id") or ""),
        "event_slug": event.get("slug"),
        "title": event.get("title"),
        "question": market.get("question"),
        "outcomes": parse_jsonish(market.get("outcomes"), []),
        "outcome_prices": [to_float(v) for v in parse_jsonish(market.get("outcomePrices"), [])],
        "end_date": event.get("endDate") or market.get("endDate"),
        "match_start_time": match_start_time,
        "market_start_time": market.get("gameStartTime") or market.get("eventStartTime") or event.get("startTime"),
        "volume": to_float(event.get("volume") or market.get("volume")),
        "volume24hr": to_float(event.get("volume24hr")),
        "liquidity": to_float(event.get("liquidity") or market.get("liquidity")),
        "closed": bool(event.get("closed")),
        "game_family": game_family_from_event(event),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_classification_set(
    events: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_days: int | None = None,
) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    records: dict[str, dict[str, Any]] = {}
    for event in events:
        record = event_to_market_record(event)
        if not record:
            continue
        end = parse_dt(record.get("end_date"))
        if not end or end > now:
            continue
        if not is_settled_binary_prices(record.get("outcome_prices") or []):
            continue
        if not is_main_match_title(str(record.get("title") or record.get("question") or "")):
            continue
        if lookback_days is not None:
            days_ago = (now - end).total_seconds() / SECONDS_PER_DAY
            if days_ago < 0 or days_ago > lookback_days:
                continue
        records[record["condition_id"]] = record
    return sorted(records.values(), key=lambda row: row.get("end_date") or "", reverse=True)


def build_discovery_slate(
    classification_set: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_steps: tuple[int, ...] = (7, 14, 30),
    min_market_volume: float = 25_000,
    fallback_min_market_volume: float = 10_000,
    target_markets: int = 30,
    max_markets_per_run: int = 50,
    market_offset: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = now or datetime.now(timezone.utc)

    def select(days: int, min_volume: float) -> list[dict[str, Any]]:
        selected = []
        for market in classification_set:
            end = parse_dt(market.get("end_date"))
            if not end:
                continue
            days_ago = (now - end).total_seconds() / SECONDS_PER_DAY
            if 0 <= days_ago <= days and to_float(market.get("volume")) >= min_volume:
                selected.append(market)
        return sorted(selected, key=lambda row: to_float(row.get("volume")), reverse=True)

    selected: list[dict[str, Any]] = []
    selected_days = lookback_steps[-1]
    selected_min_volume = min_market_volume
    for days in lookback_steps:
        selected = select(days, min_market_volume)
        selected_days = days
        if len(selected) >= target_markets:
            break
    if len(selected) < target_markets:
        selected = select(lookback_steps[-1], fallback_min_market_volume)
        selected_min_volume = fallback_min_market_volume

    total_selected_market_count = len(selected)
    selected = selected[market_offset : market_offset + max_markets_per_run]
    return selected, {
        "selected_lookback_days": selected_days,
        "selected_min_market_volume": selected_min_volume,
        "target_markets": target_markets,
        "market_offset": market_offset,
        "max_markets_per_run": max_markets_per_run,
        "total_selected_market_count": total_selected_market_count,
        "market_count": len(selected),
    }


def trade_cash(trade: dict[str, Any]) -> float:
    return to_float(trade.get("cash")) or to_float(trade.get("size")) * to_float(trade.get("price"))


def build_candidate_wallets(
    trades_by_market: dict[str, list[dict[str, Any]]],
    *,
    market_end_times: dict[str, int] | None = None,
    market_start_times: dict[str, int] | None = None,
    min_trade_cash: float = 50,
    participation_threshold: int = 8,
    top_participation_count: int = 100,
    total_cash_threshold: float = 5_000,
    single_market_cash_threshold: float = 1_000,
    max_candidate_wallets: int = 300,
    tail_entry_price_threshold: float = 0.75,
) -> list[dict[str, Any]]:
    wallets: dict[str, dict[str, Any]] = {}
    market_cash_by_wallet: dict[str, dict[str, float]] = {}
    market_size_by_wallet: dict[str, dict[str, float]] = {}
    market_trade_counts_by_wallet: dict[str, dict[str, int]] = {}
    market_outcomes_by_wallet: dict[str, dict[str, set[str]]] = {}
    market_last_trade_by_wallet: dict[str, dict[str, int]] = {}
    market_end_times = market_end_times or {}
    market_start_times = market_start_times or {}
    for condition_id, trades in trades_by_market.items():
        for trade in trades:
            wallet = normalize_wallet(trade.get("proxyWallet") or trade.get("wallet"))
            if not wallet:
                continue
            cash = trade_cash(trade)
            if cash < min_trade_cash:
                continue
            price = to_float(trade.get("price"))
            size = to_float(trade.get("size") or trade.get("amount"))
            if not size and price > 0:
                size = cash / price
            row = wallets.setdefault(
                wallet,
                {
                    "wallet": wallet,
                    "participated_markets": set(),
                    "total_trade_count": 0,
                    "total_cash_volume": 0.0,
                    "last_seen_at": 0,
                },
            )
            row["participated_markets"].add(condition_id)
            row["total_trade_count"] += 1
            row["total_cash_volume"] += cash
            row["last_seen_at"] = max(row["last_seen_at"], to_int(trade.get("timestamp")))
            wallet_market_cash = market_cash_by_wallet.setdefault(wallet, {})
            wallet_market_cash[condition_id] = wallet_market_cash.get(condition_id, 0.0) + cash
            if size > 0:
                wallet_market_size = market_size_by_wallet.setdefault(wallet, {})
                wallet_market_size[condition_id] = wallet_market_size.get(condition_id, 0.0) + size
            wallet_market_counts = market_trade_counts_by_wallet.setdefault(wallet, {})
            wallet_market_counts[condition_id] = wallet_market_counts.get(condition_id, 0) + 1
            outcome = str(trade.get("outcome") or trade.get("outcomeIndex") or "")
            if outcome:
                wallet_market_outcomes = market_outcomes_by_wallet.setdefault(wallet, {})
                wallet_market_outcomes.setdefault(condition_id, set()).add(outcome)
            wallet_market_last = market_last_trade_by_wallet.setdefault(wallet, {})
            wallet_market_last[condition_id] = max(
                wallet_market_last.get(condition_id, 0),
                to_int(trade.get("timestamp")),
            )

    rows = []
    for wallet, row in wallets.items():
        market_cash = market_cash_by_wallet.get(wallet, {})
        market_size = market_size_by_wallet.get(wallet, {})
        per_market = list(market_cash.values())
        trade_counts = market_trade_counts_by_wallet.get(wallet, {})
        outcome_sets = market_outcomes_by_wallet.get(wallet, {})
        last_trades = market_last_trade_by_wallet.get(wallet, {})
        two_sided_market_count = sum(1 for outcomes in outcome_sets.values() if len(outcomes) >= 2)
        high_churn_market_count = sum(1 for count in trade_counts.values() if count >= 20)
        last_entry_hours_to_start = []
        last_entry_hours_to_end = []
        tail_entry_market_count = 0
        for condition_id, last_ts in last_trades.items():
            start_ts = market_start_times.get(condition_id) or market_end_times.get(condition_id)
            end_ts = market_end_times.get(condition_id)
            if start_ts and last_ts:
                hours = (start_ts - last_ts) / 3600
                last_entry_hours_to_start.append(hours)
                size = market_size.get(condition_id, 0.0)
                avg_price = market_cash.get(condition_id, 0.0) / size if size > 0 else 0.0
                if hours < 2 and avg_price >= tail_entry_price_threshold:
                    tail_entry_market_count += 1
            if end_ts and last_ts:
                last_entry_hours_to_end.append((end_ts - last_ts) / 3600)
        late_entry_market_count = sum(1 for hours in last_entry_hours_to_start if hours < 2)
        early_entry_market_count = sum(1 for hours in last_entry_hours_to_start if hours >= 2)
        participated_market_count = len(row["participated_markets"])
        max_single_market_cash = max(per_market) if per_market else 0.0
        avg_market_cash = row["total_cash_volume"] / participated_market_count if participated_market_count else 0
        total_size = sum(market_size.values())
        avg_entry_price = row["total_cash_volume"] / total_size if total_size > 0 else 0.0
        rows.append(
            {
                "wallet": wallet,
                "participated_market_count": participated_market_count,
                "participated_market_ids": sorted(row["participated_markets"]),
                "total_trade_count": row["total_trade_count"],
                "total_cash_volume": round(row["total_cash_volume"], 6),
                "max_single_market_cash": round(max_single_market_cash, 6),
                "avg_market_cash": round(avg_market_cash, 6),
                "two_sided_market_count": two_sided_market_count,
                "high_churn_market_count": high_churn_market_count,
                "late_entry_market_count": late_entry_market_count,
                "tail_entry_market_count": tail_entry_market_count,
                "early_entry_market_count": early_entry_market_count,
                "avg_entry_price": round(avg_entry_price, 8),
                "median_last_entry_hours_to_start": round(median(last_entry_hours_to_start), 8)
                if last_entry_hours_to_start
                else 0.0,
                "median_last_entry_hours_to_end": round(median(last_entry_hours_to_end), 8)
                if last_entry_hours_to_end
                else 0.0,
                "last_seen_at": row["last_seen_at"],
            }
        )

    rows.sort(key=lambda row: (row["participated_market_count"], row["total_cash_volume"]), reverse=True)
    top_wallets = {
        row["wallet"]
        for row in rows[:top_participation_count]
        if row["participated_market_count"] >= 2
    }

    candidates = []
    for row in rows:
        reasons = []
        if row["participated_market_count"] >= participation_threshold or row["wallet"] in top_wallets:
            reasons.append("high_participation")
        if (
            row["total_cash_volume"] >= total_cash_threshold
            or row["max_single_market_cash"] >= single_market_cash_threshold
        ):
            reasons.append("large_size")
        if reasons:
            candidates.append({**row, "candidate_reasons": reasons})
    candidates.sort(
        key=lambda row: (
            "large_size" in row["candidate_reasons"],
            row["max_single_market_cash"],
            row["total_cash_volume"],
            row["participated_market_count"],
        ),
        reverse=True,
    )
    return candidates[:max_candidate_wallets]


def build_candidate_wallets_from_holders(
    holders_by_market: dict[str, list[dict[str, Any]]],
    prices_by_market: dict[str, list[float]],
    *,
    participation_threshold: int = 8,
    top_participation_count: int = 100,
    total_usd_threshold: float = 5_000,
    single_market_usd_threshold: float = 1_000,
    max_candidate_wallets: int = 300,
) -> list[dict[str, Any]]:
    wallets: dict[str, dict[str, Any]] = {}
    market_usd_by_wallet: dict[str, dict[str, float]] = {}
    for condition_id, token_blocks in holders_by_market.items():
        prices = prices_by_market.get(condition_id) or []
        for token_index, token_block in enumerate(token_blocks):
            price = prices[token_index] if token_index < len(prices) else 0.0
            for holder in token_block.get("holders") or []:
                wallet = normalize_wallet(holder.get("proxyWallet") or holder.get("wallet"))
                if not wallet:
                    continue
                outcome_index = to_int(holder.get("outcomeIndex"), token_index)
                outcome_price = prices[outcome_index] if outcome_index < len(prices) else price
                amount = to_float(holder.get("amount") or holder.get("balance"))
                usd_value = amount * outcome_price
                row = wallets.setdefault(
                    wallet,
                    {
                        "wallet": wallet,
                        "participated_markets": set(),
                        "holder_snapshot_count": 0,
                        "total_holder_usd": 0.0,
                        "last_seen_at": 0,
                    },
                )
                row["participated_markets"].add(condition_id)
                row["holder_snapshot_count"] += 1
                row["total_holder_usd"] += usd_value
                wallet_market_usd = market_usd_by_wallet.setdefault(wallet, {})
                wallet_market_usd[condition_id] = wallet_market_usd.get(condition_id, 0.0) + usd_value

    rows = []
    for wallet, row in wallets.items():
        per_market = list(market_usd_by_wallet.get(wallet, {}).values())
        participated_market_count = len(row["participated_markets"])
        max_single_market_usd = max(per_market) if per_market else 0.0
        avg_market_usd = row["total_holder_usd"] / participated_market_count if participated_market_count else 0.0
        rows.append(
            {
                "wallet": wallet,
                "participated_market_count": participated_market_count,
                "participated_market_ids": sorted(row["participated_markets"]),
                "holder_snapshot_count": row["holder_snapshot_count"],
                "total_holder_usd": round(row["total_holder_usd"], 6),
                "max_single_market_usd": round(max_single_market_usd, 6),
                "avg_market_usd": round(avg_market_usd, 6),
            }
        )

    rows.sort(key=lambda row: (row["participated_market_count"], row["total_holder_usd"]), reverse=True)
    top_wallets = {
        row["wallet"]
        for row in rows[:top_participation_count]
        if row["participated_market_count"] >= 2
    }

    candidates = []
    for row in rows:
        reasons = []
        if row["participated_market_count"] >= participation_threshold or row["wallet"] in top_wallets:
            reasons.append("high_participation")
        if row["total_holder_usd"] >= total_usd_threshold or row["max_single_market_usd"] >= single_market_usd_threshold:
            reasons.append("large_size")
        if reasons:
            candidates.append({**row, "candidate_reasons": reasons, "source": "holders"})
    candidates.sort(
        key=lambda row: (
            "large_size" in row["candidate_reasons"],
            row["max_single_market_usd"],
            row["total_holder_usd"],
            row["participated_market_count"],
        ),
        reverse=True,
    )
    return candidates[:max_candidate_wallets]


def wilson_lower_bound(successes: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    p = successes / n
    denominator = 1 + z * z / n
    centre = p + z * z / (2 * n)
    adjustment = z * sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - adjustment) / denominator


def summarize_closed_positions(
    positions: list[dict[str, Any]],
    esports_condition_ids: set[str],
    *,
    now_ts: int | None = None,
    bot_like_score: int = 0,
) -> dict[str, Any]:
    rows = []
    neutral_market_count = 0
    for position in positions:
        condition_id = str(position.get("conditionId") or position.get("condition_id") or "").lower()
        if condition_id not in esports_condition_ids:
            continue
        total_bought = to_float(position.get("totalBought") or position.get("total_bought"))
        realized_pnl = to_float(position.get("realizedPnl") or position.get("realized_pnl"))
        if total_bought <= 0:
            continue
        if realized_pnl == 0:
            neutral_market_count += 1
            continue
        avg_price = to_float(position.get("avgPrice") or position.get("avg_price"))
        cost_basis = total_bought * avg_price if avg_price > 0 else total_bought
        rows.append(
            {
                "condition_id": condition_id,
                "total_bought": total_bought,
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "profit_per_share": realized_pnl / total_bought,
                "roi": realized_pnl / cost_basis if cost_basis > 0 else 0.0,
                "avg_price": avg_price,
                "timestamp": to_int(position.get("timestamp")),
            }
        )

    count = len(rows)
    total_bought = sum(row["total_bought"] for row in rows)
    total_cost = sum(row["cost_basis"] for row in rows)
    realized_pnl = sum(row["realized_pnl"] for row in rows)
    positive = sum(1 for row in rows if row["realized_pnl"] > 0)
    losses = count - positive
    last_trade = max((row["timestamp"] for row in rows), default=0)
    rois = [row["roi"] for row in rows]
    profits_per_share = [row["profit_per_share"] for row in rows]
    sizes = [row["total_bought"] for row in rows]
    entry_prices = [row["avg_price"] for row in rows if row["avg_price"] > 0]
    high_price_entries = sum(1 for row in rows if row["avg_price"] >= 0.90)
    low_edge_profits = sum(1 for row in rows if 0 < row["roi"] <= 0.03)
    condition_ids = sorted({row["condition_id"] for row in rows})
    return {
        "esports_closed_count": count,
        "neutral_market_count": neutral_market_count,
        "esports_win_count": positive,
        "esports_loss_count": losses,
        "esports_condition_ids": condition_ids,
        "esports_realized_pnl": round(realized_pnl, 6),
        "esports_total_bought": round(total_bought, 6),
        "esports_total_cost": round(total_cost, 6),
        "avg_profit_per_share": round(realized_pnl / total_bought, 8) if total_bought else 0.0,
        "median_profit_per_share": round(median(profits_per_share), 8) if profits_per_share else 0.0,
        "esports_roi": round(realized_pnl / total_cost, 8) if total_cost else 0.0,
        "median_market_roi": round(median(rois), 8) if rois else 0.0,
        "positive_market_rate": round(positive / count, 8) if count else 0.0,
        "wilson_win_rate_lower_bound": round(wilson_lower_bound(positive, count), 8),
        "avg_position_size": round(total_bought / count, 6) if count else 0.0,
        "median_position_size": round(median(sizes), 6) if sizes else 0.0,
        "avg_entry_price": round(sum(entry_prices) / len(entry_prices), 8) if entry_prices else 0.0,
        "median_entry_price": round(median(entry_prices), 8) if entry_prices else 0.0,
        "high_price_entry_rate": round(high_price_entries / count, 8) if count else 0.0,
        "low_edge_profit_rate": round(low_edge_profits / count, 8) if count else 0.0,
        "last_esports_trade_at": last_trade,
        "bot_like_score": bot_like_score,
        "profiled_at": now_ts or int(datetime.now(timezone.utc).timestamp()),
        "scoring_version": SCORING_VERSION,
    }


def summarize_historical_trade_behavior(
    condition_ids: Iterable[str],
    *,
    historical_trades_loader,
    material_sell_frac: float = 0.2,
) -> dict[str, Any]:
    sold_before_resolution_market_count = 0
    two_sided_trade_market_count = 0
    behavior_market_count = 0
    for condition_id in sorted({str(value).lower() for value in condition_ids if value}):
        trades = historical_trades_loader(condition_id)
        behavior_market_count += 1
        traded_outcomes = set()
        buy_size_by_outcome: dict[int, float] = {}
        sell_size_by_outcome: dict[int, float] = {}
        for trade in trades or []:
            side = str(trade.get("side") or trade.get("type") or "").upper()
            size = to_float(trade.get("size") or trade.get("amount"))
            if size <= 0:
                continue
            outcome_index = to_int(trade.get("outcomeIndex"), -1)
            if outcome_index >= 0 and side in {"BUY", "SELL"}:
                traded_outcomes.add(outcome_index)
            if outcome_index >= 0 and side == "BUY":
                buy_size_by_outcome[outcome_index] = buy_size_by_outcome.get(outcome_index, 0.0) + size
            if outcome_index >= 0 and side == "SELL":
                sell_size_by_outcome[outcome_index] = sell_size_by_outcome.get(outcome_index, 0.0) + size
        has_material_sell = False
        for outcome_index, sell_size in sell_size_by_outcome.items():
            buy_size = buy_size_by_outcome.get(outcome_index, 0.0)
            if buy_size > 0 and sell_size / buy_size > material_sell_frac:
                has_material_sell = True
                break
        if has_material_sell:
            sold_before_resolution_market_count += 1
        if len(traded_outcomes) >= 2:
            two_sided_trade_market_count += 1

    return {
        "historical_trade_behavior_market_count": behavior_market_count,
        "sold_before_resolution_market_count": sold_before_resolution_market_count,
        "sold_before_resolution_market_rate": round(
            sold_before_resolution_market_count / behavior_market_count,
            8,
        )
        if behavior_market_count
        else 0.0,
        "two_sided_trade_market_count": two_sided_trade_market_count,
        "two_sided_trade_market_rate": round(two_sided_trade_market_count / behavior_market_count, 8)
        if behavior_market_count
        else 0.0,
    }


def classify_wallet(summary: dict[str, Any], *, now_ts: int | None = None) -> dict[str, Any]:
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    reasons = []
    count = to_int(summary.get("esports_closed_count"))
    pnl = to_float(summary.get("esports_realized_pnl"))
    median_roi = to_float(summary.get("median_market_roi"))
    positive_rate = to_float(summary.get("positive_market_rate"))
    loss_count = to_int(summary.get("esports_loss_count"))
    total_bought = to_float(summary.get("esports_total_bought"))
    total_cost = to_float(summary.get("esports_total_cost"))
    historical_roi = to_float(summary.get("esports_roi"))
    if historical_roi == 0:
        if total_cost > 0:
            historical_roi = pnl / total_cost
        elif total_bought > 0:
            historical_roi = pnl / total_bought
    median_entry = to_float(summary.get("median_entry_price"))
    wilson_lb = to_float(summary.get("wilson_win_rate_lower_bound"))
    entry_edge = wilson_lb - median_entry if median_entry > 0 else 0.0
    bot_score = to_int(summary.get("bot_like_score"))
    sold_before_resolution = to_int(summary.get("sold_before_resolution_market_count"))
    sold_before_resolution_rate = to_float(summary.get("sold_before_resolution_market_rate"))
    two_sided_trades = to_int(summary.get("two_sided_trade_market_count"))
    two_sided_trade_rate = to_float(summary.get("two_sided_trade_market_rate"))
    trade_behavior_markets = to_int(summary.get("historical_trade_behavior_market_count"))
    last_trade = to_int(summary.get("last_esports_trade_at"))
    stale = not last_trade or (now_ts - last_trade) > 90 * SECONDS_PER_DAY

    if stale:
        reasons.append("stale")
    if (
        sold_before_resolution > 0
        and trade_behavior_markets >= TRADE_BEHAVIOR_MIN_MARKETS
        and sold_before_resolution_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
    ):
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["sold_before_resolution"],
        }
    if (
        two_sided_trades > 0
        and trade_behavior_markets >= TRADE_BEHAVIOR_MIN_MARKETS
        and two_sided_trade_rate > TRADE_BEHAVIOR_EXCLUDE_RATE
    ):
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["two_sided_trading"],
        }
    if bot_score >= 70:
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["bot_like"],
        }
    if pnl <= 0:
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["negative_roi"],
        }
    if historical_roi < 0.30:
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["low_historical_roi"],
        }
    if count < 8:
        reasons.append("thin_sample")
    if total_bought < 1_000:
        reasons.append("low_volume")
    if median_entry <= 0 or median_entry > 0.65:
        reasons.append("weak_entry_price")
    if loss_count > 0:
        reasons.append("has_losses")
    if wilson_lb < 0.55:
        reasons.append("weak_wilson")
    if entry_edge < 0.05:
        reasons.append("weak_entry_edge")
    if median_roi < 0 or positive_rate < 0.5:
        reasons.append("unstable_returns")

    if (
        count >= 8
        and wilson_lb >= 0.65
        and entry_edge >= 0.10
        and pnl > 0
        and total_bought >= 5_000
        and 0 < median_entry <= 0.65
        and not stale
        and bot_score < 40
    ):
        grade = "A"
    elif (
        count >= 8
        and wilson_lb >= 0.55
        and entry_edge >= 0.05
        and pnl > 0
        and total_bought >= 1_000
        and 0 < median_entry <= 0.65
        and not stale
        and bot_score < 50
    ):
        grade = "B"
    elif stale:
        grade = "stale"
    else:
        grade = "C"
    state = "qualified" if grade in {"A", "B"} else "stale" if grade == "stale" else "unqualified"
    return {**summary, "entry_edge": round(entry_edge, 8), "grade": grade, "profile_state": state, "reasons": reasons}


def bot_like_score_from_candidate(candidate: dict[str, Any]) -> int:
    reasons = set(candidate.get("candidate_reasons") or [])
    if "high_participation" not in reasons or "large_size" in reasons:
        return 0
    participated = to_int(candidate.get("participated_market_count"))
    total_cash = to_float(candidate.get("total_cash_volume") or candidate.get("total_holder_usd"))
    max_single = to_float(candidate.get("max_single_market_cash") or candidate.get("max_single_market_usd"))
    avg_cash = total_cash / participated if participated else 0.0
    if participated >= 12 and max_single < 500 and avg_cash < 250:
        return 50
    return 0


def profile_candidate_wallet(
    candidate: dict[str, Any],
    esports_condition_ids: set[str],
    *,
    closed_positions_loader,
    current_positions_loader,
    historical_trades_loader=None,
    now_ts: int | None = None,
) -> dict[str, Any]:
    wallet = normalize_wallet(candidate.get("wallet"))
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    try:
        closed_positions = closed_positions_loader(wallet)
        current_positions = current_positions_loader(wallet)
        bot_score = max(bot_like_score_from_positions(current_positions), bot_like_score_from_candidate(candidate))
        summary = summarize_closed_positions(closed_positions, esports_condition_ids, now_ts=now_ts, bot_like_score=bot_score)
        if historical_trades_loader:
            trade_behavior = summarize_historical_trade_behavior(
                summary.get("esports_condition_ids") or [],
                historical_trades_loader=lambda condition_id: historical_trades_loader(wallet, condition_id),
            )
            summary = {**summary, **trade_behavior}
        return classify_wallet({**summary, "wallet": wallet, "candidate": candidate}, now_ts=now_ts)
    except Exception as exc:
        return {
            "wallet": wallet,
            "candidate": candidate,
            "grade": "unknown",
            "profile_state": "failed_retryable",
            "reasons": ["profile_failed"],
            "error": str(exc),
            "profiled_at": now_ts,
            "scoring_version": SCORING_VERSION,
        }


def detect_same_condition_two_sided(positions: list[dict[str, Any]]) -> set[str]:
    sides: dict[str, set[int]] = {}
    for position in positions:
        size = to_float(position.get("size") or position.get("amount"))
        if size <= 0:
            continue
        condition_id = str(position.get("conditionId") or "").lower()
        if not condition_id:
            continue
        sides.setdefault(condition_id, set()).add(to_int(position.get("outcomeIndex"), -1))
    return {condition_id for condition_id, outcome_indexes in sides.items() if len(outcome_indexes) >= 2}


def bot_like_score_from_positions(positions: list[dict[str, Any]]) -> int:
    return 80 if detect_same_condition_two_sided(positions) else 0


def analyze_holders(
    holders_response: list[dict[str, Any]],
    leaderboard: dict[str, dict[str, Any]],
    *,
    outcomes: list[str],
    outcome_prices: list[float],
) -> dict[str, Any]:
    holders_by_outcome: dict[int, list[dict[str, Any]]] = {index: [] for index in range(len(outcomes))}
    wallet_outcomes: dict[str, set[int]] = {}
    for token_index, token_block in enumerate(holders_response):
        for holder in token_block.get("holders") or []:
            outcome_index = to_int(holder.get("outcomeIndex"), token_index)
            holders_by_outcome.setdefault(outcome_index, []).append(holder)
            wallet = normalize_wallet(holder.get("proxyWallet") or holder.get("wallet"))
            if wallet:
                wallet_outcomes.setdefault(wallet, set()).add(outcome_index)
    two_sided_wallets = {wallet for wallet, wallet_outcome_indexes in wallet_outcomes.items() if len(wallet_outcome_indexes) >= 2}
    qualified_two_sided_wallets = {
        wallet
        for wallet in two_sided_wallets
        if (leaderboard.get(wallet) or {}).get("grade") in {"A", "B"}
    }

    sides = []
    all_known = []
    for index, outcome in enumerate(outcomes):
        holders = []
        qualified_usd = 0.0
        qualified_count = 0
        a_count = 0
        b_count = 0
        unknown_whales = []
        price = outcome_prices[index] if index < len(outcome_prices) else 0.0
        for rank, holder in enumerate(holders_by_outcome.get(index, []), start=1):
            wallet = normalize_wallet(holder.get("proxyWallet") or holder.get("wallet"))
            amount = to_float(holder.get("amount") or holder.get("balance"))
            usd_value = amount * price
            known = leaderboard.get(wallet)
            grade = known.get("grade") if known else "unknown"
            is_two_sided = wallet in two_sided_wallets
            if grade in {"A", "B"} and not is_two_sided:
                qualified_count += 1
                qualified_usd += usd_value
                all_known.append((index, wallet, grade, usd_value))
                if grade == "A":
                    a_count += 1
                else:
                    b_count += 1
            elif not known and usd_value >= 1_000:
                unknown_whales.append(wallet)
            holders.append(
                {
                    "rank": rank,
                    "wallet": wallet,
                    "amount": round(amount, 6),
                    "usd_value": round(usd_value, 6),
                    "grade": grade,
                }
            )
        sides.append(
            {
                "outcome": outcome,
                "price": price,
                "holders": holders,
                "qualified_wallet_count": qualified_count,
                "a_wallet_count": a_count,
                "b_wallet_count": b_count,
                "qualified_holder_usd": round(qualified_usd, 6),
                "unknown_whales": unknown_whales,
            }
        )

    reasons = []
    if qualified_two_sided_wallets:
        reasons.append("two_sided_holder")
    qualified_sides = [side for side in sides if side["qualified_wallet_count"] > 0]
    if not qualified_sides:
        if "no_known_smart_wallets" not in reasons:
            reasons.append("no_known_smart_wallets")
        return {"signal_level": "ignore", "signal_side": None, "reasons": reasons, "sides": sides}

    best_index, best = max(enumerate(sides), key=lambda item: item[1]["qualified_holder_usd"])
    others = [side for i, side in enumerate(sides) if i != best_index]
    other_best = max((side["qualified_holder_usd"] for side in others), default=0.0)
    if other_best > 0 and best["qualified_holder_usd"] <= other_best * 1.5:
        reasons.append("smart_money_disagreement")
        level = "ignore"
    elif best["qualified_wallet_count"] >= 2 and best["qualified_holder_usd"] > other_best * 1.5:
        reasons.append("qualified_usd_concentration")
        level = "candidate"
    else:
        reasons.append("weak_smart_wallet_concentration")
        level = "watch"
    return {"signal_level": level, "signal_side": best["outcome"], "reasons": reasons, "sides": sides}
