from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any

from .core import SECONDS_PER_DAY, bucket_key, bucket_label, normalize_wallet, parse_dt, to_float, to_int

MIN_ADD_RATIO_TO_FIRST = 0.10


def eligible_follow_wallets(
    leaderboard: list[dict[str, Any]],
    *,
    now_ts: int,
    recency_days: int = 30,
    quarantined_wallets: set[str] | None = None,
    favorite_wallets: set[str] | None = None,
    allowed_categories: set[str] | None = None,
) -> list[dict[str, Any]]:
    cutoff = now_ts - recency_days * SECONDS_PER_DAY
    def normalize_scope_keys(values: set[str] | None) -> set[str]:
        keys = set()
        for value in values or set():
            text = str(value or "").lower()
            if not text:
                continue
            if ":" in text:
                category, wallet = text.split(":", 1)
                wallet = normalize_wallet(wallet)
                if category and wallet:
                    keys.add(f"{category}:{wallet}")
            else:
                wallet = normalize_wallet(text)
                if wallet:
                    keys.add(wallet)
        return keys

    quarantined_wallets = normalize_scope_keys(quarantined_wallets)
    favorite_wallets = normalize_scope_keys(favorite_wallets)
    allowed_categories = {str(category).lower() for category in allowed_categories} if allowed_categories is not None else None
    rows = []
    for row in leaderboard:
        category = str(row.get("category") or "esports").lower()
        if allowed_categories is not None and category not in allowed_categories:
            continue
        wallet = normalize_wallet(row.get("wallet"))
        if not wallet:
            continue
        scope_key = f"{category}:{wallet}"
        is_favorite = wallet in favorite_wallets or scope_key in favorite_wallets
        if wallet in quarantined_wallets or scope_key in quarantined_wallets:
            continue
        eligible_market_types = [str(value) for value in (row.get("eligible_market_types") or []) if value]
        eligible_buckets = [str(value) for value in (row.get("eligible_buckets") or []) if value]
        if (row.get("grade") == "A" or is_favorite) and not eligible_market_types:
            eligible_market_types = ["main_match"]
        if row.get("grade") != "A" and not eligible_market_types and not is_favorite:
            continue
        last_trade = to_int(row.get("last_esports_trade_at"))
        if last_trade >= cutoff or is_favorite:
            rows.append({
                **row,
                "wallet": wallet,
                "category": category,
                "eligible_market_types": eligible_market_types,
                "eligible_buckets": eligible_buckets,
            })
    return rows


def market_start_ts(market: dict[str, Any]) -> int:
    start = parse_dt(market.get("match_start_time") or market.get("market_start_time"))
    return int(start.timestamp()) if start else 0


def esports_match_imminent(
    markets: list[dict[str, Any]],
    *,
    now_ts: int,
    horizon_hours: float,
) -> bool:
    horizon = now_ts + int(horizon_hours * 3600)
    has_start_time = False
    for market in markets:
        start_ts = market_start_ts(market)
        if start_ts:
            has_start_time = True
        if start_ts and now_ts <= start_ts <= horizon:
            return True
    return bool(markets) and not has_start_time


def position_condition_id(position: dict[str, Any]) -> str:
    return str(position.get("conditionId") or position.get("condition_id") or "").lower()


def position_outcome_index(position: dict[str, Any]) -> int:
    value = position.get("outcomeIndex")
    if value is None or value == "":
        value = position.get("outcome_index")
    return to_int(value, -1)


def position_size(position: dict[str, Any]) -> float:
    return to_float(position.get("size") or position.get("amount") or position.get("balance"))


def position_avg_price(position: dict[str, Any]) -> float:
    return to_float(position.get("avgPrice") or position.get("avg_price") or position.get("averagePrice"))


def position_initial_value(position: dict[str, Any]) -> float:
    return to_float(position.get("initialValue") or position.get("initial_value") or position.get("cashValue"))


def position_current_price(position: dict[str, Any]) -> float:
    return to_float(position.get("curPrice") or position.get("currentPrice") or position.get("price"))


def position_key(position: dict[str, Any]) -> str:
    return f"{position_condition_id(position)}:{position_outcome_index(position)}"


def current_position_keys(positions: list[dict[str, Any]]) -> list[str]:
    keys = []
    for position in positions:
        if position_size(position) <= 0:
            continue
        condition_id = position_condition_id(position)
        outcome_index = position_outcome_index(position)
        if condition_id and outcome_index >= 0:
            keys.append(f"{condition_id}:{outcome_index}")
    return sorted(set(keys))


def detect_new_positions(
    previous_keys: list[str] | None,
    current_positions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], bool]:
    current_keys = set(current_position_keys(current_positions))
    if previous_keys is None:
        return [], sorted(current_keys), True
    previous = set(previous_keys)
    new_positions = [
        position
        for position in current_positions
        if position_size(position) > 0 and position_key(position) in current_keys - previous
    ]
    return new_positions, sorted(current_keys), False


def _position_cursor_row(
    position: dict[str, Any],
    *,
    seen_at: int,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous = previous or {}
    first_seen_at = to_int(previous.get("first_seen_at")) or int(seen_at)
    size = position_size(position)
    avg_price = position_avg_price(position)
    initial_value = position_initial_value(position)
    first_cash = to_float(previous.get("firstCash"))
    if first_cash <= 0:
        first_cash = initial_value if initial_value > 0 else size * avg_price
    return {
        "size": size,
        "avgPrice": avg_price,
        "initialValue": initial_value,
        "first_seen_at": first_seen_at,
        "last_seen_at": int(seen_at),
        "firstSize": to_float(previous.get("firstSize")) or size,
        "firstInitialValue": to_float(previous.get("firstInitialValue")) or initial_value,
        "firstCash": round(first_cash, 8),
    }


def detect_position_shadow_events(
    previous_cursor_by_key: dict[str, dict[str, Any]] | None,
    current_positions: list[dict[str, Any]],
    *,
    tracked_condition_ids: set[str],
    wallet: str,
    category: str,
    seen_at: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], bool]:
    tracked = {str(condition_id or "").lower() for condition_id in tracked_condition_ids if condition_id}
    wallet = normalize_wallet(wallet)
    category = str(category or "esports").lower()
    cold_start = previous_cursor_by_key is None
    previous_cursor = previous_cursor_by_key or {}
    next_cursor: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    for position in current_positions:
        condition_id = position_condition_id(position)
        outcome_index = position_outcome_index(position)
        size = position_size(position)
        if not condition_id or outcome_index < 0 or size <= 0:
            continue
        if condition_id not in tracked:
            continue
        key = f"{condition_id}:{outcome_index}"
        previous = previous_cursor.get(key) if isinstance(previous_cursor.get(key), dict) else None
        row = _position_cursor_row(position, seen_at=seen_at, previous=previous)
        next_cursor[key] = row
        if cold_start:
            continue
        previous_size = to_float((previous or {}).get("size"))
        delta_size = round(size - previous_size, 10)
        if previous is not None and delta_size <= 1e-9:
            continue
        event_type = "position_new" if previous is None else "position_add"
        avg_price = position_avg_price(position)
        previous_initial_value = to_float((previous or {}).get("initialValue"))
        initial_value = position_initial_value(position)
        delta_initial_value = round(initial_value - previous_initial_value, 10)
        if avg_price > 0 and delta_size > 0:
            estimated_delta_cash = delta_size * avg_price
        else:
            estimated_delta_cash = delta_initial_value if delta_initial_value > 0 else 0.0
        first_cash = to_float(row.get("firstCash"))
        add_ratio_to_first = round(float(estimated_delta_cash) / first_cash, 8) if first_cash > 0 else 0.0
        would_follow = True
        follow_block_reason = None
        if event_type == "position_add" and add_ratio_to_first < MIN_ADD_RATIO_TO_FIRST:
            would_follow = False
            follow_block_reason = "small_add"
        events.append(
            {
                "event_type": event_type,
                "position_seen_at": int(seen_at),
                "condition_id": condition_id,
                "outcome_index": outcome_index,
                "wallet": wallet,
                "category": category,
                "position_key": key,
                "position_size": size,
                "previous_size": previous_size,
                "delta_size": round(delta_size, 10),
                "estimated_delta_cash": round(float(estimated_delta_cash), 8),
                "add_ratio_to_first": add_ratio_to_first,
                "min_add_ratio_to_first": MIN_ADD_RATIO_TO_FIRST,
                "would_follow": would_follow,
                "avg_price": avg_price,
                "cur_price": position_current_price(position),
                "initial_value": initial_value,
                "delta_initial_value": round(delta_initial_value, 8),
            }
        )
        if follow_block_reason:
            events[-1]["follow_block_reason"] = follow_block_reason
    return events, next_cursor, cold_start


def market_current_price(market: dict[str, Any], outcome_index: int, position: dict[str, Any] | None = None) -> float:
    prices = market.get("outcome_prices") or []
    if 0 <= outcome_index < len(prices):
        return to_float(prices[outcome_index])
    position = position or {}
    return to_float(position.get("curPrice") or position.get("currentPrice") or position.get("price"))


def qualify_follow(
    position: dict[str, Any],
    market: dict[str, Any] | None,
    *,
    now_ts: int,
    require_pre_match: bool = True,
) -> dict[str, Any]:
    if not market:
        return {"qualified": False, "reason": "unknown_market"}
    condition_id = position_condition_id(position)
    if condition_id != str(market.get("condition_id") or "").lower():
        return {"qualified": False, "reason": "condition_mismatch"}
    outcome_index = position_outcome_index(position)
    outcomes = market.get("outcomes") or []
    if outcome_index < 0 or outcome_index >= len(outcomes):
        return {"qualified": False, "reason": "unknown_outcome"}
    start_ts = market_start_ts(market)
    if require_pre_match and start_ts and now_ts >= start_ts:
        return {"qualified": False, "reason": "after_match_start"}
    if require_pre_match and not start_ts and market.get("closed"):
        return {"qualified": False, "reason": "closed_market"}
    return {
        "qualified": True,
        "condition_id": condition_id,
        "outcome_index": outcome_index,
        "outcome": outcomes[outcome_index],
        "wallet_avg_price": position_avg_price(position),
        "position_size": position_size(position),
    }


def should_retry_unqualified_position(reason: str | None) -> bool:
    return reason in {"unknown_market"}


def bootstrap_position_trades(
    positions: list[dict[str, Any]],
    *,
    wallet: str,
    markets_by_condition: dict[str, dict[str, Any]],
    now_ts: int,
    max_slippage: float,
    min_wallet_entry_price: float = 0.4,
    max_entry_price: float = 0.85,
    eligible_market_types: set[str] | None = None,
    eligible_buckets: set[str] | None = None,
    eligible_category: str | None = None,
    eligible_leagues: set[str] | None = None,
    require_pre_match: bool = True,
) -> list[dict[str, Any]]:
    wallet = normalize_wallet(wallet)
    trades: list[dict[str, Any]] = []
    for position in positions:
        condition_id = position_condition_id(position)
        market = markets_by_condition.get(condition_id)
        if eligible_category and str((market or {}).get("category") or "esports").lower() != str(eligible_category).lower():
            continue
        market_league = str((market or {}).get("league") or "").lower()
        if eligible_leagues is not None and market_league not in eligible_leagues:
            continue
        market_type = str((market or {}).get("market_type") or "main_match")
        market_bucket = bucket_key(str((market or {}).get("game_family") or market_league or "unknown"), market_type)
        if eligible_buckets is not None and market_bucket not in eligible_buckets:
            continue
        if eligible_buckets is None and eligible_market_types is not None and market_type not in eligible_market_types:
            continue
        qualification = qualify_follow(position, market, now_ts=now_ts, require_pre_match=require_pre_match)
        if not qualification.get("qualified"):
            continue
        outcome_index = int(qualification["outcome_index"])
        current_price = market_current_price(market, outcome_index, position)
        wallet_avg_price = to_float(qualification.get("wallet_avg_price"))
        if min_wallet_entry_price > 0 and wallet_avg_price < min_wallet_entry_price:
            continue
        if max_entry_price > 0 and current_price > max_entry_price:
            continue
        if not evaluate_slippage(wallet_avg_price, current_price, max_slippage=max_slippage).get("would_follow"):
            continue
        trades.append(
            {
                "id": f"bootstrap:{wallet}:{condition_id}:{outcome_index}",
                "proxyWallet": wallet,
                "conditionId": condition_id,
                "outcomeIndex": outcome_index,
                "side": "BUY",
                "price": wallet_avg_price,
                "size": to_float(qualification.get("position_size")),
                "timestamp": now_ts,
                "bootstrap_position": True,
            }
        )
    return trades


def desired_tick_interval(
    watched_markets: list[dict[str, Any]],
    open_signals: list[dict[str, Any]],
    *,
    now_ts: int,
    observe_window_hours: float = 24,
    min_tick_seconds: int = 180,
    max_tick_seconds: int = 900,
    fixed_tick_seconds: int = 0,
) -> int:
    # Few wallets to watch → a single fixed cadence beats the start-time-aware backoff
    # when the goal is to spot every wallet action promptly. 0 keeps the adaptive curve.
    if fixed_tick_seconds and int(fixed_tick_seconds) > 0:
        return int(fixed_tick_seconds)
    if any((signal.get("status") or "open") == "open" for signal in open_signals):
        return int(min_tick_seconds)
    intervals: list[int] = []
    window = max(1.0, float(observe_window_hours)) * 3600
    for market in watched_markets:
        start_ts = market_start_ts(market)
        if not start_ts:
            intervals.append(int(max_tick_seconds))
            continue
        seconds_to_start = start_ts - now_ts
        if seconds_to_start <= 2 * 3600:
            intervals.append(int(min_tick_seconds))
            continue
        if seconds_to_start >= window:
            intervals.append(int(max_tick_seconds))
            continue
        ratio = (seconds_to_start - 2 * 3600) / max(1.0, window - 2 * 3600)
        intervals.append(int(round(min_tick_seconds + ratio * (max_tick_seconds - min_tick_seconds))))
    return min(intervals) if intervals else int(max_tick_seconds)


def trade_condition_id(trade: dict[str, Any]) -> str:
    return str(trade.get("conditionId") or trade.get("condition_id") or trade.get("market") or "").lower()


def trade_outcome_index(trade: dict[str, Any]) -> int:
    return position_outcome_index(trade)


def trade_timestamp(trade: dict[str, Any]) -> int:
    value = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at")
    if isinstance(value, str) and not value.isdigit():
        parsed = parse_dt(value)
        return int(parsed.timestamp()) if parsed else 0
    return to_int(value)


def trade_id(trade: dict[str, Any]) -> str:
    return str(trade.get("id") or trade.get("transactionHash") or trade.get("transaction_hash") or "")


def trade_side(trade: dict[str, Any]) -> str:
    return str(trade.get("side") or trade.get("type") or "").upper()


def position_trade_latency_key(*, category: str, wallet: str, condition_id: str, outcome_index: int) -> str:
    return f"{str(category or 'esports').lower()}:{normalize_wallet(wallet)}:{str(condition_id or '').lower()}:{int(outcome_index)}"


def match_position_trade_latency(
    position_events: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    *,
    wallet: str,
    category: str,
    trade_seen_at: int,
    position_seen_by_key: dict[str, int],
    trade_seen_by_key: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[float], dict[str, int], dict[str, int]]:
    wallet = normalize_wallet(wallet)
    category = str(category or "esports").lower()
    next_position_seen = dict(position_seen_by_key or {})
    next_trade_seen = dict(trade_seen_by_key or {})
    annotated_position_events: list[dict[str, Any]] = []
    match_events: list[dict[str, Any]] = []
    lead_values: list[float] = []
    for event in position_events:
        condition_id = str(event.get("condition_id") or "").lower()
        outcome_index = to_int(event.get("outcome_index"), -1)
        position_seen_at = to_int(event.get("position_seen_at"))
        if not condition_id or outcome_index < 0 or position_seen_at <= 0:
            annotated_position_events.append(dict(event))
            continue
        key = position_trade_latency_key(category=category, wallet=wallet, condition_id=condition_id, outcome_index=outcome_index)
        annotated = dict(event)
        previous_trade_seen_at = to_int(next_trade_seen.get(key))
        if previous_trade_seen_at > 0 and previous_trade_seen_at <= position_seen_at:
            seconds = max(0, position_seen_at - previous_trade_seen_at)
            annotated["trade_before_position_seconds"] = seconds
            match_events.append(
                {
                    "event_type": "position_trade_latency",
                    "wallet": wallet,
                    "category": category,
                    "condition_id": condition_id,
                    "outcome_index": outcome_index,
                    "position_seen_at": position_seen_at,
                    "trade_seen_at": previous_trade_seen_at,
                    "trade_before_position_seconds": seconds,
                    "position_vs_trade_lead_seconds": -seconds,
                }
            )
            lead_values.append(-float(seconds))
        next_position_seen[key] = position_seen_at
        annotated_position_events.append(annotated)
    for trade in trades:
        if trade_side(trade) and trade_side(trade) != "BUY":
            continue
        condition_id = trade_condition_id(trade)
        outcome_index = trade_outcome_index(trade)
        if not condition_id or outcome_index < 0:
            continue
        key = position_trade_latency_key(category=category, wallet=wallet, condition_id=condition_id, outcome_index=outcome_index)
        next_trade_seen[key] = int(trade_seen_at)
        position_seen_at = to_int(next_position_seen.get(key))
        if position_seen_at > 0 and position_seen_at <= int(trade_seen_at):
            seconds = max(0, int(trade_seen_at) - position_seen_at)
            match_events.append(
                {
                    "event_type": "position_trade_latency",
                    "wallet": wallet,
                    "category": category,
                    "condition_id": condition_id,
                    "outcome_index": outcome_index,
                    "position_seen_at": position_seen_at,
                    "trade_seen_at": int(trade_seen_at),
                    "position_before_trade_seconds": seconds,
                    "position_vs_trade_lead_seconds": seconds,
                }
            )
            lead_values.append(float(seconds))
    return annotated_position_events, match_events, lead_values, next_position_seen, next_trade_seen


def trade_price(trade: dict[str, Any]) -> float:
    return to_float(trade.get("price") or trade.get("avgPrice") or trade.get("avg_price"))


def trade_size(trade: dict[str, Any]) -> float:
    return to_float(trade.get("size") or trade.get("amount"))


def _cursor_tuple(cursor: dict[str, Any] | None) -> tuple[int, str]:
    cursor = cursor or {}
    return to_int(cursor.get("timestamp")), str(cursor.get("id") or "")


def _trade_tuple(trade: dict[str, Any]) -> tuple[int, str]:
    return trade_timestamp(trade), trade_id(trade)


def select_new_trades(
    trades: list[dict[str, Any]],
    previous_cursor: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, bool]:
    ordered_desc = sorted(trades, key=lambda row: _trade_tuple(row), reverse=True)
    latest = ordered_desc[0] if ordered_desc else None
    latest_cursor = {"timestamp": trade_timestamp(latest), "id": trade_id(latest)} if latest else previous_cursor
    if previous_cursor is None:
        return [], latest_cursor, True
    previous_key = _cursor_tuple(previous_cursor)
    new_trades = [trade for trade in trades if _trade_tuple(trade) > previous_key]
    new_trades.sort(key=lambda row: _trade_tuple(row))
    return new_trades, latest_cursor, False


def follow_signal_id(wallet: str, condition_id: str, outcome_index: int) -> str:
    return f"{normalize_wallet(wallet)}:{condition_id.lower()}:{outcome_index}"


def paper_exit_pnl(entry_price: float, exit_price: float, stake: float) -> float:
    if entry_price <= 0:
        return 0.0
    return round(stake * (exit_price - entry_price) / entry_price, 8)


def _behavior_event(kind: str, trade: dict[str, Any], *, note: str | None = None) -> dict[str, Any]:
    event = {
        "kind": kind,
        "trade_id": trade_id(trade),
        "condition_id": trade_condition_id(trade),
        "outcome_index": trade_outcome_index(trade),
        "price": round(trade_price(trade), 8),
        "size": round(trade_size(trade), 8),
        "timestamp": trade_timestamp(trade),
    }
    if note:
        event["note"] = note
    return event


def wallet_behavior_summary(signal: dict[str, Any]) -> dict[str, Any]:
    events = signal.get("behavior_events") or []
    kinds = {event.get("kind") for event in events}
    return {
        "single_sided": "hedge" not in kinds,
        "hedged": "hedge" in kinds,
        "sold_before_resolution": bool(kinds & {"sell", "exit"}),
        "wallet_sold_before_resolution": "sell" in kinds,
        "local_exited_before_resolution": "exit" in kinds,
        "held_to_resolution": "held_to_resolution" in kinds,
        "add_count": sum(1 for event in events if event.get("kind") == "add"),
        "exit_count": sum(1 for event in events if event.get("kind") == "exit"),
        "sell_count": sum(1 for event in events if event.get("kind") == "sell"),
    }


def _open_signal_by_id(open_signals: list[dict[str, Any]], sid: str) -> dict[str, Any] | None:
    for signal in open_signals:
        if signal.get("signal_id") == sid and (signal.get("status") or "open") == "open":
            return signal
    return None


def _signal_by_id(open_signals: list[dict[str, Any]], sid: str) -> dict[str, Any] | None:
    for signal in open_signals:
        if signal.get("signal_id") == sid:
            return signal
    return None


def _opposite_open_signals(
    open_signals: list[dict[str, Any]],
    *,
    wallet: str,
    condition_id: str,
    outcome_index: int,
) -> list[dict[str, Any]]:
    wallet = normalize_wallet(wallet)
    return [
        signal
        for signal in open_signals
        if (signal.get("status") or "open") == "open"
        and normalize_wallet(signal.get("wallet")) == wallet
        and str(signal.get("condition_id") or "").lower() == condition_id.lower()
        and to_int(signal.get("outcome_index"), -1) != outcome_index
    ]


def _open_market_signals(open_signals: list[dict[str, Any]], condition_id: str) -> list[dict[str, Any]]:
    condition_id = condition_id.lower()
    return [
        signal
        for signal in open_signals
        if (signal.get("status") or "open") == "open"
        and str(signal.get("condition_id") or "").lower() == condition_id
    ]


def _exit_signal_for_opposite_buy(
    signal: dict[str, Any],
    *,
    market: dict[str, Any],
    trade: dict[str, Any],
    now_ts: int,
) -> None:
    outcome_index = to_int(signal.get("outcome_index"), -1)
    exit_price = market_current_price(market, outcome_index, trade if outcome_index == trade_outcome_index(trade) else None)
    if exit_price <= 0 and outcome_index != trade_outcome_index(trade) and len(market.get("outcomes") or []) == 2:
        exit_price = round(max(0.0, min(1.0, 1.0 - trade_price(trade))), 8)
    signal["status"] = "exited"
    signal["exit_price"] = exit_price
    signal["exit_at"] = now_ts
    signal["exit_reason"] = "opposite_wallet_buy"
    signal["contested"] = True
    signal["would_follow"] = False
    signal["updated_at"] = now_ts
    signal.setdefault("behavior_events", []).append(
        _behavior_event(
            "exit",
            trade,
            note=f"opposite_wallet_buy:{normalize_wallet(trade.get('proxyWallet') or trade.get('wallet') or trade.get('user'))}",
        )
    )
    signal["our_realized_pnl"] = round(
        sum(paper_exit_pnl(to_float(leg.get("our_entry_price")), exit_price, leg_actual_stake(leg)) for leg in signal.get("legs") or []),
        8,
    )
    signal["hypothetical_pnl"] = round(
        sum(paper_exit_pnl(to_float(leg.get("our_entry_price")), exit_price, leg_hypothetical_stake(leg)) for leg in signal.get("legs") or []),
        8,
    )
    signal["wallet_behavior"] = wallet_behavior_summary(signal)


def follow_stake_for_signal(
    *,
    wallet_trade_cash: float,
    stake_ratio_percent: float,
    min_stake_usdc: float,
    available_balance: float,
    max_stake_usdc: float = 0.0,
) -> tuple[float, str, float]:
    ratio = max(0.0, to_float(stake_ratio_percent)) / 100.0
    min_stake = max(0.0, to_float(min_stake_usdc))
    desired = max(min_stake, to_float(wallet_trade_cash) * ratio)
    max_stake = to_float(max_stake_usdc)
    limited = max_stake > 0 and desired > max_stake
    if limited:
        desired = max_stake
    if available_balance >= desired:
        mode = "limited" if limited else "minimum" if desired == min_stake else "proportional"
        return round(desired, 8), mode, round(ratio, 6)
    if available_balance >= min_stake:
        return round(available_balance, 8), "capped", round(ratio, 6)
    return 0.0, "skipped", round(ratio, 6)


def target_stake_for_signal(
    *,
    wallet_trade_cash: float,
    stake_ratio_percent: float,
    min_stake_usdc: float,
    max_stake_usdc: float = 0.0,
) -> tuple[float, str, float]:
    ratio = max(0.0, to_float(stake_ratio_percent)) / 100.0
    min_stake = max(0.0, to_float(min_stake_usdc))
    desired = max(min_stake, to_float(wallet_trade_cash) * ratio)
    max_stake = to_float(max_stake_usdc)
    if max_stake > 0 and desired > max_stake:
        return round(max_stake, 8), "limited", round(ratio, 6)
    mode = "minimum" if desired == min_stake else "proportional"
    return round(desired, 8), mode, round(ratio, 6)


def _open_signals_exposure(open_signals: list[dict[str, Any]]) -> float:
    return sum(
        leg_actual_stake(leg)
        for signal in open_signals
        if (signal.get("status") or "open") == "open"
        for leg in signal.get("legs") or []
    )


def leg_actual_stake(leg: dict[str, Any]) -> float:
    if not isinstance(leg, dict):
        return 0.0
    if leg.get("funded_stake") is not None:
        return max(0.0, to_float(leg.get("funded_stake")))
    if leg.get("would_follow") is False:
        return 0.0
    return max(0.0, to_float(leg.get("stake")))


def first_leg_wallet_trade_cash(signal: dict[str, Any] | None) -> float:
    for leg in (signal or {}).get("legs") or []:
        cash = to_float((leg or {}).get("wallet_trade_cash"))
        if cash > 0:
            return cash
    return 0.0


def leg_hypothetical_stake(leg: dict[str, Any]) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return max(0.0, to_float(leg.get("stake")))


def process_follow_trades(
    open_signals: list[dict[str, Any]],
    *,
    wallet: str,
    trades: list[dict[str, Any]],
    markets_by_condition: dict[str, dict[str, Any]],
    now_ts: int,
    observed_at: int | None = None,
    previous_poll_at: int | None = None,
    stake_usdc: float,
    max_follow_legs: int,
    max_slippage: float,
    min_wallet_entry_price: float = 0.4,
    max_entry_price: float = 0.85,
    stake_ratio_percent: float = 10.0,
    require_pre_match: bool = True,
    post_start_grace_seconds: int = 0,
    quarantine_sell_frac: float = 0.2,
    eligible_market_types: set[str] | None = None,
    eligible_buckets: set[str] | None = None,
    eligible_category: str | None = None,
    eligible_leagues: set[str] | None = None,
    conflict_policy: str = "dual_follow",
    bankroll_usdc: float = float("inf"),
    max_stake_usdc: float = 0.0,
    max_signal_stake_usdc: float = 0.0,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    wallet = normalize_wallet(wallet)
    observed_ts = to_int(observed_at) or now_ts
    previous_poll_ts = to_int(previous_poll_at)
    stats: dict[str, Any] = {
        "new_leg_count": 0,
        "exited_signal_count": 0,
        "hedge_event_count": 0,
        "ignored_trade_count": 0,
        "market_type_not_eligible_count": 0,
        "league_not_eligible_count": 0,
        "opposite_blocked_count": 0,
        "low_entry_price_blocked_count": 0,
        "high_entry_price_blocked_count": 0,
        "insufficient_balance_count": 0,
        "small_add_blocked_count": 0,
        "signal_cap_limited_count": 0,
        "signal_cap_blocked_count": 0,
        "funded_stake_usdc": 0.0,
        "unfunded_intent_count": 0,
        "quarantine_events": [],
    }
    for trade in sorted(trades, key=lambda row: _trade_tuple(row)):
        condition_id = trade_condition_id(trade)
        outcome_index = trade_outcome_index(trade)
        market = markets_by_condition.get(condition_id)
        if not market or outcome_index < 0:
            stats["ignored_trade_count"] += 1
            continue
        market_category = str(market.get("category") or "esports").lower()
        if eligible_category and market_category != str(eligible_category).lower():
            stats["ignored_trade_count"] += 1
            continue
        side = trade_side(trade)
        current_price = market_current_price(market, outcome_index, trade)
        sid = follow_signal_id(wallet, condition_id, outcome_index)
        existing = _open_signal_by_id(open_signals, sid)

        if side == "SELL":
            signal_for_sell = existing or _signal_by_id(open_signals, sid)
            if not signal_for_sell:
                stats["ignored_trade_count"] += 1
                continue
            signal_for_sell["wallet_sell_size"] = round(to_float(signal_for_sell.get("wallet_sell_size")) + trade_size(trade), 8)
            signal_for_sell.setdefault("behavior_events", []).append(_behavior_event("sell", trade))
            signal_for_sell["wallet_behavior"] = wallet_behavior_summary(signal_for_sell)
            if not existing:
                continue
            existing["status"] = "exited"
            existing["exit_price"] = current_price
            existing["exit_at"] = now_ts
            existing["updated_at"] = now_ts
            existing.setdefault("behavior_events", []).append(_behavior_event("exit", trade))
            existing["our_realized_pnl"] = round(
                sum(paper_exit_pnl(to_float(leg.get("our_entry_price")), current_price, leg_actual_stake(leg)) for leg in existing.get("legs") or []),
                8,
            )
            existing["hypothetical_pnl"] = round(
                sum(paper_exit_pnl(to_float(leg.get("our_entry_price")), current_price, leg_hypothetical_stake(leg)) for leg in existing.get("legs") or []),
                8,
            )
            existing["wallet_behavior"] = wallet_behavior_summary(existing)
            stats["exited_signal_count"] += 1
            continue

        if side != "BUY":
            stats["ignored_trade_count"] += 1
            continue

        opposite = _opposite_open_signals(
            open_signals,
            wallet=wallet,
            condition_id=condition_id,
            outcome_index=outcome_index,
        )
        for signal in opposite:
            signal.setdefault("behavior_events", []).append(_behavior_event("hedge", trade))
            signal["wallet_behavior"] = wallet_behavior_summary(signal)
            stats["hedge_event_count"] += 1

        market_open_signals = _open_market_signals(open_signals, condition_id)
        opposite_market_signals = [
            signal for signal in market_open_signals if to_int(signal.get("outcome_index"), -1) != outcome_index
        ]
        if opposite_market_signals:
            stats["opposite_blocked_count"] += 1
            if conflict_policy == "exit_on_opposite":
                for signal in market_open_signals:
                    _exit_signal_for_opposite_buy(signal, market=market, trade=trade, now_ts=now_ts)
                stats["exited_signal_count"] += len(market_open_signals)
                stats["ignored_trade_count"] += 1
                continue

        start_ts = market_start_ts(market)
        trade_ts = trade_timestamp(trade)
        detected_after_start = False
        if require_pre_match and start_ts and now_ts >= start_ts:
            grace_seconds = max(0, int(post_start_grace_seconds))
            detected_after_start = bool(trade_ts and trade_ts < start_ts and now_ts <= start_ts + grace_seconds)
            if not detected_after_start:
                stats["ignored_trade_count"] += 1
                continue
        elif not require_pre_match and start_ts and now_ts >= start_ts:
            detected_after_start = bool(not trade_ts or trade_ts >= start_ts)
        if existing and len(existing.get("legs") or []) >= max_follow_legs:
            stats["ignored_trade_count"] += 1
            continue
        market_type = str(market.get("market_type") or "main_match")
        market_league = str(market.get("league") or "").lower()
        market_bucket = bucket_key(str(market.get("game_family") or market_league or "unknown"), market_type)
        if not existing and eligible_buckets is not None and market_bucket not in eligible_buckets:
            stats["ignored_trade_count"] += 1
            stats["market_type_not_eligible_count"] += 1
            continue
        if not existing and eligible_buckets is None and eligible_market_types is not None and market_type not in eligible_market_types:
            stats["ignored_trade_count"] += 1
            stats["market_type_not_eligible_count"] += 1
            continue
        if not existing and eligible_leagues is not None and market_league not in eligible_leagues:
            stats["ignored_trade_count"] += 1
            stats["league_not_eligible_count"] += 1
            continue

        wallet_fill_price = trade_price(trade)
        slippage = evaluate_slippage(wallet_fill_price, current_price, max_slippage=max_slippage)
        follow_block_reasons = []
        if min_wallet_entry_price > 0 and wallet_fill_price < min_wallet_entry_price:
            slippage["would_follow"] = False
            follow_block_reasons.append("low_entry_price")
            stats["low_entry_price_blocked_count"] += 1
        if max_entry_price > 0 and current_price > max_entry_price:
            slippage["would_follow"] = False
            follow_block_reasons.append("high_entry_price")
            stats["high_entry_price_blocked_count"] += 1
        if not slippage["would_follow"] and slippage["slippage_over_wallet_entry"] > max_slippage:
            follow_block_reasons.append("slippage_over_entry")
        stake_mode = None
        stake_ratio = None
        wallet_cash = round(trade_size(trade) * wallet_fill_price, 8)
        available_balance = bankroll_usdc - _open_signals_exposure(open_signals)
        target_stake, target_stake_mode, _ = target_stake_for_signal(
            wallet_trade_cash=wallet_cash,
            stake_ratio_percent=stake_ratio_percent,
            min_stake_usdc=stake_usdc,
            max_stake_usdc=max_stake_usdc,
        )
        leg_stake, stake_mode, stake_ratio = follow_stake_for_signal(
            wallet_trade_cash=wallet_cash,
            stake_ratio_percent=stake_ratio_percent,
            min_stake_usdc=stake_usdc,
            available_balance=available_balance,
            max_stake_usdc=max_stake_usdc,
        )
        funded_stake = leg_stake
        funding_status = "funded"
        would_follow = slippage["would_follow"]
        if leg_stake <= 0:
            stats["insufficient_balance_count"] += 1
            stats["unfunded_intent_count"] += 1
            leg_stake = target_stake
            funded_stake = 0.0
            stake_mode = "skipped"
            funding_status = "insufficient_balance"
            would_follow = False
            follow_block_reasons.append("insufficient_balance")
        if stake_mode == "capped":
            stats["insufficient_balance_count"] += 1
        if funding_status == "funded" and not would_follow:
            funded_stake = 0.0
            funding_status = "blocked"
        add_ratio_to_first = None
        if existing:
            first_wallet_cash = first_leg_wallet_trade_cash(existing)
            add_ratio_to_first = round(wallet_cash / first_wallet_cash, 8) if first_wallet_cash > 0 else None
            if add_ratio_to_first is not None and add_ratio_to_first < MIN_ADD_RATIO_TO_FIRST:
                would_follow = False
                funded_stake = 0.0
                funding_status = "blocked"
                stake_mode = "small_add"
                follow_block_reasons.append("small_add")
                stats["small_add_blocked_count"] += 1
        signal_stake_before = sum(leg_actual_stake(leg) for leg in (existing or {}).get("legs") or [])
        max_signal_stake = to_float(max_signal_stake_usdc)
        if max_signal_stake > 0 and funding_status == "funded" and would_follow and funded_stake > 0:
            remaining_signal_stake = max(0.0, max_signal_stake - signal_stake_before)
            if remaining_signal_stake + 1e-9 < stake_usdc:
                would_follow = False
                funded_stake = 0.0
                funding_status = "blocked"
                stake_mode = "signal_cap"
                follow_block_reasons.append("signal_cap_reached")
                stats["signal_cap_blocked_count"] += 1
            elif funded_stake > remaining_signal_stake:
                funded_stake = round(remaining_signal_stake, 8)
                leg_stake = funded_stake
                funding_status = "signal_cap"
                stake_mode = "signal_cap"
                stats["signal_cap_limited_count"] += 1
        stats["funded_stake_usdc"] = round(to_float(stats.get("funded_stake_usdc")) + funded_stake, 8)
        leg = {
            "category": market_category,
            "our_entry_price": current_price,
            "wallet_fill_price": wallet_fill_price,
            "slippage_over_wallet_entry": slippage["slippage_over_wallet_entry"],
            "would_follow": would_follow,
            "would_follow_if_funded": slippage["would_follow"],
            "min_wallet_entry_price": round(min_wallet_entry_price, 8),
            "max_entry_price": round(to_float(max_entry_price), 8),
            "stake": leg_stake,
            "target_stake": target_stake,
            "target_stake_mode": target_stake_mode,
            "funded_stake": funded_stake,
            "funding_status": funding_status,
            "trade_id": trade_id(trade),
            "leg_at": now_ts,
            "wallet_trade_at": trade_ts,
            "wallet_trade_size": round(trade_size(trade), 8),
            "wallet_trade_cash": wallet_cash,
            "stake_mode": stake_mode,
            "stake_ratio_percent": round(stake_ratio_percent, 8),
            "max_stake_usdc": round(to_float(max_stake_usdc), 8),
            "max_signal_stake_usdc": round(max_signal_stake, 8),
            "signal_stake_before": round(signal_stake_before, 8),
            "observed_at": observed_ts,
        }
        if funding_status == "signal_cap":
            leg["signal_cap_limited"] = True
        if add_ratio_to_first is not None:
            leg["add_ratio_to_first"] = add_ratio_to_first
            leg["min_add_ratio_to_first"] = MIN_ADD_RATIO_TO_FIRST
        if trade_ts:
            leg["observed_delay_seconds"] = max(0, observed_ts - trade_ts)
        if previous_poll_ts > 0:
            leg["previous_poll_at"] = previous_poll_ts
            if trade_ts and trade_ts <= previous_poll_ts:
                leg["index_lag_lower_bound_seconds"] = max(0, previous_poll_ts - trade_ts)
        if follow_block_reasons:
            leg["follow_block_reason"] = follow_block_reasons[0]
            leg["follow_block_reasons"] = follow_block_reasons
        if detected_after_start:
            leg["detected_after_start"] = True
        if existing:
            existing.setdefault("legs", []).append(leg)
            existing.setdefault("behavior_events", []).append(_behavior_event("add", trade))
            existing["updated_at"] = now_ts
            existing["current_price"] = current_price
            if detected_after_start:
                existing["detected_after_start"] = True
            existing["wallet_behavior"] = wallet_behavior_summary(existing)
        else:
            outcomes = market.get("outcomes") or []
            signal = {
                "signal_id": sid,
                "wallet": wallet,
                "condition_id": condition_id,
                "outcome_index": outcome_index,
                "outcome": outcomes[outcome_index] if 0 <= outcome_index < len(outcomes) else None,
                "event_title": market.get("title"),
                "category": market_category,
                "league": market.get("league"),
                "event_slug": market.get("event_slug"),
                "market_question": market.get("question"),
                "market_type": market_type,
                "market_type_label": market.get("market_type_label"),
                "game_family": market.get("game_family"),
                "bucket_key": market_bucket,
                "bucket_label": bucket_label(market_bucket),
                "end_date": market.get("end_date"),
                "match_start_time": market.get("match_start_time") or market.get("market_start_time"),
                "status": "open",
                "created_at": now_ts,
                "updated_at": now_ts,
                "current_price": current_price,
                "signal_stake": leg_stake,
                "stake_mode": stake_mode,
                "stake_ratio": stake_ratio,
                "stake_ratio_percent": round(stake_ratio_percent, 8),
                "legs": [leg],
                "behavior_events": [_behavior_event("add", trade)],
                "wallet_behavior": {
                    "single_sided": True,
                    "hedged": False,
                    "sold_before_resolution": False,
                    "held_to_resolution": False,
                    "add_count": 1,
                    "exit_count": 0,
                },
            }
            if detected_after_start:
                signal["detected_after_start"] = True
            open_signals.append(signal)
        stats["new_leg_count"] += 1
    return open_signals, stats


def apply_closing_line_snapshots(
    open_signals: list[dict[str, Any]],
    markets_by_condition: dict[str, dict[str, Any]],
    *,
    now_ts: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    updated = 0
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        if signal.get("closing_line_prices") is not None:
            continue
        condition_id = str(signal.get("condition_id") or "").lower()
        market = markets_by_condition.get(condition_id)
        if not market:
            continue
        start_ts = market_start_ts(market)
        if not start_ts or now_ts < start_ts:
            continue
        prices = [to_float(value) for value in (market.get("outcome_prices") or [])]
        if not prices:
            continue
        clv = compute_clv(signal, prices)
        signal["closing_line_prices"] = prices
        signal["closing_line_at"] = now_ts
        signal["wallet_clv"] = clv["wallet_clv"]
        signal["our_clv"] = clv["our_clv"]
        signal["updated_at"] = now_ts
        signal.setdefault("behavior_events", []).append({"kind": "closing_line", "timestamp": now_ts, **clv})
        updated += 1
    return open_signals, {"closing_line_snapshot_count": updated}


def summarize_wallet_fills(
    trades: list[dict[str, Any]],
    *,
    wallet: str | None = None,
    outcome_index: int | None = None,
) -> dict[str, Any]:
    wallet = normalize_wallet(wallet)
    fills = []
    for trade in trades:
        trade_wallet = normalize_wallet(trade.get("proxyWallet") or trade.get("wallet") or trade.get("user"))
        if wallet and trade_wallet and trade_wallet != wallet:
            continue
        trade_outcome_index = position_outcome_index(trade)
        if outcome_index is not None and trade_outcome_index >= 0 and trade_outcome_index != outcome_index:
            continue
        price = to_float(trade.get("price") or trade.get("avgPrice"))
        size = to_float(trade.get("size") or trade.get("amount"))
        timestamp = to_int(trade.get("timestamp") or trade.get("createdAt"))
        if price <= 0 or size <= 0:
            continue
        fills.append(
            {
                "price": round(price, 8),
                "size": round(size, 8),
                "timestamp": timestamp,
            }
        )
    fills.sort(key=lambda row: row["timestamp"])
    total_size = sum(row["size"] for row in fills)
    weighted_cost = sum(row["price"] * row["size"] for row in fills)
    prices = [row["price"] for row in fills]
    return {
        "fills": fills,
        "fill_count": len(fills),
        "total_size": round(total_size, 8),
        "avg_price": round(weighted_cost / total_size, 8) if total_size else 0.0,
        "min_price": min(prices) if prices else 0.0,
        "max_price": max(prices) if prices else 0.0,
        "median_price": round(median(prices), 8) if prices else 0.0,
        "first_fill_at": fills[0]["timestamp"] if fills else 0,
        "last_fill_at": fills[-1]["timestamp"] if fills else 0,
    }


def evaluate_slippage(wallet_avg: float, current_price: float, *, max_slippage: float) -> dict[str, Any]:
    slippage = current_price - wallet_avg
    return {
        "slippage_over_wallet_entry": round(slippage, 8),
        "would_follow": slippage <= max_slippage,
    }


def contested_markets(open_signals: list[dict[str, Any]], *, now_ts: int | None = None) -> set[str]:
    outcomes_by_condition: dict[str, set[int]] = {}
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        condition_id = str(signal.get("condition_id") or "").lower()
        outcome_index = to_int(signal.get("outcome_index"), -1)
        if not condition_id or outcome_index < 0:
            continue
        outcomes_by_condition.setdefault(condition_id, set()).add(outcome_index)
    return {condition_id for condition_id, outcomes in outcomes_by_condition.items() if len(outcomes) >= 2}


def apply_contested_flags(
    open_signals: list[dict[str, Any]],
    contested_condition_ids: set[str],
    *,
    now_ts: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    count = 0
    contested_condition_ids = {str(value).lower() for value in contested_condition_ids}
    for signal in open_signals:
        condition_id = str(signal.get("condition_id") or "").lower()
        is_contested = condition_id in contested_condition_ids and (signal.get("status") or "open") == "open"
        if not is_contested:
            signal.setdefault("contested", False)
            continue
        if not signal.get("contested"):
            signal.setdefault("behavior_events", []).append({"kind": "contested", "timestamp": now_ts})
        signal["contested"] = True
        signal["updated_at"] = now_ts
        count += 1
    return open_signals, {"contested_signal_count": count}


def compute_clv(signal: dict[str, Any], closing_line_prices: list[float]) -> dict[str, Any]:
    outcome_index = to_int(signal.get("outcome_index"), -1)
    if outcome_index < 0 or outcome_index >= len(closing_line_prices):
        return {"wallet_clv": 0.0, "our_clv": 0.0}
    closing = to_float(closing_line_prices[outcome_index])
    legs = signal.get("legs") or []
    total_stake = sum(to_float(leg.get("stake")) for leg in legs)
    if not legs or total_stake <= 0:
        wallet_entry = to_float(signal.get("wallet_avg_price"))
        our_entry = to_float(signal.get("our_entry_price"))
    else:
        wallet_entry = sum(to_float(leg.get("wallet_fill_price")) * to_float(leg.get("stake")) for leg in legs) / total_stake
        our_entry = sum(to_float(leg.get("our_entry_price")) * to_float(leg.get("stake")) for leg in legs) / total_stake
    return {
        "wallet_clv": round(closing - wallet_entry, 8),
        "our_clv": round(closing - our_entry, 8),
        "closing_line_price": round(closing, 8),
        "wallet_entry_price": round(wallet_entry, 8),
        "our_entry_price": round(our_entry, 8),
    }


def material_sell(signal: dict[str, Any], trade: dict[str, Any], *, sell_frac: float = 0.2) -> bool:
    if trade_side(trade) != "SELL":
        return False
    sell_size = trade_size(trade) + to_float(signal.get("wallet_sell_size"))
    if sell_size <= 0:
        return False
    bought_size = sum(to_float(leg.get("wallet_trade_size")) for leg in signal.get("legs") or [])
    if bought_size <= 0:
        return False
    return sell_size / bought_size > sell_frac


def quarantine_reason(signal: dict[str, Any], trade: dict[str, Any], *, sell_frac: float = 0.2) -> str | None:
    return None


def paper_pnl(entry_price: float, outcome_won: bool, stake: float) -> float:
    if entry_price <= 0:
        return 0.0
    if outcome_won:
        return round(stake * (1 - entry_price) / entry_price, 8)
    return round(-stake, 8)


def winner_outcome_index(market: dict[str, Any]) -> int | None:
    prices = market.get("outcome_prices") or []
    if not prices:
        return None
    best_index = max(range(len(prices)), key=lambda index: to_float(prices[index]))
    return best_index if to_float(prices[best_index]) >= 0.99 else None


def settle_open_signals(
    open_signals: list[dict[str, Any]],
    resolutions: dict[str, int],
    *,
    now_ts: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    remaining = []
    settled = []
    for signal in open_signals:
        winner = resolutions.get(str(signal.get("condition_id") or "").lower())
        if winner is None:
            remaining.append(signal)
            continue
        outcome_won = winner == to_int(signal.get("outcome_index"), -1)
        if signal.get("legs") is not None:
            legs = signal.get("legs") or []
            wallet_pnl = round(
                sum(paper_pnl(to_float(leg.get("wallet_fill_price")), outcome_won, leg_actual_stake(leg)) for leg in legs),
                8,
            )
            our_pnl = round(
                sum(paper_pnl(to_float(leg.get("our_entry_price")), outcome_won, leg_actual_stake(leg)) for leg in legs),
                8,
            )
            hypothetical_wallet_pnl = round(
                sum(paper_pnl(to_float(leg.get("wallet_fill_price")), outcome_won, leg_hypothetical_stake(leg)) for leg in legs),
                8,
            )
            hypothetical_pnl = round(
                sum(paper_pnl(to_float(leg.get("our_entry_price")), outcome_won, leg_hypothetical_stake(leg)) for leg in legs),
                8,
            )
            compact = {
                **signal,
                "status": "settled",
                "settled_at": now_ts,
                "outcome_won": outcome_won,
                "wallet_paper_pnl_by_wallet": {normalize_wallet(signal.get("wallet")): wallet_pnl},
                "our_paper_pnl": our_pnl,
                "wallet_hypothetical_pnl_by_wallet": {normalize_wallet(signal.get("wallet")): hypothetical_wallet_pnl},
                "hypothetical_pnl": hypothetical_pnl,
            }
            compact.setdefault("behavior_events", []).append({"kind": "held_to_resolution", "timestamp": now_ts})
            compact["wallet_behavior"] = wallet_behavior_summary(compact)
            settled.append(compact)
            continue
    return remaining, settled


def aggregate_follow_performance(prev_perf: dict[str, Any], newly_settled: list[dict[str, Any]]) -> dict[str, Any]:
    wallets = dict(prev_perf.get("wallets") or {})
    total = dict(prev_perf.get("total") or {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0, "legs": 0})
    groups = dict(prev_perf.get("groups") or {})
    by_category = {
        str(category): dict(row)
        for category, row in (prev_perf.get("by_category") or {}).items()
        if isinstance(row, dict)
    }

    def update_category(category: str, result: dict[str, Any], *, won: bool = False, wallet_pnl: float = 0.0, our_pnl: float = 0.0, exited: bool = False) -> None:
        category = str(category or "esports").lower()
        row = by_category.setdefault(category, {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0, "legs": 0})
        legs = result.get("legs") or []
        row["legs"] = to_int(row.get("legs")) + len(legs)
        row["our_pnl"] = round(to_float(row.get("our_pnl")) + our_pnl, 8)
        row["wallet_pnl"] = round(to_float(row.get("wallet_pnl")) + wallet_pnl, 8)
        if exited:
            row["exits"] = to_int(row.get("exits")) + 1
        else:
            row["signals"] = to_int(row.get("signals")) + 1
            row["wins"] = to_int(row.get("wins")) + (1 if won else 0)
            row["win_rate"] = round(row["wins"] / row["signals"], 8) if row["signals"] else 0.0

    def update_group(name: str, result: dict[str, Any], *, won: bool = False, wallet_pnl: float = 0.0, our_pnl: float = 0.0, exited: bool = False) -> None:
        group = groups.setdefault(
            name,
            {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0, "legs": 0, "clv_sum": 0.0, "clv_count": 0},
        )
        legs = result.get("legs") or []
        group["legs"] = to_int(group.get("legs")) + len(legs)
        group["our_pnl"] = round(to_float(group.get("our_pnl")) + our_pnl, 8)
        group["wallet_pnl"] = round(to_float(group.get("wallet_pnl")) + wallet_pnl, 8)
        if exited:
            group["exits"] = to_int(group.get("exits")) + 1
        else:
            group["signals"] = to_int(group.get("signals")) + 1
            group["wins"] = to_int(group.get("wins")) + (1 if won else 0)
            group["win_rate"] = round(group["wins"] / group["signals"], 8) if group["signals"] else 0.0
        if result.get("wallet_clv") is not None:
            group["clv_sum"] = round(to_float(group.get("clv_sum")) + to_float(result.get("wallet_clv")), 8)
            group["clv_count"] = to_int(group.get("clv_count")) + 1
            group["avg_clv"] = round(group["clv_sum"] / group["clv_count"], 8) if group["clv_count"] else 0.0

    for result in newly_settled:
        group_name = "contested" if result.get("contested") else "clean"
        category = str(result.get("category") or "esports").lower()
        if result.get("status") == "exited":
            wallet = normalize_wallet(result.get("wallet"))
            if not wallet:
                continue
            legs = result.get("legs") or []
            row = wallets.setdefault(
                wallet,
                {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0, "legs": 0},
            )
            row["exits"] = to_int(row.get("exits")) + 1
            row["legs"] = to_int(row.get("legs")) + len(legs)
            row["our_pnl"] = round(to_float(row.get("our_pnl")) + to_float(result.get("our_realized_pnl")), 8)
            total["exits"] = to_int(total.get("exits")) + 1
            total["legs"] = to_int(total.get("legs")) + len(legs)
            total["our_pnl"] = round(to_float(total.get("our_pnl")) + to_float(result.get("our_realized_pnl")), 8)
            update_group(group_name, result, our_pnl=to_float(result.get("our_realized_pnl")), exited=True)
            update_category(category, result, our_pnl=to_float(result.get("our_realized_pnl")), exited=True)
            continue
        won = bool(result.get("outcome_won"))
        our_pnl = to_float(result.get("our_paper_pnl"))
        for wallet, wallet_pnl in (result.get("wallet_paper_pnl_by_wallet") or {}).items():
            row = wallets.setdefault(wallet, {"signals": 0, "wins": 0, "exits": 0, "wallet_pnl": 0.0, "our_pnl": 0.0, "legs": 0})
            row["signals"] += 1
            row["wins"] += 1 if won else 0
            row["wallet_pnl"] = round(to_float(row.get("wallet_pnl")) + to_float(wallet_pnl), 8)
            row["our_pnl"] = round(to_float(row.get("our_pnl")) + our_pnl, 8)
            row["win_rate"] = round(row["wins"] / row["signals"], 8) if row["signals"] else 0.0
        total["signals"] = to_int(total.get("signals")) + 1
        total["wins"] = to_int(total.get("wins")) + (1 if won else 0)
        total["wallet_pnl"] = round(to_float(total.get("wallet_pnl")) + sum(to_float(v) for v in (result.get("wallet_paper_pnl_by_wallet") or {}).values()), 8)
        total["our_pnl"] = round(to_float(total.get("our_pnl")) + our_pnl, 8)
        total["win_rate"] = round(total["wins"] / total["signals"], 8) if total["signals"] else 0.0
        update_group(
            group_name,
            result,
            won=won,
            wallet_pnl=sum(to_float(v) for v in (result.get("wallet_paper_pnl_by_wallet") or {}).values()),
            our_pnl=our_pnl,
        )
        update_category(
            category,
            result,
            won=won,
            wallet_pnl=sum(to_float(v) for v in (result.get("wallet_paper_pnl_by_wallet") or {}).values()),
            our_pnl=our_pnl,
        )
    return {
        "wallets": wallets,
        "total": total,
        "by_category": by_category,
        "groups": groups,
        "updated_at": int(datetime.now(timezone.utc).timestamp()),
    }


def prune_jsonl(rows: list[dict[str, Any]], *, now_ts: int, retention_days: int) -> list[dict[str, Any]]:
    if retention_days <= 0:
        return rows
    cutoff = now_ts - retention_days * SECONDS_PER_DAY
    kept = []
    for row in rows:
        ts = to_int(row.get("created_at") or row.get("settled_at") or row.get("timestamp"))
        if ts >= cutoff:
            kept.append(row)
    return kept
