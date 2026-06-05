from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any

from .core import SECONDS_PER_DAY, normalize_wallet, parse_dt, to_float, to_int


def eligible_follow_wallets(
    leaderboard: list[dict[str, Any]],
    *,
    now_ts: int,
    recency_days: int = 30,
) -> list[dict[str, Any]]:
    cutoff = now_ts - recency_days * SECONDS_PER_DAY
    rows = []
    for row in leaderboard:
        if row.get("grade") != "A":
            continue
        wallet = normalize_wallet(row.get("wallet"))
        last_trade = to_int(row.get("last_esports_trade_at"))
        if wallet and last_trade >= cutoff:
            rows.append({**row, "wallet": wallet})
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


def fill_summary_without_raw(fills_summary: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in fills_summary.items() if key != "fills"}


def evaluate_slippage(wallet_avg: float, current_price: float, *, max_slippage: float) -> dict[str, Any]:
    slippage = current_price - wallet_avg
    return {
        "slippage_over_wallet_entry": round(slippage, 8),
        "would_follow": slippage <= max_slippage,
    }


def paper_pnl(entry_price: float, outcome_won: bool, stake: float) -> float:
    if entry_price <= 0:
        return 0.0
    if outcome_won:
        return round(stake * (1 - entry_price) / entry_price, 8)
    return round(-stake, 8)


def signal_id(condition_id: str, outcome_index: int) -> str:
    return f"{condition_id.lower()}:{outcome_index}"


def upsert_follow_signal(
    open_signals: list[dict[str, Any]],
    *,
    wallet: str,
    market: dict[str, Any],
    qualification: dict[str, Any],
    fills_summary: dict[str, Any],
    current_price: float,
    max_slippage: float,
    stake_usdc: float,
    now_ts: int,
) -> tuple[list[dict[str, Any]], bool]:
    condition_id = qualification["condition_id"]
    outcome_index = qualification["outcome_index"]
    sid = signal_id(condition_id, outcome_index)
    slippage = evaluate_slippage(qualification["wallet_avg_price"], current_price, max_slippage=max_slippage)
    trigger = {
        "wallet": normalize_wallet(wallet),
        "wallet_avg_price": qualification["wallet_avg_price"],
        "position_size": qualification["position_size"],
        "fills_summary": fills_summary,
        "updated_at": now_ts,
    }
    for signal in open_signals:
        if signal.get("signal_id") != sid:
            continue
        signal["updated_at"] = now_ts
        signal["current_price"] = current_price
        triggered = signal.setdefault("triggered_by", [])
        for index, row in enumerate(triggered):
            if normalize_wallet(row.get("wallet")) == trigger["wallet"]:
                triggered[index] = trigger
                break
        else:
            triggered.append(trigger)
        return open_signals, False

    open_signals.append(
        {
            "signal_id": sid,
            "condition_id": condition_id,
            "outcome_index": outcome_index,
            "outcome": qualification.get("outcome"),
            "event_title": market.get("title"),
            "event_slug": market.get("event_slug"),
            "market_question": market.get("question"),
            "match_start_time": market.get("match_start_time") or market.get("market_start_time"),
            "wallet_avg_price": qualification["wallet_avg_price"],
            "our_entry_price": current_price,
            "current_price": current_price,
            "stake_usdc": stake_usdc,
            "created_at": now_ts,
            "updated_at": now_ts,
            "status": "open",
            "triggered_by": [trigger],
            **slippage,
        }
    )
    return open_signals, True


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
        stake = to_float(signal.get("stake_usdc"))
        wallet_pnls = {}
        compact_triggers = []
        for trigger in signal.get("triggered_by") or []:
            wallet = normalize_wallet(trigger.get("wallet"))
            wallet_entry = to_float(trigger.get("wallet_avg_price") or signal.get("wallet_avg_price"))
            wallet_pnls[wallet] = paper_pnl(wallet_entry, outcome_won, stake)
            compact_triggers.append(
                {
                    **{key: value for key, value in trigger.items() if key != "fills_summary"},
                    "fills_summary": fill_summary_without_raw(trigger.get("fills_summary") or {}),
                }
            )
        settled.append(
            {
                **{key: value for key, value in signal.items() if key != "triggered_by"},
                "status": "settled",
                "settled_at": now_ts,
                "outcome_won": outcome_won,
                "wallet_paper_pnl_by_wallet": wallet_pnls,
                "our_paper_pnl": paper_pnl(to_float(signal.get("our_entry_price")), outcome_won, stake),
                "triggered_by": compact_triggers,
            }
        )
    return remaining, settled


def aggregate_follow_performance(prev_perf: dict[str, Any], newly_settled: list[dict[str, Any]]) -> dict[str, Any]:
    wallets = dict(prev_perf.get("wallets") or {})
    total = dict(prev_perf.get("total") or {"signals": 0, "wins": 0, "wallet_pnl": 0.0, "our_pnl": 0.0})
    for result in newly_settled:
        won = bool(result.get("outcome_won"))
        our_pnl = to_float(result.get("our_paper_pnl"))
        for wallet, wallet_pnl in (result.get("wallet_paper_pnl_by_wallet") or {}).items():
            row = wallets.setdefault(wallet, {"signals": 0, "wins": 0, "wallet_pnl": 0.0, "our_pnl": 0.0})
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
    return {
        "wallets": wallets,
        "total": total,
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
