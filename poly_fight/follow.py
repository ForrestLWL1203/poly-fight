from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any

from .core import SECONDS_PER_DAY, bucket_key, bucket_label, normalize_wallet, parse_dt, to_float, to_int, wallet_is_followable
from .follow_strategy import evaluate_follow_candidate, normalize_follow_strategy

MIN_ADD_RATIO_TO_FIRST = 0.10
MIN_WALLET_TRADE_CASH_USDC = 10.0


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
        followable = wallet_is_followable(row)  # grade A 或有合格桶;favorite 是手动覆盖,另叠加
        if (followable or is_favorite) and not eligible_market_types:
            eligible_market_types = ["main_match"]
        if not followable and not is_favorite:
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


def market_current_price(market: dict[str, Any], outcome_index: int, price_row: dict[str, Any] | None = None) -> float:
    prices = market.get("outcome_prices") or []
    if 0 <= outcome_index < len(prices):
        return to_float(prices[outcome_index])
    price_row = price_row or {}
    return to_float(price_row.get("curPrice") or price_row.get("currentPrice") or price_row.get("price"))


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
    value = trade.get("outcomeIndex")
    if value is None or value == "":
        value = trade.get("outcome_index")
    return to_int(value, -1)


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


def trade_price(trade: dict[str, Any]) -> float:
    return to_float(trade.get("price") or trade.get("avgPrice") or trade.get("avg_price"))


def trade_size(trade: dict[str, Any]) -> float:
    return to_float(trade.get("size") or trade.get("amount"))


def _trade_tuple(trade: dict[str, Any]) -> tuple[int, str]:
    return trade_timestamp(trade), trade_id(trade)


def _cursor_seen_ids(cursor: dict[str, Any] | None) -> set[str]:
    """cursor.timestamp 这一秒里已处理过的 trade id 集合。
    向后兼容老 cursor(只有单 id、无 seen_ids)→ 退化成 {id}。"""
    cursor = cursor or {}
    seen = cursor.get("seen_ids")
    if isinstance(seen, (list, tuple, set)):
        return {str(value) for value in seen if value}
    cid = str(cursor.get("id") or "")
    return {cid} if cid else set()


def _build_trade_cursor(
    trades: list[dict[str, Any]], previous_cursor: dict[str, Any] | None
) -> dict[str, Any] | None:
    latest_ts = max((trade_timestamp(trade) for trade in trades), default=None)
    if latest_ts is None:
        return previous_cursor
    ids_at_latest = {trade_id(trade) for trade in trades if trade_timestamp(trade) == latest_ts}
    # cursor 停在同一秒 → 累积该秒已见过的 id(否则同秒、后到、id 更小的交易会被漏)
    if previous_cursor and to_int(previous_cursor.get("timestamp")) == latest_ts:
        ids_at_latest |= _cursor_seen_ids(previous_cursor)
    return {
        "timestamp": latest_ts,
        "id": max(ids_at_latest) if ids_at_latest else "",  # 保留单 id 供旧读取方/索引列
        "seen_ids": sorted(ids_at_latest),
    }


def select_new_trades(
    trades: list[dict[str, Any]],
    previous_cursor: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, bool]:
    latest_cursor = _build_trade_cursor(trades, previous_cursor)
    if previous_cursor is None:
        return [], latest_cursor, True
    prev_ts = to_int(previous_cursor.get("timestamp"))
    prev_seen = _cursor_seen_ids(previous_cursor)
    # 新交易:timestamp 更晚,或同一秒但该 id 还没在这一秒处理过。
    # (原先按 (ts, id) 严格大于 → 同秒、后到、id 更小的交易会被误判为旧而漏掉。)
    new_trades = [
        trade
        for trade in trades
        if trade_timestamp(trade) > prev_ts
        or (trade_timestamp(trade) == prev_ts and trade_id(trade) not in prev_seen)
    ]
    new_trades.sort(key=lambda row: _trade_tuple(row))
    return new_trades, latest_cursor, False


def follow_signal_id(wallet: str, condition_id: str, outcome_index: int) -> str:
    return f"{normalize_wallet(wallet)}:{condition_id.lower()}:{outcome_index}"


def paper_exit_pnl(entry_price: float, exit_price: float, stake: float) -> float:
    if entry_price <= 0:
        return 0.0
    return round(stake * (exit_price - entry_price) / entry_price, 8)


# 跟卖最小下单额(Polymarket ~$1)。等比例算出的卖额小于此 → 凑到 $1;
# 若卖完后剩余 < $1(无法再单独成单的 dust)→ 一刀切全平。
MIN_FOLLOW_SELL_USDC = 1.0


def _signal_total_stake(signal: dict[str, Any]) -> float:
    return sum(leg_actual_stake(leg) for leg in signal.get("legs") or [])


def _signal_full_exit_pnl(signal: dict[str, Any], exit_price: float, *, hypothetical: bool = False) -> float:
    stake_fn = leg_hypothetical_stake if hypothetical else leg_actual_stake
    return sum(
        paper_exit_pnl(to_float(leg.get("our_entry_price")), exit_price, stake_fn(leg))
        for leg in signal.get("legs") or []
    )


def apply_follow_sell(signal: dict[str, Any], trade: dict[str, Any], exit_price: float, now_ts: int) -> str:
    """目标卖出 → 我们按"占总仓比例"同步平仓,返回 "exited" / "partial" / "hold"。

    等比例:目标累计卖出占仓 wf → 我们目标也卖到 wf。受 $1 最小下单约束,
    比例还没攒够 $1 就等("hold");卖了之后剩余会成 dust(<$1)就一刀切全平。
    记 our_sold_fraction、our_partial_exit_pnl(各次按盘口现价实现);余量留到结算。
    """
    sold_before = to_float(signal.get("wallet_sell_size"))
    signal["wallet_sell_size"] = round(sold_before + trade_size(trade), 8)
    signal.setdefault("behavior_events", []).append(_behavior_event("sell", trade))

    bought = sum(to_float(leg.get("wallet_trade_size")) for leg in signal.get("legs") or [])
    wallet_sold_frac = min(1.0, to_float(signal.get("wallet_sell_size")) / bought) if bought > 0 else 1.0
    sold_frac = to_float(signal.get("our_sold_fraction"))
    total_stake = _signal_total_stake(signal)

    if total_stake <= 0:
        # 未注资(纯研究信号):纯比例镜像 hypothetical,不受 $1 约束。
        delta = max(0.0, wallet_sold_frac - sold_frac)
    else:
        held_dollar = total_stake * (1.0 - sold_frac)
        pending = total_stake * max(0.0, wallet_sold_frac - sold_frac)   # 还应补卖的额
        if pending <= 0:
            signal["wallet_behavior"] = wallet_behavior_summary(signal)
            return "exited" if sold_frac >= 1.0 - 1e-9 else "hold"
        if held_dollar - pending < MIN_FOLLOW_SELL_USDC:    # 卖后剩余成 dust → 全平
            sell = held_dollar
        elif pending < MIN_FOLLOW_SELL_USDC:                # 比例还没攒够 $1 → 等
            signal["wallet_behavior"] = wallet_behavior_summary(signal)
            return "hold"
        else:
            sell = pending
        delta = max(0.0, min(sell, held_dollar)) / total_stake

    if delta <= 0:
        signal["wallet_behavior"] = wallet_behavior_summary(signal)
        return "exited" if sold_frac >= 1.0 - 1e-9 else "hold"

    signal["our_partial_exit_pnl"] = round(
        to_float(signal.get("our_partial_exit_pnl")) + delta * _signal_full_exit_pnl(signal, exit_price), 8
    )
    signal["our_partial_exit_pnl_hypothetical"] = round(
        to_float(signal.get("our_partial_exit_pnl_hypothetical"))
        + delta * _signal_full_exit_pnl(signal, exit_price, hypothetical=True), 8
    )
    new_sold = min(1.0, sold_frac + delta)
    signal["our_sold_fraction"] = round(new_sold, 8)
    signal.setdefault("partial_exits", []).append({
        "timestamp": now_ts,
        "price": round(exit_price, 8),
        "sold_stake": round(delta * total_stake, 8),
        "sold_fraction_delta": round(delta, 8),
        "cumulative_sold_fraction": round(new_sold, 8),
    })
    signal.setdefault("behavior_events", []).append(_behavior_event("exit", trade))
    signal["updated_at"] = now_ts
    fully = new_sold >= 1.0 - 1e-9
    if fully:
        signal["status"] = "exited"
        signal["exit_price"] = round(exit_price, 8)
        signal["exit_at"] = now_ts
        signal["exit_reason"] = "wallet_sell"
        signal["our_realized_pnl"] = signal["our_partial_exit_pnl"]
        signal["hypothetical_pnl"] = signal["our_partial_exit_pnl_hypothetical"]
    signal["wallet_behavior"] = wallet_behavior_summary(signal)
    return "exited" if fully else "partial"


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


def signal_has_actual_follow(signal: dict[str, Any]) -> bool:
    return any(leg_actual_stake(leg) > 0 for leg in (signal or {}).get("legs") or [])


def prune_unfollowed_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [signal for signal in signals if signal_has_actual_follow(signal)]


def first_leg_wallet_trade_cash(signal: dict[str, Any] | None) -> float:
    for leg in (signal or {}).get("legs") or []:
        cash = to_float((leg or {}).get("wallet_trade_cash"))
        if cash > 0:
            return cash
    return 0.0


def _condition_strategy_counts(
    open_signals: list[dict[str, Any]],
    *,
    condition_id: str,
    wallet: str,
) -> dict[str, float | int]:
    normalized_condition = str(condition_id or "").lower()
    normalized_wallet = normalize_wallet(wallet)
    funded_stake = 0.0
    funded_order_count = 0
    wallet_funded_order_count = 0
    for signal in open_signals:
        if (signal.get("status") or "open") != "open":
            continue
        if str(signal.get("condition_id") or "").lower() != normalized_condition:
            continue
        signal_wallet = normalize_wallet(signal.get("wallet"))
        for leg in signal.get("legs") or []:
            stake = leg_actual_stake(leg)
            if stake <= 0:
                continue
            funded_stake = round(funded_stake + stake, 8)
            funded_order_count += 1
            if signal_wallet == normalized_wallet:
                wallet_funded_order_count += 1
    return {
        "condition_funded_stake_usdc": funded_stake,
        "condition_funded_order_count": funded_order_count,
        "wallet_condition_funded_order_count": wallet_funded_order_count,
    }


def leg_hypothetical_stake(leg: dict[str, Any]) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return max(0.0, to_float(leg.get("stake")))


def _follow_min_order_cash(strategy: dict[str, Any] | None, fallback_usdc: float) -> float:
    """跟单最小下单额门槛 = 策略 prefilter min_target_wallet_order_cash_usdc 与 $floor 取大;
    小单累加器据此判定"凑够没"。无策略(legacy)时退回 floor。"""
    fallback = to_float(fallback_usdc)
    if isinstance(strategy, dict):
        v = to_float((strategy.get("prefilters") or {}).get("min_target_wallet_order_cash_usdc"))
        if v > 0:
            return max(v, fallback)
    return fallback


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
    min_wallet_trade_cash_usdc: float = MIN_WALLET_TRADE_CASH_USDC,
    stake_ratio_percent: float = 10.0,
    require_pre_match: bool = True,
    post_start_grace_seconds: int = 0,
    quarantine_sell_frac: float = 0.2,
    eligible_market_types: set[str] | None = None,
    eligible_buckets: set[str] | None = None,
    bucket_theta: dict[str, float] | None = None,   # kelly:桶 → θ̂(近期加权点估胜率),策略内 ×0.95 作现价门
    bucket_edge_lb: dict[str, float] | None = None,  # kelly:桶 → edge_lb(copy-edge 下界),实力乘数
    eligible_category: str | None = None,
    eligible_leagues: set[str] | None = None,
    conflict_policy: str = "dual_follow",
    bankroll_usdc: float = float("inf"),
    max_stake_usdc: float = 0.0,
    max_signal_stake_usdc: float = 0.0,
    follow_strategy: dict[str, Any] | None = None,
    pending_small_buys: dict[str, dict[str, Any]] | None = None,   # 小单累加器(跨 tick 持久,由调用方 load/save)
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    wallet = normalize_wallet(wallet)
    active_strategy = normalize_follow_strategy(follow_strategy) if isinstance(follow_strategy, dict) else None
    observed_ts = to_int(observed_at) or now_ts
    previous_poll_ts = to_int(previous_poll_at)
    stats: dict[str, Any] = {
        "new_leg_count": 0,
        "exited_signal_count": 0,
        "partial_exit_count": 0,
        "hedge_event_count": 0,
        "ignored_trade_count": 0,
        "market_type_not_eligible_count": 0,
        "league_not_eligible_count": 0,
        "opposite_blocked_count": 0,
        "low_entry_price_blocked_count": 0,
        "high_entry_price_blocked_count": 0,
        "small_wallet_trade_blocked_count": 0,
        "insufficient_balance_count": 0,
        "small_add_blocked_count": 0,
        "signal_cap_limited_count": 0,
        "signal_cap_blocked_count": 0,
        "strategy_invalid_count": 0,
        "stake_below_minimum_count": 0,
        "condition_order_cap_blocked_count": 0,
        "wallet_condition_order_cap_blocked_count": 0,
        "condition_stake_cap_blocked_count": 0,
        "funded_stake_usdc": 0.0,
        "unfunded_intent_count": 0,
        "small_buy_cached_count": 0,
        "small_buy_triggered_count": 0,
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
            # 钱包卖出该 (cond,outcome):若有未凑够门槛的小单缓存,说明它没建够仓就跑了 → 清缓存,不补跟。
            if pending_small_buys is not None:
                pending_small_buys.pop(f"{wallet}|{condition_id}|{outcome_index}", None)
            if existing:
                # 等比例跟卖:目标累计卖到仓位 x% → 我们也卖到 x%(min $1,不够攒着;dust 全平)。
                state = apply_follow_sell(existing, trade, current_price, now_ts)
                if state == "exited":
                    stats["exited_signal_count"] += 1
                elif state == "partial":
                    stats["partial_exit_count"] += 1
                continue
            # 没有 open 信号(已平/已结算)→ 仅记录卖出供 behavior/quarantine 分析。
            signal_for_sell = _signal_by_id(open_signals, sid)
            if not signal_for_sell:
                stats["ignored_trade_count"] += 1
                continue
            signal_for_sell["wallet_sell_size"] = round(to_float(signal_for_sell.get("wallet_sell_size")) + trade_size(trade), 8)
            signal_for_sell.setdefault("behavior_events", []).append(_behavior_event("sell", trade))
            signal_for_sell["wallet_behavior"] = wallet_behavior_summary(signal_for_sell)
            continue

        if side != "BUY":
            stats["ignored_trade_count"] += 1
            continue

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
        wallet_cash = round(trade_size(trade) * wallet_fill_price, 8)
        # 小单累加器:可跟桶上的 BUY(开仓或加仓)未达最小下单额 → 缓存累加,不立即跟;
        # 多笔凑够门槛 → 用累计量(总股数 + 加权价)合成一笔,清缓存,落入正常跟单流程。
        # 触发后清零(不留余量)。卖出/结算清缓存在别处处理。
        if pending_small_buys is not None:
            acc_key = f"{wallet}|{condition_id}|{outcome_index}"
            min_order_cash = _follow_min_order_cash(active_strategy, min_wallet_trade_cash_usdc)
            prior = pending_small_buys.get(acc_key)
            if prior or wallet_cash < min_order_cash:
                acc_size = to_float((prior or {}).get("size")) + trade_size(trade)
                acc_cash = to_float((prior or {}).get("cash")) + wallet_cash
                acc_first = to_int((prior or {}).get("first_ts")) or trade_timestamp(trade)
                if acc_cash + 1e-9 < min_order_cash:                       # 没凑够 → 缓存,先不跟
                    pending_small_buys[acc_key] = {
                        "size": round(acc_size, 8), "cash": round(acc_cash, 8),
                        "first_ts": acc_first, "last_ts": trade_timestamp(trade),
                    }
                    stats["small_buy_cached_count"] += 1
                    continue
                # 凑够 → 用累计量合成一笔(加权价),清缓存,继续正常流程
                pending_small_buys.pop(acc_key, None)
                agg_price = round(acc_cash / acc_size, 8) if acc_size > 0 else wallet_fill_price
                trade = {**trade, "size": round(acc_size, 8), "price": agg_price}
                wallet_fill_price = agg_price
                wallet_cash = round(acc_cash, 8)
                stats["small_buy_triggered_count"] += 1
        condition_counts = _condition_strategy_counts(open_signals, condition_id=condition_id, wallet=wallet)
        strategy_decision: dict[str, Any] | None = None
        if active_strategy is not None:
            strategy_decision = evaluate_follow_candidate(
                strategy=active_strategy,
                target_wallet_order_cash_usdc=wallet_cash,
                available_balance_usdc=bankroll_usdc - _open_signals_exposure(open_signals),
                condition_funded_stake_usdc=to_float(condition_counts["condition_funded_stake_usdc"]),
                condition_funded_order_count=to_int(condition_counts["condition_funded_order_count"]),
                wallet_condition_funded_order_count=to_int(condition_counts["wallet_condition_funded_order_count"]),
                # kelly:被跟桶 θ̂(现价门)+ edge_lb(实力乘数)+ 实时价 → 信念×实力定额(其它 mode 忽略)
                bucket_win_rate=to_float((bucket_theta or {}).get(market_bucket) or (bucket_theta or {}).get(market_type) or 0.0),
                bucket_edge_lb=(
                    (bucket_edge_lb or {}).get(market_bucket)
                    if (bucket_edge_lb or {}).get(market_bucket) is not None
                    else (bucket_edge_lb or {}).get(market_type)
                ),
                entry_price=to_float(current_price),
                bankroll_usdc=to_float(bankroll_usdc) if bankroll_usdc != float("inf") else 0.0,
            )
            if strategy_decision.get("block_reason") == "small_target_wallet_order":
                stats["small_wallet_trade_blocked_count"] += 1
                stats["ignored_trade_count"] += 0
                continue

        slippage = evaluate_slippage(wallet_fill_price, current_price, max_slippage=max_slippage)
        # 滑点不再作为跟单门:现价是否值得跟,统一由策略 edge 闸(θ̂×0.95 − 现价)裁定。
        # 仍保留 slippage_over_wallet_entry 指标供展示/CLV。下面的 high/low/small 闸照常生效。
        slippage["would_follow"] = True
        follow_block_reasons = []
        min_wallet_trade_cash = to_float(min_wallet_trade_cash_usdc)
        if min_wallet_entry_price > 0 and wallet_fill_price < min_wallet_entry_price:
            slippage["would_follow"] = False
            follow_block_reasons.append("low_entry_price")
            stats["low_entry_price_blocked_count"] += 1
        if max_entry_price > 0 and current_price > max_entry_price:
            slippage["would_follow"] = False
            follow_block_reasons.append("high_entry_price")
            stats["high_entry_price_blocked_count"] += 1
        if min_wallet_trade_cash > 0 and wallet_cash < min_wallet_trade_cash:
            slippage["would_follow"] = False
            follow_block_reasons.append("small_wallet_trade")
            stats["small_wallet_trade_blocked_count"] += 1
        stake_mode = None
        stake_ratio = None
        available_balance = bankroll_usdc - _open_signals_exposure(open_signals)
        if active_strategy is not None and strategy_decision is not None:
            target_stake = to_float(strategy_decision.get("target_stake"))
            target_stake_mode = str(strategy_decision.get("stake_mode") or "strategy")
            leg_stake = target_stake
            funded_stake = to_float(strategy_decision.get("funded_stake"))
            stake_mode = str(strategy_decision.get("stake_mode") or "strategy")
            stake_ratio = round(to_float((active_strategy.get("stake_sizing") or {}).get("ratio_percent")) / 100.0, 6)
            funding_status = "funded" if strategy_decision.get("would_follow") else "blocked"
            would_follow = bool(strategy_decision.get("would_follow")) and bool(slippage["would_follow"])
            if not strategy_decision.get("would_follow"):
                reason = str(strategy_decision.get("block_reason") or "strategy_blocked")
                follow_block_reasons.append(reason)
                if reason == "stake_below_minimum":
                    stats["stake_below_minimum_count"] += 1
                elif reason == "insufficient_balance":
                    stats["insufficient_balance_count"] += 1
                    stats["unfunded_intent_count"] += 1
                elif reason == "condition_order_cap_reached":
                    stats["condition_order_cap_blocked_count"] += 1
                elif reason == "wallet_condition_order_cap_reached":
                    stats["wallet_condition_order_cap_blocked_count"] += 1
                elif reason == "condition_stake_cap_reached":
                    stats["condition_stake_cap_blocked_count"] += 1
                elif reason == "invalid_strategy":
                    stats["strategy_invalid_count"] += 1
                else:
                    # 其余策略 block(no_live_edge / match_cap_reached / no_bankroll / no_live_price …)
                    # 也计数,便于诊断"候选过滤但没开仓"卡在哪一道。
                    stats[f"{reason}_count"] = stats.get(f"{reason}_count", 0) + 1
                funded_stake = 0.0
            if funding_status == "funded" and not slippage["would_follow"]:
                funded_stake = 0.0
                funding_status = "blocked"
        else:
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
        signal_stake_before = sum(leg_actual_stake(leg) for leg in (existing or {}).get("legs") or [])
        max_signal_stake = to_float(max_signal_stake_usdc)
        if active_strategy is None and max_signal_stake > 0 and funding_status == "funded" and would_follow and funded_stake > 0:
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
        if not would_follow or funded_stake <= 0:
            continue
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
            "min_wallet_trade_cash_usdc": round(min_wallet_trade_cash, 8),
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
            "target_wallet_order_cash_usdc": wallet_cash,
            "stake_mode": stake_mode,
            "stake_ratio_percent": round(stake_ratio_percent, 8),
            "max_stake_usdc": round(to_float(max_stake_usdc), 8),
            "max_signal_stake_usdc": round(max_signal_stake, 8),
            "signal_stake_before": round(signal_stake_before, 8),
            "condition_funded_stake_before": round(to_float(condition_counts["condition_funded_stake_usdc"]), 8),
            "condition_funded_order_count_before": to_int(condition_counts["condition_funded_order_count"]),
            "wallet_condition_funded_order_count_before": to_int(condition_counts["wallet_condition_funded_order_count"]),
            "observed_at": observed_ts,
        }
        if strategy_decision is not None:
            leg["strategy_schema_version"] = to_int(strategy_decision.get("strategy_schema_version"))
            leg["strategy_mode"] = str((active_strategy.get("stake_sizing") or {}).get("mode") or "")
            leg["strategy_snapshot"] = strategy_decision.get("strategy_snapshot") or active_strategy
            leg["min_target_wallet_order_cash_usdc"] = round(
                to_float(((active_strategy.get("prefilters") or {}).get("min_target_wallet_order_cash_usdc"))),
                8,
            )
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
            if strategy_decision is not None:
                signal["strategy_schema_version"] = leg.get("strategy_schema_version")
                signal["strategy_mode"] = leg.get("strategy_mode")
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
        trade_outcome = trade_outcome_index(trade)
        if outcome_index is not None and trade_outcome >= 0 and trade_outcome != outcome_index:
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
            # 部分跟卖后:只把"没卖完的余量"(1−our_sold_fraction)拿到结算,加上此前各次部分平仓的累计 PnL。
            # 无部分卖出时 sold_frac=0、partial=0 → 退化为整仓结算(向后兼容)。
            sold_frac = to_float(signal.get("our_sold_fraction"))
            remaining_frac = max(0.0, 1.0 - sold_frac)
            partial_pnl = to_float(signal.get("our_partial_exit_pnl"))
            partial_pnl_hypo = to_float(signal.get("our_partial_exit_pnl_hypothetical"))
            wallet_pnl = round(
                sum(paper_pnl(to_float(leg.get("wallet_fill_price")), outcome_won, leg_actual_stake(leg)) for leg in legs),
                8,
            )
            our_pnl = round(
                partial_pnl
                + remaining_frac * sum(paper_pnl(to_float(leg.get("our_entry_price")), outcome_won, leg_actual_stake(leg)) for leg in legs),
                8,
            )
            hypothetical_wallet_pnl = round(
                sum(paper_pnl(to_float(leg.get("wallet_fill_price")), outcome_won, leg_hypothetical_stake(leg)) for leg in legs),
                8,
            )
            hypothetical_pnl = round(
                partial_pnl_hypo
                + remaining_frac * sum(paper_pnl(to_float(leg.get("our_entry_price")), outcome_won, leg_hypothetical_stake(leg)) for leg in legs),
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
