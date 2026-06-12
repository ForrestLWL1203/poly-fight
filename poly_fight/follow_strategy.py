from __future__ import annotations

import copy
import math
from typing import Any

from .core import to_float, to_int


DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION = 1
ACTIVE_FOLLOW_STRATEGY_ID = "active"


def _finite_positive(value: Any) -> bool:
    number = to_float(value, float("nan"))
    return math.isfinite(number) and number > 0


def _finite_non_negative(value: Any) -> bool:
    number = to_float(value, float("nan"))
    return math.isfinite(number) and number >= 0


def default_follow_strategy(*, balance_usdc: float | None = None) -> dict[str, Any]:
    balance = to_float(balance_usdc, 0.0) if balance_usdc is not None else 0.0
    configured = bool(balance_usdc is not None and math.isfinite(balance) and balance > 0)
    return {
        "configured": configured,
        "schema_version": DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION,
        "updated_at": 0,
        "stake_sizing": {
            "mode": "proportional",
            "ratio_percent": 10.0,
            "per_order_cap_enabled": False,
            "per_order_cap_usdc": 0.0,
            "fixed_usdc": 0.0,
            "balance_percent": 0.0,
        },
        "prefilters": {
            "min_target_wallet_order_cash_usdc": 10.0,
        },
        "condition_limits": {
            "order_count_mode": "none",
            "max_orders": 0,
            "stake_cap_mode": "none",
            "stake_cap_usdc": 0.0,
            "stake_cap_balance_percent": 0.0,
        },
        "balance": {
            "required": False,
            "usable_balance_usdc": balance if configured else 0.0,
        },
    }


def normalize_follow_strategy(strategy: dict[str, Any] | None, *, updated_at: int | None = None) -> dict[str, Any]:
    base = default_follow_strategy()
    if isinstance(strategy, dict):
        merged = copy.deepcopy(base)
        for key in ("configured", "schema_version", "updated_at"):
            if key in strategy:
                merged[key] = strategy[key]
        for section in ("stake_sizing", "prefilters", "condition_limits", "balance"):
            if isinstance(strategy.get(section), dict):
                merged[section].update(strategy[section])
        strategy = merged
    else:
        strategy = base

    sizing = strategy["stake_sizing"]
    limits = strategy["condition_limits"]
    prefilters = strategy["prefilters"]
    balance = strategy["balance"]

    sizing["mode"] = str(sizing.get("mode") or "proportional").strip().lower()
    sizing["ratio_percent"] = round(to_float(sizing.get("ratio_percent")), 8)
    sizing["per_order_cap_enabled"] = bool(sizing.get("per_order_cap_enabled"))
    sizing["per_order_cap_usdc"] = round(to_float(sizing.get("per_order_cap_usdc")), 8)
    sizing["fixed_usdc"] = round(to_float(sizing.get("fixed_usdc")), 8)
    sizing["balance_percent"] = round(to_float(sizing.get("balance_percent")), 8)

    if "min_target_wallet_order_cash_usdc" not in prefilters and "min_wallet_trade_cash_usdc" in prefilters:
        prefilters["min_target_wallet_order_cash_usdc"] = prefilters.get("min_wallet_trade_cash_usdc")
    prefilters["min_target_wallet_order_cash_usdc"] = round(
        to_float(prefilters.get("min_target_wallet_order_cash_usdc")),
        8,
    )

    limits["order_count_mode"] = str(limits.get("order_count_mode") or "none").strip().lower()
    limits["max_orders"] = max(0, to_int(limits.get("max_orders")))
    limits["stake_cap_mode"] = str(limits.get("stake_cap_mode") or "none").strip().lower()
    limits["stake_cap_usdc"] = round(to_float(limits.get("stake_cap_usdc")), 8)
    limits["stake_cap_balance_percent"] = round(to_float(limits.get("stake_cap_balance_percent")), 8)

    balance["required"] = bool(balance.get("required", True))
    balance["usable_balance_usdc"] = round(to_float(balance.get("usable_balance_usdc")), 8)

    strategy["schema_version"] = DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION
    if updated_at is not None:
        strategy["updated_at"] = int(updated_at)
    else:
        strategy["updated_at"] = to_int(strategy.get("updated_at"))
    strategy["configured"] = bool(strategy.get("configured"))
    return strategy


def validate_follow_strategy(strategy: dict[str, Any] | None) -> tuple[bool, list[str]]:
    normalized = normalize_follow_strategy(strategy)
    errors: list[str] = []
    sizing = normalized["stake_sizing"]
    prefilters = normalized["prefilters"]
    limits = normalized["condition_limits"]
    balance = normalized["balance"]

    if sizing["mode"] not in {"proportional", "fixed", "balance_percent"}:
        errors.append("stake_sizing.mode")
    if sizing["mode"] == "proportional" and not _finite_positive(sizing.get("ratio_percent")):
        errors.append("stake_sizing.ratio_percent")
    if sizing.get("per_order_cap_enabled") and not _finite_positive(sizing.get("per_order_cap_usdc")):
        errors.append("stake_sizing.per_order_cap_usdc")
    if sizing["mode"] == "fixed" and not _finite_positive(sizing.get("fixed_usdc")):
        errors.append("stake_sizing.fixed_usdc")
    if sizing["mode"] == "balance_percent" and not _finite_positive(sizing.get("balance_percent")):
        errors.append("stake_sizing.balance_percent")
    if not _finite_non_negative(prefilters.get("min_target_wallet_order_cash_usdc")):
        errors.append("prefilters.min_target_wallet_order_cash_usdc")

    if limits["order_count_mode"] not in {"none", "condition", "wallet"}:
        errors.append("condition_limits.order_count_mode")
    if limits["order_count_mode"] != "none" and to_int(limits.get("max_orders")) < 1:
        errors.append("condition_limits.max_orders")
    if limits["stake_cap_mode"] not in {"none", "fixed", "balance_percent"}:
        errors.append("condition_limits.stake_cap_mode")
    if limits["stake_cap_mode"] == "fixed" and not _finite_positive(limits.get("stake_cap_usdc")):
        errors.append("condition_limits.stake_cap_usdc")
    if limits["stake_cap_mode"] == "balance_percent" and not _finite_positive(limits.get("stake_cap_balance_percent")):
        errors.append("condition_limits.stake_cap_balance_percent")

    balance_required = (
        bool(balance.get("required"))
        or sizing["mode"] == "balance_percent"
        or limits["stake_cap_mode"] == "balance_percent"
    )
    if balance_required and not _finite_positive(balance.get("usable_balance_usdc")):
        errors.append("balance.usable_balance_usdc")
    return not errors, errors


def strategy_from_legacy_args(
    *,
    stake_usdc: float,
    stake_ratio_percent: float,
    max_stake_usdc: float,
    max_signal_stake_usdc: float,
    min_wallet_trade_cash_usdc: float,
    balance_usdc: float | None,
) -> dict[str, Any]:
    strategy = default_follow_strategy(balance_usdc=balance_usdc if balance_usdc is not None else None)
    balance = to_float(balance_usdc, float("nan")) if balance_usdc is not None else float("nan")
    if not math.isfinite(balance) or balance <= 0:
        strategy["balance"]["required"] = False
        strategy["balance"]["usable_balance_usdc"] = 0.0
    strategy["configured"] = True
    strategy["stake_sizing"]["mode"] = "proportional"
    strategy["stake_sizing"]["ratio_percent"] = to_float(stake_ratio_percent)
    max_stake = to_float(max_stake_usdc)
    if max_stake > 0:
        strategy["stake_sizing"]["per_order_cap_enabled"] = True
        strategy["stake_sizing"]["per_order_cap_usdc"] = max_stake
    strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = max(0.0, to_float(min_wallet_trade_cash_usdc))
    max_signal = to_float(max_signal_stake_usdc)
    if max_signal > 0:
        strategy["condition_limits"]["stake_cap_mode"] = "fixed"
        strategy["condition_limits"]["stake_cap_usdc"] = max_signal
    # Legacy --stake-usdc used to be a hidden minimum. Keep it only for compatibility metadata.
    strategy["legacy"] = {"stake_usdc": max(0.0, to_float(stake_usdc))}
    return normalize_follow_strategy(strategy)


def _block(reason: str, *, target_stake: int = 0, strategy: dict[str, Any]) -> dict[str, Any]:
    return {
        "would_follow": False,
        "target_stake": int(target_stake),
        "funded_stake": 0,
        "stake_mode": "blocked",
        "block_reason": reason,
        "block_reasons": [reason],
        "strategy_snapshot": copy.deepcopy(strategy),
        "strategy_schema_version": DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION,
    }


def evaluate_follow_candidate(
    *,
    strategy: dict[str, Any],
    target_wallet_order_cash_usdc: float,
    available_balance_usdc: float,
    condition_funded_stake_usdc: float,
    condition_funded_order_count: int,
    wallet_condition_funded_order_count: int,
) -> dict[str, Any]:
    normalized = normalize_follow_strategy(strategy)
    valid, errors = validate_follow_strategy(normalized)
    if not valid:
        return _block("invalid_strategy", strategy=normalized)

    order_cash = to_float(target_wallet_order_cash_usdc)
    prefilters = normalized["prefilters"]
    min_order_cash = to_float(prefilters.get("min_target_wallet_order_cash_usdc"))
    if min_order_cash > 0 and order_cash < min_order_cash:
        return _block("small_target_wallet_order", strategy=normalized)

    sizing = normalized["stake_sizing"]
    mode = str(sizing.get("mode") or "proportional")
    raw_stake = 0.0
    stake_mode = mode
    if mode == "fixed":
        raw_stake = to_float(sizing.get("fixed_usdc"))
    elif mode == "balance_percent":
        raw_stake = to_float(available_balance_usdc) * to_float(sizing.get("balance_percent")) / 100.0
    else:
        raw_stake = order_cash * to_float(sizing.get("ratio_percent")) / 100.0
        if sizing.get("per_order_cap_enabled") and to_float(sizing.get("per_order_cap_usdc")) > 0:
            cap = to_float(sizing.get("per_order_cap_usdc"))
            if raw_stake > cap:
                raw_stake = cap
                stake_mode = "proportional_cap"
            else:
                stake_mode = "proportional"

    target_stake = max(0, math.floor(raw_stake))
    if target_stake < 1:
        return _block("stake_below_minimum", target_stake=target_stake, strategy=normalized)
    available = to_float(available_balance_usdc)
    if available < target_stake:
        return _block("insufficient_balance", target_stake=target_stake, strategy=normalized)

    limits = normalized["condition_limits"]
    max_orders = to_int(limits.get("max_orders"))
    order_mode = str(limits.get("order_count_mode") or "none")
    if order_mode == "condition" and max_orders > 0 and to_int(condition_funded_order_count) >= max_orders:
        return _block("condition_order_cap_reached", target_stake=target_stake, strategy=normalized)
    if order_mode == "wallet" and max_orders > 0 and to_int(wallet_condition_funded_order_count) >= max_orders:
        return _block("wallet_condition_order_cap_reached", target_stake=target_stake, strategy=normalized)

    next_condition_stake = to_float(condition_funded_stake_usdc) + target_stake
    cap_mode = str(limits.get("stake_cap_mode") or "none")
    if cap_mode == "fixed" and next_condition_stake > to_float(limits.get("stake_cap_usdc")):
        return _block("condition_stake_cap_reached", target_stake=target_stake, strategy=normalized)
    if cap_mode == "balance_percent":
        percent = to_float(limits.get("stake_cap_balance_percent"))
        if available <= 0 or (next_condition_stake / available) > (percent / 100.0):
            return _block("condition_stake_cap_reached", target_stake=target_stake, strategy=normalized)

    return {
        "would_follow": True,
        "target_stake": int(target_stake),
        "funded_stake": int(target_stake),
        "stake_mode": stake_mode,
        "block_reason": "",
        "block_reasons": [],
        "strategy_snapshot": copy.deepcopy(normalized),
        "strategy_schema_version": DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION,
    }


def strategy_summary(strategy: dict[str, Any] | None) -> str:
    normalized = normalize_follow_strategy(strategy)
    sizing = normalized["stake_sizing"]
    mode = sizing["mode"]
    if mode == "fixed":
        stake_text = f"固定 {to_float(sizing.get('fixed_usdc')):g} USDC"
    elif mode == "balance_percent":
        stake_text = f"余额 {to_float(sizing.get('balance_percent')):g}%"
    else:
        stake_text = f"目标钱包 {to_float(sizing.get('ratio_percent')):g}%"
        if sizing.get("per_order_cap_enabled"):
            stake_text += f"，单笔 cap {to_float(sizing.get('per_order_cap_usdc')):g}"
    limits = normalized["condition_limits"]
    parts = [stake_text]
    if limits.get("order_count_mode") != "none":
        scope = "condition" if limits.get("order_count_mode") == "condition" else "钱包/condition"
        parts.append(f"{scope} 最多 {to_int(limits.get('max_orders'))} 笔")
    if limits.get("stake_cap_mode") == "fixed":
        parts.append(f"condition cap {to_float(limits.get('stake_cap_usdc')):g}")
    elif limits.get("stake_cap_mode") == "balance_percent":
        parts.append(f"condition cap 余额 {to_float(limits.get('stake_cap_balance_percent')):g}%")
    balance = normalized["balance"]
    if to_float(balance.get("usable_balance_usdc")) > 0:
        parts.append(f"可用余额 {to_float(balance.get('usable_balance_usdc')):g}")
    return "，".join(parts)
