from __future__ import annotations

import copy
import math
from typing import Any

from .core import to_float, to_int


# 单一资金模型(v3):edge 只决定能否跟；注码由目标订单 conviction 与桶级 skill 决定。
# stake = per_match_cap × conviction² × skill，最后夹到 [min_stake, 每场剩余额度]。
# conviction=min(1,(wallet_order_cash/(fill_line_x_cap×per_match_cap))²)
# skill=clamp(edge_lb/edge_ref,0,1)，edge_lb 缺失时按 1.0，避免旧榜单静默零注码。
DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION = 3
ACTIVE_FOLLOW_STRATEGY_ID = "active"

DEFAULT_PER_SIGNAL_PERCENT = 10.0         # 可选单笔硬上限；默认关闭
DEFAULT_PER_SIGNAL_CAP_ENABLED = False
DEFAULT_PER_MATCH_PERCENT = 10.0
# 【子盘】(map/game winner 等非 main_match)每场总预算,与主盘解耦、单独可调;口径同上(每 condition
# 总额,不汇总到系列)。默认 = 主盘值;调低它压制"一个队连押整个系列每局"的子盘堆叠。
DEFAULT_PER_MATCH_PERCENT_SUB = 10.0
DEFAULT_FILL_LINE_X_CAP = 10.0
DEFAULT_EDGE_REF = 0.20
# 单钱包每场(单个市场/单局)最大跟单笔数,主/子盘同一上限。0 = 无限制(默认,不改现状)。
DEFAULT_MAX_FOLLOW_ORDERS_PER_MATCH = 0
DEFAULT_MIN_STAKE_USDC = 1.0              # dust 地板(Polymarket CLOB 最小单),只防 <$1 废单
DEFAULT_MIN_TARGET_ORDER_CASH_USDC = 10.0
DEFAULT_MAX_FOLLOW_ENTRY_PRICE = 0.85     # 观察成交价硬上限；价值门另由 θ̂×0.95 执行
DEFAULT_MIN_FOLLOW_ENTRY_PRICE = 0.0      # 现价下限;0 = 不限(默认关,不改现状)。动态可在面板改
# 主盘止损跌幅%:仅 main_match 信号,现价较加权入场跌幅 ≥ 此% → 按现价全平,不等结算归零。
# 0 = 关(默认)。回测:仅主盘净正(系列赛大比分落后难翻盘),子盘净负故只作用主盘。动态可在面板改。
DEFAULT_MAIN_MATCH_STOP_LOSS_DROP_PCT = 0.0


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
        # 实时刷新 Leaderboard:启动跟单时随 runner 起 observe-live sidecar。运行中不可改。
        "realtime_refresh": False,
        "sizing": {
            "per_signal_percent": DEFAULT_PER_SIGNAL_PERCENT,
            "per_signal_cap_enabled": DEFAULT_PER_SIGNAL_CAP_ENABLED,
            "per_match_percent": DEFAULT_PER_MATCH_PERCENT,            # 主盘每场总预算(每 conditionId 总额)
            "per_match_percent_sub": DEFAULT_PER_MATCH_PERCENT_SUB,    # 子盘每场总预算(map/game winner)
            "fill_line_x_cap": DEFAULT_FILL_LINE_X_CAP,
            "edge_ref": DEFAULT_EDGE_REF,
            "max_follow_orders_per_match": DEFAULT_MAX_FOLLOW_ORDERS_PER_MATCH,  # 每场最大跟单笔数(0=无限)
            "min_stake_usdc": DEFAULT_MIN_STAKE_USDC,
        },
        "prefilters": {
            "min_target_wallet_order_cash_usdc": DEFAULT_MIN_TARGET_ORDER_CASH_USDC,
            "max_follow_entry_price": DEFAULT_MAX_FOLLOW_ENTRY_PRICE,
            "min_follow_entry_price": DEFAULT_MIN_FOLLOW_ENTRY_PRICE,
            "main_match_stop_loss_drop_pct": DEFAULT_MAIN_MATCH_STOP_LOSS_DROP_PCT,
        },
        "balance": {
            "required": True,
            "usable_balance_usdc": balance if configured else 0.0,
        },
    }


def normalize_follow_strategy(strategy: dict[str, Any] | None, *, updated_at: int | None = None) -> dict[str, Any]:
    """收敛到 v3 单一模型。容旧:v1/v2 sizing 字段，读到即映射后丢弃。"""
    out = default_follow_strategy()
    if isinstance(strategy, dict):
        for key in ("configured", "schema_version", "updated_at", "realtime_refresh"):
            if key in strategy:
                out[key] = strategy[key]

        # sizing:优先读新 "sizing",回退旧 "stake_sizing"
        new_sz = strategy.get("sizing") if isinstance(strategy.get("sizing"), dict) else {}
        old_sz = strategy.get("stake_sizing") if isinstance(strategy.get("stake_sizing"), dict) else {}

        ps = new_sz.get("per_signal_percent")
        if ps is None:
            ps = old_sz.get("per_signal_percent", old_sz.get("per_signal_cap_percent"))  # 旧 cap% → 单笔%
        pm = new_sz.get("per_match_percent")
        if pm is None:
            pm = old_sz.get("per_match_percent")
            if pm is None and old_sz.get("per_match_cap_percent") is not None:
                # 旧 per_match_cap_percent 是"整场cap"(所有钱包合计)。新 per_match_percent 也是整场总额
                # 口径 → 直接平移,不再 ×0.5(0.5 是之前每钱包口径的遗留)。
                pm = to_float(old_sz.get("per_match_cap_percent"))
        ms = new_sz.get("min_stake_usdc")
        if ms is None:
            ms = old_sz.get("min_stake_usdc")

        out["sizing"]["per_signal_percent"] = ps if ps is not None else DEFAULT_PER_SIGNAL_PERCENT
        out["sizing"]["per_signal_cap_enabled"] = bool(
            new_sz.get("per_signal_cap_enabled", old_sz.get("per_signal_cap_enabled", False))
        )
        out["sizing"]["per_match_percent"] = pm if pm is not None else DEFAULT_PER_MATCH_PERCENT
        # 子盘预算:新字段优先;旧配置无此字段 → 回退主盘值。
        psub = new_sz.get("per_match_percent_sub")
        out["sizing"]["per_match_percent_sub"] = psub if psub is not None else out["sizing"]["per_match_percent"]
        mo = new_sz.get("max_follow_orders_per_match")
        out["sizing"]["max_follow_orders_per_match"] = mo if mo is not None else DEFAULT_MAX_FOLLOW_ORDERS_PER_MATCH
        out["sizing"]["min_stake_usdc"] = ms if ms is not None else DEFAULT_MIN_STAKE_USDC
        out["sizing"]["fill_line_x_cap"] = new_sz.get("fill_line_x_cap", DEFAULT_FILL_LINE_X_CAP)
        out["sizing"]["edge_ref"] = new_sz.get("edge_ref", DEFAULT_EDGE_REF)

        if isinstance(strategy.get("prefilters"), dict):
            pf = strategy["prefilters"]
            mt = pf.get("min_target_wallet_order_cash_usdc", pf.get("min_wallet_trade_cash_usdc"))
            if mt is not None:
                out["prefilters"]["min_target_wallet_order_cash_usdc"] = mt
            if pf.get("max_follow_entry_price") is not None:
                out["prefilters"]["max_follow_entry_price"] = pf["max_follow_entry_price"]
            if pf.get("min_follow_entry_price") is not None:
                out["prefilters"]["min_follow_entry_price"] = pf["min_follow_entry_price"]
            if pf.get("main_match_stop_loss_drop_pct") is not None:
                out["prefilters"]["main_match_stop_loss_drop_pct"] = pf["main_match_stop_loss_drop_pct"]

        if isinstance(strategy.get("balance"), dict):
            out["balance"].update(strategy["balance"])

    sizing = out["sizing"]
    sizing["per_signal_percent"] = round(to_float(sizing.get("per_signal_percent")), 8)
    sizing["per_signal_cap_enabled"] = bool(sizing.get("per_signal_cap_enabled"))
    sizing["per_match_percent"] = round(to_float(sizing.get("per_match_percent")), 8)
    if sizing.get("per_match_percent_sub") is None:
        sizing["per_match_percent_sub"] = sizing["per_match_percent"]  # 默认 = 主盘
    sizing["per_match_percent_sub"] = round(to_float(sizing.get("per_match_percent_sub")), 8)
    sizing["max_follow_orders_per_match"] = max(0, to_int(sizing.get("max_follow_orders_per_match")))
    sizing["min_stake_usdc"] = round(to_float(sizing.get("min_stake_usdc")), 8)
    sizing["fill_line_x_cap"] = round(to_float(sizing.get("fill_line_x_cap"), DEFAULT_FILL_LINE_X_CAP), 8)
    sizing["edge_ref"] = round(to_float(sizing.get("edge_ref"), DEFAULT_EDGE_REF), 8)

    prefilters = out["prefilters"]
    prefilters["min_target_wallet_order_cash_usdc"] = round(to_float(prefilters.get("min_target_wallet_order_cash_usdc")), 8)
    prefilters["max_follow_entry_price"] = round(min(1.0, max(0.0, to_float(prefilters.get("max_follow_entry_price")))), 8)
    prefilters["min_follow_entry_price"] = round(min(1.0, max(0.0, to_float(prefilters.get("min_follow_entry_price")))), 8)
    # 主盘止损跌幅%:clamp 到 [0,100];缺省 0(关)。
    prefilters["main_match_stop_loss_drop_pct"] = round(min(100.0, max(0.0, to_float(prefilters.get("main_match_stop_loss_drop_pct")))), 4)

    balance = out["balance"]
    balance["required"] = bool(balance.get("required", True))
    balance["usable_balance_usdc"] = round(to_float(balance.get("usable_balance_usdc")), 8)

    out["schema_version"] = DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION
    out["updated_at"] = int(updated_at) if updated_at is not None else to_int(out.get("updated_at"))
    out["configured"] = bool(out.get("configured"))
    out["realtime_refresh"] = bool(out.get("realtime_refresh"))
    return out


def validate_follow_strategy(strategy: dict[str, Any] | None) -> tuple[bool, list[str]]:
    normalized = normalize_follow_strategy(strategy)
    errors: list[str] = []
    sizing = normalized["sizing"]
    prefilters = normalized["prefilters"]
    balance = normalized["balance"]

    if not _finite_positive(sizing.get("per_signal_percent")):
        errors.append("sizing.per_signal_percent")
    if not _finite_positive(sizing.get("per_match_percent")):
        errors.append("sizing.per_match_percent")
    elif sizing.get("per_signal_cap_enabled") and to_float(sizing.get("per_match_percent")) + 1e-9 < to_float(sizing.get("per_signal_percent")):
        errors.append("sizing.per_match_percent")  # 每场预算不能小于单笔
    # 子盘预算只需 >0;允许小于单笔(把子盘单局压到低于一整注正是它的用途)。
    if not _finite_positive(sizing.get("per_match_percent_sub")):
        errors.append("sizing.per_match_percent_sub")
    # 每场最大跟单笔数:非负整数,0 = 无限制。
    if not _finite_non_negative(sizing.get("max_follow_orders_per_match")):
        errors.append("sizing.max_follow_orders_per_match")
    if not _finite_positive(sizing.get("min_stake_usdc")):
        errors.append("sizing.min_stake_usdc")
    if not _finite_positive(sizing.get("fill_line_x_cap")):
        errors.append("sizing.fill_line_x_cap")
    if not _finite_positive(sizing.get("edge_ref")):
        errors.append("sizing.edge_ref")
    if not _finite_non_negative(prefilters.get("min_target_wallet_order_cash_usdc")):
        errors.append("prefilters.min_target_wallet_order_cash_usdc")
    # max/min_follow_entry_price 由 normalize clamp 到 [0,1];两者都启用(∈(0,1))时下限须 < 上限,
    # 否则区间为空会拦掉一切 → 视为非法配置。
    min_entry = to_float(prefilters.get("min_follow_entry_price"))
    max_entry = to_float(prefilters.get("max_follow_entry_price"))
    if 0.0 < min_entry < 1.0 and 0.0 < max_entry < 1.0 and min_entry >= max_entry:
        errors.append("prefilters.min_follow_entry_price")
    # 注:不校验 balance —— 运行时 bankroll 取自 account_balance(动态权益),strategy.balance
    # 仅作展示/回退;evaluate 缺余额时按 no_bankroll 拦,无需在此硬卡。
    _ = balance
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
    """CLI 旧参兼容:旧 ratio/固定额语义已删,统一回落到 v2 默认(余额%定额),只保留 min_target 门 + 余额。"""
    strategy = default_follow_strategy(balance_usdc=balance_usdc if balance_usdc is not None else None)
    balance = to_float(balance_usdc, float("nan")) if balance_usdc is not None else float("nan")
    if not math.isfinite(balance) or balance <= 0:
        strategy["balance"]["required"] = False
        strategy["balance"]["usable_balance_usdc"] = 0.0
    strategy["configured"] = True
    strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = max(0.0, to_float(min_wallet_trade_cash_usdc))
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
    condition_funded_stake_usdc: float = 0.0,        # 该场(整场所有钱包合计)已投 → 每场总预算门
    condition_funded_order_count: int = 0,           # 保留入参,不使用
    wallet_condition_funded_order_count: int = 0,    # 该钱包该场(单市场)已资助笔数 → 每场最大笔数门
    entry_price: float = 0.0,                        # 跟单时实时价 p
    bankroll_usdc: float = 0.0,                      # 动态权益，决定每场 cap；available 决定实际能否支付
    wallet_condition_funded_stake_usdc: float = 0.0, # 保留入参(每钱包每场口径已弃用,改用整场总额),不使用
    market_type: str | None = None,                  # 市场类型;main_match=主盘,其余=子盘 → 选各自预算cap
    theta: float | None = None,                      # 该钱包当前桶的 recency-weighted win-rate
    bucket_edge_lb: float | None = None,             # 当前桶 copy-edge lower bound；缺失→skill 1
) -> dict[str, Any]:
    normalized = normalize_follow_strategy(strategy)
    valid, _errors = validate_follow_strategy(normalized)
    if not valid:
        return _block("invalid_strategy", strategy=normalized)

    sizing = normalized["sizing"]
    prefilters = normalized["prefilters"]

    # ── 门 1:目标单太小 ──
    order_cash = to_float(target_wallet_order_cash_usdc)
    min_order_cash = to_float(prefilters.get("min_target_wallet_order_cash_usdc"))
    if min_order_cash > 0 and order_cash < min_order_cash:
        return _block("small_target_wallet_order", strategy=normalized)

    # ── 门 2:实时价有效 ──
    p = to_float(entry_price)
    if not (0.0 < p < 1.0):
        return _block("no_live_price", strategy=normalized)

    # ── 门 3:入场价上限 ──
    max_entry = to_float(prefilters.get("max_follow_entry_price"))
    if 0.0 < max_entry < 1.0 and p > max_entry:
        return _block("entry_above_ceiling", strategy=normalized)

    # ── 门 3b:入场价下限(与上限对称,卡我方现价 p;0=不限)──
    min_entry = to_float(prefilters.get("min_follow_entry_price"))
    if 0.0 < min_entry < 1.0 and p < min_entry:
        return _block("entry_below_floor", strategy=normalized)

    # sole funded-follow value gate: current price must be below discounted θ̂.
    theta_value = to_float(theta)
    if not (0.0 < theta_value <= 1.0):
        return _block("no_live_edge", strategy=normalized)
    live_edge = theta_value * 0.95 - p
    if live_edge <= 0:
        return _block("no_live_edge", strategy=normalized)

    # ── 动态权益决定每场 cap；available_balance_usdc 是本 tick 可实际支付现金。
    #    若调用方没有提供动态权益，才回退到 available，兼容旧调用。现金耗尽时必须停手。──
    available = to_float(available_balance_usdc)
    equity = to_float(bankroll_usdc)
    if equity <= 0:
        equity = available
    if available <= 0 or equity <= 0:
        return _block("no_bankroll", strategy=normalized)

    min_stake = to_float(sizing.get("min_stake_usdc"))

    # ── 门 5:每场最大跟单笔数(单个市场/单局;主子盘同一上限;0=无限)──
    max_orders = to_int(sizing.get("max_follow_orders_per_match"))
    if max_orders > 0 and to_int(wallet_condition_funded_order_count) >= max_orders:
        return _block("wallet_condition_order_cap_reached", strategy=normalized)

    # ── 门 6:每场总预算。限额单位 = 单个 conditionId(一个市场,含双边两个 outcome)。该 condition
    #         所有钱包、两个方向合计 ≤ 余额×cap%(先到先得,只填到此不叠加)。主盘/子盘各自独立 cap。──
    is_submarket = str(market_type or "main_match") != "main_match"
    match_percent = to_float(sizing.get("per_match_percent_sub")) if is_submarket else to_float(sizing.get("per_match_percent"))
    budget = equity * match_percent / 100.0
    remaining = budget - to_float(condition_funded_stake_usdc)
    if remaining < min_stake:
        return _block("match_budget_reached", strategy=normalized)

    fill_line = max(min_stake, to_float(sizing.get("fill_line_x_cap")) * budget)
    conviction = min(1.0, (order_cash / fill_line) ** 2) if fill_line > 0 else 0.0
    if bucket_edge_lb is None:
        skill = 1.0
    else:
        skill = min(1.0, max(0.0, to_float(bucket_edge_lb) / to_float(sizing.get("edge_ref"))))
    raw_stake = budget * (conviction ** 2) * skill
    if sizing.get("per_signal_cap_enabled"):
        raw_stake = min(raw_stake, equity * to_float(sizing.get("per_signal_percent")) / 100.0)
    raw_stake = min(raw_stake, remaining)
    raw_stake = max(raw_stake, min_stake)

    target_stake = max(0, math.floor(raw_stake))
    if target_stake < 1:
        return _block("stake_below_minimum", target_stake=target_stake, strategy=normalized)

    stake_mode = "kelly"
    if available < target_stake:
        # 余额不足 target,但够最小额 → cap 到余额下单;低于 $1 才真 insufficient。
        capped = math.floor(available)
        if capped >= 1:
            target_stake = capped
            stake_mode = "balance_capped"
        else:
            return _block("insufficient_balance", target_stake=target_stake, strategy=normalized)

    return {
        "would_follow": True,
        "target_stake": int(target_stake),
        "funded_stake": int(target_stake),
        "stake_mode": stake_mode,
        "block_reason": "",
        "block_reasons": [],
        "strategy_snapshot": copy.deepcopy(normalized),
        "strategy_schema_version": DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION,
        "theta": round(theta_value, 8),
        "live_edge": round(live_edge, 8),
        "bucket_edge_lb": round(to_float(bucket_edge_lb), 8) if bucket_edge_lb is not None else None,
        "skill": round(skill, 8),
        "conviction": round(conviction, 8),
        "per_match_cap_usdc": round(budget, 8),
        "fill_line_usdc": round(fill_line, 8),
    }


def strategy_summary(strategy: dict[str, Any] | None) -> str:
    normalized = normalize_follow_strategy(strategy)
    sizing = normalized["sizing"]
    pm = to_float(sizing.get('per_match_percent'))
    pms = to_float(sizing.get('per_match_percent_sub'))
    budget_txt = (f"每场预算 主盘{pm:g}%/子盘{pms:g}%" if abs(pms - pm) > 1e-9
                  else f"每场预算 {pm:g}%(主子同)")
    parts = [f"conviction²×skill（{budget_txt},最小${to_float(sizing.get('min_stake_usdc')):g}）"]
    if sizing.get("per_signal_cap_enabled"):
        parts.append(f"单笔≤余额{to_float(sizing.get('per_signal_percent')):g}%")
    max_orders = to_int(sizing.get("max_follow_orders_per_match"))
    if max_orders > 0:
        parts.append(f"每场≤{max_orders}笔")
    max_entry = to_float(normalized["prefilters"].get("max_follow_entry_price"))
    if 0 < max_entry < 1:
        parts.append(f"现价上限 {max_entry:g}")
    min_entry = to_float(normalized["prefilters"].get("min_follow_entry_price"))
    if 0 < min_entry < 1:
        parts.append(f"现价下限 {min_entry:g}")
    min_target = to_float(normalized["prefilters"].get("min_target_wallet_order_cash_usdc"))
    if min_target > 0:
        parts.append(f"目标单≥${min_target:g}")
    stop_pct = to_float(normalized["prefilters"].get("main_match_stop_loss_drop_pct"))
    if stop_pct > 0:
        parts.append(f"主盘止损 -{stop_pct:g}%")
    balance = normalized["balance"]
    if to_float(balance.get("usable_balance_usdc")) > 0:
        parts.append(f"可用余额 {to_float(balance.get('usable_balance_usdc')):g}")
    if normalized.get("realtime_refresh"):
        parts.append("动态刷新")
    return "，".join(parts)
