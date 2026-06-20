from __future__ import annotations

import copy
import math
from typing import Any

from .core import FOLLOWABLE_PRICE_CEILING, to_float, to_int


DEFAULT_FOLLOW_STRATEGY_SCHEMA_VERSION = 1
ACTIVE_FOLLOW_STRATEGY_ID = "active"
# 现价上限默认(0 = 不限)= 全系统唯一分水岭 FOLLOWABLE_PRICE_CEILING(评分/跟单/seed 同源)。
DEFAULT_MAX_FOLLOW_ENTRY_PRICE = FOLLOWABLE_PRICE_CEILING
DEFAULT_FIXED_STAKE_USDC = 1.0          # 固定注默认每信号金额(legacy)
# 【已休眠】曾用 ¼Kelly×edge/(1−p)×本金 给注码;现注码 = 单场cap×信念²×实力(见 DEFAULT_EDGE_REF
# / DEFAULT_FILL_LINE_X_CAP 与 evaluate_follow_candidate),edge 仅当准入门。kelly_fraction 字段
# 仅供旧 db 反序列化,不参与算注。
DEFAULT_KELLY_FRACTION = 0.25
DEFAULT_PER_SIGNAL_CAP_PERCENT = 5.0    # 单笔上限 = 本金 5%
DEFAULT_PER_MATCH_CAP_PERCENT = 10.0    # 单场(condition)上限 = 本金 10%,防一场亏光
DEFAULT_MIN_STAKE_USDC = 1.0            # Polymarket CLOB 最小单(INVALID_ORDER_MIN_SIZE)
# 跟单现价门:有效胜率 = θ̂(近期加权点估) × 此折扣,现价需 < 该值才跟(留 5% 相对安全边际)。
# 比纯 wilson_lb 宽(与入榜的 θ̂ 轴一致),又不至于按头版胜率满价追。
THETA_FOLLOW_DISCOUNT = 0.95
# 凸形信念 sizing:跟单 = 单场cap × (钱包这笔买入额 / 打满线)²,夹在 [min_stake, 单场剩余额度]。
# 打满线 = fill_line_x_cap × 单场cap(钱包押到打满线 → 跟满 cap;中等单按平方衰减,压低中等暴露、
# 躲"4赢1负亏光")。Kelly edge 仅当门(θ̂×0.95>现价)。follow_mirror_percent 已弃用、字段休眠。
DEFAULT_FILL_LINE_X_CAP = 10.0          # 钱包押满 10×单场cap → 跟满 cap(信念满)
# 实力(skill)= 钱包该桶 edge_lb / edge_ref,夹 [0,1]。edge_lb 是 copy-edge 下界(含样本折扣,
# ≈ edge+wilson),低实力即便高信念也把注码摁住(防"跟着菜鸟重注亏光")。edge_ref = 满实力对应的 edge。
# 0.20 是刻意保守的高门槛:只有极少数(实测 8/76)又强又重注的才逼近 cap;不追求多数钱包够到 cap。
DEFAULT_EDGE_REF = 0.20
DEFAULT_FOLLOW_MIRROR_PERCENT = 10.0    # 弃用(旧线性镜像);保留供旧 db 配置反序列化不报错


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
        # 实时刷新 Leaderboard:启动跟单时随 runner 起一个 observe-v2 sidecar
        # (每 2h 发现新钱包 + 放回冷却到期的隔离钱包 + 增量更新榜单)。
        # 作为策略字段持久化,运行中不可改。
        "realtime_refresh": False,
        # 默认 mode="kelly":注码 = 单场cap × 信念²(押注/打满线) × 实力(edge_lb/edge_ref),
        # edge 仅当准入门(θ̂×0.95>现价)。legacy fixed/proportional/balance_percent 仍可选但已少用。
        "stake_sizing": {
            "mode": "kelly",
            "kelly_fraction": DEFAULT_KELLY_FRACTION,
            # 单笔硬上限:默认**关**(不勾不生效,纯按公式)。策略编辑页勾选 → 单笔卡死在 % 上限。
            "per_signal_cap_enabled": False,
            "per_signal_cap_percent": DEFAULT_PER_SIGNAL_CAP_PERCENT,
            "per_match_cap_percent": DEFAULT_PER_MATCH_CAP_PERCENT,
            "min_stake_usdc": DEFAULT_MIN_STAKE_USDC,
            "fill_line_x_cap": DEFAULT_FILL_LINE_X_CAP,
            "edge_ref": DEFAULT_EDGE_REF,
            "follow_mirror_percent": DEFAULT_FOLLOW_MIRROR_PERCENT,  # 弃用,休眠
            # legacy(兼容保留):
            "ratio_percent": 10.0,
            "per_order_cap_enabled": False,
            "per_order_cap_usdc": 0.0,
            "fixed_usdc": DEFAULT_FIXED_STAKE_USDC,
            "balance_percent": 0.0,
        },
        "prefilters": {
            "min_target_wallet_order_cash_usdc": 10.0,
            # 现价(我们检测时)上限:跟单有延迟,钱包 0.75 买、我们发现已 0.9 就别跟。
            # current_price > 此值 → would_follow=False → 不建 leg/不进列表。0 = 不限。
            "max_follow_entry_price": DEFAULT_MAX_FOLLOW_ENTRY_PRICE,
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
        for key in ("configured", "schema_version", "updated_at", "realtime_refresh"):
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

    sizing["mode"] = str(sizing.get("mode") or "kelly").strip().lower()
    # v18 Kelly 引擎参数
    sizing["kelly_fraction"] = round(to_float(sizing.get("kelly_fraction") if sizing.get("kelly_fraction") is not None else DEFAULT_KELLY_FRACTION), 8)
    sizing["per_signal_cap_percent"] = round(to_float(sizing.get("per_signal_cap_percent") if sizing.get("per_signal_cap_percent") is not None else DEFAULT_PER_SIGNAL_CAP_PERCENT), 8)
    sizing["per_match_cap_percent"] = round(to_float(sizing.get("per_match_cap_percent") if sizing.get("per_match_cap_percent") is not None else DEFAULT_PER_MATCH_CAP_PERCENT), 8)
    sizing["min_stake_usdc"] = round(to_float(sizing.get("min_stake_usdc") if sizing.get("min_stake_usdc") is not None else DEFAULT_MIN_STAKE_USDC), 8)
    sizing["fill_line_x_cap"] = round(to_float(sizing.get("fill_line_x_cap") if sizing.get("fill_line_x_cap") is not None else DEFAULT_FILL_LINE_X_CAP), 8)
    sizing["edge_ref"] = round(to_float(sizing.get("edge_ref") if sizing.get("edge_ref") is not None else DEFAULT_EDGE_REF), 8)
    sizing["per_signal_cap_enabled"] = bool(sizing.get("per_signal_cap_enabled"))
    sizing["follow_mirror_percent"] = round(to_float(sizing.get("follow_mirror_percent") if sizing.get("follow_mirror_percent") is not None else DEFAULT_FOLLOW_MIRROR_PERCENT), 8)
    # legacy
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
    if "max_follow_entry_price" not in prefilters:
        prefilters["max_follow_entry_price"] = DEFAULT_MAX_FOLLOW_ENTRY_PRICE
    prefilters["max_follow_entry_price"] = round(
        min(1.0, max(0.0, to_float(prefilters.get("max_follow_entry_price")))), 8
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
    strategy["realtime_refresh"] = bool(strategy.get("realtime_refresh"))
    return strategy


def validate_follow_strategy(strategy: dict[str, Any] | None) -> tuple[bool, list[str]]:
    normalized = normalize_follow_strategy(strategy)
    errors: list[str] = []
    sizing = normalized["stake_sizing"]
    prefilters = normalized["prefilters"]
    limits = normalized["condition_limits"]
    balance = normalized["balance"]

    if sizing["mode"] not in {"kelly", "proportional", "fixed", "balance_percent"}:
        errors.append("stake_sizing.mode")
    if sizing["mode"] == "kelly":
        kf = to_float(sizing.get("kelly_fraction"))
        if not (_finite_positive(kf) and kf <= 1.0):
            errors.append("stake_sizing.kelly_fraction")
        if not _finite_positive(sizing.get("per_signal_cap_percent")):
            errors.append("stake_sizing.per_signal_cap_percent")
        if not _finite_positive(sizing.get("per_match_cap_percent")):
            errors.append("stake_sizing.per_match_cap_percent")
        if not _finite_positive(sizing.get("min_stake_usdc")):
            errors.append("stake_sizing.min_stake_usdc")
        if not _finite_positive(sizing.get("fill_line_x_cap")):
            errors.append("stake_sizing.fill_line_x_cap")
        if not _finite_positive(sizing.get("edge_ref")):
            errors.append("stake_sizing.edge_ref")
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
    # max_follow_entry_price 由 normalize clamp 到 [0,1],无需额外校验。

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
        or sizing["mode"] in {"balance_percent", "kelly"}   # kelly 用本金做 Kelly 缩放 + %上限基准
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
    bucket_win_rate: float = 0.0,      # kelly:被跟桶 θ̂(近期加权点估胜率);内部再 ×0.95 折扣
    bucket_edge_lb: float | None = None,  # kelly:被跟桶 edge_lb(copy-edge 下界)→ 实力乘数;None=缺则中性1.0
    entry_price: float = 0.0,          # kelly:跟单时实时价 p
    bankroll_usdc: float = 0.0,        # kelly:本金(Kelly 缩放 + %上限基准);缺则回退 available
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
    mode = str(sizing.get("mode") or "kelly")
    raw_stake = 0.0
    stake_mode = mode
    if mode == "kelly":
        # 凸形信念 sizing:跟单 = 单场cap × (钱包这笔买入额 / 打满线)²,夹在 [min_stake, 单场剩余]。
        #   打满线 = fill_line_x_cap × 单场cap(钱包押到打满线 → 跟满 cap;中等单按平方衰减 → 压低
        #   中等暴露、躲"4赢1负亏光";只有钱包重注=高信念才接近 cap)。曲线连续单调、无断崖。
        # Kelly edge 仅当门:有效胜率 θ̂×0.95 ≤ 现价则不跟(价已涨过把握)。
        # bankroll 优先用调用方传入的**动态权益**(初始本金 + 已实现盈亏;cli 侧 =
        # account_balance + funded_open_exposure),缺时回退静态 usable_balance,再回退可用。
        # 单场cap = bankroll × per_match_cap_percent,随盈亏动态。
        bankroll = to_float(bankroll_usdc)
        if bankroll <= 0:
            bankroll = to_float(normalized["balance"].get("usable_balance_usdc"))
        if bankroll <= 0:
            bankroll = to_float(available_balance_usdc)
        if bankroll <= 0:
            return _block("no_bankroll", strategy=normalized)
        p = to_float(entry_price)
        if not (0.0 < p < 1.0):
            return _block("no_live_price", strategy=normalized)
        if to_float(bucket_win_rate) * THETA_FOLLOW_DISCOUNT - p <= 0:
            return _block("no_live_edge", strategy=normalized)   # 现价 ≥ θ̂×0.95 → 不跟
        min_stake = to_float(sizing.get("min_stake_usdc"))
        per_match_cap = bankroll * to_float(sizing.get("per_match_cap_percent")) / 100.0
        if per_match_cap <= 0:
            return _block("no_bankroll", strategy=normalized)
        # 信念:钱包押多重(额/打满线)²,夹 [0,1]。打满线 = fill_line_x_cap × 单场cap。
        fill_line = to_float(sizing.get("fill_line_x_cap")) * per_match_cap
        conviction = (order_cash / fill_line) if fill_line > 0 else 1.0
        conviction = min(1.0, conviction * conviction)
        # 实力:钱包该桶 edge_lb / edge_ref,夹 [0,1]。缺(None)→ 中性 1.0(只靠信念,不静默砍光)。
        edge_ref = to_float(sizing.get("edge_ref"))
        if bucket_edge_lb is None or edge_ref <= 0:
            skill = 1.0
        else:
            skill = max(0.0, min(1.0, to_float(bucket_edge_lb) / edge_ref))
        raw_stake = per_match_cap * conviction * skill   # 单场cap × 信念 × 实力
        # 单笔硬上限:**可选**(per_signal_cap_enabled 勾选才生效)。不勾 → 纯按公式;勾 → 单笔
        # 卡死在 per_signal_cap(防一笔吃满整场)。单场cap 仍是多笔累计上限,独立生效。
        if sizing.get("per_signal_cap_enabled"):
            per_signal_cap = bankroll * to_float(sizing.get("per_signal_cap_percent")) / 100.0
            if per_signal_cap > 0:
                raw_stake = min(raw_stake, per_signal_cap)
        # 多笔累计:单场剩余额度(condition 已投合计 → per_match_cap 收口)。
        match_remaining = per_match_cap - to_float(condition_funded_stake_usdc)
        if match_remaining < min_stake:
            return _block("match_cap_reached", strategy=normalized)   # 本场已到上限
        raw_stake = min(raw_stake, match_remaining)
        raw_stake = max(raw_stake, min_stake)   # 不低于动态下限(算出更小也提到下限)
        stake_mode = "kelly_conviction"
    elif mode == "fixed":
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
        # 余额不足 target,但仍够最小额($1)→ cap 到余额下单(与 legacy follow_stake_for_signal
        # 的 limited 模式一致),而不是直接弃单。低于 $1 才真正 insufficient_balance。
        capped = math.floor(available)
        if capped >= 1:
            target_stake = capped
            stake_mode = "balance_capped"
        else:
            return _block("insufficient_balance", target_stake=target_stake, strategy=normalized)

    if mode != "kelly":   # kelly 自带单场%上限,不走 legacy condition_limits
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
    if mode == "kelly":
        signal_cap_text = (
            f"单笔≤{to_float(sizing.get('per_signal_cap_percent')):g}%/"
            if sizing.get("per_signal_cap_enabled") else ""
        )
        stake_text = (
            f"单场cap × 信念² × 实力"
            f"(打满线 {to_float(sizing.get('fill_line_x_cap')):g}×cap,"
            f"实力 edge_lb/{to_float(sizing.get('edge_ref')):g},"
            f"{signal_cap_text}单场≤{to_float(sizing.get('per_match_cap_percent')):g}%,"
            f"最小${to_float(sizing.get('min_stake_usdc')):g},edge 门 θ̂×0.95)"
        )
    elif mode == "fixed":
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
    max_entry = to_float(normalized["prefilters"].get("max_follow_entry_price"))
    if 0 < max_entry < 1:
        parts.append(f"现价上限 {max_entry:g}")
    balance = normalized["balance"]
    if to_float(balance.get("usable_balance_usdc")) > 0:
        parts.append(f"可用余额 {to_float(balance.get('usable_balance_usdc')):g}")
    if normalized.get("realtime_refresh"):
        parts.append("动态刷新")
    return "，".join(parts)
