from __future__ import annotations

from datetime import datetime, timezone
from math import sqrt
import re
from statistics import median
from typing import Any, Iterable


SECONDS_PER_DAY = 86400
# profile 复用以此为失效令牌:改任何评分口径(门槛/公式/n_eff 下限/basis)都要 +1,
# 否则采集会复用旧口径的画像、新规则不生效。改完需全量重采一次,之后才走复用加速。
SCORING_VERSION = 24
WILSON_Z = 1.28
TRADE_BEHAVIOR_MIN_MARKETS = 4
# v16:两边对冲(套利)门统一 0.20——bucket 排除(core)、systemic(cli)、v2 钱包级(V2_MAX_TWO_SIDED_RATE)同口径。
TRADE_BEHAVIOR_EXCLUDE_RATE = 0.20
# n_eff 下限统一(见下方 ESPORTS_N_EFF_FLOOR / SPORTS_N_EFF_FLOOR;v16 起不再按盘口分档)。
# Thresholds recalibrated for the de-biased trade-reconstruction win rates (v11):
# real top esports wallets win ~66-81%, not the survivorship-inflated ~90% the old
# closed_positions floors assumed. See review/scoring analysis.
MIN_A_POSITIVE_MARKET_RATE = 0.63
MIN_B_POSITIVE_MARKET_RATE = 0.55
# 技能轴 = capital_weighted_edge（赢钱占比 − 入场价，按资金加权）+ 正 hold PnL。
# roi 不再硬切（它是赔率结构副产品，会冤枉高胜率买热门的钱包），仅作软 reason。
ESPORTS_MIN_ROI = 0.20  # 软信号 low_roi 的提示线，不再硬排除
SPORTS_MIN_ROI = 0.15
# ── Copy 评分轴 v16(固定注、持有到结算 copy;目标美元盈亏/仓位与我们无关)──
# 质量门 = 同一桶同时满足三条下界(点估 → 下界:薄样本自动惩罚,抗 6 桶 look-elsewhere):
#   1) edge_lb = wilson_lb(θ̂, n_eff) − 入场中位价 ≥ EDGE_LB_MIN  对"跟着能赚"有把握(wilson 下界内含,薄样本自动惩罚)
#   2) θ̂ ≥ MIN_BUCKET_WIN_RATE                       桶内近期加权胜率硬门
#   3) n_eff ≥ N_EFF_FLOOR                            数值兜底 + 抗多重比较
# θ̂ = recency_weighted_win_rate;n_eff = effective_sample_size。价格不再单设带
# (入场端靠 seed 0.35–0.75 预筛 + edge_lb 兜大热端);美元/PnL/ROI 仅作软 reason、不判定。
ESPORTS_EDGE_LB_MIN = 0.05
ESPORTS_N_EFF_FLOOR = 12
# 桶内胜率硬门(v20;v23 2026-06-21 0.68→0.75;v24 2026-06-22 0.75→0.58):进榜/跟单的专精桶,
# 近期加权胜率 θ̂ 必须 ≥ 此值,否则判 C —— 不进榜、不跟单。
# v24 复盘(review/follow-strategy-overhaul.md):跟单价格段门 [0.58,0.72] 上线后,θ̂ 这道"偏热门"
# 胜率门与价格段高度冗余;实测(walk-forward)抬高 θ̂ 反而缩小 in-band edge、且白缩 ~半池子。
# 真正的质量底座是 edge_lb(wilson_lb−中位价≥0.05)+ n_eff,不是裸 θ̂。降到 0.58 把 edge_lb 已合格、
# 仅被 θ̂ 误杀的桶放回(实盘板 ~36→~66),质量仍由 edge_lb/n_eff 守。0.56 与 0.58 等价(空档),故取 0.58。
ESPORTS_MIN_BUCKET_WIN_RATE = 0.58
SPORTS_EDGE_LB_MIN = 0.03
SPORTS_N_EFF_FLOOR = 12
SPORTS_MIN_BUCKET_WIN_RATE = 0.60
# 可跟价区(≤ceiling)子集的全局回退地板。per-game 生产路径必须优先使用 scope
# 校准的 subset_floor：市场稀疏时允许低频强胜率专精桶，密集时仍由动态 full floor /
# anchor 抬高样本要求。全局 8 只保留给没有 scope 上下文的 overall/per-type 兼容路径，
# 不得再覆盖校准结果。
ESPORTS_SUBSET_MIN_SAMPLE = 8
SPORTS_SUBSET_MIN_SAMPLE = 6
# ── Scope-adaptive calibration（按 game_family 实测赛事密度自适应 lookback / n_eff / idle）──
# 见 review/scope-adaptive-calibration.md。供给侧信号(Gamma 已结算主盘,profiling 前即可算)→
# 统一公式推每个 scope 一组门。新游戏接入即自校准、不手调。下列阈值为初值,P1 实测 4 游戏 λ 后再标定。
CALIBRATION_WINDOW_DAYS = 90          # 测密度的校准窗(跨多个赛事周期,平滑爆发性)
CALIBRATION_RECENT_WINDOW_DAYS = 28   # 短窗响应 Major/赛季结束后的供给变化
CALIBRATION_RECENT_WEIGHT = 0.70      # 近 28d 为主、90d 为稳定基线，避免上月高密度长期锁死门槛
SCOPE_MARKET_TARGET = 180             # lookback 要装够多少 main 市场(稀疏游戏据此拿到 ~30d 窗口)
SCOPE_LOOKBACK_MIN_DAYS = 14
SCOPE_LOOKBACK_MAX_DAYS = 90
# n_eff 地板按密度 λ(main 市场/天)分三档(老三家也一起自适应,量级不同同门不公平)。
# 分档而非线性:目标值(cs2=10 / lol=8 / dota2=7)落不到一条直线(两段斜率不同),
# 分档能精确命中且各游戏 λ 离阈值有 2-3 余量、不在边界抖动。实测 λ:cs2≈16 / lol≈12 / dota2·valo≈6。
SCOPE_NEFF_DENSE = 10                 # λ ≥ λ_T2(密集,如 cs2)—— 满严格"锚点"
SCOPE_NEFF_MID = 8                    # λ_T1 ≤ λ < λ_T2(中,如 lol)—— 满严格锚点
SCOPE_NEFF_SPARSE = 7                 # λ < λ_T1(稀疏,如 dota2)—— 满严格锚点
SCOPE_NEFF_LAMBDA_T1 = 9.0
SCOPE_NEFF_LAMBDA_T2 = 14.0
# v21 缓冲缩放:实际 n_eff 地板 = 基数 + round((锚点 − 基数) × scale)。
#   scale=1.0 → 复现满严格 10/8/7;scale=0.5 → 8/7/6(密集留缓冲、稀疏并入基数)。
#   仍按 λ 自适应分档(锚点随密度走),只是整体缓冲按比例缩——单旋钮、非硬编码。
SCOPE_NEFF_CUSHION_SCALE = 0.5
# v22:自适应全样本地板的基数。曾 = ESPORTS_SUBSET_MIN_SAMPLE,病B 把子集门抬到 8 后【解耦】,
#   基数仍保持 6,避免把密集游戏全样本地板连带抬到 9 而超出"只砍最薄 subset<8"的保守范围。
SCOPE_NEFF_FLOOR_BASE = 6
# 薄样本附加门:桶 full n_eff 落在 [放松地板, 满严格锚点) 区间(即只因缩放才够样本)→
#   要求更强信号(edge_lb ≥ THIN_EDGE 且 θ̂ ≥ THIN_WR)才给 A/B,挡贴门弱信号、放行真专精。
SCOPE_NEFF_THIN_EDGE_MIN = 0.08
SCOPE_NEFF_THIN_WR_MIN = 0.80
# 价格子集低于严格锚点时，只放行“真正强”的低频专精桶：高胜率、完整历史置信下界、
# 保守复制优势同时过门。这替代固定 subset>=8 的一刀切，防止单纯降门槛放进侥幸连胜。
SCOPE_SUBSET_THIN_EDGE_MIN = 0.12
SCOPE_SUBSET_THIN_WR_MIN = 0.80
SCOPE_SUBSET_THIN_FULL_WILSON_MIN = 0.75
SCOPE_IDLE_MIN_HOURS = 72
SCOPE_IDLE_MAX_HOURS = 21 * 24
SCOPE_IDLE_GAP_MULTIPLIER = 2.0       # idle 上限 ≈ 此倍 × 赛事干涸期(p90 gap)
SCOPE_IDLE_GAP_PERCENTILE = 90        # 用 gap 分布的尾部(p90)而非中位数:中位被"天天有盘"淹没,
#                                       尾部才是 VCT 赛事之间的真空档(VCL 无量赛区不算 in-scope)


def _percentile(values: list[float], p: float) -> float:
    """第 p 百分位(线性插值)。空列表返回 0。"""
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return 0.0
    if n == 1:
        return float(ordered[0])
    rank = (p / 100.0) * (n - 1)
    lo = int(rank)
    frac = rank - lo
    if lo + 1 >= n:
        return float(ordered[-1])
    return ordered[lo] + frac * (ordered[lo + 1] - ordered[lo])


def match_day_gaps(end_timestamps: Iterable[int]) -> list[float]:
    """相邻"有赛日"间隔(天)的列表(只数 in-scope、够流动性的结算盘的日期)。<2 个有赛日 → []。"""
    days = sorted({int(ts // 86400) for ts in end_timestamps if ts})
    if len(days) < 2:
        return []
    return [float(days[i + 1] - days[i]) for i in range(len(days) - 1)]


def derive_scope_params(
    *,
    markets: int,
    window_days: int,
    gaps: list[float] | None = None,
    market_target: int = SCOPE_MARKET_TARGET,
    subset_floor: int = SCOPE_NEFF_FLOOR_BASE,
) -> dict[str, Any]:
    """从一个 scope 的密度信号推 {lookback / n_eff / idle}。纯函数、无 IO。
    见 review/scope-adaptive-calibration.md。"""
    window_days = max(1, int(window_days))
    gaps = gaps or []
    lam = float(markets) / window_days  # main 市场/天
    # 1) lookback:装够 market_target 个 main 市场所需天数,clamp。
    lookback = SCOPE_LOOKBACK_MAX_DAYS if lam <= 0 else int(round(market_target / lam))
    lookback = max(SCOPE_LOOKBACK_MIN_DAYS, min(SCOPE_LOOKBACK_MAX_DAYS, lookback))
    # 2) n_eff 地板:按 λ 分三档(dense / mid / sparse)定"满严格锚点",再按缓冲 scale 缩放。
    if lam >= SCOPE_NEFF_LAMBDA_T2:
        n_eff_full = SCOPE_NEFF_DENSE
    elif lam >= SCOPE_NEFF_LAMBDA_T1:
        n_eff_full = SCOPE_NEFF_MID
    else:
        n_eff_full = SCOPE_NEFF_SPARSE
    # 实际地板 = 子集门 + 缩放后的缓冲(永远 ≥ 子集门);锚点单独保留供薄样本附加门用。
    n_eff = subset_floor + int(round((n_eff_full - subset_floor) * SCOPE_NEFF_CUSHION_SCALE))
    n_eff = max(int(subset_floor), n_eff)
    # 3) idle 上限:锚定赛事干涸期(p90 gap),clamp。中位数会被"天天有盘"淹没,故用尾部。
    gap_p90 = _percentile(gaps, SCOPE_IDLE_GAP_PERCENTILE)
    idle_hours = int(round(SCOPE_IDLE_GAP_MULTIPLIER * gap_p90 * 24))
    idle_hours = max(SCOPE_IDLE_MIN_HOURS, min(SCOPE_IDLE_MAX_HOURS, idle_hours))
    return {
        "markets": int(markets),
        "window_days": window_days,
        "lambda_per_day": round(lam, 4),
        "gap_p50_days": round(_percentile(gaps, 50), 3),
        "gap_p90_days": round(gap_p90, 3),
        "gap_max_days": round(max(gaps) if gaps else 0.0, 3),
        "lookback_days": lookback,
        "profile_lookback_days": lookback,
        "n_eff_floor": int(n_eff),
        "n_eff_floor_full": int(n_eff_full),  # 满严格锚点(薄样本附加门用);n_eff_floor 是缩放后实际地板
        "subset_floor": int(subset_floor),
        "idle_ceiling_hours": int(idle_hours),
    }
# B(留池不上榜,observe 观察用):比 A 各放一档
GRADE_B_WILSON_RELAX = 0.05
GRADE_B_EDGE_RELAX = 0.03
# 全系统唯一的"可跟价格上限"分水岭:评分(只评 ≤此价的场次)、跟单现价上限默认、seed 高价过滤
# 同用此值 —— 评分口径 = 跟单口径。高价"安全垫"场次不参与 θ̂/n_eff/median_entry(避免淹没低价 edge)。
# 低端另有 seed 价地板 0.35(防深爆冷),那是 floor、不同概念,不在此收口。
FOLLOWABLE_PRICE_CEILING = 0.85
SCORING_PRICE_CEILING = FOLLOWABLE_PRICE_CEILING  # 向后兼容别名
# 近期活跃度:按时间半衰期对每盘加权(近期热度 > 陈旧战绩),折算 Kish 有效样本 n_eff,
# 得到近期加权点估胜率 θ̂。让"钱包当前在打的桶"靠成熟分升级。
ESPORTS_RECENCY_HALF_LIFE_DAYS = 21
# actual_minus_hold_pnl_rate 超过此值 = 利润主要靠盘中卖出（我们复制不了）→ 软标记
SWING_DEPENDENT_RATE = 0.2
# 卖出赢家侧且价格已经接近 1.0，大多是赛果基本确定后的释放资金，不按波段/提前卖出处理。
NEAR_RESOLVED_WINNER_SELL_PRICE = 0.95
# 单市场成交 >=20 笔记为 high churn。high_churn 市场占比超过此值 = 机器人/高频/做市
# （盈利来自微观价差和速度，复制不了）→ 直接排除出 leaderboard。
MAX_HIGH_CHURN_MARKET_RATE = 0.5
# 实质性双边:买了≥2个结果、且少数侧买量占比 ≥ 此值 = 真对冲/套利(无方向判断)。
# 主仓占绝对大头(少数侧 < 此值)= 方向单 + 小对冲,仍算方向性。这类市场从"方向胜率"里剔除当中性。
MATERIAL_TWO_SIDED_MIN_MINORITY_FRAC = 0.20
MAIN_MATCH = "main_match"
GAME_WINNER = "game_winner"
MAP_WINNER = "map_winner"
ALLOWED_GAME_FAMILIES = {"cs2", "dota2", "lol"}
ESPORTS_CATEGORY_TAGS = {
    "dota-2",
    "counter-strike-2",
    "cs2",
    "league-of-legends",
}
SPORTS_LEAGUE_TAGS = {
    "nba": "nba",
    "ufc": "ufc",
}
LEAGUE_LABELS = {
    "nba": "NBA",
    "ufc": "UFC",
}
MARKET_TYPE_LABELS = {
    MAIN_MATCH: "主盘",
    GAME_WINNER: "单局",
    MAP_WINNER: "地图",
}
ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS = {
    "lol:main_match": 100,
    "cs2:main_match": 100,
    "dota2:main_match": 100,
    "lol:game_winner": 50,
    "dota2:game_winner": 50,
    "cs2:map_winner": 50,
}
MARKET_TYPE_ORDER = {
    MAIN_MATCH: 0,
    GAME_WINNER: 1,
    MAP_WINNER: 2,
}
GAME_FAMILY_LABELS = {
    "cs2": "CS2",
    "dota2": "Dota2",
    "lol": "LoL",
    # 历史跟单展示兼容；不代表恢复 Valorant 的采集、榜单或新跟单范围。
    "valorant": "Valorant",
    # 跨游戏盘口专家:在某盘口上跨游戏合并后达标(per-type 合格),无单一游戏专精桶。
    "multi": "跨游戏",
}


def bucket_key(game_family: str | None, market_type: str | None) -> str:
    return f"{str(game_family or 'unknown').lower()}:{str(market_type or MAIN_MATCH)}"


def split_bucket_key(value: str | None) -> tuple[str, str]:
    text = str(value or "")
    if ":" not in text:
        return "", text or MAIN_MATCH
    game_family, market_type = text.split(":", 1)
    return game_family, market_type or MAIN_MATCH


def bucket_label(value: str | None) -> str:
    game_family, market_type = split_bucket_key(value)
    game_label = GAME_FAMILY_LABELS.get(game_family, game_family.upper() if game_family else "")
    market_label = MARKET_TYPE_LABELS.get(market_type, market_type)
    return f"{game_label} {market_label}".strip()


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
    if len(text) >= 3 and text[-3] in "+-" and text[-2:].isdigit():
        text = f"{text}:00"
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


def event_category(event: dict[str, Any]) -> str | None:
    tags = event_tags(event)
    if tags & ESPORTS_CATEGORY_TAGS:
        return "esports"
    if tags & set(SPORTS_LEAGUE_TAGS):
        return "sports"
    return None


def event_league(event: dict[str, Any]) -> str:
    tags = event_tags(event)
    for tag, league in SPORTS_LEAGUE_TAGS.items():
        if tag in tags:
            return league
    return game_family_from_event(event)


def game_family_from_event(event: dict[str, Any]) -> str:
    tags = event_tags(event)
    title = (event.get("title") or "").lower()
    if "counter-strike-2" in tags or "cs2" in tags or "counter-strike" in title:
        return "cs2"
    if "dota-2" in tags or title.startswith("dota 2:"):
        return "dota2"
    if "league-of-legends" in tags or title.startswith("lol:"):
        return "lol"
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
    has_game_prefix = text.startswith(("dota 2:", "counter-strike:", "lol:"))
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


def normalize_market_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def normalized_team_names_from_event(event: dict[str, Any]) -> list[str]:
    title = str(event.get("title") or "")
    title = re.sub(r"^[^:]+:\s*", "", title)
    title = re.split(r"\s+-\s+", title, maxsplit=1)[0]
    title = re.sub(r"\s*\([^)]*\)\s*", " ", title)
    parts = re.split(r"\s+vs\.?\s+", title, flags=re.IGNORECASE)
    if len(parts) != 2:
        return []
    return [normalize_market_text(part) for part in parts if normalize_market_text(part)]


def market_outcomes_match_event_teams(event: dict[str, Any], market: dict[str, Any]) -> bool:
    event_teams = sorted(normalized_team_names_from_event(event))
    outcomes = parse_jsonish(market.get("outcomes"), [])
    market_teams = sorted(normalize_market_text(outcome) for outcome in outcomes if normalize_market_text(outcome))
    return len(event_teams) == 2 and event_teams == market_teams


def has_prop_like_outcomes(market: dict[str, Any]) -> bool:
    outcomes = parse_jsonish(market.get("outcomes"), [])
    normalized = {normalize_market_text(outcome) for outcome in outcomes if normalize_market_text(outcome)}
    prop_outcomes = {
        "over",
        "under",
        "yes",
        "no",
        "odd",
        "even",
        "higher",
        "lower",
    }
    return bool(normalized & prop_outcomes)


def is_prop_market_question(question: str) -> bool:
    text = normalize_market_text(question)
    prop_patterns = [
        r"\btotal\b",
        r"\bover\b",
        r"\bunder\b",
        r"\bo u\b",
        r"\bhandicap\b",
        r"\bspread\b",
        r"\bcorrect score\b",
        r"\bkill\b",
        r"\bkills\b",
        r"\bfirst blood\b",
        r"\broshan\b",
        r"\bbarracks\b",
        r"\btower\b",
        r"\brampage\b",
        r"\bultra kill\b",
        r"\bdaytime\b",
        r"\binning\b",
        r"\brun line\b",
        r"\brunline\b",
        r"\bfirst to score\b",
        r"\bstrikeout\b",
        r"\bstrikeouts\b",
        r"\bhome run\b",
        r"\brbi\b",
        r"\bhit\b",
        r"\bhits\b",
        r"\btotal bases\b",
        r"\bpoints\b",
        r"\brebounds\b",
        r"\bassists\b",
        r"\bdouble double\b",
        r"\btriple double\b",
        r"\bmethod of victory\b",
        r"\bdecision\b",
        r"\bko\b",
        r"\btko\b",
        r"\bsubmission\b",
        r"\bgo the distance\b",
        r"\bdistance\b",
        r"\bround\b",
        r"\bwins by\b",
    ]
    return any(re.search(pattern, text) for pattern in prop_patterns)


def is_numbered_winner_question(question_norm: str, prefix: str) -> bool:
    return bool(
        re.search(rf"\b{prefix}\s+[1-5]\b.*\bwinner\b", question_norm)
        or re.search(rf"\bwinner\b.*\b{prefix}\s+[1-5]\b", question_norm)
    )


def classify_market_type(event: dict[str, Any], market: dict[str, Any]) -> str | None:
    category = event_category(event)
    if category is None:
        return None
    if not market.get("conditionId") or not is_binary_market(market):
        return None
    question = str(market.get("question") or "")
    question_norm = normalize_market_text(question)
    if not question_norm or is_prop_market_question(question) or has_prop_like_outcomes(market):
        return None
    if category == "sports":
        if not market_outcomes_match_event_teams(event, market):
            return None
        return MAIN_MATCH

    game_family = game_family_from_event(event)
    if game_family not in ALLOWED_GAME_FAMILIES:
        return None
    event_title_norm = normalize_market_text(event.get("title"))
    if (question_norm == event_title_norm and is_main_match_title(str(event.get("title") or ""))) or (
        is_main_match_title(question or str(event.get("title") or ""))
        and not re.search(r"\b(game|map)\s+[1-5]\b", question_norm)
    ):
        return MAIN_MATCH
    if game_family in {"dota2", "lol"} and is_numbered_winner_question(question_norm, "game"):
        return GAME_WINNER
    if game_family == "cs2" and is_numbered_winner_question(question_norm, "map"):
        return MAP_WINNER
    return None


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


def event_to_market_records(
    event: dict[str, Any],
    *,
    allowed_market_types: set[str] | None = None,
) -> list[dict[str, Any]]:
    category = event_category(event)
    if category is None:
        return []
    league = event_league(event)
    records: dict[str, dict[str, Any]] = {}
    for market in event.get("markets") or []:
        market_type = classify_market_type(event, market)
        if not market_type or (allowed_market_types is not None and market_type not in allowed_market_types):
            continue
        end_date = (
            market.get("umaEndDate")
            or market.get("closedTime")
            or event.get("finishedTimestamp")
            or event.get("closedTime")
            or market.get("endDate")
            or event.get("endDate")
        )
        match_start_time = (
            market.get("eventStartTime")
            or event.get("startTime")
            or market.get("gameStartTime")
            or end_date
        )
        market_start_time = match_start_time
        condition_id = str(market.get("conditionId")).lower()
        records[condition_id] = {
            "condition_id": condition_id,
            "event_id": str(event.get("id") or ""),
            "event_slug": event.get("slug"),
            "title": event.get("title"),
            "question": market.get("question"),
            "outcomes": parse_jsonish(market.get("outcomes"), []),
            "outcome_prices": [to_float(v) for v in parse_jsonish(market.get("outcomePrices"), [])],
            # ERC1155 token ids per outcome — drives on-chain follow detection
            # (build_asset_map maps tokenId -> conditionId/outcomeIndex).
            "clob_token_ids": [str(v) for v in parse_jsonish(market.get("clobTokenIds"), []) if v],
            "end_date": end_date,
            "match_start_time": match_start_time,
            "market_start_time": market_start_time,
            "volume": to_float(market.get("volume") or event.get("volume")),
            "volume24hr": to_float(market.get("volume24hr") or event.get("volume24hr")),
            "liquidity": to_float(market.get("liquidity") or event.get("liquidity")),
            "closed": bool(event.get("closed")),
            "game_family": game_family_from_event(event),
            "category": category,
            "league": league,
            "league_label": LEAGUE_LABELS.get(league, league.upper() if league else ""),
            "market_type": market_type,
            "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    return sorted(
        records.values(),
        key=lambda row: (MARKET_TYPE_ORDER.get(str(row.get("market_type")), 99), -to_float(row.get("volume"))),
    )


def event_to_market_record(event: dict[str, Any]) -> dict[str, Any] | None:
    records = event_to_market_records(event, allowed_market_types={MAIN_MATCH})
    if records:
        return records[0]
    market = choose_main_market(event)
    if not market:
        return None
    market_type = classify_market_type(event, market)
    if market_type != MAIN_MATCH:
        return None
    return event_to_market_records(event, allowed_market_types={MAIN_MATCH})[0]


def build_classification_set(
    events: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_days: int | None = None,
    sports_event_min_volume: float = 0.0,
) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    records: dict[str, dict[str, Any]] = {}
    for event in events:
        for record in event_to_market_records(event):
            end = parse_dt(record.get("end_date"))
            if not end or end > now:
                continue
            if not is_settled_binary_prices(record.get("outcome_prices") or []):
                continue
            if record.get("category") == "sports" and to_float(record.get("volume")) < sports_event_min_volume:
                continue
            if lookback_days is not None:
                days_ago = (now - end).total_seconds() / SECONDS_PER_DAY
                if days_ago < 0 or days_ago > lookback_days:
                    continue
            records[record["condition_id"]] = record
    return sorted(
        records.values(),
        key=lambda row: (
            row.get("end_date") or "",
            -MARKET_TYPE_ORDER.get(str(row.get("market_type")), 99),
        ),
        reverse=True,
    )


def build_discovery_slate(
    classification_set: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookback_steps: tuple[int, ...] = (7, 14, 30),
    min_market_volume: float = 25_000,
    fallback_min_market_volume: float = 10_000,
    submarket_min_market_volume: float = 5_000,
    submarket_fallback_min_market_volume: float = 1_000,
    target_markets: int = 150,
    submarket_target_markets: int = 150,
    game_winner_target_markets: int | None = None,
    map_winner_target_markets: int | None = None,
    max_markets_per_run: int = 150,
    submarket_max_markets_per_run: int = 150,
    game_winner_max_markets_per_run: int | None = None,
    map_winner_max_markets_per_run: int | None = None,
    market_offset: int = 0,
    league_target_markets: dict[str, int] | None = None,
    league_min_market_volumes: dict[str, float] | None = None,
    league_fallback_min_market_volumes: dict[str, float] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    game_winner_target_markets = int(game_winner_target_markets if game_winner_target_markets is not None else submarket_target_markets)
    map_winner_target_markets = int(map_winner_target_markets if map_winner_target_markets is not None else submarket_target_markets)
    game_winner_max_markets_per_run = int(
        game_winner_max_markets_per_run if game_winner_max_markets_per_run is not None else submarket_max_markets_per_run
    )
    map_winner_max_markets_per_run = int(
        map_winner_max_markets_per_run if map_winner_max_markets_per_run is not None else submarket_max_markets_per_run
    )

    def select(
        days: int,
        min_volume: float,
        market_types: set[str],
        *,
        league: str | None = None,
        game_family: str | None = None,
    ) -> list[dict[str, Any]]:
        selected = []
        for market in classification_set:
            market_type = str(market.get("market_type") or MAIN_MATCH)
            if market_type not in market_types:
                continue
            if league is not None and str(market.get("league") or "").lower() != league:
                continue
            if game_family is not None and str(market.get("game_family") or "").lower() != game_family:
                continue
            end = parse_dt(market.get("end_date"))
            if not end:
                continue
            days_ago = (now - end).total_seconds() / SECONDS_PER_DAY
            if 0 <= days_ago <= days and to_float(market.get("volume")) >= min_volume:
                selected.append(market)
        if league is None:
            max_volume = max((to_float(row.get("volume")) for row in selected), default=0.0)

            def score_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
                end = parse_dt(row.get("end_date"))
                days_ago = (now - end).total_seconds() / SECONDS_PER_DAY if end else days
                volume = to_float(row.get("volume"))
                volume_norm = volume / max_volume if max_volume > 0 else 0.0
                recency_norm = 1 - min(max(days_ago, 0.0) / max(days, 1), 1)
                score = 0.70 * volume_norm + 0.30 * recency_norm
                end_ts = end.timestamp() if end else 0.0
                return (-score, -volume, -end_ts, str(row.get("condition_id") or ""))

            return sorted(selected, key=score_key)
        return sorted(
            selected,
            key=lambda row: (to_float(row.get("volume")), row.get("end_date") or ""),
            reverse=True,
        )

    def select_bucket(
        *,
        market_types: set[str],
        primary_min_volume: float,
        fallback_min_volume: float,
        target: int,
        league: str | None = None,
        game_family: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        selected_days = lookback_steps[-1]
        selected_min_volume = primary_min_volume
        for days in lookback_steps:
            selected = select(days, primary_min_volume, market_types, league=league, game_family=game_family)
            selected_days = days
            if len(selected) >= target:
                break
        if len(selected) < target:
            selected = select(lookback_steps[-1], fallback_min_volume, market_types, league=league, game_family=game_family)
            selected_min_volume = fallback_min_volume
        return selected, {
            "selected_lookback_days": selected_days,
            "selected_min_market_volume": selected_min_volume,
            "target_markets": target,
            "total_selected_market_count": len(selected),
        }

    def aggregate_metas(metas: list[dict[str, Any]], *, default_target: int, default_min_volume: float) -> dict[str, Any]:
        target_total = sum(int(meta.get("target_markets") or 0) for meta in metas)
        return {
            "selected_lookback_days": max(
                (int(meta.get("selected_lookback_days") or 0) for meta in metas),
                default=lookback_steps[-1],
            ),
            "selected_min_market_volume": min(
                (to_float(meta.get("selected_min_market_volume")) for meta in metas),
                default=default_min_volume,
            ),
            "target_markets": target_total if target_total > 0 else default_target,
            "total_selected_market_count": sum(int(meta.get("total_selected_market_count") or 0) for meta in metas),
        }

    league_metas: dict[str, dict[str, Any]] = {}
    league_selected: list[dict[str, Any]] = []
    if league_target_markets:
        for league, target in sorted(league_target_markets.items()):
            normalized_league = str(league or "").lower()
            if not normalized_league or int(target) <= 0:
                continue
            selected, meta = select_bucket(
                market_types={MAIN_MATCH},
                primary_min_volume=to_float((league_min_market_volumes or {}).get(normalized_league), min_market_volume),
                fallback_min_volume=to_float((league_fallback_min_market_volumes or {}).get(normalized_league), fallback_min_market_volume),
                target=int(target),
                league=normalized_league,
            )
            league_metas[normalized_league] = meta
            league_selected.extend(selected[: int(target)])
        main_selected = league_selected
        main_meta = {
            "selected_lookback_days": max(
                (int(meta.get("selected_lookback_days") or 0) for meta in league_metas.values()),
                default=lookback_steps[-1],
            ),
            "selected_min_market_volume": min(
                (to_float(meta.get("selected_min_market_volume")) for meta in league_metas.values()),
                default=min_market_volume,
            ),
            "target_markets": sum(int(meta.get("target_markets") or 0) for meta in league_metas.values()),
            "total_selected_market_count": sum(int(meta.get("total_selected_market_count") or 0) for meta in league_metas.values()),
        }
    else:
        esports_bucket_mode = any(
            str(row.get("category") or "") == "esports" and str(row.get("game_family") or "").lower() in ALLOWED_GAME_FAMILIES
            for row in classification_set
        )
        if esports_bucket_mode:
            bucket_selected_by_type: dict[str, list[dict[str, Any]]] = {
                MAIN_MATCH: [],
                GAME_WINNER: [],
                MAP_WINNER: [],
            }
            bucket_metas: dict[str, dict[str, Any]] = {}
            for key, limit in ESPORTS_DISCOVERY_GAME_MARKET_TYPE_LIMITS.items():
                game_family, market_type = split_bucket_key(key)
                is_main = market_type == MAIN_MATCH
                selected, meta = select_bucket(
                    market_types={market_type},
                    primary_min_volume=min_market_volume if is_main else submarket_min_market_volume,
                    fallback_min_volume=fallback_min_market_volume if is_main else submarket_fallback_min_market_volume,
                    target=limit,
                    game_family=game_family,
                )
                bucket_selected_by_type[market_type].extend(selected[:limit])
                bucket_metas[key] = {
                    **meta,
                    "game_family": game_family,
                    "market_type": market_type,
                    "max_markets_per_run": limit,
                }
            main_selected = bucket_selected_by_type[MAIN_MATCH]
            game_selected = bucket_selected_by_type[GAME_WINNER]
            map_selected = bucket_selected_by_type[MAP_WINNER]
            main_meta = aggregate_metas(
                [meta for key, meta in bucket_metas.items() if key.endswith(f":{MAIN_MATCH}")],
                default_target=target_markets,
                default_min_volume=min_market_volume,
            )
            game_meta = aggregate_metas(
                [meta for key, meta in bucket_metas.items() if key.endswith(f":{GAME_WINNER}")],
                default_target=game_winner_target_markets,
                default_min_volume=submarket_min_market_volume,
            )
            map_meta = aggregate_metas(
                [meta for key, meta in bucket_metas.items() if key.endswith(f":{MAP_WINNER}")],
                default_target=map_winner_target_markets,
                default_min_volume=submarket_min_market_volume,
            )
            max_markets_per_run = main_meta["target_markets"]
            game_winner_max_markets_per_run = game_meta["target_markets"]
            map_winner_max_markets_per_run = map_meta["target_markets"]
        else:
            main_selected, main_meta = select_bucket(
                market_types={MAIN_MATCH},
                primary_min_volume=min_market_volume,
                fallback_min_volume=fallback_min_market_volume,
                target=target_markets,
            )
            game_selected, game_meta = select_bucket(
                market_types={GAME_WINNER},
                primary_min_volume=submarket_min_market_volume,
                fallback_min_volume=submarket_fallback_min_market_volume,
                target=game_winner_target_markets,
            )
            map_selected, map_meta = select_bucket(
                market_types={MAP_WINNER},
                primary_min_volume=submarket_min_market_volume,
                fallback_min_volume=submarket_fallback_min_market_volume,
                target=map_winner_target_markets,
            )
            bucket_metas = {}
    if league_target_markets:
        game_selected, game_meta = select_bucket(
            market_types={GAME_WINNER},
            primary_min_volume=submarket_min_market_volume,
            fallback_min_volume=submarket_fallback_min_market_volume,
            target=game_winner_target_markets,
        )
        map_selected, map_meta = select_bucket(
            market_types={MAP_WINNER},
            primary_min_volume=submarket_min_market_volume,
            fallback_min_volume=submarket_fallback_min_market_volume,
            target=map_winner_target_markets,
        )
        bucket_metas = {}

    if league_target_markets:
        main_slice = main_selected
    else:
        main_slice = main_selected[market_offset : market_offset + max_markets_per_run]
    game_slice = game_selected[:game_winner_max_markets_per_run]
    map_slice = map_selected[:map_winner_max_markets_per_run]
    merged: dict[str, dict[str, Any]] = {}
    for market in [*main_slice, *game_slice, *map_slice]:
        merged[str(market.get("condition_id") or "").lower()] = market
    selected = list(merged.values())

    type_counts: dict[str, int] = {}
    selected_type_counts: dict[str, int] = {}
    selected_league_counts: dict[str, int] = {}
    selected_game_market_type_counts: dict[str, int] = {}
    for market in classification_set:
        market_type = str(market.get("market_type") or MAIN_MATCH)
        type_counts[market_type] = type_counts.get(market_type, 0) + 1
    for market in selected:
        market_type = str(market.get("market_type") or MAIN_MATCH)
        selected_type_counts[market_type] = selected_type_counts.get(market_type, 0) + 1
        league = str(market.get("league") or "").lower()
        if league:
            selected_league_counts[league] = selected_league_counts.get(league, 0) + 1
        game_family = str(market.get("game_family") or "").lower()
        if str(market.get("category") or "") == "esports" and game_family:
            key = bucket_key(game_family, market_type)
            selected_game_market_type_counts[key] = selected_game_market_type_counts.get(key, 0) + 1

    return selected, {
        "selected_lookback_days": main_meta["selected_lookback_days"],
        "selected_min_market_volume": main_meta["selected_min_market_volume"],
        "target_markets": main_meta["target_markets"] if league_target_markets else target_markets,
        "market_offset": market_offset,
        "max_markets_per_run": max_markets_per_run,
        "total_selected_market_count": (
            main_meta["total_selected_market_count"]
            + game_meta["total_selected_market_count"]
            + map_meta["total_selected_market_count"]
        ),
        "market_count": len(selected),
        "market_type_counts": type_counts,
        "selected_by_market_type": selected_type_counts,
        "selected_by_league": selected_league_counts,
        "selected_by_game_market_type": selected_game_market_type_counts,
        "game_market_buckets": bucket_metas,
        "main_match": main_meta,
        "leagues": league_metas,
        "submarkets": {
            "target_markets": game_winner_target_markets + map_winner_target_markets,
            "max_markets_per_run": game_winner_max_markets_per_run + map_winner_max_markets_per_run,
            "game_winner": {
                **game_meta,
                "target_markets": game_winner_target_markets,
                "max_markets_per_run": game_winner_max_markets_per_run,
            },
            "map_winner": {
                **map_meta,
                "target_markets": map_winner_target_markets,
                "max_markets_per_run": map_winner_max_markets_per_run,
            },
        },
    }


def trade_cash(trade: dict[str, Any]) -> float:
    return to_float(trade.get("cash")) or to_float(trade.get("size")) * to_float(trade.get("price"))


def build_candidate_wallets(
    trades_by_market: dict[str, list[dict[str, Any]]],
    *,
    market_type_by_id: dict[str, str] | None = None,
    market_game_family_by_id: dict[str, str] | None = None,
    market_end_times: dict[str, int] | None = None,
    market_start_times: dict[str, int] | None = None,
    min_trade_cash: float = 50,
    participation_threshold: int = 8,
    top_participation_count: int = 100,
    total_cash_threshold: float = 5_000,
    single_market_cash_threshold: float = 1_000,
    max_candidate_wallets: int = 300,
    candidate_wallets_per_market_type: int | None = None,
    candidate_wallets_per_game_family: int | None = None,
    candidate_game_family_thresholds: dict[str, dict[str, float]] | None = None,
    tail_entry_price_threshold: float = 0.75,
) -> list[dict[str, Any]]:
    wallets: dict[str, dict[str, Any]] = {}
    market_cash_by_wallet: dict[str, dict[str, float]] = {}
    market_size_by_wallet: dict[str, dict[str, float]] = {}
    market_buy_cash_by_wallet: dict[str, dict[str, float]] = {}
    market_buy_size_by_wallet: dict[str, dict[str, float]] = {}
    market_trade_counts_by_wallet: dict[str, dict[str, int]] = {}
    market_buy_outcomes_by_wallet: dict[str, dict[str, set[str]]] = {}
    market_last_trade_by_wallet: dict[str, dict[str, int]] = {}
    market_last_buy_by_wallet: dict[str, dict[str, int]] = {}
    market_type_by_id = {str(key).lower(): str(value) for key, value in (market_type_by_id or {}).items()}
    market_game_family_by_id = {
        str(key).lower(): str(value)
        for key, value in (market_game_family_by_id or {}).items()
        if value
    }
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
            side = str(trade.get("side") or trade.get("type") or "BUY").upper()
            outcome = str(trade.get("outcome") or trade.get("outcomeIndex") or "")
            if outcome and side == "BUY":
                wallet_market_buy_cash = market_buy_cash_by_wallet.setdefault(wallet, {})
                wallet_market_buy_cash[condition_id] = wallet_market_buy_cash.get(condition_id, 0.0) + cash
                if size > 0:
                    wallet_market_buy_size = market_buy_size_by_wallet.setdefault(wallet, {})
                    wallet_market_buy_size[condition_id] = wallet_market_buy_size.get(condition_id, 0.0) + size
                wallet_market_buy_outcomes = market_buy_outcomes_by_wallet.setdefault(wallet, {})
                wallet_market_buy_outcomes.setdefault(condition_id, set()).add(outcome)
                wallet_market_last_buy = market_last_buy_by_wallet.setdefault(wallet, {})
                wallet_market_last_buy[condition_id] = max(
                    wallet_market_last_buy.get(condition_id, 0),
                    to_int(trade.get("timestamp")),
                )
            wallet_market_last = market_last_trade_by_wallet.setdefault(wallet, {})
            wallet_market_last[condition_id] = max(
                wallet_market_last.get(condition_id, 0),
                to_int(trade.get("timestamp")),
            )

    def metrics_for_market_ids(wallet: str, market_ids: set[str], base_row: dict[str, Any]) -> dict[str, Any]:
        market_cash = market_cash_by_wallet.get(wallet, {})
        buy_market_cash = market_buy_cash_by_wallet.get(wallet, {})
        buy_market_size = market_buy_size_by_wallet.get(wallet, {})
        trade_counts = market_trade_counts_by_wallet.get(wallet, {})
        buy_outcome_sets = market_buy_outcomes_by_wallet.get(wallet, {})
        last_buys = market_last_buy_by_wallet.get(wallet, {})
        per_market = [market_cash.get(condition_id, 0.0) for condition_id in market_ids]
        two_sided_market_count = sum(
            1 for condition_id in market_ids if len(buy_outcome_sets.get(condition_id, set())) >= 2
        )
        high_churn_market_count = sum(1 for condition_id in market_ids if trade_counts.get(condition_id, 0) >= 20)
        last_entry_hours_to_start = []
        last_entry_hours_to_end = []
        tail_entry_market_count = 0
        for condition_id in market_ids:
            last_ts = last_buys.get(condition_id, 0)
            start_ts = market_start_times.get(condition_id) or market_end_times.get(condition_id)
            end_ts = market_end_times.get(condition_id)
            if start_ts and last_ts:
                hours = (start_ts - last_ts) / 3600
                last_entry_hours_to_start.append(hours)
                size = buy_market_size.get(condition_id, 0.0)
                avg_price = buy_market_cash.get(condition_id, 0.0) / size if size > 0 else 0.0
                if avg_price >= tail_entry_price_threshold:
                    tail_entry_market_count += 1
            if end_ts and last_ts:
                last_entry_hours_to_end.append((end_ts - last_ts) / 3600)
        total_cash_volume = sum(per_market)
        total_buy_cash = sum(buy_market_cash.get(condition_id, 0.0) for condition_id in market_ids)
        total_buy_size = sum(buy_market_size.get(condition_id, 0.0) for condition_id in market_ids)
        participated_market_count = len(market_ids)
        return {
            "participated_market_count": participated_market_count,
            "participated_market_ids": sorted(market_ids),
            "total_trade_count": sum(trade_counts.get(condition_id, 0) for condition_id in market_ids),
            "total_cash_volume": round(total_cash_volume, 6),
            "max_single_market_cash": round(max(per_market) if per_market else 0.0, 6),
            "avg_market_cash": round(total_cash_volume / participated_market_count, 6) if participated_market_count else 0.0,
            "two_sided_market_count": two_sided_market_count,
            "high_churn_market_count": high_churn_market_count,
            "late_entry_market_count": sum(1 for hours in last_entry_hours_to_start if hours < 2),
            "tail_entry_market_count": tail_entry_market_count,
            "early_entry_market_count": sum(1 for hours in last_entry_hours_to_start if hours >= 2),
            "avg_entry_price": round(total_buy_cash / total_buy_size, 8) if total_buy_size > 0 else 0.0,
            "median_last_entry_hours_to_start": round(median(last_entry_hours_to_start), 8)
            if last_entry_hours_to_start
            else 0.0,
            "median_last_entry_hours_to_end": round(median(last_entry_hours_to_end), 8)
            if last_entry_hours_to_end
            else 0.0,
            "last_seen_at": base_row["last_seen_at"],
        }

    rows = []
    for wallet, row in wallets.items():
        global_metrics = metrics_for_market_ids(wallet, set(row["participated_markets"]), row)
        per_type_candidate: dict[str, dict[str, Any]] = {}
        market_ids_by_type: dict[str, set[str]] = {}
        per_game_family_candidate: dict[str, dict[str, Any]] = {}
        market_ids_by_game_family: dict[str, set[str]] = {}
        per_game_type_candidate: dict[str, dict[str, Any]] = {}
        market_ids_by_game_type: dict[str, set[str]] = {}
        for condition_id in row["participated_markets"]:
            condition_key = str(condition_id).lower()
            market_type = market_type_by_id.get(condition_key, MAIN_MATCH)
            market_ids_by_type.setdefault(market_type, set()).add(condition_id)
            game_family = market_game_family_by_id.get(condition_key)
            if game_family:
                market_ids_by_game_family.setdefault(game_family, set()).add(condition_id)
                market_ids_by_game_type.setdefault(bucket_key(game_family, market_type), set()).add(condition_id)
        for market_type, market_ids in sorted(market_ids_by_type.items()):
            per_type_candidate[market_type] = metrics_for_market_ids(wallet, market_ids, row)
        for game_family, market_ids in sorted(market_ids_by_game_family.items()):
            per_game_family_candidate[game_family] = metrics_for_market_ids(wallet, market_ids, row)
        for key, market_ids in sorted(market_ids_by_game_type.items()):
            game_family, market_type = split_bucket_key(key)
            per_game_type_candidate[key] = {
                **metrics_for_market_ids(wallet, market_ids, row),
                "bucket_key": key,
                "bucket_label": bucket_label(key),
                "game_family": game_family,
                "game_family_label": GAME_FAMILY_LABELS.get(game_family, game_family.upper()),
                "market_type": market_type,
                "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
            }
        row_payload = {"wallet": wallet, **global_metrics, "per_type_candidate": per_type_candidate}
        if per_game_family_candidate:
            row_payload["per_game_family_candidate"] = per_game_family_candidate
        if per_game_type_candidate:
            row_payload["per_game_type_candidate"] = per_game_type_candidate
        rows.append(row_payload)

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
        per_game_family = (
            row.get("per_game_family_candidate")
            if isinstance(row.get("per_game_family_candidate"), dict)
            else {}
        )
        for game_family, thresholds in (candidate_game_family_thresholds or {}).items():
            metrics = per_game_family.get(game_family)
            if not isinstance(metrics, dict):
                continue
            if int(metrics.get("participated_market_count") or 0) < int(
                thresholds.get("min_participated_markets") or 0
            ):
                continue
            if to_float(metrics.get("avg_market_cash")) < to_float(thresholds.get("min_avg_market_cash")):
                continue
            reasons.append(f"{game_family}_qualified_size")
            break
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
    if (
        (candidate_wallets_per_market_type and candidate_wallets_per_market_type > 0)
        or (candidate_wallets_per_game_family and candidate_wallets_per_game_family > 0)
    ):
        by_wallet: dict[str, dict[str, Any]] = {}
        if candidate_wallets_per_market_type and candidate_wallets_per_market_type > 0:
            for market_type in sorted({key for row in candidates for key in (row.get("per_type_candidate") or {})}):
                bucket = [
                    row
                    for row in candidates
                    if isinstance((row.get("per_type_candidate") or {}).get(market_type), dict)
                ]
                bucket.sort(
                    key=lambda row: (
                        to_float(row["per_type_candidate"][market_type].get("max_single_market_cash")),
                        to_float(row["per_type_candidate"][market_type].get("total_cash_volume")),
                        int(row["per_type_candidate"][market_type].get("participated_market_count") or 0),
                    ),
                    reverse=True,
                )
                for row in bucket[:candidate_wallets_per_market_type]:
                    by_wallet.setdefault(row["wallet"], row)
        if candidate_wallets_per_game_family and candidate_wallets_per_game_family > 0:
            game_families = {
                key
                for row in candidates
                for key in (row.get("per_game_family_candidate") or {})
            }
            for game_family in sorted(game_families):
                bucket = [
                    row
                    for row in candidates
                    if isinstance((row.get("per_game_family_candidate") or {}).get(game_family), dict)
                ]
                bucket.sort(
                    key=lambda row: (
                        to_float(row["per_game_family_candidate"][game_family].get("max_single_market_cash")),
                        to_float(row["per_game_family_candidate"][game_family].get("total_cash_volume")),
                        int(row["per_game_family_candidate"][game_family].get("participated_market_count") or 0),
                    ),
                    reverse=True,
                )
                for row in bucket[:candidate_wallets_per_game_family]:
                    by_wallet.setdefault(row["wallet"], row)
        return [row for row in candidates if row["wallet"] in by_wallet]
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


def wilson_lower_bound(successes: int, n: int, z: float = WILSON_Z) -> float:
    if n <= 0:
        return 0.0
    p = successes / n
    denominator = 1 + z * z / n
    centre = p + z * z / (2 * n)
    adjustment = z * sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - adjustment) / denominator


def wilson_lower_bound_rate(p: float, n: float, z: float = WILSON_Z) -> float:
    """Wilson 下界,直接吃点估率 p 和(可为浮点的有效样本)n。
    用于桶级评分:p=θ̂(recency_weighted_win_rate),n=n_eff(Kish 有效样本)。"""
    if n <= 0:
        return 0.0
    denominator = 1 + z * z / n
    centre = p + z * z / (2 * n)
    adjustment = z * sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - adjustment) / denominator


def summarize_closed_positions(
    positions: list[dict[str, Any]],
    esports_condition_ids: set[str],
    *,
    condition_type_by_id: dict[str, str] | None = None,
    condition_game_family_by_id: dict[str, str] | None = None,
    now_ts: int | None = None,
    bot_like_score: int = 0,
    scoring_basis: str = "hold",
) -> dict[str, Any]:
    condition_type_by_id = {str(key).lower(): value for key, value in (condition_type_by_id or {}).items()}
    condition_game_family_by_id = {
        str(key).lower(): str(value)
        for key, value in (condition_game_family_by_id or {}).items()
        if value
    }
    rows = []
    neutral_market_count_by_type: dict[str, int] = {}
    neutral_market_count_by_game_type: dict[str, int] = {}
    for position in positions:
        condition_id = str(position.get("conditionId") or position.get("condition_id") or "").lower()
        if condition_id not in esports_condition_ids:
            continue
        market_type = condition_type_by_id.get(condition_id, MAIN_MATCH)
        game_family = condition_game_family_by_id.get(condition_id, "unknown")
        game_type_key = bucket_key(game_family, market_type)
        total_bought = to_float(position.get("totalBought") or position.get("total_bought"))
        hold_realized = to_float(position.get("realizedPnl") or position.get("realized_pnl"))
        if total_bought <= 0:
            continue
        avg_price = to_float(position.get("avgPrice") or position.get("avg_price"))
        cost_basis = total_bought * avg_price if avg_price > 0 else total_bought
        actual_pnl = to_float(position.get("actualPnl"), hold_realized)
        hold_pnl = to_float(position.get("holdPnl"), hold_realized)
        # scoring_basis 决定 win/pnl/roi 的口径:
        #   hold   = 押对结果、持有到结算(v1 默认,只奖励单向钱包)
        #   actual = 实际进出场盈亏(v2,容纳低买高卖的 technical 钱包)
        scoring_pnl = actual_pnl if scoring_basis == "actual" else hold_realized
        if scoring_pnl == 0:
            neutral_market_count_by_type[market_type] = neutral_market_count_by_type.get(market_type, 0) + 1
            neutral_market_count_by_game_type[game_type_key] = neutral_market_count_by_game_type.get(game_type_key, 0) + 1
            continue
        rows.append(
            {
                "condition_id": condition_id,
                "market_type": market_type,
                "game_family": game_family,
                "bucket_key": game_type_key,
                "pre_match_entry": position.get("preMatchEntry"),
                "total_bought": total_bought,
                "cost_basis": cost_basis,
                "realized_pnl": scoring_pnl,
                "actual_pnl": actual_pnl,
                "hold_pnl": hold_pnl,
                "profit_per_share": scoring_pnl / total_bought,
                "roi": scoring_pnl / cost_basis if cost_basis > 0 else 0.0,
                "avg_price": avg_price,
                "timestamp": to_int(position.get("timestamp")),
                "first_buy_won": position.get("firstBuyWon") if "firstBuyWon" in position else None,
                "material_two_sided": bool(position.get("materialTwoSided")),
            }
        )

    summary_now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())

    def summarize_bucket(bucket_rows: list[dict[str, Any]], *, neutral_market_count: int) -> dict[str, Any]:
        # 实质性双边市场无方向判断 → 当中性,从方向胜率/样本/edge 里剔除(套利者因此 n_eff→0 自动出局)。
        two_sided_count = sum(1 for row in bucket_rows if row.get("material_two_sided"))
        if two_sided_count:
            bucket_rows = [row for row in bucket_rows if not row.get("material_two_sided")]
            neutral_market_count += two_sided_count
        count = len(bucket_rows)
        total_bought = sum(row["total_bought"] for row in bucket_rows)
        total_cost = sum(row["cost_basis"] for row in bucket_rows)
        realized_pnl = sum(row["realized_pnl"] for row in bucket_rows)
        actual_pnl = sum(row["actual_pnl"] for row in bucket_rows)
        hold_pnl = sum(row["hold_pnl"] for row in bucket_rows)
        positive = sum(1 for row in bucket_rows if row["realized_pnl"] > 0)
        losses = count - positive
        last_trade = max((row["timestamp"] for row in bucket_rows), default=0)
        rois = [row["roi"] for row in bucket_rows]
        profits_per_share = [row["profit_per_share"] for row in bucket_rows]
        sizes = [row["total_bought"] for row in bucket_rows]
        costs = [row["cost_basis"] for row in bucket_rows]
        entry_prices = [row["avg_price"] for row in bucket_rows if row["avg_price"] > 0]
        high_price_entries = sum(1 for row in bucket_rows if row["avg_price"] >= 0.90)
        low_edge_profits = sum(1 for row in bucket_rows if 0 < row["roi"] <= 0.03)
        condition_ids = sorted({row["condition_id"] for row in bucket_rows})
        winning_cost = sum(row["cost_basis"] for row in bucket_rows if row["realized_pnl"] > 0)
        pre_match_rows = [row for row in bucket_rows if row.get("pre_match_entry") is not None]
        pre_match_entry_count = sum(1 for row in pre_match_rows if row.get("pre_match_entry"))
        first_direction_rows = [row for row in bucket_rows if row.get("first_buy_won") is not None]
        first_direction_wins = sum(1 for row in first_direction_rows if row.get("first_buy_won"))
        capital_weighted_entry_price = total_cost / total_bought if total_bought else 0.0
        capital_weighted_win_rate = winning_cost / total_cost if total_cost else 0.0

        # 时间衰减:近期盘权重高,陈旧盘指数衰减。a_i = 0.5^(age/half_life)。
        # n_eff = (Σa)²/Σa² (Kish 有效样本);θ̂ = Σ(a·win)/Σa(近期加权点估胜率)。
        half_life_s = ESPORTS_RECENCY_HALF_LIFE_DAYS * SECONDS_PER_DAY
        decay_w = [
            0.5 ** (max(0.0, summary_now_ts - row["timestamp"]) / half_life_s) if half_life_s > 0 else 1.0
            for row in bucket_rows
        ]
        weight_sum = sum(decay_w)
        weight_sq_sum = sum(w * w for w in decay_w)
        effective_sample = (weight_sum * weight_sum / weight_sq_sum) if weight_sq_sum > 0 else 0.0
        positive_weight = sum(w for w, row in zip(decay_w, bucket_rows) if row["realized_pnl"] > 0)
        recency_weighted_win_rate = positive_weight / weight_sum if weight_sum > 0 else 0.0

        # v17:评分三件套(θ̂/n_eff/median_entry)只在可跟价区(≤ SCORING_PRICE_CEILING)上重算 ——
        # 评分口径 = 跟单口径(跟单本就有现价上限)。纯大热买家(全部 >ceiling)→ n_eff→0 自动出局;
        # "高价淹没低价 edge"的钱包 → 中位价回落、edge 浮现。展示类字段(count/正胜率/资金加权)仍用全集。
        scoring_rows = [row for row in bucket_rows if 0 < row["avg_price"] <= SCORING_PRICE_CEILING]
        scoring_entry_prices = [row["avg_price"] for row in scoring_rows]
        if scoring_rows and half_life_s > 0:
            scoring_decay = [0.5 ** (max(0.0, summary_now_ts - row["timestamp"]) / half_life_s) for row in scoring_rows]
        else:
            scoring_decay = [1.0 for _ in scoring_rows]
        scoring_weight_sum = sum(scoring_decay)
        scoring_weight_sq_sum = sum(w * w for w in scoring_decay)
        scoring_effective_sample = (scoring_weight_sum ** 2 / scoring_weight_sq_sum) if scoring_weight_sq_sum > 0 else 0.0
        scoring_positive_weight = sum(w for w, row in zip(scoring_decay, scoring_rows) if row["realized_pnl"] > 0)
        scoring_win_rate = scoring_positive_weight / scoring_weight_sum if scoring_weight_sum > 0 else 0.0

        def recent_window(days: int) -> dict[str, Any]:
            cutoff = summary_now_ts - days * SECONDS_PER_DAY
            recent_rows = [row for row in bucket_rows if row["timestamp"] >= cutoff]
            recent_count = len(recent_rows)
            recent_cost = sum(row["cost_basis"] for row in recent_rows)
            recent_pnl = sum(row["realized_pnl"] for row in recent_rows)
            recent_positive = sum(1 for row in recent_rows if row["realized_pnl"] > 0)
            return {
                "market_count": recent_count,
                "roi": round(recent_pnl / recent_cost, 8) if recent_cost else 0.0,
                "positive_rate": round(recent_positive / recent_count, 8) if recent_count else 0.0,
                "pnl": round(recent_pnl, 6),
                "total_cost": round(recent_cost, 6),
            }

        recent_7d = recent_window(7)
        recent_14d = recent_window(14)
        if recent_7d["market_count"] >= 3:
            recent_bucket = recent_7d
            recent_window_days = 7
        elif recent_14d["market_count"] >= 3:
            recent_bucket = recent_14d
            recent_window_days = 14
        else:
            recent_bucket = recent_14d if recent_14d["market_count"] >= recent_7d["market_count"] else recent_7d
            recent_window_days = 0
        entry_price_buckets = {
            "<0.40": {"market_count": 0, "total_cost": 0.0, "win_count": 0, "hold_pnl": 0.0},
            "0.40-0.55": {"market_count": 0, "total_cost": 0.0, "win_count": 0, "hold_pnl": 0.0},
            "0.55-0.70": {"market_count": 0, "total_cost": 0.0, "win_count": 0, "hold_pnl": 0.0},
            ">=0.70": {"market_count": 0, "total_cost": 0.0, "win_count": 0, "hold_pnl": 0.0},
        }
        for row in bucket_rows:
            avg_price = row["avg_price"]
            if avg_price < 0.40:
                bucket_name = "<0.40"
            elif avg_price < 0.55:
                bucket_name = "0.40-0.55"
            elif avg_price < 0.70:
                bucket_name = "0.55-0.70"
            else:
                bucket_name = ">=0.70"
            bucket = entry_price_buckets[bucket_name]
            bucket["market_count"] += 1
            bucket["total_cost"] += row["cost_basis"]
            bucket["hold_pnl"] += row["hold_pnl"]
            if row["realized_pnl"] > 0:
                bucket["win_count"] += 1
        formatted_buckets = {}
        for bucket_name, bucket in entry_price_buckets.items():
            market_count = int(bucket["market_count"])
            formatted_buckets[bucket_name] = {
                "market_count": market_count,
                "total_cost": round(bucket["total_cost"], 6),
                "win_count": int(bucket["win_count"]),
                "win_rate": round(bucket["win_count"] / market_count, 8) if market_count else 0.0,
                "hold_pnl": round(bucket["hold_pnl"], 6),
            }
        return {
        "esports_closed_count": count,
        "neutral_market_count": neutral_market_count,
        "esports_win_count": positive,
        "esports_loss_count": losses,
        "esports_condition_ids": condition_ids,
        "esports_realized_pnl": round(realized_pnl, 6),
        "hold_pnl": round(hold_pnl, 6),
        "actual_pnl": round(actual_pnl, 6),
        "actual_minus_hold_pnl": round(actual_pnl - hold_pnl, 6),
        "actual_minus_hold_pnl_rate": round((actual_pnl - hold_pnl) / hold_pnl, 8) if hold_pnl > 0 else None,
        "esports_total_bought": round(total_bought, 6),
        "esports_total_cost": round(total_cost, 6),
        "avg_profit_per_share": round(realized_pnl / total_bought, 8) if total_bought else 0.0,
        "median_profit_per_share": round(median(profits_per_share), 8) if profits_per_share else 0.0,
        "esports_roi": round(realized_pnl / total_cost, 8) if total_cost else 0.0,
        "median_market_roi": round(median(rois), 8) if rois else 0.0,
        "positive_market_rate": round(positive / count, 8) if count else 0.0,
        "wilson_z": WILSON_Z,
        "wilson_win_rate_lower_bound": round(wilson_lower_bound(positive, count), 8),
        "recency_weighted_win_rate": round(scoring_win_rate, 8),          # v17:可跟价区(≤ceiling)θ̂,评分用
        "recency_weighted_win_rate_full": round(recency_weighted_win_rate, 8),  # 全价区(展示/审计)
        "effective_sample_size": round(scoring_effective_sample, 6),       # v17:可跟价区 n_eff,评分用
        "effective_sample_size_full": round(effective_sample, 6),          # 全价区(展示/审计)
        "scoring_market_count": len(scoring_rows),                         # v17:落在可跟价区的场数
        "recency_half_life_days": ESPORTS_RECENCY_HALF_LIFE_DAYS,
        "avg_position_size": round(total_bought / count, 6) if count else 0.0,
        "median_position_size": round(median(sizes), 6) if sizes else 0.0,
        "median_position_cost_usdc": round(median(costs), 6) if costs else 0.0,
        "avg_entry_price": round(sum(entry_prices) / len(entry_prices), 8) if entry_prices else 0.0,
        "median_entry_price": round(median(scoring_entry_prices), 8) if scoring_entry_prices else 0.0,  # v17:可跟价区中位价,评分用
        "median_entry_price_full": round(median(entry_prices), 8) if entry_prices else 0.0,
        "capital_weighted_entry_price": round(capital_weighted_entry_price, 8),
        "capital_weighted_win_rate": round(capital_weighted_win_rate, 8),
        "capital_weighted_edge": round(capital_weighted_win_rate - capital_weighted_entry_price, 8),
        "pre_match_entry_count": pre_match_entry_count,
        "pre_match_entry_market_count": len(pre_match_rows),
        "pre_match_entry_rate": round(pre_match_entry_count / len(pre_match_rows), 8) if pre_match_rows else None,
        "first_direction_market_count": len(first_direction_rows),
        "first_direction_win_count": first_direction_wins,
        "first_direction_win_rate": round(first_direction_wins / len(first_direction_rows), 8)
        if first_direction_rows
        else None,
        "entry_price_buckets": formatted_buckets,
        "high_price_entry_rate": round(high_price_entries / count, 8) if count else 0.0,
        "low_edge_profit_rate": round(low_edge_profits / count, 8) if count else 0.0,
        "last_esports_trade_at": last_trade,
        "recent_7d_market_count": recent_7d["market_count"],
        "recent_7d_roi": recent_7d["roi"],
        "recent_7d_positive_rate": recent_7d["positive_rate"],
        "recent_7d_pnl": recent_7d["pnl"],
        "recent_14d_market_count": recent_14d["market_count"],
        "recent_14d_roi": recent_14d["roi"],
        "recent_14d_positive_rate": recent_14d["positive_rate"],
        "recent_14d_pnl": recent_14d["pnl"],
        "recent_bucket_market_count": recent_bucket["market_count"],
        "recent_bucket_window_days": recent_window_days,
        "recent_bucket_roi": recent_bucket["roi"],
        "recent_bucket_positive_rate": recent_bucket["positive_rate"],
        "recent_bucket_pnl": recent_bucket["pnl"],
        "bot_like_score": bot_like_score,
        "profiled_at": summary_now_ts,
        "scoring_version": SCORING_VERSION,
        }

    per_type: dict[str, dict[str, Any]] = {}
    for market_type in sorted({row["market_type"] for row in rows} | set(neutral_market_count_by_type)):
        bucket_rows = [row for row in rows if row["market_type"] == market_type]
        per_type[market_type] = {
            **summarize_bucket(
                bucket_rows,
                neutral_market_count=neutral_market_count_by_type.get(market_type, 0),
            ),
            "market_type": market_type,
            "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
        }
    per_game_type: dict[str, dict[str, Any]] = {}
    for key in sorted({row["bucket_key"] for row in rows} | set(neutral_market_count_by_game_type)):
        game_family, market_type = split_bucket_key(key)
        bucket_rows = [row for row in rows if row["bucket_key"] == key]
        per_game_type[key] = {
            **summarize_bucket(
                bucket_rows,
                neutral_market_count=neutral_market_count_by_game_type.get(key, 0),
            ),
            "bucket_key": key,
            "bucket_label": bucket_label(key),
            "game_family": game_family,
            "game_family_label": GAME_FAMILY_LABELS.get(game_family, game_family.upper() if game_family else ""),
            "market_type": market_type,
            "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
        }
    summary = summarize_bucket(rows, neutral_market_count=sum(neutral_market_count_by_type.values()))
    return {
        **summary,
        "per_type": per_type,
        "per_game_type": per_game_type,
        "data_quality": {"source": "closed_positions", "reliable_losses": False},
    }


def winning_outcome_index(record: dict[str, Any]) -> int | None:
    prices = [to_float(value) for value in record.get("outcome_prices") or record.get("outcomePrices") or []]
    if not is_settled_binary_prices(prices):
        return None
    return max(range(len(prices)), key=lambda index: prices[index])


def reconstruct_closed_positions(
    trades: list[dict[str, Any]],
    market_records_by_id: dict[str, dict[str, Any]],
    *,
    material_sell_frac: float = 0.2,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    markets = {str(key).lower(): value for key, value in market_records_by_id.items()}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades or []:
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
        if condition_id in markets:
            grouped.setdefault(condition_id, []).append(trade)

    positions: list[dict[str, Any]] = []
    behavior_by_market: dict[str, dict[str, Any]] = {}
    for condition_id, market_trades in grouped.items():
        record = markets.get(condition_id) or {}
        winner_index = winning_outcome_index(record)
        if winner_index is None:
            continue
        outcomes = [str(value).lower() for value in record.get("outcomes") or []]

        buy_size_by_outcome: dict[int, float] = {}
        buy_cost_by_outcome: dict[int, float] = {}
        sell_size_by_outcome: dict[int, float] = {}
        sell_proceeds_by_outcome: dict[int, float] = {}
        material_sell_size_by_outcome: dict[int, float] = {}
        last_ts = 0
        last_buy_ts = 0
        first_buy_sort_key: tuple[int, str] | None = None
        first_buy_outcome_index: int | None = None
        for trade in market_trades:
            side = str(trade.get("side") or trade.get("type") or "").upper()
            if side not in {"BUY", "SELL"}:
                continue
            size = to_float(trade.get("size") or trade.get("amount"))
            price = to_float(trade.get("price") or trade.get("avgPrice") or trade.get("avg_price"))
            if size <= 0:
                continue
            cash = trade_cash(trade)
            outcome_index = to_int(trade.get("outcomeIndex"), -1)
            if outcome_index < 0:
                outcome = str(trade.get("outcome") or "").lower()
                outcome_index = outcomes.index(outcome) if outcome in outcomes else -1
            if outcome_index < 0:
                continue
            trade_ts = to_int(trade.get("timestamp"))
            last_ts = max(last_ts, trade_ts)
            if side == "BUY":
                last_buy_ts = max(last_buy_ts, trade_ts)
                sort_key = (trade_ts, str(trade.get("transactionHash") or trade.get("transaction_hash") or ""))
                if first_buy_sort_key is None or sort_key < first_buy_sort_key:
                    first_buy_sort_key = sort_key
                    first_buy_outcome_index = outcome_index
                buy_size_by_outcome[outcome_index] = buy_size_by_outcome.get(outcome_index, 0.0) + size
                buy_cost_by_outcome[outcome_index] = buy_cost_by_outcome.get(outcome_index, 0.0) + cash
            elif side == "SELL":
                sell_size_by_outcome[outcome_index] = sell_size_by_outcome.get(outcome_index, 0.0) + size
                sell_proceeds_by_outcome[outcome_index] = sell_proceeds_by_outcome.get(outcome_index, 0.0) + cash
                near_resolved_winner_exit = outcome_index == winner_index and price >= NEAR_RESOLVED_WINNER_SELL_PRICE
                if not near_resolved_winner_exit:
                    material_sell_size_by_outcome[outcome_index] = (
                        material_sell_size_by_outcome.get(outcome_index, 0.0) + size
                    )

        bought_outcomes = {outcome for outcome, size in buy_size_by_outcome.items() if size > 0}
        if not bought_outcomes:
            continue
        incomplete_position = any(
            sell_size > buy_size_by_outcome.get(outcome_index, 0.0) + 0.000001
            for outcome_index, sell_size in sell_size_by_outcome.items()
        )
        if incomplete_position:
            continue
        has_material_sell = any(
            buy_size_by_outcome.get(outcome_index, 0.0) > 0
            and sell_size / buy_size_by_outcome.get(outcome_index, 0.0) > material_sell_frac
            for outcome_index, sell_size in material_sell_size_by_outcome.items()
        )
        two_sided = len(bought_outcomes) >= 2
        total_bought = sum(buy_size_by_outcome.values())
        # 少数侧(非主仓)买量占比 ≥ 阈值 → 实质性双边(对冲/套利,无方向)。
        minority_buy = total_bought - (max(buy_size_by_outcome.values()) if buy_size_by_outcome else 0.0)
        material_two_sided = (
            two_sided and total_bought > 0
            and (minority_buy / total_bought) >= MATERIAL_TWO_SIDED_MIN_MINORITY_FRAC
        )
        buy_cost = sum(buy_cost_by_outcome.values())
        sell_proceeds = sum(sell_proceeds_by_outcome.values())
        net_cost = buy_cost - sell_proceeds
        net_position_by_outcome = {
            outcome: buy_size_by_outcome.get(outcome, 0.0) - sell_size_by_outcome.get(outcome, 0.0)
            for outcome in set(buy_size_by_outcome) | set(sell_size_by_outcome)
        }
        actual_payout = max(0.0, net_position_by_outcome.get(winner_index, 0.0))
        actual_pnl = actual_payout - net_cost
        hold_payout = max(0.0, buy_size_by_outcome.get(winner_index, 0.0))
        hold_pnl = hold_payout - buy_cost
        actual_minus_hold_pnl = actual_pnl - hold_pnl
        avg_price = buy_cost / total_bought if total_bought > 0 else 0.0
        dominant_outcome = max(buy_size_by_outcome, key=lambda outcome: buy_size_by_outcome[outcome])
        # Followability: was the wallet's last BUY placed before the match started? None when
        # the market has no known start time (excluded from the pre_match_entry_rate denom).
        match_start_dt = parse_dt(record.get("match_start_time") or record.get("market_start_time"))
        pre_match_entry: bool | None = None
        if match_start_dt and last_buy_ts > 0:
            pre_match_entry = last_buy_ts < int(match_start_dt.timestamp())
        positions.append(
            {
                "conditionId": condition_id,
                "outcomeIndex": dominant_outcome,
                "preMatchEntry": pre_match_entry,
                "totalBought": round(total_bought, 8),
                "avgPrice": round(avg_price, 8),
                "realizedPnl": round(hold_pnl, 8),
                "holdPnl": round(hold_pnl, 8),
                "actualPnl": round(actual_pnl, 8),
                "actualMinusHoldPnl": round(actual_minus_hold_pnl, 8),
                "actualMinusHoldPnlRate": round(actual_minus_hold_pnl / hold_pnl, 8) if hold_pnl > 0 else None,
                "timestamp": last_ts,
                "netCost": round(net_cost, 8),
                "buyCost": round(buy_cost, 8),
                "sellProceeds": round(sell_proceeds, 8),
                "holdPayout": round(hold_payout, 8),
                "actualPayout": round(actual_payout, 8),
                "netPositionByOutcome": {
                    str(outcome): round(size, 8)
                    for outcome, size in sorted(net_position_by_outcome.items())
                    if abs(size) > 0.000001
                },
                "winningOutcomeIndex": winner_index,
                "firstBuyOutcomeIndex": first_buy_outcome_index,
                "firstBuyWon": first_buy_outcome_index == winner_index if first_buy_outcome_index is not None else None,
                "soldBeforeResolution": has_material_sell,
                "twoSidedTrade": two_sided,
                "materialTwoSided": material_two_sided,
            }
        )
        behavior_by_market[condition_id] = {
            "condition_id": condition_id,
            "sold_before_resolution": has_material_sell,
            "two_sided": two_sided,
            "material_two_sided": material_two_sided,
            "buy_size_by_outcome": {str(k): round(v, 8) for k, v in sorted(buy_size_by_outcome.items())},
            "sell_size_by_outcome": {str(k): round(v, 8) for k, v in sorted(sell_size_by_outcome.items())},
            "material_sell_size_by_outcome": {
                str(k): round(v, 8) for k, v in sorted(material_sell_size_by_outcome.items())
            },
            "first_buy_outcome_index": first_buy_outcome_index,
            "first_buy_won": first_buy_outcome_index == winner_index if first_buy_outcome_index is not None else None,
        }
    return positions, behavior_by_market


def summarize_trade_reconstructed_positions(
    trades: list[dict[str, Any]],
    market_records_by_id: dict[str, dict[str, Any]],
    *,
    now_ts: int | None = None,
    bot_like_score: int = 0,
    material_sell_frac: float = 0.2,
    scoring_basis: str = "hold",
) -> dict[str, Any]:
    markets = {str(key).lower(): value for key, value in market_records_by_id.items()}
    categories = {str(record.get("category") or "") for record in markets.values() if record.get("category")}
    category = next(iter(categories)) if len(categories) == 1 else None
    positions, behavior_by_market = reconstruct_closed_positions(
        trades,
        markets,
        material_sell_frac=material_sell_frac,
    )
    # 待结算市场:钱包买过、且在册(in scope),但市场尚无 outcome_prices(未结算)→
    # reconstruct 会因 winner_index is None 跳过它。这类市场结算后会改变评分(可能新增亏损),
    # 故画像此刻是"不完整"的。surfacing 这个计数,让缓存层据此拒绝复用、下轮重评 —— 修复
    # "钱包在结算落库前一刻被打分、清仓亏损被永久漏计"的时间竞态。
    bought_conditions = {
        str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
        for trade in (trades or [])
        if str(trade.get("side") or trade.get("type") or "").upper() == "BUY"
    }
    pending_resolution_market_count = sum(
        1
        for condition_id in bought_conditions
        if condition_id in markets and winning_outcome_index(markets[condition_id]) is None
    )
    esports_condition_ids = set(markets)
    condition_type_by_id = {
        condition_id: str(record.get("market_type") or MAIN_MATCH)
        for condition_id, record in markets.items()
    }
    condition_game_family_by_id = {
        condition_id: str(record.get("game_family") or "unknown")
        for condition_id, record in markets.items()
    }
    summary = summarize_closed_positions(
        positions,
        esports_condition_ids,
        condition_type_by_id=condition_type_by_id,
        condition_game_family_by_id=condition_game_family_by_id,
        now_ts=now_ts,
        bot_like_score=bot_like_score,
        scoring_basis=scoring_basis,
    )
    behavior_market_count = len(behavior_by_market)
    sold_before_resolution_market_count = sum(1 for row in behavior_by_market.values() if row.get("sold_before_resolution"))
    # 钱包级"系统性双边"排除也用 material(与方向胜率口径一致):频繁小对冲不触发,真套利才触发。
    two_sided_trade_market_count = sum(1 for row in behavior_by_market.values() if row.get("material_two_sided"))

    per_type_behavior: dict[str, dict[str, int]] = {}
    per_game_type_behavior: dict[str, dict[str, int]] = {}
    for condition_id, behavior_row in behavior_by_market.items():
        market_type = condition_type_by_id.get(condition_id, MAIN_MATCH)
        game_type_key = bucket_key(condition_game_family_by_id.get(condition_id, "unknown"), market_type)
        bucket = per_type_behavior.setdefault(
            market_type,
            {
                "historical_trade_behavior_market_count": 0,
                "sold_before_resolution_market_count": 0,
                "two_sided_trade_market_count": 0,
            },
        )
        bucket["historical_trade_behavior_market_count"] += 1
        if behavior_row.get("sold_before_resolution"):
            bucket["sold_before_resolution_market_count"] += 1
        if behavior_row.get("material_two_sided"):
            bucket["two_sided_trade_market_count"] += 1
        game_bucket = per_game_type_behavior.setdefault(
            game_type_key,
            {
                "historical_trade_behavior_market_count": 0,
                "sold_before_resolution_market_count": 0,
                "two_sided_trade_market_count": 0,
            },
        )
        game_bucket["historical_trade_behavior_market_count"] += 1
        if behavior_row.get("sold_before_resolution"):
            game_bucket["sold_before_resolution_market_count"] += 1
        if behavior_row.get("material_two_sided"):
            game_bucket["two_sided_trade_market_count"] += 1

    per_type = dict(summary.get("per_type") or {})
    per_game_type = dict(summary.get("per_game_type") or {})
    for market_type, behavior in per_type_behavior.items():
        behavior_count = to_int(behavior.get("historical_trade_behavior_market_count"))
        per_type[market_type] = {
            **(per_type.get(market_type) or {}),
            **behavior,
            "sold_before_resolution_market_rate": round(
                to_int(behavior.get("sold_before_resolution_market_count")) / behavior_count,
                8,
            )
            if behavior_count
            else 0.0,
            "two_sided_trade_market_rate": round(
                to_int(behavior.get("two_sided_trade_market_count")) / behavior_count,
                8,
            )
            if behavior_count
            else 0.0,
        }
    for key, metrics in list(per_game_type.items()):
        if not isinstance(metrics, dict):
            continue
        behavior = per_game_type_behavior.get(key)
        if not isinstance(behavior, dict):
            continue
        behavior_count = to_int(behavior.get("historical_trade_behavior_market_count"))
        per_game_type[key] = {
            **metrics,
            **behavior,
            "sold_before_resolution_market_rate": round(
                to_int(behavior.get("sold_before_resolution_market_count")) / behavior_count,
                8,
            )
            if behavior_count
            else 0.0,
            "two_sided_trade_market_rate": round(
                to_int(behavior.get("two_sided_trade_market_count")) / behavior_count,
                8,
            )
            if behavior_count
            else 0.0,
        }
    return {
        **summary,
        **({"category": category} if category else {}),
        "data_quality": {"source": "trade_reconstruction", "reliable_losses": True},
        "trade_reconstructed_sample_count": summary.get("esports_closed_count", 0),
        "per_type": per_type,
        "historical_trade_behavior_market_count": behavior_market_count,
        "sold_before_resolution_market_count": sold_before_resolution_market_count,
        "sold_before_resolution_market_rate": round(sold_before_resolution_market_count / behavior_market_count, 8)
        if behavior_market_count
        else 0.0,
        "two_sided_trade_market_count": two_sided_trade_market_count,
        "two_sided_trade_market_rate": round(two_sided_trade_market_count / behavior_market_count, 8)
        if behavior_market_count
        else 0.0,
        "per_game_type": per_game_type,
        "pending_resolution_market_count": pending_resolution_market_count,
    }


def summarize_historical_trade_behavior(
    condition_ids: Iterable[str],
    *,
    historical_trades_loader,
    condition_type_by_id: dict[str, str] | None = None,
    material_sell_frac: float = 0.2,
) -> dict[str, Any]:
    condition_type_by_id = {str(key).lower(): value for key, value in (condition_type_by_id or {}).items()}
    sold_before_resolution_market_count = 0
    two_sided_trade_market_count = 0
    behavior_market_count = 0
    per_type_counts: dict[str, dict[str, int]] = {}
    for condition_id in sorted({str(value).lower() for value in condition_ids if value}):
        trades = historical_trades_loader(condition_id)
        market_type = condition_type_by_id.get(condition_id, MAIN_MATCH)
        bucket = per_type_counts.setdefault(
            market_type,
            {
                "historical_trade_behavior_market_count": 0,
                "sold_before_resolution_market_count": 0,
                "two_sided_trade_market_count": 0,
            },
        )
        behavior_market_count += 1
        bucket["historical_trade_behavior_market_count"] += 1
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
            bucket["sold_before_resolution_market_count"] += 1
        # 实质性双边:买了≥2个结果且少数侧买量占比 ≥ 阈值(对冲/套利),而非单边分批。
        total_buy = sum(buy_size_by_outcome.values())
        minority_buy = total_buy - (max(buy_size_by_outcome.values()) if buy_size_by_outcome else 0.0)
        if (
            len(buy_size_by_outcome) >= 2 and total_buy > 0
            and (minority_buy / total_buy) >= MATERIAL_TWO_SIDED_MIN_MINORITY_FRAC
        ):
            two_sided_trade_market_count += 1
            bucket["two_sided_trade_market_count"] += 1

    per_type = {}
    for market_type, counts in per_type_counts.items():
        count = counts["historical_trade_behavior_market_count"]
        per_type[market_type] = {
            **counts,
            "sold_before_resolution_market_rate": round(
                counts["sold_before_resolution_market_count"] / count,
                8,
            )
            if count
            else 0.0,
            "two_sided_trade_market_rate": round(counts["two_sided_trade_market_count"] / count, 8)
            if count
            else 0.0,
        }

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
        "per_type_trade_behavior": per_type,
    }


def classify_wallet_bucket(
    summary: dict[str, Any],
    *,
    now_ts: int,
    min_sample: int = 8,
    min_followable_sample: int | None = None,
    n_eff_anchor: int | None = None,
    thin_edge_min: float = SCOPE_NEFF_THIN_EDGE_MIN,
    thin_wr_min: float = SCOPE_NEFF_THIN_WR_MIN,
) -> dict[str, Any]:
    """n_eff_anchor: 该桶的满严格 n_eff 锚点(10/8/7)。给定时启用薄样本附加门——
    full n_eff 落在 [min_sample, n_eff_anchor) 的桶须额外满足 edge_lb≥thin_edge_min 且
    θ̂≥thin_wr_min 才给 A/B。None = 不启用(默认,保持旧行为)。"""
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
    category = str(summary.get("category") or "").lower()
    is_sports = category == "sports"
    min_roi = SPORTS_MIN_ROI if is_sports else ESPORTS_MIN_ROI
    # capital_weighted_edge is the skill axis for both categories: it credits wallets that
    # win more (capital-weighted) than their entry price implied, so high-win-rate
    # favorite-buyers (low roi, negative entry_edge) are no longer wrongly cut.
    capital_weighted_edge = to_float(summary.get("capital_weighted_edge"))
    edge_value = capital_weighted_edge
    weak_edge_reason = "weak_capital_weighted_edge"
    actual_minus_hold_rate = to_float(summary.get("actual_minus_hold_pnl_rate"))
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
    # NOTE: sold_before_resolution is NOT a hard exclude anymore. Under hold-to-settlement
    # scoring, selling is already handled (profit-takers scored on their hold win,
    # loss-cutters scored on the full hold loss). A high sold rate alone — e.g. a wallet
    # that frees capital by selling winners at ~0.99 — is fine. We only soft-flag wallets
    # whose profit genuinely depends on in-game selling (actual >> hold). See below.
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
    # 注:不按"成交笔数"(high_churn)排除——分批建仓/分批止盈是单边方向性交易,合理。
    # 真正要排除的双边套利(同时买 A 和 B)已由上面的 two_sided 门槛精准抓住
    # (two_sided = 买了≥2个不同结果),与成交频率无关。
    # Copy 轴 v16:点估 → 下界。质量门 = wilson_lb ≥ W_min 且 edge_lb ≥ E_min 且 n_eff ≥ floor。
    #   θ̂(win_rate) = recency_weighted_win_rate(缺则回退 positive_market_rate);n_eff = effective_sample_size(缺则 count)。
    #   薄样本 → Wilson 区间变宽 → 下界自动掉下去 → 蒙中者出局,不再单设 n_eff/价带硬门。
    # v18 解耦:edge_lb 的 Wilson 用「可跟价区(≤0.85)子集」的 θ̂/n_eff(薄子集自动被 Wilson 惩罚);
    #          n_eff 数值地板改用「全样本」eff_sample_full —— 只验"是不是真活跃钱包",不重复惩罚薄子集。
    #          这样"全样本够活跃 + 低价子集 edge 经 Wilson 确认"的钱包能上,纯大热买家(子集为空)仍被 edge_lb 挡。
    win_rate = to_float(summary.get("recency_weighted_win_rate")) if summary.get("recency_weighted_win_rate") is not None else positive_rate
    eff_sample = to_float(summary.get("effective_sample_size")) if summary.get("effective_sample_size") is not None else float(count)  # 可跟价区子集 n_eff
    eff_sample_full = to_float(summary.get("effective_sample_size_full")) if summary.get("effective_sample_size_full") is not None else eff_sample  # 全样本 n_eff
    bucket_wilson_lb = wilson_lower_bound_rate(win_rate, eff_sample)   # 在可跟价区子集上算(薄→宽→自动扣)
    bucket_edge_lb = (bucket_wilson_lb - median_entry) if median_entry > 0 else None  # 悲观胜率下每股 edge
    copy_edge = win_rate - median_entry if median_entry > 0 else None                 # 点估 edge,仅展示
    min_edge_lb = SPORTS_EDGE_LB_MIN if is_sports else ESPORTS_EDGE_LB_MIN
    min_win_rate = SPORTS_MIN_BUCKET_WIN_RATE if is_sports else ESPORTS_MIN_BUCKET_WIN_RATE  # v20:桶内胜率硬门
    min_eff = float(min_sample)   # n_eff 数值兜底(esports=12),作用于全样本 eff_sample_full
    default_min_sub = SPORTS_SUBSET_MIN_SAMPLE if is_sports else ESPORTS_SUBSET_MIN_SAMPLE
    min_sub = float(default_min_sub if min_followable_sample is None else min_followable_sample)
    min_sub = max(1.0, min_sub)

    if eff_sample_full < min_eff:
        reasons.append("thin_sample")
    if eff_sample < min_sub:
        reasons.append("thin_followable_subset")  # v19:可跟价区子集太薄,不据此判桶
    if bucket_edge_lb is None or bucket_edge_lb < min_edge_lb:
        reasons.append("weak_edge_lb")
    if win_rate < min_win_rate:
        reasons.append("win_rate_below_floor")  # v20:桶内 θ̂ < 门(默认 0.58,见 ESPORTS_MIN_BUCKET_WIN_RATE)→ 硬排除
    # 以下全是软 reason(仅展示/观测,不参与判定)。bucket_wilson_lb 不再单设门 —— 它已隐含在
    # edge_lb(= wilson_lb − 入场中位价)里,只作为字段留存展示,不重复扣一道独立胜率门。
    if loss_count > 0:
        reasons.append("has_losses")
    if pnl <= 0:
        reasons.append("negative_pnl")  # 目标自己亏钱(常因把大注押在输的盘),均仓跟单与我们无关
    if capital_weighted_edge <= 0:
        reasons.append(weak_edge_reason)
    if historical_roi < min_roi:
        reasons.append("low_roi")
    if actual_minus_hold_rate > SWING_DEPENDENT_RATE:
        reasons.append("swing_dependent")  # 利润靠盘中卖出,我们复制不了
    if total_bought < 1_000:
        reasons.append("low_volume")

    edge_ok = bucket_edge_lb is not None
    # 全价区的方向能力置信下界。生产 summary 已带 raw Wilson；旧缓存/单测缺字段时，
    # 用全价区近期加权胜率 + full n_eff 重建，避免动态门因兼容字段缺失失效。
    full_win_rate = (
        to_float(summary.get("recency_weighted_win_rate_full"))
        if summary.get("recency_weighted_win_rate_full") is not None
        else positive_rate if summary.get("positive_market_rate") is not None
        else win_rate
    )
    full_wilson_lb = (
        to_float(summary.get("wilson_win_rate_lower_bound"))
        if summary.get("wilson_win_rate_lower_bound") is not None
        else wilson_lower_bound_rate(full_win_rate, eff_sample_full)
    )
    # v21 薄样本附加门:桶 full n_eff 落在 [min_eff, n_eff_anchor)(只因缓冲缩放才够样本)→
    #   要求更强信号(edge_lb ≥ thin_edge_min 且 θ̂ ≥ thin_wr_min)。锚点未给时恒 True(旧行为)。
    thin_ok = (
        n_eff_anchor is None
        or eff_sample_full >= n_eff_anchor
        or (edge_ok and bucket_edge_lb >= thin_edge_min and win_rate >= thin_wr_min)
    )
    if n_eff_anchor is not None and eff_sample_full < n_eff_anchor and not thin_ok:
        reasons.append("thin_underqualified")
    # 动态 subset floor 允许低频钱包进入候选，但低于严格锚点的价格切片必须是高胜率专精：
    # 完整历史也要稳、保守 edge 也要高。这使 10-0 / 7 个评分价格市场的真强钱包能上，
    # 但“全历史一般，恰好切出几场连胜”仍会被 full Wilson 拦住。
    subset_is_thin = n_eff_anchor is not None and eff_sample < float(n_eff_anchor)
    thin_specialist_ok = (
        not subset_is_thin
        or (
            edge_ok
            and bucket_edge_lb >= SCOPE_SUBSET_THIN_EDGE_MIN
            and win_rate >= SCOPE_SUBSET_THIN_WR_MIN
            and full_wilson_lb >= SCOPE_SUBSET_THIN_FULL_WILSON_MIN
        )
    )
    if subset_is_thin and not thin_specialist_ok:
        reasons.append("thin_followable_underqualified")
    # v17:edge 是唯一质量轴。bot>=70/系统性双边已在上方提前 return excluded;
    # 此处只判 edge_lb(内含 Wilson 置信)+ n_eff 兜底 + 新鲜度,不再卡独立胜率门。
    if (
        eff_sample_full >= min_eff   # v18:地板用全样本(活跃度);edge_lb 已含子集 Wilson 置信
        and eff_sample >= min_sub    # v19:可跟价区子集 ≥6,防太薄切片侥幸
        and edge_ok and bucket_edge_lb >= min_edge_lb
        and win_rate >= min_win_rate  # v20:桶内胜率硬门(默认 0.58,见 ESPORTS_MIN_BUCKET_WIN_RATE)
        and thin_ok                   # v21:薄样本(贴放松地板)须更强信号
        and thin_specialist_ok        # 薄价格子集须高胜率 + 全历史置信 + 高 edge
        and not stale
    ):
        grade = "A"
    elif (
        eff_sample_full >= min_eff
        and eff_sample >= min_sub
        and edge_ok and bucket_edge_lb >= (min_edge_lb - GRADE_B_EDGE_RELAX)
        and win_rate >= min_win_rate  # v20:B 同样要过胜率门(只放宽 edge,不放宽胜率)
        and thin_ok                   # v21:薄样本同样要过附加门
        and thin_specialist_ok
        and not stale
    ):
        grade = "B"
    elif stale:
        grade = "stale"
    else:
        grade = "C"
    state = "qualified" if grade in {"A", "B"} else "stale" if grade == "stale" else "unqualified"
    return {
        **summary,
        "entry_edge": round(entry_edge, 8),
        "bucket_win_rate": round(win_rate, 8),                       # θ̂ 近期加权点估胜率
        "bucket_eff_sample": round(eff_sample, 4),                   # n_eff 有效样本
        "bucket_wilson_lb": round(bucket_wilson_lb, 8),             # Wilson 下界(仅展示;已隐含于 edge_lb)
        "bucket_full_wilson_lb": round(full_wilson_lb, 8),          # 全价区方向能力下界
        "bucket_copy_edge": round(copy_edge, 8) if copy_edge is not None else None,        # 点估 edge(展示)
        "bucket_edge_lb": round(bucket_edge_lb, 8) if bucket_edge_lb is not None else None,  # edge 下界(质量门:edge_lb≥min)
        "min_followable_sample": round(min_sub, 4),
        "thin_specialist": bool(subset_is_thin and thin_specialist_ok),
        "grade": grade,
        "profile_state": state,
        "reasons": reasons,
    }


def wallet_bucket_min_sample(
    category: str,
    market_type: str,
    *,
    game_family: str | None = None,
    n_eff_floors: dict[str, int] | None = None,
) -> int:
    """n_eff 下限。默认全局(esports=12 / sports=12);若给 per-game 校准 map 且该桶有 game_family,
    用该游戏的自适应地板(scope-adaptive,见 review/scope-adaptive-calibration.md)。
    per_type 跨游戏桶(无 game_family)仍用全局默认 —— 它聚合多游戏、样本充足,不放宽。"""
    if str(category or "").lower() == "sports":
        return SPORTS_N_EFF_FLOOR
    if n_eff_floors and game_family and game_family in n_eff_floors:
        return int(n_eff_floors[game_family])
    return ESPORTS_N_EFF_FLOOR


def classify_wallet(
    summary: dict[str, Any],
    *,
    now_ts: int | None = None,
    n_eff_floors: dict[str, int] | None = None,
    n_eff_subset_floors: dict[str, int] | None = None,
    n_eff_anchors: dict[str, int] | None = None,
) -> dict[str, Any]:
    """n_eff_floors: per-game n_eff 地板 map(game_family→floor),来自 scope 校准;
    None=全局默认。只影响 per_game_type 桶(boarding 路径);per_type 跨游戏桶仍用全局默认。
    n_eff_subset_floors: per-game 评分价格子集地板 map；生产路径来自同一份 scope 校准，
    避免全局固定门覆盖市场密度自适应。n_eff_anchors: per-game 满严格锚点 map,
    给定时对 per_game_type 桶启用薄样本附加门
    (full n_eff < 锚点须更强 edge/胜率);只对显式给了锚点的游戏生效,其余不加门。"""
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    category = str(summary.get("category") or "").lower()
    overall_min_sample = SPORTS_N_EFF_FLOOR if category == "sports" else ESPORTS_N_EFF_FLOOR
    classified = classify_wallet_bucket(summary, now_ts=now_ts, min_sample=overall_min_sample)
    per_type = summary.get("per_type") or {}
    per_game_type = summary.get("per_game_type") or {}
    if not isinstance(per_type, dict):
        return classified

    per_type_grades: dict[str, dict[str, Any]] = {}
    per_game_type_grades: dict[str, dict[str, Any]] = {}
    eligible_market_types: list[str] = []
    eligible_buckets: list[str] = []
    eligible_market_type_modes: dict[str, str] = {}
    eligible_bucket_modes: dict[str, str] = {}
    grade_rank = {"A": 5, "B": 4, "C": 3, "stale": 2, "excluded": 1, "unknown": 0}
    best_grade = classified.get("grade") or "unknown"
    best_rank = grade_rank.get(str(best_grade), 0)
    for market_type, bucket_summary in sorted(
        per_type.items(),
        key=lambda item: MARKET_TYPE_ORDER.get(str(item[0]), 99),
    ):
        min_sample = wallet_bucket_min_sample(category, market_type)
        bucket_input = {
            **bucket_summary,
            "category": summary.get("category", bucket_summary.get("category")),
            "bot_like_score": summary.get("bot_like_score", bucket_summary.get("bot_like_score", 0)),
            "sold_before_resolution_market_count": bucket_summary.get(
                "sold_before_resolution_market_count",
                summary.get("sold_before_resolution_market_count", 0),
            ),
            "sold_before_resolution_market_rate": bucket_summary.get(
                "sold_before_resolution_market_rate",
                summary.get("sold_before_resolution_market_rate", 0.0),
            ),
            "two_sided_trade_market_count": bucket_summary.get(
                "two_sided_trade_market_count",
                summary.get("two_sided_trade_market_count", 0),
            ),
            "two_sided_trade_market_rate": bucket_summary.get(
                "two_sided_trade_market_rate",
                summary.get("two_sided_trade_market_rate", 0.0),
            ),
            "historical_trade_behavior_market_count": bucket_summary.get(
                "historical_trade_behavior_market_count",
                summary.get("historical_trade_behavior_market_count", 0),
            ),
        }
        bucket_classified = classify_wallet_bucket(bucket_input, now_ts=now_ts, min_sample=min_sample)
        bucket_classified["min_sample"] = min_sample
        per_type_grades[market_type] = bucket_classified
        if bucket_is_eligible(bucket_classified):
            bucket_classified["eligible_mode"] = "mature"
            eligible_market_types.append(market_type)
            eligible_market_type_modes[market_type] = "mature"
        bucket_rank = grade_rank.get(str(bucket_classified.get("grade")), 0)
        if bucket_rank > best_rank:
            best_grade = bucket_classified.get("grade")
            best_rank = bucket_rank

    if isinstance(per_game_type, dict):
        for key, bucket_summary in sorted(
            per_game_type.items(),
            key=lambda item: (
                GAME_FAMILY_LABELS.get(split_bucket_key(str(item[0]))[0], split_bucket_key(str(item[0]))[0]),
                MARKET_TYPE_ORDER.get(split_bucket_key(str(item[0]))[1], 99),
            ),
        ):
            game_family, market_type = split_bucket_key(key)
            min_sample = wallet_bucket_min_sample(
                category, market_type, game_family=game_family, n_eff_floors=n_eff_floors,
            )
            # 薄样本门只对显式给了锚点的游戏生效(锚点缺失 → None → 不加门,保持旧行为)。
            bucket_anchor = (
                int(n_eff_anchors[game_family])
                if n_eff_anchors and game_family in n_eff_anchors
                else None
            )
            bucket_subset_floor = (
                int(n_eff_subset_floors.get(key) or n_eff_subset_floors.get(game_family))
                if n_eff_subset_floors and (n_eff_subset_floors.get(key) or n_eff_subset_floors.get(game_family))
                else None
            )
            bucket_input = {
                **bucket_summary,
                "category": summary.get("category", bucket_summary.get("category")),
                "bot_like_score": summary.get("bot_like_score", bucket_summary.get("bot_like_score", 0)),
                "sold_before_resolution_market_count": bucket_summary.get(
                    "sold_before_resolution_market_count",
                    summary.get("sold_before_resolution_market_count", 0),
                ),
                "sold_before_resolution_market_rate": bucket_summary.get(
                    "sold_before_resolution_market_rate",
                    summary.get("sold_before_resolution_market_rate", 0.0),
                ),
                "two_sided_trade_market_count": bucket_summary.get(
                    "two_sided_trade_market_count",
                    summary.get("two_sided_trade_market_count", 0),
                ),
                "two_sided_trade_market_rate": bucket_summary.get(
                    "two_sided_trade_market_rate",
                    summary.get("two_sided_trade_market_rate", 0.0),
                ),
                "historical_trade_behavior_market_count": bucket_summary.get(
                    "historical_trade_behavior_market_count",
                    summary.get("historical_trade_behavior_market_count", 0),
                ),
            }
            bucket_classified = classify_wallet_bucket(
                bucket_input,
                now_ts=now_ts,
                min_sample=min_sample,
                min_followable_sample=bucket_subset_floor,
                n_eff_anchor=bucket_anchor,
            )
            bucket_classified.update(
                {
                    "min_sample": min_sample,
                    "bucket_key": key,
                    "bucket_label": bucket_label(key),
                    "game_family": game_family,
                    "game_family_label": GAME_FAMILY_LABELS.get(
                        game_family,
                        game_family.upper() if game_family else "",
                    ),
                    "market_type": market_type,
                    "market_type_label": MARKET_TYPE_LABELS.get(market_type, market_type),
                }
            )
            per_game_type_grades[key] = bucket_classified
            if bucket_is_eligible(bucket_classified):
                bucket_classified["eligible_mode"] = "mature"
                eligible_buckets.append(key)
                eligible_bucket_modes[key] = "mature"
            bucket_rank = grade_rank.get(str(bucket_classified.get("grade")), 0)
            if bucket_rank > best_rank:
                best_grade = bucket_classified.get("grade")
                best_rank = bucket_rank

    if eligible_buckets:
        eligible_buckets = sorted(
            set(eligible_buckets),
            key=lambda value: (
                GAME_FAMILY_LABELS.get(split_bucket_key(value)[0], split_bucket_key(value)[0]),
                MARKET_TYPE_ORDER.get(split_bucket_key(value)[1], 99),
            ),
        )
        eligible_market_types = sorted(
            {split_bucket_key(value)[1] for value in eligible_buckets},
            key=lambda value: MARKET_TYPE_ORDER.get(value, 99),
        )
        classified = {
            **classified,
            "grade": "A",
            "profile_state": "qualified",
            "reasons": [reason for reason in classified.get("reasons", []) if reason != "thin_sample"],
        }
    elif eligible_market_types and not per_game_type_grades:
        eligible_market_types = sorted(
            set(eligible_market_types),
            key=lambda value: MARKET_TYPE_ORDER.get(value, 99),
        )
        classified = {
            **classified,
            "grade": "A",
            "profile_state": "qualified",
            "reasons": [reason for reason in classified.get("reasons", []) if reason != "thin_sample"],
        }
    else:
        eligible_market_types = []
        eligible_market_type_modes = {}
        classified = {**classified, "grade": best_grade}
    observed_market_types = sorted(
        (str(value) for value in per_type_grades if value),
        key=lambda value: MARKET_TYPE_ORDER.get(value, 99),
    )
    return {
        **classified,
        "per_type": per_type,
        "per_type_grades": per_type_grades,
        "per_game_type": per_game_type,
        "per_game_type_grades": per_game_type_grades,
        "eligible_buckets": eligible_buckets,
        "eligible_bucket_modes": {key: eligible_bucket_modes[key] for key in eligible_buckets if key in eligible_bucket_modes},
        "eligible_bucket_labels": [bucket_label(value) for value in eligible_buckets],
        "eligible_game_families": sorted({split_bucket_key(value)[0] for value in eligible_buckets if split_bucket_key(value)[0]}),
        "eligible_game_family_labels": [
            GAME_FAMILY_LABELS.get(value, value.upper())
            for value in sorted({split_bucket_key(bucket)[0] for bucket in eligible_buckets if split_bucket_key(bucket)[0]})
        ],
        "eligible_market_types": eligible_market_types,
        "eligible_market_type_modes": {
            key: eligible_market_type_modes[key] for key in eligible_market_types if key in eligible_market_type_modes
        },
        "eligible_market_type_labels": [MARKET_TYPE_LABELS.get(value, value) for value in eligible_market_types],
        "observed_market_types": observed_market_types,
        "observed_market_type_labels": [MARKET_TYPE_LABELS.get(value, value) for value in observed_market_types],
    }


def bucket_is_eligible(bucket: dict[str, Any]) -> bool:
    """单个桶是否合格(grade A)。单一真相源,勿内联 grade=='A'。"""
    return str(bucket.get("grade") or "") == "A"


def wallet_is_followable(profile: dict[str, Any]) -> bool:
    """钱包是否够格上榜/被跟:整体 grade A,或有任一合格桶(eligible_market_types)。
    collector/observe/demote/follow 统一调它,勿在各处内联 grade=='A' 复制。
    (follow 端的手动 favorite 覆盖是额外叠加,不在此谓词内。)"""
    if str(profile.get("grade") or "") == "A":
        return True
    return bool(profile.get("eligible_market_types"))


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


# --- edge_type 标签 ---------------------------------------------------------
# 区分钱包盈利来源,决定我们能不能、以及怎么跟单(见 review/collector-v2-plan.md §4.4)。
#   directional = 持有到结算也盈利,edge 在"押对结果"。跟单买入持有即可,执行风险低。
#   technical   = 盈利依赖"低价买入 + 结算前精准卖出"(swing/出场)。持有到结算可能是亏的,
#                 跟单需要快速镜像出场,延迟/执行风险高。后期若跟不上可按此标签剔除。
EDGE_TYPE_DIRECTIONAL = "directional"
EDGE_TYPE_TECHNICAL = "technical"
EDGE_TYPE_UNKNOWN = "unknown"


def classify_edge_type(profile: dict[str, Any], *, swing_rate: float = SWING_DEPENDENT_RATE) -> str:
    """根据 hold-to-resolution PnL 与实际 PnL 之差给钱包打 edge_type 标签。

    复用 core 已计算的 actual_pnl / hold_pnl / actual_minus_hold_pnl_rate,
    不引入新的 API 或重算。无可用样本时返回 unknown。
    """
    actual_pnl = to_float(profile.get("actual_pnl"))
    hold_pnl = to_float(profile.get("hold_pnl"))
    closed = to_int(profile.get("esports_closed_count"))
    rate = profile.get("actual_minus_hold_pnl_rate")
    if closed <= 0 and actual_pnl == 0 and hold_pnl == 0:
        return EDGE_TYPE_UNKNOWN
    # 持有到结算会亏(或不赚),却实际盈利 → 利润纯靠出场时机。
    if hold_pnl <= 0 < actual_pnl:
        return EDGE_TYPE_TECHNICAL
    if rate is not None and to_float(rate) > swing_rate:
        return EDGE_TYPE_TECHNICAL
    return EDGE_TYPE_DIRECTIONAL


def profile_candidate_wallet(
    candidate: dict[str, Any],
    esports_condition_ids: set[str],
    *,
    market_records_by_id: dict[str, dict[str, Any]] | None = None,
    condition_type_by_id: dict[str, str] | None = None,
    condition_game_family_by_id: dict[str, str] | None = None,
    user_trades_loader=None,
    closed_positions_loader=None,
    current_positions_loader,
    historical_trades_loader=None,
    now_ts: int | None = None,
    scoring_basis: str = "hold",
    n_eff_floors: dict[str, int] | None = None,
    n_eff_subset_floors: dict[str, int] | None = None,
    n_eff_anchors: dict[str, int] | None = None,
) -> dict[str, Any]:
    wallet = normalize_wallet(candidate.get("wallet"))
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    try:
        current_positions = current_positions_loader(wallet)
        bot_score = max(bot_like_score_from_positions(current_positions), bot_like_score_from_candidate(candidate))
        if user_trades_loader and market_records_by_id:
            summary = summarize_trade_reconstructed_positions(
                user_trades_loader(wallet),
                market_records_by_id,
                now_ts=now_ts,
                bot_like_score=bot_score,
                scoring_basis=scoring_basis,
            )
        else:
            if not closed_positions_loader:
                raise ValueError("user_trades_loader or closed_positions_loader is required")
            closed_positions = closed_positions_loader(wallet)
            summary = summarize_closed_positions(
                closed_positions,
                esports_condition_ids,
                condition_type_by_id=condition_type_by_id,
                condition_game_family_by_id=condition_game_family_by_id,
                now_ts=now_ts,
                bot_like_score=bot_score,
                scoring_basis=scoring_basis,
            )
        if historical_trades_loader and not user_trades_loader:
            trade_behavior = summarize_historical_trade_behavior(
                summary.get("esports_condition_ids") or [],
                historical_trades_loader=lambda condition_id: historical_trades_loader(wallet, condition_id),
                condition_type_by_id=condition_type_by_id,
            )
            per_type_behavior = trade_behavior.pop("per_type_trade_behavior", {}) or {}
            summary = {**summary, **trade_behavior}
            per_type = dict(summary.get("per_type") or {})
            for market_type, behavior in per_type_behavior.items():
                per_type[market_type] = {**(per_type.get(market_type) or {}), **behavior}
            summary["per_type"] = per_type
        result = classify_wallet(
            {**summary, "wallet": wallet, "candidate": candidate},
            now_ts=now_ts,
            n_eff_floors=n_eff_floors,
            n_eff_subset_floors=n_eff_subset_floors,
            n_eff_anchors=n_eff_anchors,
        )
        # edge_type 标签恒在 profile 上(directional/technical/unknown),供 v2 导出/follow 按标签过滤。
        result["edge_type"] = classify_edge_type(result)
        result["scoring_basis"] = scoring_basis
        return result
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
