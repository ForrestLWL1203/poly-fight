from __future__ import annotations

from datetime import datetime, timezone
from math import sqrt
import re
from statistics import median
from typing import Any, Iterable


SECONDS_PER_DAY = 86400
SCORING_VERSION = 13
WILSON_Z = 1.28
TRADE_BEHAVIOR_MIN_MARKETS = 4
TRADE_BEHAVIOR_EXCLUDE_RATE = 0.5
# Thresholds recalibrated for the de-biased trade-reconstruction win rates (v11):
# real top esports wallets win ~66-81%, not the survivorship-inflated ~90% the old
# closed_positions floors assumed. See review/scoring analysis.
MIN_A_POSITIVE_MARKET_RATE = 0.63
MIN_B_POSITIVE_MARKET_RATE = 0.55
# 技能轴 = capital_weighted_edge（赢钱占比 − 入场价，按资金加权）+ 正 hold PnL。
# roi 不再硬切（它是赔率结构副产品，会冤枉高胜率买热门的钱包），仅作软 reason。
ESPORTS_MIN_ROI = 0.20  # 软信号 low_roi 的提示线，不再硬排除
SPORTS_MIN_ROI = 0.15
ESPORTS_MIN_A_WILSON = 0.57
SPORTS_MIN_A_WILSON = 0.50
ESPORTS_MIN_A_CAPITAL_WEIGHTED_EDGE = 0.08
SPORTS_MIN_A_CAPITAL_WEIGHTED_EDGE = 0.10
# actual_minus_hold_pnl_rate 超过此值 = 利润主要靠盘中卖出（我们复制不了）→ 软标记
SWING_DEPENDENT_RATE = 0.2
# 单市场成交 >=20 笔记为 high churn。high_churn 市场占比超过此值 = 机器人/高频/做市
# （盈利来自微观价差和速度，复制不了）→ 直接排除出 leaderboard。
MAX_HIGH_CHURN_MARKET_RATE = 0.5
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
MARKET_TYPE_ORDER = {
    MAIN_MATCH: 0,
    GAME_WINNER: 1,
    MAP_WINNER: 2,
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


def is_esports_event(event: dict[str, Any]) -> bool:
    return event_category(event) == "esports"


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
        if category == "sports":
            match_start_time = market.get("gameStartTime") or market.get("eventStartTime") or event.get("startTime")
            end_date = (
                market.get("umaEndDate")
                or market.get("closedTime")
                or event.get("finishedTimestamp")
                or event.get("closedTime")
                or market.get("endDate")
                or event.get("endDate")
            )
        else:
            match_start_time = market.get("eventStartTime") or event.get("startTime") or market.get("gameStartTime")
            end_date = event.get("endDate") or market.get("endDate")
        condition_id = str(market.get("conditionId")).lower()
        records[condition_id] = {
            "condition_id": condition_id,
            "event_id": str(event.get("id") or ""),
            "event_slug": event.get("slug"),
            "title": event.get("title"),
            "question": market.get("question"),
            "outcomes": parse_jsonish(market.get("outcomes"), []),
            "outcome_prices": [to_float(v) for v in parse_jsonish(market.get("outcomePrices"), [])],
            "end_date": end_date,
            "match_start_time": match_start_time,
            "market_start_time": market.get("gameStartTime") or market.get("eventStartTime") or event.get("startTime"),
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
    target_markets: int = 30,
    submarket_target_markets: int = 60,
    game_winner_target_markets: int | None = None,
    map_winner_target_markets: int | None = None,
    max_markets_per_run: int = 50,
    submarket_max_markets_per_run: int = 60,
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

    def select(days: int, min_volume: float, market_types: set[str], *, league: str | None = None) -> list[dict[str, Any]]:
        selected = []
        for market in classification_set:
            market_type = str(market.get("market_type") or MAIN_MATCH)
            if market_type not in market_types:
                continue
            if league is not None and str(market.get("league") or "").lower() != league:
                continue
            end = parse_dt(market.get("end_date"))
            if not end:
                continue
            days_ago = (now - end).total_seconds() / SECONDS_PER_DAY
            if 0 <= days_ago <= days and to_float(market.get("volume")) >= min_volume:
                selected.append(market)
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
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        selected_days = lookback_steps[-1]
        selected_min_volume = primary_min_volume
        for days in lookback_steps:
            selected = select(days, primary_min_volume, market_types, league=league)
            selected_days = days
            if len(selected) >= target:
                break
        if len(selected) < target:
            selected = select(lookback_steps[-1], fallback_min_volume, market_types, league=league)
            selected_min_volume = fallback_min_volume
        return selected, {
            "selected_lookback_days": selected_days,
            "selected_min_market_volume": selected_min_volume,
            "target_markets": target,
            "total_selected_market_count": len(selected),
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

    if league_target_markets:
        main_slice = main_selected
    else:
        main_slice = main_selected[market_offset : market_offset + max_markets_per_run]
    game_slice = game_selected[:game_winner_max_markets_per_run]
    map_slice = map_selected[:map_winner_max_markets_per_run]
    merged: dict[str, dict[str, Any]] = {}
    for market in [*main_slice, *game_slice, *map_slice]:
        merged[str(market.get("condition_id") or "").lower()] = market
    selected = sorted(
        merged.values(),
        key=lambda row: (
            MARKET_TYPE_ORDER.get(str(row.get("market_type") or MAIN_MATCH), 99),
            -to_float(row.get("volume")),
        ),
    )

    type_counts: dict[str, int] = {}
    selected_type_counts: dict[str, int] = {}
    selected_league_counts: dict[str, int] = {}
    for market in classification_set:
        market_type = str(market.get("market_type") or MAIN_MATCH)
        type_counts[market_type] = type_counts.get(market_type, 0) + 1
    for market in selected:
        market_type = str(market.get("market_type") or MAIN_MATCH)
        selected_type_counts[market_type] = selected_type_counts.get(market_type, 0) + 1
        league = str(market.get("league") or "").lower()
        if league:
            selected_league_counts[league] = selected_league_counts.get(league, 0) + 1

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


def wilson_lower_bound(successes: int, n: int, z: float = WILSON_Z) -> float:
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
    condition_type_by_id: dict[str, str] | None = None,
    now_ts: int | None = None,
    bot_like_score: int = 0,
) -> dict[str, Any]:
    condition_type_by_id = {str(key).lower(): value for key, value in (condition_type_by_id or {}).items()}
    rows = []
    neutral_market_count_by_type: dict[str, int] = {}
    for position in positions:
        condition_id = str(position.get("conditionId") or position.get("condition_id") or "").lower()
        if condition_id not in esports_condition_ids:
            continue
        market_type = condition_type_by_id.get(condition_id, MAIN_MATCH)
        total_bought = to_float(position.get("totalBought") or position.get("total_bought"))
        realized_pnl = to_float(position.get("realizedPnl") or position.get("realized_pnl"))
        if total_bought <= 0:
            continue
        if realized_pnl == 0:
            neutral_market_count_by_type[market_type] = neutral_market_count_by_type.get(market_type, 0) + 1
            continue
        avg_price = to_float(position.get("avgPrice") or position.get("avg_price"))
        cost_basis = total_bought * avg_price if avg_price > 0 else total_bought
        actual_pnl = to_float(position.get("actualPnl"), realized_pnl)
        hold_pnl = to_float(position.get("holdPnl"), realized_pnl)
        rows.append(
            {
                "condition_id": condition_id,
                "market_type": market_type,
                "pre_match_entry": position.get("preMatchEntry"),
                "total_bought": total_bought,
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "actual_pnl": actual_pnl,
                "hold_pnl": hold_pnl,
                "profit_per_share": realized_pnl / total_bought,
                "roi": realized_pnl / cost_basis if cost_basis > 0 else 0.0,
                "avg_price": avg_price,
                "timestamp": to_int(position.get("timestamp")),
            }
        )

    def summarize_bucket(bucket_rows: list[dict[str, Any]], *, neutral_market_count: int) -> dict[str, Any]:
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
        entry_prices = [row["avg_price"] for row in bucket_rows if row["avg_price"] > 0]
        high_price_entries = sum(1 for row in bucket_rows if row["avg_price"] >= 0.90)
        low_edge_profits = sum(1 for row in bucket_rows if 0 < row["roi"] <= 0.03)
        condition_ids = sorted({row["condition_id"] for row in bucket_rows})
        winning_cost = sum(row["cost_basis"] for row in bucket_rows if row["realized_pnl"] > 0)
        pre_match_rows = [row for row in bucket_rows if row.get("pre_match_entry") is not None]
        pre_match_entry_count = sum(1 for row in pre_match_rows if row.get("pre_match_entry"))
        capital_weighted_entry_price = total_cost / total_bought if total_bought else 0.0
        capital_weighted_win_rate = winning_cost / total_cost if total_cost else 0.0
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
        "avg_position_size": round(total_bought / count, 6) if count else 0.0,
        "median_position_size": round(median(sizes), 6) if sizes else 0.0,
        "avg_entry_price": round(sum(entry_prices) / len(entry_prices), 8) if entry_prices else 0.0,
        "median_entry_price": round(median(entry_prices), 8) if entry_prices else 0.0,
        "capital_weighted_entry_price": round(capital_weighted_entry_price, 8),
        "capital_weighted_win_rate": round(capital_weighted_win_rate, 8),
        "capital_weighted_edge": round(capital_weighted_win_rate - capital_weighted_entry_price, 8),
        "pre_match_entry_count": pre_match_entry_count,
        "pre_match_entry_market_count": len(pre_match_rows),
        "pre_match_entry_rate": round(pre_match_entry_count / len(pre_match_rows), 8) if pre_match_rows else None,
        "entry_price_buckets": formatted_buckets,
        "high_price_entry_rate": round(high_price_entries / count, 8) if count else 0.0,
        "low_edge_profit_rate": round(low_edge_profits / count, 8) if count else 0.0,
        "last_esports_trade_at": last_trade,
        "bot_like_score": bot_like_score,
        "profiled_at": now_ts or int(datetime.now(timezone.utc).timestamp()),
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
    summary = summarize_bucket(rows, neutral_market_count=sum(neutral_market_count_by_type.values()))
    return {**summary, "per_type": per_type, "data_quality": {"source": "closed_positions", "reliable_losses": False}}


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
        last_ts = 0
        last_buy_ts = 0
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
                buy_size_by_outcome[outcome_index] = buy_size_by_outcome.get(outcome_index, 0.0) + size
                buy_cost_by_outcome[outcome_index] = buy_cost_by_outcome.get(outcome_index, 0.0) + cash
            elif side == "SELL":
                sell_size_by_outcome[outcome_index] = sell_size_by_outcome.get(outcome_index, 0.0) + size
                sell_proceeds_by_outcome[outcome_index] = sell_proceeds_by_outcome.get(outcome_index, 0.0) + cash

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
            for outcome_index, sell_size in sell_size_by_outcome.items()
        )
        two_sided = len(bought_outcomes) >= 2
        total_bought = sum(buy_size_by_outcome.values())
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
                "soldBeforeResolution": has_material_sell,
                "twoSidedTrade": two_sided,
            }
        )
        behavior_by_market[condition_id] = {
            "condition_id": condition_id,
            "sold_before_resolution": has_material_sell,
            "two_sided": two_sided,
            "buy_size_by_outcome": {str(k): round(v, 8) for k, v in sorted(buy_size_by_outcome.items())},
            "sell_size_by_outcome": {str(k): round(v, 8) for k, v in sorted(sell_size_by_outcome.items())},
        }
    return positions, behavior_by_market


def summarize_trade_reconstructed_positions(
    trades: list[dict[str, Any]],
    market_records_by_id: dict[str, dict[str, Any]],
    *,
    now_ts: int | None = None,
    bot_like_score: int = 0,
    material_sell_frac: float = 0.2,
) -> dict[str, Any]:
    markets = {str(key).lower(): value for key, value in market_records_by_id.items()}
    categories = {str(record.get("category") or "") for record in markets.values() if record.get("category")}
    category = next(iter(categories)) if len(categories) == 1 else None
    positions, behavior_by_market = reconstruct_closed_positions(
        trades,
        markets,
        material_sell_frac=material_sell_frac,
    )
    esports_condition_ids = set(markets)
    condition_type_by_id = {
        condition_id: str(record.get("market_type") or MAIN_MATCH)
        for condition_id, record in markets.items()
    }
    summary = summarize_closed_positions(
        positions,
        esports_condition_ids,
        condition_type_by_id=condition_type_by_id,
        now_ts=now_ts,
        bot_like_score=bot_like_score,
    )
    behavior_market_count = len(behavior_by_market)
    sold_before_resolution_market_count = sum(1 for row in behavior_by_market.values() if row.get("sold_before_resolution"))
    two_sided_trade_market_count = sum(1 for row in behavior_by_market.values() if row.get("two_sided"))

    per_type_behavior: dict[str, dict[str, int]] = {}
    for condition_id, behavior_row in behavior_by_market.items():
        market_type = condition_type_by_id.get(condition_id, MAIN_MATCH)
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
        if behavior_row.get("two_sided"):
            bucket["two_sided_trade_market_count"] += 1

    per_type = dict(summary.get("per_type") or {})
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
        if len(traded_outcomes) >= 2:
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
) -> dict[str, Any]:
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
    min_a_wilson = SPORTS_MIN_A_WILSON if is_sports else ESPORTS_MIN_A_WILSON
    min_a_edge = SPORTS_MIN_A_CAPITAL_WEIGHTED_EDGE if is_sports else ESPORTS_MIN_A_CAPITAL_WEIGHTED_EDGE
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
    if pnl <= 0:
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["negative_roi"],
        }
    # Skill axis: exclude only wallets with no real edge (won no more capital than the
    # entry price implied). roi is NOT a hard gate — it's a payoff-structure artifact that
    # penalizes favorite-buyers (high win rate, thin payoff). roi only soft-flags below.
    if capital_weighted_edge <= 0:
        return {
            **summary,
            "entry_edge": round(entry_edge, 8),
            "grade": "excluded",
            "profile_state": "unqualified",
            "reasons": ["no_capital_edge"],
        }
    if count < min_sample:
        reasons.append("thin_sample")
    if total_bought < 1_000:
        reasons.append("low_volume")
    if median_entry <= 0 or median_entry > 0.68:
        reasons.append("weak_entry_price")
    if loss_count > 0:
        reasons.append("has_losses")
    if wilson_lb < min_a_wilson:
        reasons.append("weak_wilson")
    if edge_value < min_a_edge:
        reasons.append(weak_edge_reason)
    if positive_rate < MIN_A_POSITIVE_MARKET_RATE:
        reasons.append("low_positive_market_rate")
    if median_roi < 0 or positive_rate < 0.5:
        reasons.append("unstable_returns")
    if historical_roi < min_roi:
        reasons.append("low_roi")  # soft/informational only — not an exclusion
    if actual_minus_hold_rate > SWING_DEPENDENT_RATE:
        reasons.append("swing_dependent")  # profit leans on in-game selling we can't copy

    if (
        count >= min_sample
        and wilson_lb >= min_a_wilson
        and edge_value >= min_a_edge
        and pnl > 0
        and positive_rate >= MIN_A_POSITIVE_MARKET_RATE
        and total_bought >= 5_000
        and 0 < median_entry <= 0.68
        and not stale
        and bot_score < 40
    ):
        grade = "A"
    elif (
        count >= min_sample
        and wilson_lb >= 0.50
        and edge_value >= 0.0
        and pnl > 0
        and positive_rate >= MIN_B_POSITIVE_MARKET_RATE
        and total_bought >= 1_000
        and 0 < median_entry <= 0.68
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


def classify_wallet(summary: dict[str, Any], *, now_ts: int | None = None) -> dict[str, Any]:
    now_ts = now_ts or int(datetime.now(timezone.utc).timestamp())
    category = str(summary.get("category") or "").lower()
    classified = classify_wallet_bucket(summary, now_ts=now_ts, min_sample=8)
    per_type = summary.get("per_type") or {}
    if not isinstance(per_type, dict):
        return classified

    per_type_grades: dict[str, dict[str, Any]] = {}
    eligible_market_types: list[str] = []
    grade_rank = {"A": 5, "B": 4, "C": 3, "stale": 2, "excluded": 1, "unknown": 0}
    best_grade = classified.get("grade") or "unknown"
    best_rank = grade_rank.get(str(best_grade), 0)
    for market_type, bucket_summary in sorted(
        per_type.items(),
        key=lambda item: MARKET_TYPE_ORDER.get(str(item[0]), 99),
    ):
        min_sample = 8 if category == "sports" or market_type == MAIN_MATCH else 10
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
        if bucket_classified.get("grade") == "A":
            eligible_market_types.append(market_type)
        bucket_rank = grade_rank.get(str(bucket_classified.get("grade")), 0)
        if bucket_rank > best_rank:
            best_grade = bucket_classified.get("grade")
            best_rank = bucket_rank

    if eligible_market_types:
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
        classified = {**classified, "grade": best_grade}
    observed_market_types = sorted(
        (str(value) for value in per_type_grades if value),
        key=lambda value: MARKET_TYPE_ORDER.get(value, 99),
    )
    return {
        **classified,
        "per_type": per_type,
        "per_type_grades": per_type_grades,
        "eligible_market_types": eligible_market_types,
        "eligible_market_type_labels": [MARKET_TYPE_LABELS.get(value, value) for value in eligible_market_types],
        "observed_market_types": observed_market_types,
        "observed_market_type_labels": [MARKET_TYPE_LABELS.get(value, value) for value in observed_market_types],
    }


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
    market_records_by_id: dict[str, dict[str, Any]] | None = None,
    condition_type_by_id: dict[str, str] | None = None,
    user_trades_loader=None,
    closed_positions_loader=None,
    current_positions_loader,
    historical_trades_loader=None,
    now_ts: int | None = None,
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
            )
        else:
            if not closed_positions_loader:
                raise ValueError("user_trades_loader or closed_positions_loader is required")
            closed_positions = closed_positions_loader(wallet)
            summary = summarize_closed_positions(
                closed_positions,
                esports_condition_ids,
                condition_type_by_id=condition_type_by_id,
                now_ts=now_ts,
                bot_like_score=bot_score,
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
