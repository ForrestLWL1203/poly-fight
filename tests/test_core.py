from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from contextlib import nullcontext, redirect_stdout
import http.client
import json
import os
from io import BytesIO, StringIO
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import unittest
from unittest.mock import patch

from poly_fight.api import PolymarketClient, RateLimiter, parse_retry_after
from poly_fight.dashboard import (
    build_events,
    build_follow_detail,
    build_follows,
    build_health,
    build_overview,
    build_runner_status,
    build_wallet_follow_detail,
    category_data_dirs,
    fetch_market_prices,
    read_stream_signal,
    build_wallet_refresh_status,
    build_wallets,
    DashboardConfig,
    create_server,
    make_session_token,
    reset_dashboard_data,
    _wipe_collector_data,
    short_addr,
    start_runner,
    stop_runner,
    start_wallet_refresh,
    stream_dirty_flags,
    StreamSignal,
    verify_session_token,
)
from poly_fight import dashboard as dashboard_module
from poly_fight import storage as storage_module
from poly_fight.control import (
    read_follow_control,
    reconcile_pause_new_signals,
    reconcile_wallet_refresh_status,
    set_pause_new_signals,
    write_follow_control,
)
from poly_fight.storage import FollowStore
from poly_fight.cli import (
    BuildLockUnavailable,
    acquire_build_lock,
    load_scope_params,
    _slim_user_trade,
    scope_n_eff_floors,
    scope_lookback_by_game,
    scope_max_lookback_days,
    filter_classification_set_by_game_window,
    build_leaderboard_from_profiles,
    build_collection_diagnostics,
    build_profile_candidate_from_trades,
    build_collector_diagnostics,
    build_collector_profile_refresh_plan,
    build_collector_snapshot_diagnostics,
    command_analyze_collector_snapshot,
    aggregate_seed_wallets,
    build_collector_leaderboard_v2,
    slim_profile_for_storage,
    build_seeded_leaderboard,
    calculate_seed_bucket_min_wins,
    collect_seed_positions,
    collect_live_seed_positions,
    command_collect,
    filter_profile_seed_wallets_v2,
    ESPORTS_CANDIDATE_MARKET_TYPE_THRESHOLDS,
    ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS,
    build_profile_budget_summary,
    build_parser,
    build_wallet_overlap_report,
    build_profile_fetch_plan,
    effective_build_defaults,
    effective_discovery_defaults,
    backfill_user_trade_submarkets,
    command_follow,
    effective_bankroll_usdc,
    fetch_resolutions_for_open_signals,
    fetch_recent_esports_closed_positions_for_wallet,
    fetch_recent_esports_user_trades_for_wallet,
    fetch_recent_user_trades_for_wallet,
    fetch_user_trades_until_cursor,
    fetch_market_trades_cached,
    filter_profile_candidates,
    follow_run_log_path,
    load_active_market_cache,
    merge_cached_profile_with_candidate,
    merge_profiles_with_candidates,
    migrate_category_follow_dbs,
    publish_collector_dashboard_outputs,
    prune_profile_store,
    read_category_leaderboards,
    refresh_team_logo_cache_from_active_markets,
    read_json,
    read_jsonl,
    resolve_data_dir,
    seed_wallet_score,
    strict_final_quality_ok,
    should_refresh_file_cache,
    should_use_cached_profile,
    watched_markets,
    active_market_cache_in_follow_scope,
    write_json,
    write_jsonl,
    prepare_category_refresh_dir,
    select_collector_target_markets,
    resolve_collector_profile_wallet_limit,
    merge_collector_cached_profile_with_seed,
)
from poly_fight.core import (
    ALLOWED_GAME_FAMILIES,
    SCORING_VERSION,
    derive_scope_params,
    match_day_gaps,
    wallet_bucket_min_sample,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    classify_edge_type,
    classify_market_type,
    classify_wallet,
    classify_wallet_bucket,
    event_category,
    event_league,
    event_to_market_record,
    event_to_market_records,
    normalize_wallet,
    parse_dt,
    profile_candidate_wallet,
    reconstruct_closed_positions,
    summarize_closed_positions,
    summarize_trade_reconstructed_positions,
    wallet_is_followable,
    winning_outcome_index,
)
from poly_fight.follow import (
    aggregate_follow_performance,
    apply_closing_line_snapshots,
    apply_contested_flags,
    compute_clv,
    contested_markets,
    desired_tick_interval,
    eligible_follow_wallets,
    esports_match_imminent,
    evaluate_slippage,
    follow_signal_id,
    material_sell,
    paper_pnl,
    prune_unfollowed_signals,
    process_follow_trades,
    follow_stake_for_signal,
    quarantine_reason,
    select_new_trades,
    settle_open_signals,
    summarize_wallet_fills,
    wallet_behavior_summary,
)
from poly_fight.follow_strategy import (
    default_follow_strategy,
    evaluate_follow_candidate,
    normalize_follow_strategy,
    strategy_from_legacy_args,
    validate_follow_strategy,
)


def market(condition_id, days_ago, volume=50_000):
    now = datetime(2026, 6, 4, tzinfo=timezone.utc)
    end = now.timestamp() - days_ago * 86400
    return {
        "condition_id": condition_id,
        "title": f"Match {condition_id}",
        "question": f"Match {condition_id}",
        "end_date": datetime.fromtimestamp(end, timezone.utc).isoformat(),
        "volume": volume,
        "outcomes": ["A", "B"],
    }


def market_with_end(condition_id, end_ts):
    return datetime.fromtimestamp(end_ts, timezone.utc).isoformat()


def _seed_leaderboard(path, rows, *, summary=None):
    """Seed leaderboard.db from a legacy smart_wallet_leaderboard.json payload.

    Dashboard/cli read the leaderboard from SQLite only; tests seed via the store
    instead of writing the retired JSON file. Category is inferred from the path.
    """
    p = Path(path)
    category = p.parent.name if p.parent.name in ("esports", "sports") else "esports"
    storage_module.LeaderboardStore(p.parent / "leaderboard.db").publish_collection(
        category=category,
        leaderboard=list(rows or []),
        profiles=[],
        summary=summary or {},
        updated_at=int(time.time()),
    )


def _seed_active_market_cache(path, payload):
    """Seed follow.db active market cache from a legacy active_market_cache.json payload.

    Dashboard reads the active market cache from SQLite only; tests seed via the store
    instead of writing the retired JSON file.
    """
    db = Path(path).parent / "follow.db"
    markets_value = payload.get("markets") if isinstance(payload, dict) else []
    if isinstance(markets_value, dict):
        items = list(markets_value.values())
    elif isinstance(markets_value, list):
        items = markets_value
    else:
        items = []
    markets = {}
    for market in items:
        if isinstance(market, dict):
            cid = str(market.get("condition_id") or market.get("conditionId") or "")
            markets[cid] = market
    FollowStore(db).save_market_cache(
        markets, cache_kind="active", updated_at=int((payload or {}).get("updated_at") or 0)
    )


class CoreTest(unittest.TestCase):
    def sports_a_profile(self, wallet, *, league="nba", event_count=8, avg_market_cash=6_000, **overrides):
        profile = {
            "wallet": wallet,
            "category": "sports",
            "league": league,
            "grade": "A",
            "scoring_version": SCORING_VERSION,
            "last_esports_trade_at": 100,
            "eligible_market_types": ["main_match"],
            "esports_condition_ids": [f"{league}-{wallet}-{index}" for index in range(event_count)],
            "esports_closed_count": event_count,
            "esports_win_count": max(0, event_count - 2),
            "esports_loss_count": min(2, event_count),
            "positive_market_rate": 0.75,
            "median_market_roi": 0.20,
            "esports_roi": 0.22,
            "wilson_win_rate_lower_bound": 0.55,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.10,
            "candidate": {
                "participated_market_count": event_count,
                "avg_market_cash": avg_market_cash,
                "two_sided_market_count": 0,
                "tail_entry_market_count": 0,
            },
        }
        candidate_overrides = overrides.pop("candidate", None)
        profile.update(overrides)
        if candidate_overrides:
            profile["candidate"] = {**profile["candidate"], **candidate_overrides}
        return profile

    def test_collect_accepts_collector_tuning_flags(self):
        args = build_parser().parse_args(
            [
                "collect",
                "--bucket-market-limit",
                "7",
                "--positions-per-market",
                "9",
                "--max-profile-wallets",
                "11",
                "--max-core-wallets",
                "13",
                "--profile-lookback-days",
                "9",
                "--seed-single-bucket-min-wins",
                "6",
                "--seed-multi-bucket-min-wins",
                "9",
                "--seed-bucket-min-hit-rate",
                "0.2",
                "--seed-main-match-min-avg-cash",
                "501",
                "--seed-game-winner-min-avg-cash",
                "502",
                "--seed-map-winner-min-avg-cash",
                "301",
                "--seed-min-weighted-roi",
                "0.31",
                "--seed-max-median-avg-price",
                "0.74",
            ]
        )

        self.assertIs(args.func, command_collect)
        self.assertEqual(args.bucket_market_limit, 7)
        self.assertEqual(args.positions_per_market, 9)
        self.assertEqual(args.max_profile_wallets, 11)
        self.assertEqual(args.max_core_wallets, 13)
        self.assertEqual(args.profile_lookback_days, 9)
        self.assertEqual(args.seed_single_bucket_min_wins, 6)
        self.assertEqual(args.seed_multi_bucket_min_wins, 9)
        self.assertEqual(args.seed_bucket_min_hit_rate, 0.2)
        self.assertEqual(args.seed_main_match_min_avg_cash, 501)
        self.assertEqual(args.seed_game_winner_min_avg_cash, 502)
        self.assertEqual(args.seed_map_winner_min_avg_cash, 301)
        self.assertEqual(args.seed_min_weighted_roi, 0.31)
        self.assertEqual(args.seed_max_median_avg_price, 0.74)

        legacy_budget_args = build_parser().parse_args(["collect", "--max-profiles-per-run", "44"])
        self.assertIsNone(legacy_budget_args.max_profile_wallets)
        self.assertEqual(
            resolve_collector_profile_wallet_limit(legacy_budget_args),
            44,
        )
        default_args = build_parser().parse_args(["collect"])
        self.assertEqual(default_args.seed_bucket_min_hit_rate, 0.10)
        self.assertEqual(default_args.profile_lookback_days, 14)
        self.assertEqual(default_args.seed_single_bucket_min_wins, 5)
        self.assertEqual(default_args.seed_multi_bucket_min_wins, 8)
        self.assertEqual(default_args.seed_main_match_min_avg_cash, 100)
        self.assertEqual(default_args.seed_game_winner_min_avg_cash, 100)
        self.assertEqual(default_args.seed_map_winner_min_avg_cash, 100)
        self.assertEqual(default_args.seed_min_weighted_roi, 0.30)
        self.assertEqual(default_args.seed_max_median_avg_price, 0.85)

    def test_category_data_dirs_use_fixed_dashboard_root_mapping(self):
        root = Path("/tmp/poly-data")

        self.assertEqual(
            category_data_dirs(root),
            {"esports": root / "esports"},
        )

    def test_event_category_and_league_identify_nba_ufc_and_in_scope_esports(self):
        nba_event = {"title": "Los Angeles Lakers vs. Boston Celtics", "tags": [{"slug": "nba"}]}
        ufc_event = {"title": "Justin Gaethje vs Ilia Topuria", "tags": [{"slug": "ufc"}]}
        mlb_event = {"title": "New York Mets vs. San Diego Padres", "tags": [{"slug": "mlb"}]}
        cs_event = {"title": "Counter-Strike: A vs B (BO3)", "tags": [{"slug": "counter-strike-2"}]}
        valorant_event = {"title": "Valorant: A vs B (BO3)", "tags": [{"slug": "valorant"}]}

        self.assertEqual(event_category(nba_event), "sports")
        self.assertEqual(event_league(nba_event), "nba")
        self.assertEqual(event_category(ufc_event), "sports")
        self.assertEqual(event_league(ufc_event), "ufc")
        self.assertIsNone(event_category(mlb_event))
        self.assertEqual(event_league(mlb_event), "other")
        self.assertEqual(event_category(cs_event), "esports")
        self.assertEqual(event_league(cs_event), "cs2")
        # valorant 现为平级 in-scope 游戏(自适应采集参数落地后纳入)。
        self.assertEqual(event_category(valorant_event), "esports")
        self.assertEqual(event_league(valorant_event), "valorant")
        self.assertIn("valorant", ALLOWED_GAME_FAMILIES)

    def test_valorant_moneyline_is_in_scope(self):
        event = {
            "id": "valorant1",
            "slug": "valorant-nrg-leviatan-2026-06-08",
            "title": "Valorant: NRG vs Leviatan Esports (BO3) - VCT Masters London Group Stage",
            "closed": True,
            "endDate": "2026-06-08T20:00:00Z",
            "tags": [{"slug": "valorant"}],
            "markets": [
                {
                    "conditionId": "valorant-moneyline",
                    "question": "Valorant: NRG vs Leviatan Esports (BO3) - VCT Masters London Group Stage",
                    "outcomes": '["NRG","Leviatan Esports"]',
                    "outcomePrices": '["1","0"]',
                    "volume": "717547.49",
                }
            ],
        }

        records = event_to_market_records(event)

        self.assertEqual(classify_market_type(event, event["markets"][0]), "main_match")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["game_family"], "valorant")
        self.assertEqual(records[0]["market_type"], "main_match")

    def test_nba_and_ufc_moneylines_are_main_match_and_record_league(self):
        nba_event = {
            "id": "nba1",
            "slug": "nba-lakers-celtics-2026-06-06",
            "title": "Los Angeles Lakers vs. Boston Celtics",
            "closed": True,
            "endDate": "2026-06-06T23:00:00Z",
            "tags": [{"slug": "nba"}],
            "markets": [
                {
                    "conditionId": "nba-moneyline",
                    "question": "Los Angeles Lakers vs. Boston Celtics",
                    "outcomes": '["Los Angeles Lakers","Boston Celtics"]',
                    "outcomePrices": '["0","1"]',
                    "volume": "1005821.89",
                }
            ],
        }
        ufc_event = {
            "id": "ufc1",
            "slug": "ufc-gaethje-topuria-2026-06-06",
            "title": "Justin Gaethje vs Ilia Topuria",
            "closed": True,
            "endDate": "2026-06-06T23:00:00Z",
            "tags": [{"slug": "ufc"}],
            "markets": [
                {
                    "conditionId": "ufc-moneyline",
                    "question": "Justin Gaethje vs Ilia Topuria",
                    "outcomes": '["Justin Gaethje","Ilia Topuria"]',
                    "outcomePrices": '["1","0"]',
                    "volume": "230000",
                }
            ],
        }

        nba_records = event_to_market_records(nba_event)
        ufc_records = event_to_market_records(ufc_event)

        self.assertEqual(classify_market_type(nba_event, nba_event["markets"][0]), "main_match")
        self.assertEqual(classify_market_type(ufc_event, ufc_event["markets"][0]), "main_match")
        self.assertEqual(nba_records[0]["category"], "sports")
        self.assertEqual(nba_records[0]["league"], "nba")
        self.assertEqual(ufc_records[0]["category"], "sports")
        self.assertEqual(ufc_records[0]["league"], "ufc")

    def test_sports_props_and_out_of_scope_mlb_are_excluded(self):
        event = {
            "id": "e1",
            "slug": "mlb-nym-sd-2026-06-06",
            "title": "New York Mets vs. San Diego Padres",
            "closed": True,
            "endDate": "2026-06-06T23:00:00Z",
            "tags": [{"slug": "mlb"}],
            "markets": [
                {
                    "conditionId": "moneyline",
                    "question": "New York Mets vs. San Diego Padres",
                    "outcomes": '["New York Mets","San Diego Padres"]',
                    "outcomePrices": '["0","1"]',
                    "volume": "1005821.89",
                }
            ],
        }
        ufc_future = {
            "id": "ufc-future",
            "title": "Who will Jon Jones fight next?",
            "tags": [{"slug": "ufc"}],
            "markets": [
                {
                    "conditionId": "ufc-future-yes-no",
                    "question": "Will Jon Jones fight Tom Aspinall next?",
                    "outcomes": '["Yes","No"]',
                    "outcomePrices": '["0.4","0.6"]',
                }
            ],
        }
        nba_prop = {
            "id": "nba-prop",
            "title": "Los Angeles Lakers vs. Boston Celtics",
            "tags": [{"slug": "nba"}],
            "markets": [
                {
                    "conditionId": "nba-player-points",
                    "question": "Will LeBron James score over 24.5 points?",
                    "outcomes": '["Over","Under"]',
                    "outcomePrices": '["0.5","0.5"]',
                }
            ],
        }

        self.assertIsNone(classify_market_type(event, event["markets"][0]))
        self.assertEqual(event_to_market_records(event), [])
        self.assertIsNone(classify_market_type(ufc_future, ufc_future["markets"][0]))
        self.assertEqual(event_to_market_records(ufc_future), [])
        self.assertIsNone(classify_market_type(nba_prop, nba_prop["markets"][0]))
        self.assertEqual(event_to_market_records(nba_prop), [])

    def test_sports_moneyline_uses_actual_close_time_not_week_later_end_date(self):
        cases = [
            (
                "nba",
                "nba-lakers-celtics-2026-06-07",
                "Los Angeles Lakers vs. Boston Celtics",
                '["Los Angeles Lakers","Boston Celtics"]',
            ),
            (
                "ufc",
                "ufc-fighter-a-fighter-b-2026-06-07",
                "Fighter A vs Fighter B",
                '["Fighter A","Fighter B"]',
            ),
        ]
        for league, slug, title, outcomes in cases:
            with self.subTest(league=league):
                event = {
                    "id": f"{league}-e1",
                    "slug": slug,
                    "title": title,
                    "closed": True,
                    "startTime": "2026-06-07T20:10:00Z",
                    "endDate": "2026-06-14T20:10:00Z",
                    "closedTime": "2026-06-08T00:13:19Z",
                    "finishedTimestamp": "2026-06-07T23:08:19.040393Z",
                    "tags": [{"slug": league}],
                    "markets": [
                        {
                            "conditionId": f"{league}-moneyline",
                            "question": title,
                            "outcomes": outcomes,
                            "outcomePrices": '["1","0"]',
                            "volume": "1005821.89",
                            "gameStartTime": "2026-06-07 20:10:00+00",
                            "endDate": "2026-06-14T20:10:00Z",
                            "umaEndDate": "2026-06-07T23:27:48Z",
                            "closedTime": "2026-06-07 23:27:48+00",
                        }
                    ],
                }

                record = event_to_market_records(event)[0]

                self.assertEqual(record["match_start_time"], "2026-06-07T20:10:00Z")
                self.assertEqual(record["market_start_time"], "2026-06-07T20:10:00Z")
                self.assertEqual(record["end_date"], "2026-06-07T23:27:48Z")
                self.assertEqual(record["league"], league)
        self.assertEqual(parse_dt("2026-06-07 20:10:00+00"), datetime(2026, 6, 7, 20, 10, tzinfo=timezone.utc))

    def test_sports_classifier_rejects_spread_totals_props_and_futures(self):
        event = {
            "title": "Los Angeles Lakers vs. Boston Celtics",
            "tags": [{"slug": "nba"}],
        }
        spread = {
            "conditionId": "spread",
            "question": "Spread: Los Angeles Lakers (-5.5)",
            "outcomes": '["Los Angeles Lakers","Boston Celtics"]',
        }
        total = {
            "conditionId": "total",
            "question": "Los Angeles Lakers vs. Boston Celtics: O/U 212.5",
            "outcomes": '["Over","Under"]',
        }
        player_points = {
            "conditionId": "points",
            "question": "Will LeBron James score over 24.5 points?",
            "outcomes": '["Yes","No"]',
        }
        future_event = {"title": "Who will Jon Jones fight next?", "tags": [{"slug": "ufc"}]}
        future = {
            "conditionId": "fight-next",
            "question": "Will Jon Jones fight Tom Aspinall next?",
            "outcomes": '["Yes","No"]',
        }

        self.assertIsNone(classify_market_type(event, spread))
        self.assertIsNone(classify_market_type(event, total))
        self.assertIsNone(classify_market_type(event, player_points))
        self.assertIsNone(classify_market_type(future_event, future))

    def test_market_record_keeps_real_match_start_time(self):
        event = {
            "id": "e1",
            "slug": "lol-a-b",
            "title": "LoL: A vs B",
            "closed": True,
            "volume": 50_000,
            "endDate": "2026-06-01T18:00:00Z",
            "startTime": "2026-06-01T12:00:00Z",
            "tags": [{"slug": "league-of-legends"}],
            "markets": [
                {
                    "conditionId": "C1",
                    "question": "LoL: A vs B",
                    "outcomes": '["A","B"]',
                    "outcomePrices": "[0,1]",
                    "gameStartTime": "2026-06-01 12:05:00+00",
                    "eventStartTime": "2026-06-01T12:00:00Z",
                    "endDate": "2026-06-01T18:00:00Z",
                }
            ],
        }

        record = event_to_market_record(event)

        self.assertEqual(record["match_start_time"], "2026-06-01T12:00:00Z")
        self.assertEqual(record["market_start_time"], "2026-06-01 12:05:00+00")

    def test_event_to_market_records_includes_winner_submarkets_and_excludes_props(self):
        event = {
            "id": "e1",
            "slug": "dota2-ty-bb4-2026-06-06",
            "title": "Dota 2: Team Yandex vs BetBoom Team (BO3) - BLAST Slam",
            "closed": False,
            "startTime": "2026-06-06T12:00:00Z",
            "tags": [{"slug": "dota-2"}],
            "markets": [
                {
                    "conditionId": "main",
                    "question": "Dota 2: Team Yandex vs BetBoom Team (BO3) - BLAST Slam",
                    "outcomes": '["Team Yandex","BetBoom Team"]',
                    "outcomePrices": "[0.4,0.6]",
                    "volume": 50_000,
                },
                {
                    "conditionId": "game1",
                    "question": "Dota 2: Team Yandex vs BetBoom Team - Game 1 Winner",
                    "outcomes": '["Team Yandex","BetBoom Team"]',
                    "outcomePrices": "[0.45,0.55]",
                    "volume": 8_000,
                },
                {
                    "conditionId": "kills",
                    "question": "Dota 2: Team Yandex vs BetBoom Team - Game 1 Total Kills",
                    "outcomes": '["Over","Under"]',
                    "outcomePrices": "[0.5,0.5]",
                    "volume": 20_000,
                },
            ],
        }

        records = {row["condition_id"]: row for row in event_to_market_records(event)}

        self.assertEqual(records["main"]["market_type"], "main_match")
        self.assertEqual(records["game1"]["market_type"], "game_winner")
        self.assertNotIn("kills", records)

    def test_market_classifier_accepts_cs_and_valorant_map_winner(self):
        cs_event = {
            "title": "Counter-Strike: 9z vs FlyQuest (BO1) - IEM Cologne",
            "tags": [{"slug": "counter-strike-2"}],
            "markets": [],
        }
        cs_market = {
            "conditionId": "map1",
            "question": "Counter-Strike: 9z vs FlyQuest - Map 1 Winner",
            "outcomes": '["9z","FlyQuest"]',
        }
        valorant_event = {
            "title": "Valorant: A vs B (BO3)",
            "tags": [{"slug": "valorant"}],
            "markets": [],
        }
        valorant_market = {
            "conditionId": "v1",
            "question": "Valorant: A vs B (BO3)",
            "outcomes": '["A","B"]',
        }
        valorant_map_market = {
            "conditionId": "v-map1",
            "question": "Valorant: A vs B - Map 1 Winner",
            "outcomes": '["A","B"]',
        }

        self.assertEqual(classify_market_type(cs_event, cs_market), "map_winner")
        # valorant 现为平级游戏:主盘 = main_match,Map N Winner = map_winner(同 cs2)。
        self.assertEqual(classify_market_type(valorant_event, valorant_market), "main_match")
        self.assertEqual(classify_market_type(valorant_event, valorant_map_market), "map_winner")

    def test_market_classifier_accepts_winner_alias_outcomes_but_rejects_prop_outcomes(self):
        event = {
            "title": "Dota 2: Team Spirit vs BetBoom Team (BO3) - BLAST Slam",
            "tags": [{"slug": "dota-2"}],
            "markets": [],
        }
        alias_market = {
            "conditionId": "g1",
            "question": "Dota 2: Team Spirit vs BetBoom Team - Winner of Game 1",
            "outcomes": '["Spirit","BetBoom"]',
        }
        prop_like_market = {
            "conditionId": "g1-prop",
            "question": "Dota 2: Team Spirit vs BetBoom Team - Game 1 Winner",
            "outcomes": '["Yes","No"]',
        }

        self.assertEqual(classify_market_type(event, alias_market), "game_winner")
        self.assertIsNone(classify_market_type(event, prop_like_market))

    def test_retry_after_parses_seconds_and_http_date_with_cap(self):
        future = datetime.now(timezone.utc) + timedelta(seconds=120)

        self.assertEqual(parse_retry_after("3", max_seconds=60), 3)
        self.assertEqual(parse_retry_after("120", max_seconds=60), 60)
        self.assertGreater(parse_retry_after(format_datetime(future, usegmt=True), max_seconds=60), 0)
        self.assertLessEqual(parse_retry_after(format_datetime(future, usegmt=True), max_seconds=60), 60)

    def test_rate_limiter_allows_burst_then_sleeps_outside_lock(self):
        now = [0.0]
        sleeps = []

        def clock():
            return now[0]

        def sleeper(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        limiter = RateLimiter(rate_per_second=2, burst=2, clock=clock, sleeper=sleeper)

        limiter.acquire()
        limiter.acquire()
        limiter.acquire()

        self.assertEqual(sleeps, [0.5])

    def test_client_retries_429_retry_after_then_succeeds(self):
        calls = []
        sleeps = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b'{"ok": true}'

        def fake_urlopen(_request, timeout):
            calls.append(timeout)
            if len(calls) == 1:
                raise urllib.error.HTTPError(
                    "https://example.test",
                    429,
                    "rate limited",
                    {"Retry-After": "99"},
                    BytesIO(),
                )
            return Response()

        original = urllib.request.urlopen
        urllib.request.urlopen = fake_urlopen
        try:
            client = PolymarketClient(
                timeout=7,
                retries=2,
                pause_seconds=0.5,
                max_retry_after_seconds=1,
                sleeper=sleeps.append,
                jitter=lambda _low, _high: 0,
            )
            result = client.get_json("https://example.test", "/x", {})
        finally:
            urllib.request.urlopen = original

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [1])

    def test_client_expands_list_query_params(self):
        urls = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b"[]"

        def fake_urlopen(request, timeout):
            urls.append(request.full_url)
            return Response()

        original = urllib.request.urlopen
        urllib.request.urlopen = fake_urlopen
        try:
            client = PolymarketClient(retries=0)
            client.gamma("/markets", condition_ids=["a", "b"], closed="true", limit=2)
        finally:
            urllib.request.urlopen = original

        self.assertIn("condition_ids=a", urls[0])
        self.assertIn("condition_ids=b", urls[0])
        self.assertIn("closed=true", urls[0])

    def test_closed_positions_requests_timestamp_descending_order_and_market_filter(self):
        calls = []

        class Client(PolymarketClient):
            def data(self, path, **params):
                calls.append((path, params))
                return []

        Client().closed_positions("0xabc", limit=50, offset=100, market=["m1", "m2"])

        self.assertEqual(calls[0][0], "/closed-positions")
        self.assertEqual(calls[0][1]["market"], "m1,m2")
        self.assertEqual(calls[0][1]["sortBy"], "TIMESTAMP")
        self.assertEqual(calls[0][1]["sortDirection"], "DESC")

    def test_event_pagination_stops_after_min_end_date(self):
        calls = []

        class Client(PolymarketClient):
            def list_events(self, *, closed, active=None, limit=100, offset=0, order="endDate", tag_slug="esports"):
                calls.append(offset)
                batches = {
                    0: [{"id": f"recent-{index}", "endDate": "2026-06-01T00:00:00Z"} for index in range(100)],
                    100: [{"id": f"old-{index}", "endDate": "2026-01-01T00:00:00Z"} for index in range(100)],
                    200: [{"id": f"older-{index}", "endDate": "2025-01-01T00:00:00Z"} for index in range(100)],
                }
                return batches.get(offset, [])

        rows = Client().list_events_paginated(
            closed=True,
            max_pages=10,
            min_end_date=datetime(2026, 3, 1, tzinfo=timezone.utc),
            tag_slugs=("esports",),
        )

        self.assertEqual(rows[0]["id"], "recent-0")
        self.assertEqual(rows[-1]["id"], "old-99")
        self.assertEqual(len(rows), 200)
        self.assertEqual(calls, [0, 100])

    def test_event_pagination_passes_max_end_date_filter(self):
        calls = []
        max_end_date = datetime(2026, 6, 7, tzinfo=timezone.utc)

        class Client(PolymarketClient):
            def list_events(
                self,
                *,
                closed,
                active=None,
                limit=100,
                offset=0,
                order="endDate",
                tag_slug="esports",
                max_end_date=None,
            ):
                calls.append(max_end_date)
                return []

        Client().list_events_paginated(
            closed=True,
            max_pages=1,
            max_end_date=max_end_date,
            tag_slugs=("mlb",),
        )

        self.assertEqual(calls, [max_end_date])

    def test_event_pagination_survives_page_error_returns_partial(self):
        # 单页抛错(如 Gamma 对超大 offset 返回 HTTP 422)不致命:该 tag 优雅收口,
        # 用已累积事件继续下一个 tag,而非炸穿整个采集流程(否则 collect 在校准/发现阶段崩)。
        calls = []

        class Client(PolymarketClient):
            def list_events(self, *, closed, active=None, limit=100, offset=0,
                            order="endDate", tag_slug="esports", max_end_date=None):
                calls.append((tag_slug, offset))
                if tag_slug == "cs2" and offset == 0:
                    return [{"id": f"cs2-{i}", "endDate": "2026-06-01T00:00:00Z"} for i in range(100)]
                if tag_slug == "cs2" and offset == 100:
                    raise RuntimeError("GET failed: ...&offset=100: HTTP Error 422: Unprocessable Entity")
                if tag_slug == "lol" and offset == 0:
                    return [{"id": "lol-0", "endDate": "2026-06-01T00:00:00Z"}]
                return []

        rows = Client().list_events_paginated(closed=True, max_pages=10, tag_slugs=("cs2", "lol"))
        ids = {r["id"] for r in rows}
        # cs2 第0页 100 条保留,第1页 422 被吞掉(不抛)→ 不影响 lol 继续被采。
        self.assertEqual(len(rows), 101)
        self.assertIn("cs2-99", ids)
        self.assertIn("lol-0", ids)
        self.assertEqual(calls, [("cs2", 0), ("cs2", 100), ("lol", 0)])

    def test_classification_set_keeps_only_settled_historical_main_matches(self):
        def esports_event(condition_id, end_date, title="Dota 2: A vs B", prices='["1","0"]'):
            return {
                "id": condition_id,
                "slug": condition_id,
                "title": title,
                "closed": True,
                "endDate": end_date,
                "tags": [{"slug": "dota-2"}],
                "markets": [
                    {
                        "conditionId": condition_id,
                        "question": title,
                        "outcomes": '["A","B"]',
                        "outcomePrices": prices,
                        "volume": 50_000,
                        "liquidity": 1_000,
                    }
                ],
            }

        rows = build_classification_set(
            [
                esports_event("recent", "2026-06-01T00:00:00Z"),
                esports_event("old", "2026-05-01T00:00:00Z"),
                esports_event("future", "2026-07-01T00:00:00Z"),
                esports_event("unsettled", "2026-06-01T00:00:00Z", prices='["0.5","0.5"]'),
                esports_event("prop", "2026-06-01T00:00:00Z", title="Will Team Falcons make a roster move before July?"),
            ],
            now=datetime(2026, 6, 5, tzinfo=timezone.utc),
        )

        self.assertEqual([row["condition_id"] for row in rows], ["recent", "old"])

    def test_sports_classification_set_applies_volume_floor_and_counts_leagues(self):
        def sports_event(condition_id, *, league, title, outcomes, volume):
            return {
                "id": condition_id,
                "slug": condition_id,
                "title": title,
                "closed": True,
                "endDate": "2026-06-01T00:00:00Z",
                "tags": [{"slug": league}],
                "markets": [
                    {
                        "conditionId": condition_id,
                        "question": title,
                        "outcomes": json.dumps(outcomes),
                        "outcomePrices": '["1","0"]',
                        "volume": volume,
                    }
                ],
            }

        rows = build_classification_set(
            [
                sports_event(
                    "nba-high",
                    league="nba",
                    title="Los Angeles Lakers vs. Boston Celtics",
                    outcomes=["Los Angeles Lakers", "Boston Celtics"],
                    volume=75_000,
                ),
                sports_event(
                    "nba-low",
                    league="nba",
                    title="Denver Nuggets vs. Miami Heat",
                    outcomes=["Denver Nuggets", "Miami Heat"],
                    volume=20_000,
                ),
                sports_event(
                    "ufc-high",
                    league="ufc",
                    title="Justin Gaethje vs Ilia Topuria",
                    outcomes=["Justin Gaethje", "Ilia Topuria"],
                    volume=230_000,
                ),
            ],
            now=datetime(2026, 6, 5, tzinfo=timezone.utc),
            sports_event_min_volume=50_000,
        )

        self.assertEqual({row["condition_id"] for row in rows}, {"nba-high", "ufc-high"})
        self.assertEqual({row["league"] for row in rows}, {"nba", "ufc"})

    def test_sports_market_start_prefers_event_start_time_before_game_start_time(self):
        record = event_to_market_records(
            {
                "id": "sports-start",
                "slug": "sports-start",
                "title": "Los Angeles Lakers vs. Boston Celtics",
                "startTime": "2026-06-01T11:00:00Z",
                "tags": [{"slug": "nba"}],
                "markets": [
                    {
                        "conditionId": "sports-start-market",
                        "question": "Los Angeles Lakers vs. Boston Celtics",
                        "outcomes": json.dumps(["Los Angeles Lakers", "Boston Celtics"]),
                        "outcomePrices": '["0.5","0.5"]',
                        "volume": 500_000,
                        "gameStartTime": "2026-06-01T10:00:00Z",
                        "eventStartTime": "2026-06-01T12:00:00Z",
                        "endDate": "2026-06-01T14:00:00Z",
                    }
                ],
            }
        )[0]

        self.assertEqual(record["match_start_time"], "2026-06-01T12:00:00Z")

    def test_discovery_slate_uses_progressive_window(self):
        markets = [market(f"m{i}", days_ago=10, volume=30_000) for i in range(35)]

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            target_markets=30,
        )

        self.assertEqual(len(slate), 35)
        self.assertEqual(meta["selected_lookback_days"], 14)

    def test_discovery_slate_default_main_quota_is_not_legacy_thirty_fifty(self):
        markets = [market(f"m{i}", days_ago=1, volume=300_000 - i * 1_000) for i in range(170)]

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
        )

        self.assertEqual(len(slate), 150)
        self.assertEqual(meta["target_markets"], 150)
        self.assertEqual(meta["max_markets_per_run"], 150)
        self.assertEqual(meta["total_selected_market_count"], 170)

    def test_discovery_slate_score_sort_can_prefer_recent_market_over_old_volume(self):
        markets = [
            market("old-big", days_ago=60, volume=100_000),
            market("recent-mid", days_ago=1, volume=80_000),
        ]

        slate, _meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            lookback_steps=(60,),
            target_markets=2,
            max_markets_per_run=1,
        )

        self.assertEqual([row["condition_id"] for row in slate], ["recent-mid"])

    def test_discovery_slate_score_sort_uses_stable_tiebreakers(self):
        markets = [
            market("b", days_ago=1, volume=80_000),
            market("a", days_ago=1, volume=80_000),
        ]

        slate, _meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            target_markets=2,
        )

        self.assertEqual([row["condition_id"] for row in slate], ["a", "b"])

    def test_discovery_slate_batches_volume_sorted_markets(self):
        markets = [market(f"m{i}", days_ago=1, volume=100_000 - i * 1_000) for i in range(10)]

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            target_markets=10,
            max_markets_per_run=3,
            market_offset=3,
        )

        self.assertEqual([row["condition_id"] for row in slate], ["m3", "m4", "m5"])
        self.assertEqual(meta["total_selected_market_count"], 10)
        self.assertEqual(meta["market_offset"], 3)

    def test_sports_discovery_slate_balances_nba_and_ufc_volume_buckets(self):
        markets = []
        for index in range(60):
            row = market(f"nba{index}", days_ago=1, volume=1_000_000 - index * 10_000)
            row["category"] = "sports"
            row["league"] = "nba"
            row["market_type"] = "main_match"
            markets.append(row)
        for index in range(35):
            row = market(f"ufc{index}", days_ago=1, volume=90_000 - index * 2_000)
            row["category"] = "sports"
            row["league"] = "ufc"
            row["market_type"] = "main_match"
            markets.append(row)

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            league_target_markets={"nba": 50, "ufc": 30},
            league_min_market_volumes={"nba": 250_000, "ufc": 25_000},
            league_fallback_min_market_volumes={"nba": 100_000, "ufc": 10_000},
        )

        self.assertEqual(sum(1 for row in slate if row["league"] == "nba"), 50)
        self.assertEqual(sum(1 for row in slate if row["league"] == "ufc"), 30)
        self.assertEqual(meta["selected_by_league"], {"nba": 50, "ufc": 30})
        self.assertEqual(meta["leagues"]["nba"]["selected_min_market_volume"], 250_000)
        self.assertEqual(meta["leagues"]["ufc"]["selected_min_market_volume"], 25_000)
        self.assertIn("ufc29", {row["condition_id"] for row in slate})

    def test_discovery_slate_gives_game_and_map_winners_independent_quotas(self):
        markets = []
        for i in range(80):
            row = market(f"game{i}", days_ago=1, volume=100_000 - i * 1_000)
            row["market_type"] = "game_winner"
            markets.append(row)
        for i in range(20):
            row = market(f"map{i}", days_ago=1, volume=10_000 - i * 100)
            row["market_type"] = "map_winner"
            markets.append(row)

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            target_markets=0,
            game_winner_target_markets=60,
            map_winner_target_markets=20,
            game_winner_max_markets_per_run=60,
            map_winner_max_markets_per_run=20,
        )

        self.assertEqual(meta["selected_by_market_type"]["game_winner"], 60)
        self.assertEqual(meta["selected_by_market_type"]["map_winner"], 20)
        self.assertEqual(sum(1 for row in slate if row["market_type"] == "map_winner"), 20)

    def test_esports_discovery_slate_balances_game_family_buckets(self):
        markets = []
        for game_family in ("lol", "cs2", "dota2"):
            for i in range(120):
                row = market(f"{game_family}-main-{i}", days_ago=1, volume=1_000_000 - i * 1_000)
                row["category"] = "esports"
                row["game_family"] = game_family
                row["market_type"] = "main_match"
                markets.append(row)
        for game_family in ("lol", "dota2"):
            for i in range(60):
                row = market(f"{game_family}-game-{i}", days_ago=1, volume=100_000 - i * 1_000)
                row["category"] = "esports"
                row["game_family"] = game_family
                row["market_type"] = "game_winner"
                markets.append(row)
        for i in range(60):
            row = market(f"cs2-map-{i}", days_ago=1, volume=50_000 - i * 500)
            row["category"] = "esports"
            row["game_family"] = "cs2"
            row["market_type"] = "map_winner"
            markets.append(row)

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
        )

        counts = {}
        for row in slate:
            key = f"{row['game_family']}:{row['market_type']}"
            counts[key] = counts.get(key, 0) + 1

        self.assertEqual(counts["lol:main_match"], 100)
        self.assertEqual(counts["cs2:main_match"], 100)
        self.assertEqual(counts["dota2:main_match"], 100)
        self.assertEqual(counts["lol:game_winner"], 50)
        self.assertEqual(counts["dota2:game_winner"], 50)
        self.assertEqual(counts["cs2:map_winner"], 50)
        self.assertEqual(meta["selected_by_game_market_type"], counts)
        # valorant 现已是配置桶(本用例未提供 valorant 市场 → 选中 0,不进 counts)。
        self.assertIn("valorant:main_match", meta["game_market_buckets"])

    def test_candidate_wallets_use_participation_or_large_size(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xAAA", "size": 100, "price": 0.6, "timestamp": 100},
                {"proxyWallet": "0xBIG", "size": 3000, "price": 0.5, "timestamp": 110},
            ],
            "m2": [{"proxyWallet": "0xAAA", "size": 100, "price": 0.7, "timestamp": 120}],
            "m3": [{"proxyWallet": "0xAAA", "size": 100, "price": 0.8, "timestamp": 130}],
        }

        candidates = build_candidate_wallets(
            trades_by_market,
            min_trade_cash=50,
            participation_threshold=3,
            total_cash_threshold=5_000,
            single_market_cash_threshold=1_000,
        )

        by_wallet = {row["wallet"]: row for row in candidates}
        self.assertIn("0xaaa", by_wallet)
        self.assertIn("0xbig", by_wallet)
        self.assertEqual(by_wallet["0xaaa"]["candidate_reasons"], ["high_participation"])
        self.assertEqual(by_wallet["0xbig"]["candidate_reasons"], ["large_size"])

    def test_large_size_candidate_is_profiled_before_high_frequency_only_candidate(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xFREQ", "size": 100, "price": 0.6, "timestamp": 100},
                {"proxyWallet": "0xBIG", "size": 3000, "price": 0.5, "timestamp": 110},
            ],
            "m2": [{"proxyWallet": "0xFREQ", "size": 100, "price": 0.7, "timestamp": 120}],
            "m3": [{"proxyWallet": "0xFREQ", "size": 100, "price": 0.8, "timestamp": 130}],
            "m4": [{"proxyWallet": "0xFREQ", "size": 100, "price": 0.9, "timestamp": 140}],
        }

        candidates = build_candidate_wallets(
            trades_by_market,
            min_trade_cash=50,
            participation_threshold=3,
            total_cash_threshold=5_000,
            single_market_cash_threshold=1_000,
        )

        self.assertEqual(candidates[0]["wallet"], "0xbig")

    def test_candidate_records_two_sided_and_churn_markets(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xCHURN", "side": "BUY", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 1},
                {"proxyWallet": "0xCHURN", "side": "BUY", "size": 1000, "price": 0.5, "outcome": "B", "timestamp": 2},
            ],
            "m2": [
                {"proxyWallet": "0xCHURN", "size": 100, "price": 0.5, "outcome": "A", "timestamp": i}
                for i in range(3, 25)
            ],
        }

        candidates = build_candidate_wallets(
            trades_by_market,
            min_trade_cash=50,
            participation_threshold=2,
            single_market_cash_threshold=1_000,
        )

        candidate = {row["wallet"]: row for row in candidates}["0xchurn"]
        self.assertEqual(candidate["two_sided_market_count"], 1)
        self.assertEqual(candidate["high_churn_market_count"], 1)

    def test_candidate_two_sided_counts_opposite_buys_not_sells(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xONEWAY", "side": "BUY", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 1},
                {"proxyWallet": "0xONEWAY", "side": "SELL", "size": 500, "price": 0.5, "outcome": "B", "timestamp": 2},
            ],
            "m2": [
                {"proxyWallet": "0xTWOWAY", "side": "BUY", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 1},
                {"proxyWallet": "0xTWOWAY", "side": "BUY", "size": 500, "price": 0.5, "outcome": "B", "timestamp": 2},
            ],
        }

        candidates = build_candidate_wallets(
            trades_by_market,
            min_trade_cash=50,
            participation_threshold=1,
            single_market_cash_threshold=1_000,
        )

        by_wallet = {row["wallet"]: row for row in candidates}
        self.assertEqual(by_wallet["0xoneway"]["two_sided_market_count"], 0)
        self.assertEqual(by_wallet["0xtwoway"]["two_sided_market_count"], 1)

    def test_candidate_records_entry_timing_against_match_start(self):
        trades_by_market = {
            "m1": [{"proxyWallet": "0xEARLY", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 100}],
            "m2": [{"proxyWallet": "0xEARLY", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 990}],
        }
        market_start_times = {"m1": 10900, "m2": 1000}

        candidates = build_candidate_wallets(
            trades_by_market,
            market_start_times=market_start_times,
            min_trade_cash=50,
            participation_threshold=2,
            single_market_cash_threshold=100,
        )

        candidate = {row["wallet"]: row for row in candidates}["0xearly"]
        self.assertEqual(candidate["late_entry_market_count"], 1)
        self.assertEqual(candidate["early_entry_market_count"], 1)
        self.assertEqual(candidate["tail_entry_market_count"], 0)
        self.assertEqual(candidate["median_last_entry_hours_to_start"], 1.50138889)

    def test_tail_entry_requires_high_buy_average_price_not_timing(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xLOW", "size": 1000, "price": 0.45, "outcome": "A", "timestamp": 100},
                {"proxyWallet": "0xLOW", "size": 1000, "price": 0.55, "outcome": "A", "timestamp": 1200},
            ],
            "m2": [
                {"proxyWallet": "0xCHASE", "size": 1000, "price": 0.80, "outcome": "A", "timestamp": 1200},
            ],
            "m3": [
                {"proxyWallet": "0xEARLYCHASE", "size": 1000, "price": 0.80, "outcome": "A", "timestamp": 100},
            ],
        }
        market_start_times = {"m1": 1000, "m2": 1000, "m3": 10000}

        candidates = build_candidate_wallets(
            trades_by_market,
            market_start_times=market_start_times,
            min_trade_cash=50,
            participation_threshold=1,
            single_market_cash_threshold=100,
        )

        by_wallet = {row["wallet"]: row for row in candidates}
        self.assertEqual(by_wallet["0xlow"]["late_entry_market_count"], 1)
        self.assertEqual(by_wallet["0xlow"]["tail_entry_market_count"], 0)
        self.assertEqual(by_wallet["0xlow"]["avg_entry_price"], 0.5)
        self.assertEqual(by_wallet["0xchase"]["late_entry_market_count"], 1)
        self.assertEqual(by_wallet["0xchase"]["tail_entry_market_count"], 1)
        self.assertEqual(by_wallet["0xchase"]["avg_entry_price"], 0.8)
        self.assertEqual(by_wallet["0xearlychase"]["late_entry_market_count"], 0)
        self.assertEqual(by_wallet["0xearlychase"]["tail_entry_market_count"], 1)
        self.assertEqual(by_wallet["0xearlychase"]["avg_entry_price"], 0.8)

    def test_tail_entry_ignores_late_sells(self):
        trades_by_market = {
            "m1": [
                {
                    "proxyWallet": "0xSELLTAKE",
                    "side": "BUY",
                    "size": 1000,
                    "price": 0.40,
                    "outcome": "A",
                    "timestamp": 100,
                },
                {
                    "proxyWallet": "0xSELLTAKE",
                    "side": "SELL",
                    "size": 500,
                    "price": 0.90,
                    "outcome": "A",
                    "timestamp": 990,
                },
            ]
        }
        market_start_times = {"m1": 10000}

        candidates = build_candidate_wallets(
            trades_by_market,
            market_start_times=market_start_times,
            min_trade_cash=50,
            participation_threshold=1,
            single_market_cash_threshold=100,
        )

        candidate = {row["wallet"]: row for row in candidates}["0xselltake"]
        self.assertEqual(candidate["late_entry_market_count"], 0)
        self.assertEqual(candidate["tail_entry_market_count"], 0)
        self.assertEqual(candidate["avg_entry_price"], 0.4)

    def test_candidate_wallets_include_per_type_discovery_metrics(self):
        trades_by_market = {}
        market_type_by_id = {}

        def add_market(condition_id, market_type, wallet, cash):
            market_type_by_id[condition_id] = market_type
            trades_by_market[condition_id] = [
                {
                    "proxyWallet": wallet,
                    "side": "BUY",
                    "price": 0.5,
                    "size": cash / 0.5,
                    "cash": cash,
                    "outcomeIndex": 0,
                    "timestamp": 100,
                }
            ]

        for index in range(3):
            add_market(f"main{index}", "main_match", "0xmix", 2_000)
        for index in range(2):
            add_market(f"game{index}", "game_winner", "0xmix", 900)

        candidates = build_candidate_wallets(
            trades_by_market,
            market_type_by_id=market_type_by_id,
            participation_threshold=1,
            total_cash_threshold=999_999,
            single_market_cash_threshold=999_999,
        )

        candidate = {row["wallet"]: row for row in candidates}["0xmix"]
        self.assertEqual(candidate["participated_market_count"], 5)
        self.assertEqual(candidate["per_type_candidate"]["main_match"]["participated_market_count"], 3)
        self.assertEqual(candidate["per_type_candidate"]["main_match"]["avg_market_cash"], 2_000)
        self.assertEqual(candidate["per_type_candidate"]["game_winner"]["participated_market_count"], 2)
        self.assertEqual(candidate["per_type_candidate"]["game_winner"]["avg_market_cash"], 900)

    def test_candidate_wallets_can_use_per_type_caps_before_union(self):
        trades_by_market = {
            "main": [
                {"proxyWallet": "0xMAIN", "side": "BUY", "size": 20_000, "price": 0.5, "outcome": "A", "timestamp": 1},
            ],
            "game": [
                {"proxyWallet": "0xGAME", "side": "BUY", "size": 2_000, "price": 0.5, "outcome": "A", "timestamp": 1},
            ],
        }
        market_type_by_id = {"main": "main_match", "game": "game_winner"}

        global_capped = build_candidate_wallets(
            trades_by_market,
            market_type_by_id=market_type_by_id,
            min_trade_cash=50,
            participation_threshold=1,
            max_candidate_wallets=1,
        )
        per_type_capped = build_candidate_wallets(
            trades_by_market,
            market_type_by_id=market_type_by_id,
            min_trade_cash=50,
            participation_threshold=1,
            max_candidate_wallets=1,
            candidate_wallets_per_market_type=1,
        )

        self.assertEqual([row["wallet"] for row in global_capped], ["0xmain"])
        self.assertEqual({row["wallet"] for row in per_type_capped}, {"0xmain", "0xgame"})

    def test_candidate_wallets_can_be_built_from_top_holders(self):
        holders_by_market = {
            "m1": [
                {
                    "holders": [
                        {"proxyWallet": "0xAAA", "amount": 1000, "outcomeIndex": 0},
                        {"proxyWallet": "0xBIG", "amount": 5000, "outcomeIndex": 0},
                    ]
                },
                {
                    "holders": [
                        {"proxyWallet": "0xAAA", "amount": 500, "outcomeIndex": 1},
                    ]
                },
            ],
            "m2": [
                {"holders": [{"proxyWallet": "0xAAA", "amount": 300, "outcomeIndex": 0}]},
                {"holders": []},
            ],
        }
        prices_by_market = {"m1": [0.4, 0.6], "m2": [0.5, 0.5]}

        candidates = build_candidate_wallets_from_holders(
            holders_by_market,
            prices_by_market,
            participation_threshold=2,
            total_usd_threshold=2_000,
            single_market_usd_threshold=1_500,
        )

        by_wallet = {row["wallet"]: row for row in candidates}
        self.assertEqual(by_wallet["0xaaa"]["candidate_reasons"], ["high_participation"])
        self.assertEqual(by_wallet["0xbig"]["candidate_reasons"], ["large_size"])
        self.assertEqual(by_wallet["0xbig"]["max_single_market_usd"], 2000)

    def test_closed_positions_are_filtered_by_classification_set(self):
        positions = [
            {"conditionId": "m1", "totalBought": 100, "realizedPnl": 20, "avgPrice": 0.95, "timestamp": 10},
            {"conditionId": "m2", "totalBought": 100, "realizedPnl": -10, "avgPrice": 0.45, "timestamp": 20},
            {"conditionId": "other", "totalBought": 10_000, "realizedPnl": 9_000, "timestamp": 30},
        ]

        summary = summarize_closed_positions(positions, {"m1", "m2"}, now_ts=100)

        self.assertEqual(summary["esports_closed_count"], 2)
        self.assertEqual(summary["esports_condition_ids"], ["m1", "m2"])
        self.assertEqual(summary["esports_realized_pnl"], 10)
        self.assertEqual(summary["esports_total_bought"], 200)
        self.assertEqual(summary["esports_total_cost"], 140)
        self.assertEqual(summary["avg_profit_per_share"], 0.05)
        self.assertEqual(summary["esports_roi"], 0.07142857)
        self.assertEqual(summary["positive_market_rate"], 0.5)
        self.assertEqual(summary["avg_entry_price"], 0.7)
        # v17:median_entry_price 只算可跟价区(≤0.85),0.95 那笔被剔 → 只剩 0.45;全价区另存 _full。
        self.assertEqual(summary["median_entry_price"], 0.45)
        self.assertEqual(summary["median_entry_price_full"], 0.7)
        self.assertEqual(summary["high_price_entry_rate"], 0.5)
        self.assertEqual(summary["low_edge_profit_rate"], 0.0)

    def test_void_closed_positions_are_excluded_from_win_rate_and_roi(self):
        positions = [
            {"conditionId": "m1", "totalBought": 100, "realizedPnl": 20, "curPrice": 1, "timestamp": 10},
            {"conditionId": "m2", "totalBought": 100, "realizedPnl": 0, "curPrice": 1, "timestamp": 20},
        ]

        summary = summarize_closed_positions(positions, {"m1", "m2"}, now_ts=100)

        self.assertEqual(summary["esports_closed_count"], 1)
        self.assertEqual(summary["neutral_market_count"], 1)
        self.assertEqual(summary["positive_market_rate"], 1.0)
        self.assertEqual(summary["esports_roi"], 0.2)

    def test_closed_position_roi_uses_cost_basis_not_share_count(self):
        positions = [
            {"conditionId": "m1", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.5, "timestamp": 10},
        ]

        summary = summarize_closed_positions(positions, {"m1"}, now_ts=100)

        self.assertEqual(summary["esports_total_cost"], 50)
        self.assertEqual(summary["avg_profit_per_share"], 0.5)
        self.assertEqual(summary["esports_roi"], 1.0)
        self.assertEqual(summary["median_market_roi"], 1.0)

    def test_winning_outcome_index_reads_settled_binary_prices(self):
        self.assertEqual(winning_outcome_index({"outcome_prices": [1, 0]}), 0)
        self.assertEqual(winning_outcome_index({"outcome_prices": [0, 1]}), 1)
        self.assertIsNone(winning_outcome_index({"outcome_prices": [0.45, 0.55]}))

    def test_reconstruct_closed_positions_accounts_for_sell_proceeds_and_hedges(self):
        market_records = {
            "m1": {"condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.0, 1.0]},
            "m2": {"condition_id": "m2", "outcomes": ["A", "B"], "outcome_prices": [1.0, 0.0]},
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 100},
            {"conditionId": "m1", "side": "SELL", "outcomeIndex": 1, "size": 80, "price": 0.7, "timestamp": 120},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 0, "size": 40, "price": 0.6, "timestamp": 130},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 1, "size": 20, "price": 0.3, "timestamp": 131},
        ]

        positions, behavior = reconstruct_closed_positions(trades, market_records)
        by_market = {row["conditionId"]: row for row in positions}

        self.assertEqual(by_market["m1"]["realizedPnl"], 60)
        self.assertEqual(by_market["m1"]["holdPnl"], 60)
        self.assertEqual(by_market["m1"]["actualPnl"], 36)
        self.assertEqual(by_market["m1"]["netCost"], -16)
        self.assertEqual(by_market["m1"]["netPositionByOutcome"], {"1": 20.0})
        self.assertEqual(by_market["m2"]["realizedPnl"], 10)
        self.assertEqual(by_market["m2"]["netPositionByOutcome"], {"0": 40.0, "1": 20.0})
        self.assertTrue(behavior["m1"]["sold_before_resolution"])
        self.assertTrue(behavior["m2"]["two_sided"])

    def test_reconstruct_closed_positions_scores_hold_pnl_and_keeps_actual_pnl(self):
        market_records = {
            "win": {"condition_id": "win", "outcomes": ["A", "B"], "outcome_prices": [0.0, 1.0]},
            "loss": {"condition_id": "loss", "outcomes": ["A", "B"], "outcome_prices": [1.0, 0.0]},
        }
        trades = [
            {"conditionId": "win", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 100},
            {"conditionId": "win", "side": "SELL", "outcomeIndex": 1, "size": 100, "price": 0.999, "timestamp": 120},
            {"conditionId": "loss", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 130},
            {"conditionId": "loss", "side": "SELL", "outcomeIndex": 1, "size": 100, "price": 0.5, "timestamp": 140},
        ]

        positions, behavior = reconstruct_closed_positions(trades, market_records)
        by_market = {row["conditionId"]: row for row in positions}

        self.assertAlmostEqual(by_market["win"]["holdPnl"], 60.0)
        self.assertAlmostEqual(by_market["win"]["actualPnl"], 59.9)
        self.assertAlmostEqual(by_market["win"]["actualMinusHoldPnl"], -0.1)
        self.assertAlmostEqual(by_market["win"]["actualMinusHoldPnlRate"], -0.00166667)
        self.assertEqual(by_market["win"]["realizedPnl"], by_market["win"]["holdPnl"])
        self.assertAlmostEqual(by_market["loss"]["holdPnl"], -40.0)
        self.assertAlmostEqual(by_market["loss"]["actualPnl"], 10.0)
        self.assertAlmostEqual(by_market["loss"]["actualMinusHoldPnl"], 50.0)
        self.assertIsNone(by_market["loss"]["actualMinusHoldPnlRate"])
        self.assertEqual(by_market["loss"]["realizedPnl"], by_market["loss"]["holdPnl"])
        self.assertFalse(behavior["win"]["sold_before_resolution"])
        self.assertTrue(behavior["loss"]["sold_before_resolution"])

    def test_trade_reconstruction_ignores_near_resolved_winner_exits_as_swing(self):
        market_records = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.0, 1.0],
                "market_type": "game_winner",
            },
            "m2": {
                "condition_id": "m2",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.0, 1.0],
                "market_type": "game_winner",
            },
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 100},
            {"conditionId": "m1", "side": "SELL", "outcomeIndex": 1, "size": 100, "price": 0.999, "timestamp": 120},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 130},
            {"conditionId": "m2", "side": "SELL", "outcomeIndex": 1, "size": 80, "price": 0.7, "timestamp": 140},
        ]

        summary = summarize_trade_reconstructed_positions(trades, market_records, now_ts=200)

        self.assertEqual(summary["sold_before_resolution_market_count"], 1)
        self.assertEqual(summary["sold_before_resolution_market_rate"], 0.5)

    def test_trade_reconstruction_tracks_first_buy_direction_accuracy(self):
        market_records = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["A", "B"],
                "outcome_prices": [1.0, 0.0],
                "market_type": "game_winner",
                "game_family": "lol",
            },
            "m2": {
                "condition_id": "m2",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.0, 1.0],
                "market_type": "game_winner",
                "game_family": "lol",
            },
            "m3": {
                "condition_id": "m3",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.0, 1.0],
                "market_type": "game_winner",
                "game_family": "lol",
            },
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 0, "size": 100, "price": 0.45, "timestamp": 100},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 0, "size": 100, "price": 0.45, "timestamp": 200},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 1, "size": 20, "price": 0.20, "timestamp": 210},
            {"conditionId": "m3", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.45, "timestamp": 300},
        ]

        positions, behavior = reconstruct_closed_positions(trades, market_records)
        summary = summarize_trade_reconstructed_positions(trades, market_records, now_ts=400)

        by_market = {row["conditionId"]: row for row in positions}
        self.assertTrue(by_market["m1"]["firstBuyWon"])
        self.assertFalse(by_market["m2"]["firstBuyWon"])
        self.assertTrue(by_market["m3"]["firstBuyWon"])
        self.assertEqual(behavior["m2"]["first_buy_outcome_index"], 0)
        self.assertFalse(behavior["m2"]["first_buy_won"])
        self.assertEqual(summary["first_direction_market_count"], 3)
        self.assertEqual(summary["first_direction_win_count"], 2)
        self.assertAlmostEqual(summary["first_direction_win_rate"], 2 / 3)
        self.assertEqual(summary["per_game_type"]["lol:game_winner"]["first_direction_win_count"], 2)
        self.assertAlmostEqual(summary["per_game_type"]["lol:game_winner"]["first_direction_win_rate"], 2 / 3)

    def test_reconstruct_closed_positions_skips_incomplete_sell_history(self):
        market_records = {
            "m1": {"condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [1.0, 0.0]},
        }
        trades = [
            {"conditionId": "m1", "side": "SELL", "outcomeIndex": 0, "size": 100, "price": 0.8, "timestamp": 100},
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 0, "size": 20, "price": 0.5, "timestamp": 110},
        ]

        positions, behavior = reconstruct_closed_positions(trades, market_records)

        self.assertEqual(positions, [])
        self.assertEqual(behavior, {})

    def test_reconstruct_closed_positions_prefers_trade_cash_for_cost(self):
        market_records = {
            "m1": {"condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [1.0, 0.0]},
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 0, "size": 100, "price": 0.5, "cash": 49, "timestamp": 100},
        ]

        positions, _behavior = reconstruct_closed_positions(trades, market_records)

        self.assertEqual(positions[0]["buyCost"], 49)
        self.assertEqual(positions[0]["avgPrice"], 0.49)
        self.assertEqual(positions[0]["realizedPnl"], 51)

    def test_trade_reconstruction_counts_losing_buy_missing_from_closed_positions(self):
        market_records = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["G2", "Monte"],
                "outcome_prices": [1.0, 0.0],
                "market_type": "main_match",
            }
        }
        trades = [
            {
                "conditionId": "m1",
                "side": "BUY",
                "outcome": "Monte",
                "outcomeIndex": 1,
                "size": 10000,
                "price": 0.3,
                "timestamp": 100,
            }
        ]

        summary = summarize_trade_reconstructed_positions(trades, market_records, now_ts=200)

        self.assertEqual(summary["data_quality"]["source"], "trade_reconstruction")
        self.assertEqual(summary["trade_reconstructed_sample_count"], 1)
        self.assertEqual(summary["esports_closed_count"], 1)
        self.assertEqual(summary["esports_win_count"], 0)
        self.assertEqual(summary["esports_loss_count"], 1)
        self.assertEqual(summary["esports_realized_pnl"], -3000)
        self.assertEqual(summary["esports_total_cost"], 3000)
        self.assertEqual(summary["sold_before_resolution_market_count"], 0)

    def test_trade_reconstruction_scores_material_sell_and_tracks_behavior(self):
        market_records = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.0, 1.0],
                "market_type": "game_winner",
            }
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 1, "size": 100, "price": 0.4, "timestamp": 100},
            {"conditionId": "m1", "side": "SELL", "outcomeIndex": 1, "size": 80, "price": 0.7, "timestamp": 120},
        ]

        summary = summarize_trade_reconstructed_positions(trades, market_records, now_ts=200)

        self.assertEqual(summary["esports_closed_count"], 1)
        self.assertEqual(summary["esports_win_count"], 1)
        self.assertEqual(summary["esports_realized_pnl"], 60)
        self.assertEqual(summary["hold_pnl"], 60)
        self.assertEqual(summary["actual_pnl"], 36)
        self.assertEqual(summary["actual_minus_hold_pnl"], -24)
        self.assertEqual(summary["neutral_market_count"], 0)
        self.assertEqual(summary["sold_before_resolution_market_count"], 1)
        self.assertEqual(summary["sold_before_resolution_market_rate"], 1.0)
        self.assertEqual(summary["per_type"]["game_winner"]["sold_before_resolution_market_count"], 1)

    def test_wallet_rating_is_bucketed_by_market_type(self):
        positions = [
            {
                "conditionId": f"g{i}",
                "totalBought": 1200,
                "realizedPnl": 720,
                "avgPrice": 0.4,
                "timestamp": 100 + i,
            }
            for i in range(14)
        ]
        summary = summarize_closed_positions(
            positions,
            {f"g{i}" for i in range(14)},
            condition_type_by_id={f"g{i}": "game_winner" for i in range(14)},
            now_ts=200,
        )

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["grade"], "A")
        self.assertEqual(rated["eligible_market_types"], ["game_winner"])
        self.assertEqual(rated["per_type_grades"]["game_winner"]["grade"], "A")
        self.assertNotIn("main_match", rated["eligible_market_types"])

    def test_wallet_rating_keeps_multiple_game_market_buckets_on_one_profile(self):
        positions = [
            {
                "conditionId": f"cs{i}",
                "totalBought": 1200,
                "realizedPnl": 720,
                "avgPrice": 0.4,
                "timestamp": 100 + i,
            }
            for i in range(14)
        ] + [
            {
                "conditionId": f"d{i}",
                "totalBought": 900,
                "realizedPnl": 540,
                "avgPrice": 0.4,
                "timestamp": 200 + i,
            }
            for i in range(14)
        ]
        condition_ids = {row["conditionId"] for row in positions}
        summary = summarize_closed_positions(
            positions,
            condition_ids,
            condition_type_by_id={condition_id: "main_match" for condition_id in condition_ids},
            condition_game_family_by_id={
                **{f"cs{i}": "cs2" for i in range(14)},
                **{f"d{i}": "dota2" for i in range(14)},
            },
            now_ts=300,
        )

        rated = classify_wallet(summary, now_ts=300)

        self.assertEqual(rated["grade"], "A")
        self.assertEqual(rated["eligible_market_types"], ["main_match"])
        self.assertEqual(rated["eligible_buckets"], ["CS2:main_match".lower(), "dota2:main_match"])
        self.assertEqual(rated["eligible_bucket_labels"], ["CS2 主盘", "Dota2 主盘"])
        self.assertEqual(rated["eligible_game_families"], ["cs2", "dota2"])
        self.assertEqual(rated["per_game_type_grades"]["cs2:main_match"]["grade"], "A")
        self.assertEqual(rated["per_game_type_grades"]["dota2:main_match"]["grade"], "A")

    def test_effective_sample_floor_is_uniform_twelve(self):
        # v16:n_eff 下限统一 12(不再主盘/子盘分档)。
        def rate(count, market_type):
            positions = [
                {"conditionId": f"g{i}", "totalBought": 2500, "realizedPnl": 1500,
                 "avgPrice": 0.5, "timestamp": 100 + i}
                for i in range(count)
            ]
            summary = summarize_closed_positions(
                positions,
                {f"g{i}" for i in range(count)},
                condition_type_by_id={f"g{i}": market_type for i in range(count)},
                condition_game_family_by_id={f"g{i}": "cs2" for i in range(count)},
                now_ts=200,
            )
            return classify_wallet(summary, now_ts=200)

        # 子盘与主盘同一道 12 门:11 不够、13 够。
        self.assertIn("thin_sample", rate(11, "game_winner")["per_game_type_grades"]["cs2:game_winner"]["reasons"])
        self.assertEqual(rate(13, "game_winner")["per_game_type_grades"]["cs2:game_winner"]["grade"], "A")
        self.assertIn("thin_sample", rate(11, "main_match")["per_game_type_grades"]["cs2:main_match"]["reasons"])
        self.assertEqual(rate(13, "main_match")["per_game_type_grades"]["cs2:main_match"]["grade"], "A")

    def test_closed_position_wilson_uses_80_percent_confidence(self):
        positions = []
        for index in range(11):
            positions.append(
                {
                    "conditionId": f"m{index}",
                    "totalBought": 100,
                    "realizedPnl": 60,
                    "avgPrice": 0.5,
                    "timestamp": 100 + index,
                }
            )
        for index in range(11, 13):
            positions.append(
                {
                    "conditionId": f"m{index}",
                    "totalBought": 100,
                    "realizedPnl": -50,
                    "avgPrice": 0.5,
                    "timestamp": 100 + index,
                }
            )

        summary = summarize_closed_positions(positions, {f"m{index}" for index in range(13)}, now_ts=200)

        self.assertEqual(summary["wilson_z"], 1.28)
        self.assertAlmostEqual(summary["wilson_win_rate_lower_bound"], 0.68063869)

    def test_thin_edge_high_price_wallet_is_not_a_grade(self):
        # Bought at 0.98 with razor-thin profit → tiny capital_weighted_edge (~0.02).
        # Not excluded (edge > 0) but well below the A edge floor → graded down, roi soft.
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 2, "avgPrice": 0.98, "timestamp": 100 + i}
            for i in range(20)
        ]
        summary = summarize_closed_positions(positions, {f"m{i}" for i in range(20)}, now_ts=200)
        summary["bot_like_score"] = 0

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["low_edge_profit_rate"], 1.0)
        self.assertEqual(rated["high_price_entry_rate"], 1.0)
        self.assertNotEqual(rated["grade"], "A")
        self.assertIn("low_roi", rated["reasons"])
        self.assertNotIn("low_historical_roi", rated["reasons"])

    def test_wallet_rating_rejects_high_roi_without_stability(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": -0.01,
            "positive_market_rate": 0.45,
            "esports_loss_count": 11,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.05,
            "last_esports_trade_at": 100,
            "bot_like_score": 10,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "C")
        self.assertIn("weak_edge_lb", rated["reasons"])  # v17:edge_lb=wilson_lb(0.45,20)−0.55 负 → 唯一质量门挡掉

    def test_low_frequency_perfect_wallet_is_not_a_grade(self):
        summary = {
            "esports_closed_count": 4,
            "esports_realized_pnl": 2_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 1.0,
            "wilson_win_rate_lower_bound": 0.51,
            "esports_loss_count": 0,
            "esports_total_bought": 5_000,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.10,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "C")
        self.assertIn("thin_sample", rated["reasons"])

    def test_effective_sample_floor_is_twelve_markets(self):
        base_summary = {
            "esports_realized_pnl": 2_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 1.0,
            "wilson_win_rate_lower_bound": 0.70,
            "esports_loss_count": 0,
            "esports_total_bought": 8_000,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.15,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        thin = classify_wallet({**base_summary, "esports_closed_count": 11}, now_ts=100 + 86400)
        qualified = classify_wallet({**base_summary, "esports_closed_count": 12}, now_ts=100 + 86400)

        self.assertEqual(thin["grade"], "C")
        self.assertIn("thin_sample", thin["reasons"])
        self.assertEqual(qualified["grade"], "A")
        self.assertNotIn("thin_sample", qualified["reasons"])

    def test_big_sample_wallet_with_some_losses_can_be_a_grade(self):
        summary = {
            "esports_closed_count": 46,
            "esports_realized_pnl": 70_000,
            "median_market_roi": 0.45,
            "positive_market_rate": 44 / 46,
            "wilson_win_rate_lower_bound": 0.85,
            "esports_loss_count": 2,
            "esports_total_bought": 155_000,
            "median_entry_price": 0.51,
            "capital_weighted_edge": 0.30,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertIn("has_losses", rated["reasons"])
        self.assertEqual(rated["entry_edge"], 0.34)

    def test_marginal_win_rate_wallet_is_not_a_grade(self):
        # 准确率轴 = Wilson 下界。赢一半的钱包 wilson_lb(0.52,50)≈0.43 < 0.65 → 不上 A(weak_wilson_lb)。
        summary = {
            "esports_closed_count": 50,
            "esports_realized_pnl": 20_000,
            "median_market_roi": 0.38,
            "positive_market_rate": 0.52,
            "esports_loss_count": 24,
            "esports_total_bought": 100_000,
            "esports_total_cost": 50_000,
            "median_entry_price": 0.51,
            "capital_weighted_edge": 0.10,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertNotEqual(rated["grade"], "A")
        self.assertIn("weak_edge_lb", rated["reasons"])  # v17:edge_lb 是唯一质量门

    def test_weak_capital_edge_no_longer_blocks_a_grade(self):
        # 资金加权边际(他的美元盈亏轴)不再是门槛。高胜率 + 正 copy 边际(θ̂−入场价) → A,
        # 即便 capital_weighted_edge 很弱(仅作软 reason)。
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 0.90,  # θ̂；edge = 0.90 − 0.55 = +0.35
            "esports_loss_count": 2,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.05,  # 弱,但不再阻挡 A
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertGreater(rated["bucket_copy_edge"], 0.06)

    def test_sports_wallet_rating_uses_copy_edge_for_a_grade(self):
        # 新轴:θ⁻=0.60 @ 入场 0.50 → E⁻=+0.20,清过 sports 阈值 → A。
        summary = {
            "category": "sports",
            "esports_closed_count": 40,
            "esports_realized_pnl": 46_000,
            "esports_roi": 0.46,
            "median_market_roi": 0.35,
            "positive_market_rate": 28 / 40,
            "wilson_win_rate_lower_bound": 0.60,
            "esports_loss_count": 12,
            "esports_total_bought": 150_000,
            "esports_total_cost": 100_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.226,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertNotIn("low_win_rate", rated["reasons"])
        self.assertNotIn("weak_copy_edge", rated["reasons"])

    def test_sports_wallet_rating_requires_twelve_closed_markets_for_a_grade(self):
        base_summary = {
            "category": "sports",
            "esports_realized_pnl": 12_000,
            "esports_roi": 0.30,
            "median_market_roi": 0.30,
            "positive_market_rate": 0.75,
            "wilson_win_rate_lower_bound": 0.60,
            "esports_loss_count": 2,
            "esports_total_bought": 60_000,
            "esports_total_cost": 40_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.16,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        thin = classify_wallet({**base_summary, "esports_closed_count": 11}, now_ts=100 + 86400)
        qualified = classify_wallet({**base_summary, "esports_closed_count": 12}, now_ts=100 + 86400)

        self.assertEqual(thin["grade"], "C")
        self.assertIn("thin_sample", thin["reasons"])
        self.assertEqual(qualified["grade"], "A")
        self.assertNotIn("thin_sample", qualified["reasons"])

    def test_sports_wallet_rating_uses_lower_roi_exclusion_floor(self):
        summary = {
            "category": "sports",
            "esports_closed_count": 20,
            "esports_realized_pnl": 1_600,
            "esports_roi": 0.16,
            "median_market_roi": 0.16,
            "positive_market_rate": 0.70,
            "wilson_win_rate_lower_bound": 0.52,
            "esports_loss_count": 6,
            "esports_total_bought": 10_000,
            "esports_total_cost": 10_000,
            "median_entry_price": 0.51,
            "capital_weighted_edge": 0.12,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertNotEqual(rated["grade"], "excluded")
        self.assertNotIn("low_historical_roi", rated["reasons"])

    def test_zero_capital_edge_no_longer_excluded(self):
        # capital_weighted_edge=0(他的大注资金恰好打平)与我们无关。θ̂=0.85 @ 入场 0.50,
        # wilson_lb≈0.72、edge_lb≈0.22 → 对固定注 copy 是好标的 → A,capital edge 仅软 reason。
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 100,
            "esports_roi": 0.30,
            "positive_market_rate": 0.85,
            "wilson_win_rate_lower_bound": 0.60,
            "esports_loss_count": 3,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.0,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertIn("weak_capital_weighted_edge", rated["reasons"])  # 软 reason,不再阻挡

    def test_swing_dependent_is_soft_flagged_not_excluded(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "esports_roi": 0.40,
            "positive_market_rate": 0.85,
            "wilson_win_rate_lower_bound": 0.66,
            "esports_loss_count": 3,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.16,
            "actual_minus_hold_pnl_rate": 0.5,  # profit leans on selling
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertNotEqual(rated["grade"], "excluded")
        self.assertIn("swing_dependent", rated["reasons"])

    def test_recency_decay_promotes_recent_form_over_stale_streak(self):
        # 两个钱包同样 10/12 全历史胜率、同入场价。区别只在时间分布:
        # fresh = 近期赢、陈旧输;stale = 陈旧赢、近期输。近期加权点估胜率 θ̂ 应大幅分化,
        # 且 fresh 的有效样本 n_eff 仍 ≥8 → 上榜;stale 近期单太少 → n_eff 萎缩 → 不上。
        now = 2_000_000_000
        DAY = 86400

        def positions(win_ts, loss_ts):
            rows = []
            for i, ts in enumerate(win_ts):
                rows.append({"conditionId": f"w{i}", "totalBought": 1000, "realizedPnl": 400, "avgPrice": 0.5, "timestamp": ts})
            for i, ts in enumerate(loss_ts):
                rows.append({"conditionId": f"l{i}", "totalBought": 1000, "realizedPnl": -1000, "avgPrice": 0.5, "timestamp": ts})
            return rows

        cids = {f"w{i}" for i in range(13)} | {f"l{i}" for i in range(2)}
        ctype = {c: "main_match" for c in cids}
        cgame = {c: "cs2" for c in cids}

        fresh = summarize_closed_positions(
            positions([now - 3 * DAY] * 13, [now - 120 * DAY] * 2),
            cids, condition_type_by_id=ctype, condition_game_family_by_id=cgame, now_ts=now,
        )
        stale = summarize_closed_positions(
            positions([now - 120 * DAY] * 13, [now - 3 * DAY] * 2),
            cids, condition_type_by_id=ctype, condition_game_family_by_id=cgame, now_ts=now,
        )

        fb = fresh["per_game_type"]["cs2:main_match"]
        sb = stale["per_game_type"]["cs2:main_match"]
        # 同样的全历史胜率,但近期加权胜率天差地别
        self.assertEqual(fb["positive_market_rate"], sb["positive_market_rate"])
        self.assertGreater(fb["recency_weighted_win_rate"], sb["recency_weighted_win_rate"] + 0.3)

        fresh_rated = classify_wallet(fresh, now_ts=now)
        stale_rated = classify_wallet(stale, now_ts=now)
        self.assertIn("cs2:main_match", fresh_rated["eligible_buckets"])
        self.assertNotIn("cs2:main_match", stale_rated["eligible_buckets"])

    def test_material_two_sided_market_excluded_from_directional_win_rate(self):
        # m1: 买A 60% + 买B 40%(少数侧≥20% → 实质双边/对冲,无方向)→ A 赢也不算"猜对方向"。
        # m2: 买A 95% + 买B 5%(少数侧<20% → 方向单+小对冲)→ A 赢 = 方向胜。
        mkt = {"outcomes": ["A", "B"], "outcome_prices": [1.0, 0.0], "market_type": "main_match"}
        records = {"m1": {"condition_id": "m1", **mkt}, "m2": {"condition_id": "m2", **mkt}}
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 0, "size": 60, "price": 0.5, "timestamp": 100},
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 1, "size": 40, "price": 0.5, "timestamp": 100},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 0, "size": 95, "price": 0.5, "timestamp": 100},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 1, "size": 5, "price": 0.5, "timestamp": 100},
        ]
        positions, _ = reconstruct_closed_positions(trades, records)
        by_cid = {p["conditionId"]: p for p in positions}
        self.assertTrue(by_cid["m1"]["materialTwoSided"])
        self.assertFalse(by_cid["m2"]["materialTwoSided"])

        summary = summarize_closed_positions(
            positions, {"m1", "m2"},
            condition_type_by_id={"m1": "main_match", "m2": "main_match"},
            condition_game_family_by_id={"m1": "cs2", "m2": "cs2"},
            now_ts=200,
        )
        bucket = summary["per_game_type"]["cs2:main_match"]
        # 只剩 m2 计入方向胜率;m1 当中性
        self.assertEqual(bucket["esports_closed_count"], 1)
        self.assertEqual(bucket["positive_market_rate"], 1.0)
        self.assertGreaterEqual(bucket["neutral_market_count"], 1)

    def test_pre_match_entry_rate_from_reconstruction(self):
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [1.0, 0.0],
            "match_start_time": "2026-06-06T12:00:00Z",
        }
        before = int(datetime(2026, 6, 6, 11, 0, tzinfo=timezone.utc).timestamp())
        after = int(datetime(2026, 6, 6, 13, 0, tzinfo=timezone.utc).timestamp())
        market_records = {"m1": {**market}, "m2": {**market, "condition_id": "m2"}}
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 0, "size": 100, "price": 0.5, "timestamp": before},
            {"conditionId": "m2", "side": "BUY", "outcomeIndex": 0, "size": 100, "price": 0.5, "timestamp": after},
        ]

        positions, _ = reconstruct_closed_positions(trades, market_records)
        by_cid = {p["conditionId"]: p for p in positions}

        self.assertEqual(by_cid["m1"]["preMatchEntry"], True)
        self.assertEqual(by_cid["m2"]["preMatchEntry"], False)
        summary = summarize_trade_reconstructed_positions(trades, market_records, now_ts=200)
        self.assertEqual(summary["pre_match_entry_market_count"], 2)
        self.assertEqual(summary["pre_match_entry_rate"], 0.5)

    def test_capital_weighted_edge_can_qualify_wallet_with_moderate_wilson(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 0.85,
            "wilson_win_rate_lower_bound": 0.66,
            "esports_loss_count": 3,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.16,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertEqual(rated["entry_edge"], 0.16)

    def test_low_volume_is_soft_not_a_hard_exclude(self):
        # v16:成交额不再是门槛(均仓跟单,目标下注大小与我们无关)。高胜率 + 够样本 → A,
        # low_volume 仅作软 reason。
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 200,
            "esports_roi": 0.40,
            "median_market_roi": 0.40,
            "positive_market_rate": 1.0,
            "wilson_win_rate_lower_bound": 0.83,
            "esports_loss_count": 0,
            "esports_total_bought": 500,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.10,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertIn("low_volume", rated["reasons"])  # 软 reason,不再阻挡

    def test_low_roi_is_soft_not_a_hard_exclude(self):
        # A high-win-rate favorite-buyer: roi 0.18 (< floor) but real edge (positive
        # capital_weighted_edge). roi is a payoff-structure artifact and must NOT exclude it.
        summary = {
            "esports_closed_count": 28,
            "esports_realized_pnl": 1_800,
            "esports_roi": 0.18,
            "median_market_roi": 0.18,
            "positive_market_rate": 1.0,
            "wilson_win_rate_lower_bound": 0.88,
            "esports_loss_count": 0,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.12,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertNotEqual(rated["grade"], "excluded")
        self.assertNotIn("low_historical_roi", rated["reasons"])
        self.assertIn("low_roi", rated["reasons"])

    def test_profile_candidate_wallet_does_not_exclude_occasional_sell(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.4, "timestamp": 100 + i}
            for i in range(8)
        ]

        def trades_for_wallet_market(wallet, condition_id):
            if condition_id == "m3":
                return [
                    {"side": "BUY", "outcomeIndex": 0, "size": 100},
                    {"side": "SELL", "outcomeIndex": 0, "size": 100},
                ]
            return [{"side": "BUY", "outcomeIndex": 0, "size": 100}]

        result = profile_candidate_wallet(
            {"wallet": "0xSELLER", "candidate_reasons": ["large_size"]},
            {f"m{i}" for i in range(8)},
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            historical_trades_loader=trades_for_wallet_market,
            now_ts=200,
        )

        self.assertEqual(result["sold_before_resolution_market_count"], 1)
        self.assertEqual(result["sold_before_resolution_market_rate"], 0.125)
        self.assertNotEqual(result["grade"], "excluded")

    def test_profile_candidate_wallet_does_not_hard_exclude_systemic_selling(self):
        # Under hold-to-settlement scoring, selling is no longer a hard exclude — a wallet
        # that sells in most markets (e.g. frees capital by exiting winners) is graded on
        # its hold record, not auto-excluded.
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.4, "timestamp": 100 + i}
            for i in range(8)
        ]

        def trades_for_wallet_market(wallet, condition_id):
            index = int(condition_id[1:])
            if index < 5:
                return [
                    {"side": "BUY", "outcomeIndex": 0, "size": 100},
                    {"side": "SELL", "outcomeIndex": 0, "size": 100},
                ]
            return [{"side": "BUY", "outcomeIndex": 0, "size": 100}]

        result = profile_candidate_wallet(
            {"wallet": "0xSCALPER", "candidate_reasons": ["large_size"]},
            {f"m{i}" for i in range(8)},
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            historical_trades_loader=trades_for_wallet_market,
            now_ts=200,
        )

        self.assertEqual(result["historical_trade_behavior_market_count"], 8)
        self.assertEqual(result["sold_before_resolution_market_count"], 5)
        self.assertEqual(result["sold_before_resolution_market_rate"], 0.625)
        self.assertNotEqual(result["grade"], "excluded")
        self.assertNotIn("sold_before_resolution", result["reasons"])

    def test_profile_candidate_wallet_does_not_exclude_dust_trim(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.4, "timestamp": 100 + i}
            for i in range(8)
        ]

        def trades_for_wallet_market(wallet, condition_id):
            if condition_id == "m3":
                return [
                    {"side": "BUY", "outcomeIndex": 0, "size": 100},
                    {"side": "SELL", "outcomeIndex": 0, "size": 5},
                ]
            return [{"side": "BUY", "outcomeIndex": 0, "size": 100}]

        result = profile_candidate_wallet(
            {"wallet": "0xTRIM", "candidate_reasons": ["large_size"]},
            {f"m{i}" for i in range(8)},
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            historical_trades_loader=trades_for_wallet_market,
            now_ts=200,
        )

        self.assertEqual(result["sold_before_resolution_market_count"], 0)
        self.assertNotEqual(result["grade"], "excluded")

    def test_profile_candidate_wallet_does_not_exclude_occasional_two_sided_trade(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.4, "timestamp": 100 + i}
            for i in range(8)
        ]

        def trades_for_wallet_market(wallet, condition_id):
            if condition_id == "m4":
                return [
                    {"side": "BUY", "outcomeIndex": 0, "size": 100},
                    {"side": "BUY", "outcomeIndex": 1, "size": 100},
                ]
            return [{"side": "BUY", "outcomeIndex": 0, "size": 100}]

        result = profile_candidate_wallet(
            {"wallet": "0xSWITCH", "candidate_reasons": ["large_size"]},
            {f"m{i}" for i in range(8)},
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            historical_trades_loader=trades_for_wallet_market,
            now_ts=200,
        )

        self.assertEqual(result["two_sided_trade_market_count"], 1)
        self.assertEqual(result["two_sided_trade_market_rate"], 0.125)
        self.assertNotEqual(result["grade"], "excluded")

    def test_profile_candidate_wallet_excludes_systemic_two_sided_trading(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.4, "timestamp": 100 + i}
            for i in range(8)
        ]

        def trades_for_wallet_market(wallet, condition_id):
            index = int(condition_id[1:])
            if index < 5:
                return [
                    {"side": "BUY", "outcomeIndex": 0, "size": 100},
                    {"side": "BUY", "outcomeIndex": 1, "size": 100},
                ]
            return [{"side": "BUY", "outcomeIndex": 0, "size": 100}]

        result = profile_candidate_wallet(
            {"wallet": "0xSWITCHER", "candidate_reasons": ["large_size"]},
            {f"m{i}" for i in range(8)},
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            historical_trades_loader=trades_for_wallet_market,
            now_ts=200,
        )

        self.assertEqual(result["historical_trade_behavior_market_count"], 8)
        self.assertEqual(result["two_sided_trade_market_count"], 5)
        self.assertEqual(result["two_sided_trade_market_rate"], 0.625)
        self.assertEqual(result["grade"], "excluded")
        self.assertIn("two_sided_trading", result["reasons"])

    def test_profile_candidate_wallet_failure_becomes_retryable_state(self):
        def failing_closed_positions(wallet):
            raise RuntimeError("temporary api failure")

        result = profile_candidate_wallet(
            {"wallet": "0xABC"},
            {"m1"},
            closed_positions_loader=failing_closed_positions,
            current_positions_loader=lambda wallet: [],
            now_ts=100,
        )

        self.assertEqual(result["wallet"], "0xabc")
        self.assertEqual(result["profile_state"], "failed_retryable")
        self.assertEqual(result["grade"], "unknown")

    def test_profile_candidate_wallet_prefers_trade_reconstruction_over_closed_positions(self):
        market_records = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["G2", "Monte"],
                "outcome_prices": [1.0, 0.0],
                "market_type": "main_match",
            }
        }
        trades = [
            {"conditionId": "m1", "side": "BUY", "outcomeIndex": 1, "size": 10000, "price": 0.3, "timestamp": 100}
        ]

        result = profile_candidate_wallet(
            {"wallet": "0xABC"},
            {"m1"},
            market_records_by_id=market_records,
            user_trades_loader=lambda wallet: trades,
            closed_positions_loader=lambda wallet: (_ for _ in ()).throw(AssertionError("closed positions should not be used")),
            current_positions_loader=lambda wallet: [],
            now_ts=200,
        )

        self.assertEqual(result["wallet"], "0xabc")
        self.assertEqual(result["data_quality"]["source"], "trade_reconstruction")
        self.assertEqual(result["trade_reconstructed_sample_count"], 1)
        self.assertEqual(result["esports_loss_count"], 1)
        # 表现差(1 盘且亏)不再硬 excluded(那是行为类:bot/双边),而是落 C;负盈亏仅软标记。
        self.assertNotEqual(result["grade"], "A")
        self.assertIn("negative_pnl", result["reasons"])

    def test_high_frequency_only_candidate_gets_bot_like_score(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "avgPrice": 0.5, "timestamp": 100 + i}
            for i in range(20)
        ]
        condition_ids = {f"m{i}" for i in range(20)}

        result = profile_candidate_wallet(
            {
                "wallet": "0xBOT",
                "participated_market_count": 20,
                "total_cash_volume": 2_000,
                "max_single_market_cash": 100,
                "candidate_reasons": ["high_participation"],
            },
            condition_ids,
            closed_positions_loader=lambda wallet: positions,
            current_positions_loader=lambda wallet: [],
            now_ts=200,
        )

        # v16:bot 门统一为单一阈值 70(>=70 才 excluded)。score 50 < 70 → 不再降级,
        # 由 Wilson 双下界 + 双边门把关;此处仅验证 bot 评分逻辑本身。
        self.assertEqual(result["bot_like_score"], 50)
        self.assertEqual(result["grade"], "A")

    def test_failed_retryable_profile_is_not_reused_by_long_ttl_cache(self):
        cached = {"wallet": "0xabc", "profile_state": "failed_retryable", "profiled_at": 100}

        self.assertFalse(should_use_cached_profile(cached, now_ts=200, ttl_seconds=7 * 86400))

    def test_cached_profile_without_condition_ids_is_refreshed(self):
        cached = {"wallet": "0xabc", "profile_state": "qualified", "profiled_at": 100}

        self.assertFalse(should_use_cached_profile(cached, now_ts=200, ttl_seconds=7 * 86400))

    def test_cached_profile_without_current_scoring_version_is_refreshed(self):
        cached = {
            "wallet": "0xabc",
            "profile_state": "qualified",
            "profiled_at": 100,
            "esports_condition_ids": ["m1"],
            "scoring_version": 1,
        }

        self.assertFalse(should_use_cached_profile(cached, now_ts=200, ttl_seconds=7 * 86400))

    def test_fresh_classification_cache_is_reused_unless_forced(self):
        self.assertFalse(should_refresh_file_cache(100, now_ts=200, ttl_hours=24))
        self.assertTrue(should_refresh_file_cache(100, now_ts=200, ttl_hours=24, force_refresh=True))
        self.assertTrue(should_refresh_file_cache(None, now_ts=200, ttl_hours=24))

    def test_prune_profile_store_only_drops_low_value_inactive_current(self):
        now_ts = 1_000 * 86400
        old = now_ts - 200 * 86400
        recent = now_ts - 10 * 86400
        store = {
            "0xa": {"wallet": "0xa", "grade": "A", "scoring_version": SCORING_VERSION, "last_esports_trade_at": old},
            "0xc_old": {"wallet": "0xc_old", "grade": "C", "scoring_version": SCORING_VERSION, "last_esports_trade_at": old},
            "0xexcl_old": {"wallet": "0xexcl_old", "grade": "excluded", "scoring_version": SCORING_VERSION, "last_esports_trade_at": old},
            "0xc_recent": {"wallet": "0xc_recent", "grade": "C", "scoring_version": SCORING_VERSION, "last_esports_trade_at": recent},
            "0xc_oldver": {"wallet": "0xc_oldver", "grade": "C", "scoring_version": 0, "last_esports_trade_at": old},
            "0xretry": {"wallet": "0xretry", "grade": "C", "scoring_version": SCORING_VERSION, "profile_state": "failed_retryable", "last_esports_trade_at": old},
        }

        kept = prune_profile_store(store, now_ts=now_ts, max_age_days=180)

        self.assertEqual(
            set(kept),
            {"0xa", "0xc_recent", "0xc_oldver", "0xretry"},
        )
        # disabled when max_age_days <= 0
        self.assertEqual(set(prune_profile_store(store, now_ts=now_ts, max_age_days=0)), set(store))
        self.assertTrue(should_refresh_file_cache(100, now_ts=100 + 25 * 3600, ttl_hours=24))

    def test_market_trades_cache_avoids_repeat_fetches(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def trades_for_market(self, condition_id, *, limit, offset, min_trade_cash):
                self.calls += 1
                return [{"proxyWallet": "0xabc", "size": 100, "price": 0.5, "timestamp": 100}]

        with TemporaryDirectory() as tmp:
            client = FakeClient()
            first, _, first_source = fetch_market_trades_cached(
                client,
                "m1",
                data_dir=Path(tmp),
                now_ts=100,
                page_limit=1000,
                max_pages=3,
                min_trade_cash=50,
                cache_ttl_days=7,
            )
            second, _, second_source = fetch_market_trades_cached(
                client,
                "m1",
                data_dir=Path(tmp),
                now_ts=200,
                page_limit=1000,
                max_pages=3,
                min_trade_cash=50,
                cache_ttl_days=7,
            )

        self.assertEqual(first, second)
        self.assertEqual(client.calls, 1)
        self.assertEqual(first_source, "api")
        self.assertEqual(second_source, "cache")

    def test_failed_market_trades_fetch_is_not_cached(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def trades_for_market(self, condition_id, *, limit, offset, min_trade_cash):
                self.calls += 1
                raise RuntimeError("temporary")

        with TemporaryDirectory() as tmp:
            client = FakeClient()
            first, first_partial, first_source = fetch_market_trades_cached(
                client,
                "m1",
                data_dir=Path(tmp),
                now_ts=100,
                page_limit=1000,
                max_pages=3,
                min_trade_cash=50,
                cache_ttl_days=7,
            )
            second, second_partial, second_source = fetch_market_trades_cached(
                client,
                "m1",
                data_dir=Path(tmp),
                now_ts=200,
                page_limit=1000,
                max_pages=3,
                min_trade_cash=50,
                cache_ttl_days=7,
            )

        self.assertEqual(first, [])
        self.assertTrue(first_partial)
        self.assertEqual(first_source, "error")
        self.assertTrue(second_partial)
        self.assertEqual(second_source, "error")
        self.assertEqual(client.calls, 2)

    def test_recent_esports_closed_positions_are_market_filtered_chunked_and_capped(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def closed_positions(self, wallet, *, limit, offset, sort_direction="DESC", market=None):
                self.calls.append((limit, offset, sort_direction, tuple(market or [])))
                rows = []
                for condition_id in market or []:
                    index = int(condition_id[1:])
                    rows.append({"conditionId": condition_id, "timestamp": 10_000 - index})
                return rows

        client = FakeClient()
        condition_ids = {f"m{index}" for index in range(120)}

        positions = fetch_recent_esports_closed_positions_for_wallet(
            client,
            "0xabc",
            condition_ids,
            max_esports_closed_positions=50,
            market_chunk_size=25,
        )

        self.assertEqual(len(positions), 50)
        self.assertTrue(all(row["conditionId"] in condition_ids for row in positions))
        self.assertTrue(all(limit == 50 for limit, _offset, _direction, _market in client.calls))
        self.assertTrue(all(offset == 0 for _limit, offset, _direction, _market in client.calls))
        self.assertTrue(all(len(market) <= 25 for _limit, _offset, _direction, market in client.calls))
        self.assertTrue(all(direction == "DESC" for _limit, _offset, direction, _market in client.calls))
        self.assertEqual(positions[0]["conditionId"], "m0")

    def test_recent_esports_user_trades_are_paged_filtered_and_capped_by_market_count(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def trades_for_user(self, wallet, *, limit, offset):
                self.calls.append((limit, offset))
                pages = {
                    0: [
                        {"conditionId": "other", "timestamp": 300},
                        {"conditionId": "m1", "timestamp": 200},
                    ],
                    2: [
                        {"conditionId": "m2", "timestamp": 100},
                        {"conditionId": "m3", "timestamp": 90},
                    ],
                }
                return pages.get(offset, [])

        trades = fetch_recent_esports_user_trades_for_wallet(
            FakeClient(),
            "0xabc",
            {"m1", "m2", "m3"},
            page_limit=2,
            max_pages=3,
            max_esports_markets=2,
        )

        self.assertEqual([row["conditionId"] for row in trades], ["m1", "m2"])

    def test_recent_esports_user_trades_stops_on_deep_pagination_400(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def trades_for_user(self, wallet, *, limit, offset):
                self.calls.append((limit, offset))
                if offset >= 4:
                    raise RuntimeError("GET failed: /trades?offset=4: HTTP Error 400: Bad Request")
                return [
                    {"conditionId": f"m{offset}", "timestamp": 100 - offset},
                    {"conditionId": "other", "timestamp": 99 - offset},
                ]

        trades = fetch_recent_esports_user_trades_for_wallet(
            FakeClient(),
            "0xabc",
            {"m0", "m2"},
            page_limit=2,
            max_pages=4,
            max_esports_markets=20,
        )

        self.assertEqual(len(trades), 2)

    def test_user_trade_submarket_backfill_dedupes_and_accepts_settled_game_winner(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                self.calls.append(list(condition_ids))
                return [
                    {
                        "conditionId": "g1",
                        "question": "Dota 2: A vs B - Game 1 Winner",
                        "outcomes": ["A", "B"],
                        "outcomePrices": [0, 1],
                        "volume": 5000,
                        "events": [
                            {
                                "id": "e1",
                                "slug": "a-b",
                                "title": "Dota 2: A vs B (BO3)",
                                "closed": True,
                                "endDate": "2026-06-01T00:00:00Z",
                                "startTime": "2026-06-01T00:00:00Z",
                            }
                        ],
                    }
                ]

        client = FakeClient()
        raw_trades = {
            "0xa": [
                {"conditionId": "g1", "question": "Dota 2: A vs B - Game 1 Winner"},
                {"conditionId": "g1", "question": "Dota 2: A vs B - Game 1 Winner"},
            ],
            "0xb": [{"conditionId": "g1", "question": "Dota 2: A vs B - Game 1 Winner"}],
        }

        records, summary = backfill_user_trade_submarkets(client, raw_trades, {})

        self.assertEqual(client.calls, [["g1"]])
        self.assertEqual(sorted(records), ["g1"])
        self.assertEqual(records["g1"]["market_type"], "game_winner")
        self.assertEqual(summary["user_trade_backfill_candidate_count"], 1)
        self.assertEqual(summary["user_trade_backfilled_market_count"], 1)
        self.assertEqual(summary["user_trade_backfilled_by_market_type"], {"game_winner": 1})

    def test_user_trade_submarket_backfill_accepts_token_market_metadata(self):
        class FakeClient:
            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                return [
                    {
                        "condition_id": "g1",
                        "question": "Dota 2: A vs B - Game 1 Winner",
                        "closed": True,
                        "end_date_iso": "2026-06-01T00:00:00Z",
                        "game_start_time": "2026-06-01T00:00:00Z",
                        "tokens": [
                            {"outcome": "A", "price": 1, "winner": True},
                            {"outcome": "B", "price": 0, "winner": False},
                        ],
                        "tags": ["Sports", "Esports", "Dota 2"],
                    }
                ]

        raw_trades = {"0xa": [{"conditionId": "g1", "title": "Dota 2: A vs B - Game 1 Winner"}]}

        records, summary = backfill_user_trade_submarkets(FakeClient(), raw_trades, {})

        self.assertEqual(records["g1"]["market_type"], "game_winner")
        self.assertEqual(records["g1"]["outcome_prices"], [1.0, 0.0])
        self.assertEqual(summary["user_trade_backfilled_market_count"], 1)

    def test_user_trade_submarket_backfill_accepts_gamma_jsonish_prices(self):
        class FakeClient:
            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                return [
                    {
                        "conditionId": "g1",
                        "question": "Dota 2: A vs B - Game 1 Winner",
                        "outcomes": '["A","B"]',
                        "outcomePrices": '["1","0"]',
                        "closed": True,
                        "events": [
                            {
                                "id": "e1",
                                "slug": "a-b",
                                "title": "Dota 2: A vs B (BO3)",
                                "tags": [{"slug": "dota-2"}],
                                "closed": True,
                                "endDate": "2026-06-01T00:00:00Z",
                                "startTime": "2026-06-01T00:00:00Z",
                            }
                        ],
                    }
                ]

        raw_trades = {"0xa": [{"conditionId": "g1", "title": "Dota 2: A vs B - Game 1 Winner"}]}

        records, summary = backfill_user_trade_submarkets(FakeClient(), raw_trades, {})

        self.assertEqual(records["g1"]["market_type"], "game_winner")
        self.assertEqual(records["g1"]["outcome_prices"], [1.0, 0.0])
        self.assertEqual(summary["user_trade_backfilled_market_count"], 1)

    def test_user_trade_submarket_backfill_rejects_props_and_unsettled_markets(self):
        class FakeClient:
            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                return [
                    {
                        "conditionId": "h1",
                        "question": "Dota 2: A vs B - Game 1 Handicap",
                        "outcomes": ["A", "B"],
                        "outcomePrices": [0, 1],
                        "events": [{"title": "Dota 2: A vs B (BO3)", "tags": [{"slug": "dota-2"}]}],
                    },
                    {
                        "conditionId": "g2",
                        "question": "Dota 2: A vs B - Game 2 Winner",
                        "outcomes": ["A", "B"],
                        "outcomePrices": [0.45, 0.55],
                        "events": [{"title": "Dota 2: A vs B (BO3)", "tags": [{"slug": "dota-2"}]}],
                    },
                ]

        raw_trades = {
            "0xa": [
                {"conditionId": "h1", "question": "Dota 2: A vs B - Game 1 Handicap"},
                {"conditionId": "g2", "question": "Dota 2: A vs B - Game 2 Winner"},
            ],
        }

        records, summary = backfill_user_trade_submarkets(FakeClient(), raw_trades, {})

        self.assertEqual(records, {})
        self.assertEqual(summary["user_trade_backfill_candidate_count"], 1)
        self.assertEqual(summary["user_trade_backfilled_market_count"], 0)

    def test_user_trade_submarket_backfill_dedupes_condition_lookups(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                self.calls.append(list(condition_ids))
                return []

        raw_trades = {
            "0xa": [
                {"conditionId": f"g{index}", "question": f"Dota 2: A vs B - Game {index % 5 + 1} Winner"}
                for index in range(41)
            ],
        }
        client = FakeClient()

        _records, summary = backfill_user_trade_submarkets(client, raw_trades, {}, max_workers=4)

        self.assertEqual(summary["user_trade_backfill_candidate_count"], 41)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(len(client.calls[0]), 41)

    def test_recent_esports_user_trades_cache_avoids_repeat_fetches(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def trades_for_user(self, wallet, *, limit, offset):
                self.calls += 1
                return [{"conditionId": "m1", "timestamp": 100}]

        with TemporaryDirectory() as tmp:
            client = FakeClient()
            first = fetch_recent_esports_user_trades_for_wallet(
                client,
                "0xABC",
                {"m1"},
                data_dir=Path(tmp),
                now_ts=100,
                page_limit=10,
                max_pages=2,
            )
            second = fetch_recent_esports_user_trades_for_wallet(
                client,
                "0xABC",
                {"m1"},
                data_dir=Path(tmp),
                now_ts=200,
                page_limit=10,
                max_pages=2,
            )

        self.assertEqual(first, second)
        self.assertEqual(client.calls, 1)

    def test_recent_esports_user_trades_cache_stores_raw_returns_scoped(self):
        # 缓存存原始交易(含非 scoped 的 other,供跨 scope/窗口复用);返回仍按 scope 过滤。
        class FakeClient:
            def trades_for_user(self, wallet, *, limit, offset):
                return [
                    {"conditionId": "other", "timestamp": 200},
                    {"conditionId": "m1", "timestamp": 100},
                ]

        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            trades = fetch_recent_esports_user_trades_for_wallet(
                FakeClient(),
                "0xABC",
                {"m1"},
                data_dir=data_dir,
                now_ts=100,
                page_limit=10,
                max_pages=1,
            )
            cached = read_json(data_dir / "raw_user_trades" / "0xabc.json", {})

        self.assertEqual([row["conditionId"] for row in trades], ["m1"])          # 返回 scope 过滤
        self.assertEqual(cached["schema"], 2)
        self.assertEqual(sorted(row["conditionId"] for row in cached["trades"]), ["m1", "other"])  # 缓存存 raw

    def test_recent_esports_user_trades_incremental_fetch_only_pulls_new(self):
        # 第二次只增量拉游标之后的新交易,不重拉历史;合并后返回 scope 过滤的全部。
        class FakeClient:
            def __init__(self):
                self.offsets = []
                self.newest_ts = 100

            def trades_for_user(self, wallet, *, limit, offset):
                self.offsets.append(offset)
                if offset > 0:
                    return []
                # 始终返回当前最新交易(含历史 m1@100);增量时遇到 ≤cursor 即停。
                trades = []
                if self.newest_ts > 100:
                    trades.append({"id": "new", "conditionId": "m1", "timestamp": self.newest_ts})
                trades.append({"id": "old", "conditionId": "m1", "timestamp": 100})
                return trades

        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            client = FakeClient()
            fetch_recent_esports_user_trades_for_wallet(
                client, "0xABC", {"m1"}, data_dir=data_dir, now_ts=100,
                page_limit=10, max_pages=3, cache_ttl_days=0,
            )
            # 把缓存 mtime 压成 0,使 now_ts(200) 触发刷新(否则真实 mtime 远大于 now_ts → 判新鲜)
            os.utime(data_dir / "raw_user_trades" / "0xabc.json", (0, 0))
            client.offsets.clear()
            client.newest_ts = 300   # 来了一笔新交易
            trades = fetch_recent_esports_user_trades_for_wallet(
                client, "0xABC", {"m1"}, data_dir=data_dir, now_ts=200,
                page_limit=10, max_pages=3, cache_ttl_days=0,
            )
            cached = read_json(data_dir / "raw_user_trades" / "0xabc.json", {})

        # 增量第二次:offset 0 一页即停(遇到 old@100=cursor),不深翻
        self.assertEqual(client.offsets, [0])
        # 合并后缓存含新旧两笔(去重,不重复)
        self.assertEqual(sorted(t["id"] for t in cached["trades"]), ["new", "old"])
        self.assertEqual(sorted(t["timestamp"] for t in trades), [100, 300])

    def test_write_json_uses_target_unique_atomic_temp_files(self):
        from poly_fight.cli import write_json

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "value.json"
            write_json(path, {"a": 1})

            self.assertEqual(read_json(path, {}), {"a": 1})
            self.assertEqual(list(Path(tmp).glob("*.tmp")), [])

    def test_follow_store_initializes_indexes_and_round_trips_state(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.init_db()
            indexes = store.index_names()

            self.assertIn("idx_follow_signals_status", indexes)
            self.assertIn("idx_follow_legs_signal_id", indexes)
            wallet_state = {
                "0xabc": {
                    "last_trade_cursor": {"timestamp": 100, "id": "t1"},
                    "last_seen_at": 110,
                }
            }
            open_signals = [
                {
                    "signal_id": "sig1",
                    "wallet": "0xabc",
                    "condition_id": "m1",
                    "outcome_index": 0,
                    "status": "open",
                    "legs": [{"trade_id": "t1", "stake": 1}],
                    "behavior_events": [{"kind": "add", "timestamp": 100}],
                }
            ]
            performance = {"wallets": {"0xabc": {"signals": 1}}, "total": {"signals": 1}, "updated_at": 120}

            store.save_follow_snapshot(
                wallet_trade_state=wallet_state,
                open_signals=open_signals,
                result_events=[],
                performance=performance,
            )
            store.save_market_cache({"m1": {"condition_id": "m1"}}, cache_kind="closed", updated_at=123)

            self.assertEqual(store.load_wallet_trade_state(), wallet_state)
            self.assertEqual(store.load_open_signals(), open_signals)
            self.assertEqual(store.load_performance(), performance)
            self.assertEqual(store.load_market_cache(cache_kind="closed", now_ts=200, ttl_seconds=900)[0]["m1"]["condition_id"], "m1")
            self.assertEqual(store.load_market_cache(cache_kind="active", now_ts=200, ttl_seconds=900)[0], {})

    def test_m5_unprocessed_results_persist_and_mark(self):
        # M5 计数从 DB 派生:settled + exited 都算未处理;标记后不再计;跨"重启"(新 store 实例)持久。
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "follow.db"
            store = FollowStore(db_path)
            results = [
                {"signal_id": "s1", "status": "settled", "wallet": "0xA", "condition_id": "c1", "settled_at": 100, "legs": []},
                {"signal_id": "s2", "status": "exited", "wallet": "0xB", "condition_id": "c2", "exit_at": 101, "legs": []},
                {"signal_id": "s3", "status": "settled", "wallet": "0xA", "condition_id": "c3", "settled_at": 102, "legs": []},
            ]
            store.save_follow_snapshot(wallet_trade_state={}, open_signals=[], result_events=results, performance={})
            # settled + exited 都算未处理(共 3),钱包去重 {0xA,0xB}
            pend = store.load_unprocessed_m5_results()
            self.assertEqual(len(pend), 3)
            self.assertEqual({p["wallet"] for p in pend}, {"0xA", "0xB"})
            # 标记 s1/s2 已处理 → 只剩 s3
            self.assertEqual(store.mark_m5_results_processed(["s1", "s2"]), 2)
            self.assertEqual([p["signal_id"] for p in store.load_unprocessed_m5_results()], ["s3"])
            # 模拟重启:新 store 实例读同库,已处理仍不计(持久化、不重复处理)
            self.assertEqual([p["signal_id"] for p in FollowStore(db_path).load_unprocessed_m5_results()], ["s3"])

    def test_follow_store_market_cache_migrates_to_cache_kind_primary_key(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "follow.db"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE market_cache (
                        condition_id TEXT PRIMARY KEY,
                        cache_kind TEXT NOT NULL DEFAULT 'active',
                        updated_at INTEGER NOT NULL,
                        raw_json TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO market_cache(condition_id, cache_kind, updated_at, raw_json) VALUES (?, ?, ?, ?)",
                    ("m1", "closed", 100, json.dumps({"condition_id": "m1", "outcome_prices": [1, 0]})),
                )

            store = FollowStore(db_path)
            store.init_db()
            store.save_market_cache({"m1": {"condition_id": "m1", "title": "Active M1"}}, cache_kind="active", updated_at=200)

            closed, closed_updated_at, closed_fresh = store.load_market_cache(cache_kind="closed", now_ts=250, ttl_seconds=900)
            active, active_updated_at, active_fresh = store.load_market_cache(cache_kind="active", now_ts=250, ttl_seconds=900)

            self.assertTrue(closed_fresh)
            self.assertTrue(active_fresh)
            self.assertEqual(closed_updated_at, 100)
            self.assertEqual(active_updated_at, 200)
            self.assertEqual(closed["m1"]["outcome_prices"], [1, 0])
            self.assertEqual(active["m1"]["title"], "Active M1")

    def test_follow_store_run_ticks_round_trip(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.save_run_tick(
                {
                    "created_at": 123,
                    "status": "ok",
                    "gate_open": True,
                    "watched_market_count": 2,
                    "open_signal_count": 1,
                    "new_signal_count": 1,
                    "tick_runtime_seconds": 0.25,
                    "desired_next_interval_seconds": 30,
                }
            )

            latest = store.latest_run_tick()
            ticks = store.load_run_ticks(limit=10)

            self.assertEqual(latest["created_at"], 123)
            self.assertEqual(latest["status"], "ok")
            self.assertTrue(latest["gate_open"])
            self.assertEqual(latest["watched_market_count"], 2)
            self.assertEqual(len(ticks), 1)

    def test_follow_store_run_ticks_retention_caps_old_rows(self):
        import poly_fight.storage as storage_mod

        original = storage_mod.RUN_TICKS_RETENTION
        storage_mod.RUN_TICKS_RETENTION = 5
        try:
            with TemporaryDirectory() as tmp:
                store = FollowStore(Path(tmp) / "follow.db")
                for i in range(12):
                    store.save_run_tick({"created_at": 1000 + i, "status": "ok"})
                ticks = store.load_run_ticks(limit=100)
                # 只保留最近 5 条;且裁掉的是最旧的(created_at 1000..1006)
                self.assertEqual(len(ticks), 5)
                kept = sorted(t["created_at"] for t in ticks)
                self.assertEqual(kept, [1007, 1008, 1009, 1010, 1011])
        finally:
            storage_mod.RUN_TICKS_RETENTION = original

    def test_category_follow_db_migration_is_guarded_and_dedupes_behavior_events(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"
            source_db = root / "esports" / "follow" / "follow.db"
            follow_dir = root / "follow"
            source_store = FollowStore(source_db)
            source_store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig1",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 100,
                        "updated_at": 100,
                        "legs": [],
                        "behavior_events": [{"kind": "add", "timestamp": 100}],
                    }
                ],
                result_events=[],
                performance={},
            )

            first = migrate_category_follow_dbs(root, follow_dir, now_ts=1000)
            backups_after_first = sorted((follow_dir / "migration_backups").glob("*.db"))
            second = migrate_category_follow_dbs(root, follow_dir, now_ts=2000)
            backups_after_second = sorted((follow_dir / "migration_backups").glob("*.db"))

            self.assertTrue(first["migrated"])
            self.assertFalse(second["migrated"])
            self.assertEqual(backups_after_first, backups_after_second)
            with sqlite3.connect(follow_dir / "follow.db") as conn:
                event_count = conn.execute("SELECT COUNT(*) FROM follow_behavior_events").fetchone()[0]
                signal_raw = conn.execute("SELECT raw_json FROM follow_signals WHERE signal_id = 'sig1'").fetchone()[0]
            self.assertEqual(event_count, 1)
            self.assertEqual(json.loads(signal_raw)["category"], "esports")

    def test_follow_store_initializes_once_per_instance(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            calls = []
            original_connect = store.connect

            def tracked_connect():
                calls.append("connect")
                return original_connect()

            store.connect = tracked_connect

            store.init_db()
            store.load_wallet_trade_state()
            store.load_open_signals()
            store.load_results()
            store.load_performance()
            store.load_market_cache(cache_kind="closed", now_ts=200, ttl_seconds=900)

            self.assertEqual(len(calls), 6)

    def test_follow_store_readonly_missing_db_does_not_create_file(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "follow.db"
            store = FollowStore(db_path)

            snapshot = store.load_dashboard_snapshot()

            self.assertFalse(db_path.exists())
            self.assertFalse(snapshot["db_ready"])
            self.assertEqual(snapshot["open_signals"], [])
            self.assertEqual(snapshot["results"], [])

    def test_follow_store_readonly_snapshot_does_not_call_init_db(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={"0xabc": {"last_trade_cursor": {"timestamp": 1, "id": "t1"}}},
                open_signals=[{"signal_id": "sig1", "wallet": "0xabc", "condition_id": "m1", "status": "open"}],
                result_events=[],
                performance={"wallets": {"0xabc": {"signals": 1}}, "total": {"signals": 1}},
            )
            readonly = FollowStore(Path(tmp) / "follow.db")
            readonly.init_db = lambda: (_ for _ in ()).throw(AssertionError("init_db must not run"))

            snapshot = readonly.load_dashboard_snapshot()

            self.assertTrue(snapshot["db_ready"])
            self.assertEqual(snapshot["open_signals"][0]["signal_id"], "sig1")
            self.assertIn("0xabc", snapshot["wallet_trade_state"])

    def test_follow_store_imports_legacy_json_once(self):
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            follow_dir.mkdir()
            write_json(
                follow_dir / "follow_state.json",
                {
                    "wallet_trade_state": {
                        "0xabc": {"last_trade_cursor": {"timestamp": 100, "id": "t1"}, "last_seen_at": 101}
                    },
                    "active_market_cache": {"updated_at": 99, "markets": [{"condition_id": "m1"}]},
                },
            )
            write_json(
                follow_dir / "follow_signals_open.json",
                [{"signal_id": "sig1", "wallet": "0xabc", "condition_id": "m1", "outcome_index": 0, "status": "open"}],
            )
            write_jsonl(
                follow_dir / "follow_results.jsonl",
                [
                    {
                        "signal_id": "sig0",
                        "wallet": "0xabc",
                        "condition_id": "m0",
                        "outcome_index": 0,
                        "status": "exited",
                        "our_realized_pnl": -0.25,
                        "legs": [{"stake": 1}],
                    }
                ],
            )
            write_json(follow_dir / "follow_performance.json", {"wallets": {}, "total": {"signals": 0}})

            store = FollowStore(follow_dir / "follow.db")
            imported = store.import_legacy_json(
                state_path=follow_dir / "follow_state.json",
                open_path=follow_dir / "follow_signals_open.json",
                results_path=follow_dir / "follow_results.jsonl",
                perf_path=follow_dir / "follow_performance.json",
            )

            self.assertTrue(imported)
            self.assertIn("0xabc", store.load_wallet_trade_state())
            self.assertEqual(len(store.load_open_signals()), 1)
            self.assertEqual(len(store.load_results()), 1)
            self.assertEqual(store.load_performance()["total"]["exits"], 1)

    def test_follow_store_legacy_import_does_not_overwrite_existing_db(self):
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            follow_dir.mkdir()
            store = FollowStore(follow_dir / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={"0xa": {"last_trade_cursor": {"timestamp": 10, "id": "t1"}, "last_seen_at": 10}},
                open_signals=[],
                result_events=[],
                performance={},
            )

            imported = store.import_legacy_json(
                state_path=follow_dir / "missing_state.json",
                open_path=follow_dir / "missing_open.json",
                results_path=follow_dir / "missing_results.jsonl",
                perf_path=follow_dir / "missing_perf.json",
            )

            self.assertFalse(imported)
            self.assertEqual(store.load_wallet_trade_state()["0xa"]["last_trade_cursor"]["id"], "t1")

    def test_active_market_cache_migrates_out_of_follow_state(self):
        with TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_market_cache.json"
            state = {
                "active_market_cache": {
                    "updated_at": 100,
                    "markets": [{"condition_id": "m1", "title": "M1"}],
                }
            }

            class FakeClient:
                def __init__(self):
                    self.calls = 0

                def list_events_paginated(self, **_kwargs):
                    self.calls += 1
                    return []

            markets, new_state, source = load_active_market_cache(
                FakeClient(),
                state,
                cache_path=cache_path,
                now_ts=120,
                gamma_pages=1,
                ttl_seconds=900,
            )

            self.assertEqual(source, "api")
            self.assertEqual(markets, {})
            self.assertNotIn("active_market_cache", new_state)
            self.assertTrue(cache_path.exists())

    def test_active_market_cache_fetches_only_esports_follow_window(self):
        now = datetime(2026, 6, 10, tzinfo=timezone.utc)
        start = now + timedelta(hours=3)
        far_start = now + timedelta(hours=48)

        class FakeClient:
            def __init__(self):
                self.tag_calls = []

            def list_events_paginated(self, **kwargs):
                tags = tuple(kwargs.get("tag_slugs") or ())
                self.tag_calls.append(tags)
                if tags == ("nba", "ufc"):
                    return [
                        {
                            "id": "nba1",
                            "slug": "nba1",
                            "title": "Los Angeles Lakers vs. Boston Celtics",
                            "tags": [{"slug": "nba"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "sports-m1",
                                    "question": "Los Angeles Lakers vs. Boston Celtics",
                                    "outcomes": '["Los Angeles Lakers","Boston Celtics"]',
                                    "outcomePrices": '["0.55","0.45"]',
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]
                return [
                    {
                        "id": "esports1",
                        "slug": "esports1",
                        "title": "Dota 2: A vs B (BO3)",
                        "tags": [{"slug": "dota-2"}],
                        "startTime": start.isoformat(),
                        "markets": [
                            {
                                "conditionId": "esports-m1",
                                "question": "Dota 2: A vs B (BO3)",
                                "outcomes": '["A","B"]',
                                "outcomePrices": '["0.50","0.50"]',
                                "active": True,
                                "closed": False,
                                "volume": 100000,
                                "startTime": start.isoformat(),
                            }
                        ],
                    },
                    {
                        "id": "esports2",
                        "slug": "esports2",
                        "title": "Dota 2: C vs D (BO3)",
                        "tags": [{"slug": "dota-2"}],
                        "startTime": far_start.isoformat(),
                        "markets": [
                            {
                                "conditionId": "esports-far",
                                "question": "Dota 2: C vs D (BO3)",
                                "outcomes": '["C","D"]',
                                "outcomePrices": '["0.50","0.50"]',
                                "active": True,
                                "closed": False,
                                "volume": 100000,
                                "startTime": far_start.isoformat(),
                            }
                        ],
                    },
                ]

        client = FakeClient()
        with TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_market_cache.json"
            markets, _state, source = load_active_market_cache(
                client,
                {},
                cache_path=cache_path,
                now_ts=int(now.timestamp()),
                gamma_pages=1,
                ttl_seconds=900,
                observe_window_hours=24,
            )
            cached = read_json(cache_path, {})

        self.assertEqual(source, "api")
        self.assertIn(("counter-strike-2", "league-of-legends", "dota-2", "valorant"), client.tag_calls)
        self.assertNotIn(("nba", "ufc"), client.tag_calls)
        self.assertEqual(markets["esports-m1"]["category"], "esports")
        self.assertNotIn("sports-m1", markets)
        self.assertNotIn("esports-far", markets)
        self.assertEqual([row["condition_id"] for row in cached["markets"]], ["esports-m1"])
        self.assertEqual(cached["categories"], ["esports"])

    def test_active_market_cache_accepts_dict_shaped_markets(self):
        with TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_market_cache.json"
            now = datetime(2026, 6, 10, tzinfo=timezone.utc)
            start = now + timedelta(hours=3)
            write_json(
                cache_path,
                {
                    "updated_at": int(now.timestamp()),
                    "categories": ["esports"],
                    "markets": {
                        "e1": {"condition_id": "e1", "category": "esports", "match_start_time": start.isoformat()},
                        "s1": {"condition_id": "s1", "category": "sports", "match_start_time": start.isoformat()},
                    },
                },
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    raise AssertionError("fresh dict cache should be used")

            markets, _state, source = load_active_market_cache(
                FakeClient(),
                {},
                cache_path=cache_path,
                now_ts=int(now.timestamp()),
                gamma_pages=1,
                ttl_seconds=900,
            )

            self.assertEqual(source, "cache")
            self.assertEqual(set(markets), {"e1"})

    def test_profile_candidate_filter_keeps_only_clean_active_wallets(self):
        candidates = [
            {"wallet": "0xgood", "participated_market_count": 1, "avg_market_cash": 1_500},
            {"wallet": "0xsmall", "participated_market_count": 1, "avg_market_cash": 1_499},
            {"wallet": "0xtwosided", "participated_market_count": 10, "avg_market_cash": 1_500, "two_sided_market_count": 1},
            {"wallet": "0xchurn", "participated_market_count": 10, "avg_market_cash": 1_500, "high_churn_market_count": 6, "late_entry_market_count": 0},
            {"wallet": "0xchurnok", "participated_market_count": 10, "avg_market_cash": 1_500, "high_churn_market_count": 4, "late_entry_market_count": 0},
            {"wallet": "0xlateok", "participated_market_count": 10, "avg_market_cash": 1_500, "late_entry_market_count": 5},
            {"wallet": "0xlate", "participated_market_count": 10, "avg_market_cash": 1_500, "late_entry_market_count": 6},
            {"wallet": "0xtailone", "participated_market_count": 10, "avg_market_cash": 1_500, "tail_entry_market_count": 1},
            {"wallet": "0xtail", "participated_market_count": 10, "avg_market_cash": 1_500, "tail_entry_market_count": 6},
        ]

        filtered = filter_profile_candidates(
            candidates,
            min_participated_markets=1,
            min_avg_market_cash=1_500,
        )

        # 0xtailone (1/10 tail rate) is kept; 0xtail (6/10) exceeds the rate gate.
        # Churn remains an observation only, so both churn wallets are kept.
        self.assertEqual(
            [row["wallet"] for row in filtered],
            ["0xgood", "0xchurn", "0xchurnok", "0xlateok", "0xlate", "0xtailone"],
        )

    def test_profile_candidate_filter_uses_esports_per_type_thresholds(self):
        thresholds = {
            "main_match": {"min_participated_markets": 11, "min_avg_market_cash": 800},
            "game_winner": {"min_participated_markets": 11, "min_avg_market_cash": 800},
            "map_winner": {"min_participated_markets": 11, "min_avg_market_cash": 500},
        }
        self.assertEqual(ESPORTS_CANDIDATE_MARKET_TYPE_THRESHOLDS, thresholds)
        self.assertEqual(
            ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS,
            {
                "lol": {"min_participated_markets": 6, "min_avg_market_cash": 800},
                "dota2": {"min_participated_markets": 5, "min_avg_market_cash": 800},
            },
        )
        candidates = [
            {
                "wallet": "0xsplit",
                "participated_market_count": 30,
                "avg_market_cash": 2_000,
                "per_type_candidate": {
                    "main_match": {"participated_market_count": 10, "avg_market_cash": 2_000},
                    "game_winner": {"participated_market_count": 10, "avg_market_cash": 900},
                    "map_winner": {"participated_market_count": 10, "avg_market_cash": 600},
                },
            },
            {
                "wallet": "0xlol",
                "per_game_family_candidate": {
                    "lol": {
                        "participated_market_count": 6,
                        "participated_market_ids": [f"l{i}" for i in range(6)],
                        "avg_market_cash": 800,
                    }
                },
                "per_game_type_candidate": {
                    "lol:main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": [f"l{i}" for i in range(6)],
                        "avg_market_cash": 800,
                    }
                },
            },
            {
                "wallet": "0xdota",
                "per_game_family_candidate": {
                    "dota2": {
                        "participated_market_count": 5,
                        "participated_market_ids": [f"d{i}" for i in range(5)],
                        "avg_market_cash": 800,
                    }
                },
                "per_game_type_candidate": {
                    "dota2:game_winner": {
                        "participated_market_count": 5,
                        "participated_market_ids": [f"d{i}" for i in range(5)],
                        "avg_market_cash": 800,
                    }
                },
            },
            {
                "wallet": "0xcs2low",
                "per_type_candidate": {
                    "main_match": {"participated_market_count": 10, "avg_market_cash": 2_000},
                },
                "per_game_family_candidate": {
                    "cs2": {
                        "participated_market_count": 10,
                        "participated_market_ids": [f"c{i}" for i in range(10)],
                        "avg_market_cash": 2_000,
                    }
                },
                "per_game_type_candidate": {
                    "cs2:main_match": {
                        "participated_market_count": 10,
                        "participated_market_ids": [f"c{i}" for i in range(10)],
                        "avg_market_cash": 2_000,
                    }
                },
            },
            {
                "wallet": "0xmain",
                "per_type_candidate": {
                    "main_match": {"participated_market_count": 11, "avg_market_cash": 800},
                },
            },
            {
                "wallet": "0xgame",
                "per_type_candidate": {
                    "game_winner": {"participated_market_count": 11, "avg_market_cash": 800},
                },
            },
            {
                "wallet": "0xmap",
                "per_type_candidate": {
                    "map_winner": {"participated_market_count": 11, "avg_market_cash": 500},
                },
            },
            {
                "wallet": "0xtail",
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 11,
                        "avg_market_cash": 2_000,
                        "tail_entry_market_count": 6,
                    },
                },
            },
        ]

        filtered = filter_profile_candidates(
            candidates,
            market_type_thresholds=thresholds,
            game_family_thresholds=ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS,
        )

        self.assertEqual([row["wallet"] for row in filtered], ["0xlol", "0xdota", "0xmain", "0xgame", "0xmap"])
        self.assertEqual(filtered[0]["qualified_game_families"], ["lol"])
        self.assertEqual(filtered[0]["qualified_buckets"], ["lol:main_match"])
        self.assertEqual(filtered[1]["qualified_game_families"], ["dota2"])
        self.assertEqual(filtered[1]["qualified_buckets"], ["dota2:game_winner"])
        self.assertEqual(filtered[2]["qualified_market_types"], ["main_match"])
        self.assertEqual(filtered[3]["qualified_market_types"], ["game_winner"])
        self.assertEqual(filtered[4]["qualified_market_types"], ["map_winner"])

    def test_profile_candidate_filter_has_no_valorant_override(self):
        candidates = [
            {
                "wallet": "0xvalorant",
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["v1", "v2", "v3", "v4", "v5", "v6"],
                        "avg_market_cash": 350,
                    },
                },
                "per_game_family_candidate": {
                    "valorant": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["v1", "v2", "v3", "v4", "v5", "v6"],
                        "avg_market_cash": 350,
                    },
                },
                "per_game_type_candidate": {
                    "valorant:main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["v1", "v2", "v3", "v4", "v5", "v6"],
                        "avg_market_cash": 350,
                    },
                },
            },
            {
                "wallet": "0xcorelow",
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["c1", "c2", "c3", "c4", "c5", "c6"],
                        "avg_market_cash": 350,
                    },
                },
                "per_game_family_candidate": {
                    "cs2": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["c1", "c2", "c3", "c4", "c5", "c6"],
                        "avg_market_cash": 350,
                    },
                },
                "per_game_type_candidate": {
                    "cs2:main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["c1", "c2", "c3", "c4", "c5", "c6"],
                        "avg_market_cash": 350,
                    },
                },
            },
            {
                "wallet": "0xvaltail",
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["t1", "t2", "t3", "t4", "t5", "t6"],
                        "avg_market_cash": 350,
                    },
                },
                "per_game_family_candidate": {
                    "valorant": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["t1", "t2", "t3", "t4", "t5", "t6"],
                        "avg_market_cash": 350,
                        "tail_entry_market_count": 4,
                    },
                },
                "per_game_type_candidate": {
                    "valorant:main_match": {
                        "participated_market_count": 6,
                        "participated_market_ids": ["t1", "t2", "t3", "t4", "t5", "t6"],
                        "avg_market_cash": 350,
                        "tail_entry_market_count": 4,
                    },
                },
            },
        ]

        filtered = filter_profile_candidates(
            candidates,
            market_type_thresholds=ESPORTS_CANDIDATE_MARKET_TYPE_THRESHOLDS,
            game_family_thresholds=ESPORTS_CANDIDATE_GAME_FAMILY_THRESHOLDS,
        )

        self.assertEqual([row["wallet"] for row in filtered], [])

    def test_candidate_wallets_track_game_family_metrics_for_dota2(self):
        trades_by_market = {
            f"d{i}": [
                {
                    "proxyWallet": "0xDota2",
                    "cash": "350",
                    "price": "0.5",
                    "size": "700",
                    "side": "BUY",
                    "outcome": "NRG",
                    "timestamp": str(1_000 + i),
                }
            ]
            for i in range(6)
        }

        candidates = build_candidate_wallets(
            trades_by_market,
            market_type_by_id={f"d{i}": "main_match" for i in range(6)},
            market_game_family_by_id={f"d{i}": "dota2" for i in range(6)},
            participation_threshold=99,
            total_cash_threshold=99_000,
            single_market_cash_threshold=99_000,
            candidate_wallets_per_game_family=10,
        )

        self.assertEqual([row["wallet"] for row in candidates], ["0xdota2"])
        self.assertEqual(
            candidates[0]["per_game_family_candidate"]["dota2"]["participated_market_count"],
            6,
        )
        self.assertEqual(
            candidates[0]["per_game_type_candidate"]["dota2:main_match"]["participated_market_count"],
            6,
        )

    def test_wallet_overlap_report_uses_closed_esports_markets(self):
        report = build_wallet_overlap_report(
            [
                {"wallet": "0xA", "esports_condition_ids": ["m1", "m2", "m3"]},
                {"wallet": "0xB", "esports_condition_ids": ["m2", "m3", "m4"]},
                {"wallet": "0xC", "esports_condition_ids": ["m3", "m5"]},
            ]
        )

        self.assertEqual(report["wallet_count"], 3)
        self.assertEqual(report["union_market_count"], 5)
        self.assertEqual(report["shared_by_all_market_ids"], ["m3"])
        self.assertEqual(report["pair_overlaps"][0]["shared_market_ids"], ["m2", "m3"])

    def test_leaderboard_ranks_lossless_wallets_before_lossy_high_roi_wallets(self):
        now = 1_000_000

        def profile(wallet: str, *, losses: int, roi: float, wilson: float) -> dict:
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": now,
                "esports_loss_count": losses,
                "positive_market_rate": 1.0 if losses == 0 else 0.8,
                "wilson_win_rate_lower_bound": wilson,
                "entry_edge": wilson - 0.45,
                "median_market_roi": roi,
                "candidate": {
                    "participated_market_count": 3,
                    "avg_market_cash": 1_500,
                    "two_sided_market_count": 0,
                    "tail_entry_market_count": 0,
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xlossy": profile("0xlossy", losses=4, roi=1.2, wilson=0.66),
                "0xclean": profile("0xclean", losses=0, roi=0.8, wilson=0.83),
            },
            now_ts=now,
            min_participated_markets=3,
            min_avg_market_cash=1_500,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xclean", "0xlossy"])

    def test_leaderboard_no_longer_applies_legacy_positive_rate_floor(self):
        # v16:legacy 整体 positive-rate / ROI 门已删。质量由 Wilson 双下界 + 分桶 eligible 把关,
        # 不再用"拉通整体胜率"额外卡 grade-A 钱包。sub-specialist 保留,overall grade-A 也保留。
        now = 1_000_000
        candidate = {
            "participated_market_count": 16,
            "avg_market_cash": 5_000,
            "two_sided_market_count": 0,
            "tail_entry_market_count": 0,
        }
        sub_eligible = {
            "wallet": "0xsubspec",
            "grade": "A",
            "eligible_market_types": ["game_winner"],
            "last_esports_trade_at": now,
            "positive_market_rate": 0.62,  # weak blended overall, dragged down by main_match
            "wilson_win_rate_lower_bound": 0.66,
            "entry_edge": 0.12,
            "median_market_roi": 0.38,
            "candidate": candidate,
        }
        legacy_overall = {
            "wallet": "0xlegacy",
            "grade": "A",
            "last_esports_trade_at": now,
            "esports_roi": 0.50,
            "positive_market_rate": 0.62,
            "wilson_win_rate_lower_bound": 0.66,
            "entry_edge": 0.12,
            "median_market_roi": 0.38,
            "candidate": candidate,
        }

        ranked = build_leaderboard_from_profiles(
            {"0xsubspec": sub_eligible, "0xlegacy": legacy_overall},
            now_ts=now,
            min_participated_markets=3,
            min_avg_market_cash=1_500,
        )

        wallets = [row["wallet"] for row in ranked]
        self.assertIn("0xsubspec", wallets)  # sub-specialist kept
        self.assertIn("0xlegacy", wallets)  # v16:grade-A 不再被整体胜率门卡掉

    def test_v18_neff_floor_decoupled_from_price_restriction(self):
        # v18:n_eff 地板用全样本(eff_full),edge_lb 的 Wilson 用 ≤0.85 子集。
        now = 100 + 86400
        # 子集只有 8 场(<12)但全样本 30 场、子集 edge 经 Wilson 仍硬 → 上 A(被 v17 误挡,v18 捞回)。
        recovered = classify_wallet({
            "esports_closed_count": 30, "recency_weighted_win_rate": 0.90,
            "effective_sample_size": 8, "effective_sample_size_full": 30,
            "median_entry_price": 0.55, "esports_realized_pnl": 5000,
            "esports_total_bought": 50000, "last_esports_trade_at": 100, "bot_like_score": 0,
        }, now_ts=now)
        self.assertEqual(recovered["grade"], "A")
        self.assertNotIn("thin_sample", recovered["reasons"])

        # 纯大热买家:可跟价区子集为空(eff=0、median=0)→ edge_lb 无 → 仍被挡(即便全样本 30)。
        favorite = classify_wallet({
            "esports_closed_count": 30, "recency_weighted_win_rate": 0.0,
            "effective_sample_size": 0, "effective_sample_size_full": 30,
            "median_entry_price": 0.0, "esports_realized_pnl": 5000,
            "esports_total_bought": 50000, "last_esports_trade_at": 100, "bot_like_score": 0,
        }, now_ts=now)
        self.assertNotEqual(favorite["grade"], "A")
        self.assertIn("weak_edge_lb", favorite["reasons"])

        # v19:可跟价区子集太薄(4 < 6)→ 不据此判桶,即便全样本够 + edge 看着硬。
        thin_sub = classify_wallet({
            "esports_closed_count": 30, "recency_weighted_win_rate": 0.95,
            "effective_sample_size": 4, "effective_sample_size_full": 30,
            "median_entry_price": 0.50, "esports_realized_pnl": 5000,
            "esports_total_bought": 50000, "last_esports_trade_at": 100, "bot_like_score": 0,
        }, now_ts=now)
        self.assertNotEqual(thin_sub["grade"], "A")
        self.assertIn("thin_followable_subset", thin_sub["reasons"])

    def test_v21_thin_sample_gate_requires_stronger_signal_below_anchor(self):
        # v21:桶 full n_eff 落在 [放松地板6, 满严格锚点10) → 须 edge_lb≥0.08 且 θ̂≥0.80 才给 A。
        now = 100 + 86400
        base = {
            "category": "esports", "esports_closed_count": 8,
            "effective_sample_size": 8, "effective_sample_size_full": 8,  # 8 ∈ [6,10) → 薄
            "last_esports_trade_at": 100, "bot_like_score": 0,
        }
        # 强信号:θ̂=0.90、便宜入场 → edge_lb 大 → 过薄门 → A。
        strong = classify_wallet_bucket(
            {**base, "recency_weighted_win_rate": 0.90, "median_entry_price": 0.45},
            now_ts=now, min_sample=6, n_eff_anchor=10)
        self.assertEqual(strong["grade"], "A")
        # 弱胜率:θ̂=0.72(过基础0.68,未过薄门0.80)、edge_lb 仍≥0.08 → 被薄门砍。
        weak_wr = classify_wallet_bucket(
            {**base, "recency_weighted_win_rate": 0.72, "median_entry_price": 0.30},
            now_ts=now, min_sample=6, n_eff_anchor=10)
        self.assertNotEqual(weak_wr["grade"], "A")
        self.assertIn("thin_underqualified", weak_wr["reasons"])
        # 同样弱胜率但不给锚点(旧行为)→ 薄门不启用 → 仍 A(向后兼容)。
        no_anchor = classify_wallet_bucket(
            {**base, "recency_weighted_win_rate": 0.72, "median_entry_price": 0.30},
            now_ts=now, min_sample=6)
        self.assertEqual(no_anchor["grade"], "A")
        # full n_eff ≥ 锚点(够厚,非薄样本)→ 不受薄门约束,弱胜率走基础门即可 A。
        thick = classify_wallet_bucket(
            {**base, "effective_sample_size_full": 12,
             "recency_weighted_win_rate": 0.72, "median_entry_price": 0.30},
            now_ts=now, min_sample=6, n_eff_anchor=10)
        self.assertEqual(thick["grade"], "A")

    def test_grading_eligibility_is_single_source_across_consumers(self):
        # 单一真相源回归护栏:同一份 profile,collector/observe/demote 共用的
        # build_collector_leaderboard_v2,与 follow 的 eligible_follow_wallets / wallet_is_followable,
        # 必须对"是否够格上榜/被跟"给出一致结论(不许各自为政)。
        now = 1_000_000

        def esports_summary(wallet, win_rate, count):
            wins = round(win_rate * count)
            positions = [
                {"conditionId": f"{wallet}-w{i}", "totalBought": 1200,
                 "realizedPnl": 600, "avgPrice": 0.5, "timestamp": now - 5000 + i}
                for i in range(wins)
            ] + [
                {"conditionId": f"{wallet}-l{i}", "totalBought": 1200,
                 "realizedPnl": -1200, "avgPrice": 0.5, "timestamp": now - 5000 + wins + i}
                for i in range(count - wins)
            ]
            cids = {p["conditionId"] for p in positions}
            summary = summarize_closed_positions(
                positions, cids,
                condition_type_by_id={c: "game_winner" for c in cids},
                condition_game_family_by_id={c: "cs2" for c in cids},
                now_ts=now,
            )
            return {**summary, "wallet": wallet, "category": "esports"}

        strong = classify_wallet(esports_summary("0xstrong", 0.85, 16), now_ts=now)
        weak = classify_wallet(esports_summary("0xweak", 0.50, 16), now_ts=now)
        profiles = {"0xstrong": strong, "0xweak": weak}

        # collector / observe-v2 / rescore-demote 共用入口
        result = build_collector_leaderboard_v2(profiles, now_ts=now)
        board = {normalize_wallet(r.get("wallet")) for r in result["leaderboard"]}

        # follow 入口(同一份榜单 → 可跟集合)
        follow_rows = eligible_follow_wallets(result["leaderboard"], now_ts=now, recency_days=365)
        follow_set = {normalize_wallet(r.get("wallet")) for r in follow_rows}

        self.assertIn("0xstrong", board)
        self.assertNotIn("0xweak", board)
        self.assertTrue(wallet_is_followable(strong))
        self.assertFalse(wallet_is_followable(weak))
        self.assertEqual(board, follow_set)  # 榜单成员 == follow 可跟集合

    def test_slim_profile_for_storage_drops_redundant_blocks_only(self):
        # 写盘瘦身:删原始 per_type/per_game_type + candidate 4 个大嵌套块;
        # 保留评分产出(*_grades)、复用 key(scoring_version/esports_condition_ids)、所有扁平标量。
        now = 1_000_000
        profile = {
            "wallet": "0xslim",
            "category": "esports",
            "grade": "A",
            "scoring_version": 19,
            "last_esports_trade_at": now - 3600,
            "two_sided_trade_market_rate": 0.0,
            "bot_like_score": 0,
            "esports_condition_ids": ["0xabc", "0xdef"],
            "actual_pnl": 100.0,
            "hold_pnl": 80.0,
            "esports_closed_count": 20,
            "per_type": {"main_match": {"esports_closed_count": 20}},          # 原始,删
            "per_game_type": {"cs2:main_match": {"esports_closed_count": 20}}, # 原始,删
            "per_type_grades": {"main_match": {"grade": "A", "bucket_win_rate": 0.78}},
            "per_game_type_grades": {"cs2:main_match": {"grade": "A", "bucket_win_rate": 0.80}},
            "candidate": {
                "participated_market_count": 30,
                "tail_entry_market_count": 0,
                "avg_market_cash": 1200.0,
                "per_type_candidate": {"main_match": {"x": 1}},          # 大块,删
                "per_game_type_candidate": {"cs2:main_match": {"x": 1}}, # 大块,删
                "per_game_family_candidate": {"cs2": {"x": 1}},          # 大块,删
                "participated_market_ids": ["0xabc", "0xdef", "0xghi"],  # 大块,删
            },
        }
        slim = slim_profile_for_storage(profile)
        # 删掉的
        self.assertNotIn("per_type", slim)
        self.assertNotIn("per_game_type", slim)
        for key in ("per_type_candidate", "per_game_type_candidate", "per_game_family_candidate", "participated_market_ids"):
            self.assertNotIn(key, slim["candidate"])
        # 保留的:评分产出 + 复用 key + candidate 标量 + 扁平标量
        for key in ("wallet", "grade", "scoring_version", "esports_condition_ids",
                    "per_type_grades", "per_game_type_grades", "last_esports_trade_at",
                    "actual_pnl", "hold_pnl", "esports_closed_count"):
            self.assertIn(key, slim)
        for key in ("participated_market_count", "tail_entry_market_count", "avg_market_cash"):
            self.assertIn(key, slim["candidate"])
        # 不改原对象;幂等
        self.assertIn("per_type", profile)
        self.assertEqual(slim_profile_for_storage(slim), slim)
        # 瘦身后仍能上榜(build 只读 _grades + candidate 标量)
        result = build_collector_leaderboard_v2({"0xslim": slim}, now_ts=now)
        board = {normalize_wallet(r["wallet"]) for r in result["leaderboard"]}
        self.assertIn("0xslim", board)

    def test_slim_user_trade_drops_display_fields_keeps_scoring(self):
        # 交易缓存瘦身:删头像/标题/slug/asset 等展示字段,保留打分/游标用的字段。
        trade = {
            "conditionId": "0xabc", "outcomeIndex": 0, "outcome": "TeamA",
            "price": 0.48, "side": "BUY", "size": 20000, "timestamp": 1781492655,
            "transactionHash": "0xdead", "proxyWallet": "0xwallet",
            "asset": "112820...", "icon": "https://...png", "title": "UFC: A vs B",
            "slug": "ufc-a-b", "eventSlug": "ufc-a-b", "pseudonym": "Foo", "name": "bar",
            "bio": "", "profileImage": "", "profileImageOptimized": "",
        }
        slim = _slim_user_trade(trade)
        for k in ("asset", "icon", "title", "slug", "eventSlug", "pseudonym", "name", "bio", "profileImage", "profileImageOptimized"):
            self.assertNotIn(k, slim)
        for k in ("conditionId", "outcomeIndex", "outcome", "price", "side", "size", "timestamp", "transactionHash", "proxyWallet"):
            self.assertEqual(slim[k], trade[k])

    def test_derive_scope_params_adapts_to_density(self):
        # v21:n_eff_floor = 子集门6 + round((锚点−6)×scale=0.5);锚点(n_eff_floor_full)按 λ 分档不变。
        # 密集(λ≈16 ≥ T2=14):锚点 dense=10 → 地板 6+round(2)=8。
        dense = derive_scope_params(markets=1440, window_days=90, gaps=[1.0] * 80)
        self.assertEqual(dense["lookback_days"], 14)          # 180/16=11.25 → clamp 到下限 14
        self.assertEqual(dense["n_eff_floor_full"], 10)       # λ=16 ≥ T2 → 锚点 dense=10
        self.assertEqual(dense["n_eff_floor"], 8)             # 6+round((10-6)*0.5)=8
        self.assertEqual(dense["idle_ceiling_hours"], 72)     # p90 gap=1 → 2×1×24=48 → clamp 72
        # 中档(T1=9 ≤ λ≈11.7 < T2=14):锚点 mid=8 → 地板 6+round(1)=7。
        mid = derive_scope_params(markets=1053, window_days=90, gaps=[1.0] * 60)
        self.assertEqual(mid["n_eff_floor_full"], 8)          # λ≈11.7 ∈ [9,14) → 锚点 mid=8
        self.assertEqual(mid["n_eff_floor"], 7)               # 6+round((8-6)*0.5)=7
        # 稀疏(λ≈6 < T1=9):锚点 sparse=7 → 地板 6+round(0.5)=6(并入子集门)。
        sparse = derive_scope_params(markets=540, window_days=90, gaps=[1.0] * 40 + [9.0])
        self.assertEqual(sparse["lookback_days"], 30)         # 180/6=30
        self.assertEqual(sparse["n_eff_floor_full"], 7)       # λ=6 < T1 → 锚点 sparse=7
        self.assertEqual(sparse["n_eff_floor"], 6)            # 6+round((7-6)*0.5)=6
        # 极稀疏:lookback 封顶 90,锚点仍 sparse=7 → 地板 6。
        tiny = derive_scope_params(markets=90, window_days=90, gaps=[7.0, 14.0])
        self.assertEqual(tiny["lookback_days"], 90)           # 180/1=180 → clamp 90
        self.assertEqual(tiny["n_eff_floor_full"], 7)
        self.assertEqual(tiny["n_eff_floor"], 6)
        # idle 锚 p90 gap(尾部),clamp 到 [72h, 21d]。
        bursty = derive_scope_params(markets=200, window_days=90, gaps=[1.0, 1.0, 10.0, 10.0])
        self.assertGreater(bursty["idle_ceiling_hours"], 72)  # p90≈10d → 放宽
        self.assertLessEqual(bursty["idle_ceiling_hours"], 21 * 24)

    def test_match_day_gaps_uses_distinct_days(self):
        day = 86400
        ts = [10 * day, 10 * day + 100, 11 * day, 15 * day]  # 有赛日 {10,11,15} → gaps [1,4]
        self.assertEqual(match_day_gaps(ts), [1.0, 4.0])
        self.assertEqual(match_day_gaps([5 * day]), [])

    def test_wallet_bucket_min_sample_is_game_aware(self):
        floors = {"valorant": 8, "cs2": 12}
        # per_game_type 桶:用该游戏的自适应地板
        self.assertEqual(wallet_bucket_min_sample("esports", "main_match", game_family="valorant", n_eff_floors=floors), 8)
        self.assertEqual(wallet_bucket_min_sample("esports", "main_match", game_family="cs2", n_eff_floors=floors), 12)
        # per_type 跨游戏桶(无 game_family)或无 map:全局默认 12
        self.assertEqual(wallet_bucket_min_sample("esports", "main_match", n_eff_floors=floors), 12)
        self.assertEqual(wallet_bucket_min_sample("esports", "main_match", game_family="valorant"), 12)

    def test_scope_param_accessors_and_loader_roundtrip(self):
        import tempfile, json as _json, os
        scopes = {
            "valorant": {"n_eff_floor": 8, "lookback_days": 30},
            "cs2": {"n_eff_floor": 12, "lookback_days": 14},
        }
        self.assertEqual(scope_n_eff_floors(scopes), {"valorant": 8, "cs2": 12})
        self.assertEqual(scope_lookback_by_game(scopes), {"valorant": 30, "cs2": 14})
        self.assertEqual(scope_max_lookback_days(scopes, 15), 30)
        self.assertEqual(scope_max_lookback_days({}, 15), 15)  # 空校准 → 回退默认
        ts = 1_700_000_000
        with tempfile.TemporaryDirectory() as d:
            # 无文件 + 无 client → {}(消费方回退默认,不报错)
            self.assertEqual(load_scope_params(d, client=None), {})
            with open(os.path.join(d, "scope_calibration.json"), "w") as fh:
                _json.dump({"calibrated_at": ts, "scopes": scopes}, fh)
            loaded = load_scope_params(d, client=None, now=datetime.fromtimestamp(ts + 60, tz=timezone.utc))
            self.assertEqual(scope_n_eff_floors(loaded), {"valorant": 8, "cs2": 12})

    def test_per_game_window_filter_keeps_each_game_to_its_own_lookback(self):
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)

        def row(cid, game, days_ago):
            return {"condition_id": cid, "game_family": game,
                    "end_date": (now - timedelta(days=days_ago)).isoformat()}
        rows = [
            row("v-fresh", "valorant", 25),   # valorant 30d 窗口内 → 留
            row("c-stale", "cs2", 25),        # cs2 14d 窗口外 → 删
            row("c-fresh", "cs2", 10),        # cs2 14d 窗口内 → 留
        ]
        kept = filter_classification_set_by_game_window(
            rows, now=now, lookback_by_game={"valorant": 30, "cs2": 14}, default_days=15)
        self.assertEqual({r["condition_id"] for r in kept}, {"v-fresh", "c-fresh"})

    def test_win_rate_floor_excludes_low_winrate_high_edge_bucket(self):
        # v20:桶内 θ̂ < 0.68 即使买得便宜、edge 充足,也判 C —— 不进榜、不跟单。
        now = 1_000_000

        def summary(n, win_rate, avg_price):
            wins = round(win_rate * n)
            pos = [
                {"conditionId": f"w{i}", "totalBought": 1000, "realizedPnl": 500,
                 "avgPrice": avg_price, "timestamp": now - 500 + i} for i in range(wins)
            ] + [
                {"conditionId": f"l{i}", "totalBought": 1000, "realizedPnl": -1000,
                 "avgPrice": avg_price, "timestamp": now - 500 + wins + i} for i in range(n - wins)
            ]
            cids = {p["conditionId"] for p in pos}
            s = summarize_closed_positions(
                pos, cids,
                condition_type_by_id={c: "main_match" for c in cids},
                condition_game_family_by_id={c: "cs2" for c in cids},
                now_ts=now,
            )
            return {**s, "category": "esports"}

        # θ̂≈0.60、买在 0.25(edge 充足)→ 胜率门拦下,不合格 + 明确拒因
        low = classify_wallet(summary(20, 0.60, 0.25), now_ts=now)
        self.assertNotIn("cs2:main_match", low.get("eligible_buckets") or [])
        self.assertIn(
            "win_rate_below_floor",
            (low.get("per_game_type_grades") or {}).get("cs2:main_match", {}).get("reasons") or [],
        )
        # θ̂≈0.75 同样买便宜 → 过胜率门 → 合格
        high = classify_wallet(summary(20, 0.75, 0.25), now_ts=now)
        self.assertIn("cs2:main_match", high.get("eligible_buckets") or [])

    def test_valorant_thin_bucket_eligible_under_adaptive_neff(self):
        # 同一份 valorant profile(单游戏 ~10 场):n_eff_floors={valorant:8} → 上榜路径合格;
        # 全局默认 12 → 判薄样本不合格。锁住 per-game n_eff 自适应。
        now = 1_000_000

        def valorant_summary(n, win_rate):
            wins = round(win_rate * n)
            positions = [
                {"conditionId": f"v-w{i}", "totalBought": 1000, "realizedPnl": 500,
                 "avgPrice": 0.5, "timestamp": now - 1000 + i}
                for i in range(wins)
            ] + [
                {"conditionId": f"v-l{i}", "totalBought": 1000, "realizedPnl": -1000,
                 "avgPrice": 0.5, "timestamp": now - 1000 + wins + i}
                for i in range(n - wins)
            ]
            cids = {p["conditionId"] for p in positions}
            summary = summarize_closed_positions(
                positions, cids,
                condition_type_by_id={c: "main_match" for c in cids},
                condition_game_family_by_id={c: "valorant" for c in cids},
                now_ts=now,
            )
            return {**summary, "category": "esports"}

        summary = valorant_summary(10, 0.8)
        adaptive = classify_wallet(summary, now_ts=now, n_eff_floors={"valorant": 8})
        default = classify_wallet(summary, now_ts=now)  # 全局 12
        self.assertIn("valorant:main_match", adaptive.get("eligible_buckets") or [])
        self.assertEqual(adaptive.get("grade"), "A")
        self.assertNotIn("valorant:main_match", default.get("eligible_buckets") or [])

    def test_v2_board_recovers_per_type_eligible_wallets(self):
        # 跨游戏盘口专家:只有 per-type(跨游戏盘口)够格桶、无任何 per-game-type A 桶,
        # 也必须上榜(此前 build_collector_leaderboard_v2 只看 per_game_type_grades → 漏掉)。
        # 它们过的是和全榜完全相同的 grade-A 门,follow 本就按 eligible_market_types 跟它们。
        now = 1_000_000
        base = {
            "category": "esports",
            "grade": "A",
            "last_esports_trade_at": now - 3600,
            "two_sided_trade_market_rate": 0.0,
            "bot_like_score": 0,
            "participated_market_count": 30,
            "esports_closed_count": 30,
            "tail_entry_market_count": 0,
        }
        cross_game = {
            **base,
            "wallet": "0xcrossgame",
            "eligible_market_types": ["main_match"],
            "eligible_buckets": [],
            "per_game_type_grades": {},  # 无任何单游戏专精桶
            "per_type_grades": {
                "main_match": {
                    "grade": "A", "bucket_win_rate": 0.78, "bucket_eff_sample": 15.0,
                    "bucket_copy_edge": 0.12, "median_entry_price": 0.55,
                    "positive_market_rate": 0.8, "esports_closed_count": 20,
                }
            },
        }
        specialist = {
            **base,
            "wallet": "0xspecialist",
            "eligible_market_types": ["main_match"],
            "eligible_buckets": ["cs2:main_match"],
            "per_game_type_grades": {
                "cs2:main_match": {
                    "grade": "A", "bucket_win_rate": 0.80, "bucket_eff_sample": 15.0,
                    "bucket_copy_edge": 0.10, "median_entry_price": 0.50,
                    "positive_market_rate": 0.8, "esports_closed_count": 20,
                }
            },
            "per_type_grades": {"main_match": {"grade": "A"}},
        }
        none_a = {
            **base,
            "wallet": "0xnone",
            "grade": "B",
            "eligible_market_types": [],
            "per_game_type_grades": {},
            "per_type_grades": {"main_match": {"grade": "B", "bucket_win_rate": 0.60, "bucket_eff_sample": 8.0}},
        }
        profiles = {p["wallet"]: p for p in (cross_game, specialist, none_a)}
        result = build_collector_leaderboard_v2(profiles, now_ts=now)
        by_wallet = {normalize_wallet(r["wallet"]): r for r in result["leaderboard"]}

        # per-type 合格钱包被捞上榜,标记为跨游戏
        self.assertIn("0xcrossgame", by_wallet)
        row = by_wallet["0xcrossgame"]
        self.assertEqual(row["primary_game"], "multi")
        self.assertEqual(row["best_bucket"], "multi:main_match")
        self.assertIn("main_match", row["eligible_market_types"])
        self.assertTrue(row["eligible_bucket_details"][0].get("cross_game"))
        # 正常单游戏专精仍走 per-game-type 路径
        self.assertIn("0xspecialist", by_wallet)
        self.assertEqual(by_wallet["0xspecialist"]["primary_game"], "cs2")
        # 没有任何 A 桶 → 不上榜
        self.assertNotIn("0xnone", by_wallet)
        # 上榜集合 == follow 可跟集合(跨游戏钱包也必须可跟)
        follow_rows = eligible_follow_wallets(result["leaderboard"], now_ts=now, recency_days=365)
        follow_set = {normalize_wallet(r.get("wallet")) for r in follow_rows}
        self.assertIn("0xcrossgame", follow_set)
        self.assertEqual(set(by_wallet), follow_set)

    def test_esports_leaderboard_requires_eligible_qualified_type_overlap(self):
        now = 1_000_000

        def profile(wallet, eligible, qualified, avg_cash):
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": eligible,
                "last_esports_trade_at": now,
                "positive_market_rate": 0.90,
                "wilson_win_rate_lower_bound": 0.70,
                "entry_edge": 0.10,
                "median_market_roi": 0.30,
                "candidate": {
                    "qualified_market_types": qualified,
                    "per_type_candidate": {
                        market_type: {
                            "participated_market_count": 20,
                            "avg_market_cash": avg_cash,
                            "tail_entry_market_count": 0,
                            "two_sided_market_count": 0,
                            "high_churn_market_count": 0,
                        }
                        for market_type in qualified
                    },
                    "participated_market_count": 20,
                    "avg_market_cash": avg_cash,
                    "two_sided_market_count": 0,
                    "tail_entry_market_count": 0,
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xmismatch": profile("0xmismatch", ["game_winner"], ["main_match"], 2_000),
                "0xmap": profile("0xmap", ["map_winner"], ["map_winner"], 600),
            },
            now_ts=now,
            min_participated_markets=99,
            min_avg_market_cash=9_999,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xmap"])

    def test_esports_leaderboard_best_bucket_score_uses_quality_before_volume(self):
        now = 1_000_000

        def profile(wallet, *, wilson, edge, positive, roi, avg_cash):
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": ["game_winner"],
                "last_esports_trade_at": now,
                "positive_market_rate": 0.50,
                "wilson_win_rate_lower_bound": 0.45,
                "capital_weighted_edge": 0.01,
                "median_market_roi": -0.10,
                "esports_roi": -0.10,
                "per_type_grades": {
                    "game_winner": {
                        "grade": "A",
                        "esports_win_count": 18,
                        "esports_loss_count": 2,
                        "esports_closed_count": 20,
                        "positive_market_rate": positive,
                        "wilson_win_rate_lower_bound": wilson,
                        "entry_edge": edge,
                        "capital_weighted_edge": edge,
                        "median_market_roi": roi,
                        "esports_roi": roi,
                    }
                },
                "candidate": {
                    "qualified_market_types": ["game_winner"],
                    "per_type_candidate": {
                        "game_winner": {
                            "participated_market_count": 20,
                            "avg_market_cash": avg_cash,
                            "tail_entry_market_count": 0,
                            "two_sided_market_count": 0,
                            "high_churn_market_count": 0,
                            "total_cash_volume": avg_cash * 20,
                        }
                    },
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xvolume": profile("0xvolume", wilson=0.58, edge=0.04, positive=0.70, roi=0.18, avg_cash=10_000),
                "0xquality": profile("0xquality", wilson=0.72, edge=0.18, positive=0.90, roi=0.35, avg_cash=900),
            },
            now_ts=now,
            min_participated_markets=99,
            require_tail_entry_field=True,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xquality", "0xvolume"])
        self.assertEqual(ranked[0]["best_market_type"], "game_winner")
        self.assertIn("game_winner", ranked[0]["bucket_scores"])
        self.assertLess(ranked[0]["overall_esports_roi"], 0)

    def test_esports_leaderboard_keeps_one_wallet_with_multiple_game_buckets(self):
        now = 1_000_000
        profile = {
            "wallet": "0xmulti",
            "grade": "A",
            "eligible_market_types": ["main_match"],
            "eligible_buckets": ["cs2:main_match", "dota2:main_match"],
            "eligible_bucket_labels": ["CS2 主盘", "Dota2 主盘"],
            "eligible_game_families": ["cs2", "dota2"],
            "last_esports_trade_at": now,
            "positive_market_rate": 0.60,
            "wilson_win_rate_lower_bound": 0.50,
            "capital_weighted_edge": 0.01,
            "median_market_roi": 0.05,
            "esports_roi": 0.05,
            "per_game_type_grades": {
                "cs2:main_match": {
                    "grade": "A",
                    "bucket_key": "cs2:main_match",
                    "bucket_label": "CS2 主盘",
                    "game_family": "cs2",
                    "game_family_label": "CS2",
                    "market_type": "main_match",
                    "market_type_label": "主盘",
                    "esports_win_count": 8,
                    "esports_loss_count": 0,
                    "esports_closed_count": 8,
                    "positive_market_rate": 1.0,
                    "wilson_win_rate_lower_bound": 0.83,
                    "entry_edge": 0.30,
                    "capital_weighted_edge": 0.30,
                    "median_entry_price": 0.45,
                    "median_market_roi": 0.50,
                    "esports_roi": 0.50,
                    "last_esports_trade_at": now,
                },
                "dota2:main_match": {
                    "grade": "A",
                    "bucket_key": "dota2:main_match",
                    "bucket_label": "Dota2 主盘",
                    "game_family": "dota2",
                    "game_family_label": "Dota2",
                    "market_type": "main_match",
                    "market_type_label": "主盘",
                    "esports_win_count": 8,
                    "esports_loss_count": 0,
                    "esports_closed_count": 8,
                    "positive_market_rate": 1.0,
                    "wilson_win_rate_lower_bound": 0.82,
                    "entry_edge": 0.22,
                    "capital_weighted_edge": 0.22,
                    "median_entry_price": 0.50,
                    "median_market_roi": 0.42,
                    "esports_roi": 0.42,
                    "last_esports_trade_at": now,
                },
            },
            "candidate": {
                "qualified_buckets": ["cs2:main_match", "dota2:main_match"],
                "qualified_market_types": ["main_match"],
                "per_game_type_candidate": {
                    "cs2:main_match": {
                        "participated_market_count": 8,
                        "avg_market_cash": 2_000,
                        "tail_entry_market_count": 0,
                        "two_sided_market_count": 0,
                        "high_churn_market_count": 0,
                        "total_cash_volume": 16_000,
                    },
                    "dota2:main_match": {
                        "participated_market_count": 8,
                        "avg_market_cash": 500,
                        "tail_entry_market_count": 0,
                        "two_sided_market_count": 0,
                        "high_churn_market_count": 0,
                        "total_cash_volume": 4_000,
                    },
                },
            },
        }

        ranked = build_leaderboard_from_profiles(
            {"0xmulti": profile},
            now_ts=now,
            min_participated_markets=99,
            require_tail_entry_field=True,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xmulti"])
        self.assertEqual(ranked[0]["eligible_buckets"], ["cs2:main_match", "dota2:main_match"])
        self.assertEqual(set(ranked[0]["bucket_scores"]), {"cs2:main_match", "dota2:main_match"})
        self.assertEqual(ranked[0]["best_bucket"], "cs2:main_match")

    def test_esports_leaderboard_uses_best_bucket_activity_window(self):
        now = 1_000_000

        def profile(wallet, *, overall_last, main_last, map_last):
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": overall_last,
                "positive_market_rate": 0.80,
                "wilson_win_rate_lower_bound": 0.70,
                "capital_weighted_edge": 0.20,
                "median_market_roi": 0.30,
                "esports_roi": 0.30,
                "per_type_grades": {
                    "main_match": {
                        "grade": "A",
                        "esports_win_count": 12,
                        "esports_loss_count": 2,
                        "esports_closed_count": 14,
                        "positive_market_rate": 0.85,
                        "wilson_win_rate_lower_bound": 0.74,
                        "capital_weighted_edge": 0.20,
                        "median_entry_price": 0.50,
                        "median_market_roi": 0.40,
                        "esports_roi": 0.35,
                        "last_esports_trade_at": main_last,
                    },
                    "map_winner": {
                        "grade": "excluded",
                        "esports_win_count": 0,
                        "esports_loss_count": 1,
                        "esports_closed_count": 1,
                        "positive_market_rate": 0.0,
                        "wilson_win_rate_lower_bound": 0.0,
                        "capital_weighted_edge": -0.50,
                        "median_entry_price": 0.50,
                        "median_market_roi": -1.0,
                        "esports_roi": -1.0,
                        "last_esports_trade_at": map_last,
                    },
                },
                "candidate": {
                    "qualified_market_types": ["main_match"],
                    "per_type_candidate": {
                        "main_match": {
                            "participated_market_count": 14,
                            "avg_market_cash": 2_000,
                            "tail_entry_market_count": 0,
                            "two_sided_market_count": 0,
                            "high_churn_market_count": 0,
                        }
                    },
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xstale": profile(
                    "0xstale",
                    overall_last=now,
                    main_last=now - 4 * 86400,
                    map_last=now,
                ),
                "0xactive": profile(
                    "0xactive",
                    overall_last=now - 4 * 86400,
                    main_last=now - 2 * 86400,
                    map_last=now - 4 * 86400,
                ),
            },
            now_ts=now,
            min_participated_markets=99,
            require_tail_entry_field=True,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xactive"])
        self.assertEqual(ranked[0]["best_bucket_last_trade_at"], now - 2 * 86400)

    def test_esports_leaderboard_excludes_recent_bad_best_bucket_performance(self):
        now = 1_000_000

        def profile(wallet, *, recent_roi, recent_positive):
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": now,
                "positive_market_rate": 0.80,
                "wilson_win_rate_lower_bound": 0.70,
                "capital_weighted_edge": 0.20,
                "median_market_roi": 0.30,
                "esports_roi": 0.30,
                "per_type_grades": {
                    "main_match": {
                        "grade": "A",
                        "esports_win_count": 12,
                        "esports_loss_count": 2,
                        "esports_closed_count": 14,
                        "positive_market_rate": 0.85,
                        "wilson_win_rate_lower_bound": 0.74,
                        "capital_weighted_edge": 0.20,
                        "median_entry_price": 0.50,
                        "median_market_roi": 0.40,
                        "esports_roi": 0.35,
                        "last_esports_trade_at": now,
                        "recent_bucket_market_count": 3,
                        "recent_bucket_window_days": 7,
                        "recent_bucket_roi": recent_roi,
                        "recent_bucket_positive_rate": recent_positive,
                    },
                },
                "candidate": {
                    "qualified_market_types": ["main_match"],
                    "per_type_candidate": {
                        "main_match": {
                            "participated_market_count": 14,
                            "avg_market_cash": 2_000,
                            "tail_entry_market_count": 0,
                            "two_sided_market_count": 0,
                            "high_churn_market_count": 0,
                        }
                    },
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xbadroi": profile("0xbadroi", recent_roi=-0.01, recent_positive=0.67),
                "0xbadrate": profile("0xbadrate", recent_roi=0.20, recent_positive=0.33),
                "0xgood": profile("0xgood", recent_roi=0.10, recent_positive=0.67),
            },
            now_ts=now,
            min_participated_markets=99,
            require_tail_entry_field=True,
        )

        self.assertEqual([row["wallet"] for row in ranked], ["0xgood"])

    def test_leaderboard_tail_entry_gate_is_rate_based(self):
        now = 1_000_000

        def profile(wallet: str, *, tail: int, participated: int) -> dict:
            return {
                "wallet": wallet,
                "grade": "A",
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": now,
                "positive_market_rate": 1.0,
                "wilson_win_rate_lower_bound": 0.85,
                "entry_edge": 0.30,
                "median_market_roi": 0.5,
                "candidate": {
                    "participated_market_count": participated,
                    "avg_market_cash": 5_000,
                    "two_sided_market_count": 0,
                    "tail_entry_market_count": tail,
                },
            }

        ranked = build_leaderboard_from_profiles(
            {
                "0xelite": profile("0xelite", tail=1, participated=60),  # 1.7% -> kept
                "0xchaser": profile("0xchaser", tail=2, participated=3),  # 67% -> dropped
            },
            now_ts=now,
            min_participated_markets=3,
            min_avg_market_cash=1_500,
        )

        wallets = [row["wallet"] for row in ranked]
        self.assertIn("0xelite", wallets)
        self.assertNotIn("0xchaser", wallets)

    def test_esports_leaderboard_requires_followable_type_metrics(self):
        now = 1_000_000

        def esports_profile(wallet: str, *, per_type: dict) -> dict:
            return {
                "wallet": wallet,
                "category": "esports",
                "grade": "A",
                "eligible_market_types": list(per_type),
                "per_type": per_type,
                "per_type_grades": {key: {"grade": "A"} for key in per_type},
                "last_esports_trade_at": now,
                "positive_market_rate": 0.8,
                "wilson_win_rate_lower_bound": 0.7,
                "entry_edge": 0.1,
                "median_market_roi": 0.3,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 5_000, "two_sided_market_count": 0, "tail_entry_market_count": 0},
            }

        profiles = {
            "0xpass": esports_profile(
                "0xpass",
                per_type={
                    "main_match": {"esports_roi": 0.18, "entry_edge": -0.03, "capital_weighted_edge": 0.12, "pre_match_entry_rate": 0.5},
                    "game_winner": {"esports_roi": 0.10, "entry_edge": 0.04, "capital_weighted_edge": 0.12, "pre_match_entry_rate": 1.0},
                },
            ),
            "0xlowroi": esports_profile("0xlowroi", per_type={"main_match": {"esports_roi": 0.10, "entry_edge": 0.03, "capital_weighted_edge": 0.12, "pre_match_entry_rate": 0.5}}),
            "0xnegcapedge": esports_profile("0xnegcapedge", per_type={"main_match": {"esports_roi": 0.18, "entry_edge": 0.04, "capital_weighted_edge": -0.01, "pre_match_entry_rate": 0.5}}),
            "0xlate": esports_profile("0xlate", per_type={"main_match": {"esports_roi": 0.18, "entry_edge": 0.03, "capital_weighted_edge": 0.12, "pre_match_entry_rate": 0.1}}),
            "0xsportslate": self.sports_a_profile(
                "0xsportslate",
                league="nba",
                event_count=8,
                avg_market_cash=2_000,
                last_esports_trade_at=now,
                pre_match_entry_rate=0.0,
            ),
            "0xsportsnegedge": self.sports_a_profile(
                "0xsportsnegedge",
                league="nba",
                event_count=8,
                avg_market_cash=2_000,
                last_esports_trade_at=now,
                entry_edge=-0.01,
                capital_weighted_edge=-0.01,
            ),
            "0xsportscapedge": self.sports_a_profile(
                "0xsportscapedge",
                league="nba",
                event_count=8,
                avg_market_cash=2_000,
                last_esports_trade_at=now,
                entry_edge=-0.02,
                capital_weighted_edge=0.12,
                pre_match_entry_rate=0.8,
                per_type={
                    "main_match": {
                        "entry_edge": -0.02,
                        "capital_weighted_edge": 0.12,
                        "pre_match_entry_rate": 0.8,
                    }
                },
                per_type_grades={"main_match": {"grade": "A"}},
            ),
        }

        ranked = build_leaderboard_from_profiles(
            profiles,
            now_ts=now,
            min_participated_markets=3,
            league_event_counts={"nba": 10},
        )
        by_wallet = {row["wallet"]: row for row in ranked}

        # esports 发布层不再用美元 ROI / capital_edge 二次过滤(评分层 θ̂+edge 已守住):
        # 低 ROI、负 capital_edge 的 eligible 桶照样上榜——这正是要救的"高胜率但 ROI 低"的钱包。
        self.assertEqual(by_wallet["0xpass"]["eligible_market_types"], ["main_match", "game_winner"])
        self.assertIn("0xlowroi", by_wallet)        # 旧规则 ROI<0.12 被踢,新规则保留
        self.assertIn("0xnegcapedge", by_wallet)    # 旧规则 cap_edge<0 被踢,新规则保留
        self.assertIn("0xlate", by_wallet)
        self.assertEqual(by_wallet["0xlate"]["eligible_market_types"], ["main_match"])
        # sports 路径未改动:仍按 capital_edge 过滤。
        self.assertIn("0xsportscapedge", by_wallet)
        self.assertIn("0xsportslate", by_wallet)
        self.assertNotIn("0xsportsnegedge", by_wallet)

    def test_cached_profile_receives_fresh_candidate_metadata(self):
        cached = {"wallet": "0xabc", "grade": "A", "candidate": {"late_entry_market_count": 0}}
        candidate = {"wallet": "0xabc", "late_entry_market_count": 2, "median_last_entry_hours_to_end": 0.5}

        merged = merge_cached_profile_with_candidate(cached, candidate)

        self.assertEqual(merged["candidate"]["late_entry_market_count"], 2)
        self.assertEqual(merged["candidate"]["median_last_entry_hours_to_end"], 0.5)

    def test_existing_profiles_receive_current_candidate_metadata_before_leaderboard(self):
        profiles = {
            "0xabc": {"wallet": "0xabc", "grade": "A", "candidate": {"tail_entry_market_count": 0}},
            "0xdef": {"wallet": "0xdef", "grade": "A", "candidate": {"tail_entry_market_count": 0}},
        }
        candidates = [
            {"wallet": "0xABC", "tail_entry_market_count": 1, "avg_entry_price": 0.8},
        ]

        merged = merge_profiles_with_candidates(profiles, candidates)

        self.assertEqual(merged["0xabc"]["candidate"]["tail_entry_market_count"], 1)
        self.assertEqual(merged["0xabc"]["candidate"]["avg_entry_price"], 0.8)
        self.assertEqual(merged["0xdef"]["candidate"]["tail_entry_market_count"], 0)

    def test_existing_profiles_receive_current_esports_bucket_qualification(self):
        profiles = {
            "0xabc": {
                "wallet": "0xabc",
                "grade": "A",
                "candidate": {"participated_market_count": 50, "avg_market_cash": 2_000},
            },
        }
        profile_candidates = [
            {
                "wallet": "0xABC",
                "qualified_market_types": ["game_winner"],
                "per_type_candidate": {
                    "game_winner": {
                        "participated_market_count": 20,
                        "avg_market_cash": 900,
                        "total_cash_volume": 18_000,
                    }
                },
            },
        ]

        merged = merge_profiles_with_candidates(profiles, profile_candidates)

        self.assertEqual(merged["0xabc"]["candidate"]["qualified_market_types"], ["game_winner"])
        self.assertEqual(
            merged["0xabc"]["candidate"]["per_type_candidate"]["game_winner"]["participated_market_count"],
            20,
        )

    def test_profile_fetch_plan_uses_70_30_budget_and_candidate_priority(self):
        candidates = [
            {"wallet": "0xC1"},
            {"wallet": "0xC2"},
            {"wallet": "0xC3"},
            {"wallet": "0xOverlap"},
        ]
        existing = {
            "0xc1": {
                "wallet": "0xC1",
                "profile_state": "qualified",
                "profiled_at": 100,
                "esports_condition_ids": ["m1"],
                "scoring_version": 999,
            },
            "0xoverlap": {
                "wallet": "0xOverlap",
                "profile_state": "qualified",
                "profiled_at": 100,
                "esports_condition_ids": [],
                "scoring_version": 999,
            },
            "0xm1": {"wallet": "0xM1", "profile_state": "qualified", "profiled_at": 100},
            "0xm2": {"wallet": "0xM2", "profile_state": "qualified", "profiled_at": 100},
            "0xm3": {"wallet": "0xM3", "profile_state": "qualified", "profiled_at": 100},
        }

        plan = build_profile_fetch_plan(
            candidates,
            existing,
            now_ts=200,
            ttl_seconds=7 * 86400,
            max_profiles=4,
        )

        self.assertEqual([row["wallet"].lower() for row in plan], ["0xc1", "0xc2", "0xc3", "0xm1"])

    def test_profile_fetch_plan_prioritizes_esports_qualified_volume(self):
        candidates = [
            {
                "wallet": "0xlow",
                "qualified_market_types": ["main_match"],
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 15,
                        "avg_market_cash": 1_500,
                        "total_cash_volume": 22_500,
                    }
                },
            },
            {
                "wallet": "0xmulti",
                "qualified_market_types": ["main_match", "game_winner"],
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 15,
                        "avg_market_cash": 1_600,
                        "total_cash_volume": 24_000,
                    },
                    "game_winner": {
                        "participated_market_count": 20,
                        "avg_market_cash": 900,
                        "total_cash_volume": 18_000,
                    },
                },
            },
            {
                "wallet": "0xhigh",
                "qualified_market_types": ["main_match"],
                "per_type_candidate": {
                    "main_match": {
                        "participated_market_count": 15,
                        "avg_market_cash": 5_000,
                        "total_cash_volume": 75_000,
                    }
                },
            },
        ]

        plan = build_profile_fetch_plan(candidates, {}, now_ts=200, ttl_seconds=7 * 86400, max_profiles=3)

        self.assertEqual([row["wallet"] for row in plan], ["0xmulti", "0xhigh", "0xlow"])

    def test_profile_fetch_plan_does_not_migrate_current_unqualified_missing_edge(self):
        existing = {
            "0xweak": {
                "wallet": "0xWeak",
                "grade": "C",
                "profile_state": "unqualified",
                "profiled_at": 100,
                "esports_condition_ids": ["m1"],
                "scoring_version": SCORING_VERSION,
            }
        }

        plan = build_profile_fetch_plan(
            [],
            existing,
            now_ts=200,
            ttl_seconds=7 * 86400,
            max_profiles=10,
        )

        self.assertEqual(plan, [])

    def test_profile_fetch_plan_refreshes_candidate_with_missing_current_grade_fields(self):
        candidates = [{"wallet": "0xA"}]
        existing = {
            "0xa": {
                "wallet": "0xA",
                "grade": "A",
                "profile_state": "qualified",
                "profiled_at": 100,
                "esports_condition_ids": ["m1"],
                "scoring_version": SCORING_VERSION,
            }
        }

        plan = build_profile_fetch_plan(
            candidates,
            existing,
            now_ts=200,
            ttl_seconds=7 * 86400,
            max_profiles=10,
        )

        self.assertEqual([row["wallet"] for row in plan], ["0xa"])

    def test_profile_fetch_plan_forces_favorite_wallets_outside_budget(self):
        candidates = [
            {"wallet": "0xA"},
            {"wallet": "0xFav", "favorite": True},
        ]
        existing = {
            "0xa": {
                "wallet": "0xA",
                "grade": "A",
                "profile_state": "qualified",
                "profiled_at": 200,
                "esports_condition_ids": ["m1"],
                "scoring_version": SCORING_VERSION,
            },
            "0xfav": {
                "wallet": "0xFav",
                "grade": "A",
                "profile_state": "qualified",
                "profiled_at": 200,
                "esports_condition_ids": ["m2"],
                "scoring_version": SCORING_VERSION,
                "positive_market_rate": 1.0,
                "entry_edge": 0.2,
            },
        }

        plan = build_profile_fetch_plan(
            candidates,
            existing,
            now_ts=300,
            ttl_seconds=7 * 86400,
            max_profiles=0,
            force_refresh_wallets={"0xfav"},
        )

        self.assertEqual([row["wallet"] for row in plan], ["0xfav"])

    def test_leaderboard_is_rebuilt_from_all_profiles_not_only_current_candidates(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "esports_roi": 0.32,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xnew": {
                "wallet": "0xnew",
                "grade": "B",
                "esports_roi": 0.31,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xbad": {
                "wallet": "0xbad",
                "grade": "C",
                "esports_roi": 0.50,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(profiles_by_wallet, now_ts=now)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xold"])

    def test_leaderboard_requires_recent_discovery_participation(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {
            "0xactive": {
                "wallet": "0xactive",
                "grade": "A",
                "esports_roi": 0.80,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 3, "avg_market_cash": 2_000},
            },
            "0xinactive": {
                "wallet": "0xinactive",
                "grade": "A",
                "esports_roi": 1.20,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 1, "avg_market_cash": 20_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=now,
            min_participated_markets=3,
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xactive"])

    def test_sports_leaderboard_requires_event_count_and_skips_recency_gate(self):
        now = 100 + 120 * 86400
        profiles_by_wallet = {
            "0xactive": self.sports_a_profile("0xactive", league="nba", event_count=8),
            "0xtiny": self.sports_a_profile("0xtiny", league="ufc", event_count=1),
            "0xlowrate": self.sports_a_profile("0xlowrate", league="ufc", event_count=8),
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=now,
            min_participated_markets=3,
            min_avg_market_cash=5_000,
            require_tail_entry_field=True,
            require_current_scoring_version=True,
            league_event_counts={"nba": 10, "ufc": 20},
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xactive", "0xlowrate"])
        self.assertEqual(leaderboard[0]["league"], "nba")
        self.assertEqual(leaderboard[0]["league_label"], "NBA")
        self.assertTrue(leaderboard[0]["flat_followable"])
        self.assertEqual(leaderboard[0]["sports_follow_mode"], "flat")
        self.assertEqual(leaderboard[0]["participated_events"], 8)
        self.assertEqual(leaderboard[0]["eligible_event_count"], 10)
        self.assertEqual(leaderboard[0]["participation_rate"], 0.8)
        self.assertEqual(leaderboard[1]["league"], "ufc")
        self.assertEqual(leaderboard[1]["participation_rate"], 0.4)

    def test_sports_leaderboard_does_not_require_half_of_full_league_schedule(self):
        profile = self.sports_a_profile("0xsports", league="ufc", event_count=8)

        leaderboard = build_leaderboard_from_profiles(
            {"0xsports": profile},
            now_ts=100 + 120 * 86400,
            min_avg_market_cash=5_000,
            require_tail_entry_field=True,
            require_current_scoring_version=True,
            league_event_counts={"ufc": 120},
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xsports"])
        self.assertEqual(leaderboard[0]["participated_events"], 8)
        self.assertEqual(leaderboard[0]["eligible_event_count"], 120)
        self.assertEqual(leaderboard[0]["participation_rate"], 0.06666667)

    def test_sports_leaderboard_uses_lower_recent_cash_floor(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {
            "0xsports": self.sports_a_profile(
                "0xsports",
                league="ufc",
                event_count=8,
                avg_market_cash=3_800,
                candidate={"participated_market_count": 10},
            )
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=now,
            min_participated_markets=3,
            min_avg_market_cash=5_000,
            require_tail_entry_field=True,
            require_current_scoring_version=True,
            league_event_counts={"ufc": 10},
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xsports"])

    def test_sports_leaderboard_excludes_weighted_roi_without_flat_winrate(self):
        profiles_by_wallet = {
            "0xflat": self.sports_a_profile(
                "0xflat",
                league="nba",
                event_count=12,
                positive_market_rate=0.75,
                median_market_roi=0.24,
                esports_roi=0.21,
                wilson_win_rate_lower_bound=0.60,
                median_entry_price=0.54,
                capital_weighted_edge=0.15,
            ),
            "0xweighted": self.sports_a_profile(
                "0xweighted",
                league="nba",
                event_count=48,
                esports_win_count=24,
                esports_loss_count=24,
                positive_market_rate=0.50,
                median_market_roi=-0.27,
                esports_roi=0.55,
                wilson_win_rate_lower_bound=0.41,
                median_entry_price=0.49,
                capital_weighted_edge=0.20,
            ),
            "0xhighentry": self.sports_a_profile(
                "0xhighentry",
                league="ufc",
                event_count=20,
                positive_market_rate=0.80,
                median_market_roi=0.16,
                esports_roi=0.24,
                wilson_win_rate_lower_bound=0.62,
                median_entry_price=0.76,
                capital_weighted_edge=0.10,
            ),
            "0xthin": self.sports_a_profile(
                "0xthin",
                league="ufc",
                event_count=6,
                positive_market_rate=0.83,
                median_market_roi=0.30,
                esports_roi=0.24,
                wilson_win_rate_lower_bound=0.57,
                median_entry_price=0.56,
                capital_weighted_edge=0.12,
            ),
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 120 * 86400,
            min_avg_market_cash=5_000,
            require_tail_entry_field=True,
            require_current_scoring_version=True,
            league_event_counts={"nba": 80, "ufc": 80},
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xflat"])
        self.assertTrue(leaderboard[0]["flat_followable"])
        self.assertEqual(leaderboard[0]["sports_follow_mode"], "flat")

    def test_leaderboard_defaults_to_top_30_a_wallets(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {}
        for index in range(35):
            wallet = f"0x{index:040x}"
            profiles_by_wallet[wallet] = {
                "wallet": wallet,
                "grade": "A",
                "esports_roi": 0.30 + index / 100,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            }

        leaderboard = build_leaderboard_from_profiles(profiles_by_wallet, now_ts=now)

        self.assertEqual(len(leaderboard), 30)
        self.assertEqual(leaderboard[0]["esports_roi"], 0.64)
        self.assertEqual(leaderboard[-1]["esports_roi"], 0.35)

    def test_stale_ab_profile_is_not_kept_on_leaderboard(self):
        profiles_by_wallet = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "esports_roi": 0.32,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xfresh": {
                "wallet": "0xfresh",
                "grade": "A",
                "esports_roi": 0.31,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100 + 60 * 86400,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 120 * 86400,
            max_inactive_days=90,
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xfresh"])

    def test_esports_leaderboard_defaults_to_three_day_activity_window(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {
            "0xrecent": {
                "wallet": "0xrecent",
                "grade": "A",
                "esports_roi": 0.31,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": now - 3 * 86400 + 1,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xstale": {
                "wallet": "0xstale",
                "grade": "A",
                "esports_roi": 0.80,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.50,
                "last_esports_trade_at": now - 3 * 86400 - 1,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(profiles_by_wallet, now_ts=now)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xrecent"])

    def test_old_scoring_profile_is_not_kept_on_current_leaderboard(self):
        now = 100 + 10 * 86400
        profiles_by_wallet = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "scoring_version": SCORING_VERSION - 1,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xcurrent": {
                "wallet": "0xcurrent",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "esports_roi": 0.30,
                "last_esports_trade_at": now,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=now,
            require_current_scoring_version=True,
        )

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xcurrent"])

    def test_old_scoring_profile_remains_in_profile_store_for_migration(self):
        existing_profiles = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "scoring_version": SCORING_VERSION - 1,
                "last_esports_trade_at": 100,
            }
        }
        refreshed_profiles = [
            {
                "wallet": "0xnew",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "last_esports_trade_at": 100,
            }
        ]

        profiles_by_wallet = {
            normalize_wallet(row.get("wallet")): row
            for row in [*existing_profiles.values(), *refreshed_profiles]
            if normalize_wallet(row.get("wallet"))
        }

        self.assertEqual(set(profiles_by_wallet), {"0xold", "0xnew"})
        self.assertEqual(profiles_by_wallet["0xold"]["scoring_version"], SCORING_VERSION - 1)

    def test_leaderboard_trusts_grade_and_keeps_hard_discovery_guards(self):
        profiles_by_wallet = {
            "0xgood": {
                "wallet": "0xgood",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "high_price_entry_rate": 0.0,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 0},
            },
            "0xlowwin": {
                "wallet": "0xlowwin",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.94,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 0},
            },
            "0xhighentry": {
                "wallet": "0xhighentry",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.66,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 0},
            },
            "0xpricechaser": {
                "wallet": "0xpricechaser",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "high_price_entry_rate": 0.10,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 0},
            },
            "0xchurn": {
                "wallet": "0xchurn",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 1, "high_churn_market_count": 0},
            },
            "0xlateentry": {
                "wallet": "0xlateentry",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {
                    "participated_market_count": 10,
                    "avg_market_cash": 2_000,
                    "two_sided_market_count": 0,
                    "high_churn_market_count": 0,
                    "late_entry_market_count": 1,
                    "median_last_entry_hours_to_end": 0.25,
                },
            },
            "0xoccasionalsell": {
                "wallet": "0xoccasionalsell",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "historical_trade_behavior_market_count": 6,
                "sold_before_resolution_market_count": 1,
                "sold_before_resolution_market_rate": 1 / 6,
                "two_sided_trade_market_count": 0,
                "two_sided_trade_market_rate": 0.0,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0},
            },
            "0xscalper": {
                # High sold rate AND profit depends on in-game swing selling (actual >> hold):
                # excluded as swing_dependent, not for selling per se.
                "wallet": "0xscalper",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "actual_minus_hold_pnl_rate": 0.5,
                "historical_trade_behavior_market_count": 6,
                "sold_before_resolution_market_count": 4,
                "sold_before_resolution_market_rate": 4 / 6,
                "two_sided_trade_market_count": 0,
                "two_sided_trade_market_rate": 0.0,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0},
            },
            "0xprofittaker": {
                # High sold rate but actual ≈ hold: takes profit on near-decided winners
                # (~0.99) without depending on swing trading -> followable, kept.
                "wallet": "0xprofittaker",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "actual_minus_hold_pnl_rate": 0.0,
                "historical_trade_behavior_market_count": 6,
                "sold_before_resolution_market_count": 5,
                "sold_before_resolution_market_rate": 5 / 6,
                "two_sided_trade_market_count": 0,
                "two_sided_trade_market_rate": 0.0,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 0},
            },
            "0xhftbot": {
                # Re-trades most of its markets 20+ times: bot / market-maker, edge isn't
                # High churn is recorded as an observation, but splitting a position into
                # many buys is no longer a hard exclude.
                "wallet": "0xhftbot",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0, "high_churn_market_count": 8},
            },
            "0xsmallbehavior": {
                "wallet": "0xsmallbehavior",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "historical_trade_behavior_market_count": 3,
                "two_sided_trade_market_count": 2,
                "two_sided_trade_market_rate": 2 / 3,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0},
            },
            "0xtailentry": {
                "wallet": "0xtailentry",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {
                    "participated_market_count": 10,
                    "avg_market_cash": 2_000,
                    "two_sided_market_count": 0,
                    # 5/10 tail rate exceeds the 0.34 gate -> still excluded as a tail-chaser
                    "tail_entry_market_count": 5,
                },
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 10 * 86400,
            max_inactive_days=90,
        )

        self.assertEqual(
            {row["wallet"] for row in leaderboard},
            {
                "0xgood",
                "0xlowwin",
                "0xhighentry",
                "0xpricechaser",
                "0xlateentry",
                "0xoccasionalsell",
                "0xsmallbehavior",
                "0xprofittaker",
                "0xhftbot",
            },
        )

    def test_analyze_holders_compares_usd_not_token_amount(self):
        holders_response = [
            {
                "token": "token_a",
                "holders": [
                    {"proxyWallet": "0xSMART1", "amount": 100, "outcomeIndex": 0},
                    {"proxyWallet": "0xSMART2", "amount": 100, "outcomeIndex": 0},
                ],
            },
            {
                "token": "token_b",
                "holders": [
                    {"proxyWallet": "0xSMART3", "amount": 250, "outcomeIndex": 1},
                    {"proxyWallet": "0xNOISE", "amount": 1000, "outcomeIndex": 1},
                ],
            },
        ]
        leaderboard = {
            "0xsmart1": {"wallet": "0xsmart1", "grade": "A"},
            "0xsmart2": {"wallet": "0xsmart2", "grade": "B"},
            "0xsmart3": {"wallet": "0xsmart3", "grade": "A"},
        }

        result = analyze_holders(
            holders_response,
            leaderboard,
            outcomes=["A", "B"],
            outcome_prices=[0.8, 0.2],
        )

        self.assertEqual(result["signal_level"], "candidate")
        self.assertEqual(result["signal_side"], "A")
        self.assertEqual(result["sides"][0]["qualified_holder_usd"], 160)
        self.assertEqual(result["sides"][1]["qualified_holder_usd"], 50)

    def test_analyze_holders_uses_outcome_index_not_response_order(self):
        holders_response = [
            {
                "holders": [
                    {"proxyWallet": "0xSMART2", "amount": 100, "outcomeIndex": 1},
                ],
            },
            {
                "holders": [
                    {"proxyWallet": "0xSMART1", "amount": 100, "outcomeIndex": 0},
                ],
            },
        ]
        leaderboard = {
            "0xsmart1": {"wallet": "0xsmart1", "grade": "A"},
            "0xsmart2": {"wallet": "0xsmart2", "grade": "A"},
        }

        result = analyze_holders(
            holders_response,
            leaderboard,
            outcomes=["A", "B"],
            outcome_prices=[0.8, 0.2],
        )

        self.assertEqual(result["sides"][0]["qualified_holder_usd"], 80)
        self.assertEqual(result["sides"][1]["qualified_holder_usd"], 20)
        self.assertEqual(result["sides"][0]["holders"][0]["wallet"], "0xsmart1")
        self.assertEqual(result["sides"][1]["holders"][0]["wallet"], "0xsmart2")

    def test_two_sided_holder_is_excluded_from_qualified_signal(self):
        holders_response = [
            {"holders": [{"proxyWallet": "0xSMART", "amount": 100, "outcomeIndex": 0}]},
            {"holders": [{"proxyWallet": "0xSMART", "amount": 100, "outcomeIndex": 1}]},
        ]
        leaderboard = {"0xsmart": {"wallet": "0xsmart", "grade": "A"}}

        result = analyze_holders(
            holders_response,
            leaderboard,
            outcomes=["A", "B"],
            outcome_prices=[0.6, 0.4],
        )

        self.assertEqual(result["signal_level"], "ignore")
        self.assertIn("two_sided_holder", result["reasons"])
        self.assertEqual(result["sides"][0]["qualified_wallet_count"], 0)
        self.assertEqual(result["sides"][1]["qualified_wallet_count"], 0)

    def test_unknown_two_sided_holder_does_not_emit_two_sided_reason(self):
        holders_response = [
            {"holders": [{"proxyWallet": "0xNOISE", "amount": 100, "outcomeIndex": 0}]},
            {"holders": [{"proxyWallet": "0xNOISE", "amount": 100, "outcomeIndex": 1}]},
        ]

        result = analyze_holders(
            holders_response,
            {},
            outcomes=["A", "B"],
            outcome_prices=[0.6, 0.4],
        )

        self.assertNotIn("two_sided_holder", result["reasons"])

    def test_follow_eligible_wallets_use_30_day_window(self):
        rows = [
            {"wallet": "0xA", "grade": "A", "last_esports_trade_at": 1000},
            {"wallet": "0xB", "grade": "A", "last_esports_trade_at": 1000 - 31 * 86400},
            {"wallet": "0xC", "grade": "B", "last_esports_trade_at": 1000},
        ]

        eligible = eligible_follow_wallets(rows, now_ts=1000, recency_days=30)

        self.assertEqual([row["wallet"] for row in eligible], ["0xa"])

    def test_follow_eligible_wallets_exclude_quarantined_wallets(self):
        rows = [
            {"wallet": "0xA", "grade": "A", "last_esports_trade_at": 1000},
            {"wallet": "0xB", "grade": "A", "last_esports_trade_at": 1000},
        ]

        eligible = eligible_follow_wallets(rows, now_ts=1000, recency_days=30, quarantined_wallets={"0xa"})

        self.assertEqual([row["wallet"] for row in eligible], ["0xb"])

    def test_follow_eligible_wallets_keep_favorites_despite_stale_score_but_not_quarantine(self):
        rows = [
            {
                "wallet": "0xA",
                "category": "esports",
                "grade": "C",
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": 1000 - 90 * 86400,
            }
        ]

        stale_favorite = eligible_follow_wallets(
            rows,
            now_ts=1000,
            recency_days=30,
            favorite_wallets={"esports:0xa"},
            allowed_categories={"esports"},
        )
        eligible = eligible_follow_wallets(
            rows,
            now_ts=1000,
            recency_days=30,
            quarantined_wallets={"esports:0xa"},
            favorite_wallets={"esports:0xa"},
            allowed_categories={"esports"},
        )

        self.assertEqual([row["wallet"] for row in stale_favorite], ["0xa"])
        self.assertEqual(eligible, [])

    def test_follow_eligible_wallets_use_eligible_types_not_overall_positive_rate(self):
        rows = [
            {"wallet": "0xA", "grade": "A", "eligible_market_types": ["game_winner"], "positive_market_rate": 0.60, "last_esports_trade_at": 1000},
            {"wallet": "0xB", "grade": "A", "eligible_market_types": ["game_winner"], "positive_market_rate": 0.90, "last_esports_trade_at": 1000},
        ]

        eligible = eligible_follow_wallets(rows, now_ts=1000, recency_days=30)

        self.assertEqual([row["wallet"] for row in eligible], ["0xa", "0xb"])

    def test_follow_eligible_wallets_can_be_limited_to_esports(self):
        rows = [
            {"wallet": "0xA", "category": "esports", "grade": "A", "last_esports_trade_at": 1000},
            {"wallet": "0xB", "category": "sports", "grade": "A", "last_esports_trade_at": 1000},
        ]

        eligible = eligible_follow_wallets(rows, now_ts=1000, recency_days=30, allowed_categories={"esports"})

        self.assertEqual([row["wallet"] for row in eligible], ["0xa"])

    def test_follow_store_records_wallet_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")

            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            quarantined = store.load_wallet_quarantine()

            self.assertIn("0xa", quarantined)
            self.assertEqual(quarantined["0xa"]["reason"], "material_sell")

    def test_follow_store_records_wallet_favorites_with_snapshot(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")

            store.upsert_wallet_favorite(
                "0xA",
                category="esports",
                favorite=True,
                ts=100,
                snapshot={"wallet": "0xA", "grade": "A", "eligible_market_types": ["main_match"]},
            )
            favorites = store.load_wallet_favorites()

            self.assertIn("esports:0xa", favorites)
            self.assertEqual(favorites["esports:0xa"]["wallet"], "0xa")
            self.assertEqual(favorites["esports:0xa"]["category"], "esports")
            self.assertEqual(favorites["esports:0xa"]["favorited_at"], 100)
            self.assertEqual(favorites["esports:0xa"]["snapshot"]["eligible_market_types"], ["main_match"])

            category_favorites = store.load_wallet_favorites(category="esports")
            self.assertEqual(list(category_favorites), ["0xa"])

            store.upsert_wallet_favorite("0xA", category="esports", favorite=False, ts=200)
            self.assertEqual(store.load_wallet_favorites(), {})

    def test_follow_store_favorite_does_not_clear_existing_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.upsert_wallet_quarantine("0xA", reason="manual_dashboard_quarantine", ts=100, category="esports")

            store.upsert_wallet_favorite(
                "0xA",
                category="esports",
                favorite=True,
                ts=200,
                snapshot={"wallet": "0xA", "grade": "A", "eligible_market_types": ["main_match"]},
            )

            self.assertIn("0xa", store.load_wallet_favorites(category="esports"))
            quarantine = store.load_wallet_quarantine(category="esports")
            self.assertIn("0xa", quarantine)
            self.assertEqual(quarantine["0xa"]["reason"], "manual_dashboard_quarantine")

    def test_follow_store_account_balance_ledger_is_idempotent(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")

            store.set_account_balance(100, ts=1000, source="manual")
            state = store.load_account_balance()
            self.assertTrue(state["configured"])
            self.assertEqual(state["balance_usdc"], 100)

            debit = {
                "ledger_id": "buy:sig1:t1",
                "kind": "buy",
                "amount_usdc": -25,
                "created_at": 1001,
                "signal_id": "sig1",
                "trade_id": "t1",
            }
            applied = store.apply_account_ledger([debit])
            self.assertEqual(applied["applied_count"], 1)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 75)

            applied_again = store.apply_account_ledger([debit])
            self.assertEqual(applied_again["applied_count"], 0)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 75)

            credit = {
                "ledger_id": "settle:sig1",
                "kind": "settle",
                "amount_usdc": 40,
                "created_at": 1100,
                "signal_id": "sig1",
            }
            store.apply_account_ledger([credit])
            self.assertEqual(store.load_account_balance()["balance_usdc"], 115)

    def test_account_result_ledger_credits_each_reentry_episode(self):
        # 回归:信号 exit→再入场→再 exit 复用同一 signal_id。旧 per-signal keying
        # (exit:{signal_id})只能落一条 → 第二段回笼撞主键被吞 → 本金不回笼(CHAOS 丢 $200)。
        # per-leg keying(result:{signal_id}:{trade_id})两段各自贷记。
        from poly_fight.cli import account_buy_ledger_entries, account_result_ledger_entries

        sid = "0xw:0xc:0"
        ep1 = {
            "signal_id": sid, "status": "exited", "wallet": "0xw", "condition_id": "0xc",
            "exit_at": 100, "exit_price": 0.5,
            "legs": [{"funded_stake": 50, "our_entry_price": 0.5, "trade_id": "buy1"}],
        }
        ep2 = {
            "signal_id": sid, "status": "exited", "wallet": "0xw", "condition_id": "0xc",
            "exit_at": 200, "exit_price": 0.44,
            "legs": [{"funded_stake": 50, "our_entry_price": 0.44, "trade_id": t} for t in ("b2", "b3", "b4", "b5")],
        }
        e1 = account_result_ledger_entries([ep1], created_at=100)
        e2 = account_result_ledger_entries([ep2], created_at=200)
        self.assertAlmostEqual(sum(e["amount_usdc"] for e in e1), 50.0, places=6)
        self.assertAlmostEqual(sum(e["amount_usdc"] for e in e2), 200.0, places=6)
        # 两段 ledger_id 不相交,否则 apply 会把第二段当重复吞掉
        self.assertEqual({e["ledger_id"] for e in e1} & {e["ledger_id"] for e in e2}, set())

        # 端到端:5 笔买入扣 $250,两段退出各回笼 → 净 0,余额回到初始
        with TemporaryDirectory() as tmp2:
            store = FollowStore(Path(tmp2) / "follow.db")
            store.set_account_balance(1000, ts=1, source="manual")
            store.apply_account_ledger(account_buy_ledger_entries([ep1, ep2], created_at=1))
            self.assertEqual(store.load_account_balance()["balance_usdc"], 750.0)  # -250
            store.apply_account_ledger(e1)
            store.apply_account_ledger(e2)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 1000.0)  # 两段都回笼
            # 幂等:重放不重复贷记
            store.apply_account_ledger(e1 + e2)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 1000.0)

    def test_follow_store_persists_follow_strategy_and_balance(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            strategy = default_follow_strategy(balance_usdc=250)
            strategy["stake_sizing"]["mode"] = "fixed"
            strategy["stake_sizing"]["fixed_usdc"] = 50
            strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 12
            strategy["condition_limits"]["order_count_mode"] = "condition"
            strategy["condition_limits"]["max_orders"] = 3

            saved = store.save_follow_strategy(strategy, ts=1234)
            loaded = store.load_follow_strategy()

            self.assertTrue(saved["configured"])
            self.assertEqual(saved["updated_at"], 1234)
            self.assertEqual(loaded["stake_sizing"]["mode"], "fixed")
            self.assertEqual(loaded["stake_sizing"]["fixed_usdc"], 50)
            self.assertEqual(loaded["prefilters"]["min_target_wallet_order_cash_usdc"], 12)
            self.assertEqual(loaded["condition_limits"]["max_orders"], 3)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 250)

            readonly = store.load_follow_strategy_readonly()
            self.assertEqual(readonly["balance"]["usable_balance_usdc"], 250)

    def test_follow_store_rejects_invalid_follow_strategy(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            strategy = default_follow_strategy(balance_usdc=100)
            strategy["stake_sizing"]["mode"] = "fixed"
            strategy["stake_sizing"]["fixed_usdc"] = 0

            with self.assertRaisesRegex(ValueError, "fixed_usdc"):
                store.save_follow_strategy(strategy, ts=1234)

            self.assertFalse(store.load_follow_strategy()["configured"])

    def test_kelly_stake_sizing_engine(self):
        # 跟单额 = 镜像比例 × 钱包买入额,夹 [min_stake, 单场剩余];edge 仅当门(θ̂×0.95>现价)。
        # 见 review/follow-sizing-conviction-and-dynamic-bankroll.md。
        s = default_follow_strategy(balance_usdc=2000)
        s["stake_sizing"]["per_match_cap_percent"] = 10.0   # 单场cap = $200@2000
        s["stake_sizing"]["min_stake_usdc"] = 10.0
        s["stake_sizing"]["follow_mirror_percent"] = 10.0
        self.assertEqual(s["stake_sizing"]["mode"], "kelly")

        def ev(order_cash, theta=0.74, p=0.66, cond=0.0, avail=2000):
            return evaluate_follow_candidate(
                strategy=s, target_wallet_order_cash_usdc=order_cash, available_balance_usdc=avail,
                condition_funded_stake_usdc=cond, condition_funded_order_count=0,
                wallet_condition_funded_order_count=0,
                bucket_win_rate=theta, entry_price=p, bankroll_usdc=2000,
            )

        # 镜像 10%:随钱包买入额线性,小单不再死按 cap
        self.assertEqual(ev(300)["funded_stake"], 30)        # 10% × 300
        self.assertEqual(ev(300)["stake_mode"], "kelly_mirror")
        self.assertEqual(ev(50)["funded_stake"], 10)         # 10%×50=5 → 提到下限 $10
        self.assertEqual(ev(2000)["funded_stake"], 200)      # 10%×2000 = 单场cap $200
        self.assertEqual(ev(9508)["funded_stake"], 200)      # 撞单场cap $200
        # edge 门:现价 ≥ θ̂×0.95 → 不跟;edge 正常 → 跟
        self.assertEqual(ev(300, theta=0.80, p=0.77)["block_reason"], "no_live_edge")  # 0.76 ≤ 0.77
        self.assertTrue(ev(300, theta=0.80, p=0.50)["would_follow"])
        # 单场:已投$190 → 剩$10=min → 跟$10;已投$195 → 剩$5<min → match_cap_reached
        self.assertEqual(ev(9508, cond=190)["funded_stake"], 10)
        self.assertEqual(ev(9508, cond=195)["block_reason"], "match_cap_reached")

    def test_kelly_mirror_dynamic_bankroll(self):
        # 镜像 sizing 的动态 bankroll:单场cap 随传入权益走(非静态 usable_balance);
        # 镜像比例直接乘钱包买入额,不受权益影响(见 review/follow-sizing-...md)。
        s = default_follow_strategy(balance_usdc=5000)   # 静态 usable_balance=5000
        s["stake_sizing"]["per_match_cap_percent"] = 10.0
        s["stake_sizing"]["min_stake_usdc"] = 10.0
        s["stake_sizing"]["follow_mirror_percent"] = 10.0

        def ev(order_cash, bankroll, cond=0.0):
            return evaluate_follow_candidate(
                strategy=s, target_wallet_order_cash_usdc=order_cash, available_balance_usdc=bankroll,
                condition_funded_stake_usdc=cond, condition_funded_order_count=0,
                wallet_condition_funded_order_count=0,
                bucket_win_rate=0.70, entry_price=0.55, bankroll_usdc=bankroll,
            )

        # 大单撞 cap:单场cap = 10% × 动态权益(非静态 5000)。$9508 钱包单:
        self.assertEqual(ev(9508, 5000)["funded_stake"], 500)   # cap 10% × 5000
        self.assertEqual(ev(9508, 6000)["funded_stake"], 600)   # 权益涨 → cap 10% × 6000
        self.assertEqual(ev(9508, 4000)["funded_stake"], 400)   # 权益跌 → cap 10% × 4000
        # 小单按镜像比例,与权益无关:
        self.assertEqual(ev(200, 5000)["funded_stake"], 20)
        self.assertEqual(ev(200, 9999)["funded_stake"], 20)
        # LGD 多笔小额逐笔累加(各按 10%),不再死按 cap:
        self.assertEqual(ev(199, 5000)["funded_stake"], 19)
        self.assertEqual(ev(483, 5000, cond=30)["funded_stake"], 48)

    def test_follow_strategy_max_entry_price_default_and_clamp(self):
        # 默认 = 全系统唯一分水岭 0.85;缺字段补默认;clamp 到 [0,1];0 = 不限(均 normalize 处理)。
        self.assertEqual(default_follow_strategy()["prefilters"]["max_follow_entry_price"], 0.85)
        miss = normalize_follow_strategy({"prefilters": {"min_target_wallet_order_cash_usdc": 10}})
        self.assertEqual(miss["prefilters"]["max_follow_entry_price"], 0.85)
        self.assertEqual(
            normalize_follow_strategy({"prefilters": {"max_follow_entry_price": 1.5}})["prefilters"]["max_follow_entry_price"],
            1.0,
        )
        self.assertEqual(
            normalize_follow_strategy({"prefilters": {"max_follow_entry_price": -0.2}})["prefilters"]["max_follow_entry_price"],
            0.0,
        )
        s = default_follow_strategy(balance_usdc=100)
        s["prefilters"]["max_follow_entry_price"] = 0.7
        self.assertTrue(validate_follow_strategy(s)[0])

    def test_follow_store_allows_strategy_without_balance_limit(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.set_account_balance(100, ts=100, source="manual")
            strategy = default_follow_strategy()
            strategy["stake_sizing"]["mode"] = "fixed"
            strategy["stake_sizing"]["fixed_usdc"] = 10
            strategy["balance"]["usable_balance_usdc"] = 0

            saved = store.save_follow_strategy(strategy, ts=200)

            self.assertTrue(saved["configured"])
            self.assertEqual(saved["balance"]["usable_balance_usdc"], 0)
            self.assertFalse(store.load_account_balance()["configured"])

    def _library_strategy(self, *, ratio=10.0):
        strategy = default_follow_strategy()
        strategy["configured"] = True
        strategy["stake_sizing"]["mode"] = "proportional"
        strategy["stake_sizing"]["ratio_percent"] = ratio
        strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 10
        return strategy

    def test_follow_strategy_library_first_create_auto_activates(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            self.assertEqual(store.list_follow_strategies(), {"strategies": [], "active_slug": None})

            entry = store.create_follow_strategy("稳健", self._library_strategy(ratio=12), ts=100)
            self.assertTrue(entry["active"])
            listing = store.list_follow_strategies()
            self.assertEqual(listing["active_slug"], entry["slug"])
            self.assertEqual(len(listing["strategies"]), 1)
            # the runner-facing active row mirrors the first (auto-active) strategy
            active = store.load_follow_strategy()
            self.assertTrue(active["configured"])
            self.assertEqual(active["stake_sizing"]["ratio_percent"], 12)

    def test_follow_strategy_library_second_create_inactive(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.create_follow_strategy("A", self._library_strategy(ratio=10), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(ratio=20), ts=200)
            self.assertFalse(b["active"])
            # active row still the first one
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 10)

    def test_follow_strategy_library_rejects_duplicate_name(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.create_follow_strategy("稳健", self._library_strategy(), ts=100)
            with self.assertRaisesRegex(ValueError, "duplicate_name"):
                store.create_follow_strategy("稳健", self._library_strategy(), ts=200)
            with self.assertRaisesRegex(ValueError, "duplicate_name"):
                store.create_follow_strategy("  稳健  ", self._library_strategy(), ts=200)

    def test_follow_strategy_library_requires_name(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            with self.assertRaisesRegex(ValueError, "name_required"):
                store.create_follow_strategy("   ", self._library_strategy(), ts=100)

    def test_follow_strategy_library_activate_switches_and_mirrors(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            a = store.create_follow_strategy("A", self._library_strategy(ratio=10), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(ratio=25), ts=200)

            listing = store.activate_follow_strategy(b["slug"], ts=300)
            self.assertEqual(listing["active_slug"], b["slug"])
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 25)
            actives = [e["active"] for e in listing["strategies"]]
            self.assertEqual(actives.count(True), 1)

            store.activate_follow_strategy(a["slug"], ts=400)
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 10)

    def test_follow_strategy_library_update_active_mirrors_runner_row(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            a = store.create_follow_strategy("A", self._library_strategy(ratio=10), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(ratio=20), ts=200)

            # updating the non-active strategy must not touch the runner row
            store.update_follow_strategy_entry(b["slug"], "B2", self._library_strategy(ratio=33), ts=300)
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 10)

            # updating the active strategy mirrors into the runner row
            store.update_follow_strategy_entry(a["slug"], "A2", self._library_strategy(ratio=44), ts=400)
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 44)
            names = sorted(e["name"] for e in store.list_follow_strategies()["strategies"])
            self.assertEqual(names, ["A2", "B2"])

    def test_follow_strategy_library_update_rejects_duplicate_and_missing(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.create_follow_strategy("A", self._library_strategy(), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(), ts=200)
            with self.assertRaisesRegex(ValueError, "duplicate_name"):
                store.update_follow_strategy_entry(b["slug"], "A", self._library_strategy(), ts=300)
            with self.assertRaisesRegex(ValueError, "strategy_not_found"):
                store.update_follow_strategy_entry("nope", "X", self._library_strategy(), ts=300)

    def test_follow_strategy_library_delete_active_promotes_last_remaining(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            a = store.create_follow_strategy("A", self._library_strategy(ratio=10), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(ratio=20), ts=200)

            listing = store.delete_follow_strategy_entry(a["slug"], ts=300)
            # exactly one remains → it is auto-promoted to active and mirrored
            self.assertEqual(listing["active_slug"], b["slug"])
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 20)

    def test_follow_strategy_library_delete_active_leaves_none_when_many(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            a = store.create_follow_strategy("A", self._library_strategy(), ts=100)
            store.create_follow_strategy("B", self._library_strategy(), ts=200)
            store.create_follow_strategy("C", self._library_strategy(), ts=300)

            listing = store.delete_follow_strategy_entry(a["slug"], ts=400)
            # two remain, neither auto-activated → runner sees no configured strategy
            self.assertIsNone(listing["active_slug"])
            self.assertEqual(len(listing["strategies"]), 2)
            self.assertFalse(store.load_follow_strategy()["configured"])

    def test_follow_strategy_library_delete_inactive_keeps_active(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            a = store.create_follow_strategy("A", self._library_strategy(ratio=10), ts=100)
            b = store.create_follow_strategy("B", self._library_strategy(ratio=20), ts=200)

            listing = store.delete_follow_strategy_entry(b["slug"], ts=300)
            self.assertEqual(listing["active_slug"], a["slug"])
            self.assertEqual(store.load_follow_strategy()["stake_sizing"]["ratio_percent"], 10)

    def test_follow_store_clears_legacy_quarantine_reasons(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            store.upsert_wallet_quarantine("0xB", reason="two_sided_switch", ts=100)
            store.upsert_wallet_quarantine("0xC", reason="observed_paper_underperformance", ts=100)

            store.clear_wallet_quarantine_reasons({"material_sell", "two_sided_switch"})
            quarantined = store.load_wallet_quarantine()

            self.assertNotIn("0xa", quarantined)
            self.assertNotIn("0xb", quarantined)
            self.assertIn("0xc", quarantined)

    def test_follow_store_clears_only_revalidated_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            store.upsert_wallet_quarantine("0xB", reason="material_sell", ts=300)
            store.upsert_wallet_quarantine("0xC", reason="manual_dashboard_quarantine", ts=100)

            store.clear_revalidated_quarantine({"0xa", "0xb", "0xc"}, validated_at=200)
            quarantined = store.load_wallet_quarantine()

            self.assertNotIn("0xa", quarantined)
            self.assertIn("0xb", quarantined)
            self.assertIn("0xc", quarantined)

    def test_follow_event_gate_detects_imminent_esports_start(self):
        now = 1000
        markets = [
            {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(now + 2 * 3600, timezone.utc).isoformat()},
            {"condition_id": "m2", "match_start_time": datetime.fromtimestamp(now + 20 * 3600, timezone.utc).isoformat()},
        ]

        self.assertTrue(esports_match_imminent(markets, now_ts=now, horizon_hours=12))
        self.assertFalse(esports_match_imminent(markets, now_ts=now + 3 * 3600, horizon_hours=12))
        self.assertTrue(esports_match_imminent([{"condition_id": "m3"}], now_ts=now, horizon_hours=12))

    def test_follow_v2_desired_tick_interval_curve(self):
        now = 1000
        far = {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(now + 24 * 3600, timezone.utc).isoformat()}
        near = {"condition_id": "m2", "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()}
        post = {"condition_id": "m3", "match_start_time": datetime.fromtimestamp(now - 3600, timezone.utc).isoformat()}

        self.assertEqual(desired_tick_interval([], [], now_ts=now), 900)
        self.assertEqual(desired_tick_interval([far], [], now_ts=now), 900)
        self.assertEqual(desired_tick_interval([near], [], now_ts=now), 180)
        self.assertEqual(desired_tick_interval([post], [], now_ts=now), 180)
        self.assertEqual(desired_tick_interval([], [{"status": "open"}], now_ts=now), 180)

    def test_follow_v2_fixed_tick_interval_overrides_curve(self):
        now = 1000
        far = {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(now + 24 * 3600, timezone.utc).isoformat()}
        # fixed cadence ignores the adaptive curve and open-signal shortcut alike
        self.assertEqual(desired_tick_interval([far], [], now_ts=now, fixed_tick_seconds=120), 120)
        self.assertEqual(desired_tick_interval([], [], now_ts=now, fixed_tick_seconds=120), 120)
        self.assertEqual(desired_tick_interval([], [{"status": "open"}], now_ts=now, fixed_tick_seconds=120), 120)
        # 0 falls back to the adaptive curve
        self.assertEqual(desired_tick_interval([far], [], now_ts=now, fixed_tick_seconds=0), 900)

    def test_watched_markets_includes_upcoming_and_in_play(self):
        # 已开赛但未结算的盘持续 watch(盘中也能跟,由 edge 闸决定);太远的未来暂不纳入。
        now = 1000
        markets = {
            "in_play": {"condition_id": "in_play", "match_start_time": datetime.fromtimestamp(now - 1200, timezone.utc).isoformat()},
            "future": {"condition_id": "future", "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()},
            "far": {"condition_id": "far", "match_start_time": datetime.fromtimestamp(now + 30 * 3600, timezone.utc).isoformat()},
        }

        watched = watched_markets(markets, now_ts=now, observe_window_hours=24)

        self.assertEqual(set(watched), {"in_play", "future"})

    def test_watched_markets_excludes_resolved(self):
        # 已结算(outcome_prices 已定盘)的盘剔除,即使仍在窗口内。
        now = 1000
        markets = {
            "live": {"condition_id": "live", "match_start_time": datetime.fromtimestamp(now - 300, timezone.utc).isoformat()},
            "resolved": {"condition_id": "resolved", "match_start_time": datetime.fromtimestamp(now - 300, timezone.utc).isoformat(),
                          "outcome_prices": [1.0, 0.0]},
        }

        watched = watched_markets(markets, now_ts=now, observe_window_hours=24)

        self.assertEqual(set(watched), {"live"})

    def test_active_cache_scope_keeps_in_play_unresolved_submarkets(self):
        # 回归:活跃缓存纳入门必须与 watched_markets 一致——已开赛但未结算的子盘(Game2-5/
        # Match Winner/后续 map)即使开赛已久、且没有开放信号,也要保留;否则同一系列赛只能跟
        # 第一个建过仓的子盘。grace 不再用于按开始时间下界截断 in-play 盘。
        now = 1_000_000
        common = dict(now_ts=now, observe_window_hours=24, post_start_grace_seconds=0,
                      allowed_categories={"esports"})
        # 开赛 3 小时(远超任何 grace),未结算 → 仍应保留
        in_play = {"condition_id": "g3", "category": "esports",
                   "match_start_time": datetime.fromtimestamp(now - 3 * 3600, timezone.utc).isoformat(),
                   "outcome_prices": [0.5, 0.5]}
        self.assertTrue(active_market_cache_in_follow_scope(in_play, **common))
        # 已结算(官方定盘 1/0)→ 剔除。注:0.9995/0.0005 这种"近乎确定但未官方结算"算 in-play,
        # 仍保留(与 watched_markets 一致),直到 Polymarket 正式定盘。
        resolved = {**in_play, "condition_id": "g1", "outcome_prices": [1.0, 0.0]}
        self.assertFalse(active_market_cache_in_follow_scope(resolved, **common))
        # 太远的未来 → 暂不纳入
        far = {"condition_id": "far", "category": "esports",
               "match_start_time": datetime.fromtimestamp(now + 30 * 3600, timezone.utc).isoformat(),
               "outcome_prices": [0.5, 0.5]}
        self.assertFalse(active_market_cache_in_follow_scope(far, **common))
        # 类目不在范围 → 剔除
        offscope = {**in_play, "condition_id": "x", "category": "politics"}
        self.assertFalse(active_market_cache_in_follow_scope(offscope, **common))
        # 已结算但在 preserve 名单(有开放信号)→ 仍保留(供结算)
        self.assertTrue(active_market_cache_in_follow_scope(
            resolved, **common, preserve_condition_ids={"g1"}))

    def test_follow_v2_trade_cursor_cold_start_then_incremental(self):
        trades = [
            {"id": "t3", "timestamp": 30},
            {"id": "t2", "timestamp": 20},
            {"id": "t1", "timestamp": 10},
        ]

        new_trades, cursor, cold_start = select_new_trades(trades, None)

        self.assertTrue(cold_start)
        self.assertEqual(new_trades, [])
        self.assertEqual(cursor["timestamp"], 30)
        self.assertEqual(cursor["id"], "t3")

        next_trades = [{"id": "t4", "timestamp": 40}, *trades]
        new_trades, cursor, cold_start = select_new_trades(next_trades, cursor)

        self.assertFalse(cold_start)
        self.assertEqual([row["id"] for row in new_trades], ["t4"])
        self.assertEqual(cursor["id"], "t4")

    def test_select_new_trades_keeps_same_second_smaller_id(self):
        # 同一秒多笔,cursor 已停在该秒;后到、id 字典序更小的同秒交易不能被漏。
        first = [{"id": "0xff", "timestamp": 1000}, {"id": "0xee", "timestamp": 1000}]
        _new, cursor, cold = select_new_trades(first, None)
        self.assertTrue(cold)
        self.assertEqual(cursor["timestamp"], 1000)
        # 下一拨:同秒的 0x0a(< 0xee/0xff)+ 一笔更晚的 0x10@1001
        second = [*first, {"id": "0x0a", "timestamp": 1000}, {"id": "0x10", "timestamp": 1001}]
        new_trades, cursor2, _ = select_new_trades(second, cursor)
        ids = {row["id"] for row in new_trades}
        self.assertIn("0x0a", ids)        # 同秒小 id 不漏
        self.assertIn("0x10", ids)        # 更晚的也在
        self.assertNotIn("0xff", ids)     # 已处理过的不重复
        self.assertEqual(cursor2["timestamp"], 1001)
        # 向后兼容:老 cursor 只有单 id、无 seen_ids 时,同秒未见 id 仍不漏
        legacy_cursor = {"timestamp": 1000, "id": "0xee"}
        new2, _c, _ = select_new_trades(second, legacy_cursor)
        self.assertIn("0x0a", {row["id"] for row in new2})

    def test_follow_user_trades_fetch_pages_until_cursor(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def trades_for_user(self, wallet, *, limit=100, offset=0):
                self.calls.append((wallet, limit, offset))
                pages = {
                    0: [{"id": "t4", "timestamp": 40}, {"id": "t3", "timestamp": 30}],
                    2: [{"id": "t2", "timestamp": 20}, {"id": "t1", "timestamp": 10}],
                    4: [{"id": "old", "timestamp": 1}],
                }
                return pages[offset]

        trades = fetch_user_trades_until_cursor(
            FakeClient(),
            "0xA",
            previous_cursor={"timestamp": 20, "id": "t2"},
            limit=2,
            max_pages=3,
        )

        self.assertEqual([row["id"] for row in trades], ["t4", "t3", "t2", "t1"])

    def test_follow_user_trades_cold_start_fetches_one_page_only(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def trades_for_user(self, wallet, *, limit=100, offset=0):
                self.calls.append(offset)
                return [{"id": "t2", "timestamp": 20}, {"id": "t1", "timestamp": 10}]

        client = FakeClient()
        trades = fetch_user_trades_until_cursor(client, "0xA", previous_cursor=None, limit=2, max_pages=3)

        self.assertEqual([row["id"] for row in trades], ["t2", "t1"])
        self.assertEqual(client.calls, [0])

    def test_follow_user_trades_can_run_shallow_incremental_poll(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def trades_for_user(self, wallet, *, limit=100, offset=0):
                self.calls.append((limit, offset))
                if offset == 0:
                    return [{"id": f"new-{index}", "timestamp": 100 - index} for index in range(50)]
                return [{"id": "cursor", "timestamp": 1}]

        client = FakeClient()
        trades = fetch_user_trades_until_cursor(
            client,
            "0xA",
            previous_cursor={"timestamp": 1, "id": "cursor"},
            limit=50,
            max_pages=1,
        )

        self.assertEqual(len(trades), 50)
        self.assertEqual(client.calls, [(50, 0)])

    def test_follow_stake_uses_wallet_cash_ratio_and_minimum(self):
        stake, tier, ratio = follow_stake_for_signal(
            wallet_trade_cash=3000,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            available_balance=1000,
        )
        self.assertEqual((stake, tier), (300, "proportional"))
        self.assertEqual(ratio, 0.1)

        stake, tier, _ = follow_stake_for_signal(
            wallet_trade_cash=3,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            available_balance=1000,
        )
        self.assertEqual((stake, tier), (1, "minimum"))

        stake, tier, _ = follow_stake_for_signal(
            wallet_trade_cash=3000,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            available_balance=50,
        )
        self.assertEqual((stake, tier), (50, "capped"))

        stake, tier, _ = follow_stake_for_signal(
            wallet_trade_cash=3000,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            available_balance=0.5,
        )
        self.assertEqual((stake, tier), (0.0, "skipped"))

    def test_follow_stake_applies_single_leg_limit_before_balance(self):
        stake, tier, ratio = follow_stake_for_signal(
            wallet_trade_cash=3000,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            max_stake_usdc=100,
            available_balance=1000,
        )
        self.assertEqual((stake, tier), (100, "limited"))
        self.assertEqual(ratio, 0.1)

        stake, tier, _ = follow_stake_for_signal(
            wallet_trade_cash=3000,
            stake_ratio_percent=10,
            min_stake_usdc=1,
            max_stake_usdc=100,
            available_balance=50,
        )
        self.assertEqual((stake, tier), (50, "capped"))

    def test_follow_strategy_evaluator_sizes_and_floors_stakes(self):
        proportional = default_follow_strategy(balance_usdc=2000)
        proportional["stake_sizing"]["mode"] = "proportional"
        proportional["stake_sizing"]["ratio_percent"] = 10
        decision = evaluate_follow_candidate(
            strategy=proportional,
            target_wallet_order_cash_usdc=100,
            available_balance_usdc=2000,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertTrue(decision["would_follow"])
        self.assertEqual(decision["funded_stake"], 10)
        self.assertEqual(decision["stake_mode"], "proportional")

        capped = default_follow_strategy(balance_usdc=2000)
        capped["stake_sizing"]["mode"] = "proportional"
        capped["stake_sizing"]["ratio_percent"] = 10
        capped["stake_sizing"]["per_order_cap_enabled"] = True
        capped["stake_sizing"]["per_order_cap_usdc"] = 100
        decision = evaluate_follow_candidate(
            strategy=capped,
            target_wallet_order_cash_usdc=10000,
            available_balance_usdc=2000,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertEqual(decision["funded_stake"], 100)
        self.assertEqual(decision["stake_mode"], "proportional_cap")

        fixed = default_follow_strategy(balance_usdc=2000)
        fixed["stake_sizing"]["mode"] = "fixed"
        fixed["stake_sizing"]["fixed_usdc"] = 50
        decision = evaluate_follow_candidate(
            strategy=fixed,
            target_wallet_order_cash_usdc=100,
            available_balance_usdc=2000,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertEqual(decision["funded_stake"], 50)
        self.assertEqual(decision["stake_mode"], "fixed")

        balance_percent = default_follow_strategy(balance_usdc=1890)
        balance_percent["stake_sizing"]["mode"] = "balance_percent"
        balance_percent["stake_sizing"]["balance_percent"] = 1
        decision = evaluate_follow_candidate(
            strategy=balance_percent,
            target_wallet_order_cash_usdc=100,
            available_balance_usdc=1890,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertEqual(decision["funded_stake"], 18)
        self.assertEqual(decision["stake_mode"], "balance_percent")

    def test_follow_strategy_evaluator_blocks_without_clipping(self):
        strategy = default_follow_strategy(balance_usdc=100)
        strategy["stake_sizing"]["mode"] = "fixed"
        strategy["stake_sizing"]["fixed_usdc"] = 25
        strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 10
        strategy["condition_limits"]["order_count_mode"] = "condition"
        strategy["condition_limits"]["max_orders"] = 2
        strategy["condition_limits"]["stake_cap_mode"] = "fixed"
        strategy["condition_limits"]["stake_cap_usdc"] = 60

        decision = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=9,
            available_balance_usdc=100,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertFalse(decision["would_follow"])
        self.assertEqual(decision["block_reason"], "small_target_wallet_order")

        decision = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=100,
            condition_funded_stake_usdc=25,
            condition_funded_order_count=2,
            wallet_condition_funded_order_count=1,
        )
        self.assertFalse(decision["would_follow"])
        self.assertEqual(decision["block_reason"], "condition_order_cap_reached")

        strategy["condition_limits"]["order_count_mode"] = "none"
        decision = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=100,
            condition_funded_stake_usdc=50,
            condition_funded_order_count=1,
            wallet_condition_funded_order_count=1,
        )
        self.assertFalse(decision["would_follow"])
        self.assertEqual(decision["funded_stake"], 0)
        self.assertEqual(decision["target_stake"], 25)
        self.assertEqual(decision["block_reason"], "condition_stake_cap_reached")

    def test_follow_strategy_evaluator_blocks_wallet_condition_order_cap(self):
        strategy = default_follow_strategy(balance_usdc=100)
        strategy["stake_sizing"]["mode"] = "fixed"
        strategy["stake_sizing"]["fixed_usdc"] = 10
        strategy["condition_limits"]["order_count_mode"] = "wallet"
        strategy["condition_limits"]["max_orders"] = 2

        blocked = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=100,
            condition_funded_stake_usdc=20,
            condition_funded_order_count=8,
            wallet_condition_funded_order_count=2,
        )
        self.assertFalse(blocked["would_follow"])
        self.assertEqual(blocked["block_reason"], "wallet_condition_order_cap_reached")

        accepted = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=100,
            condition_funded_stake_usdc=20,
            condition_funded_order_count=8,
            wallet_condition_funded_order_count=1,
        )
        self.assertTrue(accepted["would_follow"])
        self.assertEqual(accepted["funded_stake"], 10)

    def test_follow_strategy_evaluator_caps_to_balance_below_target(self):
        strategy = default_follow_strategy(balance_usdc=100)
        strategy["stake_sizing"]["mode"] = "fixed"
        strategy["stake_sizing"]["fixed_usdc"] = 50
        # 余额 30 < target 50,但 ≥ $1 → cap 到余额下单,而不是弃单
        capped = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=30,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertTrue(capped["would_follow"])
        self.assertEqual(capped["funded_stake"], 30)
        self.assertEqual(capped["stake_mode"], "balance_capped")
        # 余额 < $1 → 真正 insufficient_balance
        broke = evaluate_follow_candidate(
            strategy=strategy,
            target_wallet_order_cash_usdc=50,
            available_balance_usdc=0.5,
            condition_funded_stake_usdc=0,
            condition_funded_order_count=0,
            wallet_condition_funded_order_count=0,
        )
        self.assertFalse(broke["would_follow"])
        self.assertEqual(broke["block_reason"], "insufficient_balance")

    def test_follow_strategy_from_legacy_args_preserves_old_ratio_shape(self):
        strategy = strategy_from_legacy_args(
            stake_usdc=1,
            stake_ratio_percent=10,
            max_stake_usdc=100,
            max_signal_stake_usdc=75,
            min_wallet_trade_cash_usdc=10,
            balance_usdc=1000,
        )
        valid, errors = validate_follow_strategy(strategy)
        self.assertTrue(valid, errors)
        self.assertEqual(strategy["stake_sizing"]["mode"], "proportional")
        self.assertEqual(strategy["stake_sizing"]["ratio_percent"], 10)
        self.assertTrue(strategy["stake_sizing"]["per_order_cap_enabled"])
        self.assertEqual(strategy["stake_sizing"]["per_order_cap_usdc"], 100)
        self.assertEqual(strategy["prefilters"]["min_target_wallet_order_cash_usdc"], 10)
        self.assertEqual(strategy["condition_limits"]["stake_cap_mode"], "fixed")
        self.assertEqual(strategy["condition_limits"]["stake_cap_usdc"], 75)

    def test_process_follow_trades_skips_unfunded_buy_when_balance_is_too_low(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 20, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
            bankroll_usdc=0.5,
        )

        self.assertEqual(stats["insufficient_balance_count"], 1)
        self.assertEqual(stats["new_leg_count"], 0)
        self.assertEqual(signals, [])

    def test_prune_unfollowed_signals_removes_legacy_zero_funded_open_signal(self):
        signals = [
            {
                "signal_id": "sig-unfunded",
                "wallet": "0xa",
                "condition_id": "m1",
                "outcome_index": 0,
                "status": "open",
                "legs": [
                    {
                        "stake": 10,
                        "funded_stake": 0,
                        "funding_status": "insufficient_balance",
                        "our_entry_price": 0.5,
                        "wallet_fill_price": 0.5,
                    }
                ],
            }
        ]

        self.assertEqual(prune_unfollowed_signals(signals), [])

    def test_dashboard_overview_and_follows_hide_unfunded_intents(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.set_account_balance(100, ts=100, source="manual")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "open-unfunded",
                        "wallet": "0xabc",
                        "condition_id": "m2",
                        "status": "open",
                        "legs": [{"stake": 10, "funded_stake": 0, "funding_status": "insufficient_balance", "would_follow": False}],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "settled-funded",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "outcome_won": True,
                        "our_paper_pnl": 0.8,
                        "hypothetical_pnl": 0.8,
                        "wallet_paper_pnl_by_wallet": {"0xabc": 1.0},
                        "legs": [{"stake": 1, "funded_stake": 1, "would_follow": True}],
                        "settled_at": 100,
                    },
                    {
                        "signal_id": "settled-unfunded",
                        "wallet": "0xabc",
                        "condition_id": "m3",
                        "status": "settled",
                        "outcome_won": True,
                        "our_paper_pnl": 0,
                        "hypothetical_pnl": 8,
                        "wallet_paper_pnl_by_wallet": {"0xabc": 0.0},
                        "legs": [{"stake": 10, "funded_stake": 0, "funding_status": "insufficient_balance", "would_follow": False}],
                        "settled_at": 101,
                    },
                ],
                performance={"wallets": {}, "total": {}},
            )

            overview = build_overview(data_dir)
            follows = build_follows(data_dir, size=10)

            self.assertEqual(overview["account_balance"]["configured"], True)
            self.assertEqual(overview["account_balance"]["balance_usdc"], 100)
            self.assertEqual(overview["total_stake"], 1)
            self.assertEqual(overview["resolved_stake"], 1)
            self.assertEqual(overview["open_exposure"], 0)
            self.assertEqual(overview["our_realized_pnl"], 0.8)
            self.assertEqual(overview["hypothetical_pnl"], 0.8)
            self.assertEqual(follows["total"], 1)
            self.assertEqual(follows["follows"][0]["condition_id"], "m1")

    def test_process_follow_trades_applies_wallet_cash_ratio_to_signal(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 6000, "timestamp": now}]
        signals, _ = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=0.05,
            bankroll_usdc=1000,
        )
        self.assertEqual(signals[0]["stake_mode"], "proportional")
        self.assertEqual(signals[0]["signal_stake"], 300)
        self.assertEqual(signals[0]["legs"][0]["stake"], 300)
        self.assertEqual(signals[0]["legs"][0]["wallet_trade_cash"], 3000)

    def test_process_follow_trades_records_observed_delay_diagnostics(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [
            {
                "id": "b1",
                "proxyWallet": "0xA",
                "market": "m1",
                "outcomeIndex": 0,
                "side": "BUY",
                "price": 0.5,
                "size": 100,
                "timestamp": now + 20,
            }
        ]

        signals, _ = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            observed_at=now + 125,
            previous_poll_at=now + 50,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        leg = signals[0]["legs"][0]
        self.assertEqual(leg["observed_at"], now + 125)
        self.assertEqual(leg["observed_delay_seconds"], 105)
        self.assertEqual(leg["previous_poll_at"], now + 50)
        self.assertEqual(leg["index_lag_lower_bound_seconds"], 30)

    def test_process_follow_trades_blocks_low_wallet_entry_price(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.37, 0.63],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.35, "size": 100, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            min_wallet_entry_price=0.4,
        )

        self.assertEqual(stats["low_entry_price_blocked_count"], 1)
        self.assertEqual(stats["ignored_trade_count"], 0)
        self.assertEqual(stats["new_leg_count"], 0)
        self.assertEqual(signals, [])

    def test_process_follow_trades_allows_ten_point_slippage(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.615, 0.385],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.56, "size": 100, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.10,
            min_wallet_entry_price=0.4,
            max_entry_price=0.85,
        )

        self.assertEqual(stats["ignored_trade_count"], 0)
        self.assertEqual(stats["high_entry_price_blocked_count"], 0)
        leg = signals[0]["legs"][0]
        self.assertTrue(leg["would_follow"])
        self.assertEqual(leg["slippage_over_wallet_entry"], 0.055)
        self.assertEqual(leg["max_entry_price"], 0.85)

    def test_process_follow_trades_blocks_high_our_entry_price(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.86, 0.14],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.80, "size": 100, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.10,
            min_wallet_entry_price=0.4,
            max_entry_price=0.85,
        )

        self.assertEqual(stats["high_entry_price_blocked_count"], 1)
        self.assertEqual(stats["ignored_trade_count"], 0)
        self.assertEqual(stats["new_leg_count"], 0)
        self.assertEqual(signals, [])

    def test_process_follow_trades_blocks_target_wallet_order_cash_under_threshold(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
            "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 18, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
            bankroll_usdc=1000,
        )

        self.assertEqual(stats["small_wallet_trade_blocked_count"], 1)
        self.assertEqual(stats["ignored_trade_count"], 0)
        self.assertEqual(stats["new_leg_count"], 0)
        self.assertEqual(signals, [])

    def test_process_follow_trades_recalculates_ratio_for_each_add(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 6000, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 600, "timestamp": now + 1},
        ]

        signals, stats = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=0.05,
            bankroll_usdc=1000,
        )

        self.assertEqual([leg["stake"] for leg in signals[0]["legs"]], [300, 30])
        self.assertEqual(stats["insufficient_balance_count"], 0)

    def test_process_follow_trades_allows_add_under_ten_percent_of_first_buy(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 1000, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 90, "timestamp": now + 1},
        ]

        signals, stats = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=0.05,
            bankroll_usdc=1000,
        )

        self.assertEqual(len(signals[0]["legs"]), 2)
        self.assertEqual([leg["stake"] for leg in signals[0]["legs"]], [50, 4.5])
        self.assertTrue(signals[0]["legs"][1]["would_follow"])
        self.assertEqual(stats["small_add_blocked_count"], 0)
        self.assertEqual(stats["new_leg_count"], 2)

    def test_process_follow_trades_strategy_condition_cap_uses_condition_id(self):
        now = 1000
        strategy = default_follow_strategy(balance_usdc=1000)
        strategy["stake_sizing"]["mode"] = "fixed"
        strategy["stake_sizing"]["fixed_usdc"] = 10
        strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 0
        strategy["condition_limits"]["order_count_mode"] = "condition"
        strategy["condition_limits"]["max_orders"] = 1
        existing = [
            {
                "signal_id": follow_signal_id("0xB", "m1", 0),
                "wallet": "0xb",
                "condition_id": "m1",
                "outcome_index": 0,
                "status": "open",
                "legs": [{"funded_stake": 10, "would_follow": True}],
            }
        ]
        market = {
            "condition_id": "m1", "event_slug": "same-event", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.5, "size": 100, "timestamp": now}]

        signals, stats = process_follow_trades(
            existing,
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
            bankroll_usdc=1000,
            follow_strategy=strategy,
        )

        self.assertEqual(signals, existing)
        self.assertEqual(stats["condition_order_cap_blocked_count"], 1)
        self.assertEqual(stats["new_leg_count"], 0)

    def test_process_follow_trades_strategy_condition_cap_does_not_share_event_slug(self):
        now = 1000
        strategy = default_follow_strategy(balance_usdc=1000)
        strategy["stake_sizing"]["mode"] = "fixed"
        strategy["stake_sizing"]["fixed_usdc"] = 10
        strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 0
        strategy["condition_limits"]["order_count_mode"] = "condition"
        strategy["condition_limits"]["max_orders"] = 1
        existing = [
            {
                "signal_id": follow_signal_id("0xB", "m1", 0),
                "wallet": "0xb",
                "condition_id": "m1",
                "outcome_index": 0,
                "status": "open",
                "legs": [{"funded_stake": 10, "would_follow": True}],
            }
        ]
        market_m2 = {
            "condition_id": "m2", "event_slug": "same-event", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m2", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 100, "timestamp": now}]

        signals, stats = process_follow_trades(
            existing,
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m2": market_m2},
            now_ts=now,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
            bankroll_usdc=1000,
            follow_strategy=strategy,
        )

        self.assertEqual(stats["condition_order_cap_blocked_count"], 0)
        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual(signals[-1]["condition_id"], "m2")
        self.assertEqual(signals[-1]["legs"][0]["condition_funded_order_count_before"], 0)

    def test_process_follow_trades_strategy_floors_stake_to_integer(self):
        now = 1000
        strategy = default_follow_strategy(balance_usdc=1000)
        strategy["stake_sizing"]["mode"] = "proportional"
        strategy["stake_sizing"]["ratio_percent"] = 10
        strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 0
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 90, "timestamp": now}]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            stake_ratio_percent=10,
            max_follow_legs=10,
            max_slippage=0.05,
            bankroll_usdc=1000,
            follow_strategy=strategy,
        )

        leg = signals[0]["legs"][0]
        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual(leg["stake"], 4)
        self.assertEqual(leg["funded_stake"], 4)
        self.assertEqual(leg["target_wallet_order_cash_usdc"], 45)
        self.assertEqual(leg["strategy_mode"], "proportional")

    def test_process_follow_trades_caps_total_stake_per_signal(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 1000, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 1000, "timestamp": now + 1},
            {"id": "b3", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 1000, "timestamp": now + 2},
        ]

        signals, stats = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, stake_ratio_percent=10, max_follow_legs=10, max_slippage=0.05,
            bankroll_usdc=1000, max_stake_usdc=50, max_signal_stake_usdc=75,
        )

        legs = signals[0]["legs"]
        self.assertEqual([leg["funded_stake"] for leg in legs], [50, 25])
        self.assertEqual([leg["funding_status"] for leg in legs], ["funded", "signal_cap"])
        self.assertEqual(legs[1]["stake_mode"], "signal_cap")
        self.assertEqual(stats["signal_cap_limited_count"], 1)
        self.assertEqual(stats["signal_cap_blocked_count"], 1)
        self.assertEqual(stats["funded_stake_usdc"], 75)
        self.assertEqual(stats["new_leg_count"], 2)

    def test_follow_v2_buy_legs_sell_mirror_exits_by_default(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match",
        }
        trades = [
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 30, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.46, "size": 25, "timestamp": now + 1},
        ]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["new_leg_count"], 2)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["wallet"], "0xa")
        self.assertEqual(len(signals[0]["legs"]), 2)
        self.assertEqual(signals[0]["legs"][0]["wallet_fill_price"], 0.45)
        self.assertEqual(signals[0]["behavior_events"][0]["kind"], "add")

        sell_market = {**market, "outcome_prices": [0.6, 0.4]}
        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.61, "size": 55, "timestamp": now + 2}],
            markets_by_condition={"m1": sell_market},
            now_ts=now + 2,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["exited_signal_count"], 1)
        self.assertEqual(signals[0]["status"], "exited")
        self.assertEqual(signals[0]["wallet_sell_size"], 55)
        self.assertTrue(wallet_behavior_summary(signals[0])["sold_before_resolution"])
        self.assertEqual(signals[0]["exit_price"], 0.6)
        self.assertGreater(signals[0]["our_realized_pnl"], 0)

    def test_follow_records_pre_start_trade_detected_after_start_within_grace(self):
        start = 1000
        now = start + 180
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.42, 0.58],
            "match_start_time": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "title": "Match",
        }
        trade = {
            "id": "b1",
            "proxyWallet": "0xA",
            "market": "m1",
            "outcomeIndex": 0,
            "side": "BUY",
            "price": 0.40,
            "size": 25,
            "timestamp": start - 30,
        }

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[trade],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            post_start_grace_seconds=300,
        )

        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual(len(signals), 1)
        self.assertTrue(signals[0]["detected_after_start"])
        self.assertTrue(signals[0]["legs"][0]["detected_after_start"])

    def test_follow_accepts_after_start_trade_when_pre_match_not_required(self):
        start = 1000
        now = start + 180
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.42, 0.58],
            "match_start_time": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "title": "Match",
        }
        trade = {
            "id": "b1",
            "proxyWallet": "0xA",
            "market": "m1",
            "outcomeIndex": 0,
            "side": "BUY",
            "price": 0.40,
            "size": 25,
            "timestamp": start + 30,
        }

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[trade],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            require_pre_match=False,
            post_start_grace_seconds=300,
        )

        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual(len(signals), 1)
        self.assertTrue(signals[0]["detected_after_start"])
        self.assertTrue(signals[0]["legs"][0]["detected_after_start"])

    def test_follow_v2_gates_new_signals_by_market_type(self):
        now = 1000
        market = {
            "condition_id": "g1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.45, 0.55],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Dota 2: A vs B (BO3)",
            "question": "Dota 2: A vs B - Game 1 Winner",
            "market_type": "game_winner",
            "market_type_label": "单局",
        }
        trade = {
            "id": "b1",
            "proxyWallet": "0xA",
            "market": "g1",
            "outcomeIndex": 0,
            "side": "BUY",
            "price": 0.40,
            "size": 25,
            "timestamp": now,
        }

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[trade],
            markets_by_condition={"g1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            eligible_market_types={"main_match"},
        )

        self.assertEqual(signals, [])
        self.assertEqual(stats["market_type_not_eligible_count"], 1)

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[trade],
            markets_by_condition={"g1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            eligible_market_types={"game_winner"},
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["market_type"], "game_winner")
        self.assertEqual(signals[0]["market_type_label"], "单局")

    def test_follow_v2_prefers_game_market_bucket_over_market_type(self):
        now = 1000
        base_market = {
            "outcomes": ["A", "B"],
            "outcome_prices": [0.45, 0.55],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "market_type": "main_match",
            "market_type_label": "主盘",
        }
        markets = {
            "cs1": {
                **base_market,
                "condition_id": "cs1",
                "title": "Counter-Strike: A vs B (BO3)",
                "question": "Counter-Strike: A vs B (BO3)",
                "game_family": "cs2",
            },
            "dota1": {
                **base_market,
                "condition_id": "dota1",
                "title": "Dota 2: A vs B (BO3)",
                "question": "Dota 2: A vs B (BO3)",
                "game_family": "dota2",
            },
        }
        trades = [
            {
                "id": "cs-buy",
                "proxyWallet": "0xA",
                "market": "cs1",
                "outcomeIndex": 0,
                "side": "BUY",
                "price": 0.40,
                "size": 25,
                "timestamp": now,
            },
            {
                "id": "dota-buy",
                "proxyWallet": "0xA",
                "market": "dota1",
                "outcomeIndex": 0,
                "side": "BUY",
                "price": 0.40,
                "size": 25,
                "timestamp": now,
            },
        ]

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=trades,
            markets_by_condition=markets,
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            eligible_market_types={"main_match"},
            eligible_buckets={"dota2:main_match"},
        )

        self.assertEqual([signal["condition_id"] for signal in signals], ["dota1"])
        self.assertEqual(signals[0]["bucket_key"], "dota2:main_match")
        self.assertEqual(signals[0]["bucket_label"], "Dota2 主盘")
        self.assertEqual(stats["market_type_not_eligible_count"], 1)

    def test_follow_v2_gates_sports_new_signals_by_league(self):
        now = 1000
        trade = {
            "id": "b1",
            "proxyWallet": "0xA",
            "market": "ufc1",
            "outcomeIndex": 0,
            "side": "BUY",
            "price": 0.40,
            "size": 25,
            "timestamp": now,
        }
        ufc_market = {
            "condition_id": "ufc1",
            "category": "sports",
            "league": "ufc",
            "outcomes": ["Fighter A", "Fighter B"],
            "outcome_prices": [0.45, 0.55],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "market_type": "main_match",
        }
        nba_market = {
            "condition_id": "nba1",
            "category": "sports",
            "league": "nba",
            "outcomes": ["Team A", "Team B"],
            "outcome_prices": [0.45, 0.55],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "market_type": "main_match",
        }

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[trade],
            markets_by_condition={"ufc1": ufc_market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            eligible_category="sports",
            eligible_market_types={"main_match"},
            eligible_leagues={"nba"},
        )

        self.assertEqual(signals, [])
        self.assertEqual(stats["league_not_eligible_count"], 1)

        signals, stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{**trade, "market": "nba1"}],
            markets_by_condition={"nba1": nba_market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            eligible_category="sports",
            eligible_market_types={"main_match"},
            eligible_leagues={"nba"},
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["league"], "nba")

    def test_follow_v4_proportional_sell_then_settle_pnl(self):
        # 目标买 100@0.40,我们跟 10% = $4 仓(10股@0.40)。目标卖一半(50股)@0.60 →
        # 我们等比例卖一半(5股):落袋 5×(0.60−0.40)= $1。余下 5股 持有到结算,A 赢(付1.0):
        # 5×(1.0−0.40)= $3。合计 our_paper_pnl = $4。
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.40, 0.60],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _ = process_follow_trades(
            [], wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.40, "size": 100, "timestamp": now}],
            markets_by_condition={"m1": market}, now_ts=now, stake_usdc=1, max_follow_legs=10, max_slippage=0.05,
        )
        self.assertEqual(signals[0]["legs"][0]["funded_stake"], 4.0)
        market["outcome_prices"] = [0.60, 0.40]  # A 涨到 0.60
        signals, stats = process_follow_trades(
            signals, wallet="0xA",
            trades=[{"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.60, "size": 50, "timestamp": now + 1}],
            markets_by_condition={"m1": market}, now_ts=now + 1, stake_usdc=1, max_follow_legs=10, max_slippage=0.05,
        )
        self.assertEqual(stats["partial_exit_count"], 1)
        self.assertAlmostEqual(signals[0]["our_sold_fraction"], 0.50, places=4)
        self.assertAlmostEqual(signals[0]["our_partial_exit_pnl"], 1.0, places=4)
        remaining, settled = settle_open_signals(signals, {"m1": 0}, now_ts=now + 100)
        self.assertEqual(len(settled), 1)
        self.assertAlmostEqual(settled[0]["our_paper_pnl"], 4.0, places=4)

    def test_follow_v4_small_sell_holds_below_min_order_without_quarantine(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 100, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.5, "size": 5, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            quarantine_sell_frac=0.2,
        )

        # 目标只卖了 5%(占我们 $4.5 仓的比例 < $1 最小单)→ 等比例攒不够 $1,先不卖、不退出、不隔离。
        self.assertEqual(stats["exited_signal_count"], 0)
        self.assertEqual(stats["partial_exit_count"], 0)
        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(signals[0]["status"], "open")
        self.assertEqual(signals[0]["wallet_sell_size"], 5)
        self.assertFalse(signals[0].get("our_sold_fraction"))  # 没卖 → 未设/0

    def test_follow_v4_cumulative_sells_partial_exit_without_quarantine(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 100, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[
                {"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.5, "size": 10, "timestamp": now + 1},
                {"id": "s2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.5, "size": 15, "timestamp": now + 2},
            ],
            markets_by_condition={"m1": market},
            now_ts=now + 2,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            quarantine_sell_frac=0.2,
        )

        # 目标累计卖 25%(攒过 $1)→ 我们等比例卖 25%,部分平仓(非全平),不隔离。
        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(stats["exited_signal_count"], 0)
        self.assertEqual(stats["partial_exit_count"], 1)
        self.assertEqual(signals[0]["wallet_sell_size"], 25)
        self.assertEqual(signals[0]["status"], "open")
        self.assertAlmostEqual(signals[0].get("our_sold_fraction"), 0.25, places=2)

    def test_follow_v4_high_profit_sell_exits_without_quarantine(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.45, 0.55],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 100, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )
        market["outcome_prices"] = [0.999, 0.001]

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.999, "size": 90, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            quarantine_sell_frac=0.2,
        )

        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(stats["exited_signal_count"], 1)
        self.assertEqual(signals[0]["status"], "exited")
        self.assertGreater(signals[0]["our_realized_pnl"], 0)

    def test_follow_v2_opposite_buy_dual_follows_by_default(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 30, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.48, "size": 25, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["opposite_blocked_count"], 1)
        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(stats["exited_signal_count"], 0)
        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual(len(signals), 2)
        self.assertEqual({signal["outcome_index"] for signal in signals}, {0, 1})
        self.assertTrue(all(signal["status"] == "open" for signal in signals))

        contested = contested_markets(signals, now_ts=now + 1)
        signals, contested_stats = apply_contested_flags(signals, contested, now_ts=now + 1)

        self.assertEqual(contested, {"m1"})
        self.assertEqual(contested_stats["contested_signal_count"], 2)
        self.assertTrue(all(signal["contested"] for signal in signals))
        self.assertTrue(all(leg["would_follow"] for signal in signals for leg in signal.get("legs") or []))

    def test_follow_v2_low_price_opposite_buy_is_skipped_without_quarantine(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.95, 0.05],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 25, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.5,
            max_entry_price=1.0,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.05, "size": 1, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.5,
            max_entry_price=1.0,
        )

        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(stats["hedge_event_count"], 0)
        self.assertEqual(stats["low_entry_price_blocked_count"], 1)
        self.assertEqual(stats["new_leg_count"], 0)
        self.assertEqual(len(signals), 1)

    def test_follow_v2_opposite_wallet_buy_opens_other_side_by_default(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.52, 0.48],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals, _stats = process_follow_trades(
            [],
            wallet="0xA",
            trades=[{"id": "a1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.50, "size": 25, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xB",
            trades=[{"id": "b1", "proxyWallet": "0xB", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.46, "size": 25, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["opposite_blocked_count"], 1)
        self.assertEqual(stats["exited_signal_count"], 0)
        self.assertEqual(stats["new_leg_count"], 1)
        self.assertEqual({signal["wallet"] for signal in signals}, {"0xa", "0xb"})
        self.assertEqual({signal["outcome_index"] for signal in signals}, {0, 1})
        self.assertTrue(all(signal["status"] == "open" for signal in signals))

    def test_follow_v2_opposite_buy_exit_on_opposite_legacy_mode(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.52, 0.48],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        signals: list[dict] = []
        for wallet, trade_id in [("0xA", "a1"), ("0xB", "b1")]:
            signals, _stats = process_follow_trades(
                signals,
                wallet=wallet,
                trades=[
                    {
                        "id": trade_id,
                        "proxyWallet": wallet,
                        "market": "m1",
                        "outcomeIndex": 0,
                        "side": "BUY",
                        "price": 0.50,
                        "size": 25,
                        "timestamp": now,
                    }
                ],
                markets_by_condition={"m1": market},
                now_ts=now,
                stake_usdc=1,
                max_follow_legs=10,
                max_slippage=0.05,
            )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xC",
            trades=[{"id": "c1", "proxyWallet": "0xC", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.48, "size": 25, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
            conflict_policy="exit_on_opposite",
        )

        self.assertEqual(stats["opposite_blocked_count"], 1)
        self.assertEqual(stats["exited_signal_count"], 2)
        self.assertEqual(len(signals), 2)
        self.assertTrue(all(signal["status"] == "exited" for signal in signals))
        self.assertTrue(all(signal["exit_reason"] == "opposite_wallet_buy" for signal in signals))
        self.assertNotIn(follow_signal_id("0xC", "m1", 1), {signal.get("signal_id") for signal in signals})

    def test_v4_contested_markets_mark_conflict_without_blocking(self):
        signals = [
            {"signal_id": "a:m1:0", "wallet": "0xa", "condition_id": "m1", "outcome_index": 0, "status": "open", "would_follow": True, "legs": [{"would_follow": True}]},
            {"signal_id": "b:m1:1", "wallet": "0xb", "condition_id": "m1", "outcome_index": 1, "status": "open", "would_follow": True, "legs": [{"would_follow": True}]},
            {"signal_id": "c:m2:0", "wallet": "0xc", "condition_id": "m2", "outcome_index": 0, "status": "open", "would_follow": True, "legs": [{"would_follow": True}]},
        ]

        contested = contested_markets(signals, now_ts=1000)
        updated, stats = apply_contested_flags(signals, contested, now_ts=1000)

        self.assertEqual(contested, {"m1"})
        self.assertEqual(stats["contested_signal_count"], 2)
        self.assertTrue(updated[0]["contested"])
        self.assertTrue(updated[0]["would_follow"])
        self.assertTrue(updated[0]["legs"][0]["would_follow"])
        self.assertFalse(updated[2].get("contested", False))
        self.assertTrue(updated[2]["would_follow"])

    def test_v4_clv_uses_closing_line_minus_entry_price(self):
        signal = {
            "outcome_index": 0,
            "legs": [
                {"wallet_fill_price": 0.45, "our_entry_price": 0.50, "stake": 1},
                {"wallet_fill_price": 0.47, "our_entry_price": 0.52, "stake": 1},
            ],
        }

        clv = compute_clv(signal, [0.60, 0.40])

        self.assertEqual(clv["wallet_clv"], 0.14)
        self.assertEqual(clv["our_clv"], 0.09)

    def test_v4_trade_level_quarantine_reason_is_disabled(self):
        signal = {"outcome_index": 0, "legs": [{"wallet_trade_size": 100}, {"wallet_trade_size": 50}]}
        dust_sell = {"side": "SELL", "size": 10}
        big_sell = {"side": "SELL", "size": 40}
        opposite_buy = {"side": "BUY", "outcomeIndex": 1}

        self.assertFalse(material_sell(signal, dust_sell, sell_frac=0.2))
        self.assertTrue(material_sell(signal, big_sell, sell_frac=0.2))
        self.assertIsNone(quarantine_reason(signal, big_sell, sell_frac=0.2))
        self.assertIsNone(quarantine_reason(signal, opposite_buy, sell_frac=0.2))

    def test_v4_closing_line_snapshot_marks_started_signals_once(self):
        now = 2000
        signals = [
            {
                "signal_id": "m1:0",
                "condition_id": "m1",
                "outcome_index": 0,
                "status": "open",
                "legs": [{"wallet_fill_price": 0.45, "our_entry_price": 0.50, "stake": 1}],
            }
        ]
        markets = {
            "m1": {
                "condition_id": "m1",
                "outcome_prices": [0.62, 0.38],
                "match_start_time": datetime.fromtimestamp(now - 10, timezone.utc).isoformat(),
            }
        }

        signals, stats = apply_closing_line_snapshots(signals, markets, now_ts=now)
        signals, second_stats = apply_closing_line_snapshots(signals, markets, now_ts=now + 1)

        self.assertEqual(stats["closing_line_snapshot_count"], 1)
        self.assertEqual(second_stats["closing_line_snapshot_count"], 0)
        self.assertEqual(signals[0]["closing_line_prices"], [0.62, 0.38])
        self.assertEqual(signals[0]["wallet_clv"], 0.17)
        self.assertEqual(signals[0]["our_clv"], 0.12)

    def test_v4_aggregate_splits_clean_and_contested_performance(self):
        settled = [
            {
                "signal_id": "clean",
                "status": "settled",
                "outcome_won": True,
                "wallet_paper_pnl_by_wallet": {"0xa": 1},
                "our_paper_pnl": 0.8,
                "wallet_clv": 0.1,
                "legs": [{"stake": 1}],
            },
            {
                "signal_id": "dirty",
                "status": "settled",
                "contested": True,
                "outcome_won": False,
                "wallet_paper_pnl_by_wallet": {"0xb": -1},
                "our_paper_pnl": -1,
                "wallet_clv": -0.05,
                "legs": [{"stake": 1}],
            },
        ]

        perf = aggregate_follow_performance({}, settled)

        self.assertEqual(perf["groups"]["clean"]["signals"], 1)
        self.assertEqual(perf["groups"]["clean"]["avg_clv"], 0.1)
        self.assertEqual(perf["groups"]["contested"]["signals"], 1)
        self.assertEqual(perf["groups"]["contested"]["wins"], 0)

    def test_follow_fill_summary_and_slippage(self):
        trades = [
            {"proxyWallet": "0xA", "outcomeIndex": 0, "price": 0.4, "size": 10, "timestamp": 1},
            {"proxyWallet": "0xA", "outcomeIndex": 0, "price": 0.5, "size": 30, "timestamp": 2},
            {"proxyWallet": "0xB", "outcomeIndex": 0, "price": 0.2, "size": 99, "timestamp": 3},
        ]
        fills = summarize_wallet_fills(trades, wallet="0xA", outcome_index=0)

        self.assertEqual(fills["fill_count"], 2)
        self.assertEqual(fills["avg_price"], 0.475)
        self.assertFalse(evaluate_slippage(0.5, 0.65, max_slippage=0.05)["would_follow"])
        self.assertTrue(evaluate_slippage(0.5, 0.53, max_slippage=0.05)["would_follow"])

    def test_follow_resolution_lookup_skips_pre_match_signals(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def list_events_paginated(self, **_kwargs):
                self.calls += 1
                return []

        now = 1000
        signal = {
            "condition_id": "m1",
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
        }
        state = {}
        client = FakeClient()

        resolutions = fetch_resolutions_for_open_signals(
            client,
            [signal],
            state=state,
            now_ts=now,
            gamma_pages=3,
            ttl_seconds=900,
        )

        self.assertEqual(resolutions, {})
        self.assertEqual(client.calls, 0)

    def test_follow_resolution_lookup_queries_open_condition_ids_directly(self):
        # Option A: resolution is a targeted batch query over the started open
        # signals' condition_ids — no broad closed-events pull, no scratch cache.
        class FakeClient:
            def __init__(self):
                self.event_calls = 0
                self.market_calls = []

            def list_events_paginated(self, **_kwargs):
                self.event_calls += 1
                return []

            def markets_by_condition_ids(self, condition_ids, *, limit=500):
                self.market_calls.append(list(condition_ids))
                return [
                    {
                        "conditionId": "m1",
                        "outcomePrices": '["0", "1"]',
                        "closed": True,
                    }
                ]

        signal = {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(500, timezone.utc).isoformat()}
        client = FakeClient()

        resolutions = fetch_resolutions_for_open_signals(
            client,
            [signal],
            state={},
            now_ts=1000,
            gamma_pages=1,
            ttl_seconds=900,
        )

        self.assertEqual(resolutions, {"m1": 1})
        self.assertEqual(client.event_calls, 0)  # no broad closed-events pull
        self.assertEqual(client.market_calls, [["m1"]])

    def test_follow_tick_does_not_write_performance_json(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xabc",
                        "grade": "A",
                        "last_esports_trade_at": int(now.timestamp()),
                    }
                ],
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, *_args, **_kwargs):
                    return []

                def positions(self, *_args, **_kwargs):
                    return []

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            command_follow(args, client=FakeClient(), emit=False)

            self.assertFalse((data_dir / "follow" / "follow_performance.json").exists())
            self.assertTrue((data_dir / "follow" / "follow.db").exists())
            self.assertFalse((data_dir / "follow" / "follow_run_log.jsonl").exists())
            self.assertTrue(FollowStore(data_dir / "follow" / "follow.db").load_run_ticks())

    def test_dashboard_health_prefers_sqlite_collection_summary_and_run_tick(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            follow_store = FollowStore(data_dir / "follow" / "follow.db")
            follow_store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[],
                performance={},
            )
            follow_store.save_run_tick(
                {
                    "created_at": 300,
                    "status": "ok",
                    "gate_open": True,
                    "watched_market_count": 3,
                    "open_signal_count": 1,
                    "new_signal_count": 1,
                    "desired_next_interval_seconds": 30,
                }
            )
            write_jsonl(data_dir / "logs" / "follow" / "follow_run_log.jsonl", [{"created_at": 100, "desired_next_interval_seconds": 10}])
            write_json(data_dir / "build_summary.json", {"collector": "legacy", "leaderboard_wallet_count": 99})
            storage_module.LeaderboardStore(data_dir / "esports" / "leaderboard.db").publish_collection(
                category="esports",
                leaderboard=[{"wallet": "0xabc", "grade": "A", "scoring_version": 7}],
                profiles=[{"wallet": "0xabc", "grade": "A", "profile_state": "qualified", "scoring_version": 7}],
                summary={"collector": "sqlite", "leaderboard_wallet_count": 1, "profiled_wallet_count": 1},
                updated_at=250,
            )

            health = build_health(data_dir, started_at=time.time())
            signal = read_stream_signal(data_dir)

            self.assertEqual(health["last_tick_at"], 300)
            self.assertEqual(health["watched_market_count"], 3)
            self.assertEqual(health["build_summary"]["collector"], "sqlite")
            self.assertEqual(health["build_summary"]["leaderboard_wallet_count"], 1)
            self.assertEqual(health["scoring_version"], 7)
            self.assertGreaterEqual(signal.run_log_mtime, 300)

    def test_follow_tick_preloads_team_logos_from_watched_events(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(data_dir / "smart_wallet_leaderboard.json", [])

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "cs2-lgc-mibr-2026-06-06",
                            "title": "Counter-Strike: Legacy vs MIBR (BO1)",
                            "tags": [{"slug": "cs2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Counter-Strike: Legacy vs MIBR (BO1)",
                                    "outcomes": ["Legacy", "MIBR"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

            calls = []

            def fake_logo_refresh(path, **kwargs):
                calls.append((path, kwargs))
                return {"watched_event_count": 1}

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--max-workers",
                    "3",
                ]
            )

            with patch("poly_fight.cli.refresh_team_logo_cache_from_active_markets", side_effect=fake_logo_refresh):
                summary = command_follow(args, client=FakeClient(), emit=False)

            self.assertEqual(summary["follow_wallet_count"], 0)
            self.assertEqual(summary["watched_market_count"], 1)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0], data_dir)
            self.assertEqual(calls[0][1]["max_workers"], 3)

    def test_follow_tick_ignores_sports_wallets_and_markets(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "esports" / "smart_wallet_leaderboard.json",
                [{"wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            _seed_leaderboard(
                data_dir / "sports" / "smart_wallet_leaderboard.json",
                [{"wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(now.timestamp()),
                    "categories": ["esports", "sports"],
                    "markets": [
                        {
                            "condition_id": "esports_m1",
                            "category": "esports",
                            "market_type": "main_match",
                            "match_start_time": start.isoformat(),
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.5, 0.5],
                        },
                        {
                            "condition_id": "sports_m1",
                            "category": "sports",
                            "market_type": "main_match",
                            "league": "nba",
                            "match_start_time": start.isoformat(),
                            "outcomes": ["C", "D"],
                            "outcome_prices": [0.5, 0.5],
                        },
                    ],
                },
            )

            requested_wallets = []

            class FakeClient:
                def trades_for_user(self, wallet, *_args, **_kwargs):
                    requested_wallets.append(wallet)
                    return []

                def positions(self, *_args, **_kwargs):
                    return []

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)

            self.assertEqual(summary["eligible_follow_wallet_count"], 1)
            self.assertEqual(summary["watched_market_count"], 1)
            self.assertEqual(requested_wallets, ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"])

    def test_follow_tick_opens_signal_from_onchain_collector_without_data_api(self):
        from unittest.mock import patch

        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "esports" / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(now.timestamp()),
                    "categories": ["esports"],
                    "markets": [
                        {
                            "condition_id": "esports_m1",
                            "category": "esports",
                            "market_type": "main_match",
                            "match_start_time": start.isoformat(),
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.5, 0.5],
                            "clob_token_ids": ["tok_yes", "tok_no"],
                        },
                    ],
                },
            )

            # data-api must NOT be consulted on the on-chain path.
            requested_wallets = []

            class FakeClient:
                def trades_for_user(self, w, *_args, **_kwargs):
                    requested_wallets.append(w)
                    return []

                def positions(self, *_args, **_kwargs):
                    return []

            onchain_fill = {
                "wallet": wallet,
                "conditionId": "esports_m1",
                "outcomeIndex": 0,
                "tokenId": "tok_yes",
                "side": "BUY",
                "size": 50.0,
                "price": 0.55,        # exact on-chain fill price (USDC/shares)
                "cash": 27.5,         # 50 * 0.55
                "transactionHash": "0xfeed",
                "logIndex": 0,
                "blockNumber": 100,
                "blockTs": int(now.timestamp()),
            }

            class FakeCollector:
                healthy = True

                def __init__(self):
                    self.asset_map_updates = 0
                    self.wallet_updates = []
                    self._emitted = False

                def update_asset_map(self, amap):
                    self.asset_map_updates += 1
                    self._last_asset_map = amap

                def update_wallets(self, wallets):
                    self.wallet_updates.append(set(wallets))

                def drain(self):
                    if self._emitted:
                        return {}
                    self._emitted = True
                    return {wallet: [dict(onchain_fill)]}

            parser = build_parser()
            args = parser.parse_args(
                ["--data-dir", str(data_dir), "follow", "--stake-usdc", "1",
                 "--user-trades-max-pages", "1", "--max-workers", "1"]
            )

            fake_collector = FakeCollector()
            with patch("poly_fight.cli.clob_price", return_value=0.55):
                summary = command_follow(args, client=FakeClient(), emit=False, collector=fake_collector)

            # build_asset_map wired the watched market's token ids into the collector.
            self.assertEqual(fake_collector.asset_map_updates, 1)
            self.assertEqual(fake_collector._last_asset_map.get("tok_yes"),
                             {"conditionId": "esports_m1", "outcomeIndex": 0})
            self.assertIn(wallet, fake_collector.wallet_updates[-1])
            # a paper signal opened from the WS fill, and data-api was never polled.
            self.assertEqual(summary["new_signal_count"], 1)
            self.assertEqual(requested_wallets, [])
            open_signals = FollowStore(data_dir / "follow" / "follow.db").load_open_signals()
            self.assertEqual(len(open_signals), 1)
            leg = (open_signals[0].get("legs") or [open_signals[0]])[0]
            self.assertAlmostEqual(float(leg.get("our_entry_price")), 0.55, places=4)

    def test_follow_tick_quarantine_takes_precedence_over_favorite(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": wallet,
                        "category": "esports",
                        "grade": "A",
                        "last_esports_trade_at": int((now - timedelta(days=90)).timestamp()),
                        "eligible_market_types": ["main_match"],
                    }
                ],
            )
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(now.timestamp()),
                    "categories": ["esports"],
                    "markets": [
                        {
                            "condition_id": "m1",
                            "category": "esports",
                            "market_type": "main_match",
                            "match_start_time": start.isoformat(),
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.5, 0.5],
                        }
                    ],
                },
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine(wallet, reason="manual_dashboard_quarantine", ts=100, category="esports")
            store.upsert_wallet_favorite(wallet, category="esports", favorite=True, ts=200, snapshot={"wallet": wallet})
            requested_wallets = []

            class FakeClient:
                def trades_for_user(self, wallet_arg, *_args, **_kwargs):
                    requested_wallets.append(wallet_arg)
                    return []

                def positions(self, *_args, **_kwargs):
                    return []

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)

            self.assertEqual(summary["eligible_follow_wallet_count"], 0)
            self.assertEqual(requested_wallets, [])
            self.assertIn(wallet, store.load_wallet_quarantine(category="esports"))

    def test_dashboard_events_read_active_markets_from_follow_db(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            start = datetime.now(timezone.utc) + timedelta(hours=2)
            FollowStore(data_dir / "follow" / "follow.db").save_market_cache(
                {
                    "m1": {
                        "condition_id": "m1",
                        "category": "esports",
                        "league": "cs2",
                        "market_type": "main_match",
                        "title": "Counter-Strike: A vs B",
                        "question": "Counter-Strike: A vs B",
                        "match_start_time": start.isoformat(),
                        "outcomes": ["A", "B"],
                        "outcome_prices": [0.42, 0.58],
                    }
                },
                cache_kind="active",
                updated_at=123,
            )

            events = build_events(data_dir)

            self.assertEqual(events["count"], 1)
            self.assertEqual(events["cache_updated_at"], 123)
            self.assertEqual(events["events"][0]["condition_id"], "m1")
            self.assertEqual(events["events"][0]["outcome_prices"], [0.42, 0.58])

    def test_sports_watched_events_missing_logos_trigger_refresh_scan(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            logo_dir = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboardV2" / "logo"
            logo_path = logo_dir / "team_logos.json"
            before_json = logo_path.read_bytes() if logo_path.exists() else None
            before_files = set(logo_dir.glob("*")) if logo_dir.exists() else set()
            start = datetime.now(timezone.utc) + timedelta(hours=2)
            try:
                write_json(
                    data_dir / "follow" / "active_market_cache.json",
                    {
                        "updated_at": int(time.time()),
                        "markets": [
                            {
                                "condition_id": "nba1",
                                "category": "sports",
                                "league": "nba",
                                "league_label": "NBA",
                                "event_slug": "pytest-bears-kings-2026-06-08",
                                "title": "Pytest Expansion Bears vs. Pytest Sample Kings",
                                "match_start_time": start.isoformat(),
                            }
                        ],
                    },
                )
                calls = []

                def fake_fetch_html(slug):
                    calls.append(slug)
                    return (
                        '<img alt="Expansion Bears" srcset="/_next/image?url=https%3A%2F%2F'
                        'polymarket-upload.s3.us-east-2.amazonaws.com%2Fopaque-bears-logo.png&amp;w=96&amp;q=75">'
                        '<img alt="Pytest Sample Kings" srcset="/_next/image?url=https%3A%2F%2F'
                        'polymarket-upload.s3.us-east-2.amazonaws.com%2Fsample-kings-logo.png&amp;w=96&amp;q=75">'
                        '<img alt="NBA" src="https://polymarket-upload.s3.us-east-2.amazonaws.com/Repetitive-markets/NBA.jpg">'
                    )

                def fake_fetch_logo_bytes(_url, _timeout_seconds):
                    return b"logo"

                summary = refresh_team_logo_cache_from_active_markets(
                    data_dir,
                    now_ts=int(time.time()),
                    fetch_html=fake_fetch_html,
                    fetch_logo_bytes=fake_fetch_logo_bytes,
                )
                logo_cache = read_json(Path(summary["path"]), {})
                local_url = logo_cache["teams"]["pytest expansion bears"]

                self.assertEqual(summary["watched_event_count"], 1)
                self.assertEqual(calls, ["pytest-bears-kings-2026-06-08"])
                self.assertTrue(logo_cache["teams"]["pytest sample kings"].startswith("/logo/"))
                self.assertNotIn("nba", logo_cache["teams"])
            finally:
                if before_json is None:
                    logo_path.unlink(missing_ok=True)
                else:
                    logo_path.write_bytes(before_json)
                for path in set(logo_dir.glob("*")) - before_files:
                    path.unlink(missing_ok=True)

    def test_follow_tick_excludes_quarantined_wallets_before_fetching(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xabc", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine("0xabc", reason="observed_paper_underperformance", ts=int(now.timestamp()))
            calls = []

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return []

                def trades_for_user(self, *_args, **_kwargs):
                    calls.append("trades")
                    return []

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(data_dir), "follow", "--stake-usdc", "1"])

            summary = command_follow(args, client=FakeClient(), emit=False)

            self.assertEqual(summary["follow_wallet_count"], 0)
            self.assertEqual(calls, [])

    def test_follow_tick_polls_open_signal_wallets_that_left_leaderboard(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(data_dir / "smart_wallet_leaderboard.json", [])
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "0xold": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10},
                },
                open_signals=[
                    {
                        "signal_id": "0xold:m1:0",
                        "wallet": "0xold",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "outcome": "A",
                        "status": "open",
                        "created_at": int(now.timestamp()) - 100,
                        "updated_at": int(now.timestamp()) - 100,
                        "match_start_time": start.isoformat(),
                        "legs": [{"stake": 1, "our_entry_price": 0.5, "wallet_fill_price": 0.5, "wallet_trade_size": 100}],
                        "behavior_events": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def __init__(self):
                    self.wallets = []

                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: A vs B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: A vs B (BO3)",
                                    "outcomes": ["A", "B"],
                                    "outcomePrices": ["0.60", "0.40"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                },
                                {
                                    "conditionId": "m2",
                                    "question": "C vs D",
                                    "outcomes": ["C", "D"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                },
                            ],
                        }
                    ]

                def trades_for_user(self, wallet, **_kwargs):
                    self.wallets.append(wallet)
                    return [
                        {"id": "new-buy", "timestamp": 20, "market": "m2", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 100},
                        {"id": "new-sell", "timestamp": 30, "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.6, "size": 100},
                    ]

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(data_dir), "follow", "--stake-usdc", "1", "--max-workers", "1"])
            client = FakeClient()

            summary = command_follow(args, client=client, emit=False)
            snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()

            self.assertEqual(client.wallets, ["0xold"])
            self.assertEqual(summary["follow_wallet_count"], 1)
            self.assertEqual(summary["lifecycle_follow_wallet_count"], 1)
            self.assertEqual(summary["exited_signal_count"], 1)
            self.assertEqual(summary["open_signal_count"], 0)
            self.assertEqual(len(snapshot["results"]), 1)
            self.assertEqual(snapshot["results"][0]["condition_id"], "m1")
            self.assertEqual(snapshot["results"][0]["status"], "exited")
            self.assertEqual(snapshot["results"][0]["exit_price"], 0.6)
            self.assertEqual(snapshot["results"][0]["our_realized_pnl"], 0.2)
            self.assertTrue(snapshot["results"][0]["wallet_behavior"]["wallet_sold_before_resolution"])
            self.assertNotIn("m2", {row.get("condition_id") for row in snapshot["open_signals"]})

    def test_follow_pause_new_signals_keeps_open_signal_lifecycle(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xsmart",
                        "category": "esports",
                        "grade": "A",
                        "eligible_market_types": ["main_match"],
                        "last_esports_trade_at": int(now.timestamp()),
                    }
                ],
            )
            write_follow_control(
                follow_dir,
                {"pause_new_signals": {"esports": {"status": "paused", "reason": "wallet_refresh"}}},
            )
            store = FollowStore(follow_dir / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "esports:0xsmart": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10},
                },
                open_signals=[
                    {
                        "signal_id": "0xsmart:m1:0",
                        "wallet": "0xsmart",
                        "category": "esports",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "outcome": "A",
                        "status": "open",
                        "created_at": int(now.timestamp()) - 100,
                        "updated_at": int(now.timestamp()) - 100,
                        "match_start_time": start.isoformat(),
                        "legs": [{"stake": 1, "our_entry_price": 0.5, "wallet_fill_price": 0.5, "wallet_trade_size": 100}],
                        "behavior_events": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def __init__(self):
                    self.wallets = []

                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: A vs B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: A vs B (BO3)",
                                    "outcomes": ["A", "B"],
                                    "outcomePrices": ["0.60", "0.40"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                },
                                {
                                    "conditionId": "m2",
                                    "question": "Dota 2: C vs D (BO3)",
                                    "outcomes": ["C", "D"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                },
                            ],
                        }
                    ]

                def trades_for_user(self, wallet, **_kwargs):
                    self.wallets.append(wallet)
                    return [
                        {"id": "new-buy", "timestamp": 20, "market": "m2", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 100},
                        {"id": "new-sell", "timestamp": 30, "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.6, "size": 100},
                    ]

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(data_dir), "follow", "--stake-usdc", "1", "--max-workers", "1"])
            summary = command_follow(args, client=FakeClient(), emit=False)
            snapshot = FollowStore(follow_dir / "follow.db").load_dashboard_snapshot()

            self.assertEqual(summary["new_signal_count"], 0)
            self.assertEqual(summary["exited_signal_count"], 1)
            self.assertEqual(summary["open_signal_count"], 0)
            self.assertEqual(len(snapshot["results"]), 1)
            self.assertEqual(snapshot["results"][0]["condition_id"], "m1")
            self.assertEqual(snapshot["results"][0]["status"], "exited")
            self.assertEqual(snapshot["results"][0]["exit_price"], 0.6)
            self.assertEqual(snapshot["results"][0]["our_realized_pnl"], 0.2)
            self.assertTrue(snapshot["results"][0]["wallet_behavior"]["wallet_sold_before_resolution"])
            self.assertNotIn("m2", {row.get("condition_id") for row in snapshot["open_signals"]})

    def test_follow_tick_disables_sports_wallets(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "sports" / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xa",
                        "category": "sports",
                        "league": "nba",
                        "grade": "A",
                        "eligible_market_types": ["main_match"],
                        "last_esports_trade_at": int(now.timestamp()),
                    }
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={"sports:0xa": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10}},
                open_signals=[],
                result_events=[],
                performance={},
            )
            wallets = []

            class FakeClient:
                def list_events_paginated(self, **kwargs):
                    tags = tuple(kwargs.get("tag_slugs") or ())
                    if tags != ("nba", "ufc"):
                        return []
                    return [
                        {
                            "id": "nba1",
                            "slug": "nba1",
                            "title": "Los Angeles Lakers vs. Boston Celtics",
                            "tags": [{"slug": "nba"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "nba1",
                                    "question": "Los Angeles Lakers vs. Boston Celtics",
                                    "outcomes": ["Los Angeles Lakers", "Boston Celtics"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        },
                        {
                            "id": "ufc1",
                            "slug": "ufc1",
                            "title": "Fighter A vs Fighter B",
                            "tags": [{"slug": "ufc"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "ufc1",
                                    "question": "Fighter A vs Fighter B",
                                    "outcomes": ["Fighter A", "Fighter B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        },
                    ]

                def trades_for_user(self, wallet, **_kwargs):
                    wallets.append(wallet)
                    return [
                        {"id": "ufc-buy", "timestamp": 20, "market": "ufc1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10},
                        {"id": "nba-buy", "timestamp": 30, "market": "nba1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10},
                    ]

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(data_dir), "follow", "--stake-usdc", "1", "--max-workers", "1"])

            summary = command_follow(args, client=FakeClient(), emit=False)
            snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()

            self.assertEqual(wallets, [])
            self.assertEqual(summary["eligible_follow_wallet_count"], 0, summary)
            self.assertEqual(summary["watched_market_count"], 0, summary)
            self.assertEqual(summary["new_signal_count"], 0, summary)
            self.assertEqual(snapshot["open_signals"], [])

    def test_follow_tick_does_not_request_positions_or_write_shadow_state(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xa", "category": "esports", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "esports:0xa": {
                        "last_trade_cursor": {"timestamp": 10, "id": "old"},
                        "last_seen_at": 10,
                    },
                },
                open_signals=[],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def __init__(self):
                    self.position_calls = []

                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, _wallet, **_kwargs):
                    return []

                def positions(self, wallet, *, limit=100):
                    self.position_calls.append((wallet, limit))
                    return [{"conditionId": "m1", "outcomeIndex": 0, "size": 10, "avgPrice": 0.5, "curPrice": 0.5, "initialValue": 5}]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )
            client = FakeClient()

            summary = command_follow(args, client=client, emit=False)
            snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()
            state = snapshot["wallet_trade_state"]["esports:0xa"]

            self.assertEqual(client.position_calls, [])
            self.assertNotIn("position_request_count", summary)
            self.assertNotIn("position_observed_count", summary)
            self.assertNotIn("position_new_count", summary)
            self.assertEqual(summary["new_signal_count"], 0)
            self.assertEqual(snapshot["open_signals"], [])
            self.assertNotIn("position_cursor_by_key", state)

    def test_incremental_backfill_queries_each_wallet_once(self):
        # 增量补单:backfilled_wallets 跨 tick 持有,每个钱包只查一次 positions——
        # startup 全量补,之后中途晋升的新钱包随到随补,已补过的不再重复查。
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xa", "category": "esports", "grade": "A",
                  "last_esports_trade_at": int(now.timestamp())}],
            )

            class FakeClient:
                def __init__(self):
                    self.position_calls = []

                def list_events_paginated(self, **_kwargs):
                    return [{
                        "id": "event1", "slug": "event1",
                        "title": "Dota 2: Team A vs Team B (BO3)",
                        "tags": [{"slug": "dota-2"}], "startTime": start.isoformat(),
                        "markets": [{
                            "conditionId": "m1", "question": "Dota 2: Team A vs Team B (BO3)",
                            "outcomes": ["Team A", "Team B"], "outcomePrices": ["0.50", "0.50"],
                            "active": True, "closed": False, "volume": 100000,
                            "startTime": start.isoformat(),
                        }],
                    }]

                def trades_for_user(self, _wallet, **_kwargs):
                    return []

                def positions(self, wallet, *, limit=100):
                    self.position_calls.append(wallet)
                    return []

            parser = build_parser()
            args = parser.parse_args([
                "--data-dir", str(data_dir), "follow", "--stake-usdc", "1",
                "--gamma-pages", "1", "--user-trades-max-pages", "1", "--max-workers", "1",
            ])
            client = FakeClient()
            backfilled: set[str] = set()

            command_follow(args, client=client, emit=False,
                           backfill_positions=True, backfilled_wallets=backfilled)
            self.assertEqual(client.position_calls, ["0xa"])   # tick1 全量补
            self.assertIn("0xa", backfilled)

            client.position_calls.clear()
            command_follow(args, client=client, emit=False,
                           backfill_positions=True, backfilled_wallets=backfilled)
            self.assertEqual(client.position_calls, [])        # tick2 已补过 → 不再查

    def test_follow_tick_trade_follow_does_not_depend_on_positions(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xa", "category": "esports", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "esports:0xa": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10},
                },
                open_signals=[],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, _wallet, **_kwargs):
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 30}]

                def positions(self, _wallet, *, limit=100):
                    raise RuntimeError("positions down")

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)
            snapshot = FollowStore(data_dir / "follow" / "follow.db").load_dashboard_snapshot()

            self.assertNotIn("position_request_count", summary)
            self.assertNotIn("position_fetch_error_count", summary)
            self.assertEqual(summary["new_signal_count"], 1)
            self.assertEqual(len(snapshot["open_signals"]), 1)

    def test_follow_tick_dual_follows_opposite_wallet_buys(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {"wallet": "0xa", "grade": "A", "last_esports_trade_at": int(now.timestamp())},
                    {"wallet": "0xb", "grade": "A", "last_esports_trade_at": int(now.timestamp())},
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "0xa": {"last_trade_cursor": {"timestamp": 10, "id": "old-a"}, "last_seen_at": 10},
                    "0xb": {"last_trade_cursor": {"timestamp": 10, "id": "old-b"}, "last_seen_at": 10},
                },
                open_signals=[],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, wallet, **_kwargs):
                    if wallet == "0xa":
                        return [{"id": "a1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 30}]
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.45, "size": 30}]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)
            snapshot = store.load_dashboard_snapshot()

            self.assertEqual(summary["opposite_blocked_count"], 1)
            self.assertEqual(summary["exited_signal_count"], 0)
            self.assertEqual(summary["contested_signal_count"], 2)
            self.assertEqual(summary["open_signal_count"], 2)
            self.assertEqual(len(snapshot["open_signals"]), 2)
            self.assertEqual(len(snapshot["results"]), 0)
            self.assertEqual({signal["outcome_index"] for signal in snapshot["open_signals"]}, {0, 1})
            self.assertTrue(all(signal["contested"] for signal in snapshot["open_signals"]))

    def test_follow_tick_logs_insufficient_balance_count(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xa", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={
                    "esports:0xa": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10},
                },
                open_signals=[],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, _wallet, **_kwargs):
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 30}]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--bankroll-usdc",
                    "0.5",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)
            snapshot = store.load_dashboard_snapshot()
            run_log = store.load_run_ticks(limit=100)

            self.assertEqual(summary["new_signal_count"], 0)
            self.assertEqual(summary["insufficient_balance_count"], 1)
            self.assertEqual(run_log[-1]["insufficient_balance_count"], 1)
            self.assertEqual(len(snapshot["open_signals"]), 0)

    def test_follow_tick_debits_configured_account_balance_for_funded_buy_once(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            wallet = "0xa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.set_account_balance(1.5, ts=10, source="manual")
            store.save_follow_snapshot(
                wallet_trade_state={
                    f"esports:{wallet}": {"last_trade_cursor": {"timestamp": 10, "id": "old"}, "last_seen_at": 10},
                },
                open_signals=[],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return [
                        {
                            "id": "event1",
                            "slug": "event1",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.50", "0.50"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, _wallet, **_kwargs):
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 20}]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 0.5)

            command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 0.5)

    def test_follow_tick_credits_account_balance_when_signal_settles_once(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now - timedelta(hours=2)
            wallet = "0xa"
            signal_id = f"{wallet}:m1:0"
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.set_account_balance(1.5, ts=10, source="manual")
            store.apply_account_ledger(
                [
                    {
                        "ledger_id": f"buy:{signal_id}:buy1",
                        "kind": "buy",
                        "amount_usdc": -1,
                        "created_at": 20,
                        "signal_id": signal_id,
                        "trade_id": "buy1",
                    }
                ]
            )
            self.assertEqual(store.load_account_balance()["balance_usdc"], 0.5)
            store.save_follow_snapshot(
                wallet_trade_state={
                    f"esports:{wallet}": {"last_trade_cursor": {"timestamp": 20, "id": "buy1"}, "last_seen_at": 20},
                },
                open_signals=[
                    {
                        "signal_id": signal_id,
                        "wallet": wallet,
                        "category": "esports",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "outcome": "Team A",
                        "status": "open",
                        "created_at": 20,
                        "updated_at": 20,
                        "match_start_time": start.isoformat(),
                        "legs": [
                            {
                                "stake": 1,
                                "funded_stake": 1,
                                "funding_status": "funded",
                                "would_follow": True,
                                "our_entry_price": 0.5,
                                "wallet_fill_price": 0.5,
                                "trade_id": "buy1",
                                "leg_at": 20,
                            }
                        ],
                        "behavior_events": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **kwargs):
                    if kwargs.get("closed") is True:
                        return [
                            {
                                "id": "event1",
                                "slug": "",
                                "title": "Dota 2: Team A vs Team B (BO3)",
                                "tags": [{"slug": "dota-2"}],
                                "startTime": start.isoformat(),
                                "markets": [
                                    {
                                        "conditionId": "m1",
                                        "question": "Dota 2: Team A vs Team B (BO3)",
                                        "outcomes": ["Team A", "Team B"],
                                        "outcomePrices": ["1", "0"],
                                        "active": False,
                                        "closed": True,
                                        "volume": 100000,
                                        "startTime": start.isoformat(),
                                    }
                                ],
                            }
                        ]
                    return []

                def trades_for_user(self, _wallet, **_kwargs):
                    return []

                def markets_by_condition_ids(self, condition_ids, *, limit=500):
                    return [
                        {
                            "conditionId": "m1",
                            "outcomes": ["Team A", "Team B"],
                            "outcomePrices": ["1", "0"],
                            "closed": True,
                        }
                    ]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--resolution-gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(summary["settled_signal_count"], 1)
            self.assertEqual(summary["balance_ledger_applied_amount_usdc"], 2)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 2.5)

            summary_again = command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(summary_again["balance_ledger_applied_count"], 0)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 2.5)

    def test_follow_tick_credits_account_balance_when_signal_exits_once(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            wallet = "0xa"
            signal_id = f"{wallet}:m1:0"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.set_account_balance(1.5, ts=10, source="manual")
            store.apply_account_ledger(
                [
                    {
                        "ledger_id": f"buy:{signal_id}:buy1",
                        "kind": "buy",
                        "amount_usdc": -1,
                        "created_at": 20,
                        "signal_id": signal_id,
                        "trade_id": "buy1",
                    }
                ]
            )
            self.assertEqual(store.load_account_balance()["balance_usdc"], 0.5)
            store.save_follow_snapshot(
                wallet_trade_state={
                    f"esports:{wallet}": {"last_trade_cursor": {"timestamp": 20, "id": "buy1"}, "last_seen_at": 20},
                },
                open_signals=[
                    {
                        "signal_id": signal_id,
                        "wallet": wallet,
                        "category": "esports",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "outcome": "Team A",
                        "status": "open",
                        "created_at": 20,
                        "updated_at": 20,
                        "match_start_time": start.isoformat(),
                        "legs": [
                            {
                                "stake": 1,
                                "funded_stake": 1,
                                "funding_status": "funded",
                                "would_follow": True,
                                "our_entry_price": 0.5,
                                "wallet_fill_price": 0.5,
                                "trade_id": "buy1",
                                "leg_at": 20,
                            }
                        ],
                        "behavior_events": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            class FakeClient:
                def list_events_paginated(self, **kwargs):
                    if kwargs.get("closed") is True:
                        return []
                    return [
                        {
                            "id": "event1",
                            "slug": "",
                            "title": "Dota 2: Team A vs Team B (BO3)",
                            "tags": [{"slug": "dota-2"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Dota 2: Team A vs Team B (BO3)",
                                    "outcomes": ["Team A", "Team B"],
                                    "outcomePrices": ["0.75", "0.25"],
                                    "active": True,
                                    "closed": False,
                                    "volume": 100000,
                                    "startTime": start.isoformat(),
                                }
                            ],
                        }
                    ]

                def trades_for_user(self, _wallet, **_kwargs):
                    return [{"id": "sell1", "timestamp": 30, "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.75, "size": 10}]

            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "follow",
                    "--stake-usdc",
                    "1",
                    "--gamma-pages",
                    "1",
                    "--resolution-gamma-pages",
                    "1",
                    "--user-trades-max-pages",
                    "1",
                    "--max-workers",
                    "1",
                ]
            )

            summary = command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(summary["exited_signal_count"], 1)
            self.assertEqual(summary["balance_ledger_applied_amount_usdc"], 1.5)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 2.0)

            summary_again = command_follow(args, client=FakeClient(), emit=False)
            self.assertEqual(summary_again["balance_ledger_applied_count"], 0)
            self.assertEqual(store.load_account_balance()["balance_usdc"], 2.0)

    def test_dashboard_short_addr_and_session_token(self):
        self.assertEqual(short_addr("0x1234567890abcdef1234567890abcdef12345678"), "0x123...678")
        token = make_session_token("admin", "secret", now=100)

        self.assertEqual(verify_session_token(token, "secret", max_age_seconds=60, now=120), "admin")
        self.assertIsNone(verify_session_token(token, "wrong", max_age_seconds=60, now=120))
        self.assertIsNone(verify_session_token(token, "secret", max_age_seconds=10, now=120))

    def test_dashboard_login_cookie_secure_flag_and_health_waiting(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    cookie_secure=True,
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/health")
                response = conn.getresponse()
                response.read()
                self.assertEqual(response.status, 401)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/login",
                    body='{"username":"admin","password":"pw"}',
                    headers={"Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                cookie = response.getheader("Set-Cookie") or ""
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["ok"])
                self.assertIn("HttpOnly", cookie)
                self.assertIn("SameSite=Lax", cookie)
                self.assertIn("Secure", cookie)
                conn.close()

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/health", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["ok"])
                self.assertFalse(payload["data"]["db_ready"])
                self.assertEqual(payload["data"]["status"], "waiting_for_runner")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_follow_strategy_library_endpoints(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]

                def call(method, path, body=None, cookie=None):
                    conn = http.client.HTTPConnection(host, port, timeout=5)
                    headers = {"Content-Type": "application/json"}
                    if cookie:
                        headers["Cookie"] = cookie
                    conn.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
                    resp = conn.getresponse()
                    raw = resp.read().decode()
                    conn.close()
                    return resp.status, (json.loads(raw) if raw else None), resp.getheader("Set-Cookie") or ""

                _, _, cookie = call("POST", "/api/login", {"username": "admin", "password": "pw"})

                valid = {
                    "configured": True,
                    "stake_sizing": {"mode": "proportional", "ratio_percent": 12, "per_order_cap_enabled": False,
                                     "per_order_cap_usdc": 0, "fixed_usdc": 0, "balance_percent": 0},
                    "prefilters": {"min_target_wallet_order_cash_usdc": 10},
                    "condition_limits": {"order_count_mode": "none", "max_orders": 0, "stake_cap_mode": "none",
                                          "stake_cap_usdc": 0, "stake_cap_balance_percent": 0},
                    "balance": {"required": False, "usable_balance_usdc": 0},
                }

                status, payload, _ = call("GET", "/api/follow-strategies", cookie=cookie)
                self.assertEqual(status, 200)
                self.assertEqual(payload["data"], {"strategies": [], "active_slug": None})

                status, payload, _ = call("POST", "/api/follow-strategies", {"name": "稳健", "strategy": valid}, cookie=cookie)
                self.assertEqual(status, 200)
                self.assertTrue(payload["data"]["active"])
                slug_a = payload["data"]["slug"]

                # the runner-facing strategy endpoint reflects the active strategy
                status, payload, _ = call("GET", "/api/follow-strategy", cookie=cookie)
                self.assertTrue(payload["data"]["configured"])
                self.assertEqual(payload["data"]["stake_sizing"]["ratio_percent"], 12)

                # duplicate name → 409
                status, payload, _ = call("POST", "/api/follow-strategies", {"name": "稳健", "strategy": valid}, cookie=cookie)
                self.assertEqual(status, 409)
                self.assertEqual(payload["error"], "duplicate_name")

                # second strategy, then activate it
                status, payload, _ = call("POST", "/api/follow-strategies", {"name": "激进", "strategy": valid}, cookie=cookie)
                slug_b = payload["data"]["slug"]
                self.assertFalse(payload["data"]["active"])

                status, payload, _ = call("POST", f"/api/follow-strategies/{slug_b}/activate", {}, cookie=cookie)
                self.assertEqual(status, 200)
                self.assertEqual(payload["data"]["active_slug"], slug_b)

                # update a missing slug → 404
                status, payload, _ = call("POST", "/api/follow-strategies/nope/update", {"name": "X", "strategy": valid}, cookie=cookie)
                self.assertEqual(status, 404)

                # delete active (slug_b) — slug_a remains and is auto-promoted
                status, payload, _ = call("POST", f"/api/follow-strategies/{slug_b}/delete", {}, cookie=cookie)
                self.assertEqual(status, 200)
                self.assertEqual(payload["data"]["active_slug"], slug_a)
                self.assertEqual(len(payload["data"]["strategies"]), 1)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_serves_bundled_index_html(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/")
                response = conn.getresponse()
                body = response.read().decode()
                self.assertEqual(response.status, 200)
                self.assertIn("text/html", response.getheader("Content-Type") or "")
                self.assertIn("Polymarket Sniper", body)
                self.assertIn("/app.jsx", body)
                self.assertIn("/ds/_ds_bundle.js", body)
                self.assertIn("/vendor/react-18.3.1.production.min.js", body)
                self.assertNotIn("vue-3", body)
                self.assertNotIn("cdn.jsdelivr.net", body)
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_refresh_post_requires_auth(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("POST", "/api/wallet-refresh")
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 401)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "unauthorized")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_favorites_post_requires_auth(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-favorites",
                    body=json.dumps({"wallet": "0xabc", "category": "esports", "favorite": True}),
                    headers={"Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 401)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "unauthorized")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_quarantine_post_requires_auth(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-quarantine",
                    body=json.dumps({"wallet": "0xabc", "category": "esports"}),
                    headers={"Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 401)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "unauthorized")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_favorites_post_rejects_quarantined_wallet(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "category": "esports", "grade": "A", "positive_market_rate": 1.0}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine(wallet, reason="recent_chop_loss", ts=100, category="esports")
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-favorites",
                    body=json.dumps({"wallet": wallet, "category": "esports", "favorite": True}),
                    headers={"Content-Type": "application/json", "Cookie": cookie},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 409)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "wallet_quarantined")
                self.assertEqual(store.load_wallet_favorites(category="esports"), {})
                self.assertIn(wallet, store.load_wallet_quarantine(category="esports"))

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/wallets", headers={"Cookie": cookie})
                response = conn.getresponse()
                wallets = json.loads(response.read().decode())["data"]
                conn.close()
                row = wallets["wallets"][0]
                self.assertFalse(row["favorite"])
                self.assertTrue(row["quarantined"])
                self.assertEqual(wallets["favorite_count"], 0)
                self.assertEqual(wallets["active_count"], 0)
                self.assertEqual(wallets["quarantined_count"], 1)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_favorites_form_false_removes_favorite(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "category": "esports", "grade": "A", "positive_market_rate": 1.0}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_favorite(wallet, category="esports", favorite=True, ts=100, snapshot={"wallet": wallet})
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                body = urllib.parse.urlencode({"wallet": wallet, "category": "esports", "favorite": "false"})
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-favorites",
                    body=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded", "Cookie": cookie},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 200)
                self.assertTrue(payload["ok"])
                self.assertFalse(payload["data"]["favorite"])
                self.assertEqual(store.load_wallet_favorites(category="esports"), {})
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_quarantine_post_quarantines_and_clears_favorite(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": wallet, "category": "esports", "grade": "A", "positive_market_rate": 1.0}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_favorite(wallet, category="esports", favorite=True, ts=100, snapshot={"wallet": wallet})
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-quarantine",
                    body=json.dumps({"wallet": wallet, "category": "esports"}),
                    headers={"Content-Type": "application/json", "Cookie": cookie},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 200)
                self.assertTrue(payload["ok"])
                self.assertTrue(payload["data"]["quarantined"])
                self.assertEqual(payload["data"]["reason"], "manual_dashboard_quarantine")
                self.assertEqual(store.load_wallet_favorites(category="esports"), {})
                quarantine = store.load_wallet_quarantine(category="esports")
                self.assertIn(wallet, quarantine)
                self.assertEqual(quarantine[wallet]["reason"], "manual_dashboard_quarantine")

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/wallets", headers={"Cookie": cookie})
                response = conn.getresponse()
                wallets = json.loads(response.read().decode())["data"]
                conn.close()
                row = wallets["wallets"][0]
                self.assertFalse(row["favorite"])
                self.assertTrue(row["quarantined"])
                self.assertEqual(wallets["active_count"], 0)
                self.assertEqual(wallets["favorite_count"], 0)
                self.assertEqual(wallets["quarantined_count"], 1)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_quarantine_post_unquarantines_without_restoring_missing_wallet(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine(wallet, reason="manual_dashboard_quarantine", ts=100, category="esports")
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/wallet-quarantine",
                    body=json.dumps({"wallet": wallet, "category": "esports", "quarantined": False}),
                    headers={"Content-Type": "application/json", "Cookie": cookie},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 200)
                self.assertTrue(payload["ok"])
                self.assertFalse(payload["data"]["quarantined"])
                self.assertEqual(store.load_wallet_quarantine(category="esports"), {})

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/wallets", headers={"Cookie": cookie})
                response = conn.getresponse()
                wallets = json.loads(response.read().decode())["data"]
                conn.close()
                self.assertEqual(wallets["wallets"], [])
                self.assertEqual(wallets["active_count"], 0)
                self.assertEqual(wallets["quarantined_count"], 0)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_stream_requires_auth(self):
        with TemporaryDirectory() as tmp:
            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/stream")
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 401)
                self.assertEqual(payload["error"], "unauthorized")
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_stream_sends_initial_frame_and_releases_slot(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    stream_poll_seconds=0.05,
                    stream_heartbeat_seconds=1,
                    max_stream_clients=1,
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            conn = None
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/stream", headers={"Cookie": cookie})
                response = conn.getresponse()
                self.assertEqual(response.status, 200)
                line = response.fp.readline().decode()
                self.assertTrue(line.startswith("data: "))
                payload = json.loads(line.removeprefix("data: "))
                self.assertIn("health", payload)
                self.assertIn("overview", payload)
                self.assertIn("runner", payload)
                self.assertIn("refresh", payload)
                self.assertTrue(payload["follows_dirty"])
                self.assertTrue(payload["events_dirty"])
                self.assertTrue(payload["wallets_dirty"])

                blocked = http.client.HTTPConnection(host, port, timeout=5)
                blocked.request("GET", "/api/stream", headers={"Cookie": cookie})
                blocked_response = blocked.getresponse()
                blocked_payload = json.loads(blocked_response.read().decode())
                self.assertEqual(blocked_response.status, 503)
                self.assertEqual(blocked_payload["error"], "too_many_stream_clients")
                blocked.close()

                response.close()
                conn.close()
                conn = None
                deadline = time.time() + 5
                while getattr(server, "active_stream_clients", 0) != 0 and time.time() < deadline:
                    time.sleep(0.01)
                self.assertEqual(getattr(server, "active_stream_clients", 0), 0)
            finally:
                if conn is not None:
                    conn.close()
                server.shutdown()
                server.server_close()

    def test_dashboard_api_uses_configured_follow_dir(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            follow_dir = Path(tmp) / "custom-follow"
            FollowStore(follow_dir / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-custom",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "open",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    follow_dir=follow_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/overview", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["data"]["open_signal_count"], 1)

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/follows", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["data"]["total"], 1)
            finally:
                server.shutdown()
                server.server_close()

    def test_stream_dirty_flags_and_snapshot_meta(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            before = read_stream_signal(data_dir)
            self.assertEqual(before.snapshot_updated_at, 0)

            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )
            after = read_stream_signal(data_dir)
            self.assertGreater(after.snapshot_updated_at, 0)
            flags = stream_dirty_flags(before, after)
            self.assertTrue(flags["follows_dirty"])
            self.assertTrue(flags["events_dirty"])
            self.assertTrue(flags["wallets_dirty"])

            leader_before = StreamSignal(
                snapshot_updated_at=after.snapshot_updated_at,
                run_log_mtime=after.run_log_mtime,
                control_mtime=after.control_mtime,
                leaderboard_mtime=1,
            )
            leader_after = StreamSignal(
                snapshot_updated_at=after.snapshot_updated_at,
                run_log_mtime=after.run_log_mtime,
                control_mtime=after.control_mtime,
                leaderboard_mtime=2,
            )
            flags = stream_dirty_flags(leader_before, leader_after)
            self.assertFalse(flags["follows_dirty"])
            self.assertFalse(flags["events_dirty"])
            self.assertTrue(flags["wallets_dirty"])

    def test_dashboard_runner_detects_external_matching_process(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = DashboardConfig(
                data_dir=data_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 1234,
                        "ppid": 1,
                        "pgid": 1234,
                        "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                    }
                ],
            )

            status = build_runner_status(config)

            self.assertEqual(status["status"], "running")
            self.assertEqual(status["source"], "external")
            self.assertEqual(status["pid"], 1234)

    def test_dashboard_runner_detects_script_path_process(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = DashboardConfig(
                data_dir=data_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 2234,
                        "ppid": 1,
                        "pgid": 2234,
                        "command": f"{sys.executable} /repo/poly_fight/cli.py --data-dir={data_dir} run --stake-usdc 1",
                    }
                ],
            )

            status = build_runner_status(config)

            self.assertEqual(status["status"], "running")
            self.assertEqual(status["source"], "external")
            self.assertEqual(status["pid"], 2234)

    def test_dashboard_runner_detects_default_data_dir_process(self):
        config = DashboardConfig(
            data_dir=Path("data"),
            username="admin",
            password="pw",
            cookie_secret="secret",
            runner_process_lister=lambda: [
                {
                    "pid": 3234,
                    "ppid": 1,
                    "pgid": 3234,
                    "command": f"{sys.executable} -m poly_fight.cli run --stake-usdc 1",
                }
            ],
        )

        status = build_runner_status(config)

        self.assertEqual(status["status"], "running")
        self.assertEqual(status["source"], "external")
        self.assertEqual(status["pid"], 3234)

    def test_dashboard_runner_ignores_zombie_process_and_marks_stopped(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            write_follow_control(
                follow_dir,
                {
                    "runner": {
                        "status": "stopping",
                        "pid": 777,
                        "pgid": 777,
                        "source": "dashboard",
                        "started_at": 100,
                    }
                },
            )
            config = DashboardConfig(
                data_dir=data_dir,
                follow_dir=follow_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 777,
                        "ppid": 1,
                        "pgid": 777,
                        "stat": "Z",
                        "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                    }
                ],
            )

            status = build_runner_status(config)

            self.assertEqual(status["status"], "stopped")
            self.assertEqual(read_follow_control(follow_dir)["runner"]["status"], "stopped")

    def test_dashboard_runner_stale_stopping_control_is_persisted_as_stopped(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            write_follow_control(
                follow_dir,
                {
                    "runner": {
                        "status": "stopping",
                        "pid": 777,
                        "pgid": 777,
                        "source": "dashboard",
                        "started_at": 100,
                    }
                },
            )
            config = DashboardConfig(
                data_dir=data_dir,
                follow_dir=follow_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [],
            )

            status = build_runner_status(config)

            self.assertEqual(status["status"], "stopped")
            self.assertEqual(read_follow_control(follow_dir)["runner"]["status"], "stopped")

    def test_dashboard_runner_preserves_stopping_status_while_process_exits(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            write_follow_control(
                follow_dir,
                {
                    "runner": {
                        "status": "stopping",
                        "pid": 777,
                        "pgid": 777,
                        "source": "dashboard",
                        "started_at": 100,
                        "stop_requested_at": 120,
                    }
                },
            )
            config = DashboardConfig(
                data_dir=data_dir,
                follow_dir=follow_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 777,
                        "ppid": 1,
                        "pgid": 777,
                        "stat": "S",
                        "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                    }
                ],
            )

            status = build_runner_status(config)

            self.assertEqual("stopping", status["status"])
            self.assertEqual(777, status["pid"])
            self.assertEqual(120, status["stop_requested_at"])

    def test_dashboard_runner_stopped_status_includes_default_runner_inputs(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = DashboardConfig(
                data_dir=data_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [],
                runner_stake_usdc=2.0,
                runner_stake_ratio_percent=12.5,
                runner_max_stake_usdc=7.0,
                runner_max_signal_stake_balance_percent=15.0,
            )

            status = build_runner_status(config)

            self.assertEqual(status["status"], "stopped")
            self.assertEqual(status["stake_usdc"], 2.0)
            self.assertEqual(status["stake_ratio_percent"], 12.5)
            self.assertEqual(status["max_stake_usdc"], 7.0)
            self.assertEqual(status["max_signal_stake_balance_percent"], 15.0)

    def test_dashboard_runner_start_writes_control_and_blocks_duplicate(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            config = DashboardConfig(
                data_dir=data_dir,
                follow_dir=follow_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [],
                runner_process_starter=fake_starter,
            )
            FollowStore(follow_dir / "follow.db").save_follow_strategy(default_follow_strategy(balance_usdc=100), ts=100)

            status = start_runner(config)

            self.assertEqual(status["status"], "running")
            self.assertEqual(status["pid"], 4321)
            self.assertIn("run", calls[0][0])
            self.assertNotIn("--stake-usdc", calls[0][0])
            self.assertIn("--strategy-source", calls[0][0])
            self.assertEqual(calls[0][0][calls[0][0].index("--strategy-source") + 1], "db")
            self.assertIn("--log-dir", calls[0][0])
            self.assertIn("--follow-dir", calls[0][0])
            self.assertIn(str(follow_dir), calls[0][0])
            self.assertIn("--skip-initial-build", calls[0][0])
            self.assertEqual(calls[0][1].parent, data_dir / "logs" / "follow")
            self.assertEqual(read_follow_control(follow_dir)["runner"]["pid"], 4321)
            self.assertTrue(status["strategy_configured"])

    def test_build_position_backfill_trades_gates(self):
        from poly_fight import cli as cli_mod
        market = {
            "condition_id": "0xabc", "conditionId": "0xabc",
            "outcomes": ["Team A", "Team B"], "clobTokenIds": ["111", "222"],
            "outcome_prices": [0.0, 0.0], "market_type": "main_match", "league": "cs2",
        }
        markets = {"0xabc": market}
        positions_by_wallet = {
            # 现价 0.55 < 0.9 → 候选(价格决策交给下游策略 edge 闸,补单不再自带成本闸)
            "0xw1": [{"conditionId": "0xabc", "asset": "111", "outcome": "Team A", "avgPrice": 0.50, "curPrice": 0.55, "size": 1000}],
            # 现价 0.70 < 0.9 → 也成候选(cost_ratio_cap 1.15 已删,不再因"比钱包成本高15%"被挡)
            "0xw2": [{"conditionId": "0xabc", "asset": "222", "outcome": "Team B", "avgPrice": 0.50, "curPrice": 0.70, "size": 1000}],
            # 现价 0.92 > max_entry 0.9 → price_ceiling_blocked(唯一保留的补单价格上限,与 live 同)
            "0xw3": [{"conditionId": "0xabc", "asset": "111", "outcome": "Team A", "avgPrice": 0.88, "curPrice": 0.92, "size": 1000}],
            # 不在 watch scope
            "0xw4": [{"conditionId": "0xdef", "asset": "999", "outcome": "X", "avgPrice": 0.40, "curPrice": 0.40, "size": 1000}],
        }

        class FakeClient:
            def positions(self, wallet, *, limit=200):
                return positions_by_wallet[wallet]

        follow_wallets = [{"wallet": w} for w in ("0xw1", "0xw2", "0xw3", "0xw4")]
        with patch("poly_fight.cli.clob_price", return_value=None):  # 退化用 pos.curPrice
            by_wallet, stats = cli_mod.build_position_backfill_trades(
                FakeClient(), follow_wallets, markets, max_entry_price=0.9, now_ts=1000,
            )
        self.assertEqual(set(by_wallet), {"0xw1", "0xw2"})
        trade = by_wallet["0xw1"][0]
        self.assertEqual(trade["side"], "BUY")
        self.assertEqual(trade["outcomeIndex"], 0)
        self.assertEqual(trade["price"], 0.5)                  # 钱包成本
        self.assertEqual(trade["source"], "position_backfill")
        self.assertEqual(market["outcome_prices"][0], 0.55)    # 我们现价快照
        self.assertEqual(stats["candidates"], 2)
        self.assertNotIn("cost_gate_blocked", stats)           # 闸已删
        self.assertEqual(stats["price_ceiling_blocked"], 1)

    def test_build_position_backfill_trades_idempotent(self):
        # 已有该 wallet+condition+outcome 的开放信号 → 不再补腿(防重启重复扣)。
        from poly_fight import cli as cli_mod
        from poly_fight.follow import follow_signal_id
        market = {
            "condition_id": "0xabc", "conditionId": "0xabc",
            "outcomes": ["Team A", "Team B"], "clobTokenIds": ["111", "222"],
            "outcome_prices": [0.0, 0.0], "market_type": "main_match", "league": "cs2",
        }

        class FakeClient:
            def positions(self, wallet, *, limit=200):
                return [{"conditionId": "0xabc", "asset": "111", "outcome": "Team A", "avgPrice": 0.50, "curPrice": 0.55, "size": 1000}]

        sid = follow_signal_id("0xw1", "0xabc", 0)
        with patch("poly_fight.cli.clob_price", return_value=None):
            by_wallet, stats = cli_mod.build_position_backfill_trades(
                FakeClient(), [{"wallet": "0xw1"}], {"0xabc": market},
                max_entry_price=0.9, now_ts=1000, existing_signal_ids={sid},
            )
        self.assertEqual(by_wallet, {})
        self.assertEqual(stats["already_followed"], 1)
        self.assertEqual(stats["candidates"], 0)

    def test_set_pause_stamps_owner_pid(self):
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            set_pause_new_signals(follow_dir, "esports", {"status": "paused", "reason": "wallet_refresh", "started_at": 100})
            entry = read_follow_control(follow_dir)["pause_new_signals"]["esports"]
            self.assertEqual(entry["owner_pid"], os.getpid())
            self.assertEqual(entry["reason"], "wallet_refresh")

    def test_reconcile_clears_orphaned_pause_dead_owner(self):
        # 属主进程已死(dashboard 重启 / collect 被杀)→ pause 视为孤儿被清除。
        dead = subprocess.Popen([sys.executable, "-c", "pass"])
        dead.wait()
        dead_pid = dead.pid
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            write_follow_control(follow_dir, {"pause_new_signals": {"esports": {
                "status": "paused", "reason": "wallet_refresh", "started_at": 100, "owner_pid": dead_pid, "category": "esports"}}})
            survivors = reconcile_pause_new_signals(follow_dir, now_ts=200)
            self.assertEqual(survivors, {})
            self.assertNotIn("pause_new_signals", read_follow_control(follow_dir))

    def test_reconcile_keeps_pause_live_owner(self):
        # 属主进程仍存活 → pause 保留(不会误清掉正在跑的刷新)。
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            set_pause_new_signals(follow_dir, "esports", {"status": "paused", "reason": "pool_refresh", "started_at": 100})
            survivors = reconcile_pause_new_signals(follow_dir, now_ts=200)
            self.assertIn("esports", survivors)
            self.assertIn("esports", read_follow_control(follow_dir)["pause_new_signals"])

    def test_reconcile_legacy_pause_ttl(self):
        # 旧格式(无 owner_pid)退化为 TTL 自愈:超时清除,未超时保留。
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            write_follow_control(follow_dir, {"pause_new_signals": {"esports": {
                "status": "paused", "reason": "wallet_refresh", "started_at": 100, "category": "esports"}}})
            self.assertEqual(reconcile_pause_new_signals(follow_dir, now_ts=100 + 31 * 60), {})
            write_follow_control(follow_dir, {"pause_new_signals": {"esports": {
                "status": "paused", "reason": "wallet_refresh", "started_at": 100, "category": "esports"}}})
            self.assertIn("esports", reconcile_pause_new_signals(follow_dir, now_ts=100 + 5 * 60))

    def test_reconcile_wallet_refresh_dead_owner_marks_failed(self):
        # 采集 serve 被杀 → running 永久残留、按钮永久变灰。属主已死 → 读时自愈为 failed。
        dead = subprocess.Popen([sys.executable, "-c", "pass"])
        dead.wait()
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            write_follow_control(follow_dir, {"wallet_refresh": {"esports": {
                "status": "running", "category": "esports", "started_at": 100, "owner_pid": dead.pid}}})
            healed = reconcile_wallet_refresh_status(follow_dir, now_ts=200)
            self.assertEqual(healed["esports"]["status"], "failed")
            self.assertTrue(healed["esports"]["stale"])
            self.assertEqual(read_follow_control(follow_dir)["wallet_refresh"]["esports"]["status"], "failed")

    def test_reconcile_wallet_refresh_live_owner_kept(self):
        # 属主(serve)仍存活 → 正在跑的采集不被误判。
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            write_follow_control(follow_dir, {"wallet_refresh": {"esports": {
                "status": "running", "category": "esports", "started_at": 100, "owner_pid": os.getpid()}}})
            healed = reconcile_wallet_refresh_status(follow_dir, now_ts=200)
            self.assertEqual(healed["esports"]["status"], "running")

    def test_reconcile_wallet_refresh_legacy_ttl(self):
        # 旧格式(无 owner_pid,如历史卡死的那条)退化为 TTL 自愈:超时判 failed,未超时保留。
        with TemporaryDirectory() as tmp:
            follow_dir = Path(tmp) / "follow"
            base = {"wallet_refresh": {"esports": {"status": "running", "category": "esports", "started_at": 100}}}
            write_follow_control(follow_dir, base)
            self.assertEqual(reconcile_wallet_refresh_status(follow_dir, now_ts=100 + 31 * 60)["esports"]["status"], "failed")
            write_follow_control(follow_dir, base)
            self.assertEqual(reconcile_wallet_refresh_status(follow_dir, now_ts=100 + 5 * 60)["esports"]["status"], "running")

    def test_dashboard_runner_start_realtime_refresh_spawns_observe(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append(command)
                return FakeProcess()

            config = DashboardConfig(
                data_dir=data_dir, follow_dir=follow_dir, username="admin",
                password="pw", cookie_secret="secret",
                runner_process_lister=lambda: [],
                runner_process_starter=fake_starter,
            )
            store = FollowStore(follow_dir / "follow.db")
            strat_off = default_follow_strategy(balance_usdc=100)
            strat_off["realtime_refresh"] = False
            store.save_follow_strategy(strat_off, ts=100)

            # 策略未勾实时刷新 → 只起 runner,无 observe
            off = start_runner(config)
            self.assertEqual(len(calls), 1)
            self.assertFalse(off["realtime_refresh"])
            self.assertIsNone(off["observe_pid"])

            # lister 返回 [] → 状态判定永远非 running,可直接再次 start(无需真停进程)
            calls.clear()

            # 即使上次采集命令里残留旧阈值 flag,observe-v2 也不再继承它们
            # (门槛全在 core.py 评分常量,collect/observe 天然一致;--v2-* 已从 CLI 删除)。
            write_follow_control(follow_dir, {"wallet_refresh": {"esports": {"command": [
                "python", "-m", "poly_fight.cli", "collect-v2",
                "--v2-min-positive-rate", "0.7500", "--v2-max-median-entry", "0.7500",
            ]}}})

            # 策略勾上实时刷新 → runner + observe-v2 + observe-live 三个进程
            strat_on = default_follow_strategy(balance_usdc=100)
            strat_on["realtime_refresh"] = True
            store.save_follow_strategy(strat_on, ts=200)
            on = start_runner(config)
            self.assertEqual(len(calls), 3)
            observe_cmd = calls[1]
            self.assertIn("observe-v2", observe_cmd)
            self.assertEqual(observe_cmd[observe_cmd.index("--loop-hours") + 1], "2")
            self.assertEqual(observe_cmd[observe_cmd.index("--observe-lookback-hours") + 1], "4")
            self.assertIn(str(data_dir / "esports"), observe_cmd)   # 与 collect-v2 同一 data 目录
            # 刚采集完先睡满一轮再跑
            self.assertIn("--defer-first-tick", observe_cmd)
            # 不再继承已删除的阈值 flag(传入会让 observe-v2 报 unrecognized arguments)
            self.assertNotIn("--v2-min-positive-rate", observe_cmd)
            self.assertNotIn("--v2-max-median-entry", observe_cmd)
            self.assertTrue(on["realtime_refresh"])
            self.assertEqual(on["observe_pid"], 4321)
            # observe-live(3.1):分钟级快循环,同一 esports data 目录,发布同一 leaderboard_v2.db
            observe_live_cmd = calls[2]
            self.assertIn("observe-live", observe_live_cmd)
            self.assertEqual(observe_live_cmd[observe_live_cmd.index("--loop-minutes") + 1], "10")
            self.assertIn(str(data_dir / "esports"), observe_live_cmd)
            self.assertIn("--defer-first-tick", observe_live_cmd)
            self.assertEqual(on["observe_live_pid"], 4321)

    def test_dashboard_runner_start_requires_saved_follow_strategy(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            with self.assertRaisesRegex(ValueError, "follow_strategy_required"):
                start_runner(
                    DashboardConfig(
                        data_dir=data_dir,
                        follow_dir=follow_dir,
                        username="admin",
                        password="pw",
                        cookie_secret="secret",
                        runner_process_lister=lambda: [],
                        runner_process_starter=fake_starter,
                    )
                )
            self.assertEqual(calls, [])

    def test_dashboard_runner_start_records_strategy_summary(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            strategy = default_follow_strategy(balance_usdc=250)
            strategy["stake_sizing"]["mode"] = "fixed"
            strategy["stake_sizing"]["fixed_usdc"] = 25
            FollowStore(follow_dir / "follow.db").save_follow_strategy(strategy, ts=100)

            status = start_runner(
                DashboardConfig(
                    data_dir=data_dir,
                    follow_dir=follow_dir,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [],
                    runner_process_starter=fake_starter,
                )
            )

            self.assertIn("--strategy-source", calls[0][0])
            self.assertEqual(status["strategy_summary"], "固定 25 USDC，现价上限 0.85，可用余额 250")
            self.assertEqual(read_follow_control(follow_dir)["runner"]["strategy_summary"], "固定 25 USDC，现价上限 0.85，可用余额 250")

    def test_dashboard_runner_start_allows_strategy_without_balance_limit(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            strategy = default_follow_strategy()
            strategy["stake_sizing"]["mode"] = "fixed"
            strategy["stake_sizing"]["fixed_usdc"] = 25
            FollowStore(follow_dir / "follow.db").save_follow_strategy(strategy, ts=100)

            status = start_runner(
                DashboardConfig(
                    data_dir=data_dir,
                    follow_dir=follow_dir,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [],
                    runner_process_starter=fake_starter,
                )
            )

            self.assertEqual(status["status"], "running")
            self.assertIn("--strategy-source", calls[0][0])
            self.assertEqual(FollowStore(follow_dir / "follow.db").load_account_balance()["configured"], False)

    def test_dashboard_runner_start_ignores_legacy_stake_overrides(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            follow_dir = data_dir / "follow"
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            FollowStore(follow_dir / "follow.db").save_follow_strategy(default_follow_strategy(balance_usdc=100), ts=100)
            status = start_runner(
                DashboardConfig(
                    data_dir=data_dir,
                    follow_dir=follow_dir,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [],
                    runner_process_starter=fake_starter,
                ),
                stake_ratio_percent=5,
                max_stake_usdc=25,
            )

            self.assertNotIn("--max-stake-usdc", calls[0][0])
            self.assertNotIn("--stake-ratio-percent", calls[0][0])
            self.assertEqual(status["max_stake_usdc"], 0.0)
            self.assertEqual(status["stake_ratio_percent"], 10.0)

    def test_prepare_category_refresh_dir_preserves_and_prunes_caches(self):
        with TemporaryDirectory() as tmp:
            category_dir = Path(tmp) / "sports"
            output_file = category_dir / "smart_wallet_leaderboard.json"
            profile_file = category_dir / "wallet_profiles.json"
            classification_file = category_dir / "esports_classification_set.json"
            fresh_cache = category_dir / "raw_market_trades" / "fresh.json"
            old_cache = category_dir / "raw_user_trades" / "old.json"
            old_nested_cache = category_dir / "clob_market_metadata" / "nested" / "old.json"
            for path in [
                output_file,
                profile_file,
                classification_file,
                fresh_cache,
                old_cache,
                old_nested_cache,
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(path.name, encoding="utf-8")
            now_ts = 2_000_000
            old_mtime = now_ts - 40 * 86400
            fresh_mtime = now_ts - 5 * 86400
            for path in [old_cache, old_nested_cache]:
                os.utime(path, (old_mtime, old_mtime))
            os.utime(fresh_cache, (fresh_mtime, fresh_mtime))

            prepare_category_refresh_dir(category_dir, max_lookback_days=14, now_ts=now_ts)

            self.assertFalse(output_file.exists())
            self.assertFalse(profile_file.exists())
            self.assertTrue(classification_file.exists())
            self.assertTrue(fresh_cache.exists())
            self.assertFalse(old_cache.exists())
            self.assertFalse(old_nested_cache.exists())

    def test_dashboard_runner_stop_allows_external_process(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            stopped = []
            config = DashboardConfig(
                data_dir=data_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 777,
                        "ppid": 1,
                        "pgid": 777,
                        "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                    }
                ],
                runner_process_stopper=lambda status: stopped.append(status),
            )

            status = stop_runner(config)

            self.assertEqual(status["status"], "stopping")
            self.assertEqual(status["source"], "external")
            self.assertEqual(stopped[0]["pid"], 777)

    def test_dashboard_reset_data_clears_generated_dirs(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            follow_dir = data_dir / "follow"
            log_dir = data_dir / "logs"
            for path in [
                data_dir / "esports" / "smart_wallet_leaderboard.json",
                follow_dir / "follow.db",
                log_dir / "follow" / "dashboard-runner.out",
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("old", encoding="utf-8")
            config = DashboardConfig(
                data_dir=data_dir,
                follow_dir=follow_dir,
                log_dir=log_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [],
            )

            result = reset_dashboard_data(config)

            self.assertEqual(result["status"], "reset")
            self.assertTrue((data_dir / "esports").is_dir())
            self.assertTrue(follow_dir.is_dir())
            self.assertTrue((log_dir / "follow").is_dir())
            self.assertEqual(list((data_dir / "esports").iterdir()), [])
            self.assertEqual(list(follow_dir.iterdir()), [])
            self.assertEqual(list((log_dir / "follow").iterdir()), [])

    def test_prune_old_logs_deletes_only_aged_spawn_logs(self):
        with TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            old_run = log_dir / "dashboard-runner-1000.out"
            old_observe = log_dir / "dashboard-observe-1000.out"
            fresh = log_dir / "dashboard-runner-2000.out"
            other = log_dir / "keep-me.txt"  # 非 dashboard-*.out,不该被动
            for p in (old_run, old_observe, fresh, other):
                p.write_text("x", encoding="utf-8")
            stale = time.time() - 8 * 86400  # 8 天前 > 7 天阈值
            os.utime(old_run, (stale, stale))
            os.utime(old_observe, (stale, stale))

            dashboard_module._prune_old_logs(log_dir, max_age_days=7)

            self.assertFalse(old_run.exists())
            self.assertFalse(old_observe.exists())
            self.assertTrue(fresh.exists())  # 新文件保留
            self.assertTrue(other.exists())  # 非匹配 glob 保留

    def test_wipe_collector_data_clears_category_keeps_sibling_follow(self):
        # 完整重采:清空类目采集目录(profiles/db/交易缓存),但 follow.db(独立 follow 目录)保留。
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            cat = root / "esports"
            (cat / "collector_v2").mkdir(parents=True)
            (cat / "leaderboard_v2.db").write_text("db")
            (cat / "wallet_profiles_v2.json").write_text("[]")
            (cat / "collector_v2" / "trades.json").write_text("{}")
            follow = root / "follow"
            follow.mkdir()
            (follow / "follow.db").write_text("paper-history")

            _wipe_collector_data(cat)

            self.assertTrue(cat.is_dir())
            self.assertEqual(list(cat.iterdir()), [])          # 采集库清空
            self.assertTrue((follow / "follow.db").exists())   # follow.db 保留
            self.assertEqual((follow / "follow.db").read_text(), "paper-history")

    def test_dashboard_reset_data_blocks_running_runner(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = DashboardConfig(
                data_dir=data_dir,
                username="admin",
                password="pw",
                cookie_secret="secret",
                runner_process_lister=lambda: [
                    {
                        "pid": 778,
                        "ppid": 1,
                        "pgid": 778,
                        "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                    }
                ],
            )

            with self.assertRaisesRegex(RuntimeError, "runner_running"):
                reset_dashboard_data(config)

    def test_dashboard_runner_api_auth_and_conflict(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [
                        {
                            "pid": 888,
                            "ppid": 1,
                            "pgid": 888,
                            "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                        }
                    ],
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("POST", "/api/runner/start")
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 401)
                self.assertEqual(payload["error"], "unauthorized")
                conn.close()

                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("POST", "/api/runner/start", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 409)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "runner_already_running")
                self.assertEqual(payload["data"]["pid"], 888)
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_runner_start_api_requires_saved_follow_strategy(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [],
                    runner_process_starter=fake_starter,
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("POST", "/api/runner/start", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 400)
                self.assertEqual(payload["error"], "follow_strategy_required")
                self.assertEqual(calls, [])

                strategy = default_follow_strategy(balance_usdc=100)
                strategy["stake_sizing"]["mode"] = "fixed"
                strategy["stake_sizing"]["fixed_usdc"] = 10
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/follow-strategy",
                    body=json.dumps(strategy),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["data"]["configured"])

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/runner/start",
                    body=json.dumps({"stake_ratio_percent": 5}),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 202)
                self.assertTrue(payload["data"]["strategy_configured"])
                self.assertIn("--strategy-source", calls[0][0])
                self.assertNotIn("--stake-ratio-percent", calls[0][0])
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_account_balance_api_requires_auth_and_updates_overview(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/account-balance",
                    body=json.dumps({"balance_usdc": 123.45}),
                    headers={"Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 401)
                self.assertEqual(payload["error"], "unauthorized")

                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/account-balance",
                    body=json.dumps({"balance_usdc": 123.45}),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["data"]["balance_usdc"], 123.45)

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/overview", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertEqual(payload["data"]["account_balance"]["balance_usdc"], 123.45)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_follow_strategy_api_requires_auth_and_persists(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [],
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/follow-strategy")
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 401)
                self.assertEqual(payload["error"], "unauthorized")

                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/follow-strategy", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertFalse(payload["data"]["configured"])

                strategy = default_follow_strategy(balance_usdc=321)
                strategy["stake_sizing"]["mode"] = "fixed"
                strategy["stake_sizing"]["fixed_usdc"] = 12
                strategy["prefilters"]["min_target_wallet_order_cash_usdc"] = 8
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/follow-strategy",
                    body=json.dumps(strategy),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertTrue(payload["data"]["configured"])
                self.assertEqual(payload["data"]["stake_sizing"]["fixed_usdc"], 12)

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/overview", headers={"Cookie": cookie})
                response = conn.getresponse()
                overview = json.loads(response.read().decode())["data"]
                conn.close()
                self.assertEqual(response.status, 200)
                self.assertEqual(overview["account_balance"]["balance_usdc"], 321)
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_account_balance_api_locks_while_runner_running(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    runner_process_lister=lambda: [
                        {
                            "pid": 9234,
                            "ppid": 1,
                            "pgid": 9234,
                            "command": f"{sys.executable} -m poly_fight.cli --data-dir {data_dir} run --stake-usdc 1",
                        }
                    ],
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/account-balance",
                    body=json.dumps({"balance_usdc": 123.45}),
                    headers={"Cookie": f"poly_fight_session={token}", "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 409)
                self.assertEqual(payload["error"], "account_balance_locked")
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_refresh_api_runs_refresh_without_pausing_follow(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ran = threading.Event()

            def fake_runner(category, data_dir_arg, log_path):
                self.assertEqual(category, "esports")
                self.assertEqual(data_dir_arg, data_dir / "esports")
                log_path.write_text("refreshed", encoding="utf-8")
                ran.set()
                return 0

            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    wallet_refresh_runner=fake_runner,
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                cookie = f"poly_fight_session={token}"
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("POST", "/api/wallet-refresh?category=esports", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 202)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["data"]["status"], "running")
                self.assertTrue(ran.wait(2))

                status = build_wallet_refresh_status(data_dir)
                deadline = time.time() + 2
                while status["status"].get("esports", {}).get("status") == "running" and time.time() < deadline:
                    time.sleep(0.01)
                    status = build_wallet_refresh_status(data_dir)
                command = status["status"]["esports"]["command"]
                self.assertIn("--refresh-classification", command)
                self.assertEqual(status["status"]["esports"]["status"], "succeeded")
                self.assertEqual(status["status"]["esports"]["returncode"], 0)
                self.assertEqual(read_follow_control(data_dir)["wallet_refresh"]["esports"]["status"], "succeeded")
                self.assertNotIn("pause_follow", read_follow_control(data_dir))
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_overview_uses_existing_follow_pnl_fields(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.set_account_balance(97, ts=100, source="manual")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m2",
                        "status": "open",
                        "legs": [{"stake": 3, "would_follow": True}],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "sig1",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "outcome_won": True,
                        "our_paper_pnl": 0.8,
                        "wallet_paper_pnl_by_wallet": {"0xabc": 1.0},
                        "legs": [{"stake": 1, "would_follow": True}],
                        "settled_at": 100,
                    }
                ],
                performance={"wallets": {}, "total": {}},
            )

            overview = build_overview(data_dir)

            self.assertEqual(overview["settled_count"], 1)
            self.assertEqual(overview["win_rate"], 1.0)
            self.assertEqual(overview["our_realized_pnl"], 0.8)
            self.assertEqual(overview["wallet_basis_realized_pnl"], 1.0)
            self.assertEqual(overview["total_stake"], 4.0)
            self.assertEqual(overview["resolved_stake"], 1.0)
            self.assertEqual(overview["open_exposure"], 3.0)
            self.assertEqual(overview["account_total_equity_usdc"], 100.0)
            self.assertEqual(overview["realized_roi"], 0.8)
            self.assertEqual(overview["wallet_basis_realized_roi"], 1.0)
            self.assertAlmostEqual(overview["delay_cost"], 0.2)

    def test_dashboard_overview_exposes_full_app_esports_aggregates(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "open-cs2",
                        "wallet": "0xabc",
                        "condition_id": "m-open-cs2",
                        "status": "open",
                        "category": "esports",
                        "game_family": "cs2",
                        "market_type": "main_match",
                        "legs": [{"stake": 2, "would_follow": True}],
                    },
                    {
                        "signal_id": "open-dota",
                        "wallet": "0xabc",
                        "condition_id": "m-open-dota",
                        "status": "open",
                        "category": "esports",
                        "game_family": "dota2",
                        "market_type": "game_winner",
                        "legs": [{"stake": 3, "would_follow": True}],
                    },
                    {
                        "signal_id": "open-sports",
                        "wallet": "0xabc",
                        "condition_id": "m-open-sports",
                        "status": "open",
                        "category": "sports",
                        "league": "nba",
                        "market_type": "main_match",
                        "legs": [{"stake": 9, "would_follow": True}],
                    },
                ],
                result_events=[
                    {
                        "signal_id": "win-cs2",
                        "wallet": "0xabc",
                        "condition_id": "m-win-cs2",
                        "status": "settled",
                        "category": "esports",
                        "game_family": "cs2",
                        "market_type": "main_match",
                        "outcome_won": True,
                        "our_paper_pnl": 0.5,
                        "legs": [{"stake": 1, "would_follow": True}],
                        "settled_at": 110,
                    },
                    {
                        "signal_id": "loss-cs2",
                        "wallet": "0xabc",
                        "condition_id": "m-loss-cs2",
                        "status": "settled",
                        "category": "esports",
                        "game_family": "cs2",
                        "market_type": "main_match",
                        "outcome_won": False,
                        "our_paper_pnl": -1.0,
                        "legs": [{"stake": 1, "would_follow": True}],
                        "settled_at": 120,
                    },
                    {
                        "signal_id": "win-dota",
                        "wallet": "0xabc",
                        "condition_id": "m-win-dota",
                        "status": "settled",
                        "category": "esports",
                        "game_family": "dota2",
                        "market_type": "game_winner",
                        "outcome_won": True,
                        "our_paper_pnl": 2.0,
                        "legs": [{"stake": 4, "would_follow": True}],
                        "settled_at": 130,
                    },
                    {
                        "signal_id": "win-nba",
                        "wallet": "0xabc",
                        "condition_id": "m-win-nba",
                        "status": "settled",
                        "category": "sports",
                        "league": "nba",
                        "market_type": "main_match",
                        "outcome_won": True,
                        "our_paper_pnl": 8.0,
                        "legs": [{"stake": 9, "would_follow": True}],
                        "settled_at": 140,
                    },
                ],
                performance={"wallets": {}, "total": {}},
            )

            overview = build_overview(data_dir)

            self.assertEqual(
                [
                    {"timestamp": 110, "pnl": 0.5, "cumulative_pnl": 0.5},
                    {"timestamp": 120, "pnl": -1.0, "cumulative_pnl": -0.5},
                    {"timestamp": 130, "pnl": 2.0, "cumulative_pnl": 1.5},
                ],
                overview["equity_points"],
            )
            win_rates = {row["game"]: row for row in overview["win_rates_by_game"]}
            self.assertEqual(win_rates["cs2"]["game_label"], "CS2")
            self.assertEqual(win_rates["cs2"]["wins"], 1)
            self.assertEqual(win_rates["cs2"]["losses"], 1)
            self.assertEqual(win_rates["cs2"]["settled_count"], 2)
            self.assertEqual(win_rates["cs2"]["win_rate"], 0.5)
            self.assertEqual(win_rates["dota2"]["wins"], 1)
            self.assertNotIn("nba", win_rates)

            open_by_game = {row["game"]: row["count"] for row in overview["open_by_game"]}
            self.assertEqual({"cs2": 1, "dota2": 1}, open_by_game)

            distribution = {
                row["type"]: row
                for row in overview["follow_type_distribution"]["segments"]
            }
            self.assertEqual(distribution["main_match"]["label"], "主盘")
            self.assertEqual(distribution["main_match"]["count"], 2)
            self.assertEqual(distribution["main_match"]["stake"], 2.0)
            self.assertEqual(distribution["sub_game"]["label"], "Sub Game")
            self.assertEqual(distribution["sub_game"]["count"], 1)
            self.assertEqual(distribution["sub_game"]["stake"], 4.0)
            self.assertEqual(overview["follow_type_distribution"]["total"], 3)
            self.assertEqual(overview["follow_type_distribution"]["total_stake"], 6.0)
            by_game = {
                row["game"]: {item["type"]: item for item in row["types"]}
                for row in overview["follow_type_distribution"]["by_game"]
            }
            self.assertEqual(by_game["cs2"]["main_match"]["count"], 2)
            self.assertEqual(by_game["cs2"]["sub_game"]["count"], 0)
            self.assertEqual(by_game["dota2"]["main_match"]["count"], 0)
            self.assertEqual(by_game["dota2"]["sub_game"]["count"], 1)

    def test_dashboard_overview_exposes_total_tracking_duration(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            tracking_started_at = 100_000
            now_ts = tracking_started_at + (3 * 86400) + (7 * 3600)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m2",
                        "status": "open",
                        "created_at": tracking_started_at + 60,
                        "updated_at": tracking_started_at + 120,
                        "legs": [
                            {
                                "stake": 3,
                                "would_follow": True,
                                "wallet_trade_at": tracking_started_at,
                                "leg_at": tracking_started_at + 30,
                            }
                        ],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "sig1",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "outcome_won": True,
                        "created_at": tracking_started_at + 3600,
                        "settled_at": now_ts - 60,
                        "legs": [{"stake": 1, "wallet_trade_at": tracking_started_at + 3600}],
                    }
                ],
                performance={},
            )

            with patch("poly_fight.dashboard.time.time", return_value=now_ts):
                overview = build_overview(data_dir)

            self.assertEqual(overview["tracking_started_at"], tracking_started_at)
            self.assertEqual(overview["tracking_duration_seconds"], now_ts - tracking_started_at)

    def test_dashboard_overview_exposes_contested_and_clv(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "m1:0",
                        "wallet": "0xa",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "contested": True,
                        "wallet_clv": 0.12,
                        "legs": [{"stake": 1, "would_follow": True}],
                    },
                    {
                        "signal_id": "m1:1",
                        "wallet": "0xb",
                        "condition_id": "m1",
                        "outcome_index": 1,
                        "status": "open",
                        "contested": True,
                        "wallet_clv": 0.10,
                        "legs": [{"stake": 1, "would_follow": True}],
                    },
                    {
                        "signal_id": "m2:0",
                        "wallet": "0xc",
                        "condition_id": "m2",
                        "outcome_index": 0,
                        "status": "open",
                        "wallet_clv": 0.08,
                        "legs": [{"stake": 1, "would_follow": True}],
                    },
                ],
                result_events=[],
                performance={},
            )

            overview = build_overview(data_dir)

            self.assertEqual(overview["contested_signal_count"], 2)
            self.assertEqual(overview["disagreement_signal_count"], 2)
            self.assertEqual(overview["disagreement_condition_count"], 1)
            self.assertEqual(overview["two_sided_signal_count"], 0)
            self.assertEqual(overview["clean_signal_count"], 1)
            self.assertAlmostEqual(overview["avg_wallet_clv"], 0.1)

    def test_dashboard_quality_splits_two_sided_and_disagreement(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {"signal_id": "m1:a", "wallet": "0xa", "condition_id": "m1", "outcome_index": 0, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m1:b", "wallet": "0xa", "condition_id": "m1", "outcome_index": 1, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m2:a", "wallet": "0xb", "condition_id": "m2", "outcome_index": 0, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m2:b", "wallet": "0xc", "condition_id": "m2", "outcome_index": 1, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m3:a", "wallet": "0xd", "condition_id": "m3", "outcome_index": 0, "status": "open", "legs": [{"stake": 1}]},
                    {"signal_id": "m4:a", "wallet": "0xe", "condition_id": "m4", "outcome_index": 0, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m4:b", "wallet": "0xe", "condition_id": "m4", "outcome_index": 1, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                    {"signal_id": "m4:c", "wallet": "0xf", "condition_id": "m4", "outcome_index": 0, "status": "open", "contested": True, "legs": [{"stake": 1}]},
                ],
                result_events=[],
                performance={},
            )

            overview = build_overview(data_dir)
            follows = {row["condition_id"]: row for row in build_follows(data_dir, size=10)["follows"]}

            self.assertEqual(overview["clean_signal_count"], 1)
            self.assertEqual(overview["two_sided_signal_count"], 5)
            self.assertEqual(overview["disagreement_signal_count"], 5)
            self.assertEqual(overview["contested_signal_count"], 5)
            self.assertEqual(overview["clean_condition_count"], 1)
            self.assertEqual(overview["two_sided_condition_count"], 2)
            self.assertEqual(overview["disagreement_condition_count"], 2)
            self.assertEqual(overview["mixed_quality_condition_count"], 1)
            self.assertEqual(follows["m1"]["quality_label"], "two_sided")
            self.assertEqual(follows["m1"]["contested_signal_count"], 0)
            self.assertEqual(follows["m2"]["quality_label"], "disagreement")
            self.assertEqual(follows["m3"]["quality_label"], "one_way")
            self.assertEqual(follows["m4"]["quality_label"], "two_sided_disagreement")

    def test_dashboard_follows_use_sql_pagination_not_full_snapshot(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m2",
                        "status": "open",
                        "created_at": 200,
                        "legs": [{"stake": 2, "would_follow": True}],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "sig-settled",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "created_at": 100,
                        "settled_at": 300,
                        "our_paper_pnl": 0.5,
                        "wallet_paper_pnl_by_wallet": {"0xabc": 0.7},
                        "legs": [{"stake": 1, "would_follow": True}],
                    }
                ],
                performance={"wallets": {}, "total": {}},
            )

            with patch.object(FollowStore, "load_dashboard_snapshot", side_effect=AssertionError("full snapshot")):
                page = build_follows(data_dir, page=1, size=1)

            self.assertEqual(page["total"], 2)
            self.assertEqual(len(page["follows"]), 1)
            self.assertEqual(page["follows"][0]["condition_id"], "m2")

            open_page = build_follows(data_dir, page=1, size=10, status="open")
            settled_page = build_follows(data_dir, page=1, size=10, status="settled")

            self.assertEqual(open_page["total"], 1)
            self.assertEqual(open_page["status"], "open")
            self.assertEqual(open_page["follows"][0]["condition_id"], "m2")
            self.assertEqual(settled_page["total"], 1)
            self.assertEqual(settled_page["status"], "settled")
            self.assertEqual(settled_page["follows"][0]["condition_id"], "m1")

    def test_dashboard_wallet_rows_omit_heavy_internal_payloads(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": wallet,
                        "grade": "A",
                        "best_bucket_score": 92,
                        "positive_market_rate": 0.95,
                        "wilson_win_rate_lower_bound": 0.84,
                        "entry_edge": 0.22,
                        "esports_roi": 0.41,
                        "eligible_market_types": ["main_match"],
                        "eligible_market_type_labels": ["主盘"],
                        "bucket_scores": {"main_match": {"debug_blob": "x" * 1000}},
                        "per_type_grades": {
                            "main_match": {
                                "esports_win_count": 10,
                                "esports_loss_count": 1,
                                "positive_market_rate": 0.91,
                                "wilson_win_rate_lower_bound": 0.75,
                                "entry_edge": 0.18,
                                "esports_roi": 0.36,
                                "avg_market_cash": 2000,
                                "debug_blob": "y" * 1000,
                            }
                        },
                        "per_game_type_grades": {"cs2:main_match": {"debug_blob": "z" * 1000}},
                    }
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "status": "open",
                        "created_at": 100,
                        "debug_blob": "raw-signal" * 200,
                        "legs": [
                            {
                                "stake": 1,
                                "our_entry_price": 0.5,
                                "wallet_trade_at": 100,
                                "debug_blob": "raw-leg" * 200,
                            }
                        ],
                    }
                ],
                result_events=[],
                performance={"wallets": {wallet: {"signals": 3, "wins": 2, "our_pnl": 1.25}}, "total": {}},
            )

            row = build_wallets(data_dir)["wallets"][0]

            self.assertEqual(row["wallet"], wallet)
            self.assertEqual(row["rank"], 1)
            self.assertNotIn("esports_win_count", row)  # 已从 /api/wallets 瘦身剔除(前端不展示)
            self.assertEqual(row["observed"]["open"], 1)
            self.assertEqual(row["observed"]["signals"], 3)
            self.assertNotIn("bucket_scores", row)
            self.assertNotIn("per_type_grades", row)
            self.assertNotIn("per_game_type_grades", row)
            self.assertNotIn("open_signals", row)
            self.assertNotIn("performance", row)

    def test_dashboard_follows_merges_exited_into_settled_status_with_settlement_type(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "auto",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "settled_at": 200,
                        "legs": [{"stake": 1}],
                    },
                    {
                        "signal_id": "exit",
                        "wallet": "0xdef",
                        "condition_id": "m1",
                        "status": "exited",
                        "exit_at": 300,
                        "legs": [{"stake": 1}],
                    },
                    {
                        "signal_id": "exit-only",
                        "wallet": "0xghi",
                        "condition_id": "m2",
                        "status": "exited",
                        "exit_at": 400,
                        "legs": [{"stake": 1}],
                    },
                ],
                performance={},
            )

            page = build_follows(data_dir, page=1, size=10)
            settled_page = build_follows(data_dir, page=1, size=10, status="settled")
            exited_page = build_follows(data_dir, page=1, size=10, status="exited")

            by_condition = {row["condition_id"]: row for row in page["follows"]}
            self.assertEqual(by_condition["m1"]["status"], "settled")
            self.assertEqual(by_condition["m1"]["settlement_type"], "auto_and_manual")
            self.assertEqual(by_condition["m2"]["status"], "settled")
            self.assertEqual(by_condition["m2"]["settlement_type"], "manual_exit")
            self.assertEqual({row["condition_id"] for row in settled_page["follows"]}, {"m1", "m2"})
            self.assertEqual(exited_page["status"], "")

    def test_dashboard_follows_expose_readable_market_fields(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "open",
                        "event_title": "Counter-Strike: A vs B",
                        "market_question": "A vs B",
                        "match_start_time": "2026-06-06T12:00:00Z",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            page = build_follows(data_dir, page=1, size=10)
            detail = build_follow_detail(data_dir, "m1")

            self.assertEqual(page["follows"][0]["title"], "Counter-Strike: A vs B")
            self.assertEqual(page["follows"][0]["question"], "A vs B")
            self.assertEqual(page["follows"][0]["match_start_time"], "2026-06-06T12:00:00Z")
            self.assertEqual(detail["title"], "Counter-Strike: A vs B")
            self.assertEqual(detail["match_start_time"], "2026-06-06T12:00:00Z")

    def test_dashboard_follows_expose_stake_mode_fields(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "open",
                        "event_title": "Counter-Strike: A vs B",
                        "stake_mode": "proportional",
                        "stake_ratio_percent": 10,
                        "signal_stake": 5,
                        "created_at": 100,
                        "legs": [{"stake": 5, "stake_mode": "proportional"}, {"stake": 1, "stake_mode": "minimum"}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            page = build_follows(data_dir, page=1, size=10)
            detail = build_follow_detail(data_dir, "m1")

            row = page["follows"][0]
            self.assertEqual(row["stake_mode_counts"], {"proportional": 1})
            self.assertEqual(row["signal_stake_min"], 6.0)
            self.assertEqual(row["signal_stake_max"], 6.0)
            self.assertEqual(detail["wallets"][0]["signals"][0]["stake_mode"], "proportional")
            self.assertEqual(detail["wallets"][0]["signals"][0]["signal_stake"], 5)

    def test_dashboard_follows_calculates_unrealized_pnl_from_active_cache(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": 200,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.6, 0.4],
                        }
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open-a",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 100,
                        "legs": [{"stake": 10, "our_entry_price": 0.5}],
                    },
                    {
                        "signal_id": "sig-open-b",
                        "wallet": "0xdef",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 101,
                        "legs": [{"stake": 5, "our_entry_price": 0.4}],
                    },
                ],
                result_events=[],
                performance={},
            )

            row = build_follows(data_dir, page=1, size=10)["follows"][0]

            self.assertEqual(row["current_price"], 0.6)
            self.assertEqual(row["unrealized_pnl"], 4.5)
            self.assertEqual(row["display_pnl"], 4.5)
            self.assertEqual(row["display_pnl_kind"], "unrealized")

    def test_dashboard_follows_excludes_blocked_legs_from_unrealized_pnl(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": 200,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.1, 0.9],
                        }
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "outcome_index": 1,
                        "status": "open",
                        "created_at": 100,
                        "legs": [
                            {"stake": 100, "our_entry_price": 0.5, "would_follow": True},
                            {"stake": 50, "our_entry_price": 0.75, "would_follow": False},
                        ],
                    },
                ],
                result_events=[],
                performance={},
            )

            row = build_follows(data_dir, page=1, size=10)["follows"][0]

            self.assertEqual(row["stake"], 100)
            self.assertEqual(row["signal_stake_min"], 100)
            self.assertEqual(row["signal_stake_max"], 100)
            self.assertEqual(row["unrealized_pnl"], 80)

    def test_dashboard_rows_include_cached_team_logos(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            static_logo_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboardV2" / "logo" / "team_logos.json"
            old_static_logos = read_json(static_logo_path, None) if static_logo_path.exists() else None
            self.addCleanup(lambda: write_json(static_logo_path, old_static_logos) if old_static_logos is not None else static_logo_path.unlink(missing_ok=True))
            write_json(
                static_logo_path,
                {
                    "teams": {
                        "counter strike:faze": "https://img.example/faze.png",
                        "counter strike:sinners": "https://img.example/sinners.png",
                    }
                },
            )
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(time.time()),
                    "markets": [
                        {
                            "condition_id": "m1",
                            "title": "Counter-Strike: FaZe vs Sinners (BO1) - Test Cup",
                            "match_start_time": datetime.fromtimestamp(int(time.time()) + 3600, timezone.utc).isoformat(),
                        }
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "open",
                        "event_title": "Counter-Strike: FaZe vs Sinners (BO1) - Test Cup",
                        "match_start_time": "2026-06-06T12:00:00Z",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            follows = build_follows(data_dir, page=1, size=10)
            events = build_events(data_dir)
            detail = build_follow_detail(data_dir, "m1")

            expected = {"teamA": "https://img.example/faze.png", "teamB": "https://img.example/sinners.png"}
            self.assertEqual(follows["follows"][0]["team_logos"], expected)
            self.assertEqual(events["events"][0]["team_logos"], expected)
            self.assertEqual(detail["team_logos"], expected)

    def test_refresh_team_logo_cache_scans_watched_event_slugs(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            static_logo_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboardV2" / "logo" / "team_logos.json"
            static_logo_dir = static_logo_path.parent
            old_static_logos = read_json(static_logo_path, None) if static_logo_path.exists() else None
            old_static_files = {path.name: path.read_bytes() for path in static_logo_dir.glob("*") if path.is_file()} if static_logo_dir.exists() else {}

            def restore_static_logos():
                if static_logo_dir.exists():
                    for path in static_logo_dir.glob("*"):
                        if path.is_file():
                            path.unlink()
                static_logo_dir.mkdir(parents=True, exist_ok=True)
                for name, content in old_static_files.items():
                    (static_logo_dir / name).write_bytes(content)
                if old_static_logos is not None:
                    write_json(static_logo_path, old_static_logos)

            self.addCleanup(restore_static_logos)
            if static_logo_dir.exists():
                for path in static_logo_dir.glob("*"):
                    if path.is_file():
                        path.unlink()
            static_logo_dir.mkdir(parents=True, exist_ok=True)
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(time.time()),
                    "markets": [
                        {
                            "condition_id": "m1",
                            "event_slug": "cs2-lgc-mibr-2026-06-06",
                            "title": "Counter-Strike: Legacy vs MIBR (BO1) - IEM Cologne Major Stage 2",
                            "match_start_time": "2026-06-07T00:30:00Z",
                        },
                        {
                            "condition_id": "m2",
                            "event_slug": "far-future",
                            "title": "Counter-Strike: A vs B (BO1) - Later",
                            "match_start_time": "2026-06-10T00:30:00Z",
                        }
                    ],
                },
            )

            def fake_fetch_html(slug):
                self.assertNotEqual(slug, "far-future")
                self.assertEqual(slug, "cs2-lgc-mibr-2026-06-06")
                return (
                    'url=https%3A%2F%2Fpolymarket-upload.s3.us-east-2.amazonaws.com%2Fteam_logos%2Fesports%2Fcs2%2Fcs-go_legacy_133708.png '
                    'url=https%3A%2F%2Fpolymarket-upload.s3.us-east-2.amazonaws.com%2Fteam_logos%2Fesports%2Fcs2%2FMIBR-HcOUoxRfudPA.png'
                )

            fetched_urls = []

            def fake_fetch_logo_bytes(url, timeout_seconds):
                fetched_urls.append(url)
                return f"png:{url}".encode()

            summary = refresh_team_logo_cache_from_active_markets(
                data_dir,
                observe_window_hours=24,
                now_ts=int(datetime(2026, 6, 7, 0, 0, tzinfo=timezone.utc).timestamp()),
                fetch_html=fake_fetch_html,
                fetch_logo_bytes=fake_fetch_logo_bytes,
            )
            logos = read_json(static_logo_path, {})

            self.assertEqual(summary["watched_event_count"], 1)
            self.assertEqual(len(fetched_urls), 2)
            self.assertTrue(logos["teams"]["legacy"].startswith("/logo/"))
            self.assertTrue(logos["teams"]["mibr"].startswith("/logo/"))
            self.assertTrue((static_logo_dir / logos["teams"]["legacy"].rsplit("/", 1)[-1]).exists())
            self.assertTrue((static_logo_dir / logos["teams"]["mibr"].rsplit("/", 1)[-1]).exists())

            summary = refresh_team_logo_cache_from_active_markets(
                data_dir,
                observe_window_hours=24,
                now_ts=int(datetime(2026, 6, 7, 0, 0, tzinfo=timezone.utc).timestamp()),
                fetch_html=lambda _slug: (_ for _ in ()).throw(AssertionError("cached logos should skip HTML fetch")),
                fetch_logo_bytes=fake_fetch_logo_bytes,
            )
            self.assertEqual(summary["watched_event_count"], 0)

    def test_dashboard_follow_detail_backfills_event_link_and_end_time(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": 100,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "event_slug": "counter-strike-a-vs-b",
                            "title": "Counter-Strike: A vs B",
                            "question": "A vs B",
                            "match_start_time": "2026-06-06T12:00:00Z",
                            "end_date": "2026-06-06T13:00:00Z",
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.42, 0.58],
                        }
                    ],
                },
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "open",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            detail = build_follow_detail(data_dir, "m1")

            self.assertEqual(detail["title"], "Counter-Strike: A vs B")
            self.assertEqual(detail["end_date"], "2026-06-06T13:00:00Z")
            self.assertEqual(detail["event_url"], "https://polymarket.com/event/counter-strike-a-vs-b")
            self.assertEqual(detail["outcomes"], ["A", "B"])
            self.assertEqual(detail["outcome_prices"], [0.42, 0.58])

    def test_dashboard_fetch_market_prices_updates_active_cache(self):
        class FakeClient:
            def gamma(self, path, **params):
                self.path = path
                self.params = params
                return [
                    {
                        "conditionId": "m1",
                        "question": "A vs B",
                        "slug": "a-vs-b",
                        "outcomes": json.dumps(["A", "B"]),
                        "outcomePrices": json.dumps(["0.44", "0.56"]),
                    }
                ]

        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            FollowStore(data_dir / "follow" / "follow.db").save_market_cache(
                {
                    "m1": {
                        "condition_id": "m1",
                        "outcomes": ["A", "B"],
                        "outcome_prices": [0.4, 0.6],
                    }
                },
                cache_kind="active",
                updated_at=100,
            )

            client = FakeClient()
            result = fetch_market_prices(data_dir, client, "m1")
            cached = FollowStore(data_dir / "follow" / "follow.db").get_market_cache_item("active", "m1")

            self.assertEqual(client.path, "/markets")
            self.assertEqual(client.params["condition_ids"], "m1")
            self.assertEqual(result["outcomes"], ["A", "B"])
            self.assertEqual(result["outcome_prices"], [0.44, 0.56])
            self.assertEqual(cached["outcome_prices"], [0.44, 0.56])

    def test_dashboard_follow_detail_uses_condition_sql_not_full_snapshot(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "sig1",
                        "wallet": "0xabc",
                        "condition_id": "m1",
                        "status": "settled",
                        "created_at": 100,
                        "settled_at": 300,
                        "legs": [{"stake": 1}],
                    },
                    {
                        "signal_id": "sig2",
                        "wallet": "0xdef",
                        "condition_id": "m2",
                        "status": "settled",
                        "created_at": 200,
                        "settled_at": 400,
                        "legs": [{"stake": 1}],
                    },
                ],
                performance={"wallets": {}, "total": {}},
            )

            with patch.object(FollowStore, "load_dashboard_snapshot", side_effect=AssertionError("full snapshot")):
                detail = build_follow_detail(data_dir, "m1")

            self.assertEqual(detail["signal_count"], 1)
            self.assertEqual(detail["wallets"][0]["wallet"], "0xabc")

    def test_dashboard_follow_detail_rank_matches_visible_wallet_list(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            visible_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            hidden = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            visible_b = "0xcccccccccccccccccccccccccccccccccccccccc"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": visible_a,
                        "grade": "A",
                        "best_bucket_score": 90,
                        "esports_win_count": 12,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.90,
                        "entry_edge": 0.30,
                        "esports_roi": 0.80,
                    },
                    {
                        "wallet": hidden,
                        "grade": "A",
                        "best_bucket_score": 89,
                        "esports_win_count": 12,
                        "esports_loss_count": 0,
                        "positive_market_rate": 0.10,
                        "wilson_win_rate_lower_bound": 0.89,
                        "entry_edge": 0.29,
                        "esports_roi": 0.79,
                    },
                    {
                        "wallet": visible_b,
                        "grade": "A",
                        "best_bucket_score": 88,
                        "esports_win_count": 12,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.88,
                        "entry_edge": 0.28,
                        "esports_roi": 0.78,
                    },
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-rank",
                        "wallet": visible_b,
                        "condition_id": "m1",
                        "status": "open",
                        "category": "esports",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            wallets = build_wallets(data_dir)["wallets"]
            detail = build_follow_detail(data_dir, "m1")

            # v17:展示层不再按"拉通整体胜率"过滤 → 整体胜率 0.10 的专精钱包(grade A)照常显示;
            # 三个都在,visible_b 排第 3。
            self.assertEqual([row["wallet"] for row in wallets], [visible_a, hidden, visible_b])
            self.assertEqual(detail["wallets"][0]["leaderboard_rank"], 3)

    def test_dashboard_follow_detail_rank_respects_quarantine_sorting(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            clean_top = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            quarantined = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            target = "0xcccccccccccccccccccccccccccccccccccccccc"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": clean_top,
                        "grade": "A",
                        "best_bucket_score": 90,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.90,
                        "entry_edge": 0.30,
                    },
                    {
                        "wallet": quarantined,
                        "grade": "A",
                        "best_bucket_score": 89,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.89,
                        "entry_edge": 0.29,
                    },
                    {
                        "wallet": target,
                        "grade": "A",
                        "best_bucket_score": 88,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.88,
                        "entry_edge": 0.28,
                    },
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-rank",
                        "wallet": target,
                        "condition_id": "m1",
                        "status": "open",
                        "category": "esports",
                        "created_at": 100,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )
            store.upsert_wallet_quarantine(quarantined, reason="material_sell", ts=100)

            wallets = build_wallets(data_dir)["wallets"]
            detail = build_follow_detail(data_dir, "m1")

            self.assertEqual([row["wallet"] for row in wallets], [clean_top, target, quarantined])
            self.assertEqual([row.get("rank") for row in wallets], [1, 2, None])
            self.assertEqual(detail["wallets"][0]["leaderboard_rank"], 2)

    def test_dashboard_wallet_ranks_compact_after_quarantine(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            clean_top = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            quarantined = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            shifted = "0xcccccccccccccccccccccccccccccccccccccccc"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {"wallet": clean_top, "grade": "A", "best_bucket_score": 90, "positive_market_rate": 1.0},
                    {"wallet": quarantined, "grade": "A", "best_bucket_score": 89, "positive_market_rate": 1.0},
                    {"wallet": shifted, "grade": "A", "best_bucket_score": 88, "positive_market_rate": 1.0},
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine(quarantined, reason="material_sell", ts=100)

            wallets = build_wallets(data_dir)

            by_wallet = {row["wallet"]: row for row in wallets["wallets"]}
            self.assertEqual([row["wallet"] for row in wallets["wallets"]], [clean_top, shifted, quarantined])
            self.assertEqual(by_wallet[clean_top]["rank"], 1)
            self.assertEqual(by_wallet[shifted]["rank"], 2)
            self.assertNotIn("rank", by_wallet[quarantined])
            self.assertEqual(wallets["active_count"], 2)
            self.assertEqual(wallets["quarantined_count"], 1)

    def test_dashboard_follow_detail_does_not_mix_wallet_avg_across_outcomes(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-a",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "outcome": "A",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 100,
                        "legs": [{"stake": 100, "our_entry_price": 0.7, "would_follow": True}],
                    },
                    {
                        "signal_id": "sig-b",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "outcome": "B",
                        "outcome_index": 1,
                        "status": "open",
                        "created_at": 101,
                        "legs": [{"stake": 100, "our_entry_price": 0.1, "would_follow": True}],
                    },
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            detail = build_follow_detail(data_dir, "m1")

            wallet_row = detail["wallets"][0]
            self.assertEqual(wallet_row["follow_total_stake"], 200)
            self.assertTrue(wallet_row["follow_mixed_outcomes"])
            self.assertEqual(wallet_row["followed_outcome_count"], 2)
            self.assertIsNone(wallet_row["follow_avg_entry_price"])
            signal_avgs = {signal["signal_id"]: signal["follow_avg_entry_price"] for signal in wallet_row["signals"]}
            self.assertEqual(signal_avgs, {"sig-a": 0.7, "sig-b": 0.1})

    def test_dashboard_follow_detail_realized_pnl_uses_status_specific_field(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "sig-exited-loss",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "status": "exited",
                        "created_at": 100,
                        "exit_at": 200,
                        "our_paper_pnl": 0,
                        "our_realized_pnl": -12.5,
                        "legs": [{"stake": 50, "our_entry_price": 0.8}],
                    },
                    {
                        "signal_id": "sig-settled-loss",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "status": "settled",
                        "created_at": 101,
                        "settled_at": 201,
                        "outcome_won": False,
                        "our_paper_pnl": -7.5,
                        "our_realized_pnl": 0,
                        "legs": [{"stake": 7.5, "our_entry_price": 0.5}],
                    },
                ],
                performance={"wallets": {}, "total": {}},
            )

            detail = build_follow_detail(data_dir, "m1")

            wallet_row = detail["wallets"][0]
            pnl_by_signal = {signal["signal_id"]: signal["follow_realized_pnl"] for signal in wallet_row["signals"]}
            self.assertEqual(pnl_by_signal["sig-exited-loss"], -12.5)
            self.assertEqual(pnl_by_signal["sig-settled-loss"], -7.5)
            self.assertEqual(wallet_row["follow_realized_pnl"], -20.0)

    def test_dashboard_follow_detail_exited_total_stake_uses_original_follow_stake(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "sig-exited",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "exited",
                        "created_at": 100,
                        "exit_at": 200,
                        "our_realized_pnl": -589,
                        "legs": [
                            {"stake": 1000, "funded_stake": 9.71, "our_entry_price": 0.5, "would_follow": True},
                            {"stake": 250, "funded_stake": 0, "our_entry_price": 0.45, "would_follow": False},
                        ],
                    }
                ],
                performance={"wallets": {}, "total": {}},
            )

            detail = build_follow_detail(data_dir, "m1")
            row = build_follows(data_dir, page=1, size=10)["follows"][0]

            signal = detail["wallets"][0]["signals"][0]
            self.assertEqual(signal["follow_total_stake"], 1000)
            self.assertEqual(detail["wallets"][0]["follow_total_stake"], 1000)
            self.assertEqual(row["stake"], 1000)
            self.assertEqual(row["signal_stake_min"], 1000)
            self.assertEqual(row["signal_stake_max"], 1000)

    def test_dashboard_follow_detail_open_total_stake_uses_original_follow_stake(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-open",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 100,
                        "legs": [
                            {"stake": 1729.71, "funded_stake": 9.71, "our_entry_price": 0.5, "would_follow": True},
                        ],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            detail = build_follow_detail(data_dir, "m1")
            row = build_follows(data_dir, page=1, size=10)["follows"][0]

            signal = detail["wallets"][0]["signals"][0]
            self.assertEqual(signal["follow_total_stake"], 1729.71)
            self.assertEqual(detail["wallets"][0]["follow_total_stake"], 1729.71)
            self.assertEqual(row["stake"], 1729.71)
            self.assertEqual(row["signal_stake_min"], 1729.71)
            self.assertEqual(row["signal_stake_max"], 1729.71)

    def test_dashboard_wallet_follow_detail_merges_closed_statuses_and_paginates(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            result_events = []
            for index in range(25):
                status = "exited" if index % 2 else "settled"
                result_events.append(
                    {
                        "signal_id": f"closed-{index:02d}",
                        "wallet": wallet,
                        "condition_id": f"m{index}",
                        "status": status,
                        "created_at": 100 + index,
                        "settled_at": 500 + index if status == "settled" else 0,
                        "exit_at": 500 + index if status == "exited" else 0,
                        "legs": [{"stake": 1}],
                    }
                )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "open-1",
                        "wallet": wallet,
                        "condition_id": "open-market",
                        "status": "open",
                        "created_at": 999,
                        "legs": [{"stake": 1}],
                    }
                ],
                result_events=result_events,
                performance={"wallets": {}, "total": {}},
            )

            detail = build_wallet_follow_detail(data_dir, wallet, status="closed", page=2, size=20)

            self.assertEqual(detail["status"], "closed")
            self.assertEqual(detail["total"], 25)
            self.assertEqual(detail["count"], 25)
            self.assertEqual(detail["page"], 2)
            self.assertEqual(detail["size"], 20)
            self.assertEqual(len(detail["signals"]), 5)
            self.assertEqual({row["status"] for row in detail["signals"]}, {"settled", "exited"})
            self.assertEqual(
                {row["settlement_type"] for row in detail["signals"]},
                {"auto_settlement", "manual_exit"},
            )

    def test_dashboard_wallets_expose_quarantine_state(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {"wallet": "0xabc", "grade": "A"},
                    {"wallet": "0xdef", "grade": "A"},
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine("0xabc", reason="recent_chop_loss", ts=100)

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["quarantined_count"], 1)
            self.assertEqual(wallets["active_count"], 1)
            self.assertEqual(wallets["by_category"]["esports"]["quarantined_count"], 1)
            self.assertEqual(wallets["by_category"]["esports"]["active_count"], 1)
            quarantined = [row for row in wallets["wallets"] if row["quarantined"]]
            active = [row for row in wallets["wallets"] if not row["quarantined"]]
            self.assertEqual([row["wallet"] for row in active], ["0xdef"])
            self.assertEqual([row["wallet"] for row in quarantined], ["0xabc"])
            self.assertEqual(quarantined[0]["quarantine"]["reason"], "recent_chop_loss")

    def test_dashboard_wallets_separate_active_favorite_and_quarantine_counts(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {"wallet": "0xaaa", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                    {"wallet": "0xbbb", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                    {"wallet": "0xccc", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_favorite(
                "0xbbb",
                category="esports",
                favorite=True,
                ts=200,
                snapshot={"wallet": "0xbbb", "grade": "A", "eligible_market_types": ["main_match"]},
            )
            store.upsert_wallet_quarantine("0xbbb", reason="recent_chop_loss", ts=100, category="esports")
            store.upsert_wallet_quarantine("0xccc", reason="recent_chop_loss", ts=100, category="esports")

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["active_count"], 1)
            self.assertEqual(wallets["favorite_count"], 0)
            self.assertEqual(wallets["quarantined_count"], 2)
            self.assertEqual(wallets["by_category"]["esports"]["active_count"], 1)
            self.assertEqual(wallets["by_category"]["esports"]["favorite_count"], 0)
            self.assertEqual(wallets["by_category"]["esports"]["quarantined_count"], 2)
            by_wallet = {row["wallet"]: row for row in wallets["wallets"]}
            self.assertFalse(by_wallet["0xaaa"]["favorite"])
            self.assertFalse(by_wallet["0xaaa"]["quarantined"])
            self.assertFalse(by_wallet["0xbbb"]["favorite"])
            self.assertTrue(by_wallet["0xbbb"]["quarantined"])
            self.assertTrue(by_wallet["0xccc"]["quarantined"])

    def test_dashboard_wallets_keep_leaderboard_favorites_active(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {"wallet": "0xaaa", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                    {"wallet": "0xbbb", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                    {"wallet": "0xccc", "category": "esports", "grade": "A", "positive_market_rate": 1.0},
                ],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_favorite(
                "0xbbb",
                category="esports",
                favorite=True,
                ts=200,
                snapshot={"wallet": "0xbbb", "grade": "A", "eligible_market_types": ["main_match"]},
            )

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["active_count"], 3)
            self.assertEqual(wallets["favorite_count"], 1)
            self.assertEqual(wallets["by_category"]["esports"]["active_count"], 3)
            self.assertEqual(wallets["by_category"]["esports"]["favorite_count"], 1)
            by_wallet = {row["wallet"]: row for row in wallets["wallets"]}
            self.assertTrue(by_wallet["0xbbb"]["favorite"])
            self.assertFalse(by_wallet["0xbbb"]["favorite_snapshot_only"])

    def test_dashboard_wallets_append_snapshot_only_favorites(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_favorite(
                "0xbbb",
                category="esports",
                favorite=True,
                ts=200,
                snapshot={
                    "wallet": "0xbbb",
                    "category": "esports",
                    "grade": "A",
                    "positive_market_rate": 1.0,
                    "avg_market_cash": 1234,
                    "eligible_market_types": ["game_winner"],
                },
            )

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["favorite_count"], 1)
            self.assertEqual(wallets["active_count"], 0)
            self.assertEqual(wallets["wallets"][0]["wallet"], "0xbbb")
            self.assertTrue(wallets["wallets"][0]["favorite"])
            self.assertTrue(wallets["wallets"][0]["favorite_snapshot_only"])
            self.assertEqual(wallets["wallets"][0]["avg_market_cash"], 1234)

    def test_dashboard_wallets_prefer_sqlite_leaderboard_over_legacy_json(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xlegacy", "grade": "A", "positive_market_rate": 1.0}],
            )
            storage_module.LeaderboardStore(data_dir / "leaderboard.db").replace_leaderboard(
                [
                    {
                        "wallet": "0xsqlite",
                        "grade": "A",
                        "esports_win_count": 8,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.8,
                        "entry_edge": 0.3,
                        "esports_roi": 0.5,
                    }
                ],
                category="esports",
                updated_at=123,
            )

            wallets = build_wallets(data_dir)

            self.assertEqual([row["wallet"] for row in wallets["wallets"]], ["0xsqlite"])
            self.assertEqual(wallets["leaderboard_updated_at"], 123)

    def test_follow_reader_uses_sqlite_leaderboard(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage_module.LeaderboardStore(root / "esports" / "leaderboard.db").replace_leaderboard(
                [{"wallet": "0xsqlite", "grade": "A"}],
                category="esports",
                updated_at=456,
            )

            rows, mtimes = read_category_leaderboards(root)

            self.assertEqual(rows, [{"wallet": "0xsqlite", "grade": "A", "category": "esports"}])
            self.assertEqual(mtimes["esports"], 456)

    def test_collector_dashboard_publish_writes_standard_outputs_for_dashboard_and_follow(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            collector_dir = root / "collector" / "esports"
            data_dir = root / "esports"
            collector_dir.mkdir(parents=True)
            leaderboard = [
                {
                    "wallet": "0xcollector",
                    "grade": "A",
                    "lane": "core",
                    "eligible_buckets": ["cs2:main_match"],
                    "eligible_market_types": ["main_match"],
                }
            ]
            profiles = [
                {
                    "wallet": "0xcollector",
                    "grade": "A",
                    "per_game_type": {"cs2:main_match": {"esports_roi": 0.31}},
                }
            ]
            summary = {"collector": "wallet_collector", "leaderboard_wallet_count": 1}
            write_json(collector_dir / "collector_leaderboard.json", leaderboard)
            write_json(collector_dir / "collector_wallet_profiles.json", profiles)

            publish = publish_collector_dashboard_outputs(
                collector_dir,
                data_dir,
                summary=summary,
                now_ts=789,
            )

            self.assertEqual(publish["leaderboard_wallet_count"], 1)
            self.assertEqual(read_json(data_dir / "wallet_profiles.json", []), profiles)
            rows, mtimes = read_category_leaderboards(root)
            self.assertEqual(rows[0]["wallet"], "0xcollector")
            self.assertEqual(rows[0]["lane"], "core")
            self.assertEqual(mtimes["esports"], 789)
            dashboard_summary = storage_module.LeaderboardStore(data_dir / "leaderboard.db").load_latest_collection_run(category="esports")
            self.assertEqual(dashboard_summary["collector"], "wallet_collector")
            self.assertEqual(dashboard_summary["dashboard_publish"]["collector_output_dir"], str(collector_dir))

    def test_leaderboard_store_publish_collection_persists_profiles_and_summary(self):
        with TemporaryDirectory() as tmp:
            store = storage_module.LeaderboardStore(Path(tmp) / "leaderboard.db")

            store.publish_collection(
                category="esports",
                leaderboard=[
                    {
                        "wallet": "0xaaa",
                        "grade": "A",
                        "league": "cs2",
                        "best_bucket": "cs2:main_match",
                        "best_bucket_score": 0.91,
                        "scoring_version": 7,
                        "last_trade_at": 111,
                        "positive_market_rate": 0.88,
                        "avg_market_cash": 1234,
                        "participated_market_count": 8,
                        "total_cash_volume": 9876,
                    }
                ],
                profiles=[
                    {
                        "wallet": "0xaaa",
                        "grade": "A",
                        "profile_state": "qualified",
                        "profiled_at": 110,
                        "scoring_version": 7,
                        "last_trade_at": 111,
                        "profile_lookback_days": 60,
                        "best_bucket": "cs2:main_match",
                        "esports_roi": 0.42,
                        "positive_market_rate": 0.88,
                        "avg_market_cash": 1234,
                        "participated_market_count": 8,
                    }
                ],
                summary={
                    "collector": "wallet_collector",
                    "classification_market_count": 10,
                    "target_market_count": 5,
                    "seed_wallet_count": 4,
                    "profile_wallet_count": 3,
                    "profiled_wallet_count": 2,
                    "leaderboard_wallet_count": 1,
                },
                updated_at=222,
            )

            leaderboard_rows, mtimes = store.load_leaderboard(category="esports")
            profile_rows = store.load_wallet_profiles(category="esports")
            run = store.load_latest_collection_run(category="esports")

            self.assertEqual(mtimes["esports"], 222)
            self.assertEqual(leaderboard_rows[0]["wallet"], "0xaaa")
            self.assertEqual(profile_rows[0]["wallet"], "0xaaa")
            self.assertEqual(profile_rows[0]["esports_roi"], 0.42)
            self.assertEqual(run["collector"], "wallet_collector")
            self.assertEqual(run["leaderboard_wallet_count"], 1)

    def test_dashboard_wallets_use_observed_follow_trade_time(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xabc", "grade": "A", "last_esports_trade_at": 100}],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "sig-1",
                        "wallet": "0xabc",
                        "condition_id": "0xmarket",
                        "outcome_index": 0,
                        "status": "open",
                        "created_at": 150,
                        "updated_at": 200,
                        "legs": [{"wallet_trade_at": 500, "leg_at": 510, "stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["wallets"][0]["last_esports_trade_at"], 500)

    def test_dashboard_wallets_show_eligible_bucket_stats_not_blended_overall(self):
        # Overall record is weak (30W/20L, 0.60 positive rate) and would be filtered by the
        # 0.75 floor, but the wallet is grade A purely on its strong game_winner bucket.
        # build_wallets must keep it and surface the bucket's stats, not the blended overall.
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xabc",
                        "grade": "A",
                        "esports_win_count": 30,
                        "esports_loss_count": 20,
                        "positive_market_rate": 0.60,
                        "wilson_win_rate_lower_bound": 0.51,
                        "entry_edge": 0.052,
                        "esports_roi": 0.38,
                        "eligible_market_types": ["game_winner"],
                        "eligible_market_type_labels": ["单局"],
                        "per_type_grades": {
                            "main_match": {
                                "grade": "B",
                                "esports_win_count": 19,
                                "esports_loss_count": 19,
                                "esports_closed_count": 38,
                                "positive_market_rate": 0.5,
                            },
                            "game_winner": {
                                "grade": "A",
                                "esports_win_count": 11,
                                "esports_loss_count": 1,
                                "esports_closed_count": 12,
                                "positive_market_rate": 0.9167,
                                "wilson_win_rate_lower_bound": 0.72,
                                "entry_edge": 0.22,
                                "esports_roi": 0.45,
                            }
                        },
                    }
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db")

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["count"], 1)
            row = wallets["wallets"][0]
            self.assertEqual(row["grade"], "A")
            self.assertEqual(row["rank"], 1)
            self.assertEqual(row["esports_loss_count"], 1)
            self.assertEqual(row["wilson_win_rate_lower_bound"], 0.72)
            # eligible/observed market_types 已从 /api/wallets 瘦身剔除(前端不展示,信息由 buckets 承载)

    def test_dashboard_wallets_expose_multiple_game_bucket_fields(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xabc",
                        "grade": "A",
                        "eligible_market_types": ["main_match"],
                        "eligible_buckets": ["cs2:main_match", "dota2:main_match"],
                        "eligible_bucket_labels": ["CS2 主盘", "Dota2 主盘"],
                        "eligible_game_families": ["cs2", "dota2"],
                        "eligible_game_family_labels": ["CS2", "Dota2"],
                        "positive_market_rate": 0.55,
                        "wilson_win_rate_lower_bound": 0.50,
                        "entry_edge": 0.02,
                        "esports_roi": 0.05,
                        "per_game_type_grades": {
                            "cs2:main_match": {
                                "grade": "A",
                                "bucket_key": "cs2:main_match",
                                "bucket_label": "CS2 主盘",
                                "game_family": "cs2",
                                "game_family_label": "CS2",
                                "market_type": "main_match",
                                "market_type_label": "主盘",
                                "esports_win_count": 8,
                                "esports_loss_count": 0,
                                "esports_closed_count": 8,
                                "positive_market_rate": 1.0,
                                "wilson_win_rate_lower_bound": 0.83,
                                "entry_edge": 0.25,
                                "capital_weighted_edge": 0.25,
                                "esports_roi": 0.50,
                            },
                            "dota2:main_match": {
                                "grade": "A",
                                "bucket_key": "dota2:main_match",
                                "bucket_label": "Dota2 主盘",
                                "game_family": "dota2",
                                "game_family_label": "Dota2",
                                "market_type": "main_match",
                                "market_type_label": "主盘",
                                "esports_win_count": 8,
                                "esports_loss_count": 0,
                                "esports_closed_count": 8,
                                "positive_market_rate": 0.95,
                                "wilson_win_rate_lower_bound": 0.80,
                                "entry_edge": 0.20,
                                "capital_weighted_edge": 0.20,
                                "esports_roi": 0.42,
                            },
                        },
                    }
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db")

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["count"], 1)
            row = wallets["wallets"][0]
            self.assertEqual(row["eligible_buckets"], ["cs2:main_match", "dota2:main_match"])
            self.assertEqual(row["observed_buckets"], ["cs2:main_match", "dota2:main_match"])
            self.assertEqual(row["best_bucket"], "cs2:main_match")
            self.assertEqual(row["wilson_win_rate_lower_bound"], 0.83)

    def test_dashboard_wallets_surface_local_follow_losses_without_changing_historical_rank(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": "0xloser",
                        "grade": "A",
                        "esports_win_count": 8,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.83,
                        "entry_edge": 0.3,
                        "esports_roi": 0.75,
                    },
                    {
                        "wallet": "0xclean",
                        "grade": "A",
                        "esports_win_count": 8,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.82,
                        "entry_edge": 0.29,
                        "esports_roi": 0.74,
                    },
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[],
                performance={
                    "wallets": {
                        "0xloser": {"signals": 1, "wins": 0, "our_pnl": -1.0, "wallet_pnl": -1.0},
                        "0xclean": {"signals": 1, "wins": 1, "our_pnl": 1.0, "wallet_pnl": 1.0},
                    },
                    "total": {},
                },
            )

            wallets = build_wallets(data_dir)

            self.assertEqual([row["wallet"] for row in wallets["wallets"]], ["0xloser", "0xclean"])
            self.assertEqual([row["rank"] for row in wallets["wallets"]], [1, 2])
            loser = wallets["wallets"][0]
            self.assertEqual(loser["esports_loss_count"], 0)
            self.assertEqual(loser["observed"]["signals"], 1)
            self.assertEqual(loser["observed"]["losses"], 1)
            self.assertEqual(loser["observed"]["our_pnl"], -1.0)
            self.assertTrue(loser["observed"]["has_loss"])

    def test_dashboard_wallets_count_exited_realized_loss_as_observed_loss(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            _seed_leaderboard(
                data_dir / "smart_wallet_leaderboard.json",
                [
                    {
                        "wallet": wallet,
                        "grade": "A",
                        "esports_win_count": 8,
                        "esports_loss_count": 0,
                        "positive_market_rate": 1.0,
                        "wilson_win_rate_lower_bound": 0.83,
                        "entry_edge": 0.3,
                        "esports_roi": 0.75,
                    }
                ],
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "sig-exited-loss",
                        "wallet": wallet,
                        "condition_id": "m1",
                        "status": "exited",
                        "created_at": 100,
                        "exit_at": 200,
                        "our_realized_pnl": -589,
                        "legs": [{"stake": 1000, "our_entry_price": 0.5, "would_follow": True}],
                    }
                ],
                performance={"wallets": {}, "total": {}},
            )

            row = build_wallets(data_dir)["wallets"][0]

            self.assertEqual(row["observed"]["signals"], 1)
            self.assertEqual(row["observed"]["wins"], 0)
            self.assertEqual(row["observed"]["losses"], 1)
            self.assertEqual(row["observed"]["exits"], 1)
            self.assertEqual(row["observed"]["our_pnl"], -589)
            self.assertEqual(row["observed"]["win_rate"], 0)

    def test_dashboard_events_marks_outcome_index_zero_contested(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "title": "A vs B",
                            "match_start_time": now_ts + 3600,
                        }
                    ],
                },
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s0",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "m1",
                        "outcome_index": 0,
                        "status": "open",
                        "legs": [],
                    },
                    {
                        "signal_id": "s1",
                        "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "condition_id": "m1",
                        "outcome_index": 1,
                        "status": "open",
                        "legs": [],
                    },
                ],
                result_events=[],
                performance={},
            )

            events = build_events(data_dir)

            self.assertTrue(events["events"][0]["contested"])
            self.assertEqual(events["events"][0]["side_counts"], {"0": 1, "1": 1})

    def test_dashboard_events_sort_by_start_time_ascending(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "late",
                            "title": "Late",
                            "match_start_time": datetime.fromtimestamp(now_ts + 7200, timezone.utc).isoformat(),
                        },
                        {
                            "condition_id": "early",
                            "title": "Early",
                            "match_start_time": datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat(),
                        },
                    ],
                },
            )

            events = build_events(data_dir)

            self.assertEqual([row["condition_id"] for row in events["events"]], ["early", "late"])

    def test_dashboard_events_group_submarkets_by_event_identity(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            start = datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat()
            end = datetime.fromtimestamp(now_ts + 7200, timezone.utc).isoformat()
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "main",
                            "event_id": "event-1",
                            "event_slug": "cs2-a-b",
                            "title": "Counter-Strike: A vs B (BO3) - Cup",
                            "question": "Counter-Strike: A vs B (BO3) - Cup",
                            "match_start_time": start,
                            "end_date": end,
                            "market_type": "main_match",
                            "market_type_label": "主盘",
                            "outcomes": ["A", "B"],
                        },
                        {
                            "condition_id": "map2",
                            "event_id": "event-1",
                            "event_slug": "cs2-a-b",
                            "title": "Counter-Strike: A vs B (BO3) - Cup",
                            "question": "Counter-Strike: A vs B - Map 2 Winner",
                            "match_start_time": start,
                            "end_date": end,
                            "market_type": "map_winner",
                            "market_type_label": "地图",
                            "outcomes": ["A", "B"],
                        },
                        {
                            "condition_id": "map1",
                            "event_id": "event-1",
                            "event_slug": "cs2-a-b",
                            "title": "Counter-Strike: A vs B (BO3) - Cup",
                            "question": "Counter-Strike: A vs B - Map 1 Winner",
                            "match_start_time": start,
                            "end_date": end,
                            "market_type": "map_winner",
                            "market_type_label": "地图",
                            "outcomes": ["A", "B"],
                        },
                        {
                            "condition_id": "other",
                            "event_id": "event-2",
                            "event_slug": "lol-c-d",
                            "title": "LoL: C vs D (BO3) - Cup",
                            "match_start_time": start,
                            "end_date": end,
                            "market_type": "main_match",
                            "market_type_label": "主盘",
                        },
                    ],
                },
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s-map",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "map1",
                        "outcome_index": 0,
                        "status": "open",
                        "legs": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            events = build_events(data_dir)

            self.assertEqual(events["count"], 2)
            grouped = events["events"][0]
            self.assertEqual(grouped["condition_id"], "main")
            self.assertEqual(grouped["market_count"], 3)
            self.assertEqual(grouped["market_types"], ["main_match", "map_winner"])
            self.assertEqual(grouped["market_type_label"], "3盘口")
            # 逐子盘明细(market_breakdown / condition_ids)已删(前端不展示);
            # 用事件级聚合验证分组:3 子盘合一,map1 的 1 笔信号汇总到事件级。
            self.assertEqual(grouped["open_signal_count"], 1)
            self.assertEqual(grouped["signal_count"], 1)
            self.assertEqual(grouped["side_counts"], {"0": 1})

    def test_dashboard_events_return_counts_not_raw_follow_payloads(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "title": "Counter-Strike: A vs B (BO3) - Cup",
                            "question": "Counter-Strike: A vs B (BO3) - Cup",
                            "match_start_time": datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat(),
                            "end_date": datetime.fromtimestamp(now_ts + 7200, timezone.utc).isoformat(),
                            "outcomes": ["A", "B"],
                            "market_type": "main_match",
                            "market_type_label": "主盘",
                        }
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s-open",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "m1",
                        "outcome": "A",
                        "status": "open",
                        "created_at": now_ts,
                        "debug_blob": "open" * 500,
                        "legs": [{"stake": 1, "debug_blob": "leg" * 500}],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "s-settled",
                        "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "condition_id": "m1",
                        "outcome": "B",
                        "status": "settled",
                        "outcome_won": False,
                        "settled_at": now_ts + 10,
                        "debug_blob": "result" * 500,
                        "legs": [{"stake": 1, "debug_blob": "leg" * 500}],
                    }
                ],
                performance={},
            )

            event = build_events(data_dir)["events"][0]

            self.assertEqual(event["open_signal_count"], 1)
            self.assertEqual(event["result_count"], 1)
            self.assertEqual(event["signal_count"], 2)
            self.assertEqual(event["side_counts"], {"A": 1, "B": 1})
            self.assertNotIn("open_signals", event)
            self.assertNotIn("results", event)

    def test_dashboard_events_expose_polymarket_event_urls(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "active",
                            "event_slug": "counter-strike-a-vs-b",
                            "title": "Counter-Strike: A vs B",
                            "match_start_time": datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat(),
                            "end_date": datetime.fromtimestamp(now_ts + 7200, timezone.utc).isoformat(),
                        },
                        {
                            "condition_id": "settled",
                            "event_slug": "lol-c-vs-d",
                            "title": "LoL: C vs D",
                            "match_start_time": datetime.fromtimestamp(now_ts - 7200, timezone.utc).isoformat(),
                            "end_date": datetime.fromtimestamp(now_ts - 3600, timezone.utc).isoformat(),
                        },
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "s-settled",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "settled",
                        "event_slug": "lol-c-vs-d",
                        "status": "settled",
                        "created_at": now_ts - 7000,
                        "settled_at": now_ts - 3000,
                        "legs": [],
                    }
                ],
                performance={},
            )

            events = build_events(data_dir)

            self.assertEqual(events["events"][0]["event_url"], "https://polymarket.com/event/counter-strike-a-vs-b")
            self.assertEqual(events["archived_events"][0]["event_url"], "https://polymarket.com/event/lol-c-vs-d")

    def test_dashboard_events_sort_followed_markets_first(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "unfollowed_early",
                            "title": "Unfollowed Early",
                            "match_start_time": datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat(),
                        },
                        {
                            "condition_id": "followed_late",
                            "title": "Followed Late",
                            "match_start_time": datetime.fromtimestamp(now_ts + 7200, timezone.utc).isoformat(),
                        },
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s0",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "followed_late",
                        "outcome_index": 0,
                        "status": "open",
                        "legs": [],
                    }
                ],
                result_events=[],
                performance={},
            )

            events = build_events(data_dir)

            self.assertEqual([row["condition_id"] for row in events["events"]], ["followed_late", "unfollowed_early"])

    def test_dashboard_events_exclude_result_only_past_markets(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "unfollowed_future",
                            "title": "Unfollowed Future",
                            "match_start_time": datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat(),
                        },
                        {
                            "condition_id": "settled_past",
                            "title": "Settled Past",
                            "match_start_time": datetime.fromtimestamp(now_ts - 3600, timezone.utc).isoformat(),
                        },
                    ],
                },
            )
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[
                    {
                        "signal_id": "s-settled",
                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "condition_id": "settled_past",
                        "outcome": "A",
                        "status": "settled",
                        "created_at": now_ts - 4000,
                        "settled_at": now_ts - 30,
                        "legs": [],
                    },
                    {
                        "signal_id": "s-exited",
                        "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "condition_id": "result_only",
                        "event_title": "Result Only",
                        "market_question": "Result Only",
                        "match_start_time": datetime.fromtimestamp(now_ts - 1800, timezone.utc).isoformat(),
                        "outcome": "B",
                        "status": "exited",
                        "created_at": now_ts - 3000,
                        "exit_at": now_ts - 20,
                        "legs": [],
                    },
                ],
                performance={},
            )

            events = build_events(data_dir)

            self.assertEqual([row["condition_id"] for row in events["events"]], ["unfollowed_future"])

    def test_dashboard_events_include_recently_started_markets(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            _seed_active_market_cache(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "recent",
                            "title": "Recent",
                            "match_start_time": datetime.fromtimestamp(now_ts - 120, timezone.utc).isoformat(),
                        },
                        {
                            "condition_id": "old",
                            "title": "Old",
                            "match_start_time": datetime.fromtimestamp(now_ts - 3600, timezone.utc).isoformat(),
                        },
                    ],
                },
            )

            events = build_events(data_dir, post_start_grace_seconds=300)

            self.assertEqual([row["condition_id"] for row in events["events"]], ["recent"])

    def test_dashboard_wallet_follow_detail_filters_by_status(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s-open",
                        "wallet": wallet,
                        "condition_id": "m-open",
                        "event_title": "Open Match",
                        "outcome": "A",
                        "status": "open",
                        "created_at": now_ts - 300,
                        "legs": [{"leg_at": now_ts - 300, "stake": 1, "our_entry_price": 0.4}],
                    }
                ],
                result_events=[
                    {
                        "signal_id": "s-exit-1",
                        "wallet": wallet,
                        "condition_id": "m-exit-1",
                        "event_title": "Exit One",
                        "outcome": "B",
                        "status": "exited",
                        "created_at": now_ts - 500,
                        "exit_at": now_ts - 100,
                        "exit_price": 0.62,
                        "legs": [{"leg_at": now_ts - 500, "stake": 1, "our_entry_price": 0.5}],
                    },
                    {
                        "signal_id": "s-exit-2",
                        "wallet": wallet,
                        "condition_id": "m-exit-2",
                        "event_title": "Exit Two",
                        "outcome": "C",
                        "status": "exited",
                        "created_at": now_ts - 700,
                        "exit_at": now_ts - 50,
                        "exit_price": 0.2,
                        "legs": [{"leg_at": now_ts - 700, "stake": 2, "our_entry_price": 0.3}],
                    },
                    {
                        "signal_id": "s-settled",
                        "wallet": wallet,
                        "condition_id": "m-settled",
                        "event_title": "Settled Match",
                        "outcome": "D",
                        "status": "settled",
                        "created_at": now_ts - 900,
                        "settled_at": now_ts - 80,
                        "outcome_won": True,
                        "legs": [{"leg_at": now_ts - 900, "stake": 1, "our_entry_price": 0.6}],
                    },
                ],
                performance={},
            )

            detail = build_wallet_follow_detail(data_dir, wallet, status="exited")

            self.assertEqual(detail["count"], 2)
            self.assertEqual([row["signal_id"] for row in detail["signals"]], ["s-exit-2", "s-exit-1"])
            self.assertTrue(all(row["status"] == "exited" for row in detail["signals"]))

    def test_dashboard_wallet_trades_rejects_invalid_addr_without_client_call(self):
        with TemporaryDirectory() as tmp:
            calls = []

            class FakeClient:
                def trades_for_user(self, *_args, **_kwargs):
                    calls.append("called")
                    return []

            server = create_server(
                DashboardConfig(
                    data_dir=Path(tmp),
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    client=FakeClient(),
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/api/wallets/not-an-address/trades", headers={"Cookie": f"poly_fight_session={token}"})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                self.assertEqual(response.status, 400)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"], "invalid_wallet")
                self.assertEqual(calls, [])
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_dashboard_wallet_follows_api_uses_flat_query_route(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {
                        "signal_id": "s-open",
                        "wallet": wallet,
                        "condition_id": "m-open",
                        "event_title": "Open Match",
                        "outcome": "A",
                        "status": "open",
                        "created_at": 100,
                        "legs": [{"leg_at": 100, "stake": 1, "our_entry_price": 0.4}],
                    }
                ],
                result_events=[],
                performance={},
            )
            server = create_server(
                DashboardConfig(
                    data_dir=data_dir,
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                token = make_session_token("admin", "secret", now=int(datetime.now(timezone.utc).timestamp()))
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "GET",
                    f"/api/wallet-follows?wallet={wallet}&status=open",
                    headers={"Cookie": f"poly_fight_session={token}"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

            self.assertEqual(response.status, 200)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["count"], 1)
            self.assertEqual(payload["data"]["signals"][0]["signal_id"], "s-open")

    def test_dashboard_static_serving_blocks_path_traversal(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            static_dir = root / "static"
            static_dir.mkdir()
            (static_dir / "index.html").write_text("ok", encoding="utf-8")
            (root / "secret.txt").write_text("secret", encoding="utf-8")
            server = create_server(
                DashboardConfig(
                    data_dir=root / "data",
                    host="127.0.0.1",
                    port=0,
                    username="admin",
                    password="pw",
                    cookie_secret="secret",
                    static_dir=static_dir,
                )
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request("GET", "/../secret.txt")
                response = conn.getresponse()
                response.read()
                self.assertEqual(response.status, 404)
                conn.close()
            finally:
                server.shutdown()
                server.server_close()

    def test_follow_settle_and_aggregate_dual_basis_pnl(self):
        self.assertEqual(paper_pnl(0.5, True, 100), 100)
        self.assertEqual(paper_pnl(0.5, False, 100), -100)
        signals = [
            {
                "signal_id": "m1:0",
                "condition_id": "m1",
                "outcome_index": 0,
                "wallet": "0xA",
                "our_entry_price": 0.6,
                "legs": [
                    {"stake": 100, "wallet_fill_price": 0.5, "our_entry_price": 0.6}
                ],
            }
        ]

        remaining, settled = settle_open_signals(signals, {"m1": 0}, now_ts=200)
        perf = aggregate_follow_performance({}, settled)

        self.assertEqual(remaining, [])
        self.assertEqual(settled[0]["wallet_paper_pnl_by_wallet"]["0xa"], 100)
        self.assertEqual(round(settled[0]["our_paper_pnl"], 8), 66.66666667)
        self.assertEqual(perf["wallets"]["0xa"]["signals"], 1)

    def test_follow_aggregate_includes_mirror_exits(self):
        exited = {
            "status": "exited",
            "wallet": "0xA",
            "our_realized_pnl": -0.2,
            "legs": [
                {"stake": 1, "wallet_fill_price": 0.5, "our_entry_price": 0.55},
                {"stake": 1, "wallet_fill_price": 0.52, "our_entry_price": 0.56},
            ],
        }

        perf = aggregate_follow_performance({}, [exited])

        self.assertEqual(perf["wallets"]["0xa"]["exits"], 1)
        self.assertEqual(perf["wallets"]["0xa"]["legs"], 2)
        self.assertEqual(perf["wallets"]["0xa"]["our_pnl"], -0.2)
        self.assertEqual(perf["total"]["exits"], 1)
        self.assertEqual(perf["total"]["our_pnl"], -0.2)
        self.assertEqual(perf["total"]["signals"], 0)

    def test_normalize_wallet_lowercases_keys(self):
        self.assertEqual(normalize_wallet("0xAbC"), "0xabc")

    def test_collection_diagnostics_summarizes_esports_funnel(self):
        now = 1_000_000
        discovery_slate = [
            {
                "condition_id": "m1",
                "market_type": "main_match",
                "volume": 1000,
                "end_date": datetime.fromtimestamp(now - 86400, timezone.utc).isoformat(),
            },
            {
                "condition_id": "m2",
                "market_type": "main_match",
                "volume": 500,
                "end_date": datetime.fromtimestamp(now - 3 * 86400, timezone.utc).isoformat(),
            },
            {"condition_id": "g1", "market_type": "game_winner", "volume": 800},
            {"condition_id": "p1", "market_type": "map_winner", "volume": 300},
        ]
        candidates = [
            {
                "wallet": "0xaaa",
                "per_type_candidate": {
                    "main_match": {"participated_market_count": 12, "avg_market_cash": 1700}
                },
            },
            {
                "wallet": "0xbbb",
                "per_type_candidate": {
                    "game_winner": {"participated_market_count": 12, "avg_market_cash": 900}
                },
            },
        ]
        profile_candidates = [
            {
                **candidates[0],
                "qualified_market_types": ["main_match"],
            }
        ]
        profiles_by_wallet = {
            "0xaaa": {
                "wallet": "0xaaa",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "last_esports_trade_at": now,
                "eligible_market_types": ["main_match"],
                "candidate": profile_candidates[0],
            },
            "0xbbb": {
                "wallet": "0xbbb",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "last_esports_trade_at": now - 10 * 86400,
                "eligible_market_types": ["game_winner"],
                "per_type_grades": {
                    "game_winner": {
                        "grade": "A",
                        "esports_roi": 0.40,
                        "capital_weighted_edge": 0.12,
                        "positive_market_rate": 0.75,
                        "wilson_win_rate_lower_bound": 0.62,
                        "esports_closed_count": 12,
                    }
                },
                "candidate": candidates[1],
            },
        }
        leaderboard = [
            {
                "wallet": "0xaaa",
                "best_market_type": "main_match",
                "eligible_market_types": ["main_match"],
            }
        ]

        diagnostics = build_collection_diagnostics(
            discovery_slate=discovery_slate,
            candidates=candidates,
            profile_candidates=profile_candidates,
            profiles_by_wallet=profiles_by_wallet,
            leaderboard=leaderboard,
            now_ts=now,
            stage_timings={"wallet_profiling_seconds": 1.25},
        )

        self.assertEqual(diagnostics["market_type_slate"]["main_match"]["market_count"], 2)
        self.assertEqual(diagnostics["market_type_slate"]["main_match"]["min_volume"], 500)
        self.assertEqual(diagnostics["market_type_slate"]["main_match"]["median_volume"], 750)
        self.assertEqual(diagnostics["market_type_slate"]["main_match"]["median_days_ago"], 2)
        self.assertEqual(diagnostics["market_type_slate"]["main_match"]["sort_mode"], "volume_recency_score_70_30")
        self.assertEqual(diagnostics["candidate_funnel"]["main_match"]["candidate_wallets"], 1)
        self.assertEqual(diagnostics["candidate_funnel"]["main_match"]["qualified_profile_candidates"], 1)
        self.assertEqual(diagnostics["profile_grade_counts"]["A"], 2)
        self.assertEqual(diagnostics["eligible_market_type_counts"]["main_match"], 1)
        self.assertEqual(diagnostics["leaderboard_best_market_type_counts"]["main_match"], 1)
        self.assertIn("best_bucket_inactive_gt3d", diagnostics["leaderboard_reject_reasons"])
        self.assertEqual(diagnostics["stage_timings"]["wallet_profiling_seconds"], 1.25)

    def test_profile_budget_summary_reports_unprofiled_candidates(self):
        summary = build_profile_budget_summary(
            profile_candidate_wallet_count=267,
            profile_fetch_plan_count=150,
            max_profiles_per_run_effective=150,
        )

        self.assertEqual(summary["profile_fetch_plan_count"], 150)
        self.assertEqual(summary["unprofiled_profile_candidate_count"], 117)
        self.assertEqual(summary["max_profiles_per_run_effective"], 150)

        covered = build_profile_budget_summary(
            profile_candidate_wallet_count=267,
            profile_fetch_plan_count=267,
            max_profiles_per_run_effective=300,
        )

        self.assertEqual(covered["unprofiled_profile_candidate_count"], 0)

    def test_esports_discovery_defaults_use_balanced_type_buckets(self):
        parser = build_parser()

        esports = parser.parse_args(["collect"])
        explicit_esports = parser.parse_args(
            [
                "collect",
                "--target-markets",
                "30",
                "--game-winner-target-markets",
                "40",
                "--map-winner-target-markets",
                "20",
                "--max-markets-per-run",
                "30",
                "--game-winner-max-markets-per-run",
                "40",
                "--map-winner-max-markets-per-run",
                "20",
            ]
        )

        # valorant 纳入后桶限额求和变化:main 300→400(+valorant 100)、map 50→100(+valorant 50);
        # game_winner 不变(valorant 无 game_winner 桶);submarket 是独立常量(150)不随桶和变。
        self.assertEqual(
            effective_discovery_defaults(esports),
            {
                "target_markets": 400,
                "submarket_target_markets": 150,
                "game_winner_target_markets": 100,
                "map_winner_target_markets": 100,
                "max_markets_per_run": 400,
                "submarket_max_markets_per_run": 150,
                "game_winner_max_markets_per_run": 100,
                "map_winner_max_markets_per_run": 100,
            },
        )
        self.assertEqual(
            effective_discovery_defaults(explicit_esports),
            {
                "target_markets": 30,
                "submarket_target_markets": 150,
                "game_winner_target_markets": 40,
                "map_winner_target_markets": 20,
                "max_markets_per_run": 30,
                "submarket_max_markets_per_run": 150,
                "game_winner_max_markets_per_run": 40,
                "map_winner_max_markets_per_run": 20,
            },
        )
    def test_collect_command_accepts_build_options(self):
        parser = build_parser()

        args = parser.parse_args(["collect", "--max-profiles-per-run", "3"])

        self.assertEqual(args.command, "collect")
        self.assertEqual(args.max_profiles_per_run, 3)

    def test_rescore_command_is_not_registered(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["rescore-wallets"])

    def test_run_does_not_accept_wallet_rescore_interval(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["run", "--stake-usdc", "1", "--wallet-rescore-hours", "2"])

    def test_collect_command_accepts_batched_discovery_options(self):
        parser = build_parser()

        args = parser.parse_args(
            [
                "collect",
                "--classification-lookback-days",
                "15",
                "--market-batch-size",
                "50",
                "--market-batch-index",
                "2",
            ]
        )

        self.assertEqual(args.classification_lookback_days, 15)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_index, 2)

    def test_collect_command_rejects_removed_discovery_lookback_option(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["collect", "--discovery-lookback-days", "15"])

    def test_build_leaderboard_lock_blocks_second_holder(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)

            with acquire_build_lock(data_dir, blocking=False):
                with self.assertRaises(BuildLockUnavailable):
                    with acquire_build_lock(data_dir, blocking=False):
                        pass

    def test_market_positions_wrapper_requests_v1_endpoint(self):
        calls = []

        class Client(PolymarketClient):
            def data(self, path, **params):
                calls.append((path, params))
                return []

        Client().market_positions("0xabc", limit=20, sort_by="TOTAL_PNL", sort_direction="DESC")

        self.assertEqual(calls[0][0], "/v1/market-positions")
        self.assertEqual(calls[0][1]["market"], "0xabc")
        self.assertEqual(calls[0][1]["limit"], 20)
        self.assertEqual(calls[0][1]["sortBy"], "TOTAL_PNL")
        self.assertEqual(calls[0][1]["sortDirection"], "DESC")

    def test_select_collector_target_markets_uses_six_buckets_without_backfill(self):
        now = datetime(2026, 6, 9, tzinfo=timezone.utc)

        def row(condition_id, game_family, market_type, volume, days_ago=5):
            return {
                "condition_id": condition_id,
                "category": "esports",
                "game_family": game_family,
                "market_type": market_type,
                "volume": volume,
                "end_date": (now - timedelta(days=days_ago)).isoformat(),
                "outcome_prices": [1.0, 0.0],
            }

        classification = [
            row("lol-main-low", "lol", "main_match", 100),
            row("lol-main-high", "lol", "main_match", 300),
            row("lol-main-old", "lol", "main_match", 500, days_ago=45),
            row("lol-game", "lol", "game_winner", 200),
            row("dota-main", "dota2", "main_match", 200),
            row("dota-game", "dota2", "game_winner", 200),
            row("cs-main", "cs2", "main_match", 200),
            row("cs-map", "cs2", "map_winner", 200),
        ]

        selected, meta = select_collector_target_markets(
            classification,
            now=now,
            lookback_days=30,
            bucket_market_limit=2,
        )

        self.assertEqual(
            [market["condition_id"] for market in selected],
            [
                "lol-main-high",
                "lol-main-low",
                "lol-game",
                "dota-main",
                "dota-game",
                "cs-main",
                "cs-map",
            ],
        )
        self.assertEqual(meta["bucket_counts"]["lol:main_match"], 2)
        # valorant 现为配置桶,本用例无 valorant 市场 → 计 0(不再是"不存在")。
        self.assertEqual(meta["bucket_counts"].get("valorant:main_match", 0), 0)

    def test_select_collector_target_markets_can_take_one_hundred_per_bucket(self):
        now = datetime(2026, 6, 9, tzinfo=timezone.utc)
        classification = []
        buckets = [
            ("lol", "main_match"),
            ("lol", "game_winner"),
            ("dota2", "main_match"),
            ("dota2", "game_winner"),
            ("cs2", "main_match"),
            ("cs2", "map_winner"),
        ]
        for game_family, market_type in buckets:
            for index in range(120):
                classification.append(
                    {
                        "condition_id": f"{game_family}-{market_type}-{index}",
                        "category": "esports",
                        "game_family": game_family,
                        "market_type": market_type,
                        "volume": 10_000 - index,
                        "end_date": (now - timedelta(days=1, seconds=index)).isoformat(),
                        "outcome_prices": [1.0, 0.0],
                    }
                )

        selected, meta = select_collector_target_markets(
            classification,
            now=now,
            lookback_days=30,
            bucket_market_limit=100,
        )

        self.assertEqual(len(selected), 600)
        # valorant 现为配置桶,本用例未提供 valorant 市场 → 计 0 + 满额缺口。
        self.assertEqual(
            meta["bucket_counts"],
            {
                "lol:main_match": 100,
                "lol:game_winner": 100,
                "dota2:main_match": 100,
                "dota2:game_winner": 100,
                "cs2:main_match": 100,
                "cs2:map_winner": 100,
                "valorant:main_match": 0,
                "valorant:map_winner": 0,
            },
        )
        self.assertEqual(
            meta["bucket_shortfalls"],
            {"valorant:main_match": 100, "valorant:map_winner": 100},
        )

    def test_seed_bucket_min_wins_uses_ten_percent_without_floor(self):
        self.assertEqual(
            calculate_seed_bucket_min_wins(
                {
                    "lol:main_match": 50,
                    "lol:game_winner": 75,
                    "dota2:main_match": 100,
                    "dota2:game_winner": 30,
                }
            ),
            {
                "lol:main_match": 5,
                "lol:game_winner": 8,
                "dota2:main_match": 10,
                "dota2:game_winner": 3,
            },
        )

    def test_seed_positions_keep_only_profitable_winning_outcome(self):
        market = {
            "condition_id": "m1",
            "category": "esports",
            "game_family": "lol",
            "market_type": "main_match",
            "bucket_key": "lol:main_match",
            "outcome_prices": [1.0, 0.0],
            "end_date": "2026-06-08T00:00:00Z",
        }
        response = [
            {
                "positions": [
                    {
                        "proxyWallet": "0xAAA",
                        "outcomeIndex": 0,
                        "avgPrice": 0.6,
                        "totalBought": 100,
                        "realizedPnl": 40,
                        "totalPnl": 40,
                    },
                    {
                        "proxyWallet": "0xBBB",
                        "outcomeIndex": 0,
                        "avgPrice": 0.9,
                        "totalBought": 100,
                        "realizedPnl": 0,
                        "totalPnl": 0,
                    },
                ]
            },
            {
                "positions": [
                    {
                        "proxyWallet": "0xLOSER",
                        "outcomeIndex": 1,
                        "avgPrice": 0.2,
                        "totalBought": 100,
                        "realizedPnl": 200,
                        "totalPnl": 200,
                    }
                ]
            },
        ]

        rows = collect_seed_positions(market, response, positions_per_market=20)

        self.assertEqual([row["wallet"] for row in rows], ["0xaaa"])
        self.assertEqual(rows[0]["seed_cost"], 60)
        self.assertAlmostEqual(rows[0]["seed_roi"], 40 / 60)
        self.assertEqual(rows[0]["seed_edge"], 0.4)
        self.assertEqual(rows[0]["seed_rank"], 1)
        self.assertTrue(rows[0]["seed_outcome_won"])

    def test_seed_positions_dual_side_captures_profitable_loser(self):
        # v2 collect-v2: include_losing_side 同时采集负方盈利钱包(technical 型),
        # 但仍只保留盈利持仓(0xBBB pnl=0 被排除),且双侧各自排名。
        market = {
            "condition_id": "m1",
            "category": "esports",
            "game_family": "lol",
            "market_type": "main_match",
            "bucket_key": "lol:main_match",
            "outcome_prices": [1.0, 0.0],  # index 0 wins
            "end_date": "2026-06-08T00:00:00Z",
        }
        response = [
            {
                "positions": [
                    {"proxyWallet": "0xAAA", "outcomeIndex": 0, "avgPrice": 0.6,
                     "totalBought": 100, "realizedPnl": 40, "totalPnl": 40},
                    {"proxyWallet": "0xBBB", "outcomeIndex": 0, "avgPrice": 0.9,
                     "totalBought": 100, "realizedPnl": 0, "totalPnl": 0},
                ]
            },
            {
                "positions": [
                    {"proxyWallet": "0xLOSER", "outcomeIndex": 1, "avgPrice": 0.2,
                     "totalBought": 100, "realizedPnl": 200, "totalPnl": 200},
                ]
            },
        ]

        rows = collect_seed_positions(market, response, positions_per_market=20, include_losing_side=True)
        by_wallet = {row["wallet"]: row for row in rows}

        self.assertEqual(set(by_wallet), {"0xaaa", "0xloser"})
        self.assertTrue(by_wallet["0xaaa"]["seed_outcome_won"])
        self.assertFalse(by_wallet["0xloser"]["seed_outcome_won"])
        # 各侧独立排名 → 每侧 rank 从 1 开始
        self.assertEqual(by_wallet["0xaaa"]["seed_rank"], 1)
        self.assertEqual(by_wallet["0xloser"]["seed_rank"], 1)

    def test_seed_positions_dual_side_caps_each_side_independently(self):
        # 双侧各按 positions_per_market 截断,胜方占满不会把负方挤掉。
        market = {
            "condition_id": "m1", "game_family": "lol", "market_type": "main_match",
            "outcome_prices": [1.0, 0.0], "end_date": "2026-06-08T00:00:00Z",
        }
        winners = [
            {"proxyWallet": f"0xW{i}", "outcomeIndex": 0, "avgPrice": 0.5,
             "totalBought": 100, "realizedPnl": 50, "totalPnl": 50}
            for i in range(3)
        ]
        losers = [
            {"proxyWallet": f"0xL{i}", "outcomeIndex": 1, "avgPrice": 0.2,
             "totalBought": 100, "realizedPnl": 30, "totalPnl": 30}
            for i in range(3)
        ]
        rows = collect_seed_positions(
            market, [{"positions": winners}, {"positions": losers}],
            positions_per_market=2, include_losing_side=True,
        )
        kept = {row["wallet"] for row in rows}
        self.assertEqual(len([r for r in rows if r["seed_outcome_won"]]), 2)
        self.assertEqual(len([r for r in rows if not r["seed_outcome_won"]]), 2)
        self.assertEqual(kept, {"0xw0", "0xw1", "0xl0", "0xl1"})

    def test_live_seed_positions_winner_free_dual_side_no_profit_gate(self):
        # 未结算盘:无 winner,双侧全取,亏损者也保留(软排序非硬闸),按名义额排序每侧 top-K。
        market = {
            "condition_id": "m1", "game_family": "lol", "market_type": "main_match",
            "bucket_key": "lol:main_match", "volume": 8000,
            # 注意:无 outcome_prices winner、无 end_date —— live 盘特征
        }
        side0 = [
            {"proxyWallet": "0xBIG", "outcomeIndex": 0, "avgPrice": 0.6,
             "totalBought": 1000, "totalPnl": 50},      # 名义额 600,ITM
            {"proxyWallet": "0xSMALL", "outcomeIndex": 0, "avgPrice": 0.5,
             "totalBought": 100, "totalPnl": -20},      # 名义额 50,亏损也保留
        ]
        side1 = [
            {"proxyWallet": "0xLOSS", "outcomeIndex": 1, "avgPrice": 0.4,
             "totalBought": 500, "totalPnl": -80},      # 负方 + 亏损,仍保留
        ]
        rows = collect_live_seed_positions(
            market, [{"positions": side0}, {"positions": side1}], positions_per_market=20,
        )
        by_wallet = {r["wallet"]: r for r in rows}
        # 亏损者 0xsmall / 0xloss 都没被盈亏门筛掉
        self.assertEqual(set(by_wallet), {"0xbig", "0xsmall", "0xloss"})
        self.assertIsNone(by_wallet["0xbig"]["seed_outcome_won"])   # 未结算
        self.assertTrue(by_wallet["0xbig"]["seed_in_profit"])
        self.assertFalse(by_wallet["0xsmall"]["seed_in_profit"])
        # 同侧按名义额排序:0xbig(600) 在 0xsmall(50) 前
        self.assertEqual(by_wallet["0xbig"]["seed_rank"], 1)
        self.assertEqual(by_wallet["0xsmall"]["seed_rank"], 2)
        self.assertEqual(by_wallet["0xbig"]["seed_cost"], 600)

    def test_live_seed_positions_caps_each_side_by_notional(self):
        market = {"condition_id": "m1", "game_family": "lol", "market_type": "main_match", "volume": 9000}
        side0 = [{"proxyWallet": f"0xA{i}", "outcomeIndex": 0, "avgPrice": 0.5,
                  "totalBought": (i + 1) * 100, "totalPnl": 10} for i in range(3)]
        rows = collect_live_seed_positions(market, [{"positions": side0}], positions_per_market=2)
        # 取名义额最大的两个:0xa2(150*... 即 totalBought 300)与 0xa1(200)
        self.assertEqual([r["wallet"] for r in rows], ["0xa2", "0xa1"])

    def test_classify_edge_type(self):
        # directional: 持有到结算也盈利,且 swing 占比低
        self.assertEqual(
            classify_edge_type({"esports_closed_count": 8, "actual_pnl": 1000,
                                "hold_pnl": 950, "actual_minus_hold_pnl_rate": 0.05}),
            "directional",
        )
        # technical: hold_pnl<=0 但实际盈利 → 纯出场时机(附录 A 的 0x594d 型)
        self.assertEqual(
            classify_edge_type({"esports_closed_count": 6, "actual_pnl": 16868,
                                "hold_pnl": -14482, "actual_minus_hold_pnl_rate": None}),
            "technical",
        )
        # technical: swing 占比超阈值
        self.assertEqual(
            classify_edge_type({"esports_closed_count": 8, "actual_pnl": 1000,
                                "hold_pnl": 400, "actual_minus_hold_pnl_rate": 1.5}),
            "technical",
        )
        # unknown: 无样本无盈亏
        self.assertEqual(
            classify_edge_type({"esports_closed_count": 0, "actual_pnl": 0, "hold_pnl": 0}),
            "unknown",
        )

    def _v2_profile(self, wallet, game, *, roi=0.25, pnl=2000.0, wilson=0.62,
                    positive=0.78, two_sided=0.02, edge_type="directional",
                    now=1_700_000_000, market_type="main_match", grade="A"):
        hold = pnl if edge_type != "technical" else -abs(pnl)
        rate = 0.0 if edge_type != "technical" else None
        metrics = {
            "esports_closed_count": 8,
            "median_market_roi": roi,
            "esports_roi": roi,
            "esports_realized_pnl": pnl,
            "positive_market_rate": positive,
            "median_entry_price": 0.45,
            "wilson_win_rate_lower_bound": wilson,
            "esports_total_cost": 8 * 1500.0,
            "last_esports_trade_at": now - 86400,
            "recent_14d_market_count": 0,
            "hold_pnl": hold,
            "actual_pnl": pnl,
            "actual_minus_hold_pnl_rate": rate,
        }
        bucket = {**metrics, "game_family": game, "market_type": market_type}
        # 新评分逐桶字段:builder 现在读 per_game_type_grades 的 grade=="A" + 新轴排序。
        # 测试用 wilson 当"强度"旋钮 → 映射成 bucket_win_rate;eff_sample 固定够格;copy_edge=胜率−入场价。
        graded = {
            **bucket,
            "grade": grade,
            "bucket_win_rate": wilson,
            "bucket_eff_sample": 12.0,
            "bucket_copy_edge": round(wilson - 0.45, 6),
        }
        return {
            "wallet": wallet,
            "grade": grade,   # 钱包级等级(榜单只发 grade-A)
            **metrics,  # 顶层冗余存一份(builder 实际读 per_game_type_grades 桶)
            "two_sided_trade_market_rate": two_sided,
            "bot_like_score": 10,
            "edge_type": edge_type,
            "candidate": {"avg_market_cash": 1500.0, "tail_entry_market_count": 0,
                          "participated_market_count": 8},
            "per_game_type": {f"{game}:{market_type}": bucket},
            "per_game_type_grades": {f"{game}:{market_type}": graded},
        }

    def _seed_wallet(self, wallet, game, *, score, cost=2000.0, markets=4):
        return {"wallet": wallet, "seed_market_count": markets, "seed_cost_total": cost,
                "seed_game_family_counts": {game: markets}, "seed_score": score,
                "seed_win_count": markets, "seed_pnl_total": cost * 0.3}

    def test_filter_profile_seed_wallets_v2_dust_and_round_robin(self):
        sw = {
            "0xlol1": self._seed_wallet("0xlol1", "lol", score=0.9),
            "0xlol2": self._seed_wallet("0xlol2", "lol", score=0.8),
            "0xlol3": self._seed_wallet("0xlol3", "lol", score=0.7),
            "0xlol4": self._seed_wallet("0xlol4", "lol", score=0.6),
            "0xcs2a": self._seed_wallet("0xcs2a", "cs2", score=0.3),   # 低 seed_score 的低交易游戏
            "0xdust": self._seed_wallet("0xdust", "lol", score=0.95, cost=200.0, markets=4),  # 均额 50<150
        }
        out = filter_profile_seed_wallets_v2(sw, max_wallets=3, min_avg_seed_cash=150.0)
        wallets = [r["wallet"] for r in out]
        self.assertEqual(len(out), 3)
        self.assertNotIn("0xdust", wallets)        # dust 被去掉(即使 seed_score 最高)
        # round-robin:cs2 唯一钱包即使 seed_score 全局垫底也拿到 profiling 名额
        # (全局 top-3 会是 3 个 lol、把 cs2 挤掉)。从源头解决偏科。
        self.assertIn("0xcs2a", wallets)

    def test_v2_leaderboard_per_game_quota_prevents_domination(self):
        now = 1_700_000_000
        profiles = {
            "0xlol1": self._v2_profile("0xlol1", "lol", wilson=0.80, now=now),
            "0xlol2": self._v2_profile("0xlol2", "lol", wilson=0.75, now=now),
            "0xcs2a": self._v2_profile("0xcs2a", "cs2", wilson=0.60, edge_type="technical", now=now),
            "0xdota": self._v2_profile("0xdota", "dota2", wilson=0.58, now=now),
            "0xbad": self._v2_profile("0xbad", "lol", roi=0.01, now=now),  # 残渣,被门拦
        }
        # include_technical=True 才纳入技术型(默认关闭,见下个测试)
        out = build_collector_leaderboard_v2(profiles, now_ts=now, per_game_quota=1, include_technical=True)
        games = sorted(row["primary_game"] for row in out["leaderboard"])
        # 每游戏配额=1:lol 只留最强的 0xlol1,cs2/dota 各保底 1 名 → 偏科被打破
        self.assertEqual(games, ["cs2", "dota2", "lol"])
        kept = {row["wallet"] for row in out["leaderboard"]}
        self.assertIn("0xlol1", kept)
        self.assertNotIn("0xlol2", kept)   # 同游戏次优被配额挤出
        self.assertNotIn("0xbad", kept)    # 残渣被门拦
        self.assertEqual(out["edge_type_counts"].get("technical"), 1)

    def test_v2_excludes_technical_by_default(self):
        now = 1_700_000_000
        profiles = {
            "0xdir": self._v2_profile("0xdir", "lol", wilson=0.70, now=now),                       # 单向
            "0xtech": self._v2_profile("0xtech", "cs2", wilson=0.70, edge_type="technical", now=now),  # 技术型
        }
        # 默认:技术型被排除
        out = build_collector_leaderboard_v2(profiles, now_ts=now)
        kept = {row["wallet"] for row in out["leaderboard"]}
        self.assertEqual(kept, {"0xdir"})
        self.assertNotIn("technical", out["edge_type_counts"])
        self.assertEqual(out["rejected_counts"].get("technical_excluded"), 1)
        # 一键开回:--v2-include-technical
        out2 = build_collector_leaderboard_v2(profiles, now_ts=now, include_technical=True)
        self.assertEqual({row["wallet"] for row in out2["leaderboard"]}, {"0xdir", "0xtech"})

    def test_v2_leaderboard_excludes_wallets_idle_over_limit(self):
        now = 1_700_000_000
        fresh = self._v2_profile("0xfresh", "lol", wilson=0.70, now=now)   # last trade 24h ago
        # 低频但 14d 内(8 天前):72h 旧门会误杀,336h 新门保留 → 应上榜。
        infrequent = self._v2_profile("0xinfreq", "lol", wilson=0.70, now=now)
        infrequent["last_esports_trade_at"] = now - 8 * 24 * 3600           # 8d ago < 14d
        stale = self._v2_profile("0xstale", "lol", wilson=0.70, now=now)
        stale["last_esports_trade_at"] = now - 20 * 24 * 3600               # 20d ago > 14d
        out = build_collector_leaderboard_v2(
            {"0xfresh": fresh, "0xinfreq": infrequent, "0xstale": stale}, now_ts=now)
        self.assertEqual({row["wallet"] for row in out["leaderboard"]}, {"0xfresh", "0xinfreq"})
        self.assertEqual(out["rejected_counts"].get("idle_over_limit"), 1)  # 只砍 20d 的
        # 一笔交易记录都没有 → 同样排除
        no_trade = self._v2_profile("0xnone", "lol", wilson=0.70, now=now)
        no_trade["last_esports_trade_at"] = 0
        out_n = build_collector_leaderboard_v2({"0xnone": no_trade}, now_ts=now)
        self.assertEqual(out_n["leaderboard"], [])
        # 可配置:放宽阈值 / 关闭(0)后沉寂钱包可入榜
        out_loose = build_collector_leaderboard_v2({"0xstale": stale}, now_ts=now, gate_kwargs={"max_idle_hours": 30 * 24})
        self.assertEqual({row["wallet"] for row in out_loose["leaderboard"]}, {"0xstale"})
        out_off = build_collector_leaderboard_v2({"0xstale": stale}, now_ts=now, gate_kwargs={"max_idle_hours": 0})
        self.assertEqual({row["wallet"] for row in out_off["leaderboard"]}, {"0xstale"})

    def test_v2_specialist_qualifies_via_strong_bucket(self):
        # 专精评估:钱包整体平庸(lol 主赛很差),但在 cs2 地图胜负上很强 →
        # 应凭专精桶入榜,eligible_buckets 只含该桶,不被 lol 拖累。
        now = 1_700_000_000
        strong = {"game_family": "cs2", "market_type": "map_winner", "esports_closed_count": 5,
                  "median_market_roi": 0.40, "esports_roi": 0.40, "esports_realized_pnl": 3000.0,
                  "positive_market_rate": 0.80, "median_entry_price": 0.30,
                  "wilson_win_rate_lower_bound": 0.62, "esports_total_cost": 5 * 1200.0,
                  "last_esports_trade_at": now - 86400, "recent_14d_market_count": 0,
                  "hold_pnl": 3000.0, "actual_pnl": 3000.0, "actual_minus_hold_pnl_rate": 0.0}
        weak = {"game_family": "lol", "market_type": "main_match", "esports_closed_count": 6,
                "median_market_roi": 0.02, "esports_roi": 0.02, "esports_realized_pnl": 50.0,
                "positive_market_rate": 0.40, "median_entry_price": 0.50,
                "wilson_win_rate_lower_bound": 0.30, "esports_total_cost": 6 * 1200.0,
                "last_esports_trade_at": now - 86400, "recent_14d_market_count": 0,
                "hold_pnl": 50.0, "actual_pnl": 50.0, "actual_minus_hold_pnl_rate": 0.0}
        profile = {"wallet": "0xspec", "grade": "A", "two_sided_trade_market_rate": 0.05, "bot_like_score": 10,
                   "last_esports_trade_at": now - 86400,
                   "candidate": {"tail_entry_market_count": 0, "participated_market_count": 11},
                   "per_game_type": {"cs2:map_winner": strong, "lol:main_match": weak},
                   "per_game_type_grades": {
                       "cs2:map_winner": {**strong, "grade": "A", "bucket_win_rate": 0.80,
                                          "bucket_eff_sample": 12.0, "bucket_copy_edge": 0.50},
                       "lol:main_match": {**weak, "grade": "C", "bucket_win_rate": 0.40,
                                          "bucket_eff_sample": 12.0, "bucket_copy_edge": -0.10},
                   }}
        out = build_collector_leaderboard_v2({"0xspec": profile}, now_ts=now)
        self.assertEqual(out["qualified_count"], 1)
        row = out["leaderboard"][0]
        self.assertEqual(row["eligible_buckets"], ["cs2:map_winner"])  # 只在专精盘口够格
        self.assertEqual(row["primary_game"], "cs2")
        self.assertEqual(row["best_market_type"], "map_winner")

    def test_v2_leaderboard_publishes_grade_a_only(self):
        # 榜单只发 grade-A:B 档即使有合格桶也不上榜(但不影响 profiles 池留存)。
        now = 1_700_000_000
        profiles = {
            "0xA": self._v2_profile("0xA", "lol", wilson=0.70, now=now, grade="A"),
            "0xB": self._v2_profile("0xB", "cs2", wilson=0.70, now=now, grade="B"),
        }
        out = build_collector_leaderboard_v2(profiles, now_ts=now)
        kept = {row["wallet"] for row in out["leaderboard"]}
        self.assertEqual(kept, {"0xa"})                                  # 只 A 上榜
        self.assertEqual(out["rejected_counts"].get("grade_below_floor"), 1)
        # min_grade 可放宽:显式允许 B 时,两者都上榜
        out_b = build_collector_leaderboard_v2(profiles, now_ts=now, min_grade="B")
        self.assertEqual({row["wallet"] for row in out_b["leaderboard"]}, {"0xa", "0xb"})

    def test_observe_analyzed_sqlite_store_and_prune(self):
        with TemporaryDirectory() as d:
            store = storage_module.LeaderboardStore(Path(d) / "leaderboard_v2.db")
            now = 1_700_000_000
            store.record_observe_analyzed(["0xAA", "0xBB"], now_ts=now)
            store.record_observe_analyzed(["0xCC"], now_ts=now - 10 * 86400)  # 10天前 → 剪枝
            got = store.load_observe_analyzed(now_ts=now, retain_days=7)
            self.assertEqual(set(got), {"0xaa", "0xbb"})   # 去重小写 + 过期剪掉
            self.assertNotIn("0xcc", got)
            # 不存在的 db → 空(不抛)
            self.assertEqual(storage_module.LeaderboardStore(Path(d) / "nope.db").load_observe_analyzed(now_ts=now), {})

    def _setup_rescore_demote_case(self, root, *, now, favorite=None):
        """Seed leaderboard_v2.db + 打分池 profiles + 0xdrop 原始交易缓存,返回常用路径。"""
        import poly_fight.storage as storage_module
        esports_dir = root / "esports"
        follow_dir = root / "follow"
        collector_dir = esports_dir / "collector_v2"
        collector_dir.mkdir(parents=True, exist_ok=True)
        storage_module.LeaderboardStore(esports_dir / "leaderboard_v2.db").replace_leaderboard(
            [{"wallet": "0xdrop", "grade": "A"}, {"wallet": "0xkeep", "grade": "A"}],
            category="esports", updated_at=now,
        )
        (collector_dir / "collector_v2_wallet_profiles.json").write_text(
            json.dumps([{"wallet": "0xdrop", "grade": "A"}, {"wallet": "0xkeep", "grade": "A"}]))
        from poly_fight.cli import user_trades_cache_path
        drop_cache = user_trades_cache_path(collector_dir, "0xdrop")
        drop_cache.parent.mkdir(parents=True, exist_ok=True)
        drop_cache.write_text("[]")
        if favorite:
            FollowStore(follow_dir / "follow.db").upsert_wallet_favorite(favorite, category="esports", ts=now)
        return esports_dir, follow_dir, collector_dir, drop_cache

    def test_rescore_demote_wallets_deletes_off_board_only(self):
        from unittest.mock import patch
        from poly_fight.cli import rescore_demote_wallets, normalize_wallet
        import poly_fight.storage as storage_module
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = 1_700_000_000
            esports_dir, follow_dir, collector_dir, drop_cache = self._setup_rescore_demote_case(root, now=now)

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return []  # empty scope -> profiling is cheap and offline

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(esports_dir), "run", "--stake-usdc", "1"])

            # 重评后 0xdrop 跌出 A 榜,0xkeep 留榜(决策与持久化两次 build 都走这个 fake)。
            def fake_build(profiles, **_kwargs):
                return {"leaderboard": [{"wallet": w, "grade": "A"} for w in profiles if w != "0xdrop"]}

            with patch("poly_fight.cli.build_collector_leaderboard_v2", side_effect=fake_build):
                result = rescore_demote_wallets(
                    FakeClient(), args, wallets={"0xdrop", "0xkeep", "0xnotonboard"},
                    follow_dir=follow_dir, now_ts=now,
                )

            self.assertEqual(set(result["demoted_wallets"]), {"0xdrop"})
            # 不再写 quarantine 中间态
            self.assertEqual(FollowStore(follow_dir / "follow.db").load_wallet_quarantine(category="esports"), {})
            # 直接下榜(从 leaderboard_v2.db 删除)
            board_rows, _meta = storage_module.LeaderboardStore(esports_dir / "leaderboard_v2.db").load_leaderboard(category="esports")
            board = {normalize_wallet(r.get("wallet")) for r in board_rows}
            self.assertNotIn("0xdrop", board)
            self.assertIn("0xkeep", board)
            # 打分 profile + 原始交易缓存被删
            profiles = json.loads((collector_dir / "collector_v2_wallet_profiles.json").read_text())
            self.assertEqual({normalize_wallet(p.get("wallet")) for p in profiles}, {"0xkeep"})
            self.assertFalse(drop_cache.exists())

    def test_rescore_demote_wallets_spares_favorite(self):
        from unittest.mock import patch
        from poly_fight.cli import rescore_demote_wallets, normalize_wallet
        import poly_fight.storage as storage_module
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = 1_700_000_000
            # 0xdrop 被人工置顶 → 即便跌出 A 榜也不自动删除。
            esports_dir, follow_dir, collector_dir, drop_cache = self._setup_rescore_demote_case(root, now=now, favorite="0xdrop")

            class FakeClient:
                def list_events_paginated(self, **_kwargs):
                    return []

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(esports_dir), "run", "--stake-usdc", "1"])

            def fake_build(profiles, **_kwargs):
                return {"leaderboard": [{"wallet": w, "grade": "A"} for w in profiles if w != "0xdrop"]}

            with patch("poly_fight.cli.build_collector_leaderboard_v2", side_effect=fake_build):
                result = rescore_demote_wallets(
                    FakeClient(), args, wallets={"0xdrop", "0xkeep"},
                    follow_dir=follow_dir, now_ts=now,
                )

            self.assertEqual(result["demoted_wallets"], [])      # favorite 不被淘汰
            self.assertTrue(drop_cache.exists())                 # 缓存保留
            profiles = json.loads((collector_dir / "collector_v2_wallet_profiles.json").read_text())
            self.assertEqual({normalize_wallet(p.get("wallet")) for p in profiles}, {"0xdrop", "0xkeep"})

    def test_purge_legacy_demote_quarantine_deletes_not_releases(self):
        from unittest.mock import patch
        from poly_fight.cli import purge_legacy_demote_quarantine, normalize_wallet, RESCORE_QUARANTINE_REASON
        import poly_fight.storage as storage_module
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = 1_700_000_000
            # 0xbad = 历史自动降级隔离;0xmanual = 人工隔离(必须保留);0xkeep = 正常在榜。
            esports_dir, follow_dir, collector_dir, bad_cache = self._setup_rescore_demote_case(root, now=now)
            # 复用 helper 的 0xdrop/0xkeep seed,把 0xdrop 当成 0xbad 处理。
            store = FollowStore(follow_dir / "follow.db")
            store.upsert_wallet_quarantine("0xdrop", reason=RESCORE_QUARANTINE_REASON, ts=now - 100, category="esports")
            store.upsert_wallet_quarantine("0xmanual", reason="manual_dashboard_quarantine", ts=now - 100, category="esports")

            parser = build_parser()
            args = parser.parse_args(["--data-dir", str(esports_dir), "run", "--stake-usdc", "1"])

            def fake_build(profiles, **_kwargs):
                return {"leaderboard": [{"wallet": w, "grade": "A"} for w in profiles]}

            with patch("poly_fight.cli.build_collector_leaderboard_v2", side_effect=fake_build):
                result = purge_legacy_demote_quarantine(args, follow_dir=follow_dir, now_ts=now)

            self.assertEqual(result["deleted"], 1)
            self.assertEqual(result["wallets"], ["0xdrop"])
            # 历史降级钱包:下榜 + profile/缓存删 + 隔离行清掉(不放回跟单集)
            board_rows, _meta = storage_module.LeaderboardStore(esports_dir / "leaderboard_v2.db").load_leaderboard(category="esports")
            self.assertNotIn("0xdrop", {normalize_wallet(r.get("wallet")) for r in board_rows})
            self.assertFalse(bad_cache.exists())
            profiles = json.loads((collector_dir / "collector_v2_wallet_profiles.json").read_text())
            self.assertEqual({normalize_wallet(p.get("wallet")) for p in profiles}, {"0xkeep"})
            q = store.load_wallet_quarantine(category="esports")
            qkeys = {normalize_wallet((info or {}).get("wallet") or k) for k, info in q.items()}
            self.assertNotIn("0xdrop", qkeys)        # 降级隔离行被清
            self.assertIn("0xmanual", qkeys)         # 人工隔离保留

    def test_clear_revalidated_quarantine_protects_sticky_reasons(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            now = 1_700_000_000
            store.upsert_wallet_quarantine("0xA", reason="rescore_below_grade_a", ts=now - 100, category="esports")
            store.upsert_wallet_quarantine("0xB", reason="revalidation_required", ts=now - 100, category="esports")
            store.clear_revalidated_quarantine(
                {"0xa", "0xb"}, validated_at=now,
                protected_reasons={"rescore_below_grade_a", "manual_dashboard_quarantine"},
            )
            q = store.load_wallet_quarantine(category="esports")
            self.assertIn("0xa", q)       # sticky 原因保留
            self.assertNotIn("0xb", q)    # 可复审原因被清除

    def test_detect_newly_settled_markets(self):
        from unittest.mock import patch
        from poly_fight.cli import detect_newly_settled_markets
        now = datetime(2026, 6, 14, tzinfo=timezone.utc)
        recent = (now - timedelta(hours=2)).isoformat()
        old = (now - timedelta(hours=20)).isoformat()
        markets = [
            {"condition_id": "0xNEW", "outcome_prices": [1.0, 0.0], "end_date": recent},        # 新结算 → 取
            {"condition_id": "0xSEEN", "outcome_prices": [1.0, 0.0], "end_date": recent},       # 已分析 → 跳
            {"condition_id": "0xUNRESOLVED", "outcome_prices": None, "end_date": recent},       # 未结算 → 跳
            {"condition_id": "0xOLD", "outcome_prices": [1.0, 0.0], "end_date": old},           # 超窗口 → 跳
        ]
        client = type("C", (), {"list_events_paginated": lambda self, **k: []})()
        with patch("poly_fight.cli.build_classification_set", return_value=markets):
            out = detect_newly_settled_markets(client, analyzed_ids={"0xseen"}, now=now, lookback_hours=6)
        self.assertEqual([str(m["condition_id"]).lower() for m in out], ["0xnew"])

    def test_collect_v2_loop_iterations_and_backoff(self):
        import argparse
        from unittest.mock import patch
        from poly_fight.cli import run_collect_v2_loop
        # 正常:3 轮,轮间按 interval(12h=43200s)sleep 2 次
        waits = []
        state = {"n": 0, "fail_on": None}

        def fake_collect(args, client=None, variant=None):
            state["n"] += 1
            if state["fail_on"] == state["n"]:
                raise RuntimeError("boom")
            return 0

        pauses = []
        args = argparse.Namespace(loop_hours=12, loop_error_retry_seconds=300, loop_max_iterations=3,
                                  data_dir=None, follow_dir="/tmp/_t_follow", category="esports")
        with patch("poly_fight.cli._command_collect_wallets", side_effect=fake_collect), \
             patch("poly_fight.cli.set_pause_new_signals", side_effect=lambda d, c, s: pauses.append(bool(s))):
            run_collect_v2_loop(args, client=object(), sleeper=lambda w: waits.append(w))
        self.assertEqual(state["n"], 3)
        self.assertEqual(waits, [43200, 43200])
        # 每轮:采集前暂停(True)→ 采集后恢复(None=False),共 3 轮
        self.assertEqual(pauses, [True, False, True, False, True, False])

        # 退避:第 1 轮抛错 → 用 retry(300)而非 interval;循环不崩,继续到第 2 轮
        waits2 = []
        state = {"n": 0, "fail_on": 1}
        args2 = argparse.Namespace(loop_hours=12, loop_error_retry_seconds=300, loop_max_iterations=2,
                                   data_dir=None, follow_dir="/tmp/_t_follow", category="esports")
        with patch("poly_fight.cli._command_collect_wallets", side_effect=fake_collect), \
             patch("poly_fight.cli.set_pause_new_signals"):
            run_collect_v2_loop(args2, client=object(), sleeper=lambda w: waits2.append(w))
        self.assertEqual(state["n"], 2)
        self.assertEqual(waits2, [300])

    def test_scoring_basis_actual_flips_technical_wallet(self):
        # 买了负方(hold_pnl<0)但靠出场盈利(actual_pnl>0):
        #   hold 基 → 算成亏损/押错(v1 会 excluded);actual 基 → 算成盈利市场。
        positions = [{
            "conditionId": "m1", "totalBought": 1000.0, "avgPrice": 0.1,
            "realizedPnl": -100.0, "holdPnl": -100.0, "actualPnl": 50.0,
            "timestamp": 1_700_000_000,
        }]
        cids = {"m1"}
        now = 1_700_100_000
        hold = summarize_closed_positions(positions, cids, now_ts=now, scoring_basis="hold")
        actual = summarize_closed_positions(positions, cids, now_ts=now, scoring_basis="actual")

        self.assertAlmostEqual(hold["esports_realized_pnl"], -100.0)
        self.assertEqual(hold["positive_market_rate"], 0.0)
        self.assertAlmostEqual(actual["esports_realized_pnl"], 50.0)
        self.assertEqual(actual["positive_market_rate"], 1.0)
        # hold/actual 字段两套都在,edge_type 仍可判定(用于打标)
        self.assertAlmostEqual(actual["hold_pnl"], -100.0)
        self.assertAlmostEqual(actual["actual_pnl"], 50.0)

    def test_seed_wallet_aggregation_dedupes_wallet_market_frequency(self):
        seed_positions = [
            {
                "wallet": "0xDUP",
                "condition_id": "m1",
                "bucket_key": "lol:main_match",
                "game_family": "lol",
                "market_type": "main_match",
                "seed_cost": 100,
                "seed_pnl": 30,
                "avg_price": 0.7,
                "seed_rank": 1,
                "timestamp": 100,
            },
            {
                "wallet": "0xdup",
                "condition_id": "m1",
                "bucket_key": "lol:main_match",
                "game_family": "lol",
                "market_type": "main_match",
                "seed_cost": 100,
                "seed_pnl": 30,
                "avg_price": 0.7,
                "seed_rank": 1,
                "timestamp": 100,
            },
            {
                "wallet": "0xDUP",
                "condition_id": "m2",
                "bucket_key": "cs2:map_winner",
                "game_family": "cs2",
                "market_type": "map_winner",
                "seed_cost": 150,
                "seed_pnl": 45,
                "avg_price": 0.6,
                "seed_rank": 2,
                "timestamp": 200,
            },
        ]

        wallets = aggregate_seed_wallets(seed_positions)
        wallet = wallets["0xdup"]

        self.assertEqual(wallet["seed_position_row_count"], 3)
        self.assertEqual(wallet["seed_win_count"], 2)
        self.assertEqual(wallet["seed_market_count"], 2)
        self.assertEqual(wallet["seed_cost_total"], 250)
        self.assertEqual(wallet["seed_bucket_counts"], {"cs2:map_winner": 1, "lol:main_match": 1})

    def test_profile_candidate_uses_deep_trade_behavior_metrics(self):
        now = datetime(2026, 6, 9, tzinfo=timezone.utc)
        market_records_by_id = {
            "m1": {
                "condition_id": "m1",
                "category": "esports",
                "game_family": "lol",
                "market_type": "main_match",
                "end_date": now.isoformat(),
                "match_start_time": (now - timedelta(hours=3)).isoformat(),
            },
            "m2": {
                "condition_id": "m2",
                "category": "esports",
                "game_family": "lol",
                "market_type": "main_match",
                "end_date": now.isoformat(),
                "match_start_time": (now - timedelta(hours=3)).isoformat(),
            },
            "m3": {
                "condition_id": "m3",
                "category": "esports",
                "game_family": "cs2",
                "market_type": "map_winner",
                "end_date": now.isoformat(),
                "match_start_time": (now - timedelta(hours=3)).isoformat(),
            },
        }
        trades = [
            {"conditionId": "m1", "outcomeIndex": 0, "side": "BUY", "size": 100, "price": 0.55, "timestamp": 100},
            {"conditionId": "m1", "outcomeIndex": 1, "side": "BUY", "size": 100, "price": 0.45, "timestamp": 101},
            {"conditionId": "m3", "outcomeIndex": 0, "side": "BUY", "size": 100, "price": 0.9, "timestamp": 102},
        ]
        trades.extend(
            {
                "conditionId": "m2",
                "outcomeIndex": 0,
                "side": "BUY",
                "size": 100,
                "price": 0.6,
                "timestamp": 200 + index,
            }
            for index in range(20)
        )

        candidate = build_profile_candidate_from_trades(
            {
                "wallet": "0xAAA",
                "participated_market_count": 3,
                "avg_market_cash": 100,
                "two_sided_market_count": 0,
                "high_churn_market_count": 0,
                "tail_entry_market_count": 0,
                "candidate_reasons": ["profitable_winner_seed"],
            },
            trades,
            market_records_by_id,
        )

        self.assertEqual(candidate["two_sided_market_count"], 1)
        self.assertEqual(candidate["high_churn_market_count"], 1)
        self.assertEqual(candidate["tail_entry_market_count"], 1)
        self.assertEqual(candidate["per_game_type_candidate"]["lol:main_match"]["two_sided_market_count"], 1)
        self.assertEqual(candidate["per_game_type_candidate"]["lol:main_match"]["high_churn_market_count"], 1)
        self.assertEqual(candidate["per_game_type_candidate"]["cs2:map_winner"]["tail_entry_market_count"], 1)

    def test_collector_refresh_plan_reuses_cache_and_prioritizes_recent_strong_seed(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        profile_wallets = [
            {"wallet": "0xCached", "last_seed_at": now_ts - 60, "seed_score": 10, "seed_win_count": 2, "seed_cost_total": 500},
            {"wallet": "0xRecent", "last_seed_at": now_ts - 120, "seed_score": 8, "seed_win_count": 2, "seed_cost_total": 500},
            {"wallet": "0xOldStrong", "last_seed_at": now_ts - 86400, "seed_score": 100, "seed_win_count": 10, "seed_cost_total": 5000},
        ]
        existing_profiles = {
            "0xcached": {
                "wallet": "0xCached",
                "profile_state": "qualified",
                "profiled_at": now_ts - 3600,
                "esports_condition_ids": ["m1"],
                "scoring_version": SCORING_VERSION,
            }
        }

        plan = build_collector_profile_refresh_plan(
            profile_wallets,
            existing_profiles,
            now_ts=now_ts,
            ttl_seconds=24 * 3600,
            max_refresh_profiles=1,
        )

        self.assertEqual(sorted(plan["reused_profiles_by_wallet"]), ["0xcached"])
        self.assertEqual([row["wallet"] for row in plan["refresh_plan"]], ["0xrecent"])
        self.assertEqual([row["wallet"] for row in plan["skipped_due_budget"]], ["0xoldstrong"])
        self.assertEqual(plan["stats"]["profile_cache_hits"], 1)
        self.assertEqual(plan["stats"]["profile_cache_misses"], 2)

    def test_collector_refresh_plan_rejects_invalid_cached_profiles(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        base_seed = {"wallet": "0xA", "last_seed_at": now_ts, "seed_score": 10, "seed_win_count": 2, "seed_cost_total": 500}
        invalid_profiles = [
            {"wallet": "0xA", "profile_state": "qualified", "profiled_at": now_ts - 3600, "esports_condition_ids": ["m1"], "scoring_version": SCORING_VERSION - 1},
            {"wallet": "0xA", "profile_state": "qualified", "profiled_at": now_ts - 3600, "scoring_version": SCORING_VERSION},
            {"wallet": "0xA", "profile_state": "failed_retryable", "profiled_at": now_ts - 3600, "esports_condition_ids": ["m1"], "scoring_version": SCORING_VERSION},
            {"wallet": "0xA", "profile_state": "qualified", "profiled_at": now_ts - 26 * 3600, "esports_condition_ids": ["m1"], "scoring_version": SCORING_VERSION},
        ]

        for cached in invalid_profiles:
            with self.subTest(cached=cached):
                plan = build_collector_profile_refresh_plan(
                    [base_seed],
                    {"0xa": cached},
                    now_ts=now_ts,
                    ttl_seconds=24 * 3600,
                    max_refresh_profiles=1,
                )
                self.assertEqual(plan["reused_profiles_by_wallet"], {})
                self.assertEqual([row["wallet"] for row in plan["refresh_plan"]], ["0xa"])

    def test_collector_snapshot_diagnostics_counts_lightweight_vps_snapshot(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        with TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "esports"
            raw_dir = snapshot_dir / "raw_user_trades"
            raw_dir.mkdir(parents=True)
            write_json(snapshot_dir / "collector_build_summary.json", {"stage_timings": {"wallet_profiles_seconds": 12.5}})
            write_json(snapshot_dir / "collector_profile_wallets.json", [{"wallet": "0x1"}, {"wallet": "0x2"}])
            write_json(snapshot_dir / "collector_wallet_profiles.json", [{"wallet": "0x1"}])
            write_json(snapshot_dir / "collector_seed_wallets.json", [{"wallet": "0x1"}, {"wallet": "0x2"}, {"wallet": "0x3"}])
            write_json(raw_dir / "0x1.json", {"trades": [1]})
            write_json(raw_dir / "0x2.json", {"trades": [1, 2]})

            diagnostics = build_collector_snapshot_diagnostics(snapshot_dir, now_ts=now_ts)

        self.assertEqual(diagnostics["summary_file"], "collector_build_summary.json")
        self.assertEqual(diagnostics["stage_timings"], {"wallet_profiles_seconds": 12.5})
        self.assertEqual(diagnostics["profile_wallet_count"], 2)
        self.assertEqual(diagnostics["wallet_profile_count"], 1)
        self.assertEqual(diagnostics["seed_wallet_count"], 3)
        self.assertEqual(diagnostics["raw_user_trades"]["file_count"], 2)
        self.assertGreater(diagnostics["raw_user_trades"]["total_bytes"], 0)

    def test_collector_snapshot_diagnostics_accepts_legacy_snapshot_names(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        with TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "esports"
            snapshot_dir.mkdir()
            write_json(snapshot_dir / "build_summary.json", {"stage_timings": {"raw_user_trades_seconds": 10}})
            write_json(snapshot_dir / "v3_profile_wallets.json", [{"wallet": "0x1"}])
            write_json(snapshot_dir / "v3_wallet_rawdata.json", [{"wallet": "0x1"}, {"wallet": "0x2"}])
            write_json(snapshot_dir / "v3_seed_wallets.json", [{"wallet": "0x1"}, {"wallet": "0x2"}, {"wallet": "0x3"}])

            diagnostics = build_collector_snapshot_diagnostics(snapshot_dir, now_ts=now_ts)

        self.assertEqual(diagnostics["summary_file"], "build_summary.json")
        self.assertEqual(diagnostics["profile_wallet_count"], 1)
        self.assertEqual(diagnostics["wallet_profile_count"], 2)
        self.assertEqual(diagnostics["seed_wallet_count"], 3)

    def test_analyze_collector_snapshot_command_writes_diagnostics(self):
        with TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "esports"
            snapshot_dir.mkdir()
            write_json(snapshot_dir / "collector_build_summary.json", {"stage_timings": {}})
            args = build_parser().parse_args(["analyze-collector-snapshot", "--snapshot-dir", str(snapshot_dir)])

            self.assertEqual(command_analyze_collector_snapshot(args), 0)

            self.assertTrue((snapshot_dir / "collector_snapshot_diagnostics.json").exists())

    def test_collector_diagnostics_reports_bucket_level_counts_and_rejects(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        leaderboard_row = {
            "wallet": "0xleader",
            "best_bucket": "cs2:main_match",
            "best_game_family": "cs2",
        }
        dota_profile = {
            "wallet": "0xdota",
            "category": "esports",
            "grade": "A",
            "scoring_version": SCORING_VERSION,
            "eligible_buckets": ["dota2:game_winner"],
            "eligible_bucket_modes": {"dota2:game_winner": "emerging"},
            "eligible_market_types": ["game_winner"],
            "last_esports_trade_at": now_ts - 3600,
            "esports_roi": 0.20,
            "positive_market_rate": 0.70,
            "wilson_win_rate_lower_bound": 0.60,
            "capital_weighted_edge": 0.12,
            "actual_minus_hold_pnl_rate": 0.0,
            "per_game_type": {
                "dota2:game_winner": {
                    "esports_closed_count": 12,
                    "positive_market_rate": 0.70,
                    "wilson_win_rate_lower_bound": 0.60,
                    "capital_weighted_edge": 0.12,
                    "esports_roi": 0.25,
                    "median_entry_price": 0.55,
                    "last_esports_trade_at": now_ts - 3600,
                    "recent_bucket_market_count": 8,
                    "recent_bucket_roi": 0.20,
                    "recent_bucket_positive_rate": 0.75,
                }
            },
            "per_game_type_grades": {
                "dota2:game_winner": {
                    "grade": "A",
                    "eligible_mode": "emerging",
                    "esports_closed_count": 12,
                    "positive_market_rate": 0.70,
                    "wilson_win_rate_lower_bound": 0.60,
                    "capital_weighted_edge": 0.12,
                    "esports_roi": 0.25,
                    "median_entry_price": 0.55,
                }
            },
            "candidate": {
                "per_game_type_candidate": {
                    "dota2:game_winner": {
                        "participated_market_count": 8,
                        "avg_market_cash": 5_000,
                        "two_sided_market_count": 0,
                        "tail_entry_market_count": 3,
                    }
                }
            },
        }

        diagnostics = build_collector_diagnostics(
            seed_wallets={
                "0xseed": {
                    "wallet": "0xseed",
                    "seed_bucket_counts": {"lol:main_match": 8},
                }
            },
            profile_wallets=[
                {
                    "wallet": "0xseed",
                    "seed_bucket_counts": {"lol:main_match": 8},
                    "qualified_seed_buckets": ["lol:main_match"],
                }
            ],
            profiles_by_wallet={
                "0xleader": leaderboard_row,
                "0xdota": dota_profile,
            },
            leaderboard=[leaderboard_row],
            now_ts=now_ts,
        )

        self.assertEqual(diagnostics["leaderboard_best_bucket_counts"], {"cs2:main_match": 1})
        self.assertEqual(diagnostics["leaderboard_best_game_family_counts"], {"cs2": 1})
        self.assertEqual(diagnostics["qualified_seed_bucket_counts"], {"lol:main_match": 1})
        self.assertEqual(diagnostics["eligible_bucket_mode_counts"], {"emerging": 1})
        self.assertIn("dota2:game_winner", diagnostics["eligible_bucket_reject_reasons"])
        self.assertEqual(
            diagnostics["copyable_reject_reasons_by_bucket"]["dota2:game_winner"],
            {"tail_entry_over_limit": 1},
        )

    def test_seeded_leaderboard_uses_clean_eligible_bucket_behavior_not_global_candidate_noise(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        def profile(wallet, **overrides):
            row = {
                "wallet": wallet,
                "category": "esports",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "eligible_buckets": ["dota2:game_winner"],
                "eligible_market_types": ["game_winner"],
                "last_esports_trade_at": now_ts - 3600,
                "esports_roi": 0.2,
                "positive_market_rate": 0.7,
                "wilson_win_rate_lower_bound": 0.6,
                "capital_weighted_edge": 0.1,
                "median_entry_price": 0.55,
                "actual_minus_hold_pnl_rate": 0.0,
                "per_game_type": {
                    "dota2:game_winner": {
                        "esports_closed_count": 15,
                        "positive_market_rate": 0.7,
                        "wilson_win_rate_lower_bound": 0.6,
                        "capital_weighted_edge": 0.1,
                        "esports_roi": 0.2,
                        "median_entry_price": 0.55,
                        "last_esports_trade_at": now_ts - 3600,
                        "recent_bucket_market_count": 4,
                        "recent_bucket_roi": 0.1,
                        "recent_bucket_positive_rate": 0.75,
                    }
                },
                "per_game_type_grades": {
                    "dota2:game_winner": {
                        "grade": "A",
                        "esports_closed_count": 15,
                        "positive_market_rate": 0.7,
                        "wilson_win_rate_lower_bound": 0.6,
                        "capital_weighted_edge": 0.1,
                        "esports_roi": 0.2,
                        "median_entry_price": 0.55,
                    }
                },
                "candidate": {
                    "participated_market_count": 100,
                    "avg_market_cash": 10_000,
                    "two_sided_market_count": 5,
                    "high_churn_market_count": 50,
                    "tail_entry_market_count": 20,
                    "per_game_type_candidate": {
                        "dota2:game_winner": {
                            "participated_market_count": 15,
                            "avg_market_cash": 5_000,
                            "two_sided_market_count": 0,
                            "high_churn_market_count": 2,
                            "tail_entry_market_count": 0,
                        }
                    },
                },
            }
            row.update(overrides)
            return row

        leaderboard = build_seeded_leaderboard({"0xa": profile("0xA")}, now_ts=now_ts)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xA"])

    def test_strict_final_gate_uses_bucket_metrics_for_specialists(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        profile = {
            "wallet": "0xdota",
            "category": "esports",
            "grade": "A",
            "scoring_version": SCORING_VERSION,
            "eligible_buckets": ["dota2:game_winner"],
            "eligible_market_types": ["game_winner"],
            "last_esports_trade_at": now_ts - 3600,
            "esports_roi": 0.01,
            "positive_market_rate": 0.52,
            "wilson_win_rate_lower_bound": 0.49,
            "capital_weighted_edge": 0.01,
            "median_entry_price": 0.55,
            "actual_minus_hold_pnl_rate": 0.0,
            "per_game_type": {
                "dota2:game_winner": {
                    "esports_closed_count": 16,
                    "positive_market_rate": 0.75,
                    "wilson_win_rate_lower_bound": 0.61,
                    "capital_weighted_edge": 0.18,
                    "esports_roi": 0.34,
                    "median_entry_price": 0.55,
                    "last_esports_trade_at": now_ts - 3600,
                    "recent_bucket_market_count": 8,
                    "recent_bucket_roi": 0.22,
                    "recent_bucket_positive_rate": 0.75,
                    "recent_7d_market_count": 8,
                    "recent_7d_roi": 0.22,
                    "recent_7d_positive_rate": 0.75,
                    "recent_14d_market_count": 16,
                    "recent_14d_roi": 0.34,
                    "recent_14d_positive_rate": 0.75,
                }
            },
            "per_game_type_grades": {
                "dota2:game_winner": {
                    "grade": "A",
                    "esports_closed_count": 16,
                    "positive_market_rate": 0.75,
                    "wilson_win_rate_lower_bound": 0.61,
                    "capital_weighted_edge": 0.18,
                    "esports_roi": 0.34,
                    "median_entry_price": 0.55,
                }
            },
            "candidate": {
                "participated_market_count": 40,
                "avg_market_cash": 10_000,
                "two_sided_market_count": 6,
                "tail_entry_market_count": 10,
                "per_game_type_candidate": {
                    "dota2:game_winner": {
                        "participated_market_count": 16,
                        "avg_market_cash": 5_000,
                        "two_sided_market_count": 0,
                        "tail_entry_market_count": 0,
                        "high_churn_market_count": 0,
                    }
                },
            },
        }

        self.assertTrue(strict_final_quality_ok(profile))
        leaderboard = build_seeded_leaderboard({"0xdota": profile}, now_ts=now_ts)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xdota"])

    def test_seeded_leaderboard_applies_strict_final_quality_gate(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())

        def profile(wallet, **overrides):
            bucket_overrides = overrides.pop("bucket_overrides", {})
            row = {
                "wallet": wallet,
                "category": "esports",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "eligible_buckets": ["cs2:main_match"],
                "eligible_market_types": ["main_match"],
                "last_esports_trade_at": now_ts - 3600,
                "esports_closed_count": 80,
                "positive_market_rate": 0.62,
                "wilson_win_rate_lower_bound": 0.56,
                "esports_roi": 0.12,
                "capital_weighted_edge": 0.09,
                "median_entry_price": 0.56,
                "actual_minus_hold_pnl_rate": 0.0,
                "per_game_type": {
                    "cs2:main_match": {
                        "esports_closed_count": 80,
                        "positive_market_rate": 0.62,
                        "wilson_win_rate_lower_bound": 0.56,
                        "capital_weighted_edge": 0.09,
                        "esports_roi": 0.12,
                        "median_entry_price": 0.56,
                        "last_esports_trade_at": now_ts - 3600,
                        "recent_bucket_market_count": 4,
                        "recent_bucket_roi": 0.1,
                        "recent_bucket_positive_rate": 0.75,
                    }
                },
                "per_game_type_grades": {
                    "cs2:main_match": {
                        "grade": "A",
                        "esports_closed_count": 80,
                        "positive_market_rate": 0.62,
                        "wilson_win_rate_lower_bound": 0.56,
                        "capital_weighted_edge": 0.09,
                        "esports_roi": 0.12,
                        "median_entry_price": 0.56,
                    }
                },
                "candidate": {
                    "participated_market_count": 80,
                    "avg_market_cash": 5_000,
                    "two_sided_market_count": 0,
                    "tail_entry_market_count": 0,
                    "high_churn_market_count": 0,
                    "per_game_type_candidate": {
                        "cs2:main_match": {
                            "participated_market_count": 80,
                            "avg_market_cash": 5_000,
                            "two_sided_market_count": 0,
                            "tail_entry_market_count": 0,
                            "high_churn_market_count": 0,
                        }
                    },
                },
            }
            if bucket_overrides:
                row["per_game_type"]["cs2:main_match"].update(bucket_overrides)
                row["per_game_type_grades"]["cs2:main_match"].update(bucket_overrides)
            row.update(overrides)
            return row

        profiles = {
            "0xkeep": profile("0xkeep"),
            "0xlowroi": profile("0xlowroi", bucket_overrides={"esports_roi": 0.01}),
            "0xlowpos": profile("0xlowpos", bucket_overrides={"positive_market_rate": 0.52}),
            "0xlowwilson": profile("0xlowwilson", bucket_overrides={"wilson_win_rate_lower_bound": 0.49}),
            "0xlowedge": profile("0xlowedge", bucket_overrides={"capital_weighted_edge": 0.02}),
        }

        leaderboard = build_seeded_leaderboard(profiles, now_ts=now_ts)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xkeep"])

    def test_strict_final_gate_can_be_disabled_for_research_views(self):
        now_ts = int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp())
        profile = {
            "wallet": "0xlowroi",
            "category": "esports",
            "grade": "A",
            "scoring_version": SCORING_VERSION,
            "eligible_buckets": ["dota2:game_winner"],
            "eligible_market_types": ["game_winner"],
            "last_esports_trade_at": now_ts - 3600,
            "esports_roi": 0.01,
            "positive_market_rate": 0.7,
            "capital_weighted_edge": 0.1,
            "actual_minus_hold_pnl_rate": 0.0,
            "per_game_type": {
                "dota2:game_winner": {
                    "esports_closed_count": 15,
                    "positive_market_rate": 0.7,
                    "wilson_win_rate_lower_bound": 0.6,
                    "capital_weighted_edge": 0.1,
                    "esports_roi": 0.2,
                    "median_entry_price": 0.55,
                    "last_esports_trade_at": now_ts - 3600,
                    "recent_bucket_market_count": 4,
                    "recent_bucket_roi": 0.1,
                    "recent_bucket_positive_rate": 0.75,
                }
            },
            "per_game_type_grades": {
                "dota2:game_winner": {
                    "grade": "A",
                    "esports_closed_count": 15,
                    "positive_market_rate": 0.7,
                    "wilson_win_rate_lower_bound": 0.6,
                    "capital_weighted_edge": 0.1,
                    "esports_roi": 0.2,
                    "median_entry_price": 0.55,
                }
            },
            "candidate": {
                "participated_market_count": 100,
                "avg_market_cash": 10_000,
                "two_sided_market_count": 5,
                "high_churn_market_count": 50,
                "tail_entry_market_count": 20,
                "per_game_type_candidate": {
                    "dota2:game_winner": {
                        "participated_market_count": 15,
                        "avg_market_cash": 5_000,
                        "two_sided_market_count": 0,
                        "high_churn_market_count": 2,
                        "tail_entry_market_count": 0,
                    }
                },
            },
        }

        leaderboard = build_seeded_leaderboard({"0xlowroi": profile}, now_ts=now_ts, require_strict_quality_gate=False)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xlowroi"])

    def test_collector_uses_scoped_user_history_depth_by_default(self):
        args = build_parser().parse_args(["collect"])

        self.assertEqual(args.user_history_trades_max_pages, 3)

    def test_user_trade_cache_can_reuse_deeper_cached_history(self):
        class FakeClient:
            def trades_for_user(self, wallet, *, limit=500, offset=0):
                raise AssertionError("cache should avoid API fetch")

        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            cache_path = data_dir / "raw_user_trades" / "0xaaa.json"
            cache_path.parent.mkdir(parents=True)
            write_json(
                cache_path,
                {
                    "meta": {
                        "wallet": "0xaaa",
                        "page_limit": 500,
                        "max_pages": 8,
                        "raw_user_trades": True,
                    },
                    "trades": [{"id": "cached"}],
                },
            )

            trades = fetch_recent_user_trades_for_wallet(
                FakeClient(),
                "0xAAA",
                page_limit=500,
                max_pages=3,
                data_dir=data_dir,
                now_ts=int(datetime(2026, 6, 9, tzinfo=timezone.utc).timestamp()),
                cache_ttl_days=30,
            )

        self.assertEqual(trades, [{"id": "cached"}])

    def test_follow_command_uses_paper_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["follow", "--stake-usdc", "25"])

        self.assertEqual(args.command, "follow")
        self.assertEqual(args.stake_usdc, 25)
        self.assertEqual(args.stake_ratio_percent, 10)
        self.assertEqual(args.strategy_source, "auto")
        self.assertEqual(args.max_stake_usdc, 0.0)
        self.assertEqual(args.max_signal_stake_usdc, 0.0)
        self.assertEqual(args.max_signal_stake_balance_percent, 0.0)
        self.assertEqual(args.bankroll_usdc, 0.0)
        self.assertEqual(effective_bankroll_usdc(args.bankroll_usdc), float("inf"))
        self.assertEqual(args.follow_recency_days, 30)
        self.assertEqual(args.observe_window_hours, 24)
        self.assertEqual(args.max_slippage_over_entry, 0.10)
        self.assertEqual(args.max_entry_price, 0.85)
        self.assertEqual(args.min_wallet_entry_price, 0.4)
        self.assertFalse(args.require_pre_match)
        self.assertEqual(args.run_log_retention_days, 7)
        self.assertEqual(args.resolution_cache_ttl_seconds, 60)
        self.assertEqual(args.resolution_gamma_pages, 2)
        self.assertEqual(args.event_cache_ttl_minutes, 60)
        self.assertEqual(args.user_trades_limit, 50)
        self.assertEqual(args.user_trades_max_pages, 1)
        self.assertFalse(hasattr(args, "bootstrap_current_positions"))
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.min_tick_seconds, 180)
        self.assertEqual(args.max_tick_seconds, 900)
        self.assertEqual(args.tick_seconds, 60)
        self.assertFalse(hasattr(args, "positions_limit"))
        self.assertFalse(hasattr(args, "position_observe_mode"))
        self.assertFalse(hasattr(args, "position_observe_limit"))
        self.assertEqual(args.max_workers, 8)
        self.assertFalse(hasattr(args, "consensus_block_opposite"))
        self.assertFalse(hasattr(args, "conflict_policy"))
        self.assertEqual(args.quarantine_sell_frac, 0.2)

        pre_match_args = parser.parse_args(["follow", "--stake-usdc", "25", "--require-pre-match"])
        self.assertTrue(pre_match_args.require_pre_match)

    def test_run_command_uses_v2_loop_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "1"])

        self.assertEqual(args.command, "run")
        self.assertEqual(args.stake_usdc, 1)
        self.assertEqual(args.stake_ratio_percent, 10)
        self.assertEqual(args.strategy_source, "auto")
        self.assertEqual(args.max_stake_usdc, 0.0)
        self.assertEqual(args.max_signal_stake_usdc, 0.0)
        self.assertEqual(args.max_signal_stake_balance_percent, 0.0)
        self.assertEqual(args.bankroll_usdc, 0.0)
        self.assertEqual(effective_bankroll_usdc(args.bankroll_usdc), float("inf"))
        self.assertTrue(args.skip_initial_build)
        self.assertEqual(args.max_run_ticks, 1)
        self.assertEqual(args.pool_refresh_hours, 12)
        self.assertEqual(args.observe_window_hours, 24)
        self.assertEqual(args.user_trades_limit, 50)
        self.assertEqual(args.user_trades_max_pages, 1)
        self.assertFalse(hasattr(args, "bootstrap_current_positions"))
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.error_retry_seconds, 180)
        self.assertEqual(args.max_consecutive_error_seconds, 600)
        self.assertEqual(args.max_slippage_over_entry, 0.10)
        self.assertEqual(args.max_entry_price, 0.85)
        self.assertEqual(args.min_wallet_entry_price, 0.4)
        self.assertEqual(args.event_cache_ttl_minutes, 60)
        self.assertEqual(args.resolution_cache_ttl_seconds, 60)
        self.assertEqual(args.resolution_gamma_pages, 2)
        self.assertEqual(args.tick_seconds, 60)
        self.assertFalse(hasattr(args, "positions_limit"))
        self.assertFalse(hasattr(args, "position_observe_mode"))
        self.assertFalse(hasattr(args, "position_observe_limit"))
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_count, 2)
        self.assertFalse(hasattr(args, "consensus_block_opposite"))
        self.assertFalse(hasattr(args, "conflict_policy"))
        self.assertEqual(args.quarantine_sell_frac, 0.2)
        self.assertFalse(args.require_pre_match)

        pre_match_args = parser.parse_args(["run", "--stake-usdc", "1", "--require-pre-match"])
        self.assertTrue(pre_match_args.require_pre_match)

    def test_run_command_can_use_db_strategy_without_stake_flag(self):
        parser = build_parser()

        args = parser.parse_args(["run", "--strategy-source", "db", "--skip-initial-build", "--max-run-ticks", "1"])

        self.assertEqual(args.strategy_source, "db")
        self.assertEqual(args.stake_usdc, 1.0)

    def test_zero_bankroll_disables_follow_exposure_cap(self):
        self.assertEqual(effective_bankroll_usdc(0), float("inf"))
        self.assertEqual(effective_bankroll_usdc(-1), float("inf"))
        self.assertEqual(effective_bankroll_usdc("bad"), float("inf"))
        self.assertEqual(effective_bankroll_usdc(250), 250)

    def test_follow_commands_reject_removed_dead_options(self):
        parser = build_parser()

        removed_options = [
            ["collect", "--seed-sort-by", "REALIZED_PNL"],
            ["collect", "--refresh-user-trades"],
            ["collect", "--no-user-trades-cache"],
            ["collect", "--no-dashboard-publish"],
            ["collect", "--discovery-source", "holders"],
            ["collect", "--holders-limit", "50"],
            ["collect", "--allow-dirty-profile-candidates"],
            ["collect", "--check-current-positions"],
            ["collect", "--min-pre-match-entry-rate", "0.8"],
            ["build-leaderboard", "--seed-sort-by", "REALIZED_PNL"],
            ["build-leaderboard", "--refresh-user-trades"],
            ["build-leaderboard", "--no-user-trades-cache"],
            ["build-leaderboard", "--no-dashboard-publish"],
            ["build-leaderboard", "--discovery-source", "holders"],
            ["build-leaderboard", "--holders-limit", "50"],
            ["build-leaderboard", "--allow-dirty-profile-candidates"],
            ["build-leaderboard", "--check-current-positions"],
            ["build-leaderboard", "--min-pre-match-entry-rate", "0.8"],
            ["follow", "--stake-usdc", "1", "--execution-mode", "live"],
            ["follow", "--stake-usdc", "1", "--conflict-policy", "exit_on_opposite"],
            ["follow", "--stake-usdc", "1", "--no-consensus-block-opposite"],
            ["follow", "--stake-usdc", "1", "--no-bootstrap-current-positions"],
            ["run", "--stake-usdc", "1", "--execution-mode", "live"],
            ["run", "--stake-usdc", "1", "--conflict-policy", "exit_on_opposite"],
            ["run", "--stake-usdc", "1", "--no-consensus-block-opposite"],
            ["run", "--stake-usdc", "1", "--no-bootstrap-current-positions"],
            ["follow", "--stake-usdc", "1", "--event-gate-horizon-hours", "24"],
            ["follow", "--stake-usdc", "1", "--results-retention-days", "0"],
            ["follow", "--stake-usdc", "1", "--consensus-min-same-side", "1"],
            ["run", "--stake-usdc", "1", "--event-gate-horizon-hours", "24"],
            ["run", "--stake-usdc", "1", "--results-retention-days", "0"],
            ["run", "--stake-usdc", "1", "--consensus-min-same-side", "1"],
            ["follow", "--stake-usdc", "1", "--conviction-stake-usdc", "5"],
            ["follow", "--stake-usdc", "1", "--conviction-size-multiple", "2"],
            ["follow", "--stake-usdc", "1", "--no-conviction-gate"],
            ["run", "--stake-usdc", "1", "--conviction-stake-usdc", "5"],
            ["run", "--stake-usdc", "1", "--conviction-size-multiple", "2"],
            ["run", "--stake-usdc", "1", "--no-conviction-gate"],
            ["follow", "--stake-usdc", "1", "--positions-limit", "25"],
            ["follow", "--stake-usdc", "1", "--position-observe-mode", "shadow"],
            ["follow", "--stake-usdc", "1", "--position-observe-limit", "25"],
            ["run", "--stake-usdc", "1", "--positions-limit", "25"],
            ["run", "--stake-usdc", "1", "--position-observe-mode", "shadow"],
            ["run", "--stake-usdc", "1", "--position-observe-limit", "25"],
        ]
        for argv in removed_options:
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    parser.parse_args(argv)

    def test_serve_command_uses_dashboard_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["serve"])

        self.assertEqual(args.command, "serve")
        self.assertEqual(args.host, "127.0.0.1")
        self.assertEqual(args.port, 8787)
        self.assertEqual(args.user, "admin")
        self.assertEqual(args.session_ttl_seconds, 12 * 3600)
        self.assertFalse(args.cookie_secure)
        self.assertEqual(args.max_requests_per_second, 10)
        self.assertEqual(args.stream_poll_seconds, 2.0)
        self.assertEqual(args.stream_heartbeat_seconds, 15.0)
        self.assertEqual(args.max_stream_clients, 8)
        self.assertEqual(args.follow_dir, None)

    def test_follow_and_run_accept_follow_dir(self):
        parser = build_parser()

        follow_args = parser.parse_args(["follow", "--stake-usdc", "1", "--follow-dir", "data/follow"])
        run_args = parser.parse_args(["run", "--stake-usdc", "1", "--follow-dir", "data/follow", "--skip-initial-build"])

        self.assertEqual(follow_args.follow_dir, "data/follow")
        self.assertEqual(run_args.follow_dir, "data/follow")

    def test_run_skip_initial_build_does_not_build_before_first_tick(self):
        parser = build_parser()
        args = parser.parse_args(["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "1"])

        with patch("poly_fight.cli.build_client", return_value=object()), patch(
            "poly_fight.cli._command_collect_wallets"
        ) as build, patch("poly_fight.cli.command_follow", return_value={"desired_next_interval_seconds": 900}):
            from poly_fight.cli import command_run

            with redirect_stdout(StringIO()):
                self.assertEqual(command_run(args), 0)

        self.assertEqual(build.call_count, 0)

    def test_run_loop_survives_one_follow_exception(self):
        parser = build_parser()
        args = parser.parse_args(["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "1"])

        follow_results = [RuntimeError("temporary"), {"desired_next_interval_seconds": 900}]

        def flaky_follow(*_args, **_kwargs):
            result = follow_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with patch("poly_fight.cli.build_client", return_value=object()), patch(
            "poly_fight.cli._command_collect_wallets"
        ) as build, patch("poly_fight.cli.command_follow", side_effect=flaky_follow) as follow, patch(
            "poly_fight.cli.time.sleep"
        ) as sleep:
            from poly_fight.cli import command_run

            with redirect_stdout(StringIO()):
                self.assertEqual(command_run(args), 0)

        self.assertEqual(build.call_count, 0)
        self.assertEqual(follow.call_count, 2)
        self.assertEqual(sleep.call_args_list[0].args[0], 180)

    def test_run_loop_subtracts_iteration_runtime_from_sleep(self):
        parser = build_parser()
        args = parser.parse_args(
            ["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "2", "--tick-seconds", "120"]
        )

        with patch("poly_fight.cli.build_client", return_value=object()), patch(
            "poly_fight.cli._command_collect_wallets"
        ), patch(
            "poly_fight.cli.command_follow", return_value={"desired_next_interval_seconds": 120}
        ) as follow, patch("poly_fight.cli.time.sleep") as sleep, patch(
            "poly_fight.cli.time.monotonic", side_effect=[0.0, 35.0, 120.0]
        ):
            from poly_fight.cli import command_run

            with redirect_stdout(StringIO()):
                self.assertEqual(command_run(args), 0)

        self.assertEqual(follow.call_count, 2)
        self.assertEqual(sleep.call_count, 1)
        self.assertEqual(sleep.call_args.args[0], 85)

    def test_run_full_refresh_pauses_new_signals(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            parser = build_parser()
            args = parser.parse_args(
                [
                    "--data-dir",
                    str(data_dir),
                    "run",
                    "--stake-usdc",
                    "1",
                    "--skip-initial-build",
                    "--max-run-ticks",
                    "1",
                    "--pool-refresh-hours",
                    "24",
                ]
            )
            real_datetime = datetime
            times = [1000, 1000 + 86400, 1000 + 86400, 1000 + 86400]
            paused_categories = []

            class FakeDateTime:
                @classmethod
                def now(cls, tz=None):
                    value = times.pop(0)
                    return real_datetime.fromtimestamp(value, tz=tz)

            def fake_build(build_args, **_kwargs):
                category = build_args.category
                pause = read_follow_control(data_dir / "follow").get("pause_new_signals", {})
                self.assertEqual(pause.get(category, {}).get("status"), "paused")
                self.assertEqual(pause.get(category, {}).get("reason"), "pool_refresh")
                paused_categories.append(category)
                return 0

            def fake_follow(*_args, **_kwargs):
                self.assertNotIn("pause_new_signals", read_follow_control(data_dir / "follow"))
                return {"desired_next_interval_seconds": 900}

            with patch("poly_fight.cli.datetime", FakeDateTime), patch("poly_fight.cli.build_client", return_value=object()), patch(
                "poly_fight.cli._command_collect_wallets", side_effect=fake_build
            ) as collect, patch(
                "poly_fight.cli.command_follow", side_effect=fake_follow
            ):
                from poly_fight.cli import command_run

                with redirect_stdout(StringIO()):
                    self.assertEqual(command_run(args), 0)

            self.assertEqual(collect.call_count, 1)
            self.assertEqual(set(paused_categories), {"esports"})

    def test_run_loop_stops_after_consecutive_error_window(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "run",
                "--stake-usdc",
                "1",
                "--skip-initial-build",
                "--error-retry-seconds",
                "1",
                "--max-consecutive-error-seconds",
                "2",
            ]
        )
        times = [1000, 1000, 1003, 1003]

        with patch("poly_fight.cli.build_client", return_value=object()), patch(
            "poly_fight.cli._command_collect_wallets"
        ), patch("poly_fight.cli.command_follow", side_effect=RuntimeError("poly down")) as follow, patch(
            "poly_fight.cli.time.sleep"
        ) as sleep, patch("poly_fight.cli.time.time", side_effect=lambda: times.pop(0)):
            from poly_fight.cli import command_run

            with redirect_stdout(StringIO()):
                self.assertEqual(command_run(args), 2)

        self.assertEqual(follow.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
