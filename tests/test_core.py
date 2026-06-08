from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from contextlib import nullcontext, redirect_stdout
import http.client
import json
import os
from io import BytesIO, StringIO
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
import threading
import time
import urllib.error
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
    short_addr,
    start_runner,
    stop_runner,
    start_wallet_refresh,
    stream_dirty_flags,
    StreamSignal,
    verify_session_token,
)
from poly_fight import dashboard as dashboard_module
from poly_fight.control import read_follow_control, write_follow_control
from poly_fight.storage import FollowStore
from poly_fight.cli import (
    BuildLockUnavailable,
    acquire_build_lock,
    build_leaderboard_from_profiles,
    build_parser,
    build_wallet_overlap_report,
    build_profile_fetch_plan,
    effective_build_limits,
    backfill_user_trade_submarkets,
    command_build_leaderboard,
    _command_build_leaderboard_unlocked,
    command_follow,
    fetch_resolutions_for_open_signals,
    fetch_recent_esports_closed_positions_for_wallet,
    fetch_recent_esports_user_trades_for_wallet,
    fetch_user_trades_until_cursor,
    fetch_market_trades_cached,
    filter_profile_candidates,
    follow_run_log_path,
    load_active_market_cache,
    merge_cached_profile_with_candidate,
    merge_profiles_with_candidates,
    migrate_category_follow_dbs,
    observed_performance_quarantine_events,
    prune_profile_store,
    refresh_team_logo_cache_from_active_markets,
    refresh_open_signal_fills,
    read_json,
    read_jsonl,
    resolve_data_dir,
    should_refresh_file_cache,
    should_use_cached_profile,
    watched_markets,
    write_json,
    write_jsonl,
    prepare_category_refresh_dir,
)
from poly_fight.core import (
    SCORING_VERSION,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    classify_market_type,
    classify_wallet,
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
    winning_outcome_index,
)
from poly_fight.follow import (
    aggregate_follow_performance,
    apply_closing_line_snapshots,
    apply_contested_flags,
    bootstrap_position_trades,
    compute_clv,
    contested_markets,
    desired_tick_interval,
    detect_new_positions,
    eligible_follow_wallets,
    esports_match_imminent,
    evaluate_slippage,
    follow_signal_id,
    material_sell,
    paper_pnl,
    process_follow_trades,
    follow_stake_for_signal,
    qualify_follow,
    quarantine_reason,
    select_new_trades,
    settle_open_signals,
    should_retry_unqualified_position,
    summarize_wallet_fills,
    upsert_follow_signal,
    wallet_behavior_summary,
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

    def test_collect_sports_defaults_to_isolated_data_dir(self):
        parser = build_parser()

        sports_args = parser.parse_args(["collect", "--category", "sports"])
        esports_args = parser.parse_args(["collect"])
        follow_args = parser.parse_args(["follow", "--stake-usdc", "1"])
        explicit_args = parser.parse_args(["--data-dir", "custom_dir", "collect", "--category", "sports"])

        self.assertEqual(sports_args.data_dir, None)
        self.assertEqual(resolve_data_dir(sports_args), Path("data/sports"))
        self.assertEqual(resolve_data_dir(esports_args), Path("data/esports"))
        self.assertEqual(resolve_data_dir(follow_args), Path("data/esports"))
        self.assertEqual(resolve_data_dir(explicit_args), Path("custom_dir"))

    def test_category_data_dirs_use_fixed_dashboard_root_mapping(self):
        root = Path("/tmp/poly-data")

        self.assertEqual(
            category_data_dirs(root),
            {"esports": root / "esports", "sports": root / "sports"},
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
        self.assertIsNone(event_category(valorant_event))
        self.assertEqual(event_league(valorant_event), "other")

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

                self.assertEqual(record["match_start_time"], "2026-06-07 20:10:00+00")
                self.assertEqual(record["market_start_time"], "2026-06-07 20:10:00+00")
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

    def test_market_classifier_accepts_cs_map_winner_and_rejects_valorant(self):
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

        self.assertEqual(classify_market_type(cs_event, cs_market), "map_winner")
        self.assertIsNone(classify_market_type(valorant_event, valorant_market))

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

    def test_discovery_slate_uses_progressive_window(self):
        markets = [market(f"m{i}", days_ago=10, volume=30_000) for i in range(35)]

        slate, meta = build_discovery_slate(
            markets,
            now=datetime(2026, 6, 4, tzinfo=timezone.utc),
            target_markets=30,
        )

        self.assertEqual(len(slate), 35)
        self.assertEqual(meta["selected_lookback_days"], 14)

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
                {"proxyWallet": "0xCHURN", "size": 1000, "price": 0.5, "outcome": "A", "timestamp": 1},
                {"proxyWallet": "0xCHURN", "size": 1000, "price": 0.5, "outcome": "B", "timestamp": 2},
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

    def test_tail_entry_requires_late_timing_and_high_average_price(self):
        trades_by_market = {
            "m1": [
                {"proxyWallet": "0xLOW", "size": 1000, "price": 0.45, "outcome": "A", "timestamp": 100},
                {"proxyWallet": "0xLOW", "size": 1000, "price": 0.55, "outcome": "A", "timestamp": 1200},
            ],
            "m2": [
                {"proxyWallet": "0xCHASE", "size": 1000, "price": 0.80, "outcome": "A", "timestamp": 1200},
            ],
        }
        market_start_times = {"m1": 1000, "m2": 1000}

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
        self.assertEqual(summary["median_entry_price"], 0.7)
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

        positions, _behavior = reconstruct_closed_positions(trades, market_records)
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
            for i in range(10)
        ]
        summary = summarize_closed_positions(
            positions,
            {f"g{i}" for i in range(10)},
            condition_type_by_id={f"g{i}": "game_winner" for i in range(10)},
            now_ts=200,
        )

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["grade"], "A")
        self.assertEqual(rated["eligible_market_types"], ["game_winner"])
        self.assertEqual(rated["per_type_grades"]["game_winner"]["grade"], "A")
        self.assertNotIn("main_match", rated["eligible_market_types"])

    def test_thin_submarket_sample_is_not_eligible(self):
        # A near-perfect but tiny sub-bucket (5 wins) must NOT qualify for A: sub-markets
        # require min_sample=10 to avoid trusting small-sample overfit records.
        positions = [
            {
                "conditionId": f"g{i}",
                "totalBought": 1200,
                "realizedPnl": 720,
                "avgPrice": 0.4,
                "timestamp": 100 + i,
            }
            for i in range(5)
        ]
        summary = summarize_closed_positions(
            positions,
            {f"g{i}" for i in range(5)},
            condition_type_by_id={f"g{i}": "game_winner" for i in range(5)},
            now_ts=200,
        )

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["eligible_market_types"], [])
        self.assertNotEqual(rated["per_type_grades"]["game_winner"]["grade"], "A")
        self.assertIn("thin_sample", rated["per_type_grades"]["game_winner"]["reasons"])

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
        self.assertIn("unstable_returns", rated["reasons"])

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

    def test_marginal_positive_rate_wallet_is_not_a_grade(self):
        summary = {
            "esports_closed_count": 50,
            "esports_realized_pnl": 20_000,
            "median_market_roi": 0.38,
            "positive_market_rate": 0.52,
            "wilson_win_rate_lower_bound": 0.66,
            "esports_loss_count": 24,
            "esports_total_bought": 100_000,
            "esports_total_cost": 50_000,
            "median_entry_price": 0.51,
            "capital_weighted_edge": 0.10,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "C")
        self.assertIn("low_positive_market_rate", rated["reasons"])

    def test_high_wilson_with_weak_capital_edge_is_not_a_grade(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 0.90,
            "wilson_win_rate_lower_bound": 0.67,
            "esports_loss_count": 2,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.55,
            "capital_weighted_edge": 0.05,  # >0 (not excluded) but below esports A floor 0.08
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        # high wilson but weak capital_weighted_edge → blocked from A
        self.assertNotEqual(rated["grade"], "A")
        self.assertIn("weak_capital_weighted_edge", rated["reasons"])

    def test_sports_wallet_rating_uses_capital_weighted_edge_not_entry_edge_for_a_grade(self):
        summary = {
            "category": "sports",
            "esports_closed_count": 40,
            "esports_realized_pnl": 46_000,
            "esports_roi": 0.46,
            "median_market_roi": 0.35,
            "positive_market_rate": 26 / 40,
            "wilson_win_rate_lower_bound": 0.55,
            "esports_loss_count": 14,
            "esports_total_bought": 150_000,
            "esports_total_cost": 100_000,
            "median_entry_price": 0.51,
            "capital_weighted_edge": 0.226,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertEqual(rated["entry_edge"], 0.04)
        self.assertNotIn("weak_entry_edge", rated["reasons"])
        self.assertNotIn("weak_wilson", rated["reasons"])

    def test_sports_wallet_rating_requires_eight_closed_markets_for_a_grade(self):
        base_summary = {
            "category": "sports",
            "esports_realized_pnl": 12_000,
            "esports_roi": 0.30,
            "median_market_roi": 0.30,
            "positive_market_rate": 0.75,
            "wilson_win_rate_lower_bound": 0.56,
            "esports_loss_count": 2,
            "esports_total_bought": 60_000,
            "esports_total_cost": 40_000,
            "median_entry_price": 0.52,
            "capital_weighted_edge": 0.16,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        thin = classify_wallet({**base_summary, "esports_closed_count": 7}, now_ts=100 + 86400)
        qualified = classify_wallet({**base_summary, "esports_closed_count": 8}, now_ts=100 + 86400)

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

    def test_no_capital_edge_is_excluded(self):
        # Positive hold pnl but no edge over the entry price (won exactly as much capital as
        # implied) → excluded by the skill axis.
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 100,
            "esports_roi": 0.30,
            "positive_market_rate": 0.70,
            "wilson_win_rate_lower_bound": 0.60,
            "esports_loss_count": 6,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "capital_weighted_edge": 0.0,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "excluded")
        self.assertIn("no_capital_edge", rated["reasons"])

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

    def test_low_volume_wallet_is_not_a_grade_even_when_perfect(self):
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

        self.assertEqual(rated["grade"], "C")
        self.assertIn("low_volume", rated["reasons"])

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
        self.assertEqual(result["grade"], "excluded")
        self.assertIn("negative_roi", result["reasons"])

    def test_high_frequency_only_candidate_gets_bot_like_penalty(self):
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

        self.assertEqual(result["bot_like_score"], 50)
        self.assertEqual(result["grade"], "C")

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

    def test_active_market_cache_fetches_esports_and_sports_tags(self):
        start = datetime.now(timezone.utc) + timedelta(hours=3)

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
                    }
                ]

        client = FakeClient()
        markets, _state, source = load_active_market_cache(
            client,
            {},
            now_ts=int(time.time()),
            gamma_pages=1,
            ttl_seconds=900,
        )

        self.assertEqual(source, "api")
        self.assertIn(("counter-strike-2", "league-of-legends", "dota-2"), client.tag_calls)
        self.assertIn(("nba", "ufc"), client.tag_calls)
        self.assertEqual(markets["esports-m1"]["category"], "esports")
        self.assertEqual(markets["sports-m1"]["category"], "sports")

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
        # 0xchurnok (4/10 high-churn) is kept; 0xchurn (6/10) exceeds the 0.5 churn gate.
        self.assertEqual(
            [row["wallet"] for row in filtered],
            ["0xgood", "0xchurnok", "0xlateok", "0xlate", "0xtailone"],
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

    def test_leaderboard_positive_rate_floor_skips_sub_eligible_wallets(self):
        # The blended-overall positive-rate floor must only gate legacy/overall grades.
        # A per-type sub-specialist (strong on its eligible bucket, weak overall because of
        # a different market type) already cleared the floor on its own type and must stay.
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
        self.assertNotIn("0xlegacy", wallets)  # overall-graded wallet still gated

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

        self.assertEqual(by_wallet["0xpass"]["eligible_market_types"], ["main_match"])
        self.assertIn("0xsportscapedge", by_wallet)
        # Pre-match entry rate is no longer a sports gate (aligned with esports): a low
        # pre-match-rate wallet still qualifies on capital edge alone.
        self.assertIn("0xsportslate", by_wallet)
        self.assertNotIn("0xsportsnegedge", by_wallet)
        self.assertNotIn("0xlowroi", by_wallet)
        self.assertNotIn("0xnegcapedge", by_wallet)
        self.assertIn("0xlate", by_wallet)
        self.assertEqual(by_wallet["0xlate"]["eligible_market_types"], ["main_match"])

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

    def test_leaderboard_is_rebuilt_from_all_profiles_not_only_current_candidates(self):
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
            "0xnew": {
                "wallet": "0xnew",
                "grade": "B",
                "esports_roi": 0.31,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xbad": {
                "wallet": "0xbad",
                "grade": "C",
                "esports_roi": 0.50,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(profiles_by_wallet, now_ts=100 + 10 * 86400)

        self.assertEqual([row["wallet"] for row in leaderboard], ["0xold"])

    def test_leaderboard_requires_recent_discovery_participation(self):
        profiles_by_wallet = {
            "0xactive": {
                "wallet": "0xactive",
                "grade": "A",
                "esports_roi": 0.80,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 3, "avg_market_cash": 2_000},
            },
            "0xinactive": {
                "wallet": "0xinactive",
                "grade": "A",
                "esports_roi": 1.20,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 1, "avg_market_cash": 20_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 10 * 86400,
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
        profiles_by_wallet = {}
        for index in range(35):
            wallet = f"0x{index:040x}"
            profiles_by_wallet[wallet] = {
                "wallet": wallet,
                "grade": "A",
                "esports_roi": 0.30 + index / 100,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            }

        leaderboard = build_leaderboard_from_profiles(profiles_by_wallet, now_ts=100 + 10 * 86400)

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

    def test_old_scoring_profile_is_not_kept_on_current_leaderboard(self):
        profiles_by_wallet = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "scoring_version": SCORING_VERSION - 1,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xcurrent": {
                "wallet": "0xcurrent",
                "grade": "A",
                "scoring_version": SCORING_VERSION,
                "esports_roi": 0.30,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 10 * 86400,
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
                # copyable by following one entry -> excluded on high_churn rate.
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

    def test_follow_eligible_wallets_use_eligible_types_not_overall_positive_rate(self):
        rows = [
            {"wallet": "0xA", "grade": "A", "eligible_market_types": ["game_winner"], "positive_market_rate": 0.60, "last_esports_trade_at": 1000},
            {"wallet": "0xB", "grade": "A", "eligible_market_types": ["game_winner"], "positive_market_rate": 0.90, "last_esports_trade_at": 1000},
        ]

        eligible = eligible_follow_wallets(rows, now_ts=1000, recency_days=30)

        self.assertEqual([row["wallet"] for row in eligible], ["0xa", "0xb"])

    def test_follow_store_records_wallet_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")

            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            quarantined = store.load_wallet_quarantine()

            self.assertIn("0xa", quarantined)
            self.assertEqual(quarantined["0xa"]["reason"], "material_sell")

    def test_observed_performance_quarantine_events_require_enough_bad_samples(self):
        performance = {
            "wallets": {
                "0xBad": {"signals": 10, "wins": 4, "our_pnl": -1.2},
                "0xThin": {"signals": 9, "wins": 0, "our_pnl": -2.0},
                "0xGood": {"signals": 10, "wins": 10, "our_pnl": 1.0},
            }
        }

        events = observed_performance_quarantine_events(performance, now_ts=500)

        self.assertEqual(events, [{"wallet": "0xbad", "reason": "observed_paper_underperformance", "timestamp": 500}])

    def test_follow_store_clears_only_revalidated_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            store.upsert_wallet_quarantine("0xB", reason="material_sell", ts=300)

            store.clear_revalidated_quarantine({"0xa", "0xb"}, validated_at=200)
            quarantined = store.load_wallet_quarantine()

            self.assertNotIn("0xa", quarantined)
            self.assertIn("0xb", quarantined)

    def test_follow_event_gate_detects_imminent_esports_start(self):
        now = 1000
        markets = [
            {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(now + 2 * 3600, timezone.utc).isoformat()},
            {"condition_id": "m2", "match_start_time": datetime.fromtimestamp(now + 20 * 3600, timezone.utc).isoformat()},
        ]

        self.assertTrue(esports_match_imminent(markets, now_ts=now, horizon_hours=12))
        self.assertFalse(esports_match_imminent(markets, now_ts=now + 3 * 3600, horizon_hours=12))
        self.assertTrue(esports_match_imminent([{"condition_id": "m3"}], now_ts=now, horizon_hours=12))

    def test_follow_position_diff_cold_start_then_new_position(self):
        positions = [{"conditionId": "m1", "outcomeIndex": 0, "size": 10}]

        new_positions, snapshot, cold_start = detect_new_positions(None, positions)

        self.assertTrue(cold_start)
        self.assertEqual(new_positions, [])
        self.assertEqual(snapshot, ["m1:0"])

        next_positions = [*positions, {"conditionId": "m2", "outcomeIndex": 1, "size": 5}]
        new_positions, snapshot, cold_start = detect_new_positions(snapshot, next_positions)

        self.assertFalse(cold_start)
        self.assertEqual([row["conditionId"] for row in new_positions], ["m2"])
        self.assertEqual(snapshot, ["m1:0", "m2:1"])

    def test_follow_qualifies_only_before_match_start(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "closed": False,
        }
        position = {"conditionId": "m1", "outcomeIndex": 1, "size": 10, "avgPrice": 0.45}

        qualified = qualify_follow(position, market, now_ts=now)
        rejected = qualify_follow(position, market, now_ts=now + 7200)

        self.assertTrue(qualified["qualified"])
        self.assertEqual(qualified["outcome"], "B")
        self.assertFalse(rejected["qualified"])
        self.assertEqual(rejected["reason"], "after_match_start")

    def test_follow_unqualified_retry_policy_keeps_only_transient_reasons(self):
        self.assertTrue(should_retry_unqualified_position("unknown_market"))
        self.assertFalse(should_retry_unqualified_position("after_match_start"))
        self.assertFalse(should_retry_unqualified_position("closed_market"))
        self.assertFalse(should_retry_unqualified_position("unknown_outcome"))

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

    def test_watched_markets_only_include_future_starts(self):
        now = 1000
        markets = {
            "past": {"condition_id": "past", "match_start_time": datetime.fromtimestamp(now - 60, timezone.utc).isoformat()},
            "future": {"condition_id": "future", "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()},
            "far": {"condition_id": "far", "match_start_time": datetime.fromtimestamp(now + 30 * 3600, timezone.utc).isoformat()},
        }

        watched = watched_markets(markets, now_ts=now, observe_window_hours=24)

        self.assertEqual(list(watched), ["future"])

    def test_watched_markets_can_include_recently_started_for_delayed_trades(self):
        now = 1000
        markets = {
            "recent": {"condition_id": "recent", "match_start_time": datetime.fromtimestamp(now - 120, timezone.utc).isoformat()},
            "old": {"condition_id": "old", "match_start_time": datetime.fromtimestamp(now - 1200, timezone.utc).isoformat()},
            "future": {"condition_id": "future", "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()},
        }

        watched = watched_markets(markets, now_ts=now, observe_window_hours=24, post_start_grace_seconds=300)

        self.assertEqual(list(watched), ["recent", "future"])

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

    def test_follow_stake_conviction_tiers_gate_and_balance(self):
        # Wallet whose big bets win more (capWin >= win): a bet >= 2x its own typical size
        # upsizes to the conviction stake.
        informative = {
            "median_position_size": 500,
            "capital_weighted_win_rate": 0.74,
            "positive_market_rate": 0.66,
        }
        stake, tier, ratio = follow_stake_for_signal(
            informative, wallet_trade_size=5000, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=1000,
        )
        self.assertEqual((stake, tier), (5, "conviction"))
        self.assertEqual(ratio, 10.0)  # 5000 / 500, judged against the wallet's OWN typical

        # Same big bet, but this wallet's big bets win LESS (capWin < win) -> gate keeps it normal.
        uninformative = {**informative, "capital_weighted_win_rate": 0.60, "positive_market_rate": 0.70}
        stake, tier, _ = follow_stake_for_signal(
            uninformative, wallet_trade_size=5000, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=1000,
        )
        self.assertEqual((stake, tier), (1, "normal"))

        # Normal-sized bet (< 2x typical) -> normal stake.
        stake, tier, _ = follow_stake_for_signal(
            informative, wallet_trade_size=600, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=1000,
        )
        self.assertEqual((stake, tier), (1, "normal"))

        # Balance can't cover the conviction stake -> downgrade to normal.
        stake, tier, _ = follow_stake_for_signal(
            informative, wallet_trade_size=5000, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=3,
        )
        self.assertEqual((stake, tier), (1, "downgraded"))

        # Balance can't even cover the normal stake -> skip.
        stake, tier, _ = follow_stake_for_signal(
            informative, wallet_trade_size=5000, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=0.5,
        )
        self.assertEqual((stake, tier), (0.0, "skipped"))

    def test_follow_stake_uses_per_type_typical_and_missing_falls_back(self):
        row = {
            "median_position_size": 5000,  # overall typical is large
            "capital_weighted_win_rate": 0.8,
            "positive_market_rate": 0.6,
            "per_type": {"game_winner": {"median_position_size": 200, "capital_weighted_win_rate": 0.8, "positive_market_rate": 0.6}},
        }
        # 600 is only 0.12x the overall typical, but 3x the game_winner typical -> conviction.
        stake, tier, ratio = follow_stake_for_signal(
            row, wallet_trade_size=600, market_type="game_winner",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=1000,
        )
        self.assertEqual((stake, tier), (5, "conviction"))
        self.assertEqual(ratio, 3.0)
        # No typical size known -> stays normal (never upsize on unknown conviction).
        stake, tier, _ = follow_stake_for_signal(
            {}, wallet_trade_size=5000, market_type="main_match",
            normal_stake=1, conviction_stake=5, conviction_multiple=2.0,
            conviction_gate=True, available_balance=1000,
        )
        self.assertEqual((stake, tier), (1, "normal"))

    def test_process_follow_trades_applies_conviction_stake_to_signal(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 5000, "timestamp": now}]
        wallet_row = {"median_position_size": 500, "capital_weighted_win_rate": 0.74, "positive_market_rate": 0.66}
        signals, _ = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, max_follow_legs=10, max_slippage=0.05,
            wallet_row=wallet_row, conviction_stake_usdc=5, conviction_size_multiple=2.0,
            conviction_gate=True, bankroll_usdc=1000,
        )
        self.assertEqual(signals[0]["conviction_tier"], "conviction")
        self.assertEqual(signals[0]["signal_stake"], 5)
        self.assertEqual(signals[0]["legs"][0]["stake"], 5)

    def test_process_follow_trades_caps_conviction_adds_by_bankroll(self):
        now = 1000
        market = {
            "condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.5, 0.5],
            "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat(),
            "title": "Match", "market_type": "main_match",
        }
        trades = [
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 5000, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.5, "size": 5000, "timestamp": now + 1},
        ]
        wallet_row = {"median_position_size": 500, "capital_weighted_win_rate": 0.74, "positive_market_rate": 0.66}

        signals, stats = process_follow_trades(
            [], wallet="0xA", trades=trades, markets_by_condition={"m1": market}, now_ts=now,
            stake_usdc=1, max_follow_legs=10, max_slippage=0.05,
            wallet_row=wallet_row, conviction_stake_usdc=5, conviction_size_multiple=2.0,
            conviction_gate=True, bankroll_usdc=6,
        )

        self.assertEqual([leg["stake"] for leg in signals[0]["legs"]], [5, 1])
        self.assertEqual(stats["insufficient_balance_count"], 1)

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
            {"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10, "timestamp": now},
            {"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.46, "size": 5, "timestamp": now + 1},
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
            trades=[{"id": "s1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "SELL", "price": 0.61, "size": 15, "timestamp": now + 2}],
            markets_by_condition={"m1": sell_market},
            now_ts=now + 2,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["exited_signal_count"], 1)
        self.assertEqual(signals[0]["status"], "exited")
        self.assertEqual(signals[0]["wallet_sell_size"], 15)
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
            "size": 10,
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
            "size": 10,
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
            "size": 10,
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

    def test_follow_v2_gates_sports_new_signals_by_league(self):
        now = 1000
        trade = {
            "id": "b1",
            "proxyWallet": "0xA",
            "market": "ufc1",
            "outcomeIndex": 0,
            "side": "BUY",
            "price": 0.40,
            "size": 10,
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

    def test_follow_v4_dust_sell_mirror_exits_without_quarantine(self):
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

        self.assertEqual(stats["exited_signal_count"], 1)
        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(signals[0]["status"], "exited")
        self.assertEqual(signals[0]["wallet_sell_size"], 5)

    def test_follow_v4_cumulative_sells_can_trigger_quarantine(self):
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

        self.assertEqual(stats["quarantine_events"][0]["reason"], "material_sell")
        self.assertEqual(signals[0]["wallet_sell_size"], 25)

    def test_follow_v2_bootstraps_existing_position_when_price_still_acceptable(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.49, 0.51],
            "match_start_time": datetime.fromtimestamp(now + 7200, timezone.utc).isoformat(),
        }
        positions = [
            {"conditionId": "m1", "outcomeIndex": 0, "size": 100, "avgPrice": 0.45},
            {"conditionId": "m2", "outcomeIndex": 0, "size": 100, "avgPrice": 0.45},
        ]

        trades = bootstrap_position_trades(
            positions,
            wallet="0xA",
            markets_by_condition={"m1": market},
            now_ts=now,
            max_slippage=0.05,
        )

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["side"], "BUY")
        self.assertEqual(trades[0]["price"], 0.45)
        self.assertTrue(trades[0]["bootstrap_position"])

    def test_follow_v2_does_not_bootstrap_when_current_price_too_far(self):
        now = 1000
        market = {
            "condition_id": "m1",
            "outcomes": ["A", "B"],
            "outcome_prices": [0.6, 0.4],
            "match_start_time": datetime.fromtimestamp(now + 7200, timezone.utc).isoformat(),
        }
        positions = [{"conditionId": "m1", "outcomeIndex": 0, "size": 100, "avgPrice": 0.45}]

        trades = bootstrap_position_trades(
            positions,
            wallet="0xA",
            markets_by_condition={"m1": market},
            now_ts=now,
            max_slippage=0.05,
        )

        self.assertEqual(trades, [])

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
            trades=[{"id": "b1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xA",
            trades=[{"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.48, "size": 1, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["opposite_blocked_count"], 1)
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
            trades=[{"id": "a1", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.50, "size": 10, "timestamp": now}],
            markets_by_condition={"m1": market},
            now_ts=now,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        signals, stats = process_follow_trades(
            signals,
            wallet="0xB",
            trades=[{"id": "b1", "proxyWallet": "0xB", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.46, "size": 10, "timestamp": now + 1}],
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
                        "price": 0.45,
                        "size": 10,
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
            trades=[{"id": "c1", "proxyWallet": "0xC", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.48, "size": 10, "timestamp": now + 1}],
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

    def test_v4_material_sell_and_quarantine_reason(self):
        signal = {"outcome_index": 0, "legs": [{"wallet_trade_size": 100}, {"wallet_trade_size": 50}]}
        dust_sell = {"side": "SELL", "size": 10}
        big_sell = {"side": "SELL", "size": 40}
        opposite_buy = {"side": "BUY", "outcomeIndex": 1}

        self.assertFalse(material_sell(signal, dust_sell, sell_frac=0.2))
        self.assertTrue(material_sell(signal, big_sell, sell_frac=0.2))
        self.assertEqual(quarantine_reason(signal, big_sell, sell_frac=0.2), "material_sell")
        self.assertEqual(quarantine_reason(signal, opposite_buy, sell_frac=0.2), "two_sided_switch")

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

    def test_follow_fill_summary_slippage_and_signal_dedup(self):
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

        market = {"condition_id": "m1", "outcomes": ["A", "B"], "outcome_prices": [0.53, 0.47], "title": "M"}
        qualification = {
            "condition_id": "m1",
            "outcome_index": 0,
            "outcome": "A",
            "wallet_avg_price": 0.475,
            "position_size": 40,
        }
        signals, created = upsert_follow_signal(
            [],
            wallet="0xA",
            market=market,
            qualification=qualification,
            fills_summary=fills,
            current_price=0.53,
            max_slippage=0.05,
            stake_usdc=100,
            now_ts=100,
        )
        signals, second_created = upsert_follow_signal(
            signals,
            wallet="0xB",
            market=market,
            qualification={**qualification, "wallet_avg_price": 0.49},
            fills_summary=fills,
            current_price=0.54,
            max_slippage=0.05,
            stake_usdc=100,
            now_ts=110,
        )

        self.assertTrue(created)
        self.assertFalse(second_created)
        self.assertEqual(len(signals), 1)
        self.assertEqual(len(signals[0]["triggered_by"]), 2)
        self.assertEqual(signals[0]["our_entry_price"], 0.53)
        self.assertFalse(signals[0]["would_follow"])

    def test_follow_open_signal_refresh_preserves_entry_and_updates_fills(self):
        class FakeClient:
            def trades_for_user_market(self, wallet, condition_id, *, limit=500, offset=0):
                return [
                    {"proxyWallet": wallet, "outcomeIndex": 0, "price": 0.4, "size": 10, "timestamp": 1},
                    {"proxyWallet": wallet, "outcomeIndex": 0, "price": 0.6, "size": 10, "timestamp": 2},
                ]

        now = 1000
        start = datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()
        signals = [
            {
                "signal_id": "m1:0",
                "condition_id": "m1",
                "outcome_index": 0,
                "outcome": "A",
                "wallet_avg_price": 0.4,
                "our_entry_price": 0.45,
                "current_price": 0.45,
                "stake_usdc": 100,
                "would_follow": True,
                "slippage_over_wallet_entry": 0.05,
                "triggered_by": [{"wallet": "0xA", "wallet_avg_price": 0.4, "position_size": 10}],
            }
        ]
        markets = {
            "m1": {
                "condition_id": "m1",
                "outcomes": ["A", "B"],
                "outcome_prices": [0.7, 0.3],
                "match_start_time": start,
            }
        }

        refreshed, count = refresh_open_signal_fills(
            FakeClient(),
            signals,
            markets,
            now_ts=now,
            max_slippage=0.05,
        )

        self.assertEqual(count, 1)
        self.assertEqual(refreshed[0]["our_entry_price"], 0.45)
        self.assertEqual(refreshed[0]["current_price"], 0.7)
        self.assertTrue(refreshed[0]["would_follow"])
        self.assertEqual(refreshed[0]["triggered_by"][0]["fills_summary"]["fill_count"], 2)

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

    def test_follow_resolution_lookup_uses_sqlite_closed_cache(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def list_events_paginated(self, **_kwargs):
                self.calls += 1
                return []

        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            store.save_market_cache(
                {"m1": {"condition_id": "m1", "outcome_prices": [1, 0]}},
                cache_kind="closed",
                updated_at=900,
            )
            signal = {"condition_id": "m1", "match_start_time": datetime.fromtimestamp(500, timezone.utc).isoformat()}
            client = FakeClient()

            resolutions = fetch_resolutions_for_open_signals(
                client,
                [signal],
                state={},
                store=store,
                now_ts=1000,
                gamma_pages=3,
                ttl_seconds=900,
            )

            self.assertEqual(resolutions, {"m1": 0})
            self.assertEqual(client.calls, 0)

    def test_follow_tick_does_not_write_performance_json(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            write_json(
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
            self.assertTrue(follow_run_log_path(data_dir).exists())

    def test_dashboard_health_reads_new_and_legacy_follow_logs(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            custom_log_dir = Path(tmp) / "local_logs"
            write_jsonl(data_dir / "follow" / "follow_run_log.jsonl", [{"created_at": 100, "desired_next_interval_seconds": 10}])
            write_jsonl(custom_log_dir / "follow" / "follow_run_log.jsonl", [{"created_at": 200, "desired_next_interval_seconds": 20}])
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[],
                result_events=[],
                performance={},
            )

            health = build_health(data_dir, started_at=time.time(), log_dir=custom_log_dir)
            signal = read_stream_signal(data_dir, log_dir=custom_log_dir)

            self.assertEqual(health["last_tick_at"], 200)
            self.assertGreater(signal.run_log_mtime, 0)

    def test_follow_tick_preloads_team_logos_from_watched_events(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            write_json(data_dir / "smart_wallet_leaderboard.json", [])

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

    def test_dashboard_events_parse_sports_matchups_and_team_logos(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            start = datetime.now(timezone.utc) + timedelta(hours=2)
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": int(time.time()),
                    "markets": [
                        {
                            "condition_id": "mlb1",
                            "category": "sports",
                            "league": "nba",
                            "league_label": "NBA",
                            "title": "Los Angeles Dodgers vs. San Diego Padres",
                            "question": "Los Angeles Dodgers vs. San Diego Padres",
                            "match_start_time": start.isoformat(),
                            "end_date": (start + timedelta(hours=3)).isoformat(),
                            "market_type_label": "Moneyline",
                        }
                    ],
                },
            )
            with patch.object(
                dashboard_module,
                "_load_team_logo_cache",
                return_value={
                    "nba los angeles dodgers": "/team_logos/dodgers.png",
                    "nba san diego padres": "/team_logos/padres.png",
                },
            ):
                events = build_events(data_dir)

            self.assertEqual(events["events"][0]["category"], "sports")
            self.assertEqual(
                events["events"][0]["match_parts"],
                {
                    "game": "NBA",
                    "teamA": "Los Angeles Dodgers",
                    "teamB": "San Diego Padres",
                    "meta": "Moneyline",
                },
            )
            self.assertEqual(events["events"][0]["team_logos"]["teamA"], "/team_logos/dodgers.png")
            self.assertEqual(events["events"][0]["team_logos"]["teamB"], "/team_logos/padres.png")

    def test_sports_watched_events_missing_logos_trigger_refresh_scan(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            logo_dir = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "team_logos"
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
                self.assertTrue(logo_cache["teams"]["pytest sample kings"].startswith("/team_logos/"))
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
            write_json(
                data_dir / "smart_wallet_leaderboard.json",
                [{"wallet": "0xabc", "grade": "A", "last_esports_trade_at": int(now.timestamp())}],
            )
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine("0xabc", reason="material_sell", ts=int(now.timestamp()))
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
            write_json(data_dir / "smart_wallet_leaderboard.json", [])
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
            write_json(
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

    def test_follow_tick_uses_sports_wallet_league_scope(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            write_json(
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

            self.assertEqual(wallets, ["0xa"])
            self.assertEqual(summary["new_signal_count"], 1, summary)
            self.assertEqual(summary["ignored_trade_count"], 1)
            self.assertEqual(snapshot["open_signals"][0]["condition_id"], "nba1")
            self.assertEqual(snapshot["open_signals"][0]["league"], "nba")

    def test_follow_tick_dual_follows_opposite_wallet_buys(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now = datetime.now(timezone.utc)
            start = now + timedelta(hours=2)
            write_json(
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
                        return [{"id": "a1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10}]
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.45, "size": 10}]

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
            write_json(
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
                    return [{"id": "b1", "timestamp": 20, "market": "m1", "outcomeIndex": 0, "side": "BUY", "price": 0.45, "size": 10}]

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
            run_log = read_jsonl(follow_run_log_path(data_dir))

            self.assertEqual(summary["new_signal_count"], 0)
            self.assertEqual(summary["insufficient_balance_count"], 1)
            self.assertEqual(run_log[-1]["insufficient_balance_count"], 1)

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
                self.assertIn("Aegis Alpha", body)
                self.assertIn("/app.js", body)
                self.assertIn("/vendor/vue-3.5.13.global.prod.js", body)
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

            status = start_runner(config)

            self.assertEqual(status["status"], "running")
            self.assertEqual(status["pid"], 4321)
            self.assertIn("run", calls[0][0])
            self.assertIn("--stake-usdc", calls[0][0])
            self.assertIn("--log-dir", calls[0][0])
            self.assertIn("--follow-dir", calls[0][0])
            self.assertIn(str(follow_dir), calls[0][0])
            self.assertIn("--skip-initial-build", calls[0][0])
            self.assertEqual(calls[0][1].parent, data_dir / "logs" / "follow")
            self.assertEqual(read_follow_control(follow_dir)["runner"]["pid"], 4321)

    def test_dashboard_runner_start_accepts_required_stake(self):
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
                stake_usdc=2.5,
            )

            stake_index = calls[0][0].index("--stake-usdc") + 1
            self.assertEqual(calls[0][0][stake_index], "2.5")
            self.assertEqual(status["stake_usdc"], 2.5)
            self.assertEqual(read_follow_control(follow_dir)["runner"]["stake_usdc"], 2.5)

    def test_dashboard_wallet_refresh_is_category_scoped(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "data"
            follow_dir = root / "follow"
            stale_esports = root / "esports" / "smart_wallet_leaderboard.json"
            stale_sports = root / "sports" / "smart_wallet_leaderboard.json"
            sports_cache = root / "sports" / "raw_user_trades" / "0xabc.json"
            stale_sports_profile = root / "sports" / "wallet_profiles.json"
            stale_esports.parent.mkdir(parents=True)
            stale_sports.parent.mkdir(parents=True)
            sports_cache.parent.mkdir(parents=True)
            stale_esports.write_text("old esports", encoding="utf-8")
            stale_sports.write_text("old sports", encoding="utf-8")
            stale_sports_profile.write_text("old profiles", encoding="utf-8")
            sports_cache.write_text("cached trades", encoding="utf-8")
            preserved_performance = {
                "wallets": {"0xabc": {"signals": 2, "our_pnl": 3.5}},
                "total": {"signals": 2, "resolved_stake": 10, "our_pnl": 3.5},
                "updated_at": 1234,
            }
            FollowStore(follow_dir / "follow.db").save_follow_snapshot(
                wallet_trade_state={"sports:0xabc": {"last_seen_at": 1}},
                open_signals=[],
                result_events=[],
                performance=preserved_performance,
            )
            calls = []
            ran = threading.Event()

            def fake_runner(category, data_dir_arg, log_path):
                calls.append((category, data_dir_arg, log_path))
                pause = read_follow_control(follow_dir).get("pause_new_signals", {})
                self.assertEqual(pause.get("sports", {}).get("status"), "paused")
                self.assertEqual(pause.get("sports", {}).get("reason"), "wallet_refresh")
                self.assertNotIn("esports", pause)
                self.assertFalse(stale_sports.exists())
                self.assertFalse(stale_sports_profile.exists())
                self.assertTrue(sports_cache.exists())
                self.assertTrue(stale_esports.exists())
                ran.set()
                return 0

            status = start_wallet_refresh(root, category="sports", follow_dir=follow_dir, runner=fake_runner)

            self.assertEqual(status["category"], "sports")
            self.assertIn("--category", status["command"])
            self.assertIn("sports", status["command"])
            self.assertIn(str(root / "sports"), status["command"])
            self.assertTrue(ran.wait(2))
            deadline = time.time() + 2
            control = read_follow_control(follow_dir)
            while control.get("wallet_refresh", {}).get("sports", {}).get("status") == "running" and time.time() < deadline:
                time.sleep(0.01)
                control = read_follow_control(follow_dir)
            self.assertEqual(calls[0][0], "sports")
            self.assertEqual(calls[0][1], root / "sports")
            self.assertFalse(stale_sports.exists())
            self.assertTrue(stale_esports.exists())
            self.assertEqual(control["wallet_refresh"]["sports"]["status"], "succeeded")
            self.assertNotIn("pause_new_signals", control)
            self.assertNotIn("esports", control["wallet_refresh"])
            self.assertEqual(FollowStore(follow_dir / "follow.db").load_performance(), preserved_performance)

    def test_prepare_category_refresh_dir_preserves_and_prunes_caches(self):
        with TemporaryDirectory() as tmp:
            category_dir = Path(tmp) / "sports"
            output_file = category_dir / "smart_wallet_leaderboard.json"
            profile_file = category_dir / "wallet_profiles.json"
            classification_file = category_dir / "esports_classification_set.json"
            fresh_cache = category_dir / "raw_market_trades" / "fresh.json"
            old_cache = category_dir / "raw_user_trades" / "old.json"
            old_nested_cache = category_dir / "clob_market_metadata" / "nested" / "old.json"
            for path in [output_file, profile_file, classification_file, fresh_cache, old_cache, old_nested_cache]:
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
                data_dir / "sports" / "wallet_profiles.json",
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
            self.assertTrue((data_dir / "sports").is_dir())
            self.assertTrue(follow_dir.is_dir())
            self.assertTrue((log_dir / "follow").is_dir())
            self.assertEqual(list((data_dir / "esports").iterdir()), [])
            self.assertEqual(list((data_dir / "sports").iterdir()), [])
            self.assertEqual(list(follow_dir.iterdir()), [])
            self.assertEqual(list((log_dir / "follow").iterdir()), [])

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

    def test_dashboard_runner_start_api_requires_stake(self):
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
                self.assertEqual(payload["error"], "invalid_stake_usdc")
                self.assertEqual(calls, [])

                conn = http.client.HTTPConnection(host, port, timeout=5)
                conn.request(
                    "POST",
                    "/api/runner/start",
                    body=json.dumps({"stake_usdc": 2.5}),
                    headers={"Cookie": cookie, "Content-Type": "application/json"},
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()
                self.assertEqual(response.status, 202)
                self.assertEqual(payload["data"]["stake_usdc"], 2.5)
                self.assertIn("--stake-usdc", calls[0][0])
                self.assertEqual(calls[0][0][calls[0][0].index("--stake-usdc") + 1], "2.5")
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
            self.assertEqual(overview["realized_roi"], 0.8)
            self.assertEqual(overview["wallet_basis_realized_roi"], 1.0)
            self.assertAlmostEqual(overview["delay_cost"], 0.2)

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
                        "status": "open",
                        "contested": True,
                        "wallet_clv": 0.12,
                        "legs": [{"stake": 1, "would_follow": False}],
                    },
                    {
                        "signal_id": "m2:0",
                        "wallet": "0xb",
                        "condition_id": "m2",
                        "status": "open",
                        "wallet_clv": 0.08,
                        "legs": [{"stake": 1, "would_follow": True}],
                    },
                ],
                result_events=[],
                performance={},
            )

            overview = build_overview(data_dir)

            self.assertEqual(overview["contested_signal_count"], 1)
            self.assertEqual(overview["clean_signal_count"], 1)
            self.assertEqual(overview["avg_wallet_clv"], 0.1)

    def test_dashboard_overview_exposes_category_breakdown(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {"signal_id": "e:m1:0", "wallet": "0xe", "condition_id": "m1", "status": "open", "category": "esports", "legs": [{"stake": 1}]},
                    {"signal_id": "s:m2:0", "wallet": "0xs", "condition_id": "m2", "status": "open", "category": "sports", "legs": [{"stake": 1}]},
                ],
                result_events=[
                    {"signal_id": "r:m3:0", "wallet": "0xr", "condition_id": "m3", "status": "settled", "category": "sports", "our_paper_pnl": 0.5, "legs": [{"stake": 1}]}
                ],
                performance={"wallets": {}, "total": {}},
            )

            overview = build_overview(data_dir)

            self.assertEqual(overview["by_category"]["esports"]["open_signal_count"], 1)
            self.assertEqual(overview["by_category"]["sports"]["open_signal_count"], 1)
            self.assertEqual(overview["by_category"]["sports"]["result_count"], 1)

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
            self.assertEqual(page["follows"][0]["condition_id"], "m1")

            open_page = build_follows(data_dir, page=1, size=10, status="open")
            settled_page = build_follows(data_dir, page=1, size=10, status="settled")

            self.assertEqual(open_page["total"], 1)
            self.assertEqual(open_page["status"], "open")
            self.assertEqual(open_page["follows"][0]["condition_id"], "m2")
            self.assertEqual(settled_page["total"], 1)
            self.assertEqual(settled_page["status"], "settled")
            self.assertEqual(settled_page["follows"][0]["condition_id"], "m1")

    def test_dashboard_follows_filter_by_category(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            FollowStore(data_dir / "follow" / "follow.db").save_follow_snapshot(
                wallet_trade_state={},
                open_signals=[
                    {"signal_id": "e:m1:0", "wallet": "0xe", "condition_id": "m1", "status": "open", "category": "esports", "created_at": 100, "legs": []},
                    {"signal_id": "s:m2:0", "wallet": "0xs", "condition_id": "m2", "status": "open", "category": "sports", "created_at": 200, "legs": []},
                ],
                result_events=[],
                performance={},
            )

            sports_page = build_follows(data_dir, page=1, size=10, category="sports")

            self.assertEqual(sports_page["category"], "sports")
            self.assertEqual(sports_page["total"], 1)
            self.assertEqual(sports_page["follows"][0]["condition_id"], "m2")

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

    def test_dashboard_follows_expose_conviction_stake_fields(self):
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
                        "conviction_tier": "conviction",
                        "conviction_ratio": 3.25,
                        "signal_stake": 5,
                        "created_at": 100,
                        "legs": [{"stake": 5}, {"stake": 1}],
                    }
                ],
                result_events=[],
                performance={"wallets": {}, "total": {}},
            )

            page = build_follows(data_dir, page=1, size=10)
            detail = build_follow_detail(data_dir, "m1")

            row = page["follows"][0]
            self.assertEqual(row["conviction_tier_counts"], {"conviction": 1})
            self.assertEqual(row["signal_stake_min"], 5.0)
            self.assertEqual(row["signal_stake_max"], 5.0)
            self.assertEqual(detail["wallets"][0]["signals"][0]["conviction_tier"], "conviction")
            self.assertEqual(detail["wallets"][0]["signals"][0]["signal_stake"], 5)

    def test_dashboard_rows_include_cached_team_logos(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            static_logo_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "team_logos" / "team_logos.json"
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
            write_json(
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
            static_logo_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "team_logos" / "team_logos.json"
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
            self.assertTrue(logos["teams"]["legacy"].startswith("/team_logos/"))
            self.assertTrue(logos["teams"]["mibr"].startswith("/team_logos/"))
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
            write_json(
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
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": 100,
                    "markets": [
                        {
                            "condition_id": "m1",
                            "outcomes": ["A", "B"],
                            "outcome_prices": [0.4, 0.6],
                        }
                    ],
                },
            )

            client = FakeClient()
            result = fetch_market_prices(data_dir, client, "m1")
            cached = read_json(data_dir / "follow" / "active_market_cache.json", {})

            self.assertEqual(client.path, "/markets")
            self.assertEqual(client.params["condition_ids"], "m1")
            self.assertEqual(result["outcomes"], ["A", "B"])
            self.assertEqual(result["outcome_prices"], [0.44, 0.56])
            self.assertEqual(cached["markets"][0]["outcome_prices"], [0.44, 0.56])

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

    def test_dashboard_wallets_expose_quarantine_state(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_json(data_dir / "smart_wallet_leaderboard.json", [{"wallet": "0xabc", "grade": "A"}])
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.upsert_wallet_quarantine("0xabc", reason="material_sell", ts=100)

            wallets = build_wallets(data_dir)

            self.assertEqual(wallets["quarantined_count"], 1)
            self.assertTrue(wallets["wallets"][0]["quarantined"])
            self.assertEqual(wallets["wallets"][0]["quarantine"]["reason"], "material_sell")

    def test_dashboard_wallets_use_observed_follow_trade_time(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_json(
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
            write_json(
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
            self.assertEqual(row["esports_win_count"], 11)
            self.assertEqual(row["esports_loss_count"], 1)
            self.assertEqual(row["wilson_win_rate_lower_bound"], 0.72)
            self.assertEqual(row["eligible_market_type_labels"], ["单局"])
            self.assertEqual(row["eligible_market_types"], ["game_winner"])
            self.assertEqual(row["observed_market_types"], ["main_match", "game_winner"])
            self.assertEqual(row["observed_market_type_labels"], ["主盘", "单局"])

    def test_dashboard_wallets_surface_local_follow_losses_and_rank_them_back(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_json(
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

            self.assertEqual([row["wallet"] for row in wallets["wallets"]], ["0xclean", "0xloser"])
            self.assertEqual([row["rank"] for row in wallets["wallets"]], [1, 2])
            loser = wallets["wallets"][1]
            self.assertEqual(loser["esports_win_count"], 8)
            self.assertEqual(loser["esports_loss_count"], 0)
            self.assertEqual(loser["observed"]["signals"], 1)
            self.assertEqual(loser["observed"]["losses"], 1)
            self.assertEqual(loser["observed"]["our_pnl"], -1.0)
            self.assertTrue(loser["observed"]["has_loss"])

    def test_dashboard_wallet_ranks_are_category_scoped(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            esports_rows = [
                {
                    "wallet": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee1",
                    "category": "esports",
                    "grade": "A",
                    "esports_win_count": 8,
                    "esports_loss_count": 0,
                    "positive_market_rate": 1.0,
                    "wilson_win_rate_lower_bound": 0.9 - index * 0.01,
                    "entry_edge": 0.3,
                    "esports_roi": 0.8,
                }
                for index in range(3)
            ]
            sports_rows = [
                {
                    "wallet": "0xsssssssssssssssssssssssssssssssssssssss1".replace("s", hex(index + 10)[2]),
                    "category": "sports",
                    "league": "nba" if index < 2 else "ufc",
                    "league_label": "NBA" if index < 2 else "UFC",
                    "participation_rate": 0.5 + index * 0.1,
                    "participated_events": 5 + index,
                    "eligible_event_count": 10,
                    "grade": "A",
                    "esports_win_count": 8,
                    "esports_loss_count": 0,
                    "positive_market_rate": 1.0,
                    "wilson_win_rate_lower_bound": 0.7 - index * 0.01,
                    "entry_edge": 0.2,
                    "esports_roi": 0.7,
                }
                for index in range(3)
            ]
            write_json(data_dir / "esports" / "smart_wallet_leaderboard.json", esports_rows)
            write_json(data_dir / "sports" / "smart_wallet_leaderboard.json", sports_rows)
            FollowStore(data_dir / "follow" / "follow.db")

            wallets = build_wallets(data_dir)

            ranks_by_category = {
                category: [row["rank"] for row in wallets["wallets"] if row["category"] == category]
                for category in ("esports", "sports")
            }
            self.assertEqual(ranks_by_category["esports"], [1, 2, 3])
            self.assertEqual(ranks_by_category["sports"], [1, 2, 3])
            self.assertEqual(wallets["by_category"]["esports"]["count"], 3)
            self.assertEqual(wallets["by_category"]["sports"]["count"], 3)
            first_sports = next(row for row in wallets["wallets"] if row["category"] == "sports")
            self.assertEqual(first_sports["league"], "nba")
            self.assertEqual(first_sports["league_label"], "NBA")
            self.assertEqual(first_sports["game"], "nba")
            self.assertEqual(first_sports["game_label"], "NBA")
            self.assertEqual(first_sports["participated_events"], 5)
            self.assertEqual(first_sports["eligible_event_count"], 10)
            self.assertEqual(first_sports["participation_rate"], 0.5)

    def test_dashboard_events_marks_outcome_index_zero_contested(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            write_json(
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
            write_json(
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

    def test_dashboard_events_include_sports_plus_zero_timezone_starts(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            start = datetime.fromtimestamp(now_ts + 3600, timezone.utc).isoformat().replace("+00:00", "+00")
            write_json(
                data_dir / "follow" / "active_market_cache.json",
                {
                    "updated_at": now_ts,
                    "markets": [
                        {
                            "condition_id": "sports_mlb",
                            "category": "sports",
                            "league": "ufc",
                            "league_label": "UFC",
                            "title": "Seattle Mariners vs. Baltimore Orioles",
                            "match_start_time": start,
                            "market_type": "main_match",
                            "market_type_label": "主盘",
                        }
                    ],
                },
            )

            events = build_events(data_dir)

            self.assertEqual(events["count"], 1)
            self.assertEqual(events["events"][0]["condition_id"], "sports_mlb")
            self.assertEqual(events["events"][0]["category"], "sports")
            self.assertEqual(events["events"][0]["league"], "ufc")
            self.assertEqual(events["events"][0]["league_label"], "UFC")
            self.assertEqual(events["events"][0]["match_parts"]["game"], "UFC")

    def test_dashboard_events_sort_followed_markets_first(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            now_ts = int(time.time())
            write_json(
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
            write_json(
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
            write_json(
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
                "our_entry_price": 0.6,
                "stake_usdc": 100,
                "triggered_by": [
                    {
                        "wallet": "0xA",
                        "wallet_avg_price": 0.5,
                        "fills_summary": {"fills": [{"price": 0.5, "size": 1}], "avg_price": 0.5},
                    }
                ],
            }
        ]

        remaining, settled = settle_open_signals(signals, {"m1": 0}, now_ts=200)
        perf = aggregate_follow_performance({}, settled)

        self.assertEqual(remaining, [])
        self.assertEqual(settled[0]["wallet_paper_pnl_by_wallet"]["0xa"], 100)
        self.assertEqual(round(settled[0]["our_paper_pnl"], 8), 66.66666667)
        self.assertNotIn("fills", settled[0]["triggered_by"][0]["fills_summary"])
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

    def test_collect_command_uses_build_leaderboard_handler(self):
        parser = build_parser()

        args = parser.parse_args(["collect"])

        self.assertEqual(args.command, "collect")
        self.assertEqual(args.discovery_source, "trades")
        self.assertEqual(args.target_markets, 20)
        self.assertIsNone(args.max_markets_per_run)
        self.assertIsNone(args.discovery_lookback_days)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_count, 2)
        self.assertIsNone(args.market_batch_index)
        self.assertEqual(args.market_offset, 0)
        self.assertEqual(args.max_pages_per_market, 3)
        self.assertEqual(args.max_profiles_per_run, 150)
        self.assertEqual(args.max_workers, 8)
        self.assertEqual(args.max_requests_per_second, 10)
        self.assertEqual(args.request_burst, 5)
        self.assertEqual(args.max_retry_after_seconds, 60)
        self.assertIsNone(args.classification_lookback_days)
        self.assertEqual(args.sports_nba_target_markets, 80)
        self.assertEqual(args.sports_ufc_target_markets, 80)
        self.assertEqual(args.sports_nba_min_market_volume, 250_000)
        self.assertEqual(args.sports_ufc_min_market_volume, 25_000)
        self.assertEqual(args.max_esports_closed_positions_per_wallet, 100)
        self.assertEqual(args.closed_position_market_chunk_size, 50)
        self.assertEqual(args.user_history_trades_limit, 500)
        self.assertEqual(args.user_history_trades_max_pages, 3)
        self.assertEqual(args.min_profile_participated_markets, 3)
        self.assertEqual(args.min_profile_avg_market_cash, 1_500)
        self.assertEqual(args.market_trades_cache_ttl_days, 7)
        self.assertFalse(args.refresh_market_trades)
        self.assertFalse(args.no_market_trades_cache)
        self.assertEqual(args.leaderboard_min_participated_markets, 3)
        self.assertEqual(args.leaderboard_min_avg_market_cash, 1_500)
        self.assertEqual(args.max_leaderboard_wallets, 30)
        self.assertFalse(args.check_current_positions)

    def test_sports_collect_uses_deeper_effective_history_defaults(self):
        parser = build_parser()

        esports = parser.parse_args(["collect"])
        sports = parser.parse_args(["collect", "--category", "sports"])
        explicit = parser.parse_args(
            [
                "collect",
                "--category",
                "sports",
                "--sports-nba-target-markets",
                "40",
                "--sports-ufc-target-markets",
                "20",
                "--max-profiles-per-run",
                "17",
                "--user-history-trades-max-pages",
                "2",
                "--max-esports-closed-positions-per-wallet",
                "60",
            ]
        )

        self.assertEqual(effective_build_limits(esports), {})
        self.assertEqual(
            effective_build_limits(sports),
            {
                "sports_nba_target_markets": 80,
                "sports_ufc_target_markets": 80,
                "max_profiles_per_run": 200,
                "user_history_trades_max_pages": 8,
                "max_esports_closed_positions_per_wallet": 150,
            },
        )
        self.assertEqual(
            effective_build_limits(explicit),
            {
                "sports_nba_target_markets": 40,
                "sports_ufc_target_markets": 20,
                "max_profiles_per_run": 17,
                "user_history_trades_max_pages": 2,
                "max_esports_closed_positions_per_wallet": 60,
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
                "--discovery-lookback-days",
                "15",
                "--market-batch-size",
                "50",
                "--market-batch-index",
                "2",
            ]
        )

        self.assertEqual(args.discovery_lookback_days, 15)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_index, 2)

    def test_build_leaderboard_lock_blocks_second_holder(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)

            with acquire_build_lock(data_dir, blocking=False):
                with self.assertRaises(BuildLockUnavailable):
                    with acquire_build_lock(data_dir, blocking=False):
                        pass

    def test_command_build_leaderboard_uses_build_lock(self):
        parser = build_parser()
        args = parser.parse_args(["--data-dir", "data_test", "collect"])
        client = object()

        with patch("poly_fight.cli.acquire_build_lock", return_value=nullcontext()) as lock, patch(
            "poly_fight.cli._command_build_leaderboard_unlocked", return_value=0
        ) as inner:
            result = command_build_leaderboard(args, client=client)

        self.assertEqual(result, 0)
        self.assertEqual(lock.call_args.args[0], Path("data_test"))
        inner.assert_called_once_with(args, client=client)

    def test_sports_collect_skips_esports_submarket_backfill(self):
        class FakeClient:
            def __init__(self):
                self.max_end_dates = []
                self.user_trades = []
                for index in range(3):
                    self.user_trades.append(
                        {
                            "conditionId": f"nba{index}",
                            "outcomeIndex": 0,
                            "side": "BUY",
                            "size": 4000,
                            "price": 0.5,
                            "timestamp": 100 + index,
                        }
                    )
                self.user_trades.append(
                    {
                        "conditionId": "game1",
                        "question": "Dota 2: A vs B - Game 1 Winner",
                        "side": "BUY",
                        "outcomeIndex": 0,
                        "size": 100,
                        "price": 0.5,
                        "timestamp": 200,
                    }
                )

            def list_events_paginated(self, **kwargs):
                self.tag_slugs = kwargs.get("tag_slugs")
                self.max_end_dates.append(kwargs.get("max_end_date"))
                events = []
                for index in range(3):
                    events.append(
                        {
                            "id": f"e{index}",
                            "slug": f"nba-a-b-{index}",
                            "title": f"Los Angeles Lakers vs. Boston Celtics {index}",
                            "closed": True,
                            "startTime": "2026-06-01T00:00:00Z",
                            "endDate": "2026-06-08T00:00:00Z",
                            "tags": [{"slug": "nba"}],
                            "markets": [
                                {
                                    "conditionId": f"nba{index}",
                                    "question": f"Los Angeles Lakers vs. Boston Celtics {index}",
                                    "outcomes": json.dumps(["Los Angeles Lakers", f"Boston Celtics {index}"]),
                                    "outcomePrices": '["1","0"]',
                                    "volume": 500_000,
                                    "gameStartTime": "2026-06-01 00:00:00+00",
                                    "endDate": "2026-06-08T00:00:00Z",
                                    "umaEndDate": "2026-06-01T03:00:00Z",
                                    "closedTime": "2026-06-01 03:00:00+00",
                                }
                            ],
                        }
                    )
                return events

            def trades_for_market(self, condition_id, *, limit, offset, min_trade_cash):
                return [
                    {
                        "proxyWallet": "0xA",
                        "size": 4000,
                        "price": 0.5,
                        "timestamp": 100,
                    }
                ]

            def trades_for_user(self, wallet, *, limit=500, offset=0):
                return self.user_trades[offset : offset + limit]

            def positions(self, wallet, *, limit=100):
                return []

        with TemporaryDirectory() as tmp:
            args = build_parser().parse_args(
                [
                    "--data-dir",
                    str(Path(tmp) / "sports"),
                    "collect",
                    "--category",
                    "sports",
                    "--max-workers",
                    "1",
                    "--max-profiles-per-run",
                    "1",
                    "--target-markets",
                    "3",
                    "--max-markets-per-run",
                    "3",
                    "--min-profile-avg-market-cash",
                    "0",
                    "--leaderboard-min-avg-market-cash",
                    "0",
                ]
            )
            client = FakeClient()
            with patch("poly_fight.cli.backfill_user_trade_submarkets") as backfill:
                _command_build_leaderboard_unlocked(args, client=client)

            backfill.assert_not_called()
            self.assertEqual(client.tag_slugs, ("nba", "ufc"))
            self.assertEqual(client.max_end_dates, [None])
            classification = read_json(Path(tmp) / "sports" / "esports_classification_set.json", [])
            self.assertEqual(len(classification), 3)
            self.assertTrue(all(row["end_date"] == "2026-06-01T03:00:00Z" for row in classification))

    def test_sports_collect_honors_explicit_fourteen_day_lookback(self):
        now = datetime.now(timezone.utc)
        recent_end = (now - timedelta(days=7)).isoformat()
        old_end = (now - timedelta(days=30)).isoformat()

        class FakeClient:
            def list_events_paginated(self, **kwargs):
                self.tag_slugs = kwargs.get("tag_slugs")
                return [
                    {
                        "id": "recent",
                        "slug": "recent",
                        "title": "Los Angeles Lakers vs. Boston Celtics",
                        "closed": True,
                        "endDate": recent_end,
                        "tags": [{"slug": "nba"}],
                        "markets": [
                            {
                                "conditionId": "recent",
                                "question": "Los Angeles Lakers vs. Boston Celtics",
                                "outcomes": json.dumps(["Los Angeles Lakers", "Boston Celtics"]),
                                "outcomePrices": '["1","0"]',
                                "volume": 500_000,
                            }
                        ],
                    },
                    {
                        "id": "old",
                        "slug": "old",
                        "title": "Denver Nuggets vs. Miami Heat",
                        "closed": True,
                        "endDate": old_end,
                        "tags": [{"slug": "nba"}],
                        "markets": [
                            {
                                "conditionId": "old",
                                "question": "Denver Nuggets vs. Miami Heat",
                                "outcomes": json.dumps(["Denver Nuggets", "Miami Heat"]),
                                "outcomePrices": '["1","0"]',
                                "volume": 500_000,
                            }
                        ],
                    },
                ]

            def trades_for_market(self, condition_id, *, limit, offset, min_trade_cash):
                return []

            def trades_for_user(self, wallet, *, limit=500, offset=0):
                return []

            def positions(self, wallet, *, limit=100):
                return []

        with TemporaryDirectory() as tmp:
            args = build_parser().parse_args(
                [
                    "--data-dir",
                    str(Path(tmp) / "sports"),
                    "collect",
                    "--category",
                    "sports",
                    "--classification-lookback-days",
                    "14",
                    "--discovery-lookback-days",
                    "14",
                    "--max-workers",
                    "1",
                    "--max-profiles-per-run",
                    "0",
                    "--target-markets",
                    "10",
                    "--max-markets-per-run",
                    "10",
                ]
            )

            _command_build_leaderboard_unlocked(args, client=FakeClient())

            classification = read_json(Path(tmp) / "sports" / "esports_classification_set.json", [])
            self.assertEqual([row["condition_id"] for row in classification], ["recent"])

    def test_follow_command_uses_paper_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["follow", "--stake-usdc", "25"])

        self.assertEqual(args.command, "follow")
        self.assertEqual(args.execution_mode, "paper")
        self.assertEqual(args.stake_usdc, 25)
        self.assertEqual(args.follow_recency_days, 30)
        self.assertEqual(args.observe_window_hours, 24)
        self.assertEqual(args.max_slippage_over_entry, 0.05)
        self.assertFalse(args.require_pre_match)
        self.assertEqual(args.run_log_retention_days, 7)
        self.assertEqual(args.resolution_cache_ttl_seconds, 60)
        self.assertEqual(args.resolution_gamma_pages, 2)
        self.assertEqual(args.user_trades_limit, 100)
        self.assertEqual(args.user_trades_max_pages, 3)
        self.assertTrue(args.bootstrap_current_positions)
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.min_tick_seconds, 180)
        self.assertEqual(args.max_tick_seconds, 900)
        self.assertEqual(args.max_workers, 8)
        self.assertTrue(args.consensus_block_opposite)
        self.assertEqual(args.conflict_policy, "dual_follow")
        self.assertEqual(args.quarantine_sell_frac, 0.2)

        pre_match_args = parser.parse_args(["follow", "--stake-usdc", "25", "--require-pre-match"])
        self.assertTrue(pre_match_args.require_pre_match)

    def test_run_command_uses_v2_loop_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "1"])

        self.assertEqual(args.command, "run")
        self.assertEqual(args.stake_usdc, 1)
        self.assertTrue(args.skip_initial_build)
        self.assertEqual(args.max_run_ticks, 1)
        self.assertEqual(args.pool_refresh_hours, 24)
        self.assertEqual(args.observe_window_hours, 24)
        self.assertEqual(args.user_trades_limit, 100)
        self.assertEqual(args.user_trades_max_pages, 3)
        self.assertTrue(args.bootstrap_current_positions)
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.error_retry_seconds, 180)
        self.assertEqual(args.max_consecutive_error_seconds, 600)
        self.assertEqual(args.resolution_cache_ttl_seconds, 60)
        self.assertEqual(args.resolution_gamma_pages, 2)
        self.assertIsNone(args.discovery_lookback_days)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_count, 2)
        self.assertTrue(args.consensus_block_opposite)
        self.assertEqual(args.conflict_policy, "dual_follow")
        self.assertEqual(args.quarantine_sell_frac, 0.2)
        self.assertFalse(args.require_pre_match)

        pre_match_args = parser.parse_args(["run", "--stake-usdc", "1", "--require-pre-match"])
        self.assertTrue(pre_match_args.require_pre_match)

    def test_follow_commands_reject_removed_dead_options(self):
        parser = build_parser()

        removed_options = [
            ["follow", "--stake-usdc", "1", "--event-gate-horizon-hours", "24"],
            ["follow", "--stake-usdc", "1", "--results-retention-days", "0"],
            ["follow", "--stake-usdc", "1", "--consensus-min-same-side", "1"],
            ["run", "--stake-usdc", "1", "--event-gate-horizon-hours", "24"],
            ["run", "--stake-usdc", "1", "--results-retention-days", "0"],
            ["run", "--stake-usdc", "1", "--consensus-min-same-side", "1"],
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
            "poly_fight.cli.command_build_leaderboard"
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
            "poly_fight.cli.command_build_leaderboard"
        ) as build, patch("poly_fight.cli.command_follow", side_effect=flaky_follow) as follow, patch(
            "poly_fight.cli.time.sleep"
        ) as sleep:
            from poly_fight.cli import command_run

            with redirect_stdout(StringIO()):
                self.assertEqual(command_run(args), 0)

        self.assertEqual(build.call_count, 0)
        self.assertEqual(follow.call_count, 2)
        self.assertEqual(sleep.call_args_list[0].args[0], 180)

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
                "poly_fight.cli.command_build_leaderboard", side_effect=fake_build
            ) as build, patch(
                "poly_fight.cli.command_follow", side_effect=fake_follow
            ):
                from poly_fight.cli import command_run

                with redirect_stdout(StringIO()):
                    self.assertEqual(command_run(args), 0)

            self.assertEqual(build.call_count, 2)
            self.assertEqual(set(paused_categories), {"esports", "sports"})

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
            "poly_fight.cli.command_build_leaderboard"
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
