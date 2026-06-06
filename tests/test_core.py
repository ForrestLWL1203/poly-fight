from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from contextlib import nullcontext, redirect_stdout
import http.client
import json
from io import BytesIO, StringIO
from pathlib import Path
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
    build_overview,
    build_runner_status,
    read_stream_signal,
    build_wallet_refresh_status,
    build_wallets,
    DashboardConfig,
    create_server,
    make_session_token,
    short_addr,
    start_runner,
    stop_runner,
    stream_dirty_flags,
    StreamSignal,
    verify_session_token,
)
from poly_fight.control import read_follow_control
from poly_fight.storage import FollowStore
from poly_fight.cli import (
    BuildLockUnavailable,
    acquire_build_lock,
    build_leaderboard_from_profiles,
    build_parser,
    build_wallet_overlap_report,
    build_profile_fetch_plan,
    command_build_leaderboard,
    command_follow,
    fetch_resolutions_for_open_signals,
    fetch_recent_esports_closed_positions_for_wallet,
    fetch_user_trades_until_cursor,
    fetch_market_trades_cached,
    filter_profile_candidates,
    load_active_market_cache,
    merge_cached_profile_with_candidate,
    merge_profiles_with_candidates,
    prune_profile_store,
    refresh_open_signal_fills,
    read_json,
    read_jsonl,
    should_refresh_file_cache,
    should_use_cached_profile,
    watched_markets,
    write_json,
    write_jsonl,
)
from poly_fight.core import (
    SCORING_VERSION,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_classification_set,
    build_discovery_slate,
    classify_wallet,
    event_to_market_record,
    normalize_wallet,
    profile_candidate_wallet,
    summarize_closed_positions,
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
    material_sell,
    paper_pnl,
    process_follow_trades,
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

    def test_low_edge_profit_rate_is_excluded_by_roi_floor(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 2, "avgPrice": 0.98, "timestamp": 100 + i}
            for i in range(20)
        ]
        summary = summarize_closed_positions(positions, {f"m{i}" for i in range(20)}, now_ts=200)
        summary["bot_like_score"] = 0

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["low_edge_profit_rate"], 1.0)
        self.assertEqual(rated["high_price_entry_rate"], 1.0)
        self.assertEqual(rated["grade"], "excluded")
        self.assertIn("low_historical_roi", rated["reasons"])

    def test_wallet_rating_rejects_high_roi_without_stability(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": -0.01,
            "positive_market_rate": 0.45,
            "esports_loss_count": 11,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.55,
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
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "A")
        self.assertIn("has_losses", rated["reasons"])
        self.assertEqual(rated["entry_edge"], 0.34)

    def test_high_wilson_without_entry_edge_is_not_a_grade(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 0.90,
            "wilson_win_rate_lower_bound": 0.70,
            "esports_loss_count": 2,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.66,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "C")
        self.assertIn("weak_entry_edge", rated["reasons"])

    def test_entry_edge_can_qualify_wallet_with_moderate_wilson(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 4_000,
            "median_market_roi": 0.40,
            "positive_market_rate": 0.85,
            "wilson_win_rate_lower_bound": 0.66,
            "esports_loss_count": 3,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
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
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "C")
        self.assertIn("low_volume", rated["reasons"])

    def test_wallet_rating_excludes_low_historical_roi(self):
        summary = {
            "esports_closed_count": 28,
            "esports_realized_pnl": 2_900,
            "esports_roi": 0.29,
            "median_market_roi": 0.29,
            "positive_market_rate": 1.0,
            "wilson_win_rate_lower_bound": 0.88,
            "esports_loss_count": 0,
            "esports_total_bought": 10_000,
            "median_entry_price": 0.50,
            "last_esports_trade_at": 100,
            "bot_like_score": 0,
        }

        rated = classify_wallet(summary, now_ts=100 + 86400)

        self.assertEqual(rated["grade"], "excluded")
        self.assertEqual(rated["profile_state"], "unqualified")
        self.assertIn("low_historical_roi", rated["reasons"])

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

    def test_profile_candidate_wallet_excludes_systemic_short_term_selling(self):
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
        self.assertEqual(result["grade"], "excluded")
        self.assertIn("sold_before_resolution", result["reasons"])

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

    def test_high_frequency_only_candidate_gets_bot_like_penalty(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 50, "timestamp": 100 + i}
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

            self.assertEqual(source, "legacy_state_cache")
            self.assertIn("m1", markets)
            self.assertNotIn("active_market_cache", new_state)
            self.assertTrue(cache_path.exists())

    def test_profile_candidate_filter_keeps_only_clean_active_wallets(self):
        candidates = [
            {"wallet": "0xgood", "participated_market_count": 1, "avg_market_cash": 1_500},
            {"wallet": "0xsmall", "participated_market_count": 1, "avg_market_cash": 1_499},
            {"wallet": "0xtwosided", "participated_market_count": 10, "avg_market_cash": 1_500, "two_sided_market_count": 1},
            {"wallet": "0xchurnok", "participated_market_count": 10, "avg_market_cash": 1_500, "high_churn_market_count": 10, "late_entry_market_count": 0},
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

        self.assertEqual([row["wallet"] for row in filtered], ["0xgood", "0xchurnok", "0xlateok", "0xlate"])

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
                "wallet": "0xscalper",
                "grade": "A",
                "esports_roi": 0.4,
                "positive_market_rate": 0.99,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "historical_trade_behavior_market_count": 6,
                "sold_before_resolution_market_count": 4,
                "sold_before_resolution_market_rate": 4 / 6,
                "two_sided_trade_market_count": 0,
                "two_sided_trade_market_rate": 0.0,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000, "two_sided_market_count": 0},
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
                    "tail_entry_market_count": 1,
                },
            },
        }

        leaderboard = build_leaderboard_from_profiles(
            profiles_by_wallet,
            now_ts=100 + 10 * 86400,
        )

        self.assertEqual(
            [row["wallet"] for row in leaderboard],
            [
                "0xgood",
                "0xlowwin",
                "0xhighentry",
                "0xpricechaser",
                "0xlateentry",
                "0xoccasionalsell",
                "0xsmallbehavior",
            ],
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

    def test_follow_store_records_wallet_quarantine(self):
        with TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")

            store.upsert_wallet_quarantine("0xA", reason="material_sell", ts=100)
            quarantined = store.load_wallet_quarantine()

            self.assertIn("0xa", quarantined)
            self.assertEqual(quarantined["0xa"]["reason"], "material_sell")

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

    def test_watched_markets_only_include_future_starts(self):
        now = 1000
        markets = {
            "past": {"condition_id": "past", "match_start_time": datetime.fromtimestamp(now - 60, timezone.utc).isoformat()},
            "future": {"condition_id": "future", "match_start_time": datetime.fromtimestamp(now + 3600, timezone.utc).isoformat()},
            "far": {"condition_id": "far", "match_start_time": datetime.fromtimestamp(now + 30 * 3600, timezone.utc).isoformat()},
        }

        watched = watched_markets(markets, now_ts=now, observe_window_hours=24)

        self.assertEqual(list(watched), ["future"])

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

    def test_follow_v2_buy_legs_sell_exit_and_behavior(self):
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
        self.assertEqual(signals[0]["exit_price"], 0.6)
        self.assertGreater(signals[0]["our_realized_pnl"], 0)
        self.assertTrue(wallet_behavior_summary(signals[0])["sold_before_resolution"])

    def test_follow_v4_dust_sell_does_not_mirror_exit(self):
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

        self.assertEqual(stats["exited_signal_count"], 0)
        self.assertEqual(stats["quarantine_events"], [])
        self.assertEqual(signals[0]["status"], "open")
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

    def test_follow_v2_hedge_records_opposite_buy(self):
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
            trades=[{"id": "b2", "proxyWallet": "0xA", "market": "m1", "outcomeIndex": 1, "side": "BUY", "price": 0.4, "size": 1, "timestamp": now + 1}],
            markets_by_condition={"m1": market},
            now_ts=now + 1,
            stake_usdc=1,
            max_follow_legs=10,
            max_slippage=0.05,
        )

        self.assertEqual(stats["hedge_event_count"], 1)
        self.assertTrue(wallet_behavior_summary(signals[0])["hedged"])

    def test_v4_contested_markets_mark_signals_not_followable(self):
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
        self.assertFalse(updated[0]["would_follow"])
        self.assertFalse(updated[0]["legs"][0]["would_follow"])
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
                            "title": "Event 1",
                            "tags": [{"slug": "esports"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Team A vs Team B",
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
                            "title": "Event 1",
                            "tags": [{"slug": "esports"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "A vs B",
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
            self.assertNotIn("m2", {row.get("condition_id") for row in snapshot["open_signals"]})

    def test_follow_tick_marks_opposite_same_market_signals_contested(self):
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
                            "title": "Event 1",
                            "tags": [{"slug": "esports"}],
                            "startTime": start.isoformat(),
                            "markets": [
                                {
                                    "conditionId": "m1",
                                    "question": "Team A vs Team B",
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
            open_signals = store.load_open_signals()

            self.assertEqual(summary["contested_signal_count"], 2)
            self.assertEqual(len(open_signals), 2)
            self.assertTrue(all(signal.get("contested") for signal in open_signals))
            self.assertTrue(all(not signal.get("would_follow", True) for signal in open_signals))

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
                self.assertIn("Poly Fight Dashboard", body)
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
            self.assertFalse(flags["wallets_dirty"])

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

    def test_dashboard_runner_start_writes_control_and_blocks_duplicate(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            calls = []

            class FakeProcess:
                pid = 4321
                pgid = 4321

            def fake_starter(command, log_path):
                calls.append((command, log_path))
                return FakeProcess()

            config = DashboardConfig(
                data_dir=data_dir,
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
            self.assertEqual(read_follow_control(data_dir)["runner"]["pid"], 4321)

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

    def test_dashboard_wallet_refresh_api_runs_refresh_without_pausing_follow(self):
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            ran = threading.Event()

            def fake_runner(data_dir_arg, log_path):
                self.assertEqual(data_dir_arg, data_dir)
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
                conn.request("POST", "/api/wallet-refresh", headers={"Cookie": cookie})
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                conn.close()

                self.assertEqual(response.status, 202)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["data"]["status"], "running")
                self.assertTrue(ran.wait(2))

                status = build_wallet_refresh_status(data_dir)
                deadline = time.time() + 2
                while status["status"].get("status") == "running" and time.time() < deadline:
                    time.sleep(0.01)
                    status = build_wallet_refresh_status(data_dir)
                self.assertEqual(status["status"]["status"], "succeeded")
                self.assertEqual(status["status"]["returncode"], 0)
                self.assertEqual(read_follow_control(data_dir)["wallet_refresh"]["status"], "succeeded")
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
                open_signals=[],
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
        self.assertEqual(args.discovery_lookback_days, 14)
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
        self.assertEqual(args.classification_lookback_days, 14)
        self.assertEqual(args.max_esports_closed_positions_per_wallet, 50)
        self.assertEqual(args.closed_position_market_chunk_size, 50)
        self.assertEqual(args.min_profile_participated_markets, 3)
        self.assertEqual(args.min_profile_avg_market_cash, 1_500)
        self.assertEqual(args.market_trades_cache_ttl_days, 7)
        self.assertFalse(args.refresh_market_trades)
        self.assertFalse(args.no_market_trades_cache)
        self.assertEqual(args.leaderboard_min_participated_markets, 3)
        self.assertEqual(args.leaderboard_min_avg_market_cash, 1_500)
        self.assertEqual(args.max_leaderboard_wallets, 30)
        self.assertFalse(args.check_current_positions)

    def test_collect_command_accepts_build_options(self):
        parser = build_parser()

        args = parser.parse_args(["collect", "--max-profiles-per-run", "3"])

        self.assertEqual(args.command, "collect")
        self.assertEqual(args.max_profiles_per_run, 3)

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

    def test_follow_command_uses_paper_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["follow", "--stake-usdc", "25"])

        self.assertEqual(args.command, "follow")
        self.assertEqual(args.execution_mode, "paper")
        self.assertEqual(args.stake_usdc, 25)
        self.assertEqual(args.follow_recency_days, 30)
        self.assertEqual(args.event_gate_horizon_hours, 24)
        self.assertEqual(args.observe_window_hours, 24)
        self.assertEqual(args.max_slippage_over_entry, 0.05)
        self.assertTrue(args.require_pre_match)
        self.assertEqual(args.run_log_retention_days, 7)
        self.assertEqual(args.results_retention_days, 0)
        self.assertEqual(args.resolution_cache_ttl_seconds, 60)
        self.assertEqual(args.resolution_gamma_pages, 2)
        self.assertEqual(args.user_trades_limit, 100)
        self.assertEqual(args.user_trades_max_pages, 3)
        self.assertTrue(args.bootstrap_current_positions)
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.min_tick_seconds, 180)
        self.assertEqual(args.max_tick_seconds, 900)
        self.assertEqual(args.max_workers, 8)
        self.assertEqual(args.consensus_min_same_side, 1)
        self.assertTrue(args.consensus_block_opposite)
        self.assertEqual(args.quarantine_sell_frac, 0.2)

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
        self.assertEqual(args.discovery_lookback_days, 14)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_count, 2)
        self.assertEqual(args.consensus_min_same_side, 1)
        self.assertTrue(args.consensus_block_opposite)
        self.assertEqual(args.quarantine_sell_frac, 0.2)

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
