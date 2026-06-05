from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import urllib.error
import urllib.request
import unittest
from unittest.mock import patch

from poly_fight.api import PolymarketClient, RateLimiter, parse_retry_after
from poly_fight.cli import (
    build_leaderboard_from_profiles,
    build_parser,
    build_wallet_overlap_report,
    build_profile_fetch_plan,
    fetch_resolutions_for_open_signals,
    fetch_recent_esports_closed_positions_for_wallet,
    fetch_user_trades_until_cursor,
    fetch_market_trades_cached,
    filter_profile_candidates,
    merge_cached_profile_with_candidate,
    merge_profiles_with_candidates,
    prune_profile_store,
    refresh_open_signal_fills,
    read_json,
    should_refresh_file_cache,
    should_use_cached_profile,
    watched_markets,
)
from poly_fight.core import (
    SCORING_VERSION,
    analyze_holders,
    build_candidate_wallets,
    build_candidate_wallets_from_holders,
    build_discovery_slate,
    classify_wallet,
    event_to_market_record,
    normalize_wallet,
    profile_candidate_wallet,
    summarize_closed_positions,
)
from poly_fight.follow import (
    aggregate_follow_performance,
    bootstrap_position_trades,
    desired_tick_interval,
    detect_new_positions,
    eligible_follow_wallets,
    esports_match_imminent,
    evaluate_slippage,
    paper_pnl,
    process_follow_trades,
    qualify_follow,
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

    def test_closed_positions_requests_timestamp_descending_order(self):
        calls = []

        class Client(PolymarketClient):
            def data(self, path, **params):
                calls.append((path, params))
                return []

        Client().closed_positions("0xabc", limit=50, offset=100)

        self.assertEqual(calls[0][0], "/closed-positions")
        self.assertEqual(calls[0][1]["sortBy"], "TIMESTAMP")
        self.assertEqual(calls[0][1]["sortDirection"], "DESC")

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
        self.assertEqual(summary["esports_roi"], 0.05)
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

    def test_low_edge_profit_rate_is_reported_but_does_not_change_grade(self):
        positions = [
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 2, "avgPrice": 0.98, "timestamp": 100 + i}
            for i in range(20)
        ]
        summary = summarize_closed_positions(positions, {f"m{i}" for i in range(20)}, now_ts=200)
        summary["bot_like_score"] = 0

        rated = classify_wallet(summary, now_ts=200)

        self.assertEqual(rated["low_edge_profit_rate"], 1.0)
        self.assertEqual(rated["high_price_entry_rate"], 1.0)
        self.assertEqual(rated["grade"], "C")

    def test_wallet_rating_rejects_high_roi_without_stability(self):
        summary = {
            "esports_closed_count": 20,
            "esports_realized_pnl": 1_000,
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
            "esports_realized_pnl": 1_000,
            "median_market_roi": 0.10,
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
            "esports_realized_pnl": 1_000,
            "median_market_roi": 0.10,
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
            {"conditionId": f"m{i}", "totalBought": 100, "realizedPnl": 20, "timestamp": 100 + i}
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

    def test_recent_esports_closed_positions_are_filtered_and_capped(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def closed_positions(self, wallet, *, limit, offset, sort_direction="DESC"):
                self.calls.append((limit, offset, sort_direction))
                rows = []
                for index in range(offset, offset + limit):
                    condition_id = f"m{index}" if index % 2 == 0 else f"other{index}"
                    rows.append({"conditionId": condition_id, "timestamp": 10_000 - index})
                return rows

        client = FakeClient()
        condition_ids = {f"m{index}" for index in range(0, 200, 2)}

        positions = fetch_recent_esports_closed_positions_for_wallet(
            client,
            "0xabc",
            condition_ids,
            max_raw_closed_positions=200,
            max_esports_closed_positions=50,
            page_limit=30,
        )

        self.assertEqual(len(positions), 50)
        self.assertTrue(all(row["conditionId"] in condition_ids for row in positions))
        self.assertLessEqual(sum(limit for limit, _offset, _direction in client.calls), 200)
        self.assertTrue(all(direction == "DESC" for _limit, _offset, direction in client.calls))

    def test_write_json_uses_target_unique_atomic_temp_files(self):
        from poly_fight.cli import write_json

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "value.json"
            write_json(path, {"a": 1})

            self.assertEqual(read_json(path, {}), {"a": 1})
            self.assertEqual(list(Path(tmp).glob("*.tmp")), [])

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
                "esports_roi": 0.12,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xnew": {
                "wallet": "0xnew",
                "grade": "B",
                "esports_roi": 0.08,
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

    def test_stale_ab_profile_is_not_kept_on_leaderboard(self):
        profiles_by_wallet = {
            "0xold": {
                "wallet": "0xold",
                "grade": "A",
                "esports_roi": 0.12,
                "positive_market_rate": 0.96,
                "median_entry_price": 0.55,
                "last_esports_trade_at": 100,
                "candidate": {"participated_market_count": 10, "avg_market_cash": 2_000},
            },
            "0xfresh": {
                "wallet": "0xfresh",
                "grade": "A",
                "esports_roi": 0.10,
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
            ["0xgood", "0xlowwin", "0xhighentry", "0xpricechaser", "0xlateentry"],
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
        self.assertEqual(args.max_closed_positions_per_wallet, 500)
        self.assertEqual(args.max_esports_closed_positions_per_wallet, 50)
        self.assertEqual(args.min_profile_participated_markets, 3)
        self.assertEqual(args.min_profile_avg_market_cash, 1_500)
        self.assertEqual(args.market_trades_cache_ttl_days, 7)
        self.assertFalse(args.refresh_market_trades)
        self.assertFalse(args.no_market_trades_cache)
        self.assertEqual(args.leaderboard_min_participated_markets, 3)
        self.assertEqual(args.leaderboard_min_avg_market_cash, 1_500)
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
        self.assertEqual(args.user_trades_limit, 100)
        self.assertEqual(args.user_trades_max_pages, 3)
        self.assertTrue(args.bootstrap_current_positions)
        self.assertEqual(args.max_follow_legs, 10)
        self.assertEqual(args.min_tick_seconds, 180)
        self.assertEqual(args.max_tick_seconds, 900)
        self.assertEqual(args.max_workers, 8)

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
        self.assertEqual(args.discovery_lookback_days, 14)
        self.assertEqual(args.market_batch_size, 50)
        self.assertEqual(args.market_batch_count, 2)

    def test_run_skip_initial_build_does_not_build_before_first_tick(self):
        parser = build_parser()
        args = parser.parse_args(["run", "--stake-usdc", "1", "--skip-initial-build", "--max-run-ticks", "1"])

        with patch("poly_fight.cli.build_client", return_value=object()), patch(
            "poly_fight.cli.command_build_leaderboard"
        ) as build, patch("poly_fight.cli.command_follow", return_value={"desired_next_interval_seconds": 900}):
            from poly_fight.cli import command_run

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

            self.assertEqual(command_run(args), 2)

        self.assertEqual(follow.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
