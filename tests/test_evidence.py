import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from poly_fight.evidence import EvidenceRouter
from poly_fight.leaguepedia import LeaguepediaClient, group_scoreboard_games
from poly_fight.liquipedia import LiquipediaClient, parse_recent_match_tables
from poly_fight.storage import FollowStore


def market(game="lol"):
    return {
        "condition_id": "c1", "category": "esports", "market_type": "main_match",
        "game_family": game, "title": "Alpha vs Beta (BO3)", "outcomes": ["Alpha", "Beta"],
    }


def source(provider, *, conflict=False):
    recent_a = [
        {"raw_id": f"{provider}-a-{index}", "d": f"2026-07-{20-index:02d}", "opp": "Gamma" if index == 0 else f"Opponent {index}", "r": "L" if conflict and index == 0 else "W", "event": "Tier Cup"}
        for index in range(12)
    ]
    recent_b = [
        {"raw_id": f"{provider}-b-{index}", "d": f"2026-07-{20-index:02d}", "opp": f"Other {index}", "r": "L", "event": "Tier Cup"}
        for index in range(12)
    ]
    return {
        "source": provider, "team_a": {"name": "Alpha", "record": {"n": 12}, "days_since_last": 1, "current_roster": ["a", "b", "c", "d", "e"], "recent": recent_a},
        "team_b": {"name": "Beta", "record": {"n": 12}, "days_since_last": 1, "current_roster": ["f", "g", "h", "i", "j"], "recent": recent_b},
        "h2h": [{"d": "2026-06-01", "winner": "A"}, {"d": "2026-05-01", "winner": "B"}],
        "cache_keys": [f"{provider}:alpha", f"{provider}:beta"],
    }


class StaticService:
    def __init__(self, payload):
        self.payload = payload

    def build_evidence(self, market, *, cutoff_ts, now_ts):
        return self.payload


class FailingService:
    def __init__(self, error):
        self.error = error

    def build_evidence(self, market, *, cutoff_ts, now_ts):
        raise ValueError(self.error)


class EvidenceTests(unittest.TestCase):
    def test_unresolved_team_is_a_coverage_gap_not_a_provider_outage(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            router = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "opendota": lambda: FailingService("opendota_team_unresolved"),
            })
            pack = router.build_evidence(market("dota2"), cutoff_ts=1_784_678_400, now_ts=1_784_678_000)
            self.assertEqual(pack["coverage"]["failed_sources"]["opendota"], "opendota_team_unresolved")
            health = {row["provider"]: row for row in store.load_ai_provider_health()}
            self.assertEqual(health["opendota"]["status"], "empty")
            self.assertEqual(health["opendota"]["gap_code"], "opendota_team_unresolved")
            self.assertEqual(health["opendota"]["error"], "")
            self.assertEqual(health["opendota"]["last_error_at"], 0)

    def test_unresolved_team_preserves_prior_success_as_partial_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            good = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "opendota": lambda: StaticService(source("OpenDota")),
            })
            good.build_evidence(market("dota2"), cutoff_ts=1_784_678_400, now_ts=1_784_678_000)
            gap = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "opendota": lambda: FailingService("opendota_team_unresolved"),
            })
            gap.build_evidence(market("dota2"), cutoff_ts=1_784_678_500, now_ts=1_784_678_100)
            health = {row["provider"]: row for row in store.load_ai_provider_health()}
            self.assertEqual(health["opendota"]["status"], "partial")
            self.assertEqual(health["opendota"]["coverage"], 12)
            self.assertEqual(health["opendota"]["last_success_at"], 1_784_678_000)

    def test_empty_provider_does_not_receive_multi_source_quality_credit(self):
        empty = source("Leaguepedia")
        for side in ("team_a", "team_b"):
            empty[side] = {**empty[side], "record": {"n": 0}, "recent": []}
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            router = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "leaguepedia": lambda: StaticService(empty),
            })
            pack = router.build_evidence(market(), cutoff_ts=1_784_678_400, now_ts=1_784_678_000)
            self.assertEqual(pack["score_components"]["multi_source"], 0)
            self.assertIn("single_source", pack["coverage"]["gaps"])
            health = {row["provider"]: row for row in store.load_ai_provider_health()}
            self.assertEqual(health["leaguepedia"]["status"], "empty")
            router.close()

    def test_router_merges_parallel_sources_assigns_real_ids_and_scores(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            router = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "leaguepedia": lambda: StaticService(source("Leaguepedia")),
            })
            pack = router.build_evidence(market(), cutoff_ts=1_784_678_400, now_ts=1_784_678_000)
            self.assertGreaterEqual(pack["evidence_score"], 80)
            self.assertEqual(set(pack["coverage"]["successful_sources"]), {"pandascore", "leaguepedia"})
            self.assertTrue(pack["valid_evidence_ids"])
            self.assertTrue(all(row["id"] in pack["valid_evidence_ids"] for row in pack["normalized"]["team_a"]))
            self.assertTrue(all(row["raw_id"] for row in pack["normalized"]["team_a"]))
            self.assertTrue(all("recent" not in row["team_a"] for row in pack["source_summaries"]))
            self.assertNotIn("wallet", json.dumps(pack).lower())
            self.assertLess(len(json.dumps(pack, ensure_ascii=False)), 48_500)
            health = {row["provider"]: row for row in store.load_ai_provider_health()}
            self.assertEqual(health["pandascore"]["status"], "ok")
            router.close()

    def test_source_conflict_is_excluded_and_flagged(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            router = EvidenceRouter(store, pandascore_key="x", service_factories={
                "pandascore": lambda: StaticService(source("PandaScore")),
                "leaguepedia": lambda: StaticService(source("Leaguepedia", conflict=True)),
            })
            pack = router.build_evidence(market(), cutoff_ts=1_784_678_400, now_ts=1_784_678_000)
            self.assertTrue(pack["coverage"]["conflicts"])
            self.assertIn("source_conflict", pack["coverage"]["gaps"])
            self.assertFalse(any(row["d"] == "2026-07-20" and row["opp"] == "Gamma" for row in pack["normalized"]["team_a"]))

    def test_leaguepedia_groups_games_by_match_and_excludes_future(self):
        rows = [
            {"MatchId": "m1", "GameId": "g1", "N_GameInMatch": "1", "DateTime_UTC": "2026-07-20 10:00:00", "Team1": "Alpha", "Team2": "Beta", "Winner": "Alpha", "Tournament": "Cup"},
            {"MatchId": "m1", "GameId": "g2", "N_GameInMatch": "2", "DateTime_UTC": "2026-07-20 11:00:00", "Team1": "Alpha", "Team2": "Beta", "Winner": "Alpha", "Tournament": "Cup"},
            {"MatchId": "future", "DateTime_UTC": "2026-07-23 10:00:00", "Team1": "Alpha", "Team2": "Beta", "Winner": "Beta"},
        ]
        cutoff = 1_784_688_000
        series = group_scoreboard_games(rows, team="Alpha", cutoff_ts=cutoff)
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["score"], [2, 0])

    def test_liquipedia_api_html_normalizes_series(self):
        html = """
        <div class='match-table-wrapper'><table><tr class='table2__row--body'>
          <td data-sort-value='1784636100'><span data-timestamp='1784636100'></span></td><td>A-Tier</td><td>Online</td><td></td><td></td><td>CCT</td>
          <td><div data-label-type='result-win'></div></td><td>2 : 1</td><td>Beta</td><td></td>
        </tr></table></div>
        <div class='match-table-wrapper'><table><tr class='table2__row--body'>
          <td data-sort-value='1784636100'><span data-timestamp='1784636100'></span></td><td>A-Tier</td><td>Online</td><td></td><td></td><td>CCT</td>
          <td><div data-label-type='result-loss'></div></td><td>1 : 2</td><td>Alpha</td><td></td>
        </tr></table></div>
        """
        parsed = parse_recent_match_tables(html, ["Alpha", "Beta"], cutoff_ts=1_784_700_000)
        self.assertEqual(parsed["Alpha"][0]["result"], "W")
        self.assertEqual(parsed["Beta"][0]["result"], "L")

    def test_leaguepedia_ratelimit_trips_circuit_without_waiting(self):
        LeaguepediaClient._last_request_at = 0
        LeaguepediaClient._circuit_open_until = 0
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"error": {"code": "ratelimited"}}))
        client = LeaguepediaClient(transport=transport)
        with self.assertRaisesRegex(RuntimeError, "ratelimited"):
            client.scoreboard_games(["Alpha", "Beta"], cutoff_ts=1_784_700_000)
        with self.assertRaisesRegex(RuntimeError, "circuit_open"):
            client.scoreboard_games(["Alpha", "Beta"], cutoff_ts=1_784_700_000)
        client.close()

    def test_leaguepedia_queue_waits_one_minute_between_requests(self):
        LeaguepediaClient._last_request_at = 0
        LeaguepediaClient._circuit_open_until = 0
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"cargoquery": []}))
        client = LeaguepediaClient(transport=transport)
        with patch("poly_fight.leaguepedia.time.monotonic", side_effect=[100.0, 100.0, 160.0]), patch("poly_fight.leaguepedia.time.sleep") as sleeper:
            self.assertEqual(client.scoreboard_games(["Alpha", "Beta"], cutoff_ts=1_784_700_000), [])
            self.assertEqual(client.scoreboard_games(["Alpha", "Beta"], cutoff_ts=1_784_700_000), [])
        sleeper.assert_called_once_with(60.0)
        client.close()

    def test_liquipedia_parse_queue_waits_thirty_seconds_locally(self):
        LiquipediaClient._last_parse_at = 0
        html = "<div></div>"
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"parse": {"text": {"*": html}}}))
        client = LiquipediaClient(transport=transport)
        with patch("poly_fight.liquipedia.time.monotonic", side_effect=[100.0, 100.0, 130.0]), patch("poly_fight.liquipedia.time.sleep") as sleeper:
            self.assertEqual(client.recent_matches_html(["Alpha", "Beta"]), html)
            self.assertEqual(client.recent_matches_html(["Alpha", "Beta"]), html)
        sleeper.assert_called_once_with(30.0)
        client.close()


if __name__ == "__main__":
    unittest.main()
