import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from poly_fight.opendota import MAX_PROMPT_SERIES, OpenDotaEvidenceService
from poly_fight.storage import FollowStore


class FakeOpenDota:
    def __init__(self, cutoff_ts):
        self.cutoff_ts = cutoff_ts
        self.match_calls = []
        self.player_calls = []

    def teams(self):
        return [
            {"team_id": 1, "name": "Team Spirit", "tag": "Spirit", "last_match_time": self.cutoff_ts - 10},
            {"team_id": 2, "name": "BetBoom Team", "tag": "BB", "last_match_time": self.cutoff_ts - 20},
            {"team_id": 3, "name": "Team Spirit Academy", "tag": "Spirit A", "last_match_time": self.cutoff_ts - 30},
        ]

    def team_matches(self, team_id):
        self.match_calls.append(team_id)
        series_count = 12 if team_id == 1 else 6
        rows = []
        for index in range(series_count):
            base = self.cutoff_ts - (index + 1) * (10 if team_id == 1 else 25) * 86400
            opponent_id = (2 if team_id == 1 else 1) if index == 0 else 100 + index
            opponent_name = (
                "BetBoom Team" if opponent_id == 2 else "Team Spirit" if opponent_id == 1
                else f"Opponent {index}"
            )
            for game_index in range(2):
                won = index % 3 != 0
                radiant = game_index == 0
                rows.append({
                    "match_id": team_id * 100000 + index * 10 + game_index,
                    "start_time": base + game_index * 1800,
                    "radiant": radiant,
                    "radiant_win": won if radiant else not won,
                    "opposing_team_id": opponent_id,
                    "opposing_team_name": opponent_name,
                    "leagueid": 55,
                    "league_name": "Compact League",
                })
        rows.insert(0, {
            "match_id": 999999, "start_time": self.cutoff_ts + 60,
            "radiant": True, "radiant_win": True, "opposing_team_id": 999,
            "opposing_team_name": "Future Opponent", "leagueid": 55,
            "league_name": "Must Not Leak",
        })
        return rows

    def team_players(self, team_id):
        self.player_calls.append(team_id)
        return [
            {"name": f"player-{team_id}-{index}", "is_current_team_member": True}
            for index in range(5)
        ]


class OpenDotaEvidenceTests(unittest.TestCase):
    def test_history_is_cutoff_safe_series_compacted_and_cached(self):
        cutoff_ts = int(datetime(2026, 7, 22, tzinfo=timezone.utc).timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            client = FakeOpenDota(cutoff_ts)
            service = OpenDotaEvidenceService(store, client)
            evidence = service.build_evidence(
                {"outcomes": ["Team Spirit", "BetBoom Team"]},
                cutoff_ts=cutoff_ts, now_ts=cutoff_ts + 7 * 86400,
            )

            self.assertEqual(evidence["source"], "OpenDota")
            self.assertEqual(evidence["team_a"]["record"]["n"], 12)
            self.assertEqual(evidence["team_b"]["record"]["n"], 6)
            self.assertEqual(evidence["team_a"]["window_days"], 120)
            self.assertEqual(evidence["team_b"]["window_days"], 180)
            self.assertEqual(len(evidence["team_a"]["recent"]), MAX_PROMPT_SERIES)
            self.assertEqual(len(evidence["team_b"]["recent"]), 6)
            self.assertEqual(len(evidence["h2h"]), 1)
            self.assertNotIn("current_roster", evidence["team_a"])
            encoded = json.dumps(evidence, ensure_ascii=False)
            self.assertNotIn("Future Opponent", encoded)
            self.assertNotIn("Must Not Leak", encoded)
            self.assertNotIn("match_id", encoded)
            self.assertNotIn("opponent_id", encoded)
            self.assertLess(len(encoded), 10_000)

            again = service.build_evidence(
                {"outcomes": ["Team Spirit", "BetBoom Team"]},
                cutoff_ts=cutoff_ts, now_ts=cutoff_ts + 60,
            )
            self.assertEqual(again["team_a"]["record"], evidence["team_a"]["record"])
            self.assertEqual(client.match_calls, [1, 2])
            self.assertEqual(client.player_calls, [])


if __name__ == "__main__":
    unittest.main()
