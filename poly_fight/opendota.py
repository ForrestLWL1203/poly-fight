"""OpenDota-backed pre-match evidence for Dota 2 main-match AI risk.

OpenDota exposes game-level team history rather than a stable series endpoint.
This adapter groups adjacent games against the same opponent in the same league
into a compact series record, filters everything at the target match cutoff,
and persists only normalized evidence in the existing bounded AI cache.
"""

from __future__ import annotations

import difflib
import json
import re
import time
import urllib.error
import urllib.request
from typing import Any

from .storage import FollowStore


OPENDOTA_API_URL = "https://api.opendota.com/api"
OPENDOTA_PROVIDER = "opendota"
USER_AGENT = "poly-fight/0.1 (+https://github.com/ForrestLWL1203/poly-fight)"
REQUEST_TIMEOUT_SECONDS = 12
TEAM_CACHE_TTL_SECONDS = 6 * 3600
ALIAS_CACHE_TTL_SECONDS = 7 * 86400
CACHE_IDLE_SECONDS = 24 * 3600
PRIMARY_LOOKBACK_DAYS = 120
FALLBACK_LOOKBACK_DAYS = 180
PRIMARY_MIN_SERIES = 8
MAX_TEAM_GAMES = 80
MAX_TEAM_SERIES = 30
MAX_PROMPT_SERIES = 12
MAX_H2H_SERIES = 5
SERIES_GAP_SECONDS = 6 * 3600
MAX_DETAIL_SERIES = 4


def normalize_team_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _candidate_score(query: str, candidate: dict[str, Any]) -> float:
    needle = normalize_team_name(query)
    names = [candidate.get("name"), candidate.get("tag")]
    normalized = [normalize_team_name(value) for value in names if value]
    if needle in normalized:
        return 1.0
    return max((difflib.SequenceMatcher(None, needle, value).ratio() for value in normalized), default=0.0)


class OpenDotaClient:
    def __init__(self, *, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = int(timeout_seconds)

    def _get(self, path: str) -> list[dict[str, Any]]:
        request = urllib.request.Request(
            f"{OPENDOTA_API_URL}{path}",
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"opendota_http_{exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError("opendota_unavailable") from exc
        if not isinstance(payload, list):
            raise ValueError("invalid_opendota_response")
        return [row for row in payload if isinstance(row, dict)]

    def _get_object(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{OPENDOTA_API_URL}{path}",
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"opendota_http_{exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError("opendota_unavailable") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid_opendota_response")
        return payload

    def teams(self) -> list[dict[str, Any]]:
        return self._get("/teams")

    def team_matches(self, team_id: int) -> list[dict[str, Any]]:
        return self._get(f"/teams/{int(team_id)}/matches")

    def team_players(self, team_id: int) -> list[dict[str, Any]]:
        return self._get(f"/teams/{int(team_id)}/players")

    def match(self, match_id: int) -> dict[str, Any]:
        return self._get_object(f"/matches/{int(match_id)}")


def _normalize_game(row: dict[str, Any], *, cutoff_ts: int, floor_ts: int) -> dict[str, Any] | None:
    start_ts = int(row.get("start_time") or 0)
    opponent_id = int(row.get("opposing_team_id") or 0)
    if not start_ts or start_ts >= int(cutoff_ts) or start_ts < int(floor_ts) or not opponent_id:
        return None
    radiant = bool(row.get("radiant"))
    radiant_win = row.get("radiant_win")
    if not isinstance(radiant_win, bool):
        return None
    return {
        "match_id": int(row.get("match_id") or 0),
        "start_ts": start_ts,
        "opponent_id": opponent_id,
        "opponent": str(row.get("opposing_team_name") or "unknown")[:80],
        "league_id": int(row.get("leagueid") or 0),
        "event": str(row.get("league_name") or "")[:100],
        "radiant": radiant,
        "won": radiant_win if radiant else not radiant_win,
    }


def _series_from_games(games: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[list[dict[str, Any]]] = []
    for game in sorted(games, key=lambda value: int(value.get("start_ts") or 0)):
        if groups:
            previous = groups[-1][-1]
            same_pair = int(previous["opponent_id"]) == int(game["opponent_id"])
            same_league = int(previous.get("league_id") or 0) == int(game.get("league_id") or 0)
            close = int(game["start_ts"]) - int(previous["start_ts"]) <= SERIES_GAP_SECONDS
            if same_pair and same_league and close:
                groups[-1].append(game)
                continue
        groups.append([game])
    series = []
    for rows in groups:
        wins = sum(1 for row in rows if row.get("won"))
        losses = len(rows) - wins
        result = "W" if wins > losses else "L" if losses > wins else "D"
        series.append({
            "series_key": ":".join(str(row.get("match_id") or 0) for row in rows),
            "played_at": int(rows[0]["start_ts"]),
            "date": time.strftime("%Y-%m-%d", time.gmtime(int(rows[0]["start_ts"]))),
            "opponent_id": int(rows[0]["opponent_id"]),
            "opponent": str(rows[0]["opponent"]),
            "result": result,
            "score": [wins, losses],
            "games": len(rows),
            "event": str(rows[0].get("event") or ""),
            "radiant": bool(rows[0].get("radiant")),
        })
    return sorted(series, key=lambda value: int(value.get("played_at") or 0), reverse=True)


def _record(series: list[dict[str, Any]]) -> dict[str, Any]:
    decided = [row for row in series if row.get("result") in {"W", "L"}]
    wins = sum(1 for row in decided if row.get("result") == "W")
    losses = len(decided) - wins
    draws = sum(1 for row in series if row.get("result") == "D")
    return {
        "n": len(decided), "w": wins, "l": losses, "d": draws,
        "wr": round(100 * wins / len(decided), 1) if decided else None,
    }


def _compact_team(
    team: dict[str, Any], series: list[dict[str, Any]], *, cutoff_ts: int,
    window_days: int, roster: list[str],
) -> dict[str, Any]:
    compact = {
        "name": str(team.get("name") or "")[:80],
        "tag": str(team.get("tag") or "")[:20],
        "window_days": int(window_days),
        "record": _record(series),
        "last5": _record(series[:5]),
        "last10": _record(series[:10]),
        "days_since_last": (
            round((int(cutoff_ts) - int(series[0]["played_at"])) / 86400, 1) if series else None
        ),
        "recent": [
            {
                "raw_id": row.get("series_key"),
                "d": row.get("date"), "opp": row.get("opponent"), "r": row.get("result"),
                "s": row.get("score"), "games": row.get("games"), "event": row.get("event") or None,
                "metrics": row.get("metrics"),
            }
            for row in series[:MAX_PROMPT_SERIES]
        ],
    }
    if roster:
        compact["current_roster"] = roster[:8]
    return compact


class OpenDotaEvidenceService:
    def __init__(self, store: FollowStore, client: OpenDotaClient | None = None):
        self.store = store
        self.client = client or OpenDotaClient()
        self._catalog: list[dict[str, Any]] | None = None

    def resolve_team(self, name: str, *, now_ts: int) -> dict[str, Any]:
        alias_key = f"opendota:alias:dota2:{normalize_team_name(name)}"
        cached = self.store.load_ai_data_cache(alias_key, now_ts=now_ts)
        if cached and isinstance(cached.get("team"), dict):
            return dict(cached["team"])
        if self._catalog is None:
            self._catalog = self.client.teams()
        ranked = sorted(
            ((_candidate_score(name, row), int(row.get("last_match_time") or 0), row) for row in self._catalog),
            key=lambda value: (value[0], value[1]), reverse=True,
        )
        if not ranked or ranked[0][0] < 0.82 or (
            len(ranked) > 1 and ranked[0][0] < 1 and ranked[0][0] - ranked[1][0] < 0.08
        ):
            raise ValueError("opendota_team_unresolved")
        row = ranked[0][2]
        team = {
            "id": int(row.get("team_id") or 0),
            "name": str(row.get("name") or name)[:80],
            "tag": str(row.get("tag") or "")[:20],
        }
        if not team["id"]:
            raise ValueError("opendota_team_id_missing")
        self.store.save_ai_data_cache({
            "cache_key": alias_key, "cache_kind": "team_alias", "game": "dota2",
            "team_id": str(team["id"]), "team": team, "fetched_at": now_ts,
            "last_used_at": now_ts, "expires_at": now_ts + ALIAS_CACHE_TTL_SECONDS,
        })
        return team

    def team_history(self, team: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        team_id = int(team.get("id") or 0)
        cache_key = f"opendota:team:dota2:{team_id}"
        cached = self.store.load_ai_data_cache(cache_key, now_ts=now_ts)
        if cached and abs(int(cached.get("coverage_end_ts") or 0) - int(cutoff_ts)) <= TEAM_CACHE_TTL_SECONDS:
            return cached
        floor_ts = int(cutoff_ts) - FALLBACK_LOOKBACK_DAYS * 86400
        normalized = [
            value for value in (
                _normalize_game(row, cutoff_ts=cutoff_ts, floor_ts=floor_ts)
                for row in self.client.team_matches(team_id)[:MAX_TEAM_GAMES]
            ) if value
        ]
        series = _series_from_games(normalized)
        primary_floor = int(cutoff_ts) - PRIMARY_LOOKBACK_DAYS * 86400
        primary = [row for row in series if int(row.get("played_at") or 0) >= primary_floor]
        selected = primary if len(primary) >= PRIMARY_MIN_SERIES else series
        selected = selected[:MAX_TEAM_SERIES]
        if int(cutoff_ts) >= int(now_ts) - 86400:
            for item in selected[:MAX_DETAIL_SERIES]:
                match_ids = [int(value) for value in str(item.get("series_key") or "").split(":") if value.isdigit()]
                if not match_ids:
                    continue
                try:
                    # `radiant` is captured from the first game in the grouped
                    # series, so enrich that same game rather than risking
                    # attribution to the opponent after a side swap.
                    detail = self.client.match(match_ids[0])
                    players = [row for row in detail.get("players") or [] if isinstance(row, dict)]
                    radiant = bool(item.get("radiant"))
                    own = [row for row in players if (int(row.get("player_slot") or 0) < 128) == radiant]
                    if own:
                        item["metrics"] = {
                            "kda": round(sum(float(row.get("kills") or 0) + float(row.get("assists") or 0) for row in own) / max(1.0, sum(float(row.get("deaths") or 0) for row in own)), 2),
                            "gpm": round(sum(float(row.get("gold_per_min") or 0) for row in own) / len(own), 1),
                            "xpm": round(sum(float(row.get("xp_per_min") or 0) for row in own) / len(own), 1),
                            "duration_min": round(float(detail.get("duration") or 0) / 60, 1),
                        }
                except (AttributeError, RuntimeError, ValueError, TypeError):
                    continue
        roster = []
        if int(cutoff_ts) >= int(now_ts) - 86400:
            try:
                roster = [
                    str(row.get("name") or "")[:40]
                    for row in self.client.team_players(team_id)
                    if row.get("is_current_team_member") and row.get("name")
                ]
            except (RuntimeError, ValueError):
                roster = []
        payload = {
            "cache_key": cache_key, "cache_kind": "team_history", "game": "dota2",
            "team_id": str(team_id), "team": team, "series": selected,
            "roster": roster, "window_days": (
                PRIMARY_LOOKBACK_DAYS if len(primary) >= PRIMARY_MIN_SERIES else FALLBACK_LOOKBACK_DAYS
            ),
            "coverage_end_ts": int(cutoff_ts), "fetched_at": now_ts, "last_used_at": now_ts,
            "expires_at": now_ts + TEAM_CACHE_TTL_SECONDS,
        }
        self.store.save_ai_data_cache(payload)
        return payload

    def build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        outcomes = [str(value).strip() for value in market.get("outcomes") or []]
        if len(outcomes) != 2:
            raise ValueError("opendota_match_unsupported")
        team_a = self.resolve_team(outcomes[0], now_ts=now_ts)
        team_b = self.resolve_team(outcomes[1], now_ts=now_ts)
        history_a = self.team_history(team_a, cutoff_ts=cutoff_ts, now_ts=now_ts)
        history_b = self.team_history(team_b, cutoff_ts=cutoff_ts, now_ts=now_ts)
        id_a, id_b = int(team_a["id"]), int(team_b["id"])
        h2h = [row for row in history_a.get("series") or [] if int(row.get("opponent_id") or 0) == id_b]
        evidence = {
            "source": "OpenDota",
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(cutoff_ts))),
            "window": {
                "primary_days": PRIMARY_LOOKBACK_DAYS, "fallback_days": FALLBACK_LOOKBACK_DAYS,
                "min_primary_series": PRIMARY_MIN_SERIES, "max_series_per_team": MAX_TEAM_SERIES,
                "series_grouping": "same opponent + league within 6h",
            },
            "team_a": _compact_team(
                team_a, history_a.get("series") or [], cutoff_ts=cutoff_ts,
                window_days=int(history_a.get("window_days") or FALLBACK_LOOKBACK_DAYS),
                roster=list(history_a.get("roster") or []),
            ),
            "team_b": _compact_team(
                team_b, history_b.get("series") or [], cutoff_ts=cutoff_ts,
                window_days=int(history_b.get("window_days") or FALLBACK_LOOKBACK_DAYS),
                roster=list(history_b.get("roster") or []),
            ),
            "h2h": [
                {
                    "raw_id": row.get("series_key"), "d": row.get("date"), "winner": "A" if row.get("result") == "W" else "B",
                    "s": row.get("score"), "games": row.get("games"), "event": row.get("event") or None,
                }
                for row in h2h if row.get("result") in {"W", "L"}
            ][:MAX_H2H_SERIES],
            "cache_keys": [history_a["cache_key"], history_b["cache_key"]],
        }
        self.store.prune_ai_data_cache(now_ts=now_ts, idle_seconds=CACHE_IDLE_SECONDS)
        return evidence
