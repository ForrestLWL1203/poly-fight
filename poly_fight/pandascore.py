"""Compact PandaScore evidence for the multi-source AI risk gate.

Raw provider payloads never reach the model or persistent cache.  Team history is
normalized, bounded by both time and count, and upserted per team.  Match-specific
evidence is assembled from those team rows immediately before an eligible BUY.
"""

from __future__ import annotations

import difflib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from .storage import FollowStore


PANDASCORE_API_URL = "https://api.pandascore.co"
PANDASCORE_PROVIDER = "pandascore"
REQUEST_TIMEOUT_SECONDS = 12
TEAM_CACHE_TTL_SECONDS = 6 * 3600
ALIAS_CACHE_TTL_SECONDS = 7 * 86400
CACHE_IDLE_SECONDS = 24 * 3600
PRIMARY_LOOKBACK_DAYS = 120
FALLBACK_LOOKBACK_DAYS = 180
PRIMARY_MIN_MATCHES = 8
MAX_TEAM_MATCHES = 20
MAX_PROMPT_MATCHES = 10
MAX_H2H_MATCHES = 5
GAME_PATHS = {"lol": "lol", "cs2": "csgo", "dota2": "dota2"}


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def normalize_team_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _team_candidate_score(query: str, candidate: dict[str, Any]) -> float:
    needle = normalize_team_name(query)
    names = [candidate.get("name"), candidate.get("acronym"), candidate.get("slug")]
    normalized = [normalize_team_name(value) for value in names if value]
    if needle in normalized:
        return 1.0
    return max((difflib.SequenceMatcher(None, needle, value).ratio() for value in normalized), default=0.0)


def compact_team_identity(team: dict[str, Any]) -> dict[str, Any]:
    """Keep only stable identity/roster fields; never persist a raw team response."""
    players = []
    for player in team.get("players") or []:
        if not isinstance(player, dict) or not player.get("name"):
            continue
        players.append({
            "id": int(player.get("id") or 0),
            "name": str(player.get("name") or "")[:40],
            "role": str(player.get("role") or "")[:30],
        })
    return {
        "id": int(team.get("id") or 0),
        "name": str(team.get("name") or "")[:80],
        "acronym": str(team.get("acronym") or "")[:20],
        "slug": str(team.get("slug") or "")[:100],
        "players": players[:12],
    }


def _opponent_rows(match: dict[str, Any]) -> list[dict[str, Any]]:
    values = []
    for wrapper in match.get("opponents") or []:
        row = wrapper.get("opponent") if isinstance(wrapper, dict) else None
        if isinstance(row, dict) and row.get("id") is not None:
            values.append(row)
    return values


def _score_for_team(match: dict[str, Any], team_id: int) -> tuple[int | None, int | None]:
    own = opponent = None
    for row in match.get("results") or []:
        if not isinstance(row, dict):
            continue
        if int(row.get("team_id") or 0) == int(team_id):
            own = int(row.get("score") or 0)
        elif row.get("team_id") is not None:
            opponent = int(row.get("score") or 0)
    return own, opponent


def normalize_match(match: dict[str, Any], *, team_id: int, cutoff_ts: int) -> dict[str, Any] | None:
    begin_ts = _timestamp(match.get("begin_at") or match.get("scheduled_at"))
    if not begin_ts or begin_ts >= int(cutoff_ts) or str(match.get("status") or "") != "finished":
        return None
    opponents = _opponent_rows(match)
    if len(opponents) != 2 or not any(int(row.get("id") or 0) == int(team_id) for row in opponents):
        return None
    other = next((row for row in opponents if int(row.get("id") or 0) != int(team_id)), None)
    winner_id = int(match.get("winner_id") or 0)
    if not other or not winner_id:
        return None
    own_score, opponent_score = _score_for_team(match, team_id)
    league = match.get("league") if isinstance(match.get("league"), dict) else {}
    serie = match.get("serie") if isinstance(match.get("serie"), dict) else {}
    tournament = match.get("tournament") if isinstance(match.get("tournament"), dict) else {}
    return {
        "match_id": int(match.get("id") or 0),
        "played_at": begin_ts,
        "date": time.strftime("%Y-%m-%d", time.gmtime(begin_ts)),
        "opponent_id": int(other.get("id") or 0),
        "opponent": str(other.get("name") or other.get("acronym") or "unknown")[:80],
        "won": winner_id == int(team_id),
        "score": [own_score, opponent_score] if own_score is not None and opponent_score is not None else None,
        "best_of": int(match.get("number_of_games") or 0),
        "event": str(tournament.get("name") or serie.get("full_name") or serie.get("name") or league.get("name") or "")[:100],
        "forfeit": bool(match.get("forfeit")),
    }


def _record(matches: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in matches if row.get("won"))
    total = len(matches)
    return {"n": total, "w": wins, "l": total - wins, "wr": round(100 * wins / total, 1) if total else None}


def compact_team_evidence(
    team: dict[str, Any],
    matches: list[dict[str, Any]],
    *,
    cutoff_ts: int,
    window_days: int,
    include_current_roster: bool,
) -> dict[str, Any]:
    matches = sorted(matches, key=lambda row: int(row.get("played_at") or 0), reverse=True)
    bo: dict[str, list[dict[str, Any]]] = {}
    for row in matches:
        key = f"BO{int(row.get('best_of') or 0)}" if int(row.get("best_of") or 0) else "unknown"
        bo.setdefault(key, []).append(row)
    roster = []
    for player in team.get("players") or []:
        if isinstance(player, dict) and player.get("name"):
            roster.append(str(player.get("name"))[:40])
    compact = {
        "name": str(team.get("name") or "")[:80],
        "acronym": str(team.get("acronym") or "")[:20],
        "window_days": int(window_days),
        "record": _record(matches),
        "last5": _record(matches[:5]),
        "last10": _record(matches[:10]),
        "by_bo": {key: _record(rows) for key, rows in sorted(bo.items())},
        "days_since_last": (
            round((int(cutoff_ts) - int(matches[0]["played_at"])) / 86400, 1) if matches else None
        ),
        "recent": [
            {
                "raw_id": row.get("match_id"),
                "d": row.get("date"), "opp": row.get("opponent"),
                "r": "W" if row.get("won") else "L",
                "bo": row.get("best_of") or None, "s": row.get("score"), "event": row.get("event") or None,
            }
            for row in matches[:MAX_PROMPT_MATCHES]
        ],
    }
    # PandaScore's team endpoint represents the current roster. It is useful for
    # live decisions but would leak post-match state into a historical replay.
    if include_current_roster and roster:
        compact["current_roster"] = roster[:8]
    return compact


class PandaScoreClient:
    def __init__(self, api_key: str, *, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = int(timeout_seconds)

    def _get(self, path: str, params: list[tuple[str, Any]]) -> list[dict[str, Any]]:
        url = f"{PANDASCORE_API_URL}{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"pandascore_http_{exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError("pandascore_unavailable") from exc
        if not isinstance(payload, list):
            raise ValueError("invalid_pandascore_response")
        return [row for row in payload if isinstance(row, dict)]

    def test(self) -> bool:
        self._get("/teams", [("page[size]", 1)])
        return True

    def search_teams(self, game: str, name: str) -> list[dict[str, Any]]:
        path = GAME_PATHS.get(str(game))
        if not path:
            raise ValueError("unsupported_pandascore_game")
        return self._get(f"/{path}/teams", [("search[name]", str(name)), ("page[size]", 10)])

    def past_matches(self, game: str, team_id: int, *, cutoff_ts: int) -> list[dict[str, Any]]:
        path = GAME_PATHS.get(str(game))
        if not path:
            raise ValueError("unsupported_pandascore_game")
        start_ts = int(cutoff_ts) - FALLBACK_LOOKBACK_DAYS * 86400
        return self._get(
            f"/{path}/matches/past",
            [
                ("filter[opponent_id]", int(team_id)),
                ("filter[status]", "finished"),
                ("range[begin_at]", f"{_iso(start_ts)},{_iso(cutoff_ts)}"),
                ("sort", "-begin_at"),
                ("page[size]", 50),
            ],
        )


class PandaScoreEvidenceService:
    def __init__(self, store: FollowStore, client: PandaScoreClient):
        self.store = store
        self.client = client

    def resolve_team(self, game: str, name: str, *, now_ts: int) -> dict[str, Any]:
        alias_key = f"pandascore:alias:{game}:{normalize_team_name(name)}"
        cached = self.store.load_ai_data_cache(alias_key, now_ts=now_ts)
        if cached and isinstance(cached.get("team"), dict):
            return dict(cached["team"])
        candidates = self.client.search_teams(game, name)
        ranked = sorted(
            ((_team_candidate_score(name, row), row) for row in candidates),
            key=lambda value: value[0], reverse=True,
        )
        if not ranked or ranked[0][0] < 0.82 or (len(ranked) > 1 and ranked[0][0] < 1 and ranked[0][0] - ranked[1][0] < 0.08):
            raise ValueError("pandascore_team_unresolved")
        team = compact_team_identity(ranked[0][1])
        self.store.save_ai_data_cache({
            "cache_key": alias_key, "cache_kind": "team_alias", "game": game,
            "team_id": str(team.get("id") or ""), "team": team,
            "fetched_at": now_ts, "last_used_at": now_ts,
            "expires_at": now_ts + ALIAS_CACHE_TTL_SECONDS,
        })
        return team

    def team_history(self, game: str, team: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        team_id = int(team.get("id") or 0)
        if not team_id:
            raise ValueError("pandascore_team_id_missing")
        cache_key = f"pandascore:team:{game}:{team_id}"
        cached = self.store.load_ai_data_cache(cache_key, now_ts=now_ts)
        if cached and abs(int(cached.get("coverage_end_ts") or 0) - int(cutoff_ts)) <= TEAM_CACHE_TTL_SECONDS:
            return cached
        raw = self.client.past_matches(game, team_id, cutoff_ts=cutoff_ts)
        normalized = [
            row for row in (normalize_match(value, team_id=team_id, cutoff_ts=cutoff_ts) for value in raw) if row
        ]
        normalized.sort(key=lambda row: int(row.get("played_at") or 0), reverse=True)
        primary_cutoff = int(cutoff_ts) - PRIMARY_LOOKBACK_DAYS * 86400
        primary = [row for row in normalized if int(row.get("played_at") or 0) >= primary_cutoff]
        selected = primary if len(primary) >= PRIMARY_MIN_MATCHES else normalized
        selected = selected[:MAX_TEAM_MATCHES]
        row = {
            "cache_key": cache_key, "cache_kind": "team_history", "game": game,
            "team_id": str(team_id), "team": {
                "id": team_id, "name": team.get("name"), "acronym": team.get("acronym"),
                "players": team.get("players") or [],
            },
            "matches": selected,
            "window_days": PRIMARY_LOOKBACK_DAYS if len(primary) >= PRIMARY_MIN_MATCHES else FALLBACK_LOOKBACK_DAYS,
            "coverage_end_ts": int(cutoff_ts),
            "fetched_at": now_ts, "last_used_at": now_ts,
            "expires_at": now_ts + TEAM_CACHE_TTL_SECONDS,
        }
        self.store.save_ai_data_cache(row)
        return row

    def build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        outcomes = [str(value).strip() for value in market.get("outcomes") or []]
        game = str(market.get("game_family") or market.get("league") or "").lower()
        aliases = {"league of legends": "lol", "dota 2": "dota2", "counter-strike": "cs2"}
        game = aliases.get(game, game)
        if len(outcomes) != 2 or game not in GAME_PATHS:
            raise ValueError("pandascore_match_unsupported")
        team_a = self.resolve_team(game, outcomes[0], now_ts=now_ts)
        team_b = self.resolve_team(game, outcomes[1], now_ts=now_ts)
        history_a = self.team_history(game, team_a, cutoff_ts=cutoff_ts, now_ts=now_ts)
        history_b = self.team_history(game, team_b, cutoff_ts=cutoff_ts, now_ts=now_ts)
        id_a, id_b = int(team_a["id"]), int(team_b["id"])
        combined: dict[int, dict[str, Any]] = {}
        for row in history_a.get("matches") or []:
            if int(row.get("opponent_id") or 0) == id_b:
                combined[int(row.get("match_id") or 0)] = row
        for row in history_b.get("matches") or []:
            if int(row.get("opponent_id") or 0) == id_a:
                combined.setdefault(int(row.get("match_id") or 0), {
                    **row, "won": not bool(row.get("won")), "opponent": team_b.get("name"),
                })
        h2h_rows = sorted(combined.values(), key=lambda row: int(row.get("played_at") or 0), reverse=True)
        include_current_roster = int(cutoff_ts) >= int(now_ts) - 86400
        evidence = {
            "source": "PandaScore",
            "as_of": _iso(cutoff_ts),
            "window": {
                "primary_days": PRIMARY_LOOKBACK_DAYS, "fallback_days": FALLBACK_LOOKBACK_DAYS,
                "min_primary_matches": PRIMARY_MIN_MATCHES, "max_matches_per_team": MAX_TEAM_MATCHES,
            },
            "team_a": compact_team_evidence(
                team_a, history_a.get("matches") or [], cutoff_ts=cutoff_ts,
                window_days=int(history_a.get("window_days") or FALLBACK_LOOKBACK_DAYS),
                include_current_roster=include_current_roster,
            ),
            "team_b": compact_team_evidence(
                team_b, history_b.get("matches") or [], cutoff_ts=cutoff_ts,
                window_days=int(history_b.get("window_days") or FALLBACK_LOOKBACK_DAYS),
                include_current_roster=include_current_roster,
            ),
            "h2h": [
                {"raw_id": row.get("match_id"), "d": row.get("date"), "winner": "A" if row.get("won") else "B",
                 "bo": row.get("best_of") or None, "s": row.get("score"), "event": row.get("event") or None}
                for row in h2h_rows[:MAX_H2H_MATCHES]
            ],
            "cache_keys": [history_a["cache_key"], history_b["cache_key"]],
        }
        self.store.prune_ai_data_cache(now_ts=now_ts, idle_seconds=CACHE_IDLE_SECONDS)
        return evidence

    def invalidate(self, cache_keys: list[str], *, now_ts: int) -> int:
        removed = self.store.delete_ai_data_cache(cache_keys)
        self.store.prune_ai_data_cache(now_ts=now_ts, idle_seconds=CACHE_IDLE_SECONDS)
        return removed
