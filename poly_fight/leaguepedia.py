"""Leaguepedia Cargo evidence for LoL full-match prediction.

Only Cargo's structured API is used.  Games are grouped by MatchId so an
individual map can never become a main-match sample.  The client deliberately
uses a conservative single-flight queue and a circuit breaker because Fandom's
public Cargo service frequently returns ``ratelimited`` even for light traffic.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import defaultdict
from typing import Any

import httpx

from .storage import FollowStore


LEAGUEPEDIA_API_URL = "https://lol.fandom.com/api.php"
LEAGUEPEDIA_PROVIDER = "leaguepedia"
TEAM_CACHE_TTL_SECONDS = 6 * 3600
MAX_CACHE_SECONDS = 7 * 86400
PRIMARY_LOOKBACK_DAYS = 120
FALLBACK_LOOKBACK_DAYS = 180
PRIMARY_MIN_SERIES = 8
MAX_TEAM_SERIES = 30
MAX_PROMPT_SERIES = 12
MAX_H2H_SERIES = 5
MAX_CARGO_ROWS = 200
MIN_REQUEST_INTERVAL_SECONDS = 60
CIRCUIT_BREAKER_SECONDS = 15 * 60


def normalize_team_name(value: Any) -> str:
    return "".join(ch for ch in str(value or "").casefold() if ch.isalnum())


def _user_agent() -> str:
    contact = str(os.environ.get("POLY_FIGHT_CONTACT_EMAIL") or "").strip()
    # Fandom/MediaWiki asks automated clients to identify the project/version
    # and provide an email contact. Production supplies the operator address
    # through POLY_FIGHT_CONTACT_EMAIL; keep an email-shaped project fallback
    # so every runtime remains well-formed.
    identity = contact or "ForrestLWL1203@users.noreply.github.com"
    return f"poly-fight/1.1 ({identity})"


def _credential(value: str | dict[str, Any] | None) -> dict[str, str] | None:
    if value is None:
        return None
    try:
        raw = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid_leaguepedia_credential") from exc
    credential = {
        "username": str(raw.get("username") or "").strip(),
        "password": str(raw.get("password") or "").strip(),
        "contact_email": str(raw.get("contact_email") or "").strip(),
    }
    if not all(credential.values()) or "@" not in credential["contact_email"]:
        raise ValueError("invalid_leaguepedia_credential")
    return credential


class LeaguepediaClient:
    _request_lock = threading.Lock()
    _last_request_at = 0.0
    _circuit_open_until = 0.0

    def __init__(
        self,
        *,
        credential: str | dict[str, Any] | None = None,
        timeout_seconds: int = 12,
        transport: httpx.BaseTransport | None = None,
    ):
        self.credential = _credential(credential)
        self.authenticated = False
        self._auth_lock = threading.Lock()
        contact_email = (self.credential or {}).get("contact_email")
        self.client = httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            headers={
                "User-Agent": (
                    f"poly-fight/1.1 ({contact_email})" if contact_email else _user_agent()
                ),
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
            },
        )

    def close(self) -> None:
        self.client.close()

    @classmethod
    def _enter_queue(cls) -> float:
        with cls._request_lock:
            now = time.monotonic()
            if now < cls._circuit_open_until:
                raise RuntimeError("leaguepedia_circuit_open")
            wait_seconds = MIN_REQUEST_INTERVAL_SECONDS - (now - cls._last_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
                if now < cls._circuit_open_until:
                    raise RuntimeError("leaguepedia_circuit_open")
            cls._last_request_at = now
            return now

    @classmethod
    def _trip_circuit(cls, now: float) -> None:
        with cls._request_lock:
            cls._circuit_open_until = max(cls._circuit_open_until, now + CIRCUIT_BREAKER_SECONDS)

    def _ensure_authenticated(self) -> None:
        if not self.credential or self.authenticated:
            return
        with self._auth_lock:
            if self.authenticated:
                return
            try:
                token_response = self.client.get(
                    LEAGUEPEDIA_API_URL,
                    params={
                        "action": "query", "meta": "tokens", "type": "login",
                        "format": "json", "formatversion": "2",
                    },
                )
                token_response.raise_for_status()
                token_payload = token_response.json()
                if not isinstance(token_payload, dict):
                    raise ValueError("login_token_invalid")
                token = str(
                    (((token_payload.get("query") or {}).get("tokens") or {}).get("logintoken"))
                    or ""
                )
                if not token:
                    raise ValueError("login_token_missing")
                login_response = self.client.post(
                    LEAGUEPEDIA_API_URL,
                    data={
                        "action": "login", "format": "json", "formatversion": "2",
                        "lgname": self.credential["username"],
                        "lgpassword": self.credential["password"],
                        "lgtoken": token,
                    },
                )
                login_response.raise_for_status()
                login_payload = login_response.json()
                if not isinstance(login_payload, dict):
                    raise ValueError("login_response_invalid")
                result = str((login_payload.get("login") or {}).get("result") or "")
            except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
                raise RuntimeError("leaguepedia_login_unavailable") from exc
            if result.lower() != "success":
                raise RuntimeError("leaguepedia_login_failed")
            self.authenticated = True

    def scoreboard_games(self, team_names: list[str], *, cutoff_ts: int) -> list[dict[str, Any]]:
        now = self._enter_queue()
        self._ensure_authenticated()
        escaped = [str(name).replace("'", "''") for name in team_names if str(name).strip()]
        if not escaped:
            return []
        teams = ",".join(f"'{name}'" for name in escaped)
        start_day = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(cutoff_ts) - FALLBACK_LOOKBACK_DAYS * 86400))
        cutoff_day = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(cutoff_ts)))
        params = {
            "action": "cargoquery",
            "format": "json",
            "formatversion": "2",
            "tables": "ScoreboardGames=SG",
            "fields": (
                "SG.MatchId,SG.N_GameInMatch,SG.DateTime_UTC,SG.Team1,SG.Team2,"
                "SG.Winner,SG.Tournament,SG.Team1Players,SG.Team2Players"
            ),
            "where": (
                f"(SG.Team1 IN ({teams}) OR SG.Team2 IN ({teams})) AND "
                f"SG.DateTime_UTC >= '{start_day}' AND SG.DateTime_UTC < '{cutoff_day}'"
            ),
            "order_by": "SG.DateTime_UTC DESC",
            # Both teams share one indexed, time-bounded query. Two hundred
            # game rows cover the capped series history without requesting
            # Fandom's full anonymous 500-row allowance.
            "limit": str(MAX_CARGO_ROWS),
        }
        try:
            response = self.client.get(LEAGUEPEDIA_API_URL, params=params)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            raise RuntimeError("leaguepedia_unavailable") from exc
        error = payload.get("error") if isinstance(payload, dict) else None
        code = str((error or {}).get("code") or "").lower()
        if code == "ratelimited":
            self._trip_circuit(now)
            raise RuntimeError("leaguepedia_ratelimited")
        if error:
            raise RuntimeError(f"leaguepedia_{code or 'api_error'}")
        rows = payload.get("cargoquery") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ValueError("invalid_leaguepedia_response")
        return [dict(row.get("title") or {}) for row in rows if isinstance(row, dict)]


def _timestamp(value: Any) -> int:
    text = str(value or "").strip().replace(" ", "T")
    if not text:
        return 0
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return 0


def _players(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()][:8]
    return [part.strip() for part in re.split(r"[;,]", str(value or "")) if part.strip()][:8]


def group_scoreboard_games(rows: list[dict[str, Any]], *, team: str, cutoff_ts: int) -> list[dict[str, Any]]:
    needle = normalize_team_name(team)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        played_at = _timestamp(row.get("DateTime UTC") or row.get("DateTime_UTC"))
        if not played_at or played_at >= int(cutoff_ts):
            continue
        team1, team2 = str(row.get("Team1") or ""), str(row.get("Team2") or "")
        if needle not in {normalize_team_name(team1), normalize_team_name(team2)}:
            continue
        match_id = str(row.get("MatchId") or row.get("OverviewPage") or "").strip()
        if not match_id:
            match_id = f"{played_at // 21600}:{normalize_team_name(team1)}:{normalize_team_name(team2)}"
        grouped[match_id].append({**row, "_played_at": played_at})
    series = []
    for match_id, games in grouped.items():
        games.sort(key=lambda row: (int(row.get("N GameInMatch") or row.get("N_GameInMatch") or 0), row["_played_at"]))
        team_wins = 0
        opponent_wins = 0
        opponent = ""
        roster: list[str] = []
        for game in games:
            is_a = normalize_team_name(game.get("Team1")) == needle
            opponent = str(game.get("Team2") if is_a else game.get("Team1") or opponent)
            # Cargo ScoreboardGames encodes Winner as side 1/2, not always as
            # a team name.  Treating "1" as a literal name inverted every
            # Leaguepedia result into a loss for the requested team.
            winner_raw = str(game.get("Winner") or "").strip()
            if winner_raw.casefold() in {"1", "team1"}:
                winner_raw = str(game.get("Team1") or "")
            elif winner_raw.casefold() in {"2", "team2"}:
                winner_raw = str(game.get("Team2") or "")
            winner = normalize_team_name(winner_raw)
            if winner == needle:
                team_wins += 1
            elif winner:
                opponent_wins += 1
            roster = _players(game.get("Team1Players") if is_a else game.get("Team2Players")) or roster
        if team_wins == opponent_wins:
            continue
        played_at = min(int(row["_played_at"]) for row in games)
        series.append({
            "series_key": match_id,
            "played_at": played_at,
            "date": time.strftime("%Y-%m-%d", time.gmtime(played_at)),
            "opponent": opponent[:80],
            "result": "W" if team_wins > opponent_wins else "L",
            "score": [team_wins, opponent_wins],
            "games": len(games),
            "event": str(games[0].get("Tournament") or games[0].get("OverviewPage") or "")[:120],
            "roster": roster,
        })
    return sorted(series, key=lambda row: int(row["played_at"]), reverse=True)[:MAX_TEAM_SERIES]


def _record(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(row.get("result") == "W" for row in rows)
    losses = sum(row.get("result") == "L" for row in rows)
    total = wins + losses
    return {"n": total, "w": wins, "l": losses, "wr": round(100 * wins / total, 1) if total else None}


def _compact(team: str, rows: list[dict[str, Any]], *, cutoff_ts: int, window_days: int) -> dict[str, Any]:
    latest_roster = list(rows[0].get("roster") or []) if rows else []
    return {
        "name": team,
        "window_days": window_days,
        "record": _record(rows),
        "last5": _record(rows[:5]),
        "last10": _record(rows[:10]),
        "days_since_last": round((cutoff_ts - int(rows[0]["played_at"])) / 86400, 1) if rows else None,
        "current_roster": latest_roster,
        "recent": [
            {"raw_id": row.get("series_key"), "d": row["date"], "opp": row["opponent"], "r": row["result"], "s": row["score"], "event": row["event"] or None}
            for row in rows[:MAX_PROMPT_SERIES]
        ],
    }


class LeaguepediaEvidenceService:
    def __init__(self, store: FollowStore, client: LeaguepediaClient | None = None):
        self.store = store
        self.client = client or LeaguepediaClient()

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()

    def build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        outcomes = [str(value).strip() for value in market.get("outcomes") or []]
        if len(outcomes) != 2:
            raise ValueError("leaguepedia_match_unsupported")
        keys = [f"leaguepedia:team:lol:{normalize_team_name(name)}" for name in outcomes]
        cached = [self.store.load_ai_data_cache(key, now_ts=now_ts) for key in keys]
        if all(cached) and all(abs(int(row.get("coverage_end_ts") or 0) - cutoff_ts) <= TEAM_CACHE_TTL_SECONDS for row in cached):
            histories = cached
        else:
            raw = self.client.scoreboard_games(outcomes, cutoff_ts=cutoff_ts)
            histories = []
            for name, key in zip(outcomes, keys):
                rows = group_scoreboard_games(raw, team=name, cutoff_ts=cutoff_ts)
                primary = [row for row in rows if int(row["played_at"]) >= cutoff_ts - PRIMARY_LOOKBACK_DAYS * 86400]
                selected = primary if len(primary) >= PRIMARY_MIN_SERIES else rows
                payload = {
                    "cache_key": key, "cache_kind": "team_history", "game": "lol", "team_id": normalize_team_name(name),
                    "team": {"name": name}, "series": selected[:MAX_TEAM_SERIES],
                    "window_days": PRIMARY_LOOKBACK_DAYS if len(primary) >= PRIMARY_MIN_SERIES else FALLBACK_LOOKBACK_DAYS,
                    "coverage_end_ts": cutoff_ts, "fetched_at": now_ts, "last_used_at": now_ts,
                    "expires_at": min(now_ts + TEAM_CACHE_TTL_SECONDS, now_ts + MAX_CACHE_SECONDS),
                }
                self.store.save_ai_data_cache(payload)
                histories.append(payload)
        rows_a, rows_b = list(histories[0].get("series") or []), list(histories[1].get("series") or [])
        needle_b = normalize_team_name(outcomes[1])
        h2h = [row for row in rows_a if normalize_team_name(row.get("opponent")) == needle_b][:MAX_H2H_SERIES]
        return {
            "source": "Leaguepedia",
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts)),
            "window": {"primary_days": PRIMARY_LOOKBACK_DAYS, "fallback_days": FALLBACK_LOOKBACK_DAYS, "max_series_per_team": MAX_TEAM_SERIES},
            "team_a": _compact(outcomes[0], rows_a, cutoff_ts=cutoff_ts, window_days=int(histories[0].get("window_days") or FALLBACK_LOOKBACK_DAYS)),
            "team_b": _compact(outcomes[1], rows_b, cutoff_ts=cutoff_ts, window_days=int(histories[1].get("window_days") or FALLBACK_LOOKBACK_DAYS)),
            "h2h": [{"raw_id": row.get("series_key"), "d": row["date"], "winner": "A" if row["result"] == "W" else "B", "s": row["score"], "event": row["event"] or None} for row in h2h],
            "cache_keys": keys,
        }
