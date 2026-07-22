"""Liquipedia MediaWiki API evidence for CS2 full-match prediction.

This module calls only ``/counterstrike/api.php``.  It never fetches ordinary
wiki HTML.  The API-rendered ``Team recent matches table`` is normalized into a
small series list and cached; raw MediaWiki responses are not persisted.
"""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Any

import httpx
from bs4 import BeautifulSoup

from .storage import FollowStore


LIQUIPEDIA_API_URL = "https://liquipedia.net/counterstrike/api.php"
LIQUIPEDIA_PROVIDER = "liquipedia"
TEAM_CACHE_TTL_SECONDS = 6 * 3600
PRIMARY_LOOKBACK_DAYS = 120
FALLBACK_LOOKBACK_DAYS = 180
PRIMARY_MIN_SERIES = 8
MAX_TEAM_SERIES = 30
MAX_PROMPT_SERIES = 12
MAX_H2H_SERIES = 5
PARSE_REQUEST_INTERVAL_SECONDS = 30
QUERY_REQUEST_INTERVAL_SECONDS = 2


def normalize_team_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _user_agent() -> str:
    contact = str(os.environ.get("POLY_FIGHT_CONTACT_EMAIL") or "").strip()
    identity = contact or "https://github.com/ForrestLWL1203/poly-fight"
    return f"poly-fight/1.0 ({identity})"


def _safe_template_value(value: str) -> str:
    cleaned = re.sub(r"[{}|\[\]\n\r]", " ", str(value or ""))
    return re.sub(r"\s+", " ", cleaned).strip()[:100]


class LiquipediaClient:
    _rate_lock = threading.Lock()
    _last_parse_at = 0.0
    _last_query_at = 0.0

    def __init__(self, *, timeout_seconds: int = 15, transport: httpx.BaseTransport | None = None):
        self.client = httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            headers={"User-Agent": _user_agent(), "Accept": "application/json", "Accept-Encoding": "gzip"},
        )

    def close(self) -> None:
        self.client.close()

    @classmethod
    def _reserve(cls, kind: str) -> None:
        with cls._rate_lock:
            now = time.monotonic()
            attr = "_last_parse_at" if kind == "parse" else "_last_query_at"
            interval = PARSE_REQUEST_INTERVAL_SECONDS if kind == "parse" else QUERY_REQUEST_INTERVAL_SECONDS
            last = float(getattr(cls, attr))
            wait_seconds = interval - (now - last)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
            setattr(cls, attr, now)

    def recent_matches_html(self, team_names: list[str]) -> str:
        self._reserve("parse")
        safe = [_safe_template_value(name) for name in team_names]
        if len(safe) != 2 or not all(safe):
            raise ValueError("liquipedia_teams_missing")
        source = "\n".join(
            f'<div id="poly-fight-team-{index}">{{{{Team recent matches table|team={name}}}}}</div>'
            for index, name in enumerate(safe)
        )
        try:
            response = self.client.get(
                LIQUIPEDIA_API_URL,
                params={"action": "parse", "format": "json", "contentmodel": "wikitext", "text": source, "disablelimitreport": "1"},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("liquipedia_unavailable") from exc
        if isinstance(payload, dict) and payload.get("error"):
            code = str((payload.get("error") or {}).get("code") or "api_error")
            raise RuntimeError(f"liquipedia_{code}")
        html = (((payload or {}).get("parse") or {}).get("text") or {}).get("*")
        if not isinstance(html, str):
            raise ValueError("invalid_liquipedia_response")
        return html


def parse_recent_match_tables(html: str, team_names: list[str], *, cutoff_ts: int) -> dict[str, list[dict[str, Any]]]:
    soup = BeautifulSoup(str(html or ""), "html.parser")
    marked_tables = [soup.select_one(f"#poly-fight-team-{index} table") for index in range(len(team_names))]
    tables = marked_tables if all(marked_tables) else (soup.select(".match-table-wrapper table") or soup.select("table"))
    result: dict[str, list[dict[str, Any]]] = {}
    for team, table in zip(team_names, tables):
        rows: list[dict[str, Any]] = []
        for tr in table.select("tr.table2__row--body"):
            cells = tr.select("td")
            if len(cells) < 9:
                continue
            timer = cells[0].select_one("[data-timestamp]")
            played_at = int((timer or {}).get("data-timestamp") or cells[0].get("data-sort-value") or 0)
            if not played_at or played_at >= int(cutoff_ts) or played_at < int(cutoff_ts) - FALLBACK_LOOKBACK_DAYS * 86400:
                continue
            marker = cells[6].select_one("[data-label-type]")
            label = str((marker or {}).get("data-label-type") or "").lower()
            score_text = cells[7].get_text(" ", strip=True)
            numbers = [int(value) for value in re.findall(r"\d+", score_text)[:2]]
            if "win" in label:
                match_result = "W"
            elif "loss" in label:
                match_result = "L"
            elif len(numbers) == 2 and numbers[0] != numbers[1]:
                match_result = "W" if numbers[0] > numbers[1] else "L"
            else:
                continue
            opponent = cells[8].get_text(" ", strip=True)
            if not opponent:
                continue
            rows.append({
                "series_key": f"{played_at}:{normalize_team_name(opponent)}",
                "played_at": played_at,
                "date": time.strftime("%Y-%m-%d", time.gmtime(played_at)),
                "opponent": opponent[:80],
                "result": match_result,
                "score": numbers if len(numbers) == 2 else None,
                "tier": cells[1].get_text(" ", strip=True)[:30],
                "venue": cells[2].get_text(" ", strip=True)[:20],
                "event": cells[5].get_text(" ", strip=True)[:120],
            })
        result[team] = sorted(rows, key=lambda row: int(row["played_at"]), reverse=True)[:MAX_TEAM_SERIES]
    for team in team_names:
        result.setdefault(team, [])
    return result


def _record(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(row.get("result") == "W" for row in rows)
    losses = sum(row.get("result") == "L" for row in rows)
    total = wins + losses
    return {"n": total, "w": wins, "l": losses, "wr": round(100 * wins / total, 1) if total else None}


def _compact(team: str, rows: list[dict[str, Any]], *, cutoff_ts: int, window_days: int) -> dict[str, Any]:
    return {
        "name": team,
        "window_days": int(window_days),
        "record": _record(rows),
        "last5": _record(rows[:5]),
        "last10": _record(rows[:10]),
        "days_since_last": round((cutoff_ts - int(rows[0]["played_at"])) / 86400, 1) if rows else None,
        "recent": [
            {"raw_id": row.get("series_key"), "d": row["date"], "opp": row["opponent"], "r": row["result"], "s": row["score"], "tier": row["tier"] or None, "event": row["event"] or None}
            for row in rows[:MAX_PROMPT_SERIES]
        ],
    }


class LiquipediaEvidenceService:
    def __init__(self, store: FollowStore, client: LiquipediaClient | None = None):
        self.store = store
        self.client = client or LiquipediaClient()

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()

    def build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        outcomes = [str(value).strip() for value in market.get("outcomes") or []]
        if len(outcomes) != 2:
            raise ValueError("liquipedia_match_unsupported")
        keys = [f"liquipedia:team:cs2:{normalize_team_name(name)}" for name in outcomes]
        cached = [self.store.load_ai_data_cache(key, now_ts=now_ts) for key in keys]
        if all(cached) and all(abs(int(row.get("coverage_end_ts") or 0) - cutoff_ts) <= TEAM_CACHE_TTL_SECONDS for row in cached):
            histories = cached
        else:
            parsed = parse_recent_match_tables(self.client.recent_matches_html(outcomes), outcomes, cutoff_ts=cutoff_ts)
            histories = []
            for team, key in zip(outcomes, keys):
                rows = parsed.get(team) or []
                primary = [row for row in rows if int(row["played_at"]) >= cutoff_ts - PRIMARY_LOOKBACK_DAYS * 86400]
                selected = primary if len(primary) >= PRIMARY_MIN_SERIES else rows
                payload = {
                    "cache_key": key, "cache_kind": "team_history", "game": "cs2", "team_id": normalize_team_name(team),
                    "team": {"name": team}, "series": selected[:MAX_TEAM_SERIES],
                    "window_days": PRIMARY_LOOKBACK_DAYS if len(primary) >= PRIMARY_MIN_SERIES else FALLBACK_LOOKBACK_DAYS,
                    "coverage_end_ts": cutoff_ts, "fetched_at": now_ts, "last_used_at": now_ts,
                    "expires_at": now_ts + TEAM_CACHE_TTL_SECONDS,
                }
                self.store.save_ai_data_cache(payload)
                histories.append(payload)
        rows_a, rows_b = list(histories[0].get("series") or []), list(histories[1].get("series") or [])
        needle_b = normalize_team_name(outcomes[1])
        h2h = [row for row in rows_a if normalize_team_name(row.get("opponent")) == needle_b][:MAX_H2H_SERIES]
        return {
            "source": "Liquipedia",
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts)),
            "window": {"primary_days": PRIMARY_LOOKBACK_DAYS, "fallback_days": FALLBACK_LOOKBACK_DAYS, "max_series_per_team": MAX_TEAM_SERIES},
            "team_a": _compact(outcomes[0], rows_a, cutoff_ts=cutoff_ts, window_days=int(histories[0].get("window_days") or FALLBACK_LOOKBACK_DAYS)),
            "team_b": _compact(outcomes[1], rows_b, cutoff_ts=cutoff_ts, window_days=int(histories[1].get("window_days") or FALLBACK_LOOKBACK_DAYS)),
            "h2h": [{"raw_id": row.get("series_key"), "d": row["date"], "winner": "A" if row["result"] == "W" else "B", "s": row["score"], "tier": row["tier"] or None, "event": row["event"] or None} for row in h2h],
            "cache_keys": keys,
        }
