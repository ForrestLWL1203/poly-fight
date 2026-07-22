"""Multi-provider evidence routing, normalization, scoring and compaction."""

from __future__ import annotations

import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from .leaguepedia import LeaguepediaClient, LeaguepediaEvidenceService
from .liquipedia import LiquipediaEvidenceService
from .opendota import OpenDotaEvidenceService
from .pandascore import PANDASCORE_PROVIDER, PandaScoreClient, PandaScoreEvidenceService
from .storage import FollowStore


SUPPORTED_GAMES = frozenset({"lol", "cs2", "dota2"})
PROVIDER_NAMES = ("pandascore", "opendota", "leaguepedia", "liquipedia")
EVIDENCE_PACK_MAX_CHARS = 48_000
HEALTH_CACHE_SECONDS = 7 * 86400


def game_key(market: dict[str, Any]) -> str:
    raw = str(market.get("game_family") or market.get("league") or "").strip().lower()
    return {
        "league of legends": "lol", "dota 2": "dota2", "counter-strike": "cs2",
        "counter strike": "cs2", "counter-strike 2": "cs2",
    }.get(raw, raw)


def normalize_team_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _evidence_id(source: str, side: str, row: dict[str, Any]) -> str:
    raw = "|".join((source, side, str(row.get("raw_id") or row.get("id") or ""), str(row.get("d") or row.get("date") or ""), normalize_team_name(row.get("opp") or row.get("opponent")), str(row.get("r") or row.get("result") or "")))
    return f"ev_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"


def _record_count(team: dict[str, Any]) -> int:
    return int(((team.get("record") or {}).get("n") or 0))


def _roster(team: dict[str, Any]) -> list[str]:
    return [str(value) for value in (team.get("current_roster") or team.get("roster") or []) if str(value).strip()]


def _freshness(team: dict[str, Any]) -> float | None:
    value = team.get("days_since_last")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _quality_score(
    sources: list[dict[str, Any]], *, best_of: str, conflicts: list[dict[str, Any]]
) -> tuple[int, dict[str, int], list[str]]:
    teams_a = [row.get("team_a") or {} for row in sources]
    teams_b = [row.get("team_b") or {} for row in sources]
    source_count = len(sources)
    identity = 15 if source_count and all(a.get("name") and b.get("name") for a, b in zip(teams_a, teams_b)) else 0
    meaningful_source_count = sum(
        min(_record_count(source.get("team_a") or {}), _record_count(source.get("team_b") or {})) > 0
        for source in sources
    )

    sample_a = max((_record_count(row) for row in teams_a), default=0)
    sample_b = max((_record_count(row) for row in teams_b), default=0)
    minimum_sample = min(sample_a, sample_b)
    sample_points = 20 if minimum_sample >= 12 else 16 if minimum_sample >= 8 else 10 if minimum_sample >= 5 else 4 if minimum_sample >= 2 else 0
    freshness_values = [value for row in [*teams_a, *teams_b] if (value := _freshness(row)) is not None]
    freshness_points = 5 if freshness_values and max(freshness_values) <= 14 else 3 if freshness_values and max(freshness_values) <= 30 else 0
    samples = min(25, sample_points + freshness_points)

    roster_a = max((len(_roster(row)) for row in teams_a), default=0)
    roster_b = max((len(_roster(row)) for row in teams_b), default=0)
    roster = 20 if min(roster_a, roster_b) >= 5 else 14 if min(roster_a, roster_b) >= 3 else 6 if roster_a or roster_b else 0

    rich_rows = 0
    for source in sources:
        for side in ("team_a", "team_b"):
            for row in (source.get(side) or {}).get("recent") or []:
                if row.get("opp") and (row.get("event") or row.get("tier")):
                    rich_rows += 1
    form = 20 if minimum_sample >= 8 and rich_rows >= 12 else 15 if minimum_sample >= 5 and rich_rows >= 6 else 8 if minimum_sample >= 2 else 0
    h2h_count = max((len(source.get("h2h") or []) for source in sources), default=0)
    format_points = 6 if best_of in {"BO1", "BO3", "BO5"} else 2
    h2h_format = min(10, format_points + (4 if h2h_count >= 2 else 2 if h2h_count == 1 else 0))
    multi_source = 10 if meaningful_source_count >= 2 and not conflicts else 5 if meaningful_source_count >= 2 else 0
    components = {
        "identity": identity,
        "sample_freshness": samples,
        "roster": roster,
        "form_opponent": form,
        "h2h_format": h2h_format,
        "multi_source": multi_source,
    }
    gaps = []
    if minimum_sample < 5: gaps.append("sample_thin")
    if min(roster_a, roster_b) < 3: gaps.append("roster_incomplete")
    if not h2h_count: gaps.append("h2h_missing")
    if meaningful_source_count < 2: gaps.append("single_source")
    if conflicts: gaps.append("source_conflict")
    return sum(components.values()), components, gaps


def _dedup_sources(sources: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
    merged = {"team_a": [], "team_b": [], "h2h": []}
    seen: dict[tuple[str, str, str], dict[str, Any]] = {}
    conflicts: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for source in sources:
        provider = str(source.get("provider") or source.get("source") or "unknown").lower()
        for side in ("team_a", "team_b"):
            for raw in (source.get(side) or {}).get("recent") or []:
                row = dict(raw)
                date = str(row.get("d") or row.get("date") or "")
                opponent = normalize_team_name(row.get("opp") or row.get("opponent"))
                result = str(row.get("r") or row.get("result") or "").upper()
                key = (side, date, opponent)
                if not date or not opponent or result not in {"W", "L"}:
                    continue
                existing = seen.get(key)
                if existing and existing.get("r") != result:
                    conflicts.append({"side": side, "date": date, "opponent": opponent, "sources": [existing.get("source"), provider]})
                    merged[side] = [item for item in merged[side] if item.get("id") != existing.get("id")]
                    continue
                if existing:
                    existing.setdefault("also_seen_in", []).append(provider)
                    continue
                row_id = _evidence_id(provider, side, row)
                normalized = {
                    "id": row_id, "source": provider, "d": date,
                    "raw_id": str(row.get("raw_id") or row.get("id") or "")[:100] or None,
                    "opp": str(row.get("opp") or row.get("opponent") or "")[:80],
                    "r": result, "s": row.get("s") or row.get("score"),
                    "event": str(row.get("event") or "")[:100] or None,
                    "tier": str(row.get("tier") or "")[:20] or None,
                }
                seen[key] = normalized
                merged[side].append(normalized)
                evidence_ids.append(row_id)
        for raw in source.get("h2h") or []:
            row = dict(raw)
            row_id = _evidence_id(provider, "h2h", {"raw_id": row.get("raw_id"), "d": row.get("d"), "opp": "head-to-head", "r": row.get("winner")})
            if row_id in evidence_ids:
                continue
            merged["h2h"].append({"id": row_id, "source": provider, "raw_id": str(row.get("raw_id") or "")[:100] or None, **{key: row.get(key) for key in ("d", "winner", "s", "event", "tier") if row.get(key) is not None}})
            evidence_ids.append(row_id)
    for side in ("team_a", "team_b"):
        merged[side] = sorted(merged[side], key=lambda row: str(row.get("d") or ""), reverse=True)[:12]
    merged["h2h"] = sorted(merged["h2h"], key=lambda row: str(row.get("d") or ""), reverse=True)[:5]
    kept_ids = [row["id"] for side in merged.values() for row in side if row.get("id")]
    return merged, conflicts, kept_ids


class EvidenceRouter:
    def __init__(
        self,
        store: FollowStore,
        *,
        pandascore_key: str | None = None,
        leaguepedia_credential: str | None = None,
        service_factories: dict[str, Callable[[], Any]] | None = None,
    ):
        self.store = store
        self.pandascore_key = str(pandascore_key or "").strip()
        self.leaguepedia_credential = str(leaguepedia_credential or "").strip()
        self.service_factories = service_factories or {}
        self._services: dict[str, Any] = {}

    def close(self) -> None:
        for service in self._services.values():
            close = getattr(service, "close", None)
            if close:
                close()
        self._services.clear()

    def _service(self, provider: str) -> Any:
        if provider in self._services:
            return self._services[provider]
        if provider in self.service_factories:
            service = self.service_factories[provider]()
        elif provider == "pandascore":
            if not self.pandascore_key:
                raise ValueError("pandascore_not_configured")
            service = PandaScoreEvidenceService(self.store, PandaScoreClient(self.pandascore_key))
        elif provider == "opendota":
            service = OpenDotaEvidenceService(self.store)
        elif provider == "leaguepedia":
            service = LeaguepediaEvidenceService(
                self.store,
                LeaguepediaClient(credential=self.leaguepedia_credential or None),
            )
        elif provider == "liquipedia":
            service = LiquipediaEvidenceService(self.store)
        else:
            raise ValueError("unknown_evidence_provider")
        self._services[provider] = service
        return service

    def _save_health(self, provider: str, *, now_ts: int, ok: bool, game: str, error: str = "", coverage: int = 0) -> None:
        previous = self.store.load_ai_data_cache(f"provider_health:{provider}", now_ts=now_ts, touch=False) or {}
        error_text = str(error)[:180]
        limited = any(token in error_text.lower() for token in ("rate_limit", "ratelimited", "circuit"))
        coverage_gap = any(token in error_text.lower() for token in ("team_unresolved", "team_id_missing"))
        previous_success = int(previous.get("last_success_at") or 0)
        if ok:
            status = "ok" if coverage > 0 else "empty"
        elif coverage_gap:
            status = "partial" if previous_success else "empty"
        elif limited:
            status = "limited"
        else:
            status = "error"
        self.store.save_ai_data_cache({
            "cache_key": f"provider_health:{provider}", "cache_kind": "provider_health", "game": game,
            "team_id": provider, "provider": provider,
            "status": status,
            "last_success_at": now_ts if ok else previous_success,
            "last_error_at": int(previous.get("last_error_at") or 0) if ok or coverage_gap else now_ts,
            "last_gap_at": now_ts if coverage_gap else int(previous.get("last_gap_at") or 0),
            "gap_code": error_text if coverage_gap else "",
            "error": "" if ok or coverage_gap else error_text,
            "coverage": int(coverage) if ok else int(previous.get("coverage") or 0), "fetched_at": now_ts,
            "last_used_at": now_ts, "expires_at": now_ts + HEALTH_CACHE_SECONDS,
        })

    def build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        game = game_key(market)
        if game not in SUPPORTED_GAMES:
            raise ValueError("unsupported_ai_game")
        providers = ["pandascore", {"dota2": "opendota", "lol": "leaguepedia", "cs2": "liquipedia"}[game]]
        successes: list[dict[str, Any]] = []
        failures: dict[str, str] = {}

        def fetch(provider: str) -> tuple[str, dict[str, Any]]:
            service = self._service(provider)
            return provider, service.build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts)

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="evidence") as pool:
            jobs = {pool.submit(fetch, provider): provider for provider in providers}
            for future in as_completed(jobs):
                provider = jobs[future]
                try:
                    _, evidence = future.result()
                    evidence = {**evidence, "provider": provider}
                    successes.append(evidence)
                    coverage = min(_record_count(evidence.get("team_a") or {}), _record_count(evidence.get("team_b") or {}))
                    self._save_health(provider, now_ts=now_ts, ok=True, game=game, coverage=coverage)
                except Exception as exc:
                    failures[provider] = str(exc)[:180]
                    self._save_health(provider, now_ts=now_ts, ok=False, game=game, error=str(exc))
        if not successes:
            raise RuntimeError("all_evidence_sources_failed:" + ";".join(f"{key}={value}" for key, value in sorted(failures.items())))

        title = str(market.get("title") or market.get("question") or "")
        match = re.search(r"\bBO\s*([135])\b", title, flags=re.IGNORECASE)
        best_of = f"BO{match.group(1)}" if match else "unknown"
        merged, conflicts, evidence_ids = _dedup_sources(successes)
        score, components, gaps = _quality_score(successes, best_of=best_of, conflicts=conflicts)
        outcomes = [str(value).strip() for value in market.get("outcomes") or []]
        source_summaries = []
        cache_keys: list[str] = []
        for source in sorted(successes, key=lambda row: str(row.get("provider") or "")):
            cache_keys.extend(str(key) for key in source.get("cache_keys") or [] if str(key))
            # Detailed match rows appear only in `normalized`, where every row
            # has an immutable evidence ID.  Provider summaries retain useful
            # aggregates/rosters without creating uncitable duplicate facts.
            team_summaries = {}
            for side in ("team_a", "team_b"):
                team = dict(source.get(side) or {})
                team.pop("recent", None)
                team_summaries[side] = team
            source_summaries.append({
                "provider": source.get("provider"), **team_summaries,
                "window": source.get("window"),
            })
        pack = {
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts)),
            "game": game, "best_of": best_of,
            "teams": {"team_a": outcomes[0] if len(outcomes) > 0 else "", "team_b": outcomes[1] if len(outcomes) > 1 else ""},
            "evidence_score": score, "score_components": components,
            "coverage": {"successful_sources": [row["provider"] for row in source_summaries], "failed_sources": failures, "gaps": gaps, "conflicts": conflicts},
            "normalized": merged,
            "source_summaries": source_summaries,
            "valid_evidence_ids": evidence_ids,
            "cache_keys": sorted(set(cache_keys)),
        }
        # Defensive compaction: verbose provider summaries are useful, but the
        # normalized immutable evidence is the authoritative model context.
        if len(json.dumps(pack, ensure_ascii=False, separators=(",", ":"))) > EVIDENCE_PACK_MAX_CHARS:
            pack["source_summaries"] = [
                {"provider": row["provider"], "team_a": {"record": (row.get("team_a") or {}).get("record")}, "team_b": {"record": (row.get("team_b") or {}).get("record")}}
                for row in source_summaries
            ]
        return pack
