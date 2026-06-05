from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any


class PolymarketClient:
    def __init__(
        self,
        *,
        gamma_base: str = "https://gamma-api.polymarket.com",
        data_base: str = "https://data-api.polymarket.com",
        timeout: int = 30,
        retries: int = 2,
        pause_seconds: float = 0.15,
    ) -> None:
        self.gamma_base = gamma_base.rstrip("/")
        self.data_base = data_base.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.pause_seconds = pause_seconds

    def get_json(self, base: str, path: str, params: dict[str, Any]) -> Any:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{base}{path}?{query}" if query else f"{base}{path}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "poly-fight/0.1 (+https://polymarket.com)",
            },
        )
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(self.pause_seconds * (attempt + 1))
        raise RuntimeError(f"GET failed: {url}: {last_error}")

    def gamma(self, path: str, **params: Any) -> Any:
        return self.get_json(self.gamma_base, path, params)

    def data(self, path: str, **params: Any) -> Any:
        return self.get_json(self.data_base, path, params)

    def list_events(
        self,
        *,
        closed: bool,
        active: bool | None = None,
        limit: int = 100,
        offset: int = 0,
        order: str = "endDate",
        tag_slug: str = "esports",
    ) -> list[dict]:
        return self.gamma(
            "/events",
            closed=str(closed).lower(),
            active=None if active is None else str(active).lower(),
            archived="false",
            limit=limit,
            offset=offset,
            tag_slug=tag_slug,
            order=order,
            ascending="false",
        )

    def list_events_paginated(
        self,
        *,
        closed: bool,
        active: bool | None = None,
        max_pages: int = 10,
        order: str = "endDate",
        tag_slugs: tuple[str, ...] = ("esports", "dota-2", "counter-strike-2", "league-of-legends", "valorant"),
    ) -> list[dict]:
        all_events: list[dict] = []
        limit = 100
        seen: set[str] = set()
        for tag_slug in tag_slugs:
            for page in range(max_pages):
                batch = self.list_events(
                    closed=closed,
                    active=active,
                    limit=limit,
                    offset=page * limit,
                    order=order,
                    tag_slug=tag_slug,
                )
                if not batch:
                    break
                for event in batch:
                    key = str(event.get("id") or event.get("slug") or "")
                    if key and key not in seen:
                        seen.add(key)
                        all_events.append(event)
                if len(batch) < limit:
                    break
        return all_events

    def trades_for_market(
        self,
        condition_id: str,
        *,
        limit: int = 1000,
        offset: int = 0,
        min_trade_cash: float = 50,
    ) -> list[dict]:
        return self.data(
            "/trades",
            market=condition_id,
            limit=limit,
            offset=offset,
            takerOnly="false",
            filterType="CASH",
            filterAmount=min_trade_cash,
        )

    def closed_positions(self, wallet: str, *, limit: int = 500, offset: int = 0) -> list[dict]:
        return self.data("/closed-positions", user=wallet, limit=limit, offset=offset, sortBy="TIMESTAMP")

    def positions(self, wallet: str, *, limit: int = 100) -> list[dict]:
        return self.data("/positions", user=wallet, limit=limit)

    def holders(self, condition_id: str, *, limit: int = 10) -> list[dict]:
        return self.data("/holders", market=condition_id, limit=limit, minBalance=1)
