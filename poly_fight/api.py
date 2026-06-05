from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any


def parse_retry_after(value: str | None, *, max_seconds: float, now: datetime | None = None) -> float:
    if not value:
        return 0.0
    text = str(value).strip()
    try:
        seconds = float(text)
        return max(0.0, min(float(max_seconds), seconds))
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return 0.0
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    delay = (retry_at.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()
    return max(0.0, min(float(max_seconds), delay))


class RateLimiter:
    def __init__(
        self,
        *,
        rate_per_second: float,
        burst: int = 1,
        clock=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        self.rate_per_second = float(rate_per_second)
        self.burst = max(1, int(burst))
        self.clock = clock
        self.sleeper = sleeper
        self._lock = threading.Lock()
        self._tokens = float(self.burst)
        self._updated_at = self.clock()

    def acquire(self) -> None:
        if self.rate_per_second <= 0:
            return
        wait_seconds = 0.0
        with self._lock:
            now = self.clock()
            if now > self._updated_at:
                elapsed = now - self._updated_at
                self._tokens = min(float(self.burst), self._tokens + elapsed * self.rate_per_second)
                self._updated_at = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            token_delay = (1.0 - self._tokens) / self.rate_per_second
            self._tokens = 0.0
            self._updated_at += token_delay
            wait_seconds = max(0.0, self._updated_at - now)
        if wait_seconds > 0:
            self.sleeper(wait_seconds)


class PolymarketClient:
    def __init__(
        self,
        *,
        gamma_base: str = "https://gamma-api.polymarket.com",
        data_base: str = "https://data-api.polymarket.com",
        timeout: int = 30,
        retries: int = 5,
        pause_seconds: float = 0.5,
        rate_limiter: RateLimiter | None = None,
        max_retry_after_seconds: float = 60,
        sleeper=time.sleep,
        jitter=random.uniform,
    ) -> None:
        self.gamma_base = gamma_base.rstrip("/")
        self.data_base = data_base.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.pause_seconds = pause_seconds
        self.rate_limiter = rate_limiter
        self.max_retry_after_seconds = max_retry_after_seconds
        self.sleeper = sleeper
        self.jitter = jitter

    def _backoff_seconds(self, attempt: int) -> float:
        jitter_seconds = self.jitter(0, self.pause_seconds)
        return min(float(self.max_retry_after_seconds), self.pause_seconds * (2**attempt) + jitter_seconds)

    def _sleep_before_retry(self, exc: Exception, attempt: int) -> None:
        delay = 0.0
        if isinstance(exc, urllib.error.HTTPError) and exc.code in {429, 503}:
            delay = parse_retry_after(
                exc.headers.get("Retry-After") if exc.headers else None,
                max_seconds=self.max_retry_after_seconds,
            )
        if delay <= 0:
            delay = self._backoff_seconds(attempt)
        self.sleeper(delay)

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
                if self.rate_limiter:
                    self.rate_limiter.acquire()
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_error = exc
                if attempt < self.retries:
                    self._sleep_before_retry(exc, attempt)
            except Exception as exc:
                last_error = exc
                if attempt < self.retries:
                    self._sleep_before_retry(exc, attempt)
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

    def trades_for_user_market(
        self,
        wallet: str,
        condition_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        return self.data(
            "/trades",
            user=wallet,
            market=condition_id,
            limit=limit,
            offset=offset,
            takerOnly="false",
        )

    def closed_positions(
        self,
        wallet: str,
        *,
        limit: int = 500,
        offset: int = 0,
        sort_direction: str = "DESC",
    ) -> list[dict]:
        return self.data(
            "/closed-positions",
            user=wallet,
            limit=limit,
            offset=offset,
            sortBy="TIMESTAMP",
            sortDirection=sort_direction,
        )

    def positions(self, wallet: str, *, limit: int = 100) -> list[dict]:
        return self.data("/positions", user=wallet, limit=limit)

    def holders(self, condition_id: str, *, limit: int = 10) -> list[dict]:
        return self.data("/holders", market=condition_id, limit=limit, minBalance=1)
