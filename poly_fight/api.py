from __future__ import annotations

import json
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any


def parse_gamma_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_gamma_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
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
        max_end_date: datetime | None = None,
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
            end_date_max=format_gamma_datetime(max_end_date),
        )

    def list_events_paginated(
        self,
        *,
        closed: bool,
        active: bool | None = None,
        max_pages: int = 10,
        order: str = "endDate",
        min_end_date: datetime | None = None,
        max_end_date: datetime | None = None,
        # In-scope esports games. The broad "esports" umbrella tag also returns
        # MLBB/Overwatch/CoD/etc. (~40% of its pages), so querying the specific game
        # tags keeps the fetch matched to scope and gives deeper per-game history.
        # Keep in sync with cli.CATEGORY_TAG_SLUGS["esports"].
        tag_slugs: tuple[str, ...] = ("counter-strike-2", "league-of-legends", "dota-2"),
    ) -> list[dict]:
        all_events: list[dict] = []
        limit = 100
        seen: set[str] = set()
        min_end_date = min_end_date.astimezone(timezone.utc) if min_end_date else None
        for tag_slug in tag_slugs:
            for page in range(max_pages):
                event_params = {
                    "closed": closed,
                    "active": active,
                    "limit": limit,
                    "offset": page * limit,
                    "order": order,
                    "tag_slug": tag_slug,
                }
                if max_end_date is not None:
                    event_params["max_end_date"] = max_end_date
                try:
                    batch = self.list_events(**event_params)
                except Exception as exc:
                    # 单页失败(例:Gamma 对超大 offset 返回 422,或瞬时网络错)不致命:
                    # 当作该 tag 已翻到头,用已累积的事件继续下个 tag,而非炸穿整个采集流程。
                    # offset 上限是确定性错误、重试无用(get_json 已重试),故此处只能优雅收口。
                    print(
                        json.dumps({
                            "event": "list_events_page_error",
                            "tag_slug": tag_slug,
                            "offset": page * limit,
                            "error": str(exc),
                        }),
                        file=sys.stderr,
                    )
                    break
                if not batch:
                    break
                for event in batch:
                    key = str(event.get("id") or event.get("slug") or "")
                    if key and key not in seen:
                        seen.add(key)
                        all_events.append(event)
                if min_end_date and order == "endDate":
                    dated_events = [
                        parse_gamma_datetime(event.get("endDate") or event.get("end_date"))
                        for event in batch
                    ]
                    dated_events = [end for end in dated_events if end is not None]
                    if dated_events and max(dated_events) < min_end_date:
                        break
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

    def trades_for_user(
        self,
        wallet: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        return self.data(
            "/trades",
            user=wallet,
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
        market: str | list[str] | tuple[str, ...] | None = None,
    ) -> list[dict]:
        market_value = ",".join(str(value) for value in market) if isinstance(market, (list, tuple)) else market
        return self.data(
            "/closed-positions",
            user=wallet,
            market=market_value,
            limit=limit,
            offset=offset,
            sortBy="TIMESTAMP",
            sortDirection=sort_direction,
        )

    def positions(self, wallet: str, *, limit: int = 100) -> list[dict]:
        return self.data("/positions", user=wallet, limit=limit)

    def market_positions(
        self,
        condition_id: str,
        *,
        limit: int = 20,
        sort_by: str = "TOTAL_PNL",
        sort_direction: str = "DESC",
    ) -> list[dict]:
        return self.data(
            "/v1/market-positions",
            market=condition_id,
            limit=limit,
            sortBy=sort_by,
            sortDirection=sort_direction,
        )

    def holders(self, condition_id: str, *, limit: int = 10) -> list[dict]:
        return self.data("/holders", market=condition_id, limit=limit, minBalance=1)

    def markets_by_condition_ids(self, condition_ids: list[str], *, limit: int = 500) -> list[dict]:
        ids = [str(value).lower() for value in condition_ids if value][: max(1, int(limit))]
        if not ids:
            return []
        return self.gamma("/markets", condition_ids=ids, closed="true", limit=max(len(ids), 1))
