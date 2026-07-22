"""Read-only Polymarket CLOB orderbook helpers for proprietary paper entries."""

from __future__ import annotations

import json
from typing import Any

import httpx


CLOB_API_URL = "https://clob.polymarket.com"


def market_token_ids(market: dict[str, Any]) -> list[str]:
    raw = market.get("clob_token_ids") or market.get("clobTokenIds") or market.get("tokens") or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = [part.strip() for part in raw.split(",") if part.strip()]
    tokens = []
    for item in raw if isinstance(raw, list) else []:
        value = item.get("token_id") or item.get("tokenId") if isinstance(item, dict) else item
        if str(value or "").strip():
            tokens.append(str(value).strip())
    return tokens


def _levels(book: dict[str, Any], side: str) -> list[tuple[float, float]]:
    rows = []
    for row in book.get(side) or []:
        try:
            price, size = float(row.get("price")), float(row.get("size"))
        except (AttributeError, TypeError, ValueError):
            continue
        if 0 < price < 1 and size > 0:
            rows.append((price, size))
    return sorted(rows, key=lambda value: value[0], reverse=side == "bids")


def vwap_for_cash(asks: list[tuple[float, float]], cash: float) -> tuple[float, float]:
    remaining = max(0.0, float(cash))
    if remaining <= 0:
        return 0.0, 0.0
    spent = 0.0
    shares = 0.0
    for price, size in sorted(asks, key=lambda value: value[0]):
        level_cash = price * size
        take_cash = min(remaining, level_cash)
        if take_cash <= 0:
            continue
        spent += take_cash
        shares += take_cash / price
        remaining -= take_cash
        if remaining <= 1e-9:
            break
    return (spent / shares if shares > 0 else 0.0), spent


def evaluate_books(
    books: list[dict[str, Any]], *, predicted_index: int, planned_stake: float,
    max_spread: float = 0.04, depth_band: float = 0.02, depth_multiple: float = 3.0,
) -> dict[str, Any]:
    if len(books) != 2:
        return {"eligible": False, "reason": "orderbook_missing_side"}
    normalized = [{"bids": _levels(book, "bids"), "asks": _levels(book, "asks")} for book in books]
    if any(not book["bids"] or not book["asks"] for book in normalized):
        return {"eligible": False, "reason": "orderbook_missing_side"}
    if predicted_index not in {0, 1}:
        return {"eligible": False, "reason": "prediction_missing"}
    selected = normalized[predicted_index]
    best_bid = selected["bids"][0][0]
    best_ask = selected["asks"][0][0]
    spread = best_ask - best_bid
    if spread < 0 or spread > float(max_spread) + 1e-9:
        return {"eligible": False, "reason": "spread_too_wide", "spread": round(spread, 8)}
    depth_cash = sum(price * size for price, size in selected["asks"] if price <= best_ask + float(depth_band) + 1e-9)
    required_depth = max(0.0, float(planned_stake)) * float(depth_multiple)
    if depth_cash + 1e-9 < required_depth:
        return {"eligible": False, "reason": "depth_insufficient", "depth_usdc": round(depth_cash, 8), "required_depth_usdc": round(required_depth, 8)}
    vwap, filled = vwap_for_cash(selected["asks"], planned_stake)
    if filled + 1e-8 < float(planned_stake) or not 0 < vwap < 1:
        return {"eligible": False, "reason": "vwap_unfillable", "filled_usdc": round(filled, 8)}
    return {
        "eligible": True, "reason": "eligible", "best_bid": round(best_bid, 8),
        "best_ask": round(best_ask, 8), "spread": round(spread, 8),
        "depth_usdc": round(depth_cash, 8), "required_depth_usdc": round(required_depth, 8),
        "vwap": round(vwap, 8), "filled_usdc": round(filled, 8),
    }


class PolymarketOrderbookClient:
    def __init__(self, *, timeout_seconds: int = 10, transport: httpx.BaseTransport | None = None):
        self.client = httpx.Client(timeout=timeout_seconds, transport=transport, headers={"Accept": "application/json"})

    def close(self) -> None:
        self.client.close()

    def books(self, token_ids: list[str]) -> list[dict[str, Any]]:
        if len(token_ids) != 2:
            raise ValueError("orderbook_token_ids_missing")
        try:
            response = self.client.post(f"{CLOB_API_URL}/books", json=[{"token_id": token} for token in token_ids])
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("orderbook_unavailable") from exc
        if not isinstance(payload, list):
            raise ValueError("invalid_orderbook_response")
        by_token = {str(row.get("asset_id") or row.get("token_id") or ""): row for row in payload if isinstance(row, dict)}
        if by_token and all(token in by_token for token in token_ids):
            return [by_token[token] for token in token_ids]
        return [dict(row) for row in payload[:2] if isinstance(row, dict)]
