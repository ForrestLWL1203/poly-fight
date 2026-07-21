"""DeepSeek-backed, pre-match risk gate for esports main-match paper follows.

The model receives match metadata only.  Wallet identity, intended side, price and
stake never enter the prompt; the local gate compares the returned independent
prediction with the candidate side after all ordinary strategy checks pass.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .core import to_float
from .storage import FollowStore


DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_BALANCE_URL = "https://api.deepseek.com/user/balance"
DEFAULT_MODEL = "deepseek-v4-pro"
PROMPT_VERSION = "esports-main-v1"
WIN_PROBABILITY_THRESHOLD = 65.0
CONFIDENCE_THRESHOLD = 75.0
REQUEST_TIMEOUT_SECONDS = 15
ERROR_RETRY_SECONDS = 300
SUPPORTED_GAMES = frozenset({"lol", "cs2", "dota2"})
HARD_UNCERTAINTY_FLAGS = frozenset({"roster_unknown", "stale_knowledge"})


SYSTEM_PROMPT = (
    "你是电竞赛前历史实力判断器。仅按已有历史资料判断，不参考赔率、钱包、下注方向；"
    "资料不足或过时则不要猜。只返回JSON。"
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now() -> int:
    # Keep config/audit timestamps independent from runner tests and clocks that
    # intentionally patch time.time() to exercise retry windows.
    return int(time.time_ns() // 1_000_000_000)


def _db_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _db_connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=1")
    return conn


class AiConfigStore:
    """Persistent AI settings and envelope ciphertext, intentionally outside reset targets."""

    def __init__(self, data_dir: Path):
        self.secret_dir = Path(data_dir) / ".secrets"
        self.db_path = self.secret_dir / "ai_config.db"
        self.private_key_path = Path(
            os.environ.get("POLY_FIGHT_CREDENTIAL_KEY_FILE")
            or self.secret_dir / "credential_wrap_private.pem"
        )
        self.public_key_path = self.secret_dir / "credential_wrap_public.pem"
        self.init_db()

    def init_db(self) -> None:
        self.secret_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.secret_dir, 0o700)
        except OSError:
            pass
        with _db_connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ai_risk_settings (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    enabled INTEGER NOT NULL DEFAULT 0,
                    model TEXT NOT NULL,
                    win_probability_threshold REAL NOT NULL,
                    confidence_threshold REAL NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS provider_credential (
                    provider TEXT PRIMARY KEY,
                    envelope_version INTEGER NOT NULL,
                    key_id TEXT NOT NULL,
                    wrapped_key TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    ciphertext TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_validated_at INTEGER
                );
                CREATE TABLE IF NOT EXISTS provider_balance_snapshot (
                    balance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT NOT NULL,
                    checked_at INTEGER NOT NULL,
                    currency TEXT,
                    total_balance REAL,
                    granted_balance REAL,
                    topped_up_balance REAL,
                    is_available INTEGER,
                    error TEXT
                );
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO ai_risk_settings "
                "(id,enabled,model,win_probability_threshold,confidence_threshold,updated_at) "
                "VALUES (1,0,?,?,?,?)",
                (DEFAULT_MODEL, WIN_PROBABILITY_THRESHOLD, CONFIDENCE_THRESHOLD, _now()),
            )
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass
        self.ensure_keypair()

    def ensure_keypair(self) -> None:
        if not self.private_key_path.exists():
            self.private_key_path.parent.mkdir(parents=True, exist_ok=True)
            key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
            pem = key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            try:
                fd = os.open(str(self.private_key_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                # Dashboard and runner can perform first-time initialization at
                # the same moment; the other process has already won the race.
                pass
            else:
                try:
                    os.write(fd, pem)
                finally:
                    os.close(fd)
        try:
            os.chmod(self.private_key_path, 0o600)
        except OSError:
            pass
        private = serialization.load_pem_private_key(self.private_key_path.read_bytes(), password=None)
        public_pem = private.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        if not self.public_key_path.exists() or self.public_key_path.read_bytes() != public_pem:
            tmp = self.public_key_path.with_name(
                f"{self.public_key_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            tmp.write_bytes(public_pem)
            os.chmod(tmp, 0o644)
            os.replace(tmp, self.public_key_path)

    def public_wrap_key(self) -> dict[str, Any]:
        public = serialization.load_pem_public_key(self.public_key_path.read_bytes())
        der = public.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        return {
            "ready": True,
            "algorithm": "RSA-OAEP-256+A256GCM",
            "envelopeVersion": 1,
            "keyId": hashlib.sha256(der).hexdigest()[:24],
            "spki": base64.b64encode(der).decode("ascii"),
        }

    def decrypt_envelope(self, envelope: dict[str, Any]) -> str:
        expected = self.public_wrap_key()
        if int(envelope.get("envelopeVersion") or 0) != 1 or envelope.get("keyId") != expected["keyId"]:
            raise ValueError("unknown_credential_wrapping_key")
        try:
            wrapped = base64.b64decode(str(envelope["wrappedKey"]), validate=True)
            nonce = base64.b64decode(str(envelope["nonce"]), validate=True)
            ciphertext = base64.b64decode(str(envelope["ciphertext"]), validate=True)
        except (KeyError, ValueError) as exc:
            raise ValueError("invalid_credential_envelope") from exc
        private = serialization.load_pem_private_key(self.private_key_path.read_bytes(), password=None)
        dek = private.decrypt(
            wrapped,
            padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
        )
        value = AESGCM(dek).decrypt(nonce, ciphertext, None).decode("utf-8").strip()
        if not value:
            raise ValueError("empty_credential")
        return value

    def settings(self) -> dict[str, Any]:
        with _db_connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM ai_risk_settings WHERE id=1").fetchone()
        return {
            "enabled": bool(row["enabled"]),
            "model": str(row["model"]),
            "win_probability_threshold": float(row["win_probability_threshold"]),
            "confidence_threshold": float(row["confidence_threshold"]),
            "updated_at": int(row["updated_at"]),
        }

    def save_settings(self, *, enabled: bool) -> dict[str, Any]:
        with _db_connect(self.db_path) as conn:
            conn.execute("UPDATE ai_risk_settings SET enabled=?, updated_at=? WHERE id=1", (1 if enabled else 0, _now()))
        return self.settings()

    def credential_envelope(self) -> dict[str, Any] | None:
        with _db_connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT envelope_version,key_id,wrapped_key,nonce,ciphertext FROM provider_credential WHERE provider='deepseek'"
            ).fetchone()
        if not row:
            return None
        return {
            "envelopeVersion": int(row["envelope_version"]),
            "keyId": row["key_id"],
            "wrappedKey": row["wrapped_key"],
            "nonce": row["nonce"],
            "ciphertext": row["ciphertext"],
        }

    def secret(self) -> str | None:
        envelope = self.credential_envelope()
        return self.decrypt_envelope(envelope) if envelope else None

    def save_credential(self, envelope: dict[str, Any]) -> None:
        # Decrypt before writing so malformed envelopes never replace a working key.
        self.decrypt_envelope(envelope)
        now = _now()
        with _db_connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_credential
                (provider,envelope_version,key_id,wrapped_key,nonce,ciphertext,status,last_error,created_at,updated_at,last_validated_at)
                VALUES ('deepseek',?,?,?,?,?,'valid',NULL,?,?,?)
                ON CONFLICT(provider) DO UPDATE SET
                    envelope_version=excluded.envelope_version,key_id=excluded.key_id,
                    wrapped_key=excluded.wrapped_key,nonce=excluded.nonce,ciphertext=excluded.ciphertext,
                    status='valid',last_error=NULL,updated_at=excluded.updated_at,last_validated_at=excluded.last_validated_at
                """,
                (
                    int(envelope["envelopeVersion"]), envelope["keyId"], envelope["wrappedKey"],
                    envelope["nonce"], envelope["ciphertext"], now, now, now,
                ),
            )

    def mark_credential_error(self, error: str) -> None:
        with _db_connect(self.db_path) as conn:
            conn.execute(
                "UPDATE provider_credential SET status='error',last_error=?,updated_at=? WHERE provider='deepseek'",
                (str(error)[:300], _now()),
            )

    def mark_credential_valid(self) -> None:
        now = _now()
        with _db_connect(self.db_path) as conn:
            conn.execute(
                "UPDATE provider_credential SET status='valid',last_error=NULL,updated_at=?,last_validated_at=? "
                "WHERE provider='deepseek'",
                (now, now),
            )

    def delete_credential(self) -> bool:
        with _db_connect(self.db_path) as conn:
            changed = conn.execute("DELETE FROM provider_credential WHERE provider='deepseek'").rowcount
        return bool(changed)

    def save_balance(self, response: dict[str, Any] | None, *, error: str = "") -> dict[str, Any]:
        info = ((response or {}).get("balance_infos") or [{}])[0]
        available = bool((response or {}).get("is_available")) if response else False
        row = {
            "checked_at": _now(),
            "currency": str(info.get("currency") or ""),
            "total_balance": to_float(info.get("total_balance")),
            "granted_balance": to_float(info.get("granted_balance")),
            "topped_up_balance": to_float(info.get("topped_up_balance")),
            "is_available": available,
            "error": str(error)[:300] if error else "",
        }
        with _db_connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO provider_balance_snapshot "
                "(provider,checked_at,currency,total_balance,granted_balance,topped_up_balance,is_available,error) "
                "VALUES ('deepseek',?,?,?,?,?,?,?)",
                (
                    row["checked_at"], row["currency"], row["total_balance"], row["granted_balance"],
                    row["topped_up_balance"], 1 if available else 0, row["error"] or None,
                ),
            )
        return row

    def status(self) -> dict[str, Any]:
        with _db_connect(self.db_path) as conn:
            settings = conn.execute("SELECT * FROM ai_risk_settings WHERE id=1").fetchone()
            credential = conn.execute(
                "SELECT status,last_error,updated_at,last_validated_at FROM provider_credential WHERE provider='deepseek'"
            ).fetchone()
            balance = conn.execute(
                "SELECT * FROM provider_balance_snapshot WHERE provider='deepseek' ORDER BY balance_id DESC LIMIT 1"
            ).fetchone()
        return self._status_rows(settings, credential, balance)

    @staticmethod
    def _status_rows(
        settings: sqlite3.Row | None,
        credential: sqlite3.Row | None,
        balance: sqlite3.Row | None,
    ) -> dict[str, Any]:
        return {
            "settings": {
                "enabled": bool(settings["enabled"]) if settings else False,
                "model": str(settings["model"]) if settings else DEFAULT_MODEL,
                "win_probability_threshold": (
                    float(settings["win_probability_threshold"]) if settings else WIN_PROBABILITY_THRESHOLD
                ),
                "confidence_threshold": (
                    float(settings["confidence_threshold"]) if settings else CONFIDENCE_THRESHOLD
                ),
                "updated_at": int(settings["updated_at"]) if settings else 0,
            },
            "credential": {
                "configured": bool(credential),
                "status": str(credential["status"]) if credential else "not_configured",
                "last_error": str(credential["last_error"] or "") if credential else "",
                "updated_at": int(credential["updated_at"] or 0) if credential else 0,
                "last_validated_at": int(credential["last_validated_at"] or 0) if credential else 0,
            },
            "balance": ({
                "checked_at": int(balance["checked_at"]),
                "currency": balance["currency"],
                "total_balance": float(balance["total_balance"] or 0),
                "is_available": bool(balance["is_available"]),
                "error": str(balance["error"] or ""),
            } if balance else None),
        }

    @classmethod
    def read_existing_status(cls, data_dir: Path) -> dict[str, Any]:
        """Read existing settings without creating files, keys, tables, or rows."""
        db_path = Path(data_dir) / ".secrets" / "ai_config.db"
        if not db_path.exists():
            return cls._status_rows(None, None, None)
        try:
            with _db_connect_readonly(db_path) as conn:
                settings = conn.execute("SELECT * FROM ai_risk_settings WHERE id=1").fetchone()
                credential = conn.execute(
                    "SELECT status,last_error,updated_at,last_validated_at "
                    "FROM provider_credential WHERE provider='deepseek'"
                ).fetchone()
                balance = conn.execute(
                    "SELECT * FROM provider_balance_snapshot WHERE provider='deepseek' "
                    "ORDER BY balance_id DESC LIMIT 1"
                ).fetchone()
        except sqlite3.Error:
            return cls._status_rows(None, None, None)
        return cls._status_rows(settings, credential, balance)


class DeepSeekClient:
    def __init__(self, api_key: str, *, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def _request(self, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method="POST" if body is not None else "GET",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # Never include provider bodies: they can contain request echoes or operational detail.
            raise RuntimeError(f"deepseek_http_{exc.code}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError("deepseek_unavailable") from exc

    def balance(self) -> dict[str, Any]:
        response = self._request(DEEPSEEK_BALANCE_URL)
        if not isinstance(response.get("is_available"), bool) or not isinstance(response.get("balance_infos"), list):
            raise ValueError("invalid_deepseek_balance")
        return response

    def assess(self, prompt_payload: dict[str, Any], *, model: str) -> tuple[dict[str, Any], dict[str, Any], int]:
        started = time.monotonic()
        response = self._request(
            DEEPSEEK_API_URL,
            {
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": _json_dumps(prompt_payload)},
                ],
                "thinking": {"type": "disabled"},
                "response_format": {"type": "json_object"},
                "temperature": 0.1,
                "max_tokens": 120,
            },
        )
        try:
            parsed = json.loads(response["choices"][0]["message"]["content"])
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid_deepseek_json") from exc
        return parsed, response, int((time.monotonic() - started) * 1000)


def validate_assessment_output(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("assessment_not_object")
    required = {"winner", "a", "b", "confidence", "knowledge", "reason"}
    if not required.issubset(value):
        raise ValueError("assessment_missing_fields")
    winner = str(value["winner"]).strip().upper()
    knowledge = str(value["knowledge"]).strip().lower()
    if winner not in {"A", "B", "UNKNOWN"}:
        raise ValueError("assessment_bad_verdict")
    if knowledge not in {"ok", "stale", "insufficient"}:
        raise ValueError("assessment_bad_quality")
    try:
        pa = float(value["a"])
        pb = float(value["b"])
        confidence = float(value["confidence"])
    except (TypeError, ValueError) as exc:
        raise ValueError("assessment_bad_scores") from exc
    if not all(math.isfinite(n) and 0 <= n <= 100 for n in (pa, pb, confidence)) or not 98 <= pa + pb <= 102:
        raise ValueError("assessment_bad_scores")
    if winner == "A" and pa < pb or winner == "B" and pb < pa:
        raise ValueError("assessment_verdict_probability_mismatch")
    verdict = "team_a" if winner == "A" else "team_b" if winner == "B" else "insufficient"
    quality = "medium" if knowledge == "ok" else "low"
    recency = "recent" if knowledge == "ok" else "stale" if knowledge == "stale" else "unknown"
    flags = ["stale_knowledge"] if knowledge == "stale" else ["insufficient_history"] if knowledge == "insufficient" else []
    return {
        "verdict": verdict,
        "team_a_win_probability": round(pa, 2),
        "team_b_win_probability": round(pb, 2),
        "confidence": round(confidence, 2),
        "data_quality": quality,
        "knowledge_recency": recency,
        "reason_zh": str(value["reason"] or "")[:120],
        "key_factors": [],
        "risk_flags": flags,
    }


def _game_key(market: dict[str, Any]) -> str:
    raw = str(market.get("game_family") or market.get("league") or "").strip().lower()
    aliases = {"league of legends": "lol", "dota 2": "dota2", "counter-strike": "cs2", "counter strike": "cs2"}
    return aliases.get(raw, raw)


def build_match_prompt(market: dict[str, Any], *, now_ts: int) -> tuple[dict[str, Any], dict[str, Any]]:
    outcomes = [str(value).strip() for value in (market.get("outcomes") or [])]
    if len(outcomes) != 2 or not outcomes[0] or not outcomes[1]:
        raise ValueError("main_match_teams_missing")
    game = _game_key(market)
    if game not in SUPPORTED_GAMES:
        raise ValueError("unsupported_ai_game")
    title = str(market.get("title") or market.get("question") or "")
    bo = re.search(r"\bBO\s*([1357])\b", title, flags=re.IGNORECASE)
    metadata = {
        "condition_id": str(market.get("condition_id") or market.get("conditionId") or "").lower(),
        "game": game,
        "team_a": outcomes[0],
        "team_b": outcomes[1],
        "tournament": str(market.get("event_title") or market.get("series") or title)[:300],
        "stage": str(market.get("stage") or "")[:120],
        "best_of": f"BO{bo.group(1)}" if bo else "unknown",
        "start_time": market.get("match_start_time") or market.get("market_start_time") or market.get("eventStartTime"),
        "analysis_date_utc": time.strftime("%Y-%m-%d", time.gmtime(now_ts)),
    }
    # Deliberately excludes condition_id from the provider payload as well; it adds no sporting evidence.
    # Provider input intentionally stays close to the user's shortest useful
    # question. Tournament/title is retained locally for audit/UI, but omitted
    # here because it commonly repeats both team names and wastes prompt tokens.
    compact_match = "｜".join(
        str(value) for value in (
            game.upper(), f"{outcomes[0]} vs {outcomes[1]}", metadata["best_of"],
            metadata["analysis_date_utc"],
        ) if value and value != "unknown"
    )
    prompt = {
        "question": f"{compact_match}。按历史实力判断谁赢面更大？",
        "json": {"winner": "A|B|unknown", "a": "0-100", "b": "0-100", "confidence": "0-100", "knowledge": "ok|stale|insufficient", "reason": "24字内"},
    }
    return metadata, prompt


def assessment_direction(assessment: dict[str, Any], settings: dict[str, Any]) -> str:
    if assessment.get("status") != "ok":
        return "unavailable"
    verdict = str(assessment.get("verdict") or "insufficient")
    if verdict not in {"team_a", "team_b"}:
        return "insufficient"
    probability = to_float(assessment.get(f"{verdict}_win_probability"))
    confidence = to_float(assessment.get("confidence"))
    flags = {str(flag).lower() for flag in assessment.get("risk_flags") or []}
    if (
        probability < to_float(settings.get("win_probability_threshold"), WIN_PROBABILITY_THRESHOLD)
        or confidence < to_float(settings.get("confidence_threshold"), CONFIDENCE_THRESHOLD)
        or assessment.get("data_quality") not in {"high", "medium"}
        or assessment.get("knowledge_recency") not in {"current", "recent"}
        or flags & HARD_UNCERTAINTY_FLAGS
    ):
        return "insufficient"
    return verdict


class AiRiskService:
    """Hot-reloadable AI gate shared across runner ticks."""

    def __init__(self, data_dir: Path, follow_store: FollowStore, *, client_factory=DeepSeekClient):
        self.config = AiConfigStore(Path(data_dir))
        self.follow_store = follow_store
        self.client_factory = client_factory
        self._lock = threading.Lock()
        self._assessment_locks: dict[str, threading.Lock] = {}

    def close(self) -> None:
        """Compatibility hook; the signal-triggered service owns no background workers."""

    def enabled(self) -> bool:
        status = self.config.status()
        return bool(status["settings"]["enabled"] and status["credential"]["configured"])

    def eligible_market(self, market: dict[str, Any]) -> bool:
        return (
            str(market.get("category") or "esports").lower() == "esports"
            and str(market.get("market_type") or "main_match").lower() == "main_match"
            and _game_key(market) in SUPPORTED_GAMES
        )

    def ensure_assessment(self, market: dict[str, Any], *, now_ts: int) -> dict[str, Any]:
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        if not condition_id:
            return {"condition_id": "", "status": "unavailable", "error": "condition_id_missing"}
        with self._lock:
            condition_lock = self._assessment_locks.setdefault(condition_id, threading.Lock())
        with condition_lock:
            return self._ensure_assessment_locked(market, now_ts=now_ts, condition_id=condition_id)

    def _ensure_assessment_locked(
        self, market: dict[str, Any], *, now_ts: int, condition_id: str
    ) -> dict[str, Any]:
        try:
            metadata, prompt = build_match_prompt({**market, "condition_id": condition_id}, now_ts=now_ts)
        except ValueError as exc:
            return {"condition_id": condition_id, "status": "unavailable", "error": str(exc)}
        input_hash = hashlib.sha256((PROMPT_VERSION + _json_dumps(prompt)).encode("utf-8")).hexdigest()
        cached = self.follow_store.load_ai_assessment(condition_id)
        if cached and cached.get("input_hash") == input_hash:
            if cached.get("status") == "ok" or now_ts - int(cached.get("updated_at") or 0) < ERROR_RETRY_SECONDS:
                return cached
        settings = self.config.settings()
        created_at = int((cached or {}).get("created_at") or now_ts)
        base = {
            **metadata,
            "prompt_version": PROMPT_VERSION,
            "model": settings["model"],
            "input_hash": input_hash,
            "created_at": created_at,
            "updated_at": now_ts,
        }
        try:
            secret = self.config.secret()
            if not secret:
                raise ValueError("deepseek_not_configured")
            parsed, response, latency_ms = self.client_factory(secret).assess(prompt, model=settings["model"])
            validated = validate_assessment_output(parsed)
            self.config.mark_credential_valid()
            usage = response.get("usage") or {}
            assessment = {
                **base,
                **validated,
                "status": "ok",
                "latency_ms": latency_ms,
                "usage": {
                    "prompt_tokens": int(usage.get("prompt_tokens") or 0),
                    "completion_tokens": int(usage.get("completion_tokens") or 0),
                    "total_tokens": int(usage.get("total_tokens") or 0),
                },
                "provider_request": prompt,
                "provider_response": response,
                "parsed_output": parsed,
                "response_model": str(response.get("model") or settings["model"]),
            }
        except Exception as exc:  # fail open, but preserve a sanitized audit record
            assessment = {**base, "status": "unavailable", "error": str(exc)[:300]}
        self.follow_store.save_ai_assessment(assessment)
        return assessment

    def decide(
        self,
        *,
        market: dict[str, Any],
        wallet: str,
        outcome_index: int,
        intended_stake: float,
        entry_price: float,
        trade_id: str,
        wallet_trade_size: float,
        now_ts: int,
    ) -> dict[str, Any] | None:
        settings = self.config.settings()
        if not settings.get("enabled") or not self.eligible_market(market):
            return None
        assessment = self.ensure_assessment(market, now_ts=now_ts)
        direction = assessment_direction(assessment, settings)
        intended_side = "team_a" if int(outcome_index) == 0 else "team_b" if int(outcome_index) == 1 else "unknown"
        action = "unavailable" if direction == "unavailable" else "insufficient" if direction == "insufficient" else "agree" if direction == intended_side else "blocked"
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        intent_id = "ai:" + hashlib.sha256(
            f"{str(wallet).lower()}|{condition_id}|{outcome_index}|{trade_id}".encode("utf-8")
        ).hexdigest()[:32]
        outcomes = list(market.get("outcomes") or [])
        intent = {
            "intent_id": intent_id,
            "condition_id": condition_id,
            "wallet": str(wallet).lower(),
            "outcome_index": int(outcome_index),
            "outcome": outcomes[outcome_index] if 0 <= outcome_index < len(outcomes) else None,
            "action": action,
            "status": "open",
            "intended_stake": round(float(intended_stake), 8),
            "actual_stake": 0.0 if action == "blocked" else round(float(intended_stake), 8),
            "entry_price": round(float(entry_price), 8),
            "source_trade_id": str(trade_id),
            "assessment_condition_id": condition_id,
            "event_title": market.get("title"),
            "market_question": market.get("question"),
            "market_type": market.get("market_type"),
            "category": market.get("category") or "esports",
            "game_family": market.get("game_family") or market.get("league"),
            "match_start_time": market.get("match_start_time") or market.get("market_start_time"),
            "end_date": market.get("end_date"),
            "outcomes": outcomes,
            "created_at": now_ts,
            "updated_at": now_ts,
        }
        self.follow_store.save_ai_intent(intent)
        if action == "blocked":
            self.follow_store.save_ai_shadow(
                {
                    "shadow_id": "shadow:" + intent_id,
                    "intent_id": intent_id,
                    "condition_id": condition_id,
                    "wallet": str(wallet).lower(),
                    "outcome_index": int(outcome_index),
                    "outcome": intent.get("outcome"),
                    "status": "open",
                    "match_start_time": intent.get("match_start_time"),
                    "end_date": intent.get("end_date"),
                    "event_title": intent.get("event_title"),
                    "market_type": intent.get("market_type"),
                    "game_family": intent.get("game_family"),
                    "entry_price": round(float(entry_price), 8),
                    "baseline_stake": round(float(intended_stake), 8),
                    "remaining_stake": round(float(intended_stake), 8),
                    "wallet_trade_size": round(float(wallet_trade_size), 8),
                    "wallet_sell_size": 0.0,
                    "realized_pnl": 0.0,
                    "created_at": now_ts,
                    "updated_at": now_ts,
                }
            )
        return {
            "intent_id": intent_id,
            "action": action,
            "blocked": action == "blocked",
            "assessment": {
                key: assessment.get(key)
                for key in (
                    "status", "verdict", "team_a", "team_b", "team_a_win_probability",
                    "team_b_win_probability", "confidence", "data_quality", "knowledge_recency",
                    "reason_zh", "key_factors", "risk_flags", "model", "prompt_version", "updated_at",
                )
            },
        }

    def observe_sell(self, *, wallet: str, trade: dict[str, Any], condition_id: str, outcome_index: int, price: float, now_ts: int) -> int:
        matches = [
            row for row in self.follow_store.load_ai_shadows(open_only=True)
            if str(row.get("wallet") or "").lower() == str(wallet).lower()
            and str(row.get("condition_id") or "").lower() == str(condition_id).lower()
            and int(row.get("outcome_index") or 0) == int(outcome_index)
        ]
        if not matches:
            return 0
        sell_size = to_float(trade.get("size") or trade.get("amount"))
        wallet_remaining_by_shadow = {
            str(row.get("shadow_id") or ""): max(
                0.0, to_float(row.get("wallet_trade_size")) - to_float(row.get("wallet_sell_size"))
            )
            for row in matches
        }
        total_wallet_remaining = sum(wallet_remaining_by_shadow.values())
        fraction = min(1.0, sell_size / total_wallet_remaining) if total_wallet_remaining > 0 else 1.0
        changed = 0
        for shadow in matches:
            remaining = to_float(shadow.get("remaining_stake"))
            sold_stake = remaining * fraction
            wallet_remaining = wallet_remaining_by_shadow.get(str(shadow.get("shadow_id") or ""), 0.0)
            entry = to_float(shadow.get("entry_price"))
            pnl = sold_stake * (float(price) - entry) / entry if entry > 0 else 0.0
            shadow["remaining_stake"] = round(max(0.0, remaining - sold_stake), 8)
            shadow["wallet_sell_size"] = round(
                to_float(shadow.get("wallet_sell_size")) + wallet_remaining * fraction, 8
            )
            shadow["realized_pnl"] = round(to_float(shadow.get("realized_pnl")) + pnl, 8)
            shadow["status"] = "exited" if shadow["remaining_stake"] <= 1e-8 else "open"
            shadow["exit_price"] = round(float(price), 8)
            shadow["updated_at"] = now_ts
            if shadow["status"] == "exited":
                shadow["resolved_at"] = now_ts
                shadow["ai_net_effect"] = round(-to_float(shadow.get("realized_pnl")), 8)
            self.follow_store.save_ai_shadow(shadow)
            if shadow["status"] == "exited":
                self.follow_store.update_ai_intent_status(shadow.get("intent_id"), "exited", updated_at=now_ts)
            changed += 1
        return changed

    def apply_shadow_stop_loss(self, prices_by_condition: dict[str, list[float]], *, drop_pct: float, now_ts: int) -> int:
        if drop_pct <= 0:
            return 0
        changed = 0
        for shadow in self.follow_store.load_ai_shadows(open_only=True):
            prices = prices_by_condition.get(str(shadow.get("condition_id") or "").lower()) or []
            index = int(shadow.get("outcome_index") or 0)
            current = to_float(prices[index]) if 0 <= index < len(prices) else 0.0
            entry = to_float(shadow.get("entry_price"))
            if current <= 0 or entry <= 0 or current > entry * (1.0 - drop_pct / 100.0):
                continue
            remaining = to_float(shadow.get("remaining_stake"))
            pnl = remaining * (current - entry) / entry
            shadow["realized_pnl"] = round(to_float(shadow.get("realized_pnl")) + pnl, 8)
            shadow["remaining_stake"] = 0.0
            shadow["status"] = "exited"
            shadow["exit_price"] = round(current, 8)
            shadow["exit_reason"] = "stop_loss"
            shadow["resolved_at"] = now_ts
            shadow["updated_at"] = now_ts
            shadow["ai_net_effect"] = round(-to_float(shadow.get("realized_pnl")), 8)
            self.follow_store.save_ai_shadow(shadow)
            self.follow_store.update_ai_intent_status(shadow.get("intent_id"), "exited", updated_at=now_ts)
            changed += 1
        return changed

    def settle_shadows(self, resolutions: dict[str, int], *, now_ts: int, void_index: int = -2) -> int:
        changed = 0
        for shadow in self.follow_store.load_ai_shadows(open_only=True):
            winner = resolutions.get(str(shadow.get("condition_id") or "").lower())
            if winner is None:
                continue
            entry = to_float(shadow.get("entry_price"))
            remaining = to_float(shadow.get("remaining_stake"))
            if winner == void_index:
                pnl = remaining * (0.5 - entry) / entry if entry > 0 else 0.0
                outcome_won = None
            else:
                outcome_won = int(winner) == int(shadow.get("outcome_index") or 0)
                pnl = remaining * (1.0 - entry) / entry if outcome_won and entry > 0 else -remaining
            shadow["realized_pnl"] = round(to_float(shadow.get("realized_pnl")) + pnl, 8)
            shadow["remaining_stake"] = 0.0
            shadow["status"] = "settled"
            shadow["outcome_won"] = outcome_won
            shadow["resolved_at"] = now_ts
            shadow["updated_at"] = now_ts
            shadow["ai_net_effect"] = round(-to_float(shadow.get("realized_pnl")), 8)
            self.follow_store.save_ai_shadow(shadow)
            self.follow_store.update_ai_intent_status(shadow.get("intent_id"), "settled", updated_at=now_ts)
            changed += 1
        return changed


def ai_audit_summary(store: FollowStore) -> dict[str, Any]:
    return store.load_ai_summary()
