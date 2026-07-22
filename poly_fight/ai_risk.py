"""Gemini-backed, multi-source RAG risk gate for esports main-match paper follows.

The model receives compact pre-cutoff team evidence merged from game-specific
public APIs. Wallet identity, intended side, price and stake never enter the
prompt; local gates alone decide wallet blocking and self-run shadow entry.
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
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .core import to_float
from .evidence import EvidenceRouter, game_key
from .orderbook import PolymarketOrderbookClient, evaluate_books, market_token_ids
from .pandascore import PANDASCORE_PROVIDER
from .storage import FollowStore


PROVIDER = "gemini"
LEGACY_PROVIDER = "deepseek"
DEFAULT_MODEL = "gemini-3.6-flash"
PROMPT_VERSION = "esports-main-rag-v2"
WIN_PROBABILITY_THRESHOLD = 65.0
CONFIDENCE_THRESHOLD = 75.0
WALLET_MIN_EVIDENCE_SCORE = 70
PROPRIETARY_MIN_EVIDENCE_SCORE = 80
REQUEST_TIMEOUT_SECONDS = 15
ERROR_RETRY_SECONDS = 300
SUPPORTED_GAMES = frozenset({"lol", "cs2", "dota2"})
PROPRIETARY_INITIAL_BANKROLL = 5000.0
PROPRIETARY_MIN_VOLUME = 1000.0
PROPRIETARY_LIQUIDITY_PROBE_HOURS = (3, 2, 1)


SYSTEM_PROMPT = """你是全球电竞（CS2、LoL、Dota2）全场赛前胜负精算师。
只评估整场比赛最终大比分胜方（Match Winner），绝不预测或提及 Map、单局、击杀等子盘口。

只能使用用户提供并标明证据ID的赛前证据，不得用模型记忆补全比赛、阵容或赛果。双方从50:50开始，综合：
长期与近期战绩、对手含金量、历史交锋、阵容稳定性、赛事等级和BO赛制。
不得参考投注市场、赔率、钱包、下注方向或金额；不得使用目标比赛开赛后的赛果；不得编造资料。

两队分值必须在0-100且合计100。除非资料完全镜像，否则不得给50:50。
证据不足、互相冲突或无法识别队伍时必须返回 insufficient，不要勉强猜测。
supporting_evidence_ids只能引用输入中真实存在的证据ID。reason_zh只写一句简短结论。"""

GEMINI_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["decisive", "insufficient"]},
        "winner": {"type": ["string", "null"], "enum": ["team_a", "team_b", None]},
        "team_a_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "team_b_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "supporting_evidence_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
        "risk_flags": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
        "reason_zh": {"type": "string", "maxLength": 120},
    },
    "required": ["status", "winner", "team_a_score", "team_b_score", "confidence", "supporting_evidence_ids", "risk_flags", "reason_zh"],
    "additionalProperties": False,
}


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
            # Migrate the active model to Gemini without touching the legacy
            # DeepSeek ciphertext.  Provider credentials are keyed separately,
            # so the historical key remains available for audit/delete only.
            conn.execute(
                "UPDATE ai_risk_settings SET model=?,updated_at=? WHERE model<>?",
                (DEFAULT_MODEL, _now(), DEFAULT_MODEL),
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

    def credential_envelope(self, provider: str = PROVIDER) -> dict[str, Any] | None:
        with _db_connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT envelope_version,key_id,wrapped_key,nonce,ciphertext "
                "FROM provider_credential WHERE provider=?",
                (str(provider),),
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

    def secret(self, provider: str = PROVIDER) -> str | None:
        envelope = self.credential_envelope(provider)
        return self.decrypt_envelope(envelope) if envelope else None

    def save_credential(self, envelope: dict[str, Any], provider: str = PROVIDER) -> None:
        # Decrypt before writing so malformed envelopes never replace a working key.
        self.decrypt_envelope(envelope)
        now = _now()
        with _db_connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_credential
                (provider,envelope_version,key_id,wrapped_key,nonce,ciphertext,status,last_error,created_at,updated_at,last_validated_at)
                VALUES (?,?,?,?,?,?,'valid',NULL,?,?,?)
                ON CONFLICT(provider) DO UPDATE SET
                    envelope_version=excluded.envelope_version,key_id=excluded.key_id,
                    wrapped_key=excluded.wrapped_key,nonce=excluded.nonce,ciphertext=excluded.ciphertext,
                    status='valid',last_error=NULL,updated_at=excluded.updated_at,last_validated_at=excluded.last_validated_at
                """,
                (
                    str(provider), int(envelope["envelopeVersion"]), envelope["keyId"], envelope["wrappedKey"],
                    envelope["nonce"], envelope["ciphertext"], now, now, now,
                ),
            )

    def mark_credential_error(self, error: str, provider: str = PROVIDER) -> None:
        with _db_connect(self.db_path) as conn:
            conn.execute(
                "UPDATE provider_credential SET status='error',last_error=?,updated_at=? WHERE provider=?",
                (str(error)[:300], _now(), str(provider)),
            )

    def mark_credential_valid(self, provider: str = PROVIDER) -> None:
        now = _now()
        with _db_connect(self.db_path) as conn:
            conn.execute(
                "UPDATE provider_credential SET status='valid',last_error=NULL,updated_at=?,last_validated_at=? "
                "WHERE provider=?",
                (now, now, str(provider)),
            )

    def delete_credential(self, provider: str = PROVIDER) -> bool:
        with _db_connect(self.db_path) as conn:
            changed = conn.execute("DELETE FROM provider_credential WHERE provider=?", (str(provider),)).rowcount
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
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    PROVIDER, row["checked_at"], row["currency"], row["total_balance"], row["granted_balance"],
                    row["topped_up_balance"], 1 if available else 0, row["error"] or None,
                ),
            )
        return row

    def status(self) -> dict[str, Any]:
        with _db_connect(self.db_path) as conn:
            settings = conn.execute("SELECT * FROM ai_risk_settings WHERE id=1").fetchone()
            credential = conn.execute(
                "SELECT status,last_error,updated_at,last_validated_at FROM provider_credential WHERE provider=?",
                (PROVIDER,),
            ).fetchone()
            data_credential = conn.execute(
                "SELECT status,last_error,updated_at,last_validated_at FROM provider_credential WHERE provider=?",
                (PANDASCORE_PROVIDER,),
            ).fetchone()
            balance = conn.execute(
                "SELECT * FROM provider_balance_snapshot WHERE provider=? ORDER BY balance_id DESC LIMIT 1",
                (PROVIDER,),
            ).fetchone()
        value = self._status_rows(settings, credential, balance)
        value["data_credential"] = self._credential_status_row(data_credential)
        return value

    @staticmethod
    def _credential_status_row(credential: sqlite3.Row | None) -> dict[str, Any]:
        return {
            "configured": bool(credential),
            "status": str(credential["status"]) if credential else "not_configured",
            "last_error": str(credential["last_error"] or "") if credential else "",
            "updated_at": int(credential["updated_at"] or 0) if credential else 0,
            "last_validated_at": int(credential["last_validated_at"] or 0) if credential else 0,
        }

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
            "credential": AiConfigStore._credential_status_row(credential),
            "data_credential": AiConfigStore._credential_status_row(None),
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
                    "FROM provider_credential WHERE provider=?",
                    (PROVIDER,),
                ).fetchone()
                data_credential = conn.execute(
                    "SELECT status,last_error,updated_at,last_validated_at "
                    "FROM provider_credential WHERE provider=?",
                    (PANDASCORE_PROVIDER,),
                ).fetchone()
                balance = conn.execute(
                    "SELECT * FROM provider_balance_snapshot WHERE provider=? ORDER BY balance_id DESC LIMIT 1",
                    (PROVIDER,),
                ).fetchone()
        except sqlite3.Error:
            return cls._status_rows(None, None, None)
        value = cls._status_rows(settings, credential, balance)
        value["data_credential"] = cls._credential_status_row(data_credential)
        return value


class GeminiClient:
    """Gemini structured-output client with every optional external tool disabled."""

    def __init__(self, api_key: str, *, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = int(timeout_seconds)
        self.client = genai.Client(api_key=self.api_key, http_options=types.HttpOptions(timeout=self.timeout_seconds * 1000))

    def test(self, *, model: str = DEFAULT_MODEL) -> dict[str, Any]:
        parsed, response, latency = self.assess({"task": "连接测试；返回insufficient", "valid_evidence_ids": []}, model=model)
        return {"ok": bool(parsed), "model": str(getattr(response, "model_version", None) or model), "latency_ms": latency}

    def assess(self, prompt_payload: dict[str, Any], *, model: str) -> tuple[dict[str, Any], Any, int]:
        started = time.monotonic()
        try:
            response = self.client.models.generate_content(
                model=model,
                contents=_json_dumps(prompt_payload),
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_json_schema=GEMINI_RESPONSE_SCHEMA,
                    # Gemini 3.x accounts for internal reasoning inside the
                    # generation ceiling.  Medium thinking keeps the matchup
                    # comparison substantive; 4K prevents a valid short JSON
                    # answer from being cut off after its thought phase.
                    thinking_config=types.ThinkingConfig(
                        thinking_level=types.ThinkingLevel.MEDIUM,
                    ),
                    max_output_tokens=4096,
                ),
            )
            parsed = response.parsed if isinstance(getattr(response, "parsed", None), dict) else json.loads(str(response.text or ""))
        except json.JSONDecodeError as exc:
            raise ValueError("invalid_gemini_json") from exc
        except Exception as exc:
            # Provider SDK errors can include request metadata; store only a
            # stable class name, never the raw response or credential-bearing request.
            raise RuntimeError(f"gemini_unavailable:{type(exc).__name__}") from exc
        return parsed, response, int((time.monotonic() - started) * 1000)


def _gemini_usage(response: Any) -> dict[str, int]:
    usage = getattr(response, "usage_metadata", None)
    return {
        "prompt_tokens": int(getattr(usage, "prompt_token_count", 0) or 0),
        "completion_tokens": int(getattr(usage, "candidates_token_count", 0) or 0),
        "total_tokens": int(getattr(usage, "total_token_count", 0) or 0),
    }


def validate_assessment_output(value: Any, *, valid_evidence_ids: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("assessment_not_object")
    required = {"status", "winner", "team_a_score", "team_b_score", "confidence", "supporting_evidence_ids", "risk_flags", "reason_zh"}
    if not required.issubset(value):
        raise ValueError("assessment_missing_fields")
    status = str(value["status"]).strip().lower()
    winner = value.get("winner")
    if winner is not None:
        winner = str(winner).strip().lower()
    if status not in {"decisive", "insufficient"} or winner not in {"team_a", "team_b", None}:
        raise ValueError("assessment_bad_verdict")
    try:
        pa = float(value["team_a_score"])
        pb = float(value["team_b_score"])
        confidence = float(value["confidence"])
    except (TypeError, ValueError) as exc:
        raise ValueError("assessment_bad_scores") from exc
    if not all(math.isfinite(n) and 0 <= n <= 100 for n in (pa, pb, confidence)) or abs(pa + pb - 100) > 1e-9:
        raise ValueError("assessment_bad_scores")
    if status == "decisive" and winner is None or status == "insufficient" and winner is not None:
        raise ValueError("assessment_status_winner_mismatch")
    if winner == "team_a" and pa <= pb or winner == "team_b" and pb <= pa:
        raise ValueError("assessment_verdict_probability_mismatch")
    ids = [str(item) for item in value.get("supporting_evidence_ids") or []]
    if valid_evidence_ids is not None and any(item not in valid_evidence_ids for item in ids):
        raise ValueError("assessment_unknown_evidence_id")
    if status == "decisive" and not ids:
        raise ValueError("assessment_evidence_missing")
    return {
        "verdict": winner or "insufficient",
        "team_a_win_probability": round(pa, 2),
        "team_b_win_probability": round(pb, 2),
        "confidence": round(confidence, 2),
        "reason_zh": str(value["reason_zh"] or "")[:120],
        "supporting_evidence_ids": ids[:8],
        "risk_flags": [str(flag)[:60] for flag in value.get("risk_flags") or []][:8],
    }


def _game_key(market: dict[str, Any]) -> str:
    return game_key(market)


def _parse_timestamp(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def build_match_prompt(
    market: dict[str, Any], *, now_ts: int, evidence: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
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
    # Keep the provider blind to condition_id, wallet, intended side, price and
    # stake. Tournament/stage and the time cutoff help disambiguate similarly
    # named teams and prevent accidental use of a later result.
    prompt = {
        "task": "只根据Evidence Pack独立评估两队赢下整场比赛的赢面与置信度",
        "match": {
            "game": game.upper(),
            "team_a": outcomes[0],
            "team_b": outcomes[1],
            "tournament_stage": " · ".join(
                value for value in (metadata["tournament"], metadata["stage"]) if value
            )[:360] or "unknown",
            "best_of": metadata["best_of"],
            "match_start": str(metadata["start_time"] or "unknown"),
            "analysis_date_utc": metadata["analysis_date_utc"],
        },
        "evidence_pack": {
            key: value for key, value in (evidence or {}).items() if key != "cache_keys"
        },
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
    if (
        to_float(assessment.get("evidence_score")) < 65
        or
        probability < to_float(settings.get("win_probability_threshold"), WIN_PROBABILITY_THRESHOLD)
        or confidence < to_float(settings.get("confidence_threshold"), CONFIDENCE_THRESHOLD)
    ):
        return "insufficient"
    return verdict


class AiRiskService:
    """Hot-reloadable AI gate shared across runner ticks."""

    def __init__(
        self,
        data_dir: Path,
        follow_store: FollowStore,
        *,
        client_factory=GeminiClient,
        evidence_factory=None,
        orderbook_client: PolymarketOrderbookClient | None = None,
    ):
        self.config = AiConfigStore(Path(data_dir))
        self.follow_store = follow_store
        self.client_factory = client_factory
        self.evidence_factory = evidence_factory
        self._evidence_router: EvidenceRouter | None = None
        self.orderbook_client = orderbook_client or PolymarketOrderbookClient()
        self._lock = threading.Lock()
        self._assessment_locks: dict[str, threading.Lock] = {}

    def close(self) -> None:
        if self._evidence_router is not None:
            self._evidence_router.close()
            self._evidence_router = None
        close = getattr(self.orderbook_client, "close", None)
        if close:
            close()

    def enabled(self) -> bool:
        status = self.config.status()
        return bool(
            status["settings"]["enabled"]
            and status["credential"]["configured"]
        )

    def _build_evidence(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        if self.evidence_factory is not None:
            return self.evidence_factory(self.follow_store).build_evidence(
                market, cutoff_ts=cutoff_ts, now_ts=now_ts
            )
        if self._evidence_router is None:
            self._evidence_router = EvidenceRouter(
                self.follow_store, pandascore_key=self.config.secret(PANDASCORE_PROVIDER)
            )
        return self._evidence_router.build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts)

    def eligible_market(self, market: dict[str, Any]) -> bool:
        return (
            str(market.get("category") or "esports").lower() == "esports"
            and str(market.get("market_type") or "main_match").lower() == "main_match"
            and _game_key(market) in SUPPORTED_GAMES
        )

    def assess_backtest(self, market: dict[str, Any], *, cutoff_ts: int, now_ts: int) -> dict[str, Any]:
        """Replay one settled match from cutoff-safe provider evidence without mutating live assessments.

        The result is indicative rather than a guarantee: target results are excluded by timestamp and the
        prompt forbids model-memory completion, but callers should still label historical LLM evaluation as
        a replay rather than a perfectly leakage-free experiment.
        """
        settings = self.config.settings()
        base = {"prompt_version": PROMPT_VERSION, "model": settings["model"], "cutoff_ts": int(cutoff_ts)}
        try:
            evidence = self._build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts)
            evidence_score = int(evidence.get("evidence_score") or 0)
            if evidence_score < 65:
                return {**base, "status": "ok", "verdict": "insufficient", "direction": "insufficient", "evidence_score": evidence_score, "evidence_summary": evidence.get("coverage") or {}}
            metadata, prompt = build_match_prompt(market, now_ts=cutoff_ts, evidence=evidence)
            prompt["mode"] = "historical_replay"
            prompt["leakage_guard"] = "只能使用所给证据；目标比赛结果已从数据中截断"
            secret = self.config.secret()
            if not secret:
                raise ValueError("gemini_not_configured")
            parsed, response, latency_ms = self.client_factory(secret).assess(prompt, model=settings["model"])
            validated = validate_assessment_output(parsed, valid_evidence_ids=set(evidence.get("valid_evidence_ids") or []))
            return {
                **base, **metadata, **validated, "status": "ok", "latency_ms": latency_ms,
                "evidence_score": evidence_score,
                "direction": assessment_direction({"status": "ok", "evidence_score": evidence_score, **validated}, settings),
                "usage": _gemini_usage(response),
                "evidence_summary": {
                    "coverage": evidence.get("coverage"), "score_components": evidence.get("score_components"),
                },
            }
        except Exception as exc:
            return {**base, "status": "unavailable", "error": str(exc)[:300], "direction": "unavailable"}

    def ensure_assessment(
        self,
        market: dict[str, Any],
        *,
        now_ts: int,
        prefetched_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        condition_id = str(market.get("condition_id") or market.get("conditionId") or "").lower()
        if not condition_id:
            return {"condition_id": "", "status": "unavailable", "error": "condition_id_missing"}
        with self._lock:
            condition_lock = self._assessment_locks.setdefault(condition_id, threading.Lock())
        with condition_lock:
            return self._ensure_assessment_locked(
                market,
                now_ts=now_ts,
                condition_id=condition_id,
                prefetched_evidence=prefetched_evidence,
            )

    def _ensure_assessment_locked(
        self,
        market: dict[str, Any],
        *,
        now_ts: int,
        condition_id: str,
        prefetched_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        cached = self.follow_store.load_ai_assessment(condition_id)
        if cached and cached.get("prompt_version") == PROMPT_VERSION:
            if cached.get("status") == "ok" or now_ts - int(cached.get("updated_at") or 0) < ERROR_RETRY_SECONDS:
                return cached
        settings = self.config.settings()
        created_at = int((cached or {}).get("created_at") or now_ts)
        base = {"condition_id": condition_id, "prompt_version": PROMPT_VERSION,
                "model": settings["model"], "created_at": created_at, "updated_at": now_ts}
        try:
            start_value = (
                market.get("match_start_time") or market.get("market_start_time")
                or market.get("eventStartTime")
            )
            # A scheduled start can be hours in the future. Never let a live
            # evidence query see records newer than the signal that triggered
            # this assessment, even if an upstream API exposes them early.
            start_ts = _parse_timestamp(start_value)
            cutoff_ts = min(start_ts, int(now_ts)) if start_ts else int(now_ts)
            evidence = prefetched_evidence or self._build_evidence(
                {**market, "condition_id": condition_id}, cutoff_ts=cutoff_ts, now_ts=now_ts
            )
            evidence_score = int(evidence.get("evidence_score") or 0)
            metadata, prompt = build_match_prompt(
                {**market, "condition_id": condition_id}, now_ts=now_ts, evidence=evidence
            )
            input_hash = hashlib.sha256((PROMPT_VERSION + _json_dumps(prompt)).encode("utf-8")).hexdigest()
            base.update({
                **metadata,
                "input_hash": input_hash,
                "evidence_cache_keys": list(evidence.get("cache_keys") or []),
                "evidence_score": evidence_score,
                "evidence_summary": {
                    "coverage": evidence.get("coverage"),
                    "score_components": evidence.get("score_components"),
                },
                "evidence_snapshot": prompt.get("evidence_pack") or {},
            })
            if evidence_score < 65:
                assessment = {
                    **base, "status": "ok", "verdict": "insufficient",
                    "team_a_win_probability": 50.0, "team_b_win_probability": 50.0,
                    "confidence": 0.0, "supporting_evidence_ids": [],
                    "risk_flags": ["evidence_below_65"], "reason_zh": "赛前证据不足",
                    "latency_ms": 0, "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                }
                self.follow_store.save_ai_assessment(assessment)
                return assessment
            secret = self.config.secret()
            if not secret:
                raise ValueError("gemini_not_configured")
            parsed, response, latency_ms = self.client_factory(secret).assess(prompt, model=settings["model"])
            validated = validate_assessment_output(
                parsed, valid_evidence_ids=set(evidence.get("valid_evidence_ids") or [])
            )
            self.config.mark_credential_valid()
            assessment = {
                **base,
                **validated,
                "status": "ok",
                "latency_ms": latency_ms,
                "usage": _gemini_usage(response),
                "provider_request": prompt,
                "parsed_output": parsed,
                "response_model": str(getattr(response, "model_version", None) or settings["model"]),
            }
        except Exception as exc:  # fail open, but preserve a sanitized audit record
            assessment = {
                **base,
                "input_hash": str(base.get("input_hash") or hashlib.sha256(
                    f"{PROMPT_VERSION}|{condition_id}".encode("utf-8")
                ).hexdigest()),
                "status": "unavailable", "error": str(exc)[:300],
            }
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
        if not self.enabled() or not self.eligible_market(market):
            return None
        assessment = self.ensure_assessment(market, now_ts=now_ts)
        direction = assessment_direction(assessment, settings)
        intended_side = "team_a" if int(outcome_index) == 0 else "team_b" if int(outcome_index) == 1 else "unknown"
        action = (
            "unavailable" if direction == "unavailable"
            else "insufficient" if direction == "insufficient"
            else "agree" if direction == intended_side
            else "blocked" if to_float(assessment.get("evidence_score")) >= WALLET_MIN_EVIDENCE_SCORE
            else "insufficient"
        )
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
        if action == "blocked":
            predicted_index = 0 if direction == "team_a" else 1
            prices = [to_float(value) for value in (market.get("outcome_prices") or [])]
            ai_entry_price = prices[predicted_index] if 0 <= predicted_index < len(prices) else 0.0
            intent["ai_outcome_index"] = predicted_index
            intent["ai_outcome"] = outcomes[predicted_index] if predicted_index < len(outcomes) else None
            intent["ai_entry_price"] = round(ai_entry_price, 8) if 0 < ai_entry_price < 1 else None
        self.follow_store.save_ai_intent(intent)
        if action == "blocked":
            self.follow_store.save_ai_shadow(
                {
                    "shadow_id": "wallet:" + intent_id,
                    "intent_id": intent_id,
                    "shadow_kind": "wallet_original",
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
            ai_entry_price = to_float(intent.get("ai_entry_price"))
            if 0 < ai_entry_price < 1:
                predicted_index = int(intent.get("ai_outcome_index") or 0)
                self.follow_store.save_ai_shadow(
                    {
                        "shadow_id": "ai:" + intent_id,
                        "intent_id": intent_id,
                        "shadow_kind": "ai_prediction",
                        "condition_id": condition_id,
                        "wallet": str(wallet).lower(),
                        "outcome_index": predicted_index,
                        "outcome": intent.get("ai_outcome"),
                        "status": "open",
                        "match_start_time": intent.get("match_start_time"),
                        "end_date": intent.get("end_date"),
                        "event_title": intent.get("event_title"),
                        "market_type": intent.get("market_type"),
                        "game_family": intent.get("game_family"),
                        "entry_price": round(ai_entry_price, 8),
                        "baseline_stake": round(float(intended_stake), 8),
                        "remaining_stake": round(float(intended_stake), 8),
                        "realized_pnl": 0.0,
                        "hold_policy": "to_settlement",
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
                    "team_b_win_probability", "confidence", "evidence_score", "evidence_summary",
                    "reason_zh", "supporting_evidence_ids", "risk_flags", "model", "prompt_version", "updated_at",
                )
            },
        }

    def _proprietary_plan(self) -> dict[str, float]:
        rows = self.follow_store.load_ai_proprietary_positions(limit=10_000)
        realized = sum(to_float(row.get("realized_pnl")) for row in rows if row.get("status") == "settled")
        open_exposure = sum(to_float(row.get("stake_usdc")) for row in rows if row.get("status") == "open")
        equity = PROPRIETARY_INITIAL_BANKROLL + realized
        available = max(0.0, equity - open_exposure)
        strategy = self.follow_store.load_follow_strategy_readonly()
        sizing = strategy.get("sizing") or {}
        percent = max(0.0, to_float(sizing.get("per_signal_percent"), 1.0))
        match_percent = max(percent, to_float(sizing.get("per_match_percent"), percent))
        minimum = max(1.0, to_float(sizing.get("min_stake_usdc"), 1.0))
        raw_stake = math.floor(available * percent / 100.0)
        match_cap = math.floor(available * match_percent / 100.0)
        stake = min(float(raw_stake), float(match_cap), available)
        return {"equity": equity, "available": available, "stake": stake if stake >= minimum else 0.0, "minimum": minimum}

    def scan_proprietary(self, markets: dict[str, dict[str, Any]], *, now_ts: int) -> dict[str, int]:
        """Screen all followed-scope pre-match main markets into an isolated $5k paper book."""
        stats = {"screened": 0, "entered": 0, "watching": 0, "skipped": 0, "errors": 0}
        if not self.enabled():
            return stats
        existing = {str(row.get("condition_id") or "").lower(): row for row in self.follow_store.load_ai_proprietary_positions(limit=10_000)}
        plan = self._proprietary_plan()
        assessments_started = 0
        for condition_id, market in sorted(markets.items()):
            cid = str(condition_id or market.get("condition_id") or "").lower()
            if not cid or not self.eligible_market(market):
                continue
            start_ts = _parse_timestamp(market.get("match_start_time") or market.get("market_start_time") or market.get("eventStartTime"))
            if start_ts and start_ts <= now_ts:
                continue
            prior = existing.get(cid)
            if prior and prior.get("status") in {"open", "settled", "skipped"}:
                continue
            if prior and int(prior.get("next_retry_at") or 0) > now_ts:
                continue
            stake = to_float(plan.get("stake"))
            base = {
                "condition_id": cid, "status": "watching", "decision": "pending", "outcome_index": -1,
                "evidence_score": 0, "stake_usdc": 0.0, "realized_pnl": 0.0,
                "game_family": _game_key(market), "market_type": "main_match",
                "event_title": market.get("title") or market.get("event_title"),
                "market_question": market.get("question"), "outcomes": list(market.get("outcomes") or []),
                "match_start_time": market.get("match_start_time") or market.get("market_start_time"),
                "end_date": market.get("end_date"), "created_at": int((prior or {}).get("created_at") or now_ts),
                "updated_at": now_ts,
                "probe_count": int((prior or {}).get("probe_count") or 0),
                "liquidity_qualified": bool((prior or {}).get("liquidity_qualified")),
            }
            if not start_ts:
                self.follow_store.save_ai_proprietary_position({**base, "status": "skipped", "decision": "start_time_missing"})
                stats["skipped"] += 1
                continue
            if start_ts - now_ts > PROPRIETARY_LIQUIDITY_PROBE_HOURS[0] * 3600:
                self.follow_store.save_ai_proprietary_position({
                    **base, "decision": "awaiting_liquidity_window",
                    "next_retry_at": start_ts - PROPRIETARY_LIQUIDITY_PROBE_HOURS[0] * 3600,
                })
                stats["watching"] += 1
                continue

            def next_probe_at() -> int | None:
                slots = [
                    start_ts - hours * 3600
                    for hours in PROPRIETARY_LIQUIDITY_PROBE_HOURS[1:]
                    if start_ts - hours * 3600 > now_ts
                ]
                return min(slots) if slots else None

            def record_retry_or_skip(decision: str, **details: Any) -> None:
                retry_at = next_probe_at()
                if retry_at is None:
                    self.follow_store.save_ai_proprietary_position({
                        **base, "status": "skipped", "decision": str(decision), **details,
                    })
                    stats["skipped"] += 1
                    return
                self.follow_store.save_ai_proprietary_position({
                    **base, "decision": str(decision), **details, "next_retry_at": retry_at,
                })
                stats["watching"] += 1

            def record_liquidity_failure(reason: str, **details: Any) -> None:
                retry_at = next_probe_at()
                if retry_at is None:
                    self.follow_store.save_ai_proprietary_position({
                        **base, "status": "skipped", "decision": "cold_market",
                        "cold_reason": str(reason), **details,
                    })
                    stats["skipped"] += 1
                    return
                self.follow_store.save_ai_proprietary_position({
                    **base, "decision": str(reason), **details, "next_retry_at": retry_at,
                })
                stats["watching"] += 1

            if stake <= 0:
                self.follow_store.save_ai_proprietary_position({**base, "decision": "bankroll_insufficient", "next_retry_at": now_ts + 300})
                stats["watching"] += 1
                continue
            base["probe_count"] += 1
            base["last_probe_at"] = now_ts
            volume = to_float(market.get("volume") or market.get("volumeNum") or market.get("volume24hr"))
            required_volume = max(PROPRIETARY_MIN_VOLUME, stake * 10.0)
            if volume + 1e-9 < required_volume:
                record_liquidity_failure(
                    "volume_insufficient", volume_usdc=volume, required_volume_usdc=required_volume,
                )
                continue
            tokens = market_token_ids(market)
            if len(tokens) != 2:
                record_liquidity_failure("orderbook_token_missing")
                continue
            try:
                books = self.orderbook_client.books(tokens)
                if len(books) != 2 or any(not (book.get("bids") and book.get("asks")) for book in books):
                    record_liquidity_failure("orderbook_missing_side")
                    continue
                # The eventual AI side is not known yet.  At least one side
                # must already satisfy the exact spread/depth/VWAP gate before
                # we spend provider or model quota; the selected side is
                # checked again below by indexing this immutable snapshot.
                book_checks = [
                    evaluate_books(books, predicted_index=index, planned_stake=stake)
                    for index in (0, 1)
                ]
                if not any(check.get("eligible") for check in book_checks):
                    reasons = sorted({str(check.get("reason") or "orderbook_rejected") for check in book_checks})
                    record_liquidity_failure(
                        reasons[0] if len(reasons) == 1 else "orderbook_rejected",
                        book_candidates=book_checks,
                    )
                    continue
                base["liquidity_qualified"] = True
                base["liquidity_passed_at"] = int((prior or {}).get("liquidity_passed_at") or now_ts)
                cached_assessment = self.follow_store.load_ai_assessment(cid)
                if not cached_assessment and assessments_started >= 2:
                    record_retry_or_skip("assessment_queued")
                    continue
                prefetched_evidence = None
                if not cached_assessment:
                    assessments_started += 1
                    cutoff_ts = min(start_ts, now_ts) if start_ts else now_ts
                    prefetched_evidence = self._build_evidence(
                        {**market, "condition_id": cid}, cutoff_ts=cutoff_ts, now_ts=now_ts
                    )
                    prefetched_score = int(prefetched_evidence.get("evidence_score") or 0)
                    if prefetched_score < PROPRIETARY_MIN_EVIDENCE_SCORE:
                        coverage = prefetched_evidence.get("coverage") or {}
                        failed_sources = coverage.get("failed_sources") or {}
                        if failed_sources:
                            record_retry_or_skip(
                                "evidence_source_unavailable",
                                evidence_score=prefetched_score,
                                evidence_summary=coverage,
                            )
                            stats["errors"] += 1
                        else:
                            self.follow_store.save_ai_proprietary_position({
                                **base, "status": "skipped", "decision": "evidence_insufficient",
                                "evidence_score": prefetched_score, "evidence_summary": coverage,
                            })
                            stats["skipped"] += 1
                        continue
                assessment = self.ensure_assessment(
                    {**market, "condition_id": cid},
                    now_ts=now_ts,
                    prefetched_evidence=prefetched_evidence,
                )
                stats["screened"] += 1
                evidence_score = int(assessment.get("evidence_score") or 0)
                verdict = str(assessment.get("verdict") or "insufficient")
                probability = to_float(assessment.get(f"{verdict}_win_probability")) if verdict in {"team_a", "team_b"} else 0.0
                confidence = to_float(assessment.get("confidence"))
                terminal = {
                    **base, "status": "skipped", "decision": "evidence_insufficient",
                    "evidence_score": evidence_score, "assessment": {
                        key: assessment.get(key) for key in ("verdict", "team_a_win_probability", "team_b_win_probability", "confidence", "reason_zh", "model", "prompt_version")
                    },
                }
                if assessment.get("status") != "ok":
                    record_retry_or_skip("assessment_unavailable")
                    stats["errors"] += 1
                    continue
                if evidence_score < PROPRIETARY_MIN_EVIDENCE_SCORE or verdict not in {"team_a", "team_b"} or probability < WIN_PROBABILITY_THRESHOLD or confidence < CONFIDENCE_THRESHOLD:
                    self.follow_store.save_ai_proprietary_position(terminal)
                    stats["skipped"] += 1
                    continue
                predicted_index = 0 if verdict == "team_a" else 1
                book_check = book_checks[predicted_index]
                if not book_check.get("eligible"):
                    record_liquidity_failure(
                        str(book_check.get("reason") or "orderbook_rejected"),
                        book=book_check,
                    )
                    continue
                entry = to_float(book_check.get("vwap"))
                prefilters = (self.follow_store.load_follow_strategy_readonly().get("prefilters") or {})
                max_entry = to_float(prefilters.get("max_follow_entry_price"))
                min_entry = to_float(prefilters.get("min_follow_entry_price"))
                if (max_entry > 0 and entry > max_entry) or (min_entry > 0 and entry < min_entry):
                    self.follow_store.save_ai_proprietary_position({**terminal, "decision": "strategy_price_gate", "book": book_check})
                    stats["skipped"] += 1
                    continue
                if probability / 100.0 * 0.95 <= entry:
                    self.follow_store.save_ai_proprietary_position({**terminal, "decision": "no_positive_edge", "book": book_check})
                    stats["skipped"] += 1
                    continue
                outcomes = list(market.get("outcomes") or [])
                self.follow_store.save_ai_proprietary_position({
                    **base, "status": "open", "decision": "entered", "outcome_index": predicted_index,
                    "outcome": outcomes[predicted_index] if predicted_index < len(outcomes) else None,
                    "evidence_score": evidence_score, "stake_usdc": round(stake, 8), "entry_price": round(entry, 8),
                    "ai_probability": round(probability, 2), "confidence": round(confidence, 2),
                    "book": book_check, "volume_usdc": volume, "required_volume_usdc": required_volume,
                    "hold_policy": "to_settlement_no_stop_loss",
                })
                plan = self._proprietary_plan()
                stats["entered"] += 1
            except Exception as exc:
                record_retry_or_skip("transient_error", error=str(exc)[:180])
                stats["errors"] += 1
        return stats

    def settle_proprietary(self, resolutions: dict[str, int], *, now_ts: int, void_index: int = -2) -> int:
        changed = 0
        for row in self.follow_store.load_ai_proprietary_positions(open_only=True):
            winner = resolutions.get(str(row.get("condition_id") or "").lower())
            if winner is None:
                continue
            stake = to_float(row.get("stake_usdc"))
            entry = to_float(row.get("entry_price"))
            selected = int(row.get("outcome_index") or 0)
            if int(winner) == int(void_index):
                row.update({
                    "status": "void", "decision": "void", "winner_index": int(winner),
                    "realized_pnl": 0.0, "resolved_at": now_ts, "updated_at": now_ts,
                    "prediction_correct": None, "brier_score": None,
                    "market_implied_probability": round(entry, 8),
                })
                self.follow_store.save_ai_proprietary_position(row)
                changed += 1
                continue
            pnl = stake * (1.0 - entry) / entry if int(winner) == selected and entry > 0 else -stake
            probability = to_float(row.get("ai_probability")) / 100.0
            actual = 1.0 if int(winner) == selected else 0.0
            row.update({
                "status": "settled", "decision": "settled", "winner_index": int(winner),
                "realized_pnl": round(pnl, 8), "resolved_at": now_ts, "updated_at": now_ts,
                "prediction_correct": bool(int(winner) == selected),
                "brier_score": round((probability - actual) ** 2, 8),
                "market_implied_probability": round(entry, 8),
            })
            self.follow_store.save_ai_proprietary_position(row)
            changed += 1
        return changed

    def observe_sell(self, *, wallet: str, trade: dict[str, Any], condition_id: str, outcome_index: int, price: float, now_ts: int) -> int:
        matches = [
            row for row in self.follow_store.load_ai_shadows(open_only=True)
            if str(row.get("shadow_kind") or "wallet_original") == "wallet_original"
            and str(row.get("wallet") or "").lower() == str(wallet).lower()
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
            if str(shadow.get("shadow_kind") or "wallet_original") != "wallet_original":
                continue
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
        affected_intents: set[str] = set()
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
            if str(shadow.get("shadow_kind") or "wallet_original") == "wallet_original":
                shadow["ai_net_effect"] = round(-to_float(shadow.get("realized_pnl")), 8)
            self.follow_store.save_ai_shadow(shadow)
            affected_intents.add(str(shadow.get("intent_id") or ""))
            changed += 1
        for intent_id in affected_intents:
            rows = self.follow_store.load_ai_shadows_for_intent(intent_id)
            wallet_shadow = next((row for row in rows if str(row.get("shadow_kind") or "wallet_original") == "wallet_original"), None)
            ai_shadow = next((row for row in rows if row.get("shadow_kind") == "ai_prediction"), None)
            if wallet_shadow and ai_shadow and all(row.get("status") in {"settled", "exited"} for row in rows):
                wallet_pnl = to_float(wallet_shadow.get("realized_pnl"))
                ai_pnl = to_float(ai_shadow.get("realized_pnl"))
                ai_shadow["comparison_pnl"] = round(ai_pnl - wallet_pnl, 8)
                ai_shadow["ai_vs_no_trade_pnl"] = round(ai_pnl, 8)
                ai_shadow["ai_vs_wallet_pnl"] = round(ai_pnl - wallet_pnl, 8)
                ai_shadow["updated_at"] = now_ts
                self.follow_store.save_ai_shadow(ai_shadow)
            self.follow_store.update_ai_intent_status(intent_id, "settled", updated_at=now_ts)
        for condition_id in resolutions:
            self.finalize_condition(str(condition_id).lower(), now_ts=now_ts)
        return changed

    def finalize_condition(self, condition_id: str, *, now_ts: int) -> int:
        """Drop large request/cache material after settlement, retaining compact audit results."""
        assessment = self.follow_store.load_ai_assessment(condition_id)
        if not assessment:
            return 0
        # Team histories are shared by every pending match involving that team;
        # never delete them merely because one condition settled.  The bounded
        # 6h TTL + idle/absolute pruning owns cache reclamation.
        removed = 0
        for key in ("provider_request", "parsed_output"):
            assessment.pop(key, None)
        assessment["finalized_at"] = int(now_ts)
        assessment["updated_at"] = int(now_ts)
        self.follow_store.save_ai_assessment(assessment)
        self.follow_store.prune_ai_data_cache(now_ts=now_ts)
        with self._lock:
            self._assessment_locks.pop(str(condition_id).lower(), None)
        return removed


def ai_audit_summary(store: FollowStore) -> dict[str, Any]:
    return store.load_ai_summary()
