import base64
import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import httpx

from poly_fight.ai_risk import (
    AiConfigStore,
    AiRiskService,
    DeepSeekClient,
    SYSTEM_PROMPT,
    assessment_direction,
    build_match_prompt,
    validate_assessment_output,
)
from poly_fight.follow import process_follow_trades
from poly_fight.dashboard import (
    DashboardConfig,
    build_overview,
    create_server,
    make_session_token,
    reset_dashboard_data,
)
from poly_fight.storage import FollowStore
from poly_fight.pandascore import (
    MAX_PROMPT_MATCHES,
    PANDASCORE_PROVIDER,
    PandaScoreEvidenceService,
)


def encrypted_envelope(config: AiConfigStore, secret: str) -> dict:
    wrap = config.public_wrap_key()
    public = serialization.load_der_public_key(base64.b64decode(wrap["spki"]))
    dek = AESGCM.generate_key(bit_length=256)
    nonce = b"0123456789ab"
    return {
        "envelopeVersion": 1,
        "keyId": wrap["keyId"],
        "wrappedKey": base64.b64encode(public.encrypt(
            dek,
            padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
        )).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ciphertext": base64.b64encode(AESGCM(dek).encrypt(nonce, secret.encode(), None)).decode(),
    }


def market(condition_id="c1"):
    return {
        "condition_id": condition_id,
        "category": "esports",
        "market_type": "main_match",
        "game_family": "lol",
        "title": "League of Legends: T1 vs Kiwoom DRX (BO1) - KeSPA Cup",
        "question": "T1 vs Kiwoom DRX",
        "outcomes": ["T1", "Kiwoom DRX"],
        "outcome_prices": [0.62, 0.38],
        "match_start_time": "2030-01-01T12:00:00Z",
    }


class FakeDeepSeek:
    calls = []

    def __init__(self, secret):
        self.secret = secret

    def assess(self, prompt, *, model):
        self.__class__.calls.append((prompt, model))
        return ({
            "status": "decisive", "winner": "team_a", "team_a_score": 82,
            "team_b_score": 18, "confidence": 91,
            "supporting_evidence_ids": ["ev_test"], "risk_flags": ["bo1_variance"],
            "reason_zh": "历史实力与交手明显占优",
        }, type("Response", (), {"model_version": model, "usage_metadata": None})(), 12)


class FakeEvidence:
    def __init__(self, store):
        self.store = store

    def build_evidence(self, market, *, cutoff_ts, now_ts):
        return {
            "as_of": "2026-07-22T00:00:00Z",
            "evidence_score": 88,
            "score_components": {"identity": 15, "sample_freshness": 25},
            "coverage": {"successful_sources": ["pandascore", "leaguepedia"]},
            "normalized": {"team_a": [{"id": "ev_test", "d": "2026-07-01", "opp": "DRX", "r": "W"}], "team_b": [], "h2h": []},
            "valid_evidence_ids": ["ev_test"],
            "cache_keys": ["pandascore:team:lol:1", "pandascore:team:lol:2"],
        }


class MediumEvidence(FakeEvidence):
    def build_evidence(self, market, *, cutoff_ts, now_ts):
        return {**super().build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts), "evidence_score": 67}


class RecordingEvidence(FakeEvidence):
    cutoffs = []

    def build_evidence(self, market, *, cutoff_ts, now_ts):
        self.__class__.cutoffs.append(cutoff_ts)
        return super().build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts)


class FakePandaScore:
    def __init__(self, *, cutoff_ts):
        self.cutoff_ts = cutoff_ts
        self.calls = []

    def search_teams(self, game, name):
        team_id = 1 if name == "T1" else 2
        return [{"id": team_id, "name": name, "acronym": name, "players": [
            {"name": f"{name}-player-{index}", "url": "must-not-reach-prompt"}
            for index in range(5)
        ]}]

    def past_matches(self, game, team_id, *, cutoff_ts):
        self.calls.append((game, team_id, cutoff_ts))
        count = 12 if team_id == 1 else 6
        rows = []
        for index in range(count):
            played_at = cutoff_ts - (index + 1) * (10 if team_id == 1 else 25) * 86400
            opponent_id = (2 if team_id == 1 else 1) if index == 0 else 100 + index
            opponents = [
                {"opponent": {"id": team_id, "name": "T1" if team_id == 1 else "Kiwoom DRX"}},
                {"opponent": {
                    "id": opponent_id,
                    "name": "Kiwoom DRX" if opponent_id == 2 else "T1" if opponent_id == 1 else f"Opponent {index}",
                }},
            ]
            winner_id = team_id if index % 3 else opponent_id
            rows.append({
                "id": team_id * 1000 + index,
                "begin_at": datetime.fromtimestamp(played_at, tz=timezone.utc).isoformat(),
                "status": "finished",
                "opponents": opponents,
                "winner_id": winner_id,
                "results": [
                    {"team_id": team_id, "score": 2 if winner_id == team_id else 1},
                    {"team_id": opponent_id, "score": 1 if winner_id == team_id else 2},
                ],
                "number_of_games": 3,
                "tournament": {"name": "Compact Cup", "url": "must-not-reach-prompt"},
                "streams_list": [{"raw_url": "must-not-reach-prompt"}],
            })
        # A target/future result must be rejected even if the provider returns it.
        rows.append({
            "id": 99999, "begin_at": datetime.fromtimestamp(cutoff_ts + 60, tz=timezone.utc).isoformat(),
            "status": "finished", "opponents": [
                {"opponent": {"id": team_id, "name": "future"}},
                {"opponent": {"id": 999, "name": "future opponent"}},
            ], "winner_id": team_id, "results": [], "number_of_games": 1,
        })
        return rows


class AiRiskTests(unittest.TestCase):
    def test_deepseek_uses_compact_json_without_thinking_or_sampling(self):
        captured = {}

        def handle(request):
            captured.update(json.loads(request.content.decode()))
            return httpx.Response(200, json={
                "model": "deepseek-v4-pro",
                "choices": [{"message": {"content": '{"s":"i","w":null,"a":50,"b":50,"c":0,"e":[],"f":[],"r":"证据不足"}'}}],
                "usage": {"prompt_tokens": 80, "completion_tokens": 20, "total_tokens": 100},
            })

        client = DeepSeekClient("sk-test", transport=httpx.MockTransport(handle))
        parsed, response, _ = client.assess({"M": {}, "E": {}}, model="deepseek-v4-pro")
        self.assertEqual(parsed["s"], "i")
        self.assertEqual(response["model"], "deepseek-v4-pro")
        self.assertEqual(captured["thinking"], {"type": "disabled"})
        self.assertEqual(captured["response_format"], {"type": "json_object"})
        self.assertEqual(captured["max_tokens"], 256)
        self.assertNotIn("temperature", captured)
        self.assertNotIn("top_p", captured)
        self.assertNotIn("top_k", captured)
        self.assertIn("json", captured["messages"][0]["content"].lower())
        self.assertIn("0-100", captured["messages"][0]["content"])
        self.assertIn("0.7", captured["messages"][0]["content"])
        canonical = validate_assessment_output(parsed, valid_evidence_ids=set())
        self.assertEqual(canonical["verdict"], "insufficient")
        self.assertEqual(canonical["team_a_win_probability"], 50)
        self.assertEqual(canonical["team_b_win_probability"], 50)

    def test_live_assessment_cutoff_never_advances_to_future_match_start(self):
        RecordingEvidence.cutoffs = []
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            service = AiRiskService(
                data_dir, store, client_factory=FakeDeepSeek, evidence_factory=RecordingEvidence,
            )
            service.config.save_credential(encrypted_envelope(service.config, "deepseek-test"))
            service.config.save_settings(enabled=True)
            now_ts = 1_783_000_000
            service.ensure_assessment(market("future-cutoff"), now_ts=now_ts)
            self.assertEqual(RecordingEvidence.cutoffs, [now_ts])
            service.close()

    def test_pandascore_history_is_time_bounded_compacted_and_cached(self):
        cutoff_ts = int(datetime(2026, 7, 22, tzinfo=timezone.utc).timestamp())
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            client = FakePandaScore(cutoff_ts=cutoff_ts)
            service = PandaScoreEvidenceService(store, client)
            evidence = service.build_evidence(market(), cutoff_ts=cutoff_ts, now_ts=cutoff_ts - 60)
            self.assertEqual(evidence["team_a"]["record"]["n"], 12)
            self.assertEqual(evidence["team_b"]["record"]["n"], 6)
            self.assertEqual(evidence["team_a"]["window_days"], 120)
            self.assertEqual(evidence["team_b"]["window_days"], 180)
            self.assertEqual(len(evidence["team_a"]["recent"]), MAX_PROMPT_MATCHES)
            self.assertEqual(len(evidence["team_b"]["recent"]), 6)
            self.assertEqual(evidence["team_a"]["days_since_last"], 10)
            self.assertNotIn("must-not-reach-prompt", json.dumps(evidence))
            self.assertNotIn("streams_list", json.dumps(evidence))
            self.assertNotIn("match_id", json.dumps(evidence))
            self.assertNotIn("opponent_id", json.dumps(evidence))
            self.assertLess(len(json.dumps(evidence, ensure_ascii=False)), 10_000)
            # Alias and history rows are reused for another signal in the same cache window.
            again = service.build_evidence(market(), cutoff_ts=cutoff_ts, now_ts=cutoff_ts)
            self.assertEqual(again["team_a"]["record"], evidence["team_a"]["record"])
            self.assertEqual(len(client.calls), 2)

    def test_minimal_prompt_excludes_follow_direction_and_wallet_data(self):
        metadata, prompt = build_match_prompt(
            market(), now_ts=1_783_000_000,
            evidence={"source": "PandaScore", "team_a": {"record": {"n": 4}}},
        )
        encoded = str(prompt)
        self.assertIn("T1", encoded)
        self.assertIn("Kiwoom DRX", encoded)
        self.assertIn("KeSPA Cup", encoded)
        self.assertIn("Match Winner", SYSTEM_PROMPT)
        self.assertIn("A/B从50开始", SYSTEM_PROMPT)
        self.assertIn("只输出JSON", SYSTEM_PROMPT)
        self.assertNotIn("wallet", encoded.lower())
        self.assertNotIn("outcome_index", encoded)
        self.assertNotIn("condition_id", encoded)
        self.assertNotIn("0.62", encoded)
        self.assertIn("E", prompt)
        self.assertNotIn("valid_evidence_ids", encoded)
        self.assertEqual(metadata["team_a"], "T1")

    def test_dota2_is_supported_by_ai_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow.db")
            service = AiRiskService(Path(tmp), store, client_factory=FakeDeepSeek)
            dota_market = {**market(), "game_family": "dota2"}
            self.assertTrue(service.eligible_market(dota_market))
            service.close()

    def test_minimal_output_validation_and_threshold(self):
        parsed = validate_assessment_output({
            "s": "d", "w": "b", "a": 31, "b": 69, "c": 80,
            "e": ["ev1"], "f": ["roster_change"], "r": "B队历史表现更稳",
        }, valid_evidence_ids={"ev1"})
        self.assertEqual(parsed["verdict"], "team_b")
        self.assertEqual(assessment_direction({"status": "ok", "evidence_score": 80, **parsed}, {
            "win_probability_threshold": 65, "confidence_threshold": 75,
        }), "team_b")
        boundary = {**parsed, "status": "ok", "evidence_score": 80, "team_a_win_probability": 35, "team_b_win_probability": 65, "confidence": 75}
        self.assertEqual(assessment_direction(boundary, {
            "win_probability_threshold": 65, "confidence_threshold": 75,
        }), "team_b")
        weak = {**parsed, "status": "ok", "confidence": 74}
        self.assertEqual(assessment_direction(weak, {
            "win_probability_threshold": 65, "confidence_threshold": 75,
        }), "insufficient")

    def test_fractional_confidence_and_scalar_lists_are_normalized(self):
        parsed = validate_assessment_output({
            "s": "d", "w": "a", "a": 70, "b": 30, "c": .8,
            "e": "ev1", "f": "roster_change", "r": "A队近期表现更稳",
        }, valid_evidence_ids={"ev1"})
        self.assertEqual(parsed["confidence"], 80)
        self.assertEqual(parsed["supporting_evidence_ids"], ["ev1"])
        self.assertEqual(parsed["risk_flags"], ["roster_change"])

    def test_envelope_round_trip_and_reset_independent_location(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = AiConfigStore(Path(tmp))
            envelope = encrypted_envelope(config, "sk-private-test")
            config.save_credential(envelope)
            self.assertEqual(config.secret(), "sk-private-test")
            self.assertTrue(config.db_path.is_relative_to(Path(tmp) / ".secrets"))
            self.assertNotIn("sk-private-test", config.db_path.read_bytes().decode("latin1"))

    def test_model_migrates_to_fixed_deepseek_without_deleting_gemini_ciphertext(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = AiConfigStore(data_dir)
            with sqlite3.connect(config.db_path) as conn:
                conn.execute("UPDATE ai_risk_settings SET enabled=1,model='gemini-3.6-flash' WHERE id=1")
                conn.execute(
                    "INSERT INTO provider_credential "
                    "(provider,envelope_version,key_id,wrapped_key,nonce,ciphertext,status,created_at,updated_at) "
                    "VALUES ('gemini',1,'old','old','old','old','valid',1,1)"
                )
            migrated = AiConfigStore(data_dir)
            self.assertEqual(migrated.settings()["model"], "deepseek-v4-pro")
            self.assertTrue(migrated.settings()["enabled"])
            self.assertIsNone(migrated.credential_envelope())
            self.assertIsNotNone(migrated.credential_envelope("gemini"))

    def test_explicit_data_reset_preserves_encrypted_credential(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            config = AiConfigStore(data_dir)
            config.save_credential(encrypted_envelope(config, "sk-survives-reset"))
            dashboard = DashboardConfig(
                data_dir=data_dir, follow_dir=data_dir / "follow", log_dir=data_dir / "logs",
                username="admin", password="pw", cookie_secret="secret",
                runner_process_lister=lambda: [],
            )
            reset_dashboard_data(dashboard)
            self.assertEqual(AiConfigStore(data_dir).secret(), "sk-survives-reset")

    def test_strong_conflict_blocks_and_shadow_settles(self):
        FakeDeepSeek.calls = []
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.init_db()
            service = AiRiskService(
                data_dir, store, client_factory=FakeDeepSeek, evidence_factory=FakeEvidence,
            )
            service.config.save_credential(encrypted_envelope(service.config, "sk-test"))
            service.config.save_credential(
                encrypted_envelope(service.config, "pandascore-test"), PANDASCORE_PROVIDER,
            )
            service.config.save_settings(enabled=True)
            for team_id in (1, 2):
                store.save_ai_data_cache({
                    "cache_key": f"pandascore:team:lol:{team_id}", "cache_kind": "team_history",
                    "game": "lol", "team_id": str(team_id), "fetched_at": 1_783_000_000,
                    "last_used_at": 1_783_000_000, "expires_at": 1_783_100_000,
                })
            decision = service.decide(
                market=market(), wallet="0x" + "1" * 40, outcome_index=1,
                intended_stake=100, entry_price=0.4, trade_id="tx1",
                wallet_trade_size=250, now_ts=1_783_000_000,
            )
            self.assertTrue(decision["blocked"])
            self.assertEqual(decision["action"], "blocked")
            self.assertEqual(len(FakeDeepSeek.calls), 1)
            # Same condition reuses the neutral assessment.
            agree = service.decide(
                market=market(), wallet="0x" + "2" * 40, outcome_index=0,
                intended_stake=50, entry_price=0.6, trade_id="tx2",
                wallet_trade_size=80, now_ts=1_783_000_001,
            )
            self.assertEqual(agree["action"], "agree")
            self.assertEqual(len(FakeDeepSeek.calls), 1)
            service.settle_shadows({"c1": 0}, now_ts=1_783_000_100)
            shadows = store.load_ai_shadows()
            wallet_shadow = next(row for row in shadows if row["shadow_kind"] == "wallet_original")
            ai_shadow = next(row for row in shadows if row["shadow_kind"] == "ai_prediction")
            self.assertEqual(wallet_shadow["realized_pnl"], -100)
            self.assertEqual(wallet_shadow["ai_net_effect"], 100)
            self.assertAlmostEqual(ai_shadow["realized_pnl"], 100 * (1 - 0.62) / 0.62)
            self.assertAlmostEqual(
                ai_shadow["comparison_pnl"], ai_shadow["realized_pnl"] - wallet_shadow["realized_pnl"],
            )
            self.assertIsNotNone(store.load_ai_data_cache(
                "pandascore:team:lol:1", now_ts=1_783_000_101, touch=False,
            ))
            finalized = store.load_ai_assessment("c1")
            self.assertNotIn("provider_request", finalized)
            self.assertNotIn("parsed_output", finalized)
            self.assertEqual(finalized["evidence_snapshot"]["a"][0]["id"], "ev_test")
            service.close()

    def test_medium_evidence_is_assessed_but_cannot_block_wallet(self):
        FakeDeepSeek.calls = []
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            service = AiRiskService(
                data_dir, store, client_factory=FakeDeepSeek, evidence_factory=MediumEvidence,
            )
            service.config.save_credential(encrypted_envelope(service.config, "deepseek-test"))
            service.config.save_settings(enabled=True)
            decision = service.decide(
                market=market("medium"), wallet="0x" + "4" * 40, outcome_index=1,
                intended_stake=25, entry_price=0.4, trade_id="tx-medium",
                wallet_trade_size=50, now_ts=1_783_000_000,
            )
            self.assertEqual(decision["action"], "insufficient")
            self.assertFalse(decision["blocked"])
            self.assertEqual(len(FakeDeepSeek.calls), 1)
            service.close()

    def test_ai_scope_is_esports_main_match_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow" / "follow.db")
            service = AiRiskService(Path(tmp), store, client_factory=FakeDeepSeek)
            self.assertFalse(hasattr(service, "prefetch"))
            self.assertTrue(service.eligible_market(market()))
            self.assertFalse(service.eligible_market({**market(), "market_type": "game_winner"}))
            self.assertFalse(service.eligible_market({**market(), "category": "sports"}))
            self.assertFalse(service.eligible_market({**market(), "game_family": "valorant"}))
            service.close()

    def test_overview_exposes_ai_status_counts_and_net_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            store = FollowStore(data_dir / "follow" / "follow.db")
            store.init_db()
            store.save_ai_intent({
                "intent_id": "i1", "condition_id": "c1", "wallet": "0x" + "a" * 40,
                "outcome_index": 1, "action": "blocked", "status": "settled",
                "created_at": 10, "updated_at": 20,
            })
            store.save_ai_shadow({
                "shadow_id": "s1", "intent_id": "i1", "condition_id": "c1",
                "wallet": "0x" + "a" * 40, "outcome_index": 1, "status": "settled",
                "realized_pnl": -100, "ai_net_effect": 100, "created_at": 10, "updated_at": 20,
            })
            AiConfigStore(data_dir).save_settings(enabled=True)
            ai = build_overview(data_dir)["ai_risk"]
            self.assertFalse(ai["enabled"])
            self.assertTrue(ai["requested_enabled"])
            self.assertEqual(ai["blocked_count"], 1)
            self.assertEqual(ai["net_effect_usdc"], 100)

    def test_overview_reads_existing_ai_config_without_recreating_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = AiConfigStore(data_dir)
            config.save_settings(enabled=True)
            config.private_key_path.unlink()
            config.public_key_path.unlink()
            ai = build_overview(data_dir)["ai_risk"]
            self.assertFalse(ai["enabled"])
            self.assertTrue(ai["requested_enabled"])
            self.assertFalse(config.private_key_path.exists())
            self.assertFalse(config.public_key_path.exists())

    def test_existing_public_wrap_key_read_skips_store_initialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config = AiConfigStore(data_dir)
            expected = config.public_wrap_key()
            with patch.object(AiConfigStore, "init_db", side_effect=AssertionError("must not initialize")):
                actual = AiConfigStore.read_existing_public_wrap_key(data_dir)
            self.assertEqual(actual, expected)

    def test_ai_dashboard_routes_require_auth_and_accept_encrypted_envelope(self):
        class ModelClient:
            def __init__(self, secret):
                self.secret = secret

            def test(self, *, model):
                return {"ok": True, "model": model, "latency_ms": 1}

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            config_store = AiConfigStore(data_dir)
            envelope = encrypted_envelope(config_store, "sk-route-test")
            server = create_server(DashboardConfig(
                data_dir=data_dir, host="127.0.0.1", port=0,
                username="admin", password="pw", cookie_secret="secret",
            ))
            thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
            thread.start()

            def call(method, path, body=None, cookie=""):
                conn = http.client.HTTPConnection(*server.server_address[:2], timeout=5)
                headers = {"Content-Type": "application/json"}
                if cookie:
                    headers["Cookie"] = cookie
                conn.request(method, path, body=json.dumps(body or {}) if body is not None else None, headers=headers)
                response = conn.getresponse()
                payload = json.loads(response.read().decode())
                status = response.status
                conn.close()
                return status, payload

            try:
                status, _ = call("GET", "/api/ai-risk")
                self.assertEqual(status, 401)
                token = make_session_token("admin", "secret")
                cookie = "poly_fight_session=" + token
                with patch("poly_fight.dashboard.DeepSeekClient", ModelClient):
                    status, payload = call("POST", "/api/ai-risk/credential", {"envelope": envelope}, cookie)
                self.assertEqual(status, 200)
                self.assertTrue(payload["data"]["configured"])
                data_envelope = encrypted_envelope(config_store, "pandascore-route-test")
                with patch("poly_fight.dashboard.PandaScoreClient.test", return_value=True):
                    status, payload = call(
                        "POST", "/api/ai-risk/data-credential", {"envelope": data_envelope}, cookie,
                    )
                self.assertEqual(status, 200)
                self.assertTrue(payload["data"]["configured"])
                status, payload = call("POST", "/api/ai-risk/settings", {"enabled": True}, cookie)
                self.assertEqual(status, 200)
                self.assertTrue(payload["data"]["enabled"])
                status, payload = call("GET", "/api/ai-risk", cookie=cookie)
                self.assertEqual(status, 200)
                self.assertTrue(payload["data"]["settings"]["enabled"])
                self.assertTrue(payload["data"]["data_credential"]["configured"])
                self.assertNotIn("sk-route-test", json.dumps(payload))
                self.assertNotIn("pandascore-route-test", json.dumps(payload))
            finally:
                server.shutdown()
                server.server_close()

    def test_process_follow_gate_runs_after_normal_candidate_checks(self):
        class Gate:
            def decide(self, **kwargs):
                self.kwargs = kwargs
                return {"intent_id": "i1", "action": "blocked", "blocked": True, "assessment": {}}

            def observe_sell(self, **kwargs):
                return 0

        gate = Gate()
        now = int(datetime(2026, 7, 22, tzinfo=timezone.utc).timestamp())
        signals, stats = process_follow_trades(
            [], wallet="0x" + "3" * 40,
            trades=[{"conditionId": "c1", "outcomeIndex": 1, "side": "BUY", "price": 0.6, "size": 100, "timestamp": now, "id": "tx"}],
            markets_by_condition={"c1": market()}, now_ts=now,
            stake_usdc=1, max_follow_legs=5, max_slippage=1,
            min_wallet_entry_price=0, max_entry_price=0.85,
            bankroll_usdc=1000, ai_risk_handler=gate,
        )
        self.assertEqual(signals, [])
        self.assertEqual(stats["ai_blocked_count"], 1)
        self.assertGreater(gate.kwargs["intended_stake"], 0)


if __name__ == "__main__":
    unittest.main()
