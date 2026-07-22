import base64
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from poly_fight.ai_risk import AiRiskService
from poly_fight.orderbook import evaluate_books, vwap_for_cash
from poly_fight.storage import FollowStore


def envelope(config, secret):
    wrap = config.public_wrap_key()
    public = serialization.load_der_public_key(base64.b64decode(wrap["spki"]))
    key = AESGCM.generate_key(bit_length=256)
    nonce = b"0123456789ab"
    return {
        "envelopeVersion": 1, "keyId": wrap["keyId"],
        "wrappedKey": base64.b64encode(public.encrypt(key, padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None))).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ciphertext": base64.b64encode(AESGCM(key).encrypt(nonce, secret.encode(), None)).decode(),
    }


class Evidence:
    def __init__(self, store):
        self.store = store

    def build_evidence(self, market, *, cutoff_ts, now_ts):
        return {
            "evidence_score": 90, "score_components": {"identity": 15},
            "coverage": {"successful_sources": ["pandascore", "liquipedia"]},
            "normalized": {"team_a": [{"id": "ev1", "d": "2026-07-20", "opp": "Gamma", "r": "W"}], "team_b": [], "h2h": []},
            "valid_evidence_ids": ["ev1"], "cache_keys": [],
        }


class ThinEvidence(Evidence):
    def build_evidence(self, market, *, cutoff_ts, now_ts):
        return {**super().build_evidence(market, cutoff_ts=cutoff_ts, now_ts=now_ts), "evidence_score": 70}


class Model:
    calls = 0

    def __init__(self, key):
        self.key = key

    def assess(self, prompt, *, model):
        Model.calls += 1
        payload = {
            "status": "decisive", "winner": "team_a", "team_a_score": 75, "team_b_score": 25,
            "confidence": 82, "supporting_evidence_ids": ["ev1"], "risk_flags": [], "reason_zh": "近期强度更高",
        }
        return payload, type("Response", (), {"model_version": model, "usage_metadata": None})(), 3


class UnavailableModel(Model):
    def assess(self, prompt, *, model):
        Model.calls += 1
        raise RuntimeError("temporary_model_failure")


class Books:
    def books(self, token_ids):
        return [
            {"asset_id": token_ids[0], "bids": [{"price": ".59", "size": "200"}], "asks": [{"price": ".60", "size": "300"}]},
            {"asset_id": token_ids[1], "bids": [{"price": ".38", "size": "200"}], "asks": [{"price": ".40", "size": "300"}]},
        ]

    def close(self):
        pass


class ShallowBooks(Books):
    def books(self, token_ids):
        return [
            {"asset_id": token_ids[0], "bids": [{"price": ".59", "size": "1"}], "asks": [{"price": ".60", "size": "1"}]},
            {"asset_id": token_ids[1], "bids": [{"price": ".39", "size": "1"}], "asks": [{"price": ".40", "size": "1"}]},
        ]


TEST_NOW = 1_784_700_000


def market(*, start_ts: int = TEST_NOW + 9_000, volume: float = 2500):
    return {
        "condition_id": "c-self", "category": "esports", "market_type": "main_match", "game_family": "cs2",
        "title": "Alpha vs Beta (BO3)", "question": "Alpha vs Beta", "outcomes": ["Alpha", "Beta"],
        "match_start_time": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
        "end_date": datetime.fromtimestamp(start_ts + 6 * 3600, tz=timezone.utc).isoformat(),
        "volume": volume, "clob_token_ids": ["token-a", "token-b"],
    }


class ProprietaryAiTests(unittest.TestCase):
    def test_cold_market_is_probed_only_at_three_two_and_one_hours(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 4 * 3600
            thin_market = market(start_ts=start, volume=0)

            service.scan_proprietary({"c-self": thin_market}, now_ts=TEST_NOW)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["decision"], "awaiting_liquidity_window")
            self.assertEqual(row["next_retry_at"], start - 3 * 3600)
            self.assertEqual(row["probe_count"], 0)

            for hours, expected_next, expected_status in (
                (3, start - 2 * 3600, "watching"),
                (2, start - 1 * 3600, "watching"),
                (1, None, "skipped"),
            ):
                service.scan_proprietary({"c-self": thin_market}, now_ts=start - hours * 3600)
                row = store.load_ai_proprietary_positions()[0]
                self.assertEqual(row["status"], expected_status)
                if expected_next is not None:
                    self.assertEqual(row["next_retry_at"], expected_next)
            self.assertEqual(row["decision"], "cold_market")
            self.assertEqual(row["cold_reason"], "volume_insufficient")
            self.assertEqual(row["probe_count"], 3)
            service.close()

    def test_model_retry_waits_for_next_scheduled_liquidity_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(
                root, store, client_factory=UnavailableModel,
                evidence_factory=Evidence, orderbook_client=Books(),
            )
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 4 * 3600
            candidate = market(start_ts=start)
            Model.calls = 0

            service.scan_proprietary({"c-self": candidate}, now_ts=start - 3 * 3600)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["decision"], "assessment_unavailable")
            self.assertEqual(row["next_retry_at"], start - 2 * 3600)
            self.assertEqual(row["probe_count"], 1)
            self.assertEqual(Model.calls, 1)

            service.scan_proprietary({"c-self": candidate}, now_ts=start - 3 * 3600 + 300)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["probe_count"], 1)
            self.assertEqual(Model.calls, 1)

            service.scan_proprietary({"c-self": candidate}, now_ts=start - 2 * 3600)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["probe_count"], 2)
            self.assertEqual(Model.calls, 2)
            service.close()

    def test_self_shadow_does_not_call_model_before_liquidity_and_evidence_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=ShallowBooks())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            Model.calls = 0
            service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW)
            self.assertEqual(Model.calls, 0)
            self.assertIn(store.load_ai_proprietary_positions()[0]["decision"], {"depth_insufficient", "orderbook_rejected"})
            service.close()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=ThinEvidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            Model.calls = 0
            service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW)
            self.assertEqual(Model.calls, 0)
            self.assertEqual(store.load_ai_proprietary_positions()[0]["decision"], "evidence_insufficient")
            service.close()

    def test_team_cache_prunes_after_one_day_idle_or_absolute_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow" / "follow.db")
            now = 1_800_000_000
            for key, last_used, expires in (
                ("idle", now - 24 * 3600 - 1, now + 3600),
                ("expired", now - 10, now - 1),
                ("recent", now - 10, now + 3600),
            ):
                store.save_ai_data_cache({
                    "cache_key": key, "cache_kind": "team_history", "game": "cs2",
                    "team_id": key, "fetched_at": now - 100, "last_used_at": last_used,
                    "expires_at": expires, "value": key,
                })
            self.assertEqual(store.prune_ai_data_cache(now_ts=now), 2)
            self.assertIsNotNone(store.load_ai_data_cache("recent", now_ts=now, touch=False))

    def test_vwap_spread_and_three_times_depth(self):
        asks = [(0.60, 50), (0.61, 100)]
        vwap, filled = vwap_for_cash(asks, 50)
        self.assertAlmostEqual(filled, 50)
        self.assertGreater(vwap, .60)
        rejected = evaluate_books(
            [{"bids": [{"price": .59, "size": 10}], "asks": [{"price": .60, "size": 10}]}, {"bids": [{"price": .39, "size": 10}], "asks": [{"price": .40, "size": 10}]}],
            predicted_index=0, planned_stake=50,
        )
        self.assertEqual(rejected["reason"], "depth_insufficient")

    def test_self_shadow_uses_independent_bankroll_enters_once_and_settles(self):
        Model.calls = 0
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            stats = service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW)
            self.assertEqual(stats["entered"], 1)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["status"], "open")
            self.assertEqual(row["stake_usdc"], 50)
            self.assertEqual(row["hold_policy"], "to_settlement_no_stop_loss")
            service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW + 10)
            self.assertEqual(Model.calls, 1)
            service.settle_proprietary({"c-self": 0}, now_ts=1_784_800_000)
            settled = store.load_ai_proprietary_positions()[0]
            self.assertEqual(settled["status"], "settled")
            self.assertTrue(settled["prediction_correct"])
            self.assertAlmostEqual(settled["realized_pnl"], 50 * .4 / .6)
            summary = store.load_ai_summary()
            self.assertEqual(summary["proprietary_win_count"], 1)
            self.assertGreater(summary["proprietary_pnl_usdc"], 0)
            service.close()

    def test_void_self_shadow_does_not_pollute_win_rate_or_brier(self):
        Model.calls = 0
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW)
            service.settle_proprietary({"c-self": -2}, now_ts=1_784_800_000)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["status"], "void")
            self.assertEqual(row["realized_pnl"], 0)
            summary = store.load_ai_summary()
            self.assertEqual(summary["proprietary_settled_count"], 0)
            self.assertIsNone(summary["proprietary_brier_score"])
            service.close()


if __name__ == "__main__":
    unittest.main()
