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


class LowProbabilityModel(Model):
    def assess(self, prompt, *, model):
        return ({
            "status": "decisive", "winner": "team_a", "team_a_score": 63, "team_b_score": 37,
            "confidence": 82, "supporting_evidence_ids": ["ev1"], "risk_flags": [],
            "reason_zh": "A队略占优势",
        }, type("Response", (), {"model_version": model, "usage_metadata": None})(), 3)


class LowConfidenceModel(Model):
    def assess(self, prompt, *, model):
        return ({
            "status": "decisive", "winner": "team_a", "team_a_score": 75, "team_b_score": 25,
            "confidence": 74, "supporting_evidence_ids": ["ev1"], "risk_flags": [],
            "reason_zh": "A队占优但变数较大",
        }, type("Response", (), {"model_version": model, "usage_metadata": None})(), 3)


class InsufficientModel(Model):
    def assess(self, prompt, *, model):
        return ({
            "status": "insufficient", "winner": None, "team_a_score": 50, "team_b_score": 50,
            "confidence": 0, "supporting_evidence_ids": [], "risk_flags": [],
            "reason_zh": "数据冲突",
        }, type("Response", (), {"model_version": model, "usage_metadata": None})(), 3)


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


class ExpensiveBooks(Books):
    def books(self, token_ids):
        return [
            {"asset_id": token_ids[0], "bids": [{"price": ".74", "size": "300"}], "asks": [{"price": ".75", "size": "300"}]},
            {"asset_id": token_ids[1], "bids": [{"price": ".24", "size": "300"}], "asks": [{"price": ".25", "size": "300"}]},
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
    def test_wallet_triggered_high_quality_assessment_gets_one_early_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            candidate = market(start_ts=TEST_NOW + 30 * 3600)
            service.ensure_assessment(candidate, now_ts=TEST_NOW)
            store.save_ai_intent({
                "intent_id": "wallet-trigger", "condition_id": "c-self", "action": "agree",
                "status": "open", "created_at": TEST_NOW, "updated_at": TEST_NOW,
            })

            stats = service.scan_proprietary({"c-self": candidate}, now_ts=TEST_NOW + 60)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(stats["entered"], 1)
            self.assertEqual(row["status"], "open")
            self.assertEqual(row["probe_trigger"], "wallet_assessment")
            self.assertEqual(row["probe_count"], 1)
            self.assertEqual(row["scheduled_probe_count"], 0)
            service.close()

    def test_wallet_probe_failure_waits_for_twenty_four_hour_slot_instead_of_polling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 30 * 3600
            candidate = market(start_ts=start, volume=0)
            service.ensure_assessment(candidate, now_ts=TEST_NOW)
            store.save_ai_intent({
                "intent_id": "wallet-trigger", "condition_id": "c-self", "action": "agree",
                "status": "open", "created_at": TEST_NOW, "updated_at": TEST_NOW,
            })

            service.scan_proprietary({"c-self": candidate}, now_ts=TEST_NOW + 60)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["decision"], "volume_insufficient")
            self.assertEqual(row["next_retry_at"], start - 24 * 3600)
            self.assertEqual(row["probe_count"], 1)
            service.scan_proprietary({"c-self": candidate}, now_ts=TEST_NOW + 120)
            self.assertEqual(store.load_ai_proprietary_positions()[0]["probe_count"], 1)
            service.close()

    def test_price_gate_retries_at_remaining_scheduled_slots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=ExpensiveBooks())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            strategy = store.load_follow_strategy_readonly()
            strategy["prefilters"]["max_follow_entry_price"] = .68
            store.save_follow_strategy(strategy, ts=TEST_NOW)
            start = TEST_NOW + 4 * 3600
            candidate = market(start_ts=start)

            for hours, expected_status, expected_retry in (
                (3, "watching", start - 2 * 3600),
                (2, "watching", start - 1 * 3600),
                (1, "watching", int(start - .5 * 3600)),
                (.5, "skipped", None),
            ):
                service.scan_proprietary({"c-self": candidate}, now_ts=start - hours * 3600)
                row = store.load_ai_proprietary_positions()[0]
                self.assertEqual(row["decision"], "strategy_price_gate")
                self.assertEqual(row["status"], expected_status)
                self.assertEqual(row.get("next_retry_at") if expected_retry is not None else None, expected_retry)
                self.assertEqual(row["observed_entry_price"], .75)
                self.assertEqual(row["max_entry_price"], .68)
            service.close()

    def test_same_start_assessments_queue_for_next_tick_instead_of_terminal_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 30 * 60
            candidates = {}
            for cid in ("c-1", "c-2", "c-3"):
                candidates[cid] = {**market(start_ts=start), "condition_id": cid}

            service.scan_proprietary(candidates, now_ts=TEST_NOW)
            rows = {row["condition_id"]: row for row in store.load_ai_proprietary_positions()}
            queued = [row for row in rows.values() if row["decision"] == "assessment_queued"]
            self.assertEqual(len(queued), 1)
            self.assertEqual(queued[0]["status"], "watching")
            self.assertEqual(queued[0]["next_retry_at"], TEST_NOW + 60)

            service.scan_proprietary(candidates, now_ts=TEST_NOW + 60)
            rows = {row["condition_id"]: row for row in store.load_ai_proprietary_positions()}
            self.assertEqual(rows[queued[0]["condition_id"]]["status"], "open")
            service.close()

    def test_pre_v3_terminal_queue_is_reactivated_once_before_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 30 * 60
            candidate = market(start_ts=start)
            store.save_ai_proprietary_position({
                "condition_id": "c-self", "status": "skipped", "decision": "assessment_queued",
                "schedule_version": 2, "match_start_time": candidate["match_start_time"],
                "created_at": TEST_NOW - 300, "updated_at": TEST_NOW - 300,
            })

            service.scan_proprietary({"c-self": candidate}, now_ts=TEST_NOW)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["schedule_version"], 4)
            self.assertEqual(row["status"], "open")
            service.close()

    def test_popular_market_first_seen_inside_twenty_four_hours_is_screened_immediately(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            stats = service.scan_proprietary({"c-self": market(start_ts=TEST_NOW + 20 * 3600)}, now_ts=TEST_NOW)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(stats["entered"], 1)
            self.assertEqual(row["probe_trigger"], "initial_window")
            self.assertEqual(row["status"], "open")
            service.close()

    def test_proprietary_rows_sort_upcoming_near_to_far_then_recent_past(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FollowStore(Path(tmp) / "follow" / "follow.db")
            for cid, start in (("far", TEST_NOW + 7200), ("past", TEST_NOW - 60), ("near", TEST_NOW + 600)):
                store.save_ai_proprietary_position({
                    "condition_id": cid, "status": "watching", "decision": "pending",
                    "match_start_time": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "created_at": TEST_NOW, "updated_at": TEST_NOW,
                })
            rows = store.load_ai_proprietary_positions(order_by_start=True, now_ts=TEST_NOW)
            self.assertEqual([row["condition_id"] for row in rows], ["near", "far", "past"])

    def test_cold_market_uses_adaptive_twenty_four_hour_to_thirty_minute_schedule(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "gemini-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 25 * 3600
            thin_market = market(start_ts=start, volume=0)

            service.scan_proprietary({"c-self": thin_market}, now_ts=TEST_NOW)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["decision"], "awaiting_liquidity_window")
            self.assertEqual(row["next_retry_at"], start - 24 * 3600)
            self.assertEqual(row["probe_count"], 0)

            for hours, expected_next, expected_status in (
                (24, start - 12 * 3600, "watching"),
                (12, start - 6 * 3600, "watching"),
                (6, start - 3 * 3600, "watching"),
                (3, start - 2 * 3600, "watching"),
                (2, start - 1 * 3600, "watching"),
                (1, int(start - .5 * 3600), "watching"),
                (.5, None, "skipped"),
            ):
                service.scan_proprietary({"c-self": thin_market}, now_ts=start - hours * 3600)
                row = store.load_ai_proprietary_positions()[0]
                self.assertEqual(row["status"], expected_status)
                if expected_next is not None:
                    self.assertEqual(row["next_retry_at"], expected_next)
            self.assertEqual(row["decision"], "cold_market")
            self.assertEqual(row["cold_reason"], "volume_insufficient")
            self.assertEqual(row["probe_count"], 7)
            self.assertEqual(row["scheduled_probe_count"], 7)
            self.assertEqual(row["scheduled_probe_limit"], 7)
            service.close()

    def test_schedule_migration_resets_current_probe_counter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FollowStore(root / "follow" / "follow.db")
            service = AiRiskService(root, store, client_factory=Model, evidence_factory=Evidence, orderbook_client=Books())
            service.config.save_credential(envelope(service.config, "deepseek-test"))
            service.config.save_settings(enabled=True)
            start = TEST_NOW + 3 * 3600
            candidate = market(start_ts=start, volume=0)
            store.save_ai_proprietary_position({
                "condition_id": "c-self", "status": "watching", "decision": "volume_insufficient",
                "schedule_version": 3, "scheduled_probe_count": 8, "probe_count": 8,
                "match_start_time": candidate["match_start_time"], "next_retry_at": 0,
                "created_at": TEST_NOW - 3600, "updated_at": TEST_NOW - 300,
            })

            service.scan_proprietary({"c-self": candidate}, now_ts=TEST_NOW)
            row = store.load_ai_proprietary_positions()[0]
            self.assertEqual(row["schedule_version"], 4)
            self.assertEqual(row["scheduled_probe_count"], 1)
            self.assertEqual(row["scheduled_probe_limit"], 7)
            self.assertEqual(row["legacy_scheduled_probe_count"], 8)
            service.close()

    def test_proprietary_rejection_reasons_are_not_all_called_evidence_insufficient(self):
        cases = (
            (LowProbabilityModel, "probability_insufficient"),
            (LowConfidenceModel, "confidence_insufficient"),
            (InsufficientModel, "assessment_insufficient"),
        )
        for model, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                store = FollowStore(root / "follow" / "follow.db")
                service = AiRiskService(root, store, client_factory=model, evidence_factory=Evidence, orderbook_client=Books())
                service.config.save_credential(envelope(service.config, "deepseek-test"))
                service.config.save_settings(enabled=True)
                service.scan_proprietary({"c-self": market()}, now_ts=TEST_NOW)
                row = store.load_ai_proprietary_positions()[0]
                self.assertEqual(row["decision"], expected)
                self.assertEqual(row["required_evidence_score"], 80)
                self.assertEqual(row["required_win_probability"], 65)
                self.assertEqual(row["required_confidence"], 75)
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
