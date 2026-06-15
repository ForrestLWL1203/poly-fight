import json
import unittest

from poly_fight.market_ws import WSResolutionCollector


def _collector():
    return WSResolutionCollector(reconcile=None)


class WSResolutionCollectorTest(unittest.TestCase):
    def test_set_conditions_selects_subscribed_tokens(self):
        c = _collector()
        c.merge_asset_map({
            "tokA": {"conditionId": "0xcid1", "outcomeIndex": 0},
            "tokB": {"conditionId": "0xcid1", "outcomeIndex": 1},
            "tokC": {"conditionId": "0xcid2", "outcomeIndex": 0},
        })
        c.set_conditions({"0xcid1"})
        self.assertEqual(c._sub_tokens, ("tokA", "tokB"))     # only cid1's tokens
        self.assertEqual(c.mapped_conditions(), {"0xcid1", "0xcid2"})

    def test_market_resolved_maps_winning_asset_to_outcome(self):
        c = _collector()
        c.merge_asset_map({
            "tokA": {"conditionId": "0xcid1", "outcomeIndex": 0},
            "tokB": {"conditionId": "0xcid1", "outcomeIndex": 1},
        })
        c._handle_payload(json.dumps({"event_type": "market_resolved",
                                      "market": "0xcid1", "winning_asset_id": "tokB"}))
        self.assertEqual(c.drain(), {"0xcid1": 1})
        self.assertEqual(c.drain(), {})                       # drain clears
        self.assertEqual(c.resolved_count, 1)

    def test_payload_list_and_pong_and_offscope_ignored(self):
        c = _collector()
        c.merge_asset_map({"tokA": {"conditionId": "0xcid1", "outcomeIndex": 0}})
        c._handle_payload("PONG")                              # heartbeat -> ignored
        c._handle_payload(json.dumps([{"event_type": "book"},  # noise events ignored
                                      {"event_type": "market_resolved",
                                       "winning_asset_id": "tokA"}]))
        c._handle_payload(json.dumps({"event_type": "market_resolved",
                                      "winning_asset_id": "unknown_token"}))  # off-scope
        self.assertEqual(c.drain(), {"0xcid1": 0})

    def test_retain_conditions_prunes_stale_markets(self):
        c = _collector()
        c.merge_asset_map({
            "tokA": {"conditionId": "0xkeep", "outcomeIndex": 0},
            "tokB": {"conditionId": "0xstale", "outcomeIndex": 0},
        })
        self.assertEqual(c.mapped_conditions(), {"0xkeep", "0xstale"})
        c.retain_conditions({"0xkeep"})                       # drop ended matches
        self.assertEqual(c.mapped_conditions(), {"0xkeep"})

    def test_reconcile_seeds_buffer(self):
        c = WSResolutionCollector(reconcile=lambda conds: {"0xcid1": 1})
        c.merge_asset_map({"tokA": {"conditionId": "0xcid1", "outcomeIndex": 0}})
        c.set_conditions({"0xcid1"})
        c._reconcile_now()
        self.assertEqual(c.drain(), {"0xcid1": 1})


if __name__ == "__main__":
    unittest.main()
