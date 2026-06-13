"""Runs the dashboardV2 strategy-mapper JS unit test (adapt.js) via node.

The frontend strategyToKit/strategyFromKit mappers convert between the kit's
flat form state and the backend follow_strategy schema. This wrapper executes
the Node assertions in dashboardv2_strategy_mapping.test.js so they run as part
of `python -m unittest`. Skipped when node is unavailable.
"""

import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS_TEST = Path(__file__).with_name("dashboardv2_strategy_mapping.test.js")


class DashboardV2StrategyMappingTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "node not available")
    def test_strategy_mappers_roundtrip_and_backend_validity(self):
        proc = subprocess.run(
            [shutil.which("node"), str(JS_TEST)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        self.assertIn("ALL PASS", proc.stdout)


if __name__ == "__main__":
    unittest.main()
