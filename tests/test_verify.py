"""The independent prefix verifier and byte-level determinism."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest

from support import CASES, ROOT, base_case, batch, ev, load, tenant

from solution.verify import verify_prefix_invariants


class Determinism(unittest.TestCase):
    def test_every_shipped_case_replays_to_the_same_bytes(self) -> None:
        for path in sorted(CASES.glob("*.json")):
            case = load(path.name)
            first = verify_prefix_invariants(case)
            second = verify_prefix_invariants(case)
            self.assertTrue(first["deterministic"], path.name)
            self.assertEqual(first["final_state"], second["final_state"],
                             path.name)

    def test_cli_output_is_byte_identical_across_runs(self) -> None:
        case_path = str(CASES / "group_reservation.json")
        outputs = []
        for _ in range(3):
            proc = subprocess.run(
                [sys.executable, "-m", "solution", "ingest", case_path,
                 "--json-indent", "0"],
                cwd=str(ROOT), capture_output=True, text=True,
                encoding="utf-8")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            outputs.append(proc.stdout)
        self.assertEqual(len(set(outputs)), 1)


class PrefixSafety(unittest.TestCase):
    def test_all_shipped_cases_have_no_violations(self) -> None:
        for path in sorted(CASES.glob("*.json")):
            report = verify_prefix_invariants(load(path.name))
            self.assertEqual(report["violations"], [], path.name)
            self.assertEqual(report["prefixes_checked"],
                             report["batches"], path.name)

    def test_verifier_detects_cross_tenant_reuse(self) -> None:
        """A deliberately unsafe history must be caught, not rubber-stamped.

        This drives the verifier's check directly with a hand-built state that
        hands a beta connection to alpha while the context still says beta.
        """
        from solution.verify import _prefix_violations
        from solution.state import new_state, new_connection
        state = new_state({})
        conn = new_connection("c1")
        conn["lifecycle"] = "IN_USE"
        conn["context"] = {"tenant": "beta", "peer_identity_version": 1,
                           "trust_config_version": 1,
                           "purpose_level": "interactive"}
        conn["request_id"] = "r1"
        state["connections"]["c1"] = conn
        state["requests"]["r1"] = {
            "request_id": "r1", "tenant": "alpha", "state": "IN_USE",
            "reserved": ["c1"]}
        state["request_receipts"] = {}
        found = _prefix_violations(state, "synthetic")
        kinds = {v["kind"] for v in found}
        self.assertIn("CROSS_TENANT_REUSE", kinds)

    def test_verifier_detects_duplicate_terminal_receipts(self) -> None:
        from solution.verify import _prefix_violations
        from solution.state import new_state
        state = new_state({})
        state["requests"]["r1"] = {
            "request_id": "r1", "tenant": "alpha", "state": "SUCCEEDED",
            "reserved": []}
        state["receipts"] = {
            "rc-1": {"receipt_id": "rc-1", "request_id": "r1",
                     "type": "SUCCEEDED", "terminal": True, "digest": "a"},
            "rc-2": {"receipt_id": "rc-2", "request_id": "r1",
                     "type": "TERMINATED", "terminal": True, "digest": "b"},
        }
        state["request_receipts"] = {"r1": ["rc-1", "rc-2"]}
        found = _prefix_violations(state, "synthetic")
        self.assertIn("DUPLICATE_TERMINAL_RECEIPT",
                      {v["kind"] for v in found})


class VerifierStructure(unittest.TestCase):
    def test_report_shape(self) -> None:
        report = verify_prefix_invariants(load("two_tenants_one_connection.json"))
        for key in ("command", "deterministic", "violations", "prefixes",
                    "prefixes_checked", "final_state", "ok"):
            self.assertIn(key, report)
        self.assertEqual(report["command"], "verify")
        self.assertTrue(report["ok"])

    def test_cli_verify_exit_zero(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-m", "solution", "verify",
             str(CASES / "takeover.json"), "--json-indent", "0"],
            cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["ok"])


if __name__ == "__main__":
    unittest.main()
