"""Idempotency of ingest, batch and request, and the read commands."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest

from support import CASES, ROOT, run_ingest

from solution.errors import SimError


class BatchIdempotency(unittest.TestCase):
    def test_reapplying_a_batch_is_a_replay(self) -> None:
        from solution.runner import Runner
        from support import base_case, batch, ev, tenant
        case = base_case(
            "idem-batch", connections=["c1"], tenants=[tenant("alpha")],
            batches=[batch("b1", 0, [
                ev("e1", "submit_request", request_id="r1", tenant="alpha",
                   candidates=["c1"])])])
        runner = Runner(case, None)
        first = runner.engine.apply_batch(case["batches"][0])
        state_after_first = len(runner.state["receipts"])
        second = runner.engine.apply_batch(case["batches"][0])
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        # The replay returns the stored result verbatim and touched no state.
        self.assertEqual(second["events"], first["events"])
        self.assertEqual(len(runner.state["receipts"]), state_after_first)
        self.assertEqual(len(runner.state["requests"]), 1)
        self.assertEqual(len(runner.state["submit_order"]), 1)

    def test_reapplying_changed_batch_conflicts(self) -> None:
        from solution.runner import Runner
        from support import base_case, batch, ev, tenant
        case = base_case(
            "idem-batch-conflict", connections=["c1"],
            tenants=[tenant("alpha")],
            batches=[batch("b1", 0, [
                ev("e1", "submit_request", request_id="r1", tenant="alpha",
                   candidates=["c1"])])])
        runner = Runner(case, None)
        runner.engine.apply_batch(case["batches"][0])
        changed = batch("b1", 0, [
            ev("e1", "submit_request", request_id="r2", tenant="alpha",
               candidates=["c1"])])
        from solution.schema import normalize_case
        changed = normalize_case({
            "sim_version": 1, "connections": [{"connection_id": "c1"}],
            "tenants": [tenant("alpha")], "batches": [changed],
        })["batches"][0]
        with self.assertRaises(SimError) as ctx:
            runner.engine.apply_batch(changed)
        self.assertEqual(ctx.exception.code, "E_BATCH_CONFLICT")


class BatchAtomicity(unittest.TestCase):
    def test_mid_batch_error_rolls_back_the_whole_batch(self) -> None:
        from solution.runner import Runner
        from support import base_case, batch, ev, tenant
        case = base_case(
            "rollback", connections=["c1"], tenants=[tenant("alpha")],
            batches=[batch("b1", 0, [
                ev("e1", "submit_request", request_id="r1",
                   tenant="alpha", candidates=["c1"]),
                ev("e2", "submit_request", request_id="r2",
                   tenant="ghost", candidates=["c1"]),
            ])])
        runner = Runner(case, None)
        before = json.dumps(runner.state, sort_keys=True)
        with self.assertRaises(SimError):
            runner.engine.apply_batch(case["batches"][0])
        after = json.dumps(runner.state, sort_keys=True)
        self.assertEqual(before, after)
        self.assertNotIn("r1", runner.state["requests"])
        self.assertNotIn("b1", runner.state["batches"])


class RequestIdempotency(unittest.TestCase):
    def test_duplicate_submit_with_same_content_is_ignored(self) -> None:
        from support import base_case, batch, ev, tenant
        case = base_case(
            "idem-request", connections=["c1"], tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [
                    ev("e1", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c1"])]),
                batch("b2", 1, [
                    ev("e2", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c1"])]),
            ])
        result = run_ingest(case)
        second = result["batches"][1]["events"][0]["outcome"]
        self.assertEqual(second["result"], "duplicate_ignored")

    def test_duplicate_submit_with_new_content_conflicts(self) -> None:
        from support import base_case, batch, ev, tenant
        case = base_case(
            "idem-request-conflict", connections=["c1", "c2"],
            tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [
                    ev("e1", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c1"])]),
                batch("b2", 1, [
                    ev("e2", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c2"])]),
            ])
        with self.assertRaises(SimError) as ctx:
            run_ingest(case)
        self.assertEqual(ctx.exception.code, "E_BATCH_CONFLICT")


class ReadCommands(unittest.TestCase):
    def _cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "solution", *args, "--json-indent", "0"],
            cwd=str(ROOT), capture_output=True, text=True, encoding="utf-8")

    def test_explain_one_request(self) -> None:
        proc = self._cli("explain",
                         str(CASES / "late_completion.json"), "--request", "r1")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["final_state"], "CANCELLED")
        self.assertEqual([t["to"] for t in payload["transitions"]],
                         ["PENDING", "RESERVED", "CANCELLED"])
        kinds = {x["kind"] for x in payload["late_completions"]}
        self.assertIn("stale_async_completion", kinds)

    def test_explain_all_requests(self) -> None:
        proc = self._cli("explain", str(CASES / "group_reservation.json"))
        self.assertEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(set(payload["requests"]),
                         {"r-warm-1", "r-warm-2", "r-holder", "r-group",
                          "r-single"})

    def test_explain_unknown_request_errors(self) -> None:
        proc = self._cli("explain", str(CASES / "smoke.json"),
                         "--request", "missing")
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["error"]["code"], "E_USAGE")

    def test_inspect_lists_operations(self) -> None:
        proc = self._cli("inspect", str(CASES / "purge_cooldown_failure.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["operations"])
        kinds = {op["kind"] for op in payload["operations"].values()}
        self.assertIn("PURGE", kinds)
        self.assertIn("AUTH", kinds)

    def test_replay_reports_determinism(self) -> None:
        proc = self._cli("replay", str(CASES / "takeover.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["deterministic"])
        self.assertEqual(payload["runs"], 2)

    def test_no_command_is_usage_error(self) -> None:
        proc = self._cli("ingest")
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
