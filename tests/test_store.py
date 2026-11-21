"""SQLite atomicity, crash recovery and idempotency tests."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

from support import ROOT, TempDb, base_case, batch, ev, load, run_ingest, tenant

from solution.crash import SimulatedCrash
from solution.runner import cmd_ingest, cmd_recover
from solution.store import PoolStore


class AtomicCommit(TempDb):
    def test_crash_before_commit_leaves_no_trace(self) -> None:
        case = load("crash_and_resume.json")
        with self.assertRaises(SimulatedCrash) as ctx:
            cmd_ingest(case, self.db_path, crash="before-commit", crash_batch=2)
        self.assertFalse(ctx.exception.committed)
        with PoolStore(self.db_path).open() as store:
            applied = store.applied_batches()
            receipt_count = store.conn.execute(
                "SELECT COUNT(*) c FROM receipt").fetchone()["c"]
        self.assertEqual(sorted(applied), ["b01-submit", "b02-p0-exits"])
        # At this prefix the request is only PENDING: no reservation and no
        # terminal receipt has been emitted, and the loseable batch wrote none.
        self.assertEqual(receipt_count, 0)

    def test_crash_after_commit_keeps_batch_and_receipt(self) -> None:
        case = load("crash_and_resume.json")
        with self.assertRaises(SimulatedCrash) as ctx:
            cmd_ingest(case, self.db_path, crash="after-commit", crash_batch=5)
        self.assertTrue(ctx.exception.committed)
        with PoolStore(self.db_path).open() as store:
            applied = store.applied_batches()
            receipt = store.receipt_for("r1")
        self.assertIn("b06-complete", applied)
        self.assertEqual(receipt["type"], "SUCCEEDED")

    def test_reingest_after_commit_loss_is_idempotent(self) -> None:
        case = load("crash_and_resume.json")
        with self.assertRaises(SimulatedCrash):
            cmd_ingest(case, self.db_path, crash="after-commit", crash_batch=5)
        result = cmd_ingest(case, self.db_path)
        replayed = {b["batch_id"] for b in result["batches"] if b.get("replayed")}
        self.assertIn("b06-complete", replayed)
        self.assertEqual(result["final_state"]["receipts"]["r1"],
                         ["RESERVED", "SUCCEEDED"])
        with PoolStore(self.db_path).open() as store:
            rows = store.conn.execute(
                "SELECT COUNT(*) c FROM receipt WHERE request_id='r1' "
                "AND terminal=1").fetchone()["c"]
            self.assertEqual(rows, 1)

    def test_lease_and_pending_ack_persist_together(self) -> None:
        case = load("takeover.json")
        cmd_ingest(case, self.db_path)
        with PoolStore(self.db_path).open() as store:
            lease = store.lease_row()
            state = store.load(case["config"])
        self.assertEqual(lease["holder"], "p1")
        self.assertEqual(lease["generation"], state["lease"]["generation"])
        # After the full run every operation has settled.
        self.assertEqual(
            [op for op in state["operations"].values()
             if op["status"] == "PENDING_ACK"], [])

    def test_pending_ack_list_written_when_worker_dies(self) -> None:
        case = load("takeover.json")
        partial = dict(case)
        partial["batches"] = case["batches"][:5]  # up to the worker exit
        cmd_ingest(partial, self.db_path)
        with PoolStore(self.db_path).open() as store:
            acks = store.pending_acks()
        self.assertEqual([a["op_id"] for a in acks], ["op-purge-c1-2"])
        self.assertEqual(acks[0]["kind"], "PURGE")

    def test_conflicting_batch_rejected_at_store_boundary(self) -> None:
        case = load("smoke.json")
        cmd_ingest(case, self.db_path)
        changed = load("smoke.json")
        changed["batches"][0]["events"][0]["request_id"] = "different"
        with self.assertRaises(Exception) as ctx:
            cmd_ingest(changed, self.db_path)
        self.assertEqual(getattr(ctx.exception, "code", None),
                         "E_BATCH_CONFLICT")

    def test_recover_reports_no_duplicate_terminal_receipts(self) -> None:
        case = load("crash_and_resume.json")
        cmd_ingest(case, self.db_path)
        report = cmd_recover(case, self.db_path)
        self.assertEqual(report["duplicate_terminal_receipts"], [])
        self.assertEqual(report["terminal_receipts"]["r1"], "SUCCEEDED")


class PersistentHandoff(TempDb):
    def test_two_tenants_across_separate_processes(self) -> None:
        """Each batch group runs in its own interpreter, sharing one DB."""
        case = load("two_tenants_one_connection.json")
        scripts = []
        for cut in (3, len(case["batches"])):
            part = dict(case)
            part["batches"] = case["batches"][:cut]
            scripts.append(part)
        # Simulate separate runs by re-ingesting progressively longer prefixes.
        first = cmd_ingest(scripts[0], self.db_path)
        self.assertEqual(
            {b["batch_id"] for b in first["batches"]},
            {"b01-submit-alpha", "b02-auth-alpha", "b03-finish-alpha"})
        final = cmd_ingest(case, self.db_path)
        replayed = {b["batch_id"] for b in final["batches"] if b.get("replayed")}
        self.assertEqual(len(replayed), 3)
        self.assertEqual(final["final_state"]["receipts"]["r-beta"],
                         ["RESERVED", "SUCCEEDED"])


class CliIntegration(TempDb):
    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "solution", *args],
            cwd=str(ROOT), capture_output=True, text=True,
            encoding="utf-8")

    def test_ingest_db_then_recover(self) -> None:
        case_path = str(ROOT / "cases" / "two_tenants_one_connection.json")
        first = self._run("ingest", case_path, "--db", self.db_path,
                          "--json-indent", "0")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertIn('"command":"ingest"', first.stdout.replace(" ", ""))
        second = self._run("recover", case_path, "--db", self.db_path,
                           "--json-indent", "0")
        self.assertEqual(second.returncode, 0)
        self.assertIn('"duplicate_terminal_receipts":[]',
                      second.stdout.replace(" ", ""))

    def test_conflict_exit_code_is_three(self) -> None:
        case_path = str(ROOT / "cases" / "smoke.json")
        self._run("ingest", case_path, "--db", self.db_path)
        # A different case file reusing batch ids must be refused.
        tmp = Path(self._tmp.name) / "changed.json"
        import json
        raw = json.loads((ROOT / "cases" / "smoke.json").read_text("utf-8"))
        raw["batches"][0]["events"][0]["request_id"] = "r-other"
        tmp.write_text(json.dumps(raw), encoding="utf-8")
        result = self._run("ingest", str(tmp), "--db", self.db_path,
                           "--json-indent", "0")
        self.assertEqual(result.returncode, 3)


if __name__ == "__main__":
    unittest.main()
