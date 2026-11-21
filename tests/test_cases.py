"""End-to-end assertions over the shipped example cases."""

from __future__ import annotations

import unittest

from support import load, run_ingest


class TwoTenantsOneConnection(unittest.TestCase):
    def setUp(self) -> None:
        self.result = run_ingest(load("two_tenants_one_connection.json"))

    def test_both_requests_succeed(self) -> None:
        reqs = self.result["final_state"]["requests"]
        self.assertEqual(reqs["r-alpha"]["state"], "SUCCEEDED")
        self.assertEqual(reqs["r-beta"]["state"], "SUCCEEDED")

    def test_one_reserved_one_success_each(self) -> None:
        receipts = self.result["final_state"]["receipts"]
        self.assertEqual(receipts["r-alpha"], ["RESERVED", "SUCCEEDED"])
        self.assertEqual(receipts["r-beta"], ["RESERVED", "SUCCEEDED"])

    def test_beta_required_a_purge(self) -> None:
        sched = {b["batch_id"]: b["schedule"] for b in self.result["batches"]}
        self.assertEqual(sched["b04-submit-beta"][0]["purge"], ["c1"])
        self.assertEqual(sched["b04-submit-beta"][0]["action"], "committed")

    def test_final_context_belongs_to_beta(self) -> None:
        conn = self.result["final_state"]["connections"]["c1"]
        self.assertEqual(conn["lifecycle"], "IDLE")
        self.assertEqual(conn["context"]["tenant"], "beta")


class PurposeLevelChange(unittest.TestCase):
    def test_changing_purpose_forces_purge_and_reauth(self) -> None:
        result = run_ingest(load("purpose_level_change.json"))
        sched = {b["batch_id"]: b["schedule"] for b in result["batches"]}
        self.assertEqual(sched["b04-purpose-level-changes"][0]["purge"], ["c1"])
        conn = result["final_state"]["connections"]["c1"]
        self.assertEqual(conn["context"]["purpose_level"], "batch")
        self.assertEqual(result["final_state"]["receipts"]["r2"],
                         ["RESERVED", "SUCCEEDED"])


class AuthAndRevokeSameBatch(unittest.TestCase):
    def setUp(self) -> None:
        self.result = run_ingest(load("auth_and_revoke_same_batch.json"))

    def test_alpha_requests_terminated(self) -> None:
        reqs = self.result["final_state"]["requests"]
        self.assertEqual(reqs["r-alpha-live"]["state"], "TERMINATED")
        self.assertEqual(reqs["r-alpha-waiting"]["state"], "TERMINATED")

    def test_termination_receipt_arrives_once(self) -> None:
        receipts = self.result["final_state"]["receipts"]
        self.assertEqual(receipts["r-alpha-live"].count("TERMINATED"), 1)
        self.assertEqual(receipts["r-alpha-waiting"].count("TERMINATED"), 1)

    def test_late_success_is_not_delivered(self) -> None:
        receipts = self.result["final_state"]["receipts"]
        self.assertNotIn("SUCCEEDED", receipts["r-alpha-live"])
        self.assertNotIn("SUCCEEDED", receipts["r-alpha-waiting"])

    def test_beta_is_not_collateral(self) -> None:
        self.assertEqual(self.result["final_state"]["requests"]["r-beta"]["state"],
                         "SUCCEEDED")
        self.assertEqual(
            self.result["final_state"]["receipts"]["r-beta"],
            ["RESERVED", "SUCCEEDED"])

    def test_alpha_connections_closed(self) -> None:
        conns = self.result["final_state"]["connections"]
        self.assertEqual(conns["c1"]["lifecycle"], "CLOSED")
        self.assertEqual(conns["c3"]["lifecycle"], "CLOSED")
        self.assertEqual(conns["c2"]["lifecycle"], "IDLE")


class LateCompletion(unittest.TestCase):
    def test_late_auth_after_cancel_and_reuse_is_rejected(self) -> None:
        result = run_ingest(load("late_completion.json"))
        receipts = result["final_state"]["receipts"]
        self.assertEqual(receipts["r1"], ["CANCELLED"])
        self.assertEqual(receipts["r2"], ["RESERVED", "SUCCEEDED"])
        late = [b for b in result["batches"]
                if b["batch_id"].startswith("b05")]
        self.assertIn("STALE_OPERATION", late[0]["rejections"])


class Takeover(unittest.TestCase):
    def test_old_lease_message_rejected_new_one_accepted(self) -> None:
        result = run_ingest(load("takeover.json"))
        by_id = {b["batch_id"]: b for b in result["batches"]}
        self.assertIn("STALE_LEASE",
                      by_id["b07-p0-completion-arrives-under-old-lease"]["rejections"])
        self.assertEqual(
            by_id["b08-p1-purge-completes"]["rejections"], [])
        self.assertEqual(result["final_state"]["lease"]["holder"], "p1")
        self.assertEqual(result["final_state"]["lease"]["generation"], 2)

    def test_worker_exit_marks_pending_ack_not_completed(self) -> None:
        result = run_ingest(load("takeover.json"))
        exit_out = None
        for b in result["batches"]:
            for e in b.get("events", []):
                if e["type"] == "worker_exit":
                    exit_out = e["outcome"]
        self.assertIsNotNone(exit_out)
        self.assertEqual(exit_out["pending_ack_operations"],
                         ["op-purge-c1-2"])


class GroupReservation(unittest.TestCase):
    def setUp(self) -> None:
        self.result = run_ingest(load("group_reservation.json"))

    def test_partial_group_takes_nothing(self) -> None:
        sched = {b["batch_id"]: b["schedule"] for b in self.result["batches"]}
        step = sched["b04-group-wants-three-but-c3-busy"][0]
        self.assertEqual(step["action"], "deferred")
        self.assertEqual(step["busy"], ["c3"])
        self.assertEqual(step["reusable"], ["c1", "c2"])

    def test_later_single_request_still_progresses(self) -> None:
        sched = {b["batch_id"]: b["schedule"] for b in self.result["batches"]}
        step = sched["b05-single-request-takes-c2"][-1]
        self.assertEqual(step["request_id"], "r-single")
        self.assertEqual(step["action"], "committed")

    def test_group_commits_once_all_free(self) -> None:
        sched = {b["batch_id"]: b["schedule"] for b in self.result["batches"]}
        step = sched["b07-finish-single-frees-c2"][0]
        self.assertEqual(step["action"], "committed")
        self.assertEqual(step["connections"], ["c1", "c2", "c3"])


class PurgeCooldown(unittest.TestCase):
    def test_failed_purge_cools_down_then_retries(self) -> None:
        result = run_ingest(load("purge_cooldown_failure.json"))
        by_id = {b["batch_id"]: b for b in result["batches"]}
        fail = by_id["b05-purge-fails"]["events"][0]["outcome"]
        self.assertTrue(fail["will_retry"])
        cooling = by_id["b06-still-cooling"]["schedule"][0]
        self.assertEqual(cooling["action"], "deferred")
        self.assertIn("c1", cooling["busy"])
        retried = by_id["b07-cooldown-expired-retry"]["schedule"][0]
        self.assertEqual(retried["purge"], ["c1"])
        self.assertEqual(result["final_state"]["receipts"]["r-beta"],
                         ["RESERVED", "SUCCEEDED"])


class CrashAndResume(unittest.TestCase):
    def test_duplicate_completion_yields_one_success(self) -> None:
        result = run_ingest(load("crash_and_resume.json"))
        receipts = result["final_state"]["receipts"]
        self.assertEqual(receipts["r1"].count("SUCCEEDED"), 1)
        self.assertEqual(receipts["r1"], ["RESERVED", "SUCCEEDED"])
        by_id = {b["batch_id"]: b for b in result["batches"]}
        self.assertIn("STALE_OPERATION",
                      by_id["b05-duplicate-completion-after-restart"]["rejections"])
        self.assertIn("TERMINAL_STATE",
                      by_id["b07-duplicate-complete-after-restart"]["rejections"])


if __name__ == "__main__":
    unittest.main()
