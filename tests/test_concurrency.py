"""Concurrency-shaped tests built with a synchronisation barrier.

The simulator is single-threaded by design, so "simultaneous" here means
**one batch at one logical instant**: every event in the barrier batch is
folded before a single scheduling pass runs.  These tests assert the outcomes
that a genuine race would have to produce, and that no interleaving leaks a
second receipt or a stale state transition.
"""

from __future__ import annotations

import unittest

from support import base_case, batch, ev, load, outcome, run_ingest, tenant


def _barrier(events: list[dict]) -> list[dict]:
    """A barrier batch: all events share exact logical time 100."""
    return batch("barrier", 100, events)


class TakeoverAndRevokeSameInstant(unittest.TestCase):
    """p0's lease expires exactly as alpha's peer is revoked."""

    def setUp(self) -> None:
        self.case = base_case(
            "takeover-revoke-same-instant",
            config={"lease_ttl_ms": 50},
            connections=["c1", "c2"],
            tenants=[tenant("alpha"), tenant("beta")],
            batches=[
                batch("warm", 0, [
                    ev("w0", "submit_request", request_id="r-alpha",
                       tenant="alpha", candidates=["c1"]),
                    ev("w1", "submit_request", request_id="r-beta",
                       tenant="beta", candidates=["c2"]),
                ]),
                batch("auth", 1, [
                    ev("a1", "auth_complete", connection_id="c1",
                       generation=1, op_id="op-auth-c1-1",
                       expected_config_version=1, lease_generation=1,
                       outcome="ok"),
                    ev("a2", "auth_complete", connection_id="c2",
                       generation=1, op_id="op-auth-c2-2",
                       expected_config_version=1, lease_generation=1,
                       outcome="ok"),
                ]),
                batch("release-p0", 2, [
                    ev("x1", "release_lease", process_id="p0"),
                ]),
                _barrier([
                    ev("b1", "acquire_lease", process_id="p1"),
                    ev("b2", "revoke_peer", tenant="alpha",
                       reason="cert withdrawn"),
                    ev("b3", "complete_request", request_id="r-beta",
                       outcome="success"),
                ]),
            ],
        )
        self.result = run_ingest(self.case)

    def test_takeover_advances_generation(self) -> None:
        lease = self.result["final_state"]["lease"]
        self.assertEqual(lease["holder"], "p1")
        self.assertEqual(lease["generation"], 2)

    def test_alpha_terminated_beta_survives(self) -> None:
        reqs = self.result["final_state"]["requests"]
        self.assertEqual(reqs["r-alpha"]["state"], "TERMINATED")
        self.assertEqual(reqs["r-beta"]["state"], "SUCCEEDED")

    def test_no_second_terminal_receipt(self) -> None:
        receipts = self.result["final_state"]["receipts"]
        self.assertEqual(receipts["r-alpha"].count("TERMINATED"), 1)
        self.assertEqual(receipts["r-beta"].count("SUCCEEDED"), 1)


class TwoProcessesRaceForOneGroup(unittest.TestCase):
    """Both processes acquire the lease in one barrier; only one may win."""

    def test_second_acquire_in_same_instant_is_rejected(self) -> None:
        case = base_case(
            "two-race-group",
            connections=["c1"],
            tenants=[tenant("alpha")],
            batches=[
                batch("start", 0, [
                    ev("s1", "release_lease", process_id="p0"),
                ]),
                _barrier([
                    ev("b1", "acquire_lease", process_id="p1"),
                    ev("b2", "acquire_lease", process_id="p2"),
                ]),
            ],
        )
        result = run_ingest(case)
        outcomes = [e["outcome"] for e in result["batches"][1]["events"]]
        self.assertEqual(outcomes[0]["result"], "takeover")
        self.assertEqual(outcomes[1]["result"], "rejected")
        self.assertEqual(outcomes[1]["rejected"], "LEASE_NOT_HELD")
        self.assertEqual(result["final_state"]["lease"]["holder"], "p1")

    def test_only_winner_can_reserve(self) -> None:
        case = base_case(
            "two-race-group-reserve",
            connections=["c1"],
            tenants=[tenant("alpha")],
            batches=[
                batch("start", 0, [ev("s1", "release_lease", process_id="p0")]),
                _barrier([
                    ev("b1", "acquire_lease", process_id="p1"),
                    ev("b2", "acquire_lease", process_id="p2"),
                    ev("b3", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c1"]),
                ]),
            ],
        )
        result = run_ingest(case)
        # The loser's attempt is recorded, the winner's scheduling pass claims
        # the connection exactly once.
        lease = result["final_state"]["lease"]
        self.assertEqual(lease["holder"], "p1")
        conn = result["final_state"]["connections"]["c1"]
        self.assertEqual(conn["request_id"], "r1")


class GroupWithOnlyOneAvailable(unittest.TestCase):
    """A group of two where only one candidate is usable commits nothing."""

    def test_single_available_candidate_does_not_park(self) -> None:
        case = base_case(
            "group-one-available",
            connections=["c1", "c2"],
            tenants=[tenant("alpha")],
            batches=[
                batch("hold-c2", 0, [
                    ev("h1", "submit_request", request_id="r-hold",
                       tenant="alpha", candidates=["c2"]),
                ]),
                batch("group", 10, [
                    ev("g1", "submit_request", request_id="r-group",
                       tenant="alpha", candidates=["c1", "c2"],
                       group_size=2),
                ]),
            ],
        )
        result = run_ingest(case)
        step = result["batches"][1]["schedule"][0]
        self.assertEqual(step["action"], "deferred")
        # c1 is the one usable candidate (unauthenticated but free), c2 is
        # committed elsewhere; the group takes neither.
        self.assertEqual(step["authable"], ["c1"])
        self.assertEqual(step["busy"], ["c2"])
        # c1 stays free for other work rather than being parked.
        self.assertIsNone(result["final_state"]["connections"]["c1"]["request_id"])
        self.assertEqual(result["final_state"]["connections"]["c1"]["lifecycle"],
                         "IDLE")


class LaterBatchStillAdvances(unittest.TestCase):
    """A waiting first batch must not stop a later batch from progressing."""

    def test_first_batch_waits_while_later_advances(self) -> None:
        case = base_case(
            "later-batch-advances",
            connections=["c1", "c2"],
            tenants=[tenant("alpha")],
            batches=[
                batch("blocked", 0, [
                    ev("b1", "submit_request", request_id="r-blocked",
                       tenant="alpha", candidates=["c1"], group_size=1),
                ]),
                batch("holder", 5, [
                    ev("h1", "submit_request", request_id="r-holder",
                       tenant="alpha", candidates=["c1"]),
                ]),
                batch("later", 10, [
                    ev("l1", "submit_request", request_id="r-later",
                       tenant="alpha", candidates=["c2"]),
                ]),
            ],
        )
        result = run_ingest(case)
        # r-blocked was first but c1 was free, so it took c1; r-holder waits.
        first = result["batches"][0]["schedule"][0]
        self.assertEqual(first["action"], "committed")
        holder = result["batches"][1]["schedule"][0]
        self.assertEqual(holder["action"], "deferred")
        later_entries = [s for s in result["batches"][2]["schedule"]
                         if s.get("request_id") == "r-later"]
        self.assertEqual(later_entries[0]["action"], "committed")


class SignificantEventsAcrossBarrier(unittest.TestCase):
    """Auth completion and reuse written in one barrier must not interleave."""

    def test_barrier_auth_then_immediate_completion(self) -> None:
        case = base_case(
            "barrier-auth-complete",
            connections=["c1"],
            tenants=[tenant("alpha")],
            batches=[
                batch("submit", 0, [
                    ev("s1", "submit_request", request_id="r1",
                       tenant="alpha", candidates=["c1"]),
                ]),
                _barrier([
                    ev("b1", "auth_complete", connection_id="c1",
                       generation=1, op_id="op-auth-c1-1",
                       expected_config_version=1, lease_generation=1,
                       outcome="ok"),
                    ev("b2", "complete_request", request_id="r1",
                       outcome="success"),
                ]),
            ],
        )
        result = run_ingest(case)
        # The completion is applied before the scheduling pass promotes the
        # request to IN_USE, so it is NOT_IN_USE rather than a success.
        outcomes = result["batches"][1]["events"]
        self.assertEqual(outcomes[0]["outcome"]["result"], "accepted")
        self.assertEqual(outcomes[1]["outcome"]["rejected"], "NOT_IN_USE")
        receipts = result["final_state"]["receipts"]
        self.assertNotIn("SUCCEEDED", receipts.get("r1", []))


if __name__ == "__main__":
    unittest.main()
