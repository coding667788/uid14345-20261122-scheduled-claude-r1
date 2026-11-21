"""Engine-level behaviour: classification, expiry, reuse and queue order."""

from __future__ import annotations

import unittest

from support import base_case, batch, ev, run_ingest, schedule_of, tenant


class AuthExpiry(unittest.TestCase):
    def test_expired_context_is_reauthenticated_not_reused(self) -> None:
        case = base_case(
            "ttl", connections=["c1"], tenants=[tenant("alpha")],
            config={"auth_ttl_ms": 30},
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", candidates=["c1"])]),
                batch("b2", 10, [ev("e2", "auth_complete",
                                    connection_id="c1", generation=1,
                                    op_id="op-auth-c1-1",
                                    expected_config_version=1,
                                    lease_generation=1, outcome="ok")]),
                batch("b3", 20, [ev("e3", "complete_request",
                                    request_id="r1", outcome="success")]),
                batch("b4", 100, [ev("e4", "submit_request", request_id="r2",
                                     tenant="alpha", candidates=["c1"])]),
            ])
        result = run_ingest(case)
        step = schedule_of(result, "b4")[0]
        self.assertEqual(step["action"], "committed")
        # Same four-part context, but the auth has lapsed: re-authenticate.
        self.assertEqual(step["reused"], [])
        self.assertEqual(step["auth"], ["c1"])
        self.assertEqual(step["purge"], [])

    def test_unexpired_context_is_reused_directly(self) -> None:
        case = base_case(
            "ttl-live", connections=["c1"], tenants=[tenant("alpha")],
            config={"auth_ttl_ms": 1000},
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", candidates=["c1"])]),
                batch("b2", 10, [ev("e2", "auth_complete",
                                    connection_id="c1", generation=1,
                                    op_id="op-auth-c1-1",
                                    expected_config_version=1,
                                    lease_generation=1, outcome="ok")]),
                batch("b3", 20, [ev("e3", "complete_request",
                                    request_id="r1", outcome="success")]),
                batch("b4", 30, [ev("e4", "submit_request", request_id="r2",
                                    tenant="alpha", candidates=["c1"])]),
            ])
        result = run_ingest(case)
        step = schedule_of(result, "b4")[0]
        self.assertEqual(step["reused"], ["c1"])
        self.assertEqual(step["auth"], [])


class TrustConfigChange(unittest.TestCase):
    def test_trust_version_change_forces_purge_and_reauth(self) -> None:
        case = base_case(
            "trust", connections=["c1"], tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", candidates=["c1"])]),
                batch("b2", 10, [ev("e2", "auth_complete",
                                    connection_id="c1", generation=1,
                                    op_id="op-auth-c1-1",
                                    expected_config_version=1,
                                    lease_generation=1, outcome="ok")]),
                batch("b3", 20, [ev("e3", "complete_request",
                                    request_id="r1", outcome="success")]),
                batch("b4", 30, [
                    ev("e4", "set_trust_config_version", tenant="alpha",
                       version=2),
                    ev("e5", "submit_request", request_id="r2",
                       tenant="alpha", candidates=["c1"]),
                ]),
            ])
        result = run_ingest(case)
        step = schedule_of(result, "b4")[0]
        self.assertEqual(step["purge"], ["c1"])
        self.assertEqual(step["reused"], [])


class LeftToRightQueueOrder(unittest.TestCase):
    def test_earlier_request_gets_the_connection(self) -> None:
        case = base_case(
            "queue-order", connections=["c1"], tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [
                    ev("e1", "submit_request", request_id="first",
                       tenant="alpha", candidates=["c1"]),
                    ev("e2", "submit_request", request_id="second",
                       tenant="alpha", candidates=["c1"]),
                ]),
            ])
        result = run_ingest(case)
        conn = result["final_state"]["connections"]["c1"]
        self.assertEqual(conn["request_id"], "first")
        second = [s for s in schedule_of(result, "b1")
                  if s.get("request_id") == "second"][0]
        self.assertEqual(second["action"], "deferred")

    def test_tie_break_is_lexicographic_connection_id(self) -> None:
        case = base_case(
            "lex-order", connections=["c9", "c2", "c5"],
            tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", group_size=1,
                                   candidates=["c9", "c2", "c5"])]),
            ])
        result = run_ingest(case)
        step = schedule_of(result, "b1")[0]
        # All three are unauthenticated; with one needed, the lowest id wins.
        self.assertEqual(step["auth"], ["c2"])


class BlockedReasons(unittest.TestCase):
    def test_no_lease_blocks_pending_requests(self) -> None:
        case = base_case(
            "no-lease", config={"auto_lease": False},
            connections=["c1"], tenants=[tenant("alpha")],
            batches=[batch("b1", 0, [
                ev("e1", "submit_request", request_id="r1",
                   tenant="alpha", candidates=["c1"])])])
        result = run_ingest(case)
        self.assertEqual(result["final_state"]["requests"]["r1"]["blocked_reason"],
                         "NO_ACTIVE_LEASE")

    def test_busy_candidates_reported(self) -> None:
        case = base_case(
            "busy", connections=["c1"], tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", candidates=["c1"])]),
                batch("b2", 10, [ev("e2", "auth_complete",
                                    connection_id="c1", generation=1,
                                    op_id="op-auth-c1-1",
                                    expected_config_version=1,
                                    lease_generation=1, outcome="ok")]),
                batch("b3", 20, [ev("e3", "submit_request", request_id="r2",
                                    tenant="alpha", candidates=["c1"])]),
            ])
        result = run_ingest(case)
        self.assertEqual(result["final_state"]["requests"]["r2"]["blocked_reason"],
                         "CANDIDATES_BUSY")


class CancelReleasesConnection(unittest.TestCase):
    def test_cancel_before_use_returns_connection_to_pool(self) -> None:
        case = base_case(
            "cancel-release", connections=["c1"], tenants=[tenant("alpha")],
            batches=[
                batch("b1", 0, [ev("e1", "submit_request", request_id="r1",
                                   tenant="alpha", candidates=["c1"])]),
                batch("b2", 10, [ev("e2", "cancel_request", request_id="r1")]),
                batch("b3", 20, [ev("e3", "submit_request", request_id="r2",
                                    tenant="alpha", candidates=["c1"])]),
            ])
        result = run_ingest(case)
        self.assertEqual(result["final_state"]["receipts"]["r1"],
                         ["CANCELLED"])
        conn = result["final_state"]["connections"]["c1"]
        self.assertEqual(conn["request_id"], "r2")


if __name__ == "__main__":
    unittest.main()
