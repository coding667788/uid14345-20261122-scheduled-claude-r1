"""Schema validation: stable error codes and exact JSON field paths."""

from __future__ import annotations

import json
import unittest

from support import ROOT, base_case, batch, ev, tenant

from solution import schema
from solution.errors import SimError


def _err(raw) -> SimError:
    with unittest.TestCase().assertRaises(SimError) as ctx:
        schema.normalize_case(raw)
    return ctx.exception


def _minimal(**over) -> dict:
    raw = {
        "sim_version": 1,
        "connections": [{"connection_id": "c1"}],
        "tenants": [tenant("alpha")],
        "batches": [],
    }
    raw.update(over)
    return raw


class JsonParse(unittest.TestCase):
    def test_bad_json_reports_line_and_column(self) -> None:
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "bad.json")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write('{"sim_version": 1, ')
            with self.assertRaises(SimError) as ctx:
                schema.load_case(path)
        err = ctx.exception
        self.assertEqual(err.code, "E_JSON_PARSE")
        self.assertEqual(err.path, "$")
        self.assertIn("line", err.extra)

    def test_non_utf8_is_reported(self) -> None:
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "latin1.json")
            with open(path, "wb") as fh:
                fh.write(b'{"case_id": "\xe9"}')
            with self.assertRaises(SimError) as ctx:
                schema.load_case(path)
        self.assertEqual(ctx.exception.code, "E_JSON_PARSE")


class StructuralErrors(unittest.TestCase):
    def test_missing_required_field_names_path(self) -> None:
        err = _err({"sim_version": 1, "connections": [{}],
                    "tenants": [], "batches": []})
        self.assertEqual(err.code, "E_SCHEMA_MISSING")
        self.assertEqual(err.path, "$.connections[0].connection_id")

    def test_wrong_type_names_expected_and_got(self) -> None:
        err = _err({"sim_version": 1, "connections": "c1",
                    "tenants": [], "batches": []})
        self.assertEqual(err.code, "E_SCHEMA_TYPE")
        self.assertEqual(err.path, "$.connections")
        self.assertEqual(err.extra["expected"], "array")

    def test_unknown_event_type_lists_allowed(self) -> None:
        err = _err(_minimal(batches=[batch("b", 0, [
            {"event_id": "e", "type": "teleport"}])]))
        self.assertEqual(err.code, "E_SCHEMA_ENUM")
        self.assertEqual(err.path, "$.batches[0].events[0].type")
        self.assertIn("submit_request", err.extra["allowed"])

    def test_unknown_field_is_flagged_with_its_name(self) -> None:
        err = _err(_minimal(batches=[batch("b", 0, [
            ev("e", "tick", surprise=1)])]))
        self.assertEqual(err.code, "E_SCHEMA_UNKNOWN_KEY")
        self.assertEqual(err.path, "$.batches[0].events[0].surprise")

    def test_duplicate_connection_id_points_at_second(self) -> None:
        err = _err({"sim_version": 1,
                    "connections": [{"connection_id": "c1"},
                                    {"connection_id": "c1"}],
                    "tenants": [], "batches": []})
        self.assertEqual(err.code, "E_SCHEMA_DUPLICATE")
        self.assertEqual(err.path, "$.connections[1].connection_id")

    def test_group_size_larger_than_candidates(self) -> None:
        err = _err(_minimal(batches=[batch("b", 0, [
            ev("e", "submit_request", request_id="r", tenant="alpha",
               candidates=["c1"], group_size=2)])]))
        self.assertEqual(err.code, "E_SCHEMA_RANGE")
        self.assertEqual(err.path, "$.batches[0].events[0].group_size")

    def test_batch_times_must_not_go_backwards(self) -> None:
        err = _err(_minimal(batches=[
            batch("b1", 10, [ev("e1", "tick")]),
            batch("b2", 5, [ev("e2", "tick")]),
        ]))
        self.assertEqual(err.code, "E_SCHEMA_RANGE")
        self.assertEqual(err.path, "$.batches[1].at")

    def test_event_time_must_match_batch_time(self) -> None:
        err = _err(_minimal(batches=[batch("b", 10, [
            ev("e", "tick", at=12)])]))
        self.assertEqual(err.code, "E_SCHEMA_RANGE")
        self.assertEqual(err.path, "$.batches[0].events[0].at")

    def test_duplicate_event_id_inside_a_batch(self) -> None:
        err = _err(_minimal(batches=[batch("b", 0, [
            ev("e", "tick"), ev("e", "tick")])]))
        self.assertEqual(err.code, "E_SCHEMA_DUPLICATE")
        self.assertEqual(err.path, "$.batches[0].events[1].event_id")

    def test_duplicate_batch_id(self) -> None:
        err = _err(_minimal(batches=[
            batch("b", 0, [ev("e1", "tick")]),
            batch("b", 0, [ev("e2", "tick")]),
        ]))
        self.assertEqual(err.code, "E_SCHEMA_DUPLICATE")
        self.assertEqual(err.path, "$.batches[1].batch_id")

    def test_empty_candidates_rejected(self) -> None:
        err = _err(_minimal(batches=[batch("b", 0, [
            ev("e", "submit_request", request_id="r", tenant="alpha",
               candidates=[])])]))
        self.assertEqual(err.code, "E_SCHEMA_RANGE")
        self.assertEqual(err.path, "$.batches[0].events[0].candidates")

    def test_flat_events_are_grouped_by_time(self) -> None:
        raw = {
            "sim_version": 1, "clock": 0,
            "connections": [{"connection_id": "c1"}],
            "tenants": [tenant("alpha")],
            "events": [
                ev("e1", "submit_request", request_id="r1", tenant="alpha",
                   candidates=["c1"], at=0),
                {"event_id": "e2", "type": "tick", "at": 5},
            ],
        }
        case = schema.normalize_case(raw)
        self.assertEqual([b["at"] for b in case["batches"]], [0, 5])
        self.assertEqual(len(case["batches"][0]["events"]), 1)


class References(unittest.TestCase):
    def test_unknown_tenant_is_a_reference_error_with_event_path(self) -> None:
        from solution.runner import cmd_ingest
        case = base_case(
            "ref", connections=["c1"], tenants=[tenant("alpha")],
            batches=[batch("b", 0, [
                ev("e", "submit_request", request_id="r", tenant="ghost",
                   candidates=["c1"])])])
        with self.assertRaises(SimError) as ctx:
            cmd_ingest(case, None)
        err = ctx.exception
        self.assertEqual(err.code, "E_REFERENCE")
        self.assertEqual(err.path, "$.batches[0].events[0].tenant")

    def test_unknown_candidate_connection_is_a_reference_error(self) -> None:
        from solution.runner import cmd_ingest
        case = base_case(
            "ref2", connections=["c1"], tenants=[tenant("alpha")],
            batches=[batch("b", 0, [
                ev("e", "submit_request", request_id="r", tenant="alpha",
                   candidates=["c9"])])])
        with self.assertRaises(SimError) as ctx:
            cmd_ingest(case, None)
        self.assertEqual(ctx.exception.code, "E_REFERENCE")

    def test_unknown_request_for_cancel(self) -> None:
        from solution.runner import cmd_ingest
        case = base_case(
            "ref3", connections=[], tenants=[],
            batches=[batch("b", 0, [
                ev("e", "cancel_request", request_id="nope")])])
        with self.assertRaises(SimError) as ctx:
            cmd_ingest(case, None)
        self.assertEqual(ctx.exception.code, "E_REFERENCE")
        self.assertEqual(ctx.exception.path, "$.batches[0].events[0].request_id")


if __name__ == "__main__":
    unittest.main()
