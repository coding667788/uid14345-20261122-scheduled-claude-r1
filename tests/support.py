"""Shared helpers for the test suite."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CASES = ROOT / "cases"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from solution.schema import load_case  # noqa: E402


def load(name: str) -> dict:
    return load_case(str(CASES / name))


def run_ingest(case: dict, db: str | None = None) -> dict:
    from solution.runner import cmd_ingest
    return cmd_ingest(case, db)


def base_case(case_id: str, *, config: dict | None = None,
              connections: list[str] | None = None,
              tenants: list[dict] | None = None,
              batches: list[dict] | None = None) -> dict:
    """Build a normalised case dict through the real schema validator."""
    raw = {
        "sim_version": 1,
        "case_id": case_id,
        "config": {
            "auth_ttl_ms": 1000,
            "lease_ttl_ms": 5000,
            "purge_cooldown_ms": 0,
            "max_purge_attempts": 3,
            "max_auth_attempts": 3,
            "initial_process": "p0",
            "auto_lease": True,
            **(config or {}),
        },
        "connections": [{"connection_id": c} for c in (connections or [])],
        "tenants": tenants or [],
        "batches": batches or [],
    }
    from solution.schema import normalize_case
    return normalize_case(raw)


def tenant(name: str, peer: int = 1, trust: int = 1,
           purpose: str = "interactive") -> dict:
    return {"tenant": name, "peer_identity_version": peer,
            "trust_config_version": trust, "purpose_level": purpose}


def batch(bid: str, at: int, events: list[dict]) -> dict:
    return {"batch_id": bid, "at": at, "events": events}


def ev(event_id: str, etype: str, **fields) -> dict:
    return {"event_id": event_id, "type": etype, **fields}


class TempDb(unittest.TestCase):
    """Mixin giving each test a scratch SQLite path."""

    def setUp(self) -> None:  # noqa: D401
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = os.path.join(self._tmp.name, "pool.sqlite")

    def cleanup_db(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db_path + suffix)
            except FileNotFoundError:
                pass


def events_of(result: dict, batch_id: str) -> list[dict]:
    for b in result["batches"]:
        if b["batch_id"] == batch_id:
            return b.get("events", [])
    raise AssertionError(f"batch {batch_id} not in result")


def outcome(result: dict, batch_id: str, event_id: str) -> dict:
    for e in events_of(result, batch_id):
        if e["event_id"] == event_id:
            return e["outcome"]
    raise AssertionError(f"event {event_id} not in batch {batch_id}")


def schedule_of(result: dict, batch_id: str) -> list[dict]:
    for b in result["batches"]:
        if b["batch_id"] == batch_id:
            return b.get("schedule", [])
    raise AssertionError(f"batch {batch_id} not in result")
