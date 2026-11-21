"""SQLite persistence for the durable pool.

One batch is one transaction.  Inside it we write, together:

* the lease (inside the state document *and* mirrored in ``lease``),
* the transactional pending-acknowledgement list,
* stable receipts keyed by request,
* the applied-batch log with its content digest.

If the process dies before COMMIT nothing is visible; if it dies after COMMIT
the batch is durable even though the caller never saw the response, and a
later ingest of the same batch returns the stored result and the same receipt.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager

from .errors import E_DB_STATE, E_USAGE, SimError
from .model import OP_PENDING_ACK
from .state import new_state

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_state (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    doc   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_batch (
    batch_id TEXT PRIMARY KEY,
    at       INTEGER NOT NULL,
    digest   TEXT NOT NULL,
    result   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipt (
    receipt_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    tenant     TEXT NOT NULL,
    type       TEXT NOT NULL,
    terminal   INTEGER NOT NULL,
    digest     TEXT NOT NULL,
    at         INTEGER NOT NULL,
    body       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS receipt_by_request ON receipt(request_id);

CREATE TABLE IF NOT EXISTS pending_ack (
    op_id                   TEXT PRIMARY KEY,
    connection_id           TEXT NOT NULL,
    generation              INTEGER NOT NULL,
    kind                    TEXT NOT NULL,
    request_id              TEXT NOT NULL,
    tenant                  TEXT NOT NULL,
    lease_generation        INTEGER NOT NULL,
    expected_config_version INTEGER NOT NULL,
    status                  TEXT NOT NULL,
    since                   INTEGER
);

CREATE TABLE IF NOT EXISTS lease_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder     TEXT,
    generation INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    acquired_at INTEGER
);
"""


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _loads(text: str):
    return json.loads(text)


class PoolStore:
    """A thin, explicit wrapper over one SQLite file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------- lifecycle
    def open(self, *, create: bool = True) -> "PoolStore":
        if not create and not os.path.exists(self.path):
            raise SimError(E_USAGE, "--db",
                           f"database {self.path!r} does not exist",
                           {"db": self.path})
        try:
            self._conn = sqlite3.connect(self.path)
        except sqlite3.Error as exc:
            raise SimError(E_DB_STATE, "--db",
                           f"cannot open database: {exc}", {"db": self.path})
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "PoolStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("store is not open")
        return self._conn

    # -------------------------------------------------------------- read API
    def has_state(self) -> bool:
        row = self.conn.execute("SELECT 1 FROM pool_state WHERE id=1").fetchone()
        return row is not None

    def load(self, config: dict) -> dict:
        row = self.conn.execute("SELECT doc FROM pool_state WHERE id=1").fetchone()
        if row is None:
            return new_state(config)
        state = _loads(row["doc"])
        if int(state.get("version", 0)) != SCHEMA_VERSION:
            raise SimError(E_DB_STATE, "--db",
                           "database state version is not supported",
                           {"found": state.get("version"),
                            "supported": SCHEMA_VERSION})
        return state

    def applied_batches(self) -> dict[str, dict]:
        rows = self.conn.execute(
            "SELECT batch_id, at, digest, result FROM applied_batch").fetchall()
        return {r["batch_id"]: {"at": r["at"], "digest": r["digest"],
                                "result": _loads(r["result"])} for r in rows}

    def receipts(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT body FROM receipt ORDER BY at, receipt_id").fetchall()
        return [_loads(r["body"]) for r in rows]

    def pending_acks(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM pending_ack ORDER BY op_id").fetchall()
        return [dict(r) for r in rows]

    def lease_row(self) -> dict | None:
        row = self.conn.execute("SELECT * FROM lease_state WHERE id=1").fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------- write API
    @contextmanager
    def transaction(self):
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def commit_batch(self, state: dict, batch_result: dict,
                     batch_digest: str, *,
                     crash_after_commit: bool = False) -> None:
        """Atomically persist state + lease + pending acks + receipts + log.

        ``crash_after_commit`` commits and then raises, emulating a process
        that dies after the write but before the response reaches the caller.
        """
        from .crash import SimulatedCrash

        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO pool_state(id, doc) VALUES(1, ?) "
                "ON CONFLICT(id) DO UPDATE SET doc=excluded.doc",
                (_dumps(state),))
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),))

            lease = state["lease"]
            conn.execute(
                "INSERT INTO lease_state(id, holder, generation, expires_at, "
                "acquired_at) VALUES(1, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET holder=excluded.holder, "
                "generation=excluded.generation, "
                "expires_at=excluded.expires_at, "
                "acquired_at=excluded.acquired_at",
                (lease.get("holder"), int(lease.get("generation", 0)),
                 int(lease.get("expires_at", 0)), lease.get("acquired_at")))

            conn.execute("DELETE FROM pending_ack")
            for op in sorted(state["operations"].values(),
                             key=lambda o: o["op_id"]):
                if op["status"] != OP_PENDING_ACK:
                    continue
                conn.execute(
                    "INSERT INTO pending_ack(op_id, connection_id, generation,"
                    " kind, request_id, tenant, lease_generation,"
                    " expected_config_version, status, since)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (op["op_id"], op["connection_id"], int(op["generation"]),
                     op["kind"], op["request_id"], op["tenant"],
                     int(op["lease_generation"]),
                     int(op["expected_config_version"]), op["status"],
                     op.get("pending_ack_since")))

            for rid, ids in state["request_receipts"].items():
                for receipt_id in ids:
                    body = state["receipts"][receipt_id]
                    conn.execute(
                        "INSERT INTO receipt(receipt_id, request_id, tenant,"
                        " type, terminal, digest, at, body)"
                        " VALUES(?,?,?,?,?,?,?,?)"
                        " ON CONFLICT(receipt_id) DO UPDATE SET"
                        " body=excluded.body",
                        (receipt_id, body["request_id"], body["tenant"],
                         body["type"], 1 if body["terminal"] else 0,
                         body["digest"], int(body["at"]), _dumps(body)))

            conn.execute(
                "INSERT INTO applied_batch(batch_id, at, digest, result)"
                " VALUES(?,?,?,?)", (batch_result["batch_id"],
                                     int(batch_result["at"]), batch_digest,
                                     _dumps(batch_result)))

        if crash_after_commit:
            raise SimulatedCrash(
                stage="after_commit", batch_id=batch_result["batch_id"],
                committed=True,
                note="batch is durable but the response was lost")

    def stored_batch_result(self, batch_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT result FROM applied_batch WHERE batch_id=?",
            (batch_id,)).fetchone()
        return _loads(row["result"]) if row else None

    def receipt_for(self, request_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT body FROM receipt WHERE request_id=? AND terminal=1 "
            "ORDER BY at DESC, receipt_id DESC LIMIT 1",
            (request_id,)).fetchone()
        return _loads(row["body"]) if row else None

