"""Command implementations: ingest, explain, recover, inspect, verify.

These glue the pure :class:`~solution.engine.Engine` to the durable
:class:`~solution.store.PoolStore`.  All time comes from the case; nothing here
reads the wall clock.
"""

from __future__ import annotations

from .crash import SimulatedCrash
from .engine import Engine
from .errors import (
    E_BATCH_CONFLICT,
    E_DB_STATE,
    E_SCHEMA_DUPLICATE,
    E_USAGE,
    SimError,
)
from .model import OP_PENDING_ACK, digest
from .state import new_state
from .store import PoolStore


class Runner:
    def __init__(self, case: dict, store: PoolStore | None = None) -> None:
        self.case = case
        self.config = case["config"]
        self.store = store
        self.state = self._load_or_init()
        self.engine = Engine(self.state, self.config)
        if not self._seeded():
            self.engine.bootstrap(case["connections"], case["tenants"])
            self._mark_seeded()

    # ------------------------------------------------------------------ setup
    def _load_or_init(self) -> dict:
        if self.store is not None and self.store.has_state():
            state = self.store.load(self.config)
            self._check_boot_config(state)
            return state
        return new_state(self.config)

    def _seeded(self) -> bool:
        return int(self.state.get("boot_seeded", 0)) == 1

    def _mark_seeded(self) -> None:
        self.state["boot_seeded"] = 1

    def _check_boot_config(self, state: dict) -> None:
        """Reject a case that disagrees with what the database already holds."""
        prior = state.get("boot_config")
        if prior is None:
            return
        keys = ("auth_ttl_ms", "lease_ttl_ms", "purge_cooldown_ms",
                "max_purge_attempts", "max_auth_attempts")
        diffs = {k: [prior.get(k), self.config.get(k)] for k in keys
                 if prior.get(k) != self.config.get(k)}
        if diffs:
            raise SimError(
                E_DB_STATE, "$.config",
                "case config disagrees with the database that already holds "
                "pool state", {"differences": diffs}, exit_code=3)

    # ----------------------------------------------------------------- ingest
    def ingest(self, *, crash: str | None = None,
               crash_batch: int = 0) -> dict:
        self.state.setdefault("boot_config", dict(self.config))
        self.state["boot_config"] = dict(self.config)
        self.state.setdefault("boot_seeded", 0)
        self._mark_seeded()

        batches = self.case["batches"]
        results: list[dict] = []
        already: dict[str, dict] = (self.store.applied_batches()
                                    if self.store else
                                    self.state.get("applied_results", {}))
        if self.store is None:
            for bid, rec in self.state.get("batches", {}).items():
                already.setdefault(bid, {"digest": rec["digest"],
                                         "result": rec.get("result")})
        seen_batch_ids: dict[str, int] = {}

        for bi, batch in enumerate(batches):
            bid = batch["batch_id"]
            if bid in seen_batch_ids:
                raise SimError(
                    E_SCHEMA_DUPLICATE, f"$.batches[{bi}].batch_id",
                    f"batch_id {bid!r} appears twice in the input case",
                    {"batch_id": bid})
            seen_batch_ids[bid] = bi

            batch_digest = digest({"at": batch["at"], "events": batch["events"]})
            if bid in already:
                prior = already[bid]
                if prior["digest"] != batch_digest:
                    raise SimError(
                        E_BATCH_CONFLICT, f"$.batches[{bi}].batch_id",
                        f"batch {bid!r} was already applied with different "
                        f"content", {"batch_id": bid,
                                     "previous_digest": prior["digest"],
                                     "current_digest": batch_digest},
                        exit_code=3)
                stored = prior.get("result")
                results.append(_replay_marker(bid, stored))
                continue

            do_crash = crash is not None and bi == crash_batch
            try:
                result = self.engine.apply_batch(batch, batch_index=bi)
            except SimulatedCrash:
                raise
            except SimError as exc:
                exc.extra.setdefault("batch_id", bid)
                exc.extra.setdefault("batch_index", bi)
                raise

            if do_crash and crash == "before-commit":
                # Die inside the transaction: nothing this batch touched may
                # become durable, and the in-memory state must be abandoned.
                raise SimulatedCrash(
                    stage="before_commit", batch_id=bid, committed=False,
                    note="the transaction never committed")

            if self.store is not None:
                self.store.commit_batch(
                    self.state, result, batch_digest,
                    crash_after_commit=(do_crash and crash == "after-commit"))
            else:
                self.state.setdefault("applied_results", {})[bid] = {
                    "digest": batch_digest, "result": result}
            results.append(result)

        return {
            "command": "ingest",
            "case_id": self.case.get("case_id"),
            "now": self.state["now"],
            "batches": results,
            "final_state": self.snapshot(),
        }

    # --------------------------------------------------------------- snapshot
    def snapshot(self) -> dict:
        s = self.state
        return {
            "now": s["now"],
            "lease": dict(s["lease"]),
            "connections": {
                cid: {
                    "generation": c["generation"],
                    "lifecycle": c["lifecycle"],
                    "context": c["context"],
                    "auth_expires_at": c["auth_expires_at"],
                    "request_id": c["request_id"],
                    "op": c["op"],
                    "cooldown_until": c["cooldown_until"],
                    "reuse_count": c["reuse_count"],
                }
                for cid, c in sorted(s["connections"].items())
            },
            "requests": {
                rid: {
                    "tenant": r["tenant"],
                    "state": r["state"],
                    "reserved": r["reserved"],
                    "group_size": r["group_size"],
                    "blocked_reason": r["blocked_reason"],
                    "terminal_receipt": r.get("terminal_receipt"),
                }
                for rid, r in sorted(s["requests"].items())
            },
            "receipts": {
                rid: [s["receipts"][i]["type"] for i in ids]
                for rid, ids in sorted(s["request_receipts"].items())
            },
            "pending_ack": sorted(
                op["op_id"] for op in s["operations"].values()
                if op["status"] == OP_PENDING_ACK),
            "exits": list(s["exits"]),
        }

    # ---------------------------------------------------------------- explain
    def materialize(self) -> None:
        """Apply any not-yet-applied batches so read commands see full state.

        Idempotent: batches already durable in the store are skipped, and the
        in-memory state is left untouched for already-applied ones.
        """
        self.ingest()

    def explain(self, request_id: str) -> dict:
        req = self.state["requests"].get(request_id)
        if req is None:
            raise SimError(E_USAGE, "--request",
                           f"unknown request {request_id!r}",
                           {"request_id": request_id})
        receipts = [self.state["receipts"][i] for i in
                    self.state["request_receipts"].get(request_id, [])]
        relevant_rejections = [
            r for r in self.state["rejections"]
            if r.get("target_id") == request_id
            or (r.get("target") == "connection"
                and r["target_id"] in req["candidates"])
        ]
        return {
            "command": "explain",
            "request_id": request_id,
            "tenant": req["tenant"],
            "final_state": req["state"],
            "group_size": req["group_size"],
            "candidates": req["candidates"],
            "reserved": req["reserved"],
            "transitions": req["transitions"],
            "blocked_history": req["blocked_history"],
            "receipts": receipts,
            "rejections": relevant_rejections,
            "late_completions": _late_completion_report(
                self.state, request_id),
            "connection_history": {
                cid: self.state["connections"][cid]["history"]
                for cid in req["candidates"]
                if cid in self.state["connections"]
            },
        }

    def explain_all(self) -> dict:
        return {
            "command": "explain",
            "requests": {rid: self.explain(rid)
                         for rid in self.state["submit_order"]},
        }

    # ---------------------------------------------------------------- recover
    def recover(self) -> dict:
        """Explain what survives a restart and prove no duplicate receipts."""
        terminal = {}
        duplicates = []
        for rid, ids in self.state["request_receipts"].items():
            types = [self.state["receipts"][i]["type"] for i in ids]
            term = [t for t in types if t in
                    ("SUCCEEDED", "TERMINATED", "CANCELLED", "FAILED")]
            if len(term) > 1:
                duplicates.append({"request_id": rid, "types": types})
            terminal[rid] = term[0] if term else None
        pending = sorted(op["op_id"] for op in self.state["operations"].values()
                         if op["status"] == OP_PENDING_ACK)
        return {
            "command": "recover",
            "now": self.state["now"],
            "batches_applied": sorted(self.state["batches"]),
            "terminal_receipts": terminal,
            "duplicate_terminal_receipts": duplicates,
            "pending_acknowledgement": pending,
            "lease": dict(self.state["lease"]),
        }

    # --------------------------------------------------------------- inspect
    def inspect(self) -> dict:
        return {
            "command": "inspect",
            "case_id": self.case.get("case_id"),
            "snapshot": self.snapshot(),
            "rejections": self.state["rejections"],
            "operations": {
                op_id: {
                    "kind": op["kind"],
                    "connection_id": op["connection_id"],
                    "generation": op["generation"],
                    "request_id": op["request_id"],
                    "lease_generation": op["lease_generation"],
                    "expected_config_version": op["expected_config_version"],
                    "status": op["status"],
                    "attempt": op["attempt"],
                    "messages": op["messages"],
                }
                for op_id, op in sorted(self.state["operations"].items())
            },
        }


def _replay_marker(bid: str, stored: dict | None) -> dict:
    if stored is None:
        return {"batch_id": bid, "replayed": True}
    marker = dict(stored)
    marker["replayed"] = True
    return marker


def _late_completion_report(state: dict, request_id: str) -> list[dict]:
    """Why any late completion touching this request changed nothing.

    Covers both direct request messages (a completion arriving after the
    request is terminal) and stale async messages on connections this request
    once used, which is where "the old message arrived too late" shows up.
    """
    out: list[dict] = []
    req = state["requests"].get(request_id)
    if req is None:
        return out
    candidates = set(req["candidates"])
    for rej in state["rejections"]:
        if rej["target"] == "request" and rej["target_id"] == request_id:
            out.append({
                "kind": "request_message",
                "code": rej["code"],
                "at": rej["at"],
                "batch": rej["batch"],
                "explanation": _explain_code(rej["code"], rej["detail"]),
            })
        elif rej["target"] == "connection" and rej["target_id"] in candidates:
            op_kind = rej["detail"].get("kind")
            if op_kind not in ("PURGE", "AUTH"):
                continue
            out.append({
                "kind": "stale_async_completion",
                "connection_id": rej["target_id"],
                "operation": op_kind,
                "op_id": rej["detail"].get("op_id"),
                "code": rej["code"],
                "at": rej["at"],
                "batch": rej["batch"],
                "explanation": _explain_code(rej["code"], rej["detail"]),
            })
    return out


def _explain_code(code: str, detail: dict) -> str:
    if code == "TERMINAL_STATE":
        return ("the request already reached a terminal state, so a later "
                "success/termination cannot create a second receipt")
    if code == "NOT_IN_USE":
        return ("the request was not holding a connection at that moment, so "
                "there was nothing to complete")
    if code == "STALE_GENERATION":
        return ("the connection's generation has advanced, so this completion "
                "describes a use cycle that already ended")
    if code == "STALE_OPERATION":
        return ("the one-time operation id is neither the connection's current "
                "operation nor an unsettled one; it was already retired or "
                "superseded")
    if code == "STALE_LEASE":
        return ("this completion was issued under a control-lease generation "
                "that is no longer current, e.g. a process that has since "
                "been taken over")
    if code == "STALE_EXPECTED_VERSION":
        return ("the expected configuration version no longer matches, so the "
                "result would install an out-of-date trust configuration")
    if code == "UNKNOWN_OP":
        return "no operation with this id was ever issued"
    return "rejected"


# ---------------------------------------------------------------- commands
def cmd_ingest(case: dict, db: str | None, *, crash: str | None = None,
               crash_batch: int = 0) -> dict:
    if db is not None:
        with PoolStore(db).open() as store:
            runner = Runner(case, store)
            return runner.ingest(crash=crash, crash_batch=crash_batch)
    return Runner(case, None).ingest(crash=crash, crash_batch=crash_batch)


def cmd_explain(case: dict, db: str | None, *, request_id: str | None) -> dict:
    with _open(case, db) as runner:
        runner.materialize()
        if request_id is None:
            return runner.explain_all()
        return runner.explain(request_id)


def cmd_recover(case: dict, db: str | None) -> dict:
    # Recover deliberately does NOT materialize: it reports only what a fresh
    # process can already see in the durable store, so a crash mid-case stays
    # observable.
    with _open(case, db) as runner:
        return runner.recover()


def cmd_inspect(case: dict, db: str | None) -> dict:
    with _open(case, db) as runner:
        runner.materialize()
        return runner.inspect()


def cmd_replay(case: dict, db: str | None) -> dict:
    """Run the whole case into a scratch in-memory database twice.

    Two independent runs must agree byte-for-byte, which is the determinism
    check the README promises.
    """
    first = Runner(case, None).ingest()
    second = Runner(case, None).ingest()
    same = digest(first) == digest(second)
    return {
        "command": "replay",
        "deterministic": same,
        "digest": digest(first),
        "runs": 2,
        "final_state": first["final_state"],
    }


class _open:
    def __init__(self, case: dict, db: str | None) -> None:
        self.case = case
        self.db = db
        self.runner: Runner | None = None

    def __enter__(self) -> Runner:
        if self.db is not None:
            self.store = PoolStore(self.db).open()
            self.runner = Runner(self.case, self.store)
        else:
            self.store = None
            self.runner = Runner(self.case, None)
        return self.runner

    def __exit__(self, *exc) -> None:
        if self.store is not None:
            self.store.close()
