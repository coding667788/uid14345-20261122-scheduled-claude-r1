"""The deterministic pool engine.

``Engine.apply_batch`` is a pure function of ``(state, batch, config)``: it
reads only the batch's logical time and never touches the wall clock, the
filesystem or the network.  Replaying the same logical input therefore yields
byte-identical output.

Model
-----
* Simulated worker processes contend for one *control lease* carrying a
  holder, a generation and a deadline.  Mutating the pool requires holding it;
  a message issued under an older lease generation cannot settle an operation.
* A batch is the unit of atomicity: events fold in input order, then one
  scheduling pass runs, then the state document is written once.
* A request names candidate connections and a group size.  It is promoted only
  when its **whole** group can be committed in one step, so reading a partial
  match never parks a connection and starves another queue.
* Establishing a security context on a stale connection is a two-step
  asynchronous dance: PURGE clears the old context, then AUTH installs the
  current one.  Both completions carry the connection generation, a one-time
  operation id, the control-lease generation and the expected config version.
"""

from __future__ import annotations

import copy

from .errors import (
    E_BATCH_CONFLICT,
    E_REFERENCE,
    E_SCHEMA_DUPLICATE,
    SimError,
)
from .model import (
    B_BUSY,
    B_COOLDOWN,
    B_INSUFFICIENT,
    B_NO_LEASE,
    B_PURGE_FAILED,
    C_LEASE_NOT_HELD,
    C_NOT_IN_USE,
    C_STALE_EXPECTED_VERSION,
    C_STALE_GENERATION,
    C_STALE_LEASE,
    C_STALE_OPERATION,
    C_TERMINAL_STATE,
    C_UNKNOWN_OP,
    L_AUTHENTICATING,
    L_CLOSED,
    L_IDLE,
    L_IN_USE,
    L_PENDING_PURGE,
    L_RESERVED,
    OP_AUTH,
    OP_IN_FLIGHT,
    OP_PENDING_ACK,
    OP_PURGE,
    OP_SETTLED,
    RC_CANCELLED,
    RC_FAILED,
    RC_RESERVED,
    RC_SUCCEEDED,
    RC_TERMINATED,
    R_CANCELLED,
    R_FAILED,
    R_IN_USE,
    R_PENDING,
    R_RESERVED,
    R_SUCCEEDED,
    R_TERMINATED,
    TERMINAL_RECEIPTS,
    TERMINAL_STATES,
    context_of,
    contexts_equal,
    digest,
)
from .state import new_connection, new_request, new_tenant


class Engine:
    def __init__(self, state: dict, config: dict) -> None:
        self.state = state
        self.config = config
        self.now: int = int(state["now"])
        self.batch_id: str | None = None
        self.batch_index: int = 0
        self.event_id: str | None = None
        self.event_path: str | None = None
        self.replayed = False
        self.new_rejections: list[dict] = []

    # ================================================================= setup
    def bootstrap(self, connections: list[dict], tenants: list[dict]) -> None:
        for t in tenants:
            rec = self.state["tenants"].get(t["tenant"])
            if rec is None:
                rec = new_tenant(t["tenant"], t["peer_identity_version"],
                                 t["trust_config_version"], t["purpose_level"])
                self.state["tenants"][t["tenant"]] = rec
            else:
                rec["peer_identity_version"] = int(t["peer_identity_version"])
                rec["trust_config_version"] = int(t["trust_config_version"])
                rec["purpose_level"] = t["purpose_level"]
        for c in connections:
            cid = c["connection_id"]
            if cid not in self.state["connections"]:
                self.state["connections"][cid] = new_connection(
                    cid, int(c["generation"]))
        initial = self.config.get("initial_process")
        if initial and self.config.get("auto_lease") \
                and not self.state["initial_lease_done"]:
            self._install_lease(initial)
            self.state["initial_lease_done"] = True

    # ============================================================ accessors
    def _tenant(self, tenant: str, path: str) -> dict:
        rec = self.state["tenants"].get(tenant)
        if rec is None:
            raise SimError(E_REFERENCE, path, f"unknown tenant {tenant!r}",
                           {"tenant": tenant})
        return rec

    def _conn(self, cid: str, path: str) -> dict:
        rec = self.state["connections"].get(cid)
        if rec is None:
            raise SimError(E_REFERENCE, path, f"unknown connection {cid!r}",
                           {"connection_id": cid})
        return rec

    def _request(self, rid: str, path: str) -> dict:
        rec = self.state["requests"].get(rid)
        if rec is None:
            raise SimError(E_REFERENCE, path, f"unknown request {rid!r}",
                           {"request_id": rid})
        return rec

    # ================================================================ lease
    def _live_lease(self) -> dict | None:
        lease = self.state["lease"]
        if lease.get("holder") is None:
            return None
        if self.now > int(lease["expires_at"]):
            return None
        return lease

    def _lease_gen(self) -> int:
        return int(self.state["lease"].get("generation", 0))

    def _lease_snapshot(self) -> dict:
        lease = self.state["lease"]
        return {"holder": lease.get("holder"),
                "generation": int(lease.get("generation", 0)),
                "expires_at": lease.get("expires_at"),
                "active": self._live_lease() is not None}

    def _install_lease(self, process_id: str) -> None:
        lease = self.state["lease"]
        previous = lease.get("holder")
        if previous is not None and previous != process_id:
            self._demote_owned(previous, reason="takeover")
        lease["generation"] = int(lease.get("generation", 0)) + 1
        lease["holder"] = process_id
        lease["acquired_at"] = self.now
        lease["expires_at"] = self.now + int(self.config["lease_ttl_ms"])

    def _demote_owned(self, process_id: str, *, reason: str) -> list[str]:
        """Turn a departed process's in-flight operations into pending-ack."""
        demoted: list[str] = []
        for op_id in sorted(self.state["operations"]):
            op = self.state["operations"][op_id]
            if op["process_id"] == process_id and op["status"] == OP_IN_FLIGHT:
                op["status"] = OP_PENDING_ACK
                op["pending_ack_since"] = self.now
                op["pending_ack_reason"] = reason
                demoted.append(op_id)
        return demoted

    def _ev_acquire_lease(self, ev: dict) -> dict:
        process_id = ev["process_id"]
        live = self._live_lease()
        if live is not None and live["holder"] == process_id:
            self.state["lease"]["expires_at"] = (
                self.now + int(self.config["lease_ttl_ms"]))
            return {"process_id": process_id, "result": "renewed",
                    "lease": self._lease_snapshot()}
        if live is not None:
            self._reject(C_LEASE_NOT_HELD, "lease", process_id, {
                "holder": live["holder"],
                "expires_at": live["expires_at"], "now": self.now})
            return {"process_id": process_id, "result": "rejected",
                    "rejected": C_LEASE_NOT_HELD,
                    "lease": self._lease_snapshot()}
        takeover = self.state["lease"].get("holder") is None
        self._install_lease(process_id)
        return {"process_id": process_id,
                "result": "takeover" if takeover else "acquired",
                "previous_generation": self._lease_gen() - 1,
                "lease": self._lease_snapshot()}

    def _ev_renew_lease(self, ev: dict) -> dict:
        live = self._live_lease()
        if live is None or live["holder"] != ev["process_id"]:
            self._reject(C_LEASE_NOT_HELD, "lease", ev["process_id"],
                         {"lease": self._lease_snapshot()})
            return {"process_id": ev["process_id"], "result": "rejected",
                    "rejected": C_LEASE_NOT_HELD,
                    "lease": self._lease_snapshot()}
        self.state["lease"]["expires_at"] = (
            self.now + int(self.config["lease_ttl_ms"]))
        return {"process_id": ev["process_id"], "result": "renewed",
                "lease": self._lease_snapshot()}

    def _ev_release_lease(self, ev: dict) -> dict:
        lease = self.state["lease"]
        released = lease.get("holder") == ev["process_id"]
        if released:
            lease["holder"] = None
        return {"process_id": ev["process_id"], "released": released,
                "lease": self._lease_snapshot()}

    # ============================================================ operations
    def _new_op(self, kind: str, conn: dict, req: dict, *, attempt: int,
                expected_version: int) -> dict:
        self.state["op_counter"] = int(self.state["op_counter"]) + 1
        op_id = "op-%s-%s-%d" % (kind.lower(), conn["connection_id"],
                                 self.state["op_counter"])
        op = {
            "op_id": op_id,
            "kind": kind,
            "connection_id": conn["connection_id"],
            "generation": int(conn["generation"]),
            "request_id": req["request_id"],
            "tenant": req["tenant"],
            "lease_generation": self._lease_gen(),
            "process_id": (self._live_lease() or {}).get("holder"),
            "expected_config_version": int(expected_version),
            "status": OP_IN_FLIGHT,
            "attempt": int(attempt),
            "issued_at": self.now,
            "pending_ack_since": None,
            "settled_at": None,
            "outcome": None,
            "messages": [],
        }
        self.state["operations"][op_id] = op
        conn["op"] = op_id
        conn["op_attempts"][kind] = int(attempt)
        return op

    def _settle(self, op: dict, outcome: str) -> None:
        op["status"] = OP_SETTLED
        op["settled_at"] = self.now
        op["outcome"] = outcome

    # ============================================================= receipts
    def _emit(self, req: dict, rtype: str, *, reason: str,
              detail: dict | None = None) -> dict:
        """Append a receipt; never a second terminal receipt for one request."""
        ids = self.state["request_receipts"].setdefault(req["request_id"], [])
        for rid in ids:
            prior = self.state["receipts"][rid]
            if prior["type"] == rtype:
                return prior
            if rtype in TERMINAL_RECEIPTS and prior["type"] in TERMINAL_RECEIPTS:
                return prior
        self.state["receipt_counter"] = int(self.state["receipt_counter"]) + 1
        receipt = {
            "receipt_id": "rc-%04d" % self.state["receipt_counter"],
            "request_id": req["request_id"],
            "tenant": req["tenant"],
            "type": rtype,
            "at": self.now,
            "batch": self.batch_id,
            "event": self.event_id,
            "reason": reason,
            "detail": detail or {},
            "terminal": rtype in TERMINAL_RECEIPTS,
        }
        receipt["digest"] = digest({
            "request_id": receipt["request_id"],
            "tenant": receipt["tenant"],
            "type": receipt["type"],
            "detail": receipt["detail"],
        })
        self.state["receipts"][receipt["receipt_id"]] = receipt
        ids.append(receipt["receipt_id"])
        return receipt

    def last_receipt(self, rid: str) -> dict | None:
        ids = self.state["request_receipts"].get(rid) or []
        return self.state["receipts"][ids[-1]] if ids else None

    # ========================================================= transitions
    def _transition(self, req: dict, to_state: str, *, reason: str,
                    detail: dict | None = None) -> None:
        req["transitions"].append({
            "seq": len(req["transitions"]) + 1,
            "at": self.now,
            "batch": self.batch_id,
            "event": self.event_id,
            "from": req["state"],
            "to": to_state,
            "reason": reason,
            "detail": detail or {},
        })
        req["state"] = to_state

    def _note(self, conn: dict, kind: str, **detail) -> None:
        conn["history"].append({"at": self.now, "batch": self.batch_id,
                                "event": self.event_id, "kind": kind,
                                **detail})

    def _reject(self, code: str, target: str, target_id: str,
                detail: dict) -> dict:
        rec = {"code": code, "at": self.now, "batch": self.batch_id,
               "event": self.event_id, "target": target,
               "target_id": target_id, "detail": detail}
        self.state["rejections"].append(rec)
        self.new_rejections.append(rec)
        return rec

    # ============================================================ batch API
    def apply_batch(self, batch: dict, *, batch_index: int = 0) -> dict:
        """Apply one batch atomically.

        The state is snapshotted first; any error (a batch conflict, an unknown
        reference, a rejected mid-batch event) restores the snapshot, so the
        pool never keeps a half-applied batch in memory.
        """
        snapshot = copy.deepcopy(self.state)
        try:
            return self._apply_batch(batch, batch_index=batch_index)
        except BaseException:
            self.state.clear()
            self.state.update(snapshot)
            raise

    def _apply_batch(self, batch: dict, *, batch_index: int = 0) -> dict:
        self.batch_index = batch_index
        self.batch_id = batch["batch_id"]
        self.now = int(batch["at"])
        self.state["now"] = self.now
        self.new_rejections = []

        batch_digest = digest({"at": batch["at"], "events": batch["events"]})
        prior = self.state["batches"].get(self.batch_id)
        if prior is not None:
            if prior["digest"] != batch_digest:
                raise SimError(
                    E_BATCH_CONFLICT, f"$.batches[{batch_index}].batch_id",
                    f"batch {self.batch_id!r} was already applied with "
                    f"different content",
                    {"batch_id": self.batch_id,
                     "previous_digest": prior["digest"],
                     "current_digest": batch_digest}, exit_code=3)
            self.replayed = True
            result = dict(prior["result"])
            result["replayed"] = True
            return result

        self.replayed = False
        seen: dict[str, int] = {}
        event_results: list[dict] = []
        for i, ev in enumerate(batch["events"]):
            self.event_id = ev["event_id"]
            self.event_path = f"$.batches[{batch_index}].events[{i}]"
            if ev["event_id"] in seen:
                raise SimError(
                    E_SCHEMA_DUPLICATE, self.event_path + ".event_id",
                    f"event_id {ev['event_id']!r} appears twice in one batch",
                    {"event_id": ev["event_id"]})
            seen[ev["event_id"]] = i
            handler = getattr(self, "_ev_" + ev["type"])
            event_results.append({"event_id": ev["event_id"],
                                  "type": ev["type"],
                                  "outcome": handler(ev)})

        schedule = self._schedule()
        result = {
            "batch_id": self.batch_id,
            "at": self.now,
            "replayed": False,
            "events": event_results,
            "schedule": schedule,
            "rejections": [r["code"] for r in self.new_rejections],
        }
        self.state["batches"][self.batch_id] = {
            "batch_id": self.batch_id, "at": self.now,
            "digest": batch_digest,
            "events": [ev["event_id"] for ev in batch["events"]],
            "result": result,
        }
        return result

    # ============================================================== events
    def _ev_register_connection(self, ev: dict) -> dict:
        cid = ev["connection_id"]
        gen = int(ev["generation"])
        existing = self.state["connections"].get(cid)
        if existing is None:
            self.state["connections"][cid] = new_connection(cid, gen)
            return {"result": "created", "connection_id": cid,
                    "generation": gen}
        if gen == int(existing["generation"]):
            return {"result": "unchanged", "connection_id": cid,
                    "generation": gen}
        if gen < int(existing["generation"]):
            self._reject(C_STALE_GENERATION, "connection", cid,
                         {"given": gen, "current": existing["generation"]})
            return {"result": "ignored", "connection_id": cid,
                    "rejected": C_STALE_GENERATION,
                    "current_generation": existing["generation"]}
        self.state["connections"][cid] = new_connection(cid, gen)
        return {"result": "replaced", "connection_id": cid, "generation": gen}

    def _ev_register_tenant(self, ev: dict) -> dict:
        tenant = ev["tenant"]
        rec = self.state["tenants"].get(tenant)
        if rec is None:
            rec = new_tenant(tenant, ev["peer_identity_version"],
                             ev["trust_config_version"], ev["purpose_level"])
            self.state["tenants"][tenant] = rec
        else:
            self._apply_tenant_config(rec, ev["peer_identity_version"],
                                      ev["trust_config_version"],
                                      ev["purpose_level"])
            rec["revoked"] = False
        return {"tenant": tenant, "version": rec["version"],
                "context": self._context_for(tenant)}

    def _apply_tenant_config(self, rec: dict, peer: int, trust: int,
                             purpose: str) -> bool:
        changed = (rec["peer_identity_version"] != int(peer)
                   or rec["trust_config_version"] != int(trust)
                   or rec["purpose_level"] != purpose
                   or rec.get("revoked"))
        rec["peer_identity_version"] = int(peer)
        rec["trust_config_version"] = int(trust)
        rec["purpose_level"] = purpose
        rec["revoked"] = False
        rec["revision"] = int(rec.get("revision", 0)) + 1
        if changed:
            rec["version"] = int(rec.get("version", 1)) + 1
        return changed

    def _ev_set_peer_identity_version(self, ev: dict) -> dict:
        rec = self._tenant(ev["tenant"], self.event_path + ".tenant")
        changed = self._apply_tenant_config(rec, ev["version"],
                                            rec["trust_config_version"],
                                            rec["purpose_level"])
        return {"tenant": ev["tenant"],
                "peer_identity_version": rec["peer_identity_version"],
                "version": rec["version"], "changed": changed}

    def _ev_set_trust_config_version(self, ev: dict) -> dict:
        rec = self._tenant(ev["tenant"], self.event_path + ".tenant")
        changed = self._apply_tenant_config(rec,
                                            rec["peer_identity_version"],
                                            ev["version"],
                                            rec["purpose_level"])
        return {"tenant": ev["tenant"],
                "trust_config_version": rec["trust_config_version"],
                "version": rec["version"], "changed": changed}

    def _ev_set_purpose_level(self, ev: dict) -> dict:
        rec = self._tenant(ev["tenant"], self.event_path + ".tenant")
        changed = self._apply_tenant_config(rec,
                                            rec["peer_identity_version"],
                                            rec["trust_config_version"],
                                            ev["purpose_level"])
        return {"tenant": ev["tenant"], "purpose_level": rec["purpose_level"],
                "version": rec["version"], "changed": changed}

    def _ev_revoke_peer(self, ev: dict) -> dict:
        tenant = ev["tenant"]
        rec = self._tenant(tenant, self.event_path + ".tenant")
        rec["revoked"] = True
        rec["revoked_at"] = self.now
        rec["revision"] = int(rec["revision"]) + 1
        rec["version"] = int(rec["version"]) + 1

        terminated: list[str] = []
        closed: set[str] = set()
        for rid in list(self.state["submit_order"]):
            req = self.state["requests"][rid]
            if req["tenant"] != tenant or req["state"] in TERMINAL_STATES:
                continue
            # Every connection committed to a request of the revoked tenant is
            # suspect, whether or not it already carries a context (it may
            # still be mid-authentication).  Close them before terminating so
            # nothing is silently returned to the idle pool.
            owned = list(req["reserved"])
            for cid in owned:
                self._close_connection(cid, reason="peer_revoked")
                closed.add(cid)
            self._terminate(req, reason="peer_revoked",
                            detail={"event": ev["event_id"],
                                    "connections_closed": sorted(owned)})
            terminated.append(rid)

        for cid in sorted(self.state["connections"]):
            if self._conn_owned_by(cid, tenant):
                self._close_connection(cid, reason="peer_revoked")
                closed.add(cid)
        # A connection reserved for the revoked tenant may not have finished
        # authenticating yet, so it carries no context.  It is still committed
        # to a request of the revoked tenant and must be dropped.
        for cid in sorted(self.state["connections"]):
            conn = self.state["connections"][cid]
            if conn["lifecycle"] == L_CLOSED or cid in closed:
                continue
            holder = self.state["requests"].get(conn.get("request_id") or "")
            if holder is not None and holder["tenant"] == tenant:
                self._close_connection(cid, reason="peer_revoked")
                closed.add(cid)
        return {"tenant": tenant, "revoked": True,
                "terminated": terminated,
                "connections_closed": sorted(closed),
                "version": rec["version"]}

    def _conn_owned_by(self, cid: str, tenant: str) -> bool:
        conn = self.state["connections"].get(cid)
        if conn is None or conn.get("context") is None:
            return False
        return conn["context"]["tenant"] == tenant

    # ------------------------------------------------------------ worker evs
    def _ev_worker_exit(self, ev: dict) -> dict:
        process_id = ev["process_id"]
        held = self.state["lease"].get("holder") == process_id
        self.state["exits"].append({
            "process_id": process_id, "at": self.now,
            "batch": self.batch_id, "event": ev["event_id"],
            "reason": ev.get("reason", "exit"), "held_lease": held,
        })
        demoted = self._demote_owned(process_id, reason="worker_exit")
        if held:
            self.state["lease"]["holder"] = None
        return {"process_id": process_id, "held_lease": held,
                "pending_ack_operations": demoted,
                "lease": self._lease_snapshot()}

    def _ev_worker_resume(self, ev: dict) -> dict:
        return {"process_id": ev["process_id"], "now": self.now}

    # ------------------------------------------------------------- requests
    def _ev_submit_request(self, ev: dict) -> dict:
        rid = ev["request_id"]
        existing = self.state["requests"].get(rid)
        if existing is not None:
            same = (existing["tenant"] == ev["tenant"]
                    and existing["candidates"] == ev["candidates"]
                    and existing["group_size"] == ev["group_size"])
            if same:
                receipt = self.last_receipt(rid)
                return {"request_id": rid, "state": existing["state"],
                        "result": "duplicate_ignored",
                        "receipt": receipt["receipt_id"] if receipt else None}
            raise SimError(
                E_BATCH_CONFLICT, self.event_path + ".request_id",
                f"request_id {rid!r} was already submitted with different "
                f"content", {"request_id": rid}, exit_code=3)
        tenant = self._tenant(ev["tenant"], self.event_path + ".tenant")
        for cid in ev["candidates"]:
            self._conn(cid, self.event_path + ".candidates")
        purpose = ev.get("purpose_level", tenant["purpose_level"])
        req = new_request(rid, ev["tenant"], purpose, ev["candidates"],
                          ev["group_size"], self.now,
                          int(self.state["next_submit_seq"]))
        self.state["next_submit_seq"] += 1
        self.state["requests"][rid] = req
        self.state["submit_order"].append(rid)
        self._transition(req, R_PENDING, reason="submitted",
                         detail={"candidates": ev["candidates"],
                                 "group_size": ev["group_size"],
                                 "purpose_level": purpose})
        return {"request_id": rid, "state": R_PENDING,
                "submitted_seq": req["submitted_seq"]}

    def _ev_cancel_request(self, ev: dict) -> dict:
        req = self._request(ev["request_id"], self.event_path + ".request_id")
        if req["state"] in TERMINAL_STATES:
            receipt = self.last_receipt(req["request_id"])
            self._reject(C_TERMINAL_STATE, "request", req["request_id"],
                         {"state": req["state"]})
            return {"request_id": req["request_id"], "state": req["state"],
                    "result": "ignored", "rejected": C_TERMINAL_STATE,
                    "receipt_type": receipt["type"] if receipt else None}
        self._release_holds(req, reason="cancelled")
        self._transition(req, R_CANCELLED, reason="cancelled",
                         detail={"event": ev["event_id"],
                                 "reason": ev.get("reason")})
        receipt = self._emit(req, RC_CANCELLED, reason="cancelled")
        return {"request_id": req["request_id"], "state": R_CANCELLED,
                "receipt": receipt["receipt_id"],
                "receipt_type": RC_CANCELLED}

    def _ev_complete_request(self, ev: dict) -> dict:
        req = self._request(ev["request_id"], self.event_path + ".request_id")
        if req["state"] in TERMINAL_STATES:
            self._reject(C_TERMINAL_STATE, "request", req["request_id"],
                         {"state": req["state"]})
            receipt = self.last_receipt(req["request_id"])
            return {"request_id": req["request_id"], "state": req["state"],
                    "result": "ignored", "rejected": C_TERMINAL_STATE,
                    "receipt_type": receipt["type"] if receipt else None}
        if req["state"] != R_IN_USE:
            self._reject(C_NOT_IN_USE, "request", req["request_id"],
                         {"state": req["state"]})
            return {"request_id": req["request_id"], "state": req["state"],
                    "result": "ignored", "rejected": C_NOT_IN_USE}
        conns = list(req["reserved"])
        for cid in conns:
            self._release_to_idle(self.state["connections"][cid],
                                  reason="request_complete")
        req["reserved"] = []
        if ev["outcome"] == "success":
            self._transition(req, R_SUCCEEDED, reason="completed",
                             detail={"payload": ev.get("payload")})
            receipt = self._emit(req, RC_SUCCEEDED, reason="success",
                                 detail={"payload": ev.get("payload")})
            return {"request_id": req["request_id"], "state": R_SUCCEEDED,
                    "receipt": receipt["receipt_id"],
                    "receipt_type": RC_SUCCEEDED, "connections": conns}
        self._transition(req, R_FAILED, reason="completed_failure",
                         detail={"reason": ev.get("reason")})
        receipt = self._emit(req, RC_FAILED,
                             reason=ev.get("reason") or "failure")
        return {"request_id": req["request_id"], "state": R_FAILED,
                "receipt": receipt["receipt_id"], "receipt_type": RC_FAILED,
                "connections": conns}

    # ------------------------------------------------------------ async ops
    def _ev_purge_complete(self, ev: dict) -> dict:
        return self._complete_op(ev, OP_PURGE)

    def _ev_auth_complete(self, ev: dict) -> dict:
        return self._complete_op(ev, OP_AUTH)

    def _complete_op(self, ev: dict, kind: str) -> dict:
        cid = ev["connection_id"]
        conn = self._conn(cid, self.event_path + ".connection_id")
        code, detail = self._validate(ev, conn, kind)
        if code is not None:
            op = self.state["operations"].get(ev["op_id"])
            if op is not None:
                op["messages"].append({
                    "at": self.now, "batch": self.batch_id,
                    "event": ev["event_id"], "accepted": False,
                    "code": code, "reason": _reject_reason(code)})
            self._reject(code, "connection", cid, {
                "kind": kind, "op_id": ev["op_id"],
                "generation": ev["generation"],
                "lease_generation": ev["lease_generation"],
                "expected_config_version": ev["expected_config_version"],
                **detail})
            return {"connection_id": cid, "op_id": ev["op_id"],
                    "result": "rejected", "rejected": code,
                    "why": _reject_reason(code),
                    "lifecycle": conn["lifecycle"],
                    "generation": conn["generation"], "detail": detail}

        op = self.state["operations"][ev["op_id"]]
        op["messages"].append({"at": self.now, "batch": self.batch_id,
                               "event": ev["event_id"], "accepted": True,
                               "code": None, "reason": None})
        if kind == OP_PURGE:
            return self._apply_purge(ev, conn, op)
        return self._apply_auth(ev, conn, op)

    def _validate(self, ev: dict, conn: dict,
                  kind: str) -> tuple[str | None, dict]:
        if int(ev["generation"]) != int(conn["generation"]):
            return C_STALE_GENERATION, {
                "current_generation": conn["generation"]}
        current_id = conn.get("op")
        if current_id is None or current_id != ev["op_id"]:
            prior = self.state["operations"].get(ev["op_id"])
            if prior is None:
                return C_UNKNOWN_OP, {"current_op_id": current_id}
            if prior["status"] == OP_SETTLED:
                return C_STALE_OPERATION, {"current_op_id": current_id,
                                           "settled_outcome": prior["outcome"]}
            return C_STALE_OPERATION, {"current_op_id": current_id}
        op = self.state["operations"][current_id]
        if op["kind"] != kind:
            return C_STALE_OPERATION, {"expected_kind": op["kind"],
                                       "got_kind": kind}
        if op["status"] == OP_SETTLED:
            return C_STALE_OPERATION, {"settled_outcome": op["outcome"]}
        if int(ev["lease_generation"]) != int(op["lease_generation"]):
            return C_STALE_LEASE, {
                "message_lease_generation": ev["lease_generation"],
                "operation_lease_generation": op["lease_generation"]}
        if int(ev["lease_generation"]) != self._lease_gen():
            return C_STALE_LEASE, {
                "message_lease_generation": ev["lease_generation"],
                "current_lease_generation": self._lease_gen()}
        if int(ev["expected_config_version"]) != int(
                op["expected_config_version"]):
            return C_STALE_EXPECTED_VERSION, {
                "message": ev["expected_config_version"],
                "operation": op["expected_config_version"]}
        current = self._current_config_version(op["tenant"])
        if current is not None and int(ev["expected_config_version"]) != current:
            return C_STALE_EXPECTED_VERSION, {
                "message": ev["expected_config_version"],
                "current": current}
        return None, {}

    def _current_config_version(self, tenant: str) -> int | None:
        rec = self.state["tenants"].get(tenant)
        return int(rec["version"]) if rec else None

    def _apply_purge(self, ev: dict, conn: dict, op: dict) -> dict:
        req = self.state["requests"].get(op["request_id"])
        if ev["outcome"] != "ok":
            return self._purge_failed(ev, conn, op, req)
        self._settle(op, "ok")
        # Clearing the context does not end the use cycle: the generation
        # still identifies this live connection object.  It advances only when
        # the connection is closed and its cycle ends.
        conn["context"] = None
        conn["auth_expires_at"] = None
        conn["op"] = None
        conn["cooldown_until"] = None
        self._note(conn, "purged", generation=conn["generation"])
        if req is not None and req["state"] == R_RESERVED \
                and conn["connection_id"] in req["reserved"]:
            # The group still wants this connection: authenticate with the
            # current configuration version.
            conn["lifecycle"] = L_AUTHENTICATING
            attempt = int(conn["op_attempts"].get(OP_AUTH, 0)) + 1
            version = int(self.state["tenants"][req["tenant"]]["version"])
            auth_op = self._new_op(OP_AUTH, conn, req, attempt=attempt,
                                   expected_version=version)
            self._note(conn, "auth_started", op=auth_op["op_id"],
                       request=req["request_id"])
            return {"connection_id": conn["connection_id"],
                    "op_id": op["op_id"], "result": "accepted",
                    "outcome": "ok", "lifecycle": conn["lifecycle"],
                    "generation": conn["generation"],
                    "next_op": auth_op["op_id"]}
        conn["lifecycle"] = L_IDLE
        conn["request_id"] = None
        return {"connection_id": conn["connection_id"], "op_id": op["op_id"],
                "result": "accepted", "outcome": "ok",
                "lifecycle": conn["lifecycle"],
                "generation": conn["generation"]}

    def _purge_failed(self, ev: dict, conn: dict, op: dict,
                      req: dict | None) -> dict:
        self._settle(op, "fail")
        conn["op"] = None
        attempts = int(conn["op_attempts"].get(OP_PURGE, 0))
        conn["cooldown_until"] = self.now + int(self.config["purge_cooldown_ms"])
        self._note(conn, "purge_failed", attempts=attempts,
                   cooldown_until=conn["cooldown_until"])
        if attempts >= int(self.config["max_purge_attempts"]):
            conn["lifecycle"] = L_CLOSED
            conn["generation"] = int(conn["generation"]) + 1
            conn["context"] = None
            conn["request_id"] = None
            reason = B_PURGE_FAILED
        else:
            conn["lifecycle"] = L_IDLE
            conn["request_id"] = None
            reason = B_COOLDOWN
        if req is not None:
            self._break_group(req, reason)
        return {"connection_id": conn["connection_id"], "op_id": op["op_id"],
                "result": "accepted", "outcome": "fail",
                "lifecycle": conn["lifecycle"],
                "generation": conn["generation"],
                "attempts": attempts,
                "will_retry": reason == B_COOLDOWN,
                "cooldown_until": conn["cooldown_until"],
                "reason": ev.get("reason") or "purge failed"}

    def _apply_auth(self, ev: dict, conn: dict, op: dict) -> dict:
        req = self.state["requests"].get(op["request_id"])
        if ev["outcome"] != "ok":
            return self._auth_failed(ev, conn, op, req)
        ctx = ev.get("context")
        if ctx is None and req is not None:
            ctx = self._context_for(req["tenant"], req.get("purpose_level"))
        if ctx is None:
            self._settle(op, "void")
            conn["op"] = None
            self._close_connection(conn["connection_id"],
                                   reason="auth_without_context")
            self._reject(C_STALE_EXPECTED_VERSION, "connection",
                         conn["connection_id"],
                         {"message": "auth completion carried no context"})
            return {"connection_id": conn["connection_id"],
                    "op_id": op["op_id"], "result": "rejected",
                    "rejected": C_STALE_EXPECTED_VERSION,
                    "lifecycle": conn["lifecycle"]}
        self._settle(op, "ok")
        conn["op"] = None
        conn["context"] = dict(ctx)
        conn["auth_expires_at"] = int(
            ev.get("expires_at",
                   self.now + int(self.config["auth_ttl_ms"])))
        conn["op_attempts"][OP_AUTH] = 0
        conn["cooldown_until"] = None
        if req is not None and conn["connection_id"] in req["reserved"]:
            conn["lifecycle"] = L_RESERVED
            conn["request_id"] = req["request_id"]
        else:
            conn["lifecycle"] = L_IDLE
            conn["request_id"] = None
        self._note(conn, "authenticated", generation=conn["generation"],
                   context=conn["context"])
        return {"connection_id": conn["connection_id"], "op_id": op["op_id"],
                "result": "accepted", "outcome": "ok",
                "lifecycle": conn["lifecycle"],
                "generation": conn["generation"],
                "context": conn["context"],
                "auth_expires_at": conn["auth_expires_at"]}

    def _auth_failed(self, ev: dict, conn: dict, op: dict,
                     req: dict | None) -> dict:
        self._settle(op, "fail")
        conn["op"] = None
        conn["lifecycle"] = L_CLOSED
        conn["generation"] = int(conn["generation"]) + 1
        conn["context"] = None
        conn["auth_expires_at"] = None
        conn["request_id"] = None
        conn["cooldown_until"] = self.now + int(self.config["purge_cooldown_ms"])
        self._note(conn, "auth_failed", generation=conn["generation"])
        if req is not None:
            self._break_group(req, B_PURGE_FAILED)
        return {"connection_id": conn["connection_id"], "op_id": op["op_id"],
                "result": "accepted", "outcome": "fail",
                "lifecycle": conn["lifecycle"],
                "generation": conn["generation"],
                "reason": ev.get("reason") or "auth failed"}

    def _ev_retry_op(self, ev: dict) -> dict:
        op = self.state["operations"].get(ev["op_id"])
        if op is None:
            raise SimError(E_REFERENCE, self.event_path + ".op_id",
                           f"unknown op_id {ev['op_id']!r}",
                           {"op_id": ev["op_id"]})
        if op["status"] != OP_PENDING_ACK:
            return {"op_id": op["op_id"], "result": "ignored",
                    "status": op["status"],
                    "rejected": C_STALE_OPERATION
                    if op["status"] == OP_SETTLED else None}
        conn = self.state["connections"].get(op["connection_id"])
        if conn is None or conn.get("op") != op["op_id"]:
            return {"op_id": op["op_id"], "result": "ignored",
                    "rejected": C_STALE_OPERATION, "status": op["status"]}
        # Idempotent resend under the current control lease.
        op["status"] = OP_IN_FLIGHT
        op["attempt"] = int(op["attempt"]) + 1
        op["pending_ack_since"] = None
        op["process_id"] = (self._live_lease() or {}).get("holder")
        op["lease_generation"] = self._lease_gen()
        self._note(conn, "operation_resent", op=op["op_id"],
                   attempt=op["attempt"])
        return {"op_id": op["op_id"], "result": "resent",
                "status": op["status"], "attempt": op["attempt"],
                "kind": op["kind"],
                "connection_id": conn["connection_id"],
                "generation": op["generation"],
                "expected_config_version": op["expected_config_version"],
                "lease_generation": op["lease_generation"]}

    def _ev_tick(self, ev: dict) -> dict:
        return {"now": self.now}

    # ============================================================ scheduling
    def _context_for(self, tenant: str,
                     purpose: str | None = None) -> dict:
        rec = self.state["tenants"][tenant]
        return context_of(tenant, rec["peer_identity_version"],
                          rec["trust_config_version"],
                          purpose if purpose is not None
                          else rec["purpose_level"])

    def _is_expired(self, conn: dict) -> bool:
        exp = conn.get("auth_expires_at")
        return exp is not None and self.now > int(exp)

    def _free(self, conn: dict) -> bool:
        return (conn["lifecycle"] == L_IDLE and conn.get("op") is None
                and conn.get("request_id") is None)

    def _cooldown_ok(self, conn: dict) -> bool:
        cd = conn.get("cooldown_until")
        return cd is None or self.now >= int(cd)

    def _classify(self, conn: dict | None, ctx: dict) -> tuple[str, str]:
        """Return ``(class, reason)`` for a candidate connection."""
        if conn is None or conn["lifecycle"] == L_CLOSED:
            return "unusable", "CLOSED"
        if not self._cooldown_ok(conn):
            return "busy", "COOLDOWN"
        if not self._free(conn):
            return "busy", "HELD"
        if contexts_equal(conn.get("context"), ctx) and not self._is_expired(conn):
            return "reusable", "CONTEXT_MATCH"
        if contexts_equal(conn.get("context"), ctx) and self._is_expired(conn):
            return "auth", "AUTH_EXPIRED"
        if conn.get("context") is None:
            return "auth", "NO_CONTEXT"
        mism = [f for f in ("tenant", "peer_identity_version",
                            "trust_config_version", "purpose_level")
                if conn["context"].get(f) != ctx.get(f)]
        return "purge", "CONTEXT_MISMATCH:" + ",".join(mism)

    def _schedule(self) -> list[dict]:
        log: list[dict] = []
        self._sweep_config_drift(log)
        log.extend(self._promote_groups())

        pending = [rid for rid in self.state["submit_order"]
                   if self.state["requests"][rid]["state"] == R_PENDING]
        if not pending:
            return log
        if self._live_lease() is None:
            for rid in pending:
                req = self.state["requests"][rid]
                self._block(req, B_NO_LEASE,
                            {"reason": "no active control lease",
                             "lease": self._lease_snapshot()})
                log.append({"request_id": rid, "action": "deferred",
                            "reason": B_NO_LEASE})
            return log
        for rid in pending:
            log.append(self._plan(self.state["requests"][rid]))
        log.extend(self._promote_groups())
        return log

    def _sweep_config_drift(self, log: list[dict]) -> None:
        """Void operations whose expected config version is now stale.

        An operation already rejected by an event in this same batch has
        recorded why; the sweep does not add a second rejection for it.
        """
        for op_id in sorted(self.state["operations"]):
            op = self.state["operations"][op_id]
            if op["status"] not in (OP_IN_FLIGHT, OP_PENDING_ACK):
                continue
            current = self._current_config_version(op["tenant"])
            if current is None or current == int(op["expected_config_version"]):
                continue
            already_rejected = any(
                m["batch"] == self.batch_id and not m["accepted"]
                for m in op["messages"])
            self._settle(op, "stale_config")
            conn = self.state["connections"].get(op["connection_id"])
            req = self.state["requests"].get(op["request_id"])
            if conn is not None and conn.get("op") == op_id:
                conn["op"] = None
                if conn["lifecycle"] in (L_AUTHENTICATING, L_PENDING_PURGE):
                    conn["lifecycle"] = L_IDLE
                    conn["request_id"] = None
            if not already_rejected:
                self._reject(C_STALE_EXPECTED_VERSION, "operation", op_id,
                             {"expected": op["expected_config_version"],
                              "current": current})
            if req is not None and req["state"] not in TERMINAL_STATES:
                self._break_group(req, B_COOLDOWN)
            log.append({"action": "config_drift_void", "op_id": op_id,
                        "connection_id": op["connection_id"]})

    def _plan(self, req: dict) -> dict:
        rid = req["request_id"]
        tenant = self.state["tenants"][req["tenant"]]
        if tenant["revoked"]:
            self._terminate(req, reason="peer_revoked",
                            detail={"phase": "schedule"})
            return {"request_id": rid, "action": "terminated",
                    "reason": "peer_revoked"}

        ctx = self._context_for(req["tenant"], req.get("purpose_level"))
        need = req["group_size"]
        committed = set(req["reserved"])
        needed = need - len(committed)
        if needed <= 0:
            return {"request_id": rid, "action": "already_committed",
                    "connections": sorted(committed)}

        reusable: list[str] = []
        purge: list[str] = []
        auth: list[str] = []
        busy: list[str] = []
        unusable: list[str] = []
        detail: list[dict] = []
        for cid in sorted(req["candidates"]):
            if cid in committed:
                continue
            cls, reason = self._classify(
                self.state["connections"].get(cid), ctx)
            detail.append({"connection_id": cid, "class": cls,
                           "reason": reason})
            if cls == "reusable":
                reusable.append(cid)
            elif cls == "purge":
                purge.append(cid)
            elif cls == "auth":
                auth.append(cid)
            elif cls == "busy":
                busy.append(cid)
            else:
                unusable.append(cid)

        capacity = len(reusable) + len(purge) + len(auth)
        if capacity >= needed:
            remaining = needed
            reuse = reusable[:remaining]
            remaining -= len(reuse)
            to_purge = purge[:remaining]
            remaining -= len(to_purge)
            to_auth = auth[:remaining]
            self._commit(req, reuse, to_purge, to_auth)
            return {"request_id": rid, "action": "committed",
                    "reused": reuse, "purge": to_purge, "auth": to_auth,
                    "connections": sorted(req["reserved"])}

        # All-or-nothing: not enough for the whole group, so take nothing and
        # record why every candidate was unusable this pass.
        if busy and not (reusable or purge or auth):
            reason = B_BUSY
        elif busy:
            reason = B_BUSY
        else:
            reason = B_INSUFFICIENT
        self._block(req, reason, {
            "needed": needed, "group_size": need,
            "already_committed": sorted(committed),
            "reusable": reusable, "purgeable": purge, "authable": auth,
            "busy": busy, "unusable": unusable,
            "candidates": detail,
        })
        return {"request_id": rid, "action": "deferred", "reason": reason,
                "reusable": reusable, "purgeable": purge,
                "authable": auth, "busy": busy, "unusable": unusable}

    def _commit(self, req: dict, reuse: list[str], to_purge: list[str],
                to_auth: list[str]) -> None:
        version = int(self.state["tenants"][req["tenant"]]["version"])
        for cid in reuse:
            conn = self.state["connections"][cid]
            conn["lifecycle"] = L_RESERVED
            conn["request_id"] = req["request_id"]
            self._note(conn, "reserved_reusable", request=req["request_id"])
        for cid in to_purge:
            conn = self.state["connections"][cid]
            conn["lifecycle"] = L_PENDING_PURGE
            conn["request_id"] = req["request_id"]
            attempt = int(conn["op_attempts"].get(OP_PURGE, 0)) + 1
            op = self._new_op(OP_PURGE, conn, req, attempt=attempt,
                              expected_version=version)
            self._note(conn, "purge_started", op=op["op_id"],
                       request=req["request_id"])
        for cid in to_auth:
            conn = self.state["connections"][cid]
            conn["lifecycle"] = L_AUTHENTICATING
            conn["request_id"] = req["request_id"]
            attempt = int(conn["op_attempts"].get(OP_AUTH, 0)) + 1
            op = self._new_op(OP_AUTH, conn, req, attempt=attempt,
                              expected_version=version)
            self._note(conn, "auth_started", op=op["op_id"],
                       request=req["request_id"])
        req["reserved"] = sorted(set(req["reserved"]) | set(reuse)
                                 | set(to_purge) | set(to_auth))
        req["blocked_reason"] = None
        req["blocked_detail"] = None
        req["blocked_at"] = None
        req["blocked_count"] = 0
        self._transition(req, R_RESERVED, reason="group_committed",
                         detail={"connections": req["reserved"],
                                 "reused": reuse, "purge": to_purge,
                                 "auth": to_auth})
        self._promote_one(req)

    def _promote_one(self, req: dict) -> bool:
        if req["state"] != R_RESERVED:
            return False
        conns = [self.state["connections"][cid] for cid in req["reserved"]]
        if not conns:
            return False
        if all(c["lifecycle"] == L_RESERVED and c.get("op") is None
               for c in conns):
            for c in conns:
                c["lifecycle"] = L_IN_USE
            self._transition(req, R_IN_USE, reason="contexts_confirmed",
                             detail={"connections": req["reserved"]})
            self._emit(req, RC_RESERVED, reason="group_reserved",
                       detail={"connections": req["reserved"]})
            return True
        return False

    def _promote_groups(self) -> list[dict]:
        log = []
        for rid in self.state["submit_order"]:
            req = self.state["requests"][rid]
            if req["state"] == R_RESERVED and self._promote_one(req):
                log.append({"request_id": rid, "action": "in_use",
                            "connections": req["reserved"]})
        return log

    def _block(self, req: dict, reason: str, detail: dict | None) -> None:
        req["blocked_reason"] = reason
        req["blocked_detail"] = detail
        req["blocked_at"] = self.now
        req["blocked_count"] = int(req.get("blocked_count", 0)) + 1
        req["blocked_history"].append({
            "at": self.now, "batch": self.batch_id, "reason": reason,
            "reusable": list((detail or {}).get("reusable", [])),
            "purgeable": list((detail or {}).get("purgeable", [])),
            "authable": list((detail or {}).get("authable", [])),
            "busy": list((detail or {}).get("busy", [])),
            "unusable": list((detail or {}).get("unusable", [])),
        })

    def _break_group(self, req: dict, reason: str) -> None:
        """A committed group lost a member: release the rest and re-plan.

        Only groups still in ``RESERVED`` (not yet handed to the caller) can be
        broken this way.  An ``IN_USE`` group is never torn down by an async
        operation, so a caller holding its connections is not surprised.
        """
        if req["state"] in TERMINAL_STATES or req["state"] != R_RESERVED:
            return
        for cid in list(req["reserved"]):
            conn = self.state["connections"].get(cid)
            if conn is None:
                continue
            if conn.get("op"):
                op = self.state["operations"].get(conn["op"])
                if op is not None and op["status"] != OP_SETTLED:
                    self._settle(op, "void")
                conn["op"] = None
            if conn["lifecycle"] in (L_RESERVED, L_AUTHENTICATING,
                                     L_PENDING_PURGE, L_IN_USE):
                conn["lifecycle"] = L_IDLE
            conn["request_id"] = None
            self._note(conn, "group_broken", reason=reason)
        req["reserved"] = []
        self._transition(req, R_PENDING, reason="group_broken",
                         detail={"cause": reason})
        self._block(req, reason, {"cause": "group member failed"})

    # ============================================================== teardown
    def _terminate(self, req: dict, *, reason: str,
                   detail: dict | None) -> None:
        if req["state"] in TERMINAL_STATES:
            return
        for cid in list(req["reserved"]):
            conn = self.state["connections"].get(cid)
            if conn is None:
                continue
            if conn.get("op"):
                op = self.state["operations"].get(conn["op"])
                if op is not None and op["status"] != OP_SETTLED:
                    self._settle(op, "void")
                conn["op"] = None
            if conn["lifecycle"] != L_CLOSED:
                conn["lifecycle"] = L_IDLE
                conn["request_id"] = None
        req["reserved"] = []
        self._transition(req, R_TERMINATED, reason=reason, detail=detail)
        receipt = self._emit(req, RC_TERMINATED, reason=reason,
                             detail=detail)
        req["terminal_receipt"] = receipt["receipt_id"]
        req["accepts_completion"] = False

    def _release_holds(self, req: dict, *, reason: str) -> None:
        for cid in list(req["reserved"]):
            conn = self.state["connections"].get(cid)
            if conn is None:
                continue
            if conn.get("op"):
                op = self.state["operations"].get(conn["op"])
                if op is not None and op["status"] != OP_SETTLED:
                    self._settle(op, "cancelled")
                conn["op"] = None
            if conn["lifecycle"] in (L_RESERVED, L_AUTHENTICATING,
                                     L_PENDING_PURGE):
                conn["lifecycle"] = L_IDLE
            conn["request_id"] = None
            conn["cooldown_until"] = None
            self._note(conn, "released", reason=reason,
                       request=req["request_id"])
        req["reserved"] = []

    def _close_connection(self, cid: str, *, reason: str) -> None:
        conn = self.state["connections"][cid]
        if conn["lifecycle"] == L_CLOSED:
            return
        if conn.get("op"):
            op = self.state["operations"].get(conn["op"])
            if op is not None and op["status"] != OP_SETTLED:
                self._settle(op, "void")
        conn["lifecycle"] = L_CLOSED
        conn["generation"] = int(conn["generation"]) + 1
        conn["context"] = None
        conn["auth_expires_at"] = None
        conn["request_id"] = None
        conn["op"] = None
        conn["cooldown_until"] = None
        self._note(conn, "closed", reason=reason,
                   generation=conn["generation"])

    def _release_to_idle(self, conn: dict, *, reason: str) -> None:
        if conn.get("op"):
            op = self.state["operations"].get(conn["op"])
            if op is not None and op["status"] != OP_SETTLED:
                self._settle(op, "void")
            conn["op"] = None
        conn["lifecycle"] = L_IDLE
        conn["request_id"] = None
        conn["cooldown_until"] = None
        conn["reuse_count"] = int(conn.get("reuse_count", 0)) + 1
        self._note(conn, "released_to_idle", reason=reason)


def _reject_reason(code: str) -> str:
    return {
        C_STALE_GENERATION: "the connection generation has moved on, so this "
                            "completion describes a use cycle that ended",
        C_STALE_OPERATION: "the one-time operation id is no longer the "
                           "connection's current or unsettled operation",
        C_STALE_LEASE: "the control lease generation that issued this "
                       "completion is no longer current",
        C_STALE_EXPECTED_VERSION: "the expected configuration version does "
                                  "not match the operation or the current "
                                  "tenant configuration",
        C_UNKNOWN_OP: "no operation with this id was ever issued",
    }.get(code, "rejected")
