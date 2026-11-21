"""Independent checks over every historical prefix.

The verifier does **not** reuse the scheduler.  It replays prefix by prefix and
asks two questions of each recorded snapshot:

1. Determinism: replaying the same logical batches must give the same digest.
2. Safety: no connection may ever be handed to a second tenant while it still
   carries the first tenant's security context.  Concretely, across *every*
   prefix, a connection whose context belongs to tenant A must never be in a
   state where it is reserved or in use for tenant B.  It must have been
   purged (context cleared) or closed first.

``ARGUMENT``: cross-tenant reuse is safe iff, at every observed instant, for
every connection with a non-null context, the request holding it belongs to
the context's tenant.
"""

from __future__ import annotations

from .engine import Engine
from .errors import SimError
from .model import (
    L_AUTHENTICATING,
    L_IN_USE,
    L_PENDING_PURGE,
    L_RESERVED,
    TERMINAL_STATES,
    digest,
)
from .runner import Runner
from .state import new_state
from .store import PoolStore

HOLDING = frozenset((L_RESERVED, L_AUTHENTICATING, L_IN_USE, L_PENDING_PURGE))
# States in which the connection has actually been handed to the request, so
# its security context must already be the holder's context.
DELIVERED = frozenset((L_RESERVED, L_IN_USE))


def _snapshot_digest(state: dict) -> str:
    view = {
        "now": state["now"],
        "lease": state["lease"],
        "connections": {
            cid: {"generation": c["generation"],
                  "lifecycle": c["lifecycle"],
                  "context": c["context"],
                  "request_id": c["request_id"],
                  "op": c["op"]}
            for cid, c in sorted(state["connections"].items())
        },
        "requests": {
            rid: {"tenant": r["tenant"], "state": r["state"],
                  "reserved": r["reserved"]}
            for rid, r in sorted(state["requests"].items())
        },
        "receipts": {
            rid: [state["receipts"][i]["digest"] for i in ids]
            for rid, ids in sorted(state["request_receipts"].items())
        },
    }
    return digest(view)


def _prefix_violations(state: dict, prefix: str) -> list[dict]:
    """Cross-tenant reuse and receipt-uniqueness violations at one instant."""
    out: list[dict] = []
    for cid, conn in sorted(state["connections"].items()):
        ctx = conn.get("context")
        req_id = conn.get("request_id")
        if ctx is None or req_id is None:
            continue
        req = state["requests"].get(req_id)
        if req is None:
            out.append({"kind": "ORPHAN_HOLD", "prefix": prefix,
                        "connection_id": cid, "request_id": req_id})
            continue
        if req["tenant"] != ctx["tenant"] and conn["lifecycle"] in DELIVERED:
            out.append({
                "kind": "CROSS_TENANT_REUSE",
                "prefix": prefix,
                "connection_id": cid,
                "generation": conn["generation"],
                "context_tenant": ctx["tenant"],
                "holder_tenant": req["tenant"],
                "lifecycle": conn["lifecycle"],
            })
        if req["state"] in TERMINAL_STATES and conn["lifecycle"] in (
                L_RESERVED, L_IN_USE):
            out.append({"kind": "HOLD_AFTER_TERMINAL", "prefix": prefix,
                        "connection_id": cid, "request_id": req_id,
                        "request_state": req["state"],
                        "lifecycle": conn["lifecycle"]})
    for rid, ids in state["request_receipts"].items():
        terminal = [state["receipts"][i] for i in ids
                    if state["receipts"][i]["terminal"]]
        if len(terminal) > 1:
            out.append({"kind": "DUPLICATE_TERMINAL_RECEIPT",
                        "prefix": prefix, "request_id": rid,
                        "types": [t["type"] for t in terminal]})
    return out


def verify_prefix_invariants(case: dict, db: str | None = None) -> dict:
    scenario = "db" if db else "memory"
    if db is not None:
        with PoolStore(db).open() as store:
            base = store.load(case["config"])
    else:
        base = new_state(case["config"])

    engine = Engine(base, case["config"])
    if not base.get("boot_seeded"):
        engine.bootstrap(case["connections"], case["tenants"])
        base["boot_seeded"] = 1
    base.setdefault("boot_config", dict(case["config"]))

    violations: list[dict] = []
    prefixes: list[dict] = []
    digests_run_1: list[str] = []

    for bi, batch in enumerate(case["batches"]):
        prefix_id = f"after:{batch['batch_id']}"
        if batch["batch_id"] in base["batches"]:
            # Already applied (persistent db) - treat as replay.
            prefixes.append({"batch_id": batch["batch_id"],
                             "replayed": True})
            digests_run_1.append(_snapshot_digest(base))
            violations.extend(_prefix_violations(base, prefix_id))
            continue
        try:
            result = engine.apply_batch(batch, batch_index=bi)
        except SimError as exc:
            exc.extra.setdefault("batch_index", bi)
            exc.extra.setdefault("batch_id", batch["batch_id"])
            raise
        prefixes.append({"batch_id": batch["batch_id"], "replayed": False,
                         "rejections": result["rejections"]})
        digests_run_1.append(_snapshot_digest(base))
        violations.extend(_prefix_violations(base, prefix_id))

    # Determinism: replay the whole case from scratch and compare per-prefix
    # digests.  Independent of SQLite.
    fresh = new_state(case["config"])
    engine2 = Engine(fresh, case["config"])
    engine2.bootstrap(case["connections"], case["tenants"])
    fresh["boot_seeded"] = 1
    digests_run_2: list[str] = []
    for bi, batch in enumerate(case["batches"]):
        engine2.apply_batch(batch, batch_index=bi)
        digests_run_2.append(_snapshot_digest(fresh))

    mismatch = [i for i, (a, b) in enumerate(zip(digests_run_1, digests_run_2))
                if a != b]
    deterministic = not mismatch and len(digests_run_1) == len(digests_run_2)

    return {
        "command": "verify",
        "scenario": scenario,
        "batches": len(case["batches"]),
        "prefixes_checked": len(prefixes),
        "deterministic": deterministic,
        "determinism_mismatch_indices": mismatch,
        "violations": violations,
        "ok": deterministic and not violations,
        "prefixes": prefixes,
        "final_state": Runner(case, None).ingest()["final_state"],
    }
