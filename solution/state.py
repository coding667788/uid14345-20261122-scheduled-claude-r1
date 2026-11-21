"""Construction of the in-memory pool state.

The state is one plain JSON-serialisable document so a batch can be committed
to SQLite atomically, alongside the lease and the pending-acknowledgement list.
"""

from __future__ import annotations

from .model import L_IDLE, OP_AUTH, OP_PURGE, R_PENDING

STATE_VERSION = 1


def new_state(config: dict) -> dict:
    return {
        "version": STATE_VERSION,
        "now": 0,
        "tenants": {},
        "connections": {},
        "requests": {},
        "submit_order": [],
        "next_submit_seq": 1,
        "operations": {},
        "op_counter": 0,
        "receipts": {},
        "request_receipts": {},
        "receipt_counter": 0,
        "batches": {},
        "rejections": [],
        "exits": [],
        "lease": {
            "holder": None,
            "generation": 0,
            "expires_at": 0,
            "acquired_at": None,
        },
        "initial_lease_done": False,
    }


def new_tenant(tenant: str, peer: int = 1, trust: int = 1,
               purpose: str = "default") -> dict:
    return {
        "tenant": tenant,
        "peer_identity_version": int(peer),
        "trust_config_version": int(trust),
        "purpose_level": purpose,
        "revoked": False,
        "revoked_at": None,
        "revision": 0,
        "version": 1,
        "blocked_watermark": 0,
    }


def new_connection(connection_id: str, generation: int = 1) -> dict:
    return {
        "connection_id": connection_id,
        "generation": int(generation),
        "lifecycle": L_IDLE,
        "context": None,
        "auth_expires_at": None,
        "request_id": None,
        "op": None,
        "cooldown_until": None,
        "op_attempts": {OP_PURGE: 0, OP_AUTH: 0},
        "reuse_count": 0,
        "history": [],
    }


def new_request(request_id: str, tenant: str, purpose: str,
                candidates: list[str], group_size: int, submitted_at: int,
                submitted_seq: int) -> dict:
    return {
        "request_id": request_id,
        "tenant": tenant,
        "purpose_level": purpose,
        "candidates": list(candidates),
        "group_size": int(group_size),
        "state": R_PENDING,
        "reserved": [],
        "submitted_at": submitted_at,
        "submitted_seq": submitted_seq,
        "blocked_reason": None,
        "blocked_detail": None,
        "blocked_at": None,
        "blocked_count": 0,
        "blocked_history": [],
        "transitions": [],
        "terminal_receipt": None,
        "accepts_completion": True,
    }
