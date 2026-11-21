"""Domain vocabulary and small pure helpers.

Nothing here reads the clock, the filesystem or the network: every value is
derived from the case's logical clock.
"""

from __future__ import annotations

import hashlib
import json

# --- Connection lifecycle --------------------------------------------------
# 空闲 / 预留 / 认证 / 在用 / 待清除 / 关闭

L_IDLE = "IDLE"
L_RESERVED = "RESERVED"
L_AUTHENTICATING = "AUTHENTICATING"
L_IN_USE = "IN_USE"
L_PENDING_PURGE = "PENDING_PURGE"
L_CLOSED = "CLOSED"

LIFECYCLES = (L_IDLE, L_RESERVED, L_AUTHENTICATING, L_IN_USE,
              L_PENDING_PURGE, L_CLOSED)

# Lifecycles in which a connection is committed to a request and must not be
# handed to any other queue.
HELD_LIFECYCLES = frozenset((L_RESERVED, L_AUTHENTICATING, L_IN_USE,
                             L_PENDING_PURGE))

# --- Request states --------------------------------------------------------

R_PENDING = "PENDING"
R_RESERVED = "RESERVED"
R_IN_USE = "IN_USE"
R_SUCCEEDED = "SUCCEEDED"
R_CANCELLED = "CANCELLED"
R_TERMINATED = "TERMINATED"
R_FAILED = "FAILED"

REQUEST_STATES = (R_PENDING, R_RESERVED, R_IN_USE, R_SUCCEEDED, R_CANCELLED,
                  R_TERMINATED, R_FAILED)

TERMINAL_STATES = frozenset((R_SUCCEEDED, R_CANCELLED, R_TERMINATED, R_FAILED))
OPEN_STATES = frozenset((R_PENDING, R_RESERVED, R_IN_USE))
# Requests that currently hold at least one connection.
HOLDING_STATES = frozenset((R_RESERVED, R_IN_USE))

# --- Operations ------------------------------------------------------------

OP_PURGE = "PURGE"
OP_AUTH = "AUTH"

OP_IN_FLIGHT = "IN_FLIGHT"
OP_PENDING_ACK = "PENDING_ACK"
OP_SETTLED = "SETTLED"

# --- Receipt types ---------------------------------------------------------

RC_RESERVED = "RESERVED"
RC_SUCCEEDED = "SUCCEEDED"
RC_TERMINATED = "TERMINATED"
RC_CANCELLED = "CANCELLED"
RC_FAILED = "FAILED"

TERMINAL_RECEIPTS = frozenset((RC_SUCCEEDED, RC_TERMINATED, RC_CANCELLED,
                               RC_FAILED))

# --- Rejection codes -------------------------------------------------------
# Why a late or stale asynchronous message changed nothing.

C_STALE_LEASE = "STALE_LEASE"
C_STALE_GENERATION = "STALE_GENERATION"
C_STALE_OPERATION = "STALE_OPERATION"
C_STALE_EXPECTED_VERSION = "STALE_EXPECTED_VERSION"
C_STALE_CONFIG = "STALE_CONFIG"
C_TERMINAL_STATE = "TERMINAL_STATE"
C_NOT_IN_USE = "NOT_IN_USE"
C_NOT_PENDING = "NOT_PENDING"
C_LEASE_NOT_HELD = "LEASE_NOT_HELD"
C_UNKNOWN_OP = "UNKNOWN_OP"

# --- Blocking reasons ------------------------------------------------------
# Why a pending request could not be planned in this scheduling pass.

B_INSUFFICIENT = "INSUFFICIENT_CANDIDATES"
B_BUSY = "CANDIDATES_BUSY"
B_COOLDOWN = "PURGE_COOLDOWN"
B_NO_LEASE = "NO_ACTIVE_LEASE"
B_PURGE_FAILED = "PURGE_FAILED"

DEFAULT_PURPOSE = "default"


def canonical_json(value) -> str:
    """Stable UTF-8 JSON used for digests and equality checks."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def digest(value) -> str:
    return "sha256:" + hashlib.sha256(
        canonical_json(value).encode("utf-8")).hexdigest()


def context_of(tenant: str, peer_identity_version: int,
               trust_config_version: int, purpose_level: str) -> dict:
    """The four-part security context."""
    return {
        "tenant": tenant,
        "peer_identity_version": int(peer_identity_version),
        "trust_config_version": int(trust_config_version),
        "purpose_level": purpose_level,
    }


CONTEXT_FIELDS = ("tenant", "peer_identity_version", "trust_config_version",
                  "purpose_level")


def contexts_equal(a: dict | None, b: dict | None) -> bool:
    if a is None or b is None:
        return False
    return all(a.get(f) == b.get(f) for f in CONTEXT_FIELDS)

