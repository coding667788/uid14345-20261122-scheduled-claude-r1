# Offline service-proxy connection-pool simulator

A deterministic, offline simulator of an authenticated connection pool behind a
service proxy. It never opens a socket, never calls a real proxy and never reads
the wall clock: every result is a function of the case file's logical clock and
ordered event batches.

It models the hard parts of connection hand-off:

* connections registered by **permanent id + use-cycle generation**,
* a six-state lifecycle — `IDLE`, `RESERVED`, `AUTHENTICATING`, `IN_USE`,
  `PENDING_PURGE`, `CLOSED`,
* a four-part **security context** — tenant, peer-identity version,
  trust-config version, purpose level — that must match on all four parts
  **and** not have exceeded its auth deadline before a connection is reused,
* asynchronous **purge** and **auth** completions that carry the connection
  generation, a one-time operation id, the control-lease generation and the
  expected config version,
* two simulated worker processes that **preemptably share a control lease**
  carrying a generation and a deadline,
* **group reservations** that are all-or-nothing,
* revocation that terminates only the affected tenant's work,
* SQLite-backed atomic ingest with stable, idempotent receipts, and
  crash-before/after-commit recovery.

Runs on **Windows-native Python 3.12**, standard library only, CPU only.

---

## Quick start

```bat
python -m solution ingest  cases\two_tenants_one_connection.json
python -m solution ingest  cases\two_tenants_one_connection.json --db pool.sqlite
python -m solution explain cases\late_completion.json --request r1
python -m solution inspect cases\takeover.json
python -m solution recover cases\crash_and_resume.json --db pool.sqlite
python -m solution verify  cases\group_reservation.json
python -m solution replay  cases\takeover.json
```

Run the tests:

```bat
python -m unittest discover -s tests -v
```

Every command writes UTF-8 JSON to stdout. The same logical input always
produces the same bytes (the `verify` and `replay` commands assert this).

---

## Commands

| Command | Purpose | Reads/writes durable state |
|---|---|---|
| `ingest CASE.json` | Apply every batch, reporting per-event outcomes, the scheduling pass and final state. | writes (with `--db`) |
| `explain CASE.json --request ID` | Reconstruct one request's transitions, blocked history, receipts and why any late completion was rejected. Omitting `--request` explains all. | replay-only |
| `inspect CASE.json` | Dump terminal state, the rejection log and every operation with its message history. | replay-only |
| `recover CASE.json` | Report exactly what a **fresh process** can see: applied batches, terminal receipts, pending-acknowledgement operations, lease. **Does not replay**, so a crash mid-case stays observable. | reads only |
| `verify CASE.json` | Replay twice, compare per-prefix digests, and independently check no historical prefix reused a connection across tenants. | reads only |
| `replay CASE.json` | Ingest twice in memory and report whether the two runs are byte-identical. | in-memory |

Options:

* `--db DB.sqlite` — use a persistent pool. Required for `ingest`/`recover`
  to demonstrate atomicity and crash recovery; without it, everything is
  in-memory.
* `--json-indent N` — pretty-print with indent `N`; `0` emits compact JSON.
* `ingest --crash before-commit|after-commit --crash-batch N` — simulate a
  process dying during batch index `N`.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | success |
| `2` | input or schema error |
| `3` | conflict (batch id reused with different content) |
| `1` | simulated crash (`ingest --crash`), or an internal error |

On failure stdout is `{"ok": false, "error": {...}}` with a stable `code` and a
JSON `path`.

---

## Case file format

A case is a JSON object. Keys beginning with `_` are ignored, so you can add
comments. Unknown keys are a schema error.

```jsonc
{
  "sim_version": 1,                 // must be 1
  "case_id": "two-tenants-one-connection",
  "config": {                        // optional; defaults shown
    "auth_ttl_ms": 60000,            // an auth older than this must be redone
    "lease_ttl_ms": 1000000000,      // control-lease lifetime
    "purge_cooldown_ms": 50,         // pause after a failed purge/close
    "max_purge_attempts": 3,
    "max_auth_attempts": 3,
    "initial_process": "p0",         // holder of the first lease
    "auto_lease": true               // acquire the first lease automatically
  },
  "connections": [{"connection_id": "c1", "generation": 1}],
  "tenants": [
    {"tenant": "alpha", "peer_identity_version": 1,
     "trust_config_version": 1, "purpose_level": "interactive"}
  ],
  "batches": [
    {
      "batch_id": "b1",
      "at": 0,                       // logical time; must not go backwards
      "events": [ /* see below */ ]
    }
  ]
}
```

`batches` may be replaced by a flat `events` list where each event carries its
own `at`; events with equal `at` form one batch. Providing both `events` and
`batches` is an error.

### Events

Every event has `event_id` (unique across the file) and `type`. Fields below are
exact; extra fields are a schema error.

| `type` | Fields | Effect |
|---|---|---|
| `register_connection` | `connection_id`, `generation?` | Create or replace a connection record. |
| `register_tenant` | `tenant`, `peer_identity_version?`, `trust_config_version?`, `purpose_level?` | Create/update a tenant's context. |
| `set_peer_identity_version` | `tenant`, `version` | Bump one context part. |
| `set_trust_config_version` | `tenant`, `version` | Bump one context part. |
| `set_purpose_level` | `tenant`, `purpose_level` | Bump one context part. |
| `revoke_peer` | `tenant`, `peer_identity_version?`, `reason?` | Terminate that tenant's outstanding and in-use requests and close its connections. |
| `acquire_lease` | `process_id` | Take the control lease, or renew if already held. A held live lease by another process is rejected (`LEASE_NOT_HELD`). |
| `renew_lease` | `process_id` | Extend a lease the process already holds. |
| `release_lease` | `process_id` | Give up the lease. |
| `worker_exit` | `process_id`, `reason?` | The process dies: its in-flight operations become `PENDING_ACK`, and its lease is dropped. No request is completed or receipted. |
| `worker_resume` | `process_id` | Bookkeeping marker. |
| `submit_request` | `request_id`, `tenant`, `candidates[]`, `group_size?`, `purpose_level?`, `peer_identity_version?`, `trust_config_version?` | Ask for connections. `group_size` defaults to the candidate count. |
| `cancel_request` | `request_id`, `reason?` | Cancel if not terminal; releases held connections and emits `CANCELLED`. |
| `complete_request` | `request_id`, `outcome`, `payload?`, `reason?` | `success`/`failure` for an `IN_USE` request. |
| `purge_complete` | `connection_id`, `generation`, `op_id`, `expected_config_version`, `lease_generation`, `outcome`, `reason?` | Asynchronous purge result: `ok`/`fail`. |
| `auth_complete` | (as purge) + `context?`, `expires_at?` | Asynchronous auth result; `context` defaults to the request's tenant context. |
| `retry_op` | `op_id` | Idempotently resend a `PENDING_ACK` operation under the current lease. |
| `tick` | — | Advance logical time and re-run the scheduling pass. |

### The security context

`{tenant, peer_identity_version, trust_config_version, purpose_level}` — four
parts, all compared. A connection is **directly reusable** only if all four
match *and* `now <= auth_expires_at`. A mismatch or a lapsed deadline sends it
through purge-then-auth with the *current* versions. `set_purpose_level` alone
is enough to force a purge for the next tenant that wants the connection.

---

## Semantics

### Lifecycle and hand-off

```
IDLE ──reserve──▶ RESERVED ──auth ok──▶ IN_USE ──release──▶ IDLE
  │                   │
  │                   └─ if context stale: PENDING_PURGE ─purge ok─▶ AUTHENTICATING ─auth ok─▶ RESERVED
  └─ revoke / max attempts / auth failure ──▶ CLOSED   (+1 to generation)
```

* The **generation** counts use cycles. It advances only when a connection is
  closed and its cycle ends; a purge or auth within a live cycle keeps it, which
  is what lets late messages be told apart from current ones.
* Clearing the context does **not** end the cycle, so a stale purge
  acknowledgement is recognised as stale by its operation id and generation.

### Scheduling and groups

`ingest` runs one scheduling pass after all events of a batch are folded, so a
batch is the unit of atomicity.

* Pending requests are considered in submission order.
* Candidates are classified per pass: reusable (all four parts match and auth is
  live), purge-needed (stale context), auth-needed (unused cycle), busy, or
  unusable.
* A request is committed **all at once or not at all**. If a group of three has
  only two usable candidates it takes *nothing*; it never parks a partial match
  and block another queue with it. If a later request can use one of those
  candidates, it does.
* Ties within a class are broken by lexicographic connection id.
* When a request cannot be committed, why every candidate failed is recorded
  (`INSUFFICIENT_CANDIDATES`, `CANDIDATES_BUSY`, `NO_ACTIVE_LEASE`, ...). It is
  retried on the next scheduling pass, i.e. at the next batch or state change,
  in submission order.

### Lease and takeover

Mutating the pool requires a live control lease. Its `generation` rises on each
*new* acquisition (exercising a preemption), and async completions must carry the
**current** generation. When a process dies or its lease lapses:

1. Its in-flight operations move to `PENDING_ACK` — recorded as incomplete,
   never as done.
2. A new process takes over; a `retry_op` resends the command idempotently.
3. The stale completions of the old process are rejected with `STALE_LEASE` (or
   `STALE_OPERATION`/`STALE_GENERATION`) — they cannot move the connection back
   to an obsolete state.

### Revocation

`revoke_peer` terminates every outstanding and in-use request of that tenant and
closes its connections. Each gets exactly one `TERMINATED` receipt, and no
`SUCCEEDED` can follow. Other tenants are untouched.

### Stable, idempotent receipts

Each request gets at most one terminal receipt (`SUCCEEDED`, `TERMINATED`,
`CANCELLED`, `FAILED`). Repeating an operation returns the original; a
completion after termination is a no-op.

### SQLite atomicity

One batch is one transaction. The state document, the mirrored lease row, the
transactional `pending_ack` list, the receipts table and the applied-batch log
commit together or not at all. If the process dies before COMMIT nothing is
visible; if it dies after COMMIT the batch is durable even though the response
was lost, and re-ingesting the same batches returns the stored results and the
same receipts (no duplicate terminal receipt).

---

## Output shapes

`ingest`:

```jsonc
{
  "command": "ingest",
  "case_id": "...",
  "now": 60,
  "batches": [
    {
      "batch_id": "b04-submit-beta",
      "at": 30,
      "replayed": false,
      "events": [{"event_id": "...", "type": "...", "outcome": {...}}],
      "schedule": [{"request_id": "r-beta", "action": "committed",
                    "reused": [], "purge": ["c1"], "auth": [],
                    "connections": ["c1"]}],
      "rejections": []
    }
  ],
  "final_state": {
    "now": 60,
    "lease": {"holder": "...", "generation": 1, "expires_at": 5000, "active": true},
    "connections": {"c1": {"generation": 1, "lifecycle": "IDLE",
                           "context": {...}, "auth_expires_at": 1060,
                           "request_id": null, "op": null, ...}},
    "requests": {"r-alpha": {"state": "SUCCEEDED", "reserved": [], ...}},
    "receipts": {"r-alpha": ["RESERVED", "SUCCEEDED"]},
    "pending_ack": [],
    "exits": []
  }
}
```

`schedule` action is one of `committed`, `deferred` (with a `reason`),
`already_committed`, `in_use`, `terminated`.

A rejected async completion reports `{"result": "rejected", "rejected":
"<CODE>", "why": "..."}`. Codes: `STALE_GENERATION`, `STALE_OPERATION`,
`STALE_LEASE`, `STALE_EXPECTED_VERSION`, `STALE_CONFIG`, `UNKNOWN_OP`.

`explain` returns `transitions`, `blocked_history`, `receipts`, `rejections`,
`late_completions` (with a plain-language explanation per rejection) and
`connection_history`.

---

## Input errors

Validation is strict and points at the exact field. Example:

```json
{"ok": false, "error": {
  "code": "E_SCHEMA_MISSING", "path": "$.batches[0].events[0].request_id",
  "message": "required field 'request_id' is missing",
  "missing_field": "request_id"}}
```

| Code | Meaning |
|---|---|
| `E_JSON_PARSE` | file missing / not UTF-8 / invalid JSON (with line/column) |
| `E_SCHEMA_TYPE` | wrong JSON type (`expected`/`got`) |
| `E_SCHEMA_MISSING` | required field absent (`missing_field`) |
| `E_SCHEMA_ENUM` | value not in the allowed set (`allowed`) |
| `E_SCHEMA_RANGE` | number out of range, time went backwards, etc. |
| `E_SCHEMA_DUPLICATE` | duplicate id or key |
| `E_SCHEMA_UNKNOWN_KEY` | unknown field (`unknown_field`) |
| `E_REFERENCE` | unknown tenant / connection / request / op |
| `E_BATCH_CONFLICT` | batch id reused with different content |
| `E_DB_STATE` | database incompatible with this case's config |
| `E_USAGE` | bad command-line usage |

---

## Shipped example cases

| File | Demonstrates |
|---|---|
| `smoke.json` | Minimal submit → auth → complete. |
| `two_tenants_one_connection.json` | Two tenants in turn on one connection; purge then auth at hand-off. |
| `late_completion.json` | Cancelled request's connection reused; the old auth completion arrives late and is rejected. |
| `auth_and_revoke_same_batch.json` | Auth completion and revocation in one batch; alpha terminated once, beta untouched. |
| `takeover.json` | p0 dies mid-purge; p1 takes over; stale lease message rejected while the op is live, stale op id rejected after. |
| `group_reservation.json` | Group of three with one busy candidate takes nothing; a later single request proceeds; the group commits when the last frees up. |
| `purpose_level_change.json` | A purpose-level change forces purge-then-auth. |
| `purge_cooldown_failure.json` | A failed purge cools down, then retries after the cooldown. |
| `crash_and_resume.json` | Worker exit → `PENDING_ACK` → idempotent resend → duplicate completion yields one success. |
| `barrier_takeover_revoke.json` | Takeover and revocation at the same logical instant. |
| `barrier_two_processes.json` | Two processes acquire the same lease in one barrier: only one wins. |
| `barrier_auth_then_complete.json` | Auth completion and request completion in one barrier do not interleave unsafely. |

Reproduce any of them:

```bat
python -m solution verify cases\auth_and_revoke_same_batch.json --json-indent 0
python -m solution explain cases\takeover.json --request r-beta
```

---

## Tests

```bat
python -m unittest discover -s tests -v
```

* `test_cases.py` — end-to-end assertions on every shipped case.
* `test_engine.py` — auth expiry, trust-config change, queue order, tie-break,
  blocked reasons, cancel-and-release.
* `test_concurrency.py` — barrier-built races: takeover+revoke in one instant,
  two processes racing one lease, a group with one free candidate, a blocked
  first batch while a later advances, and a same-batch auth/complete that must
  not interleave.
* `test_store.py` — SQLite atomicity, crash-before/after-commit, idempotent
  re-ingest, no duplicate terminal receipts.
* `test_schema.py` — every error code and JSON path.
* `test_idempotency.py` — batch and request replay/conflict; read commands.
* `test_verify.py` — byte-level determinism, and the verifier's own ability to
  detect cross-tenant reuse and duplicate terminal receipts.

The verifier (`solution/verify.py`) does **not** reuse the scheduler: it replays
the case prefix by prefix and asks, independently, whether any connection that
still carries tenant A's context was ever reserved or in use for tenant B. That
is the cross-tenant-reuse check the task asks for.

---

## Layout

```
solution/
  __main__.py   python -m solution entry point
  cli.py        argparse front end, exit codes, JSON emission
  schema.py     case loading and structural validation with JSON paths
  model.py      lifecycle/state vocabulary, context helpers, digests
  state.py      construction of the pool state document
  engine.py     the pure, deterministic state machine
  store.py      SQLite persistence: atomic batch + lease + pending_ack + receipts
  runner.py     ingest / explain / recover / inspect / replay wiring
  verify.py     prefix replay and cross-tenant-reuse invariants
  crash.py      simulated process death
cases/          shipped example cases
tests/          unittest suite
```

No third-party packages, no network access, and no real proxy anywhere.
