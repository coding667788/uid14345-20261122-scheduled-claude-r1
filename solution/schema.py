"""Case-file parsing and structural validation.

Every failure raises :class:`~solution.errors.SimError` with a stable code and
a JSON path pointing at the offending field, e.g.
``$.batches[1].events[3].candidates[0]``.
"""

from __future__ import annotations

import json
from typing import Any

from .errors import (
    E_JSON_PARSE,
    E_SCHEMA_DUPLICATE,
    E_SCHEMA_ENUM,
    E_SCHEMA_MISSING,
    E_SCHEMA_RANGE,
    E_SCHEMA_TYPE,
    E_SCHEMA_UNKNOWN_KEY,
    SimError,
)
from .model import DEFAULT_PURPOSE

# --- Event catalogue -------------------------------------------------------
# name -> set of allowed fields (``event_id``/``at``/``batch``/``type`` are
# added for every event by ``_COMMON``).
_COMMON = {"event_id", "at", "batch", "type"}

EVENT_FIELDS: dict[str, set[str]] = {
    "register_connection": {"connection_id", "generation"},
    "register_tenant": {
        "tenant", "peer_identity_version", "trust_config_version", "purpose_level",
    },
    "set_peer_identity_version": {"tenant", "version"},
    "set_trust_config_version": {"tenant", "version"},
    "set_purpose_level": {"tenant", "purpose_level"},
    "revoke_peer": {"tenant", "peer_identity_version", "reason"},
    "acquire_lease": {"process_id"},
    "renew_lease": {"process_id"},
    "release_lease": {"process_id"},
    "worker_exit": {"process_id", "reason"},
    "worker_resume": {"process_id"},
    "submit_request": {
        "request_id", "tenant", "candidates", "group_size", "purpose_level",
        "peer_identity_version", "trust_config_version",
    },
    "cancel_request": {"request_id", "reason"},
    "complete_request": {"request_id", "outcome", "payload"},
    "purge_complete": {
        "connection_id", "generation", "op_id", "expected_config_version",
        "lease_generation", "outcome", "reason",
    },
    "auth_complete": {
        "connection_id", "generation", "op_id", "expected_config_version",
        "lease_generation", "outcome", "reason", "context", "expires_at",
    },
    "retry_op": {"op_id"},
    "tick": set(),
}

EVENT_TYPES = tuple(EVENT_FIELDS)

_TOP_KEYS = {"sim_version", "case_id", "description", "config", "connections",
             "tenants", "events", "batches", "clock"}
_CONFIG_KEYS = {
    "auth_ttl_ms", "lease_ttl_ms", "purge_cooldown_ms",
    "max_purge_attempts", "max_auth_attempts",
    "initial_process", "auto_lease",
}
_CONN_KEYS = {"connection_id", "generation"}
_TENANT_KEYS = {
    "tenant", "peer_identity_version", "trust_config_version", "purpose_level",
}
_BATCH_KEYS = {"batch_id", "at", "events", "note"}

DEFAULT_CONFIG = {
    "auth_ttl_ms": 60000,
    "lease_ttl_ms": 1_000_000_000,
    "purge_cooldown_ms": 50,
    "max_purge_attempts": 3,
    "max_auth_attempts": 3,
    "initial_process": "p0",
    "auto_lease": True,
}


# --- primitive checks ------------------------------------------------------

def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _where(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else f"$.{key}"


def _index(prefix: str, i: int) -> str:
    return f"{prefix}[{i}]"


def _fail_type(path: str, expected: str, value: Any) -> None:
    raise SimError(
        E_SCHEMA_TYPE, path,
        f"expected {expected}, got {type(value).__name__}",
        {"expected": expected, "got": type(value).__name__},
    )


def _object(value: Any, path: str) -> dict:
    if not isinstance(value, dict):
        _fail_type(path, "object", value)
    return value


def _array(value: Any, path: str) -> list:
    if not isinstance(value, list):
        _fail_type(path, "array", value)
    return value


def _string(value: Any, path: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str):
        _fail_type(path, "string", value)
    if nonempty and not value.strip():
        raise SimError(E_SCHEMA_RANGE, path, "string must not be empty")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:  # pragma: no cover - defensive
        raise SimError(E_SCHEMA_TYPE, path, f"string is not valid UTF-8: {exc}")
    return value


def _integer(value: Any, path: str, *, minimum: int | None = None) -> int:
    if not _is_int(value):
        _fail_type(path, "integer", value)
    if minimum is not None and value < minimum:
        raise SimError(
            E_SCHEMA_RANGE, path, f"must be >= {minimum}", {"minimum": minimum}
        )
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail_type(path, "boolean", value)
    return value


def _enum(value: Any, path: str, allowed: tuple[str, ...]) -> str:
    if not isinstance(value, str):
        _fail_type(path, "string", value)
    if value not in allowed:
        raise SimError(
            E_SCHEMA_ENUM, path,
            f"must be one of {list(allowed)}", {"allowed": list(allowed)},
        )
    return value


def _check_keys(obj: dict, allowed: set[str], path: str) -> None:
    for key in obj:
        if key not in allowed:
            raise SimError(
                E_SCHEMA_UNKNOWN_KEY, _where(path, key),
                f"unknown field {key!r}",
                {"unknown_field": key},
            )


def _require(obj: dict, key: str, path: str) -> Any:
    if key not in obj or obj[key] is None:
        raise SimError(
            E_SCHEMA_MISSING, _where(path, key),
            f"required field {key!r} is missing", {"missing_field": key},
        )
    return obj[key]


# --- case-level ------------------------------------------------------------

def load_case(path: str) -> dict:
    try:
        with open(path, "rb") as fh:
            raw_bytes = fh.read()
    except OSError as exc:
        raise SimError(
            E_JSON_PARSE, "$", f"cannot read case file: {exc.strerror or exc}",
            {"file": path},
        )
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SimError(
            E_JSON_PARSE, "$", f"case file is not valid UTF-8: {exc}",
            {"file": path},
        )
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SimError(
            E_JSON_PARSE, "$", f"invalid JSON at line {exc.lineno} column {exc.colno}: {exc.msg}",
            {"line": exc.lineno, "column": exc.colno, "file": path},
        )
    return normalize_case(raw)


def normalize_case(raw: Any) -> dict:
    raw = _object(raw, "$")
    _check_keys(raw, _TOP_KEYS, "$")

    sim_version = raw.get("sim_version", 1)
    sim_version = _integer(sim_version, "$.sim_version", minimum=1)
    if sim_version != 1:
        raise SimError(
            E_SCHEMA_RANGE, "$.sim_version",
            "only sim_version 1 is supported", {"supported": [1]},
        )

    clock = _integer(raw.get("clock", 0), "$.clock", minimum=0)

    config = dict(DEFAULT_CONFIG)
    if "config" in raw and raw["config"] is not None:
        cfg = _object(raw["config"], "$.config")
        _check_keys(cfg, _CONFIG_KEYS, "$.config")
        if "auth_ttl_ms" in cfg:
            config["auth_ttl_ms"] = _integer(cfg["auth_ttl_ms"], "$.config.auth_ttl_ms", minimum=1)
        if "lease_ttl_ms" in cfg:
            config["lease_ttl_ms"] = _integer(cfg["lease_ttl_ms"], "$.config.lease_ttl_ms", minimum=1)
        if "purge_cooldown_ms" in cfg:
            config["purge_cooldown_ms"] = _integer(cfg["purge_cooldown_ms"], "$.config.purge_cooldown_ms", minimum=0)
        if "max_purge_attempts" in cfg:
            config["max_purge_attempts"] = _integer(cfg["max_purge_attempts"], "$.config.max_purge_attempts", minimum=1)
        if "max_auth_attempts" in cfg:
            config["max_auth_attempts"] = _integer(cfg["max_auth_attempts"], "$.config.max_auth_attempts", minimum=1)
        if "initial_process" in cfg:
            config["initial_process"] = _string(cfg["initial_process"], "$.config.initial_process")
        if "auto_lease" in cfg:
            config["auto_lease"] = _boolean(cfg["auto_lease"], "$.config.auto_lease")

    connections = _normalize_connections(raw.get("connections", []))
    tenants = _normalize_tenants(raw.get("tenants", []))
    batches = _normalize_batches(raw)
    _check_global_duplicates(batches)

    return {
        "sim_version": sim_version,
        "case_id": raw.get("case_id") if isinstance(raw.get("case_id"), str) else None,
        "clock": clock,
        "config": config,
        "connections": connections,
        "tenants": tenants,
        "batches": batches,
        "description": raw.get("description") if isinstance(raw.get("description"), str) else None,
    }


def _normalize_connections(value: Any) -> list[dict]:
    arr = _array(value, "$.connections")
    out: list[dict] = []
    seen: dict[str, int] = {}
    for i, item in enumerate(arr):
        path = _index("$.connections", i)
        obj = _object(item, path)
        _check_keys(obj, _CONN_KEYS, path)
        cid = _string(_require(obj, "connection_id", path), _where(path, "connection_id"))
        if cid in seen:
            raise SimError(
                E_SCHEMA_DUPLICATE, _where(path, "connection_id"),
                f"duplicate connection_id {cid!r}",
                {"duplicate_of": _index("$.connections", seen[cid])},
            )
        seen[cid] = i
        gen = obj.get("generation", 1)
        gen = _integer(gen, _where(path, "generation"), minimum=1)
        out.append({"connection_id": cid, "generation": gen})
    return out


def _normalize_tenants(value: Any) -> list[dict]:
    arr = _array(value, "$.tenants")
    out: list[dict] = []
    seen: dict[str, int] = {}
    for i, item in enumerate(arr):
        path = _index("$.tenants", i)
        obj = _object(item, path)
        _check_keys(obj, _TENANT_KEYS, path)
        tenant = _string(_require(obj, "tenant", path), _where(path, "tenant"))
        if tenant in seen:
            raise SimError(
                E_SCHEMA_DUPLICATE, _where(path, "tenant"),
                f"duplicate tenant {tenant!r}",
                {"duplicate_of": _index("$.tenants", seen[tenant])},
            )
        seen[tenant] = i
        peer = obj.get("peer_identity_version", 1)
        trust = obj.get("trust_config_version", 1)
        purpose = obj.get("purpose_level", DEFAULT_PURPOSE)
        out.append({
            "tenant": tenant,
            "peer_identity_version": _integer(peer, _where(path, "peer_identity_version"), minimum=1),
            "trust_config_version": _integer(trust, _where(path, "trust_config_version"), minimum=1),
            "purpose_level": _string(purpose, _where(path, "purpose_level")),
        })
    return out


def _normalize_batches(raw: dict) -> list[dict]:
    has_batches = "batches" in raw and raw["batches"] is not None
    has_events = "events" in raw and raw["events"] is not None
    if has_batches and has_events:
        raise SimError(
            E_SCHEMA_DUPLICATE, "$",
            "provide either 'events' (flat) or 'batches', not both",
            {"conflicting_fields": ["events", "batches"]},
        )
    if has_batches:
        arr = _array(raw["batches"], "$.batches")
        batches: list[dict] = []
        seen: dict[str, int] = {}
        last_at = raw.get("clock", 0)
        for i, item in enumerate(arr):
            path = _index("$.batches", i)
            obj = _object(item, path)
            _check_keys(obj, _BATCH_KEYS, path)
            at = _integer(_require(obj, "at", path), _where(path, "at"), minimum=0)
            bid = obj.get("batch_id")
            if bid is None:
                bid = f"b{i + 1}"
            else:
                bid = _string(bid, _where(path, "batch_id"))
            if bid in seen:
                raise SimError(
                    E_SCHEMA_DUPLICATE, _where(path, "batch_id"),
                    f"duplicate batch_id {bid!r}",
                    {"duplicate_of": _index("$.batches", seen[bid])},
                )
            seen[bid] = i
            if _is_int(last_at) and at < last_at:
                raise SimError(
                    E_SCHEMA_RANGE, _where(path, "at"),
                    "batch time must not go backwards",
                    {"previous_at": last_at},
                )
            last_at = at
            events_raw = _array(_require(obj, "events", path), _where(path, "events"))
            events = [
                _normalize_event(ev, _index(_where(path, "events"), j), at, path)
                for j, ev in enumerate(events_raw)
            ]
            batches.append({"batch_id": bid, "at": at, "events": events})
        return batches

    arr = _array(raw.get("events", []), "$.events")
    events = [
        _normalize_event(ev, _index("$.events", j), None, "$.events")
        for j, ev in enumerate(arr)
    ]
    return _group_flat_events(events, start_clock=_integer(raw.get("clock", 0), "$.clock", minimum=0))


def _group_flat_events(events: list[dict], *, start_clock: int) -> list[dict]:
    batches: list[dict] = []
    last_key: tuple | None = None
    last_at = start_clock
    for ev in events:
        key = ("explicit", ev["_batch"]) if ev.get("_batch") else ("at", ev["at"])
        if key != last_key:
            at = ev["at"]
            if at < last_at:
                raise SimError(
                    E_SCHEMA_RANGE, ev["_path"] + ".at",
                    "event time must not go backwards",
                    {"previous_at": last_at},
                )
            last_at = at
            bid = ev["_batch"] if ev.get("_batch") else f"b{len(batches) + 1}"
            batches.append({"batch_id": bid, "at": at, "events": []})
            last_key = key
        batch = batches[-1]
        if ev["at"] != batch["at"]:
            raise SimError(
                E_SCHEMA_RANGE, ev["_path"] + ".at",
                "events in the same batch must share one logical time",
                {"batch_at": batch["at"]},
            )
        batch["events"].append(ev)
    for b in batches:
        for ev in b["events"]:
            ev.pop("_batch", None)
            ev.pop("_path", None)
    return batches


def _normalize_event(ev: Any, path: str, batch_at: int | None, batch_path: str) -> dict:
    obj = _object(ev, path)
    etype = _enum(_require(obj, "type", path), _where(path, "type"), EVENT_TYPES)
    allowed = _COMMON | EVENT_FIELDS[etype]
    _check_keys(obj, allowed, path)

    event_id = _string(_require(obj, "event_id", path), _where(path, "event_id"))
    at = obj.get("at", batch_at)
    if at is None:
        raise SimError(
            E_SCHEMA_MISSING, _where(path, "at"),
            "field 'at' is required for flat events", {"missing_field": "at"},
        )
    at = _integer(at, _where(path, "at"), minimum=0)
    if batch_at is not None and at != batch_at:
        raise SimError(
            E_SCHEMA_RANGE, _where(path, "at"),
            "event time must equal its batch time",
            {"batch_at": batch_at},
        )

    out: dict[str, Any] = {"event_id": event_id, "at": at, "type": etype}
    if "batch" in obj and obj["batch"] is not None:
        out["_batch"] = _string(obj["batch"], _where(path, "batch"))
    out["_path"] = path

    p = lambda k: _where(path, k)  # noqa: E731

    if etype == "register_connection":
        out["connection_id"] = _string(_require(obj, "connection_id", path), p("connection_id"))
        out["generation"] = _integer(obj.get("generation", 1), p("generation"), minimum=1)

    elif etype == "register_tenant":
        out["tenant"] = _string(_require(obj, "tenant", path), p("tenant"))
        out["peer_identity_version"] = _integer(
            obj.get("peer_identity_version", 1), p("peer_identity_version"), minimum=1)
        out["trust_config_version"] = _integer(
            obj.get("trust_config_version", 1), p("trust_config_version"), minimum=1)
        out["purpose_level"] = _string(obj.get("purpose_level", DEFAULT_PURPOSE), p("purpose_level"))

    elif etype in ("set_peer_identity_version", "set_trust_config_version"):
        out["tenant"] = _string(_require(obj, "tenant", path), p("tenant"))
        out["version"] = _integer(_require(obj, "version", path), p("version"), minimum=1)

    elif etype == "set_purpose_level":
        out["tenant"] = _string(_require(obj, "tenant", path), p("tenant"))
        out["purpose_level"] = _string(_require(obj, "purpose_level", path), p("purpose_level"))

    elif etype == "revoke_peer":
        out["tenant"] = _string(_require(obj, "tenant", path), p("tenant"))
        if "peer_identity_version" in obj and obj["peer_identity_version"] is not None:
            out["peer_identity_version"] = _integer(
                obj["peer_identity_version"], p("peer_identity_version"), minimum=1)
        if "reason" in obj and obj["reason"] is not None:
            out["reason"] = _string(obj["reason"], p("reason"), nonempty=False)

    elif etype in ("acquire_lease", "renew_lease", "release_lease",
                   "worker_exit", "worker_resume"):
        out["process_id"] = _string(_require(obj, "process_id", path), p("process_id"))
        if "reason" in obj and obj["reason"] is not None:
            out["reason"] = _string(obj["reason"], p("reason"), nonempty=False)

    elif etype == "submit_request":
        out["request_id"] = _string(_require(obj, "request_id", path), p("request_id"))
        out["tenant"] = _string(_require(obj, "tenant", path), p("tenant"))
        cands = _array(_require(obj, "candidates", path), p("candidates"))
        if not cands:
            raise SimError(E_SCHEMA_RANGE, p("candidates"),
                           "must list at least one candidate connection")
        seen: dict[str, int] = {}
        norm_c: list[str] = []
        for i, c in enumerate(cands):
            cpath = _index(p("candidates"), i)
            cid = _string(c, cpath)
            if cid in seen:
                raise SimError(E_SCHEMA_DUPLICATE, cpath,
                               f"duplicate candidate {cid!r}",
                               {"duplicate_of": _index(p("candidates"), seen[cid])})
            seen[cid] = i
            norm_c.append(cid)
        out["candidates"] = norm_c
        if "group_size" in obj and obj["group_size"] is not None:
            gs = _integer(obj["group_size"], p("group_size"), minimum=1)
            if gs > len(norm_c):
                raise SimError(
                    E_SCHEMA_RANGE, p("group_size"),
                    "group_size exceeds the number of candidates",
                    {"group_size": gs, "candidates": len(norm_c)},
                )
            out["group_size"] = gs
        else:
            out["group_size"] = len(norm_c)
        if "purpose_level" in obj and obj["purpose_level"] is not None:
            out["purpose_level"] = _string(obj["purpose_level"], p("purpose_level"))
        for key in ("peer_identity_version", "trust_config_version"):
            if key in obj and obj[key] is not None:
                out[key] = _integer(obj[key], p(key), minimum=1)

    elif etype == "cancel_request":
        out["request_id"] = _string(_require(obj, "request_id", path), p("request_id"))
        if "reason" in obj and obj["reason"] is not None:
            out["reason"] = _string(obj["reason"], p("reason"), nonempty=False)

    elif etype == "complete_request":
        out["request_id"] = _string(_require(obj, "request_id", path), p("request_id"))
        out["outcome"] = _enum(_require(obj, "outcome", path), p("outcome"), ("success", "failure"))
        if "payload" in obj and obj["payload"] is not None:
            if not isinstance(obj["payload"], (dict, list, str, int, float, bool)):
                _fail_type(p("payload"), "JSON value", obj["payload"])
            out["payload"] = obj["payload"]
        if "reason" in obj and obj["reason"] is not None:
            out["reason"] = _string(obj["reason"], p("reason"), nonempty=False)

    elif etype in ("purge_complete", "auth_complete"):
        out["connection_id"] = _string(_require(obj, "connection_id", path), p("connection_id"))
        out["generation"] = _integer(_require(obj, "generation", path), p("generation"), minimum=1)
        out["op_id"] = _string(_require(obj, "op_id", path), p("op_id"))
        out["expected_config_version"] = _integer(
            _require(obj, "expected_config_version", path),
            p("expected_config_version"), minimum=1)
        out["lease_generation"] = _integer(
            _require(obj, "lease_generation", path), p("lease_generation"), minimum=1)
        out["outcome"] = _enum(_require(obj, "outcome", path), p("outcome"), ("ok", "fail"))
        if "reason" in obj and obj["reason"] is not None:
            out["reason"] = _string(obj["reason"], p("reason"), nonempty=False)
        if etype == "auth_complete":
            if "context" in obj and obj["context"] is not None:
                out["context"] = _normalize_context(obj["context"], p("context"))
            if "expires_at" in obj and obj["expires_at"] is not None:
                out["expires_at"] = _integer(obj["expires_at"], p("expires_at"), minimum=0)

    elif etype == "retry_op":
        out["op_id"] = _string(_require(obj, "op_id", path), p("op_id"))

    elif etype == "tick":
        pass

    return out


def _normalize_context(value: Any, path: str) -> dict:
    obj = _object(value, path)
    _check_keys(obj, set(_TENANT_KEYS), path)
    return {
        "tenant": _string(_require(obj, "tenant", path), _where(path, "tenant")),
        "peer_identity_version": _integer(
            _require(obj, "peer_identity_version", path),
            _where(path, "peer_identity_version"), minimum=1),
        "trust_config_version": _integer(
            _require(obj, "trust_config_version", path),
            _where(path, "trust_config_version"), minimum=1),
        "purpose_level": _string(
            _require(obj, "purpose_level", path), _where(path, "purpose_level")),
    }


def _check_global_duplicates(batches: list[dict]) -> None:
    seen: dict[str, tuple[int, int]] = {}
    for bi, batch in enumerate(batches):
        for ei, ev in enumerate(batch["events"]):
            eid = ev["event_id"]
            if eid in seen:
                raise SimError(
                    E_SCHEMA_DUPLICATE, f"$.batches[{bi}].events[{ei}].event_id",
                    f"duplicate event_id {eid!r} in input",
                    {"duplicate_of": "batch %d event %d" % seen[eid]},
                )
            seen[eid] = (bi, ei)


def event_path(batch_index: int, event_index: int, field: str | None = None) -> str:
    path = f"$.batches[{batch_index}].events[{event_index}]"
    return f"{path}.{field}" if field else path
