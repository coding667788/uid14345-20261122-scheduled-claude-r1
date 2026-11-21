"""Structured errors with stable codes and JSON field paths."""

from __future__ import annotations

# Exit codes used by the CLI.
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_INPUT = 2
EXIT_CONFLICT = 3


class SimError(Exception):
    """A deterministic, machine-readable failure.

    ``code``   stable error code (``E_SCHEMA_MISSING`` ...)
    ``path``   JSON path of the offending field (``$.events[2].request.tenant``)
    ``extra``  optional structured detail merged into the error payload
    """

    def __init__(
        self,
        code: str,
        path: str,
        message: str,
        extra: dict | None = None,
        exit_code: int = EXIT_INPUT,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.message = message
        self.extra = extra or {}
        self.exit_code = exit_code

    def as_payload(self) -> dict:
        payload = {
            "code": self.code,
            "path": self.path,
            "message": self.message,
        }
        payload.update(self.extra)
        return payload


# --- Canonical error codes -------------------------------------------------

E_JSON_PARSE = "E_JSON_PARSE"
E_SCHEMA_TYPE = "E_SCHEMA_TYPE"
E_SCHEMA_MISSING = "E_SCHEMA_MISSING"
E_SCHEMA_ENUM = "E_SCHEMA_ENUM"
E_SCHEMA_RANGE = "E_SCHEMA_RANGE"
E_SCHEMA_DUPLICATE = "E_SCHEMA_DUPLICATE"
E_SCHEMA_UNKNOWN_KEY = "E_SCHEMA_UNKNOWN_KEY"
E_BATCH_CONFLICT = "E_BATCH_CONFLICT"
E_UNKNOWN_EVENT = "E_UNKNOWN_EVENT"
E_REFERENCE = "E_REFERENCE"
E_COMMAND = "E_COMMAND"
E_LEASE_CONFLICT = "E_LEASE_CONFLICT"
E_DB_STATE = "E_DB_STATE"
E_USAGE = "E_USAGE"
