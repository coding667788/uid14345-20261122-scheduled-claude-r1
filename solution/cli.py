"""Command-line interface: ``python -m solution <command> CASE.json [opts]``.

Commands
--------
``ingest``   run the case's batches against the pool (optionally ``--db``)
``explain``  replay the case and explain one request (``--request``) or all
``recover``  report what a restart sees: applied batches, receipts, pending acks
``inspect``  dump the terminal state, operations and rejection log
``verify``   replay twice and check determinism plus no cross-tenant reuse
``replay``   alias of ``verify`` that also prints the final state

Exit codes: 0 ok, 2 input/schema error, 3 conflict, 1 internal error.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .crash import SimulatedCrash
from .errors import (
    E_COMMAND,
    E_USAGE,
    EXIT_INTERNAL,
    EXIT_OK,
    SimError,
)
from .runner import (
    cmd_explain,
    cmd_ingest,
    cmd_inspect,
    cmd_recover,
    cmd_replay,
)
from .schema import load_case
from .verify import verify_prefix_invariants

COMMANDS = ("ingest", "explain", "recover", "inspect", "verify", "replay")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse path
        raise SimError(E_USAGE, "--args", message)


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="python -m solution",
                     description="Offline service-proxy connection-pool "
                                 "simulator")
    parser.add_argument("--version", action="version",
                        version=f"solution {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("case", metavar="CASE.json", help="case file to run")
        p.add_argument("--db", metavar="DB.sqlite", default=None,
                       help="persistent pool database")
        p.add_argument("--json-indent", type=int, default=2,
                       help="pretty-print JSON with this indent (0 = compact)")

    p_ingest = sub.add_parser("ingest", help="apply the case batches")
    common(p_ingest)
    p_ingest.add_argument("--crash", choices=("before-commit", "after-commit"),
                          default=None,
                          help="simulate a process death during a batch")
    p_ingest.add_argument("--crash-batch", type=int, default=0,
                          help="index of the batch that dies")

    p_explain = sub.add_parser("explain", help="explain request transitions")
    common(p_explain)
    p_explain.add_argument("--request", default=None,
                           help="request id (omit to explain all)")

    for name, help_text in (("recover", "report restart-visible state"),
                            ("inspect", "dump terminal state"),
                            ("verify", "check determinism and invariants"),
                            ("replay", "replay twice and print final state")):
        p = sub.add_parser(name, help=help_text)
        common(p)
    return parser


def _emit(payload: dict, indent: int) -> None:
    if indent and indent > 0:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                          indent=indent)
    else:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"))
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def _dispatch(args) -> dict:
    if args.command is None:
        raise SimError(E_USAGE, "--command",
                       "a command is required: " + ", ".join(COMMANDS),
                       {"commands": list(COMMANDS)})
    case = load_case(args.case)
    db = args.db

    if args.command == "ingest":
        return cmd_ingest(case, db, crash=args.crash,
                          crash_batch=args.crash_batch)
    if args.command == "explain":
        return cmd_explain(case, db, request_id=args.request)
    if args.command == "recover":
        return cmd_recover(case, db)
    if args.command == "inspect":
        return cmd_inspect(case, db)
    if args.command == "verify":
        return verify_prefix_invariants(case, db)
    if args.command == "replay":
        return cmd_replay(case, db)
    raise SimError(E_COMMAND, "--command", f"unknown command {args.command!r}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SimError as exc:
        return _report(exc, indent=2)
    except SystemExit as exc:  # --help / --version
        return int(exc.code or 0)

    indent = getattr(args, "json_indent", 2)
    try:
        payload = _dispatch(args)
    except SimulatedCrash as crash:
        _emit({"ok": False, "crashed": True,
               "error": crash.as_payload()}, indent)
        return EXIT_INTERNAL
    except SimError as exc:
        return _report(exc, indent=indent)
    except Exception as exc:  # pragma: no cover - defensive
        _emit({"ok": False, "error": {
            "code": "E_INTERNAL", "path": "$",
            "message": f"{type(exc).__name__}: {exc}"}}, indent)
        return EXIT_INTERNAL
    _emit(payload, indent)
    return EXIT_OK


def _report(exc: SimError, indent: int) -> int:
    _emit({"ok": False, "error": exc.as_payload()}, indent)
    return exc.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
