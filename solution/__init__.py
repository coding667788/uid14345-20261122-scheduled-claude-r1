"""Offline service-proxy connection-pool simulator.

Public entry point: ``python -m solution <command> CASE.json``.
Everything runs on the standard library and a deterministic logical clock;
no network, no real proxy, no wall-clock time.
"""

__all__ = ["__version__"]
__version__ = "1.0.0"
