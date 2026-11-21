"""Simulated process death, used to exercise crash/recovery paths."""

from __future__ import annotations


class SimulatedCrash(Exception):
    """Raised to emulate a worker dying at a named point in the batch.

    ``committed`` says whether the SQLite transaction had already committed,
    which decides what recovery may legitimately observe.
    """

    def __init__(self, *, stage: str, batch_id: str, committed: bool,
                 note: str = "") -> None:
        super().__init__(f"simulated crash at {stage} during {batch_id}")
        self.stage = stage
        self.batch_id = batch_id
        self.committed = committed
        self.note = note

    def as_payload(self) -> dict:
        return {
            "code": "SIMULATED_CRASH",
            "stage": self.stage,
            "batch_id": self.batch_id,
            "committed": self.committed,
            "note": self.note,
        }
