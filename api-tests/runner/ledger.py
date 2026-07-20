from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class LedgerRow:
    operation_id: str
    path: str
    method: str
    case_id: str
    profile: str
    state: str = "planned"
    error_category: str | None = None
    error_message: str | None = None
    result: dict[str, Any] | None = None

    @property
    def key(self) -> str:
        return f"{self.method.upper()} {self.path} [{self.case_id}]"


class Ledger:
    def __init__(self, cases: list[Any]) -> None:
        self.rows = {
            f"{case.operation.operation_id}:{case.case_id}": LedgerRow(
                operation_id=case.operation.operation_id,
                path=case.operation.path,
                method=case.operation.method,
                case_id=case.case_id,
                profile=case.profile,
            )
            for case in cases
        }

    def row(self, case: Any) -> LedgerRow:
        return self.rows[f"{case.operation.operation_id}:{case.case_id}"]

    def transition(self, case: Any, state: str) -> None:
        self.row(case).state = state

    def fail(self, case: Any, category: str, message: str, result: dict[str, Any] | None = None) -> None:
        row = self.row(case)
        row.state = category
        row.error_category = category
        row.error_message = message
        row.result = result

    def complete(self, case: Any, result: dict[str, Any]) -> None:
        row = self.row(case)
        row.state = "checks_complete"
        row.result = result

    def finalize(self) -> bool:
        return all(row.state == "checks_complete" for row in self.rows.values())

    def summary(self) -> dict[str, int]:
        states: dict[str, int] = {}
        for row in self.rows.values():
            states[row.state] = states.get(row.state, 0) + 1
        return states

    def as_dict(self) -> list[dict[str, Any]]:
        return [
            {
                "operation_id": row.operation_id,
                "path": row.path,
                "method": row.method.upper(),
                "case_id": row.case_id,
                "profile": row.profile,
                "state": row.state,
                "error_category": row.error_category,
                "error_message": row.error_message,
                "result": row.result,
            }
            for row in self.rows.values()
        ]
