from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from .ledger import Ledger


def write_reports(
    report_dir: Path,
    ledger: Ledger,
    target: str,
    base_url: str,
    reference_commit: str,
    profile: str,
    static_findings: list[dict[str, Any]],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    rows = ledger.as_dict()
    payload = {
        "target": target,
        "base_url": base_url,
        "reference_commit": reference_commit,
        "profile": profile,
        "cases": len(rows),
        "states": ledger.summary(),
        "static_findings": static_findings,
        "ledger": rows,
    }
    (report_dir / "report.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_junit(report_dir / "junit.xml", rows, profile)
    _write_summary(report_dir / "summary.txt", payload)


def _write_junit(path: Path, rows: list[dict[str, Any]], profile: str) -> None:
    failures = sum(row["state"] != "checks_complete" for row in rows)
    suite = ElementTree.Element(
        "testsuite",
        {
            "name": f"koios-preview-{profile}",
            "tests": str(len(rows)),
            "failures": str(failures),
            "errors": "0",
        },
    )
    for row in rows:
        case = ElementTree.SubElement(
            suite,
            "testcase",
            {
                "classname": row["operation_id"],
                "name": f"{row['method']} {row['path']} [{row['case_id']}]",
            },
        )
        if row["state"] != "checks_complete":
            failure = ElementTree.SubElement(
                case,
                "failure",
                {
                    "type": row.get("error_category") or row["state"],
                    "message": row.get("error_message") or row["state"],
                },
            )
            failure.text = json.dumps(row.get("result") or {}, sort_keys=True)
        output = ElementTree.SubElement(case, "system-out")
        output.text = json.dumps(row.get("result") or {}, sort_keys=True)
    ElementTree.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        f"target: {payload['target']}",
        f"base_url: {payload['base_url']}",
        f"reference_commit: {payload['reference_commit']}",
        f"profile: {payload['profile']}",
        f"cases: {payload['cases']}",
    ]
    for state, count in sorted(payload["states"].items()):
        lines.append(f"{state}: {count}")
    if payload["static_findings"]:
        lines.append("static_findings:")
        for finding in payload["static_findings"]:
            lines.append(f"- {finding['category']}: {finding['message']}")
    path.write_text("\n".join(lines) + "\n")
