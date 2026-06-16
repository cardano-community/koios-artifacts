#!/usr/bin/env python3
"""Generate network-specific OpenAPI specs from fragments and fixtures."""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import tempfile
import textwrap

SPECS_DIR = pathlib.Path(__file__).parent

PATH_ORDER = [
    "network", "epoch", "block", "transactions", "address", "account",
    "asset", "governance", "pool", "script", "ogmios",
]

SCHEMA_ORDER = [
    "network", "pool", "epoch", "block", "address", "account",
    "transactions", "asset", "governance", "script",
]

MACRO_RE = re.compile(r"^([ \t]*)#!macro:(\w+)!#", re.MULTILINE)
PLACEHOLDER_RE = re.compile(
    r"^(?P<indent>[ \t]*)#!(?P<key>\w+)!#",
    flags=re.MULTILINE,
)


def _read(path: pathlib.Path) -> str:
    return path.read_text().rstrip("\n")


def _load_simple_yaml(path: pathlib.Path) -> dict:
    """Load a shallow nested YAML dict (no lists) using stdlib only."""
    root: dict = {}
    stack: list[tuple[int, dict]] = [(-1, root)]

    for raw_line in path.read_text().splitlines():
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, _, value = raw_line.strip().partition(":")
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value:
            parent[key] = {}
            stack.append((indent, parent[key]))
        else:
            parent[key] = value.strip('"').strip("'")
    return root


def _fixtures_value(val: str) -> str:
    """Return fixture value as a string for placeholder substitution."""
    return str(val)


def load_macros() -> dict[str, str]:
    macros: dict[str, str] = {}
    content = _read(SPECS_DIR / "fragments" / "macros.yaml")
    for match in re.finditer(r"^(\w+): \|\n", content, re.MULTILINE):
        name = match.group(1)
        start = match.end()
        next_match = re.search(r"^\w+:", content[start:], re.MULTILINE)
        end = start + next_match.start() if next_match else len(content)
        body = content[start:end].rstrip("\n")
        lines = body.splitlines()
        nonempty = [line for line in lines if line.strip()]
        if nonempty:
            min_indent = min(len(line) - len(line.lstrip()) for line in nonempty)
            body = "\n".join(
                line[min_indent:] if line else "" for line in lines
            )
        macros[name] = body
    return macros


def expand_macros(text: str, macros: dict[str, str]) -> str:
    def _replace(match: re.Match) -> str:
        indent = match.group(1)
        name = match.group(2)
        if name not in macros:
            raise KeyError(f"Unknown macro: {name}")
        body = macros[name].rstrip("\n")
        # Macro lines are relative to the placeholder indent (no extra base offset).
        return "\n".join(indent + line if line else "" for line in body.splitlines())

    return MACRO_RE.sub(_replace, text)


def merge_paths() -> str:
    parts = []
    for name in PATH_ORDER:
        path_file = SPECS_DIR / "fragments" / "paths" / f"{name}.yaml"
        if not path_file.exists():
            continue
        fixed_lines = []
        for line in _read(path_file).splitlines():
            if line.startswith("/"):
                fixed_lines.append("  " + line)
            else:
                fixed_lines.append(line)
        parts.append("\n".join(fixed_lines))
    return "\n\n".join(parts)


def merge_schemas() -> str:
    parts = []
    for name in SCHEMA_ORDER:
        schema_file = SPECS_DIR / "fragments" / "schemas" / f"{name}.yaml"
        if schema_file.exists():
            parts.append(_read(schema_file))
    return "schemas:\n" + "\n\n".join(parts)


def inject_placeholders(template: str, fragments: dict[str, str]) -> str:
    def _inject(match: re.Match) -> str:
        indent = match.group("indent")
        key = match.group("key")
        if key not in fragments:
            raise KeyError(f"Unknown placeholder: {key}")
        return textwrap.indent(fragments[key], indent)

    return PLACEHOLDER_RE.sub(_inject, template)


def load_fixtures() -> dict:
    fixtures_path = SPECS_DIR / "examples" / "fixtures.yaml"
    data = _load_simple_yaml(fixtures_path)
    return data


def load_networks() -> dict:
    data = _load_simple_yaml(SPECS_DIR / "networks.yaml")
    return data["networks"]


def substitute_examples(text: str, network_name: str, fixtures: dict) -> str:
    for section, suffix in (("params", "param"), ("requestBodies", "rb")):
        for key, values in fixtures.get(section, {}).items():
            if network_name not in values:
                continue
            placeholder = f"##{key}_{suffix}##"
            text = text.replace(placeholder, _fixtures_value(values[network_name]))
    return text


def build_spec(network_name: str, network_key: str, fixtures: dict, macros: dict) -> str:
    fragments = {
        "info": _read(SPECS_DIR / "fragments" / "info.yaml"),
        "paths": merge_paths(),
        "parameters": _read(SPECS_DIR / "fragments" / "parameters.yaml"),
        "requestBodies": _read(SPECS_DIR / "fragments" / "request-bodies.yaml"),
        "schemas": merge_schemas(),
    }
    template = _read(SPECS_DIR / "template.yaml")
    spec = inject_placeholders(template, fragments)
    spec = expand_macros(spec, macros)
    spec = substitute_examples(spec, network_name, fixtures)
    return spec + "\n"


def write_spec(network_name: str, output: pathlib.Path, fixtures: dict, macros: dict, network_key: str) -> None:
    spec = build_spec(network_name, network_key, fixtures, macros)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(spec)
    print(f"Created {output} ({network_name})")


def check_specs(networks: dict, fixtures: dict, macros: dict) -> bool:
    ok = True
    for name, cfg in networks.items():
        output = SPECS_DIR / cfg["output"]
        expected = output.read_text()
        generated = build_spec(name, cfg["key"], fixtures, macros)
        if generated != expected:
            print(f"MISMATCH: {output}", file=sys.stderr)
            ok = False
    if ok:
        print("All specs match committed results.")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Koios OpenAPI specs from fragments.")
    parser.add_argument(
        "--network",
        choices=["mainnet", "preview", "preprod", "guild"],
        help="Generate a single network spec (default: all)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify generated specs match committed results/",
    )
    args = parser.parse_args()

    fixtures = load_fixtures()
    networks = load_networks()
    macros = load_macros()

    if args.check:
        return 0 if check_specs(networks, fixtures, macros) else 1

    targets = networks.items()
    if args.network:
        targets = [(args.network, networks[args.network])]

    for name, cfg in targets:
        output = SPECS_DIR / cfg["output"]
        write_spec(name, output, fixtures, macros, cfg["key"])

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise
