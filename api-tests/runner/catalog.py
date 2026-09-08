from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
MISSING = object()


class CatalogError(ValueError):
    pass


class ExampleResolutionError(CatalogError):
    pass


@dataclass(frozen=True)
class Operation:
    operation_id: str
    path: str
    method: str
    definition: dict[str, Any]
    parameters: tuple[dict[str, Any], ...]
    request_body: dict[str, Any] | None
    responses: dict[str, Any]
    deprecated: bool

    @property
    def label(self) -> str:
        return f"{self.method.upper()} {self.path}"

    @property
    def success_statuses(self) -> tuple[int, ...]:
        statuses = []
        for status in self.responses:
            if status.isdigit() and 200 <= int(status) < 300:
                statuses.append(int(status))
        return tuple(sorted(statuses))

    @property
    def declared_statuses(self) -> tuple[str, ...]:
        return tuple(self.responses)


def load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise CatalogError(f"Expected a mapping in {path}")
    return value


def resolve_ref(document: dict[str, Any], value: Any) -> Any:
    if not isinstance(value, dict) or set(value) != {"$ref"}:
        return value
    reference = value["$ref"]
    if not isinstance(reference, str) or not reference.startswith("#/"):
        raise CatalogError(f"Only local references are supported: {reference}")
    current: Any = document
    for part in reference[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or part not in current:
            raise CatalogError(f"Unresolved reference: {reference}")
        current = current[part]
    return copy.deepcopy(current)


def resolve_recursive(document: dict[str, Any], value: Any) -> Any:
    if isinstance(value, dict) and "$ref" in value:
        resolved = resolve_ref(document, value)
        if len(value) > 1:
            merged = dict(resolved)
            merged.update({k: v for k, v in value.items() if k != "$ref"})
            resolved = merged
        return resolve_recursive(document, resolved)
    if isinstance(value, dict):
        return {key: resolve_recursive(document, item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_recursive(document, item) for item in value]
    return value


def _example_value(value: dict[str, Any], document: dict[str, Any]) -> Any:
    if "example" in value:
        return copy.deepcopy(value["example"])
    examples = value.get("examples")
    if isinstance(examples, dict) and examples:
        first = next(iter(examples.values()))
        if isinstance(first, dict) and "value" in first:
            return copy.deepcopy(first["value"])
        if isinstance(first, dict) and "externalValue" in first:
            raise ExampleResolutionError("externalValue examples are not supported")
        return copy.deepcopy(first)
    if "default" in value:
        return copy.deepcopy(value["default"])
    enum = value.get("enum")
    if isinstance(enum, list) and enum:
        return copy.deepcopy(enum[0])
    schema = value.get("schema")
    if isinstance(schema, dict):
        return schema_example(schema, document)
    return MISSING


def schema_example(schema: dict[str, Any] | bool, document: dict[str, Any]) -> Any:
    if schema is True:
        raise ExampleResolutionError("A true schema has no concrete example")
    if schema is False:
        raise ExampleResolutionError("A false schema cannot have an example")
    schema = resolve_recursive(document, schema)
    direct = _example_value({key: value for key, value in schema.items() if key != "schema"}, document)
    if direct is not MISSING:
        return direct
    for key in ("oneOf", "anyOf", "allOf"):
        alternatives = schema.get(key)
        if isinstance(alternatives, list):
            for alternative in alternatives:
                try:
                    return schema_example(alternative, document)
                except ExampleResolutionError:
                    continue
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        schema_type = next((item for item in schema_type if item != "null"), None)
    if schema_type == "object" or "properties" in schema:
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        result: dict[str, Any] = {}
        for name, property_schema in properties.items():
            try:
                result[name] = schema_example(property_schema, document)
            except ExampleResolutionError:
                if name in required:
                    raise ExampleResolutionError(f"No example for required property {name}")
        if result or not required:
            return result
    if schema_type == "array":
        items = schema.get("items")
        if isinstance(items, (dict, bool)):
            return [schema_example(items, document)]
    raise ExampleResolutionError("No concrete example is available")


def media_example(
    media: dict[str, Any], document: dict[str, Any], name: str | None = None
) -> Any:
    if name is not None:
        examples = media.get("examples")
        if not isinstance(examples, dict) or name not in examples:
            raise ExampleResolutionError(f"Unknown named example: {name}")
        selected = examples[name]
        if isinstance(selected, dict) and "value" in selected:
            return copy.deepcopy(selected["value"])
        if isinstance(selected, dict) and "externalValue" in selected:
            raise ExampleResolutionError("externalValue examples are not supported")
        return copy.deepcopy(selected)
    return _example_value(media, document)


def operation_example(
    operation: Operation,
    document: dict[str, Any],
    parameter_bindings: dict[str, Any] | None = None,
    named_example: str | None = None,
) -> dict[str, Any]:
    bindings = parameter_bindings or {}
    query: dict[str, Any] = {}
    headers: dict[str, Any] = {}
    path_params: dict[str, Any] = {}
    for parameter in operation.parameters:
        parameter = resolve_recursive(document, parameter)
        location = parameter.get("in")
        name = parameter.get("name")
        if not isinstance(location, str) or not isinstance(name, str):
            continue
        value = _example_value(parameter, document)
        if name in bindings:
            value = bindings[name]
        if value is MISSING:
            if parameter.get("required"):
                raise ExampleResolutionError(
                    f"No example for required {location} parameter {name}"
                )
            continue
        target = {"query": query, "header": headers, "path": path_params}.get(location)
        if target is not None:
            target[name] = copy.deepcopy(value)
    result: dict[str, Any] = {"query": query, "headers": headers, "path": path_params}
    if operation.request_body is not None:
        body = resolve_recursive(document, operation.request_body)
        content = body.get("content", {})
        if not content:
            raise ExampleResolutionError(f"{operation.label} has an empty request body")
        media_type = next(iter(content))
        selected_media = content[media_type]
        result["media_type"] = media_type
        value = media_example(selected_media, document, named_example)
        if value is MISSING:
            raise ExampleResolutionError(f"No request-body example for {operation.label}")
        result["body"] = value
    return result


def response_schema(operation: Operation, document: dict[str, Any], status: int) -> Any:
    response = operation.responses.get(str(status))
    if response is None:
        response = operation.responses.get(f"{status // 100}XX")
    if response is None:
        return None
    response = resolve_recursive(document, response)
    content = response.get("content", {})
    if not content:
        return None
    media = content.get("application/json") or next(iter(content.values()))
    schema = media.get("schema") if isinstance(media, dict) else None
    return resolve_recursive(document, schema) if schema is not None else None


def response_media_types(operation: Operation, document: dict[str, Any], status: int) -> tuple[str, ...]:
    response = operation.responses.get(str(status))
    if response is None:
        response = operation.responses.get(f"{status // 100}XX")
    if response is None:
        return ()
    response = resolve_recursive(document, response)
    return tuple(response.get("content", {}))


def build_catalog(document: dict[str, Any]) -> list[Operation]:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        raise CatalogError("OpenAPI document has no paths mapping")
    operations: list[Operation] = []
    seen: set[str] = set()
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            raise CatalogError(f"Path item is not a mapping: {path}")
        path_parameters = path_item.get("parameters", [])
        for method, raw_definition in path_item.items():
            if method.lower() not in METHODS:
                continue
            definition = resolve_recursive(document, raw_definition)
            operation_id = definition.get("operationId")
            if not operation_id:
                raise CatalogError(f"Missing operationId for {method.upper()} {path}")
            if operation_id in seen:
                raise CatalogError(f"Duplicate operationId: {operation_id}")
            seen.add(operation_id)
            parameters = [
                resolve_recursive(document, item)
                for item in [*path_parameters, *definition.get("parameters", [])]
            ]
            request_body = definition.get("requestBody")
            if request_body is not None:
                request_body = resolve_recursive(document, request_body)
            responses = resolve_recursive(document, definition.get("responses", {}))
            if not isinstance(responses, dict):
                raise CatalogError(f"Invalid responses for {operation_id}")
            operations.append(
                Operation(
                    operation_id=operation_id,
                    path=path,
                    method=method.lower(),
                    definition=definition,
                    parameters=tuple(parameters),
                    request_body=request_body,
                    responses=responses,
                    deprecated=bool(definition.get("deprecated", False)),
                )
            )
    return operations
