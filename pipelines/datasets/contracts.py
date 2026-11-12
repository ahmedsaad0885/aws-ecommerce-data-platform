"""Validator for the subset of JSON Schema used by the data contracts.

The pipeline tests use this so a generated record has to match the committed
schema without a third-party package. It covers the keywords those schemas
actually use: object properties, ``additionalProperties: false``, required,
``$ref`` into ``$defs``, type (including null unions), const, enum, pattern,
minLength, minimum, maximum, minItems, and array items.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from pipelines.datasets.common import load_json, repo_path


class ContractError(ValueError):
    """A record does not match its schema."""


def schema_path(name: str) -> Path:
    return repo_path("data", "schemas", "json", name)


@lru_cache(maxsize=None)
def load_schema(name: str) -> dict:
    return load_json(schema_path(name))


def validate(instance: object, schema: dict, path: str = "$") -> None:
    """Raise ``ContractError`` when ``instance`` does not match ``schema``."""
    _validate(instance, schema, schema, path)


def _validate(instance: object, schema: dict, root: dict, path: str) -> None:
    if "$ref" in schema:
        ref = schema["$ref"]
        prefix = "#/$defs/"
        if not ref.startswith(prefix):
            raise ContractError(f"{path} uses an unsupported $ref: {ref}")
        try:
            target = root["$defs"][ref[len(prefix) :]]
        except KeyError as exc:
            raise ContractError(f"{path} $ref not found: {ref}") from exc
        _validate(instance, target, root, path)
        return

    expected = schema.get("type")
    if expected is not None and not _type_ok(instance, expected):
        raise ContractError(f"{path} has type {type(instance).__name__}, expected {expected}")

    if "const" in schema and instance != schema["const"]:
        raise ContractError(f"{path} must be {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        raise ContractError(f"{path} must be one of {schema['enum']!r}")

    if isinstance(instance, str):
        min_length = schema.get("minLength")
        if min_length is not None and len(instance) < min_length:
            raise ContractError(f"{path} is shorter than {min_length}")
        pattern = schema.get("pattern")
        if pattern is not None and re.fullmatch(pattern, instance) is None:
            raise ContractError(f"{path} does not match {pattern}")

    if type(instance) is int:
        if "minimum" in schema and instance < schema["minimum"]:
            raise ContractError(f"{path} is below {schema['minimum']}")
        if "maximum" in schema and instance > schema["maximum"]:
            raise ContractError(f"{path} is above {schema['maximum']}")

    if isinstance(instance, list):
        min_items = schema.get("minItems")
        if min_items is not None and len(instance) < min_items:
            raise ContractError(f"{path} needs at least {min_items} items")
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, item in enumerate(instance):
                _validate(item, item_schema, root, f"{path}[{index}]")

    if isinstance(instance, dict) and "properties" in schema:
        properties = schema["properties"]
        if schema.get("additionalProperties") is False:
            extra = sorted(set(instance) - set(properties))
            if extra:
                raise ContractError(f"{path} has unknown properties: {extra}")
        for key in schema.get("required", []):
            if key not in instance:
                raise ContractError(f"{path}.{key} is required")
        for key, value in instance.items():
            if key in properties:
                _validate(value, properties[key], root, f"{path}.{key}")


def _type_ok(instance: object, expected: object) -> bool:
    if isinstance(expected, list):
        return any(_type_ok(instance, item) for item in expected)
    if expected == "null":
        return instance is None
    if expected == "string":
        return isinstance(instance, str)
    if expected == "integer":
        return type(instance) is int
    if expected == "boolean":
        return type(instance) is bool
    if expected == "array":
        return isinstance(instance, list)
    if expected == "object":
        return isinstance(instance, dict)
    raise ContractError(f"unsupported schema type {expected!r}")
