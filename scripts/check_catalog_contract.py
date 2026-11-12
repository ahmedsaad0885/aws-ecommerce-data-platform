#!/usr/bin/env python3
"""Check Glue table registration against the repository contracts.

tables.json is what Terraform registers. This script rebuilds the expected
columns from the JSON Schemas, the Avro order event, the gold SQL stub, and
the gold-order field list, then fails if the registration drifts.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ANALYTICS = ROOT / "infra" / "modules" / "analytics"
TABLES_PATH = ANALYTICS / "tables.json"
QUERIES_JSON = ANALYTICS / "queries.json"
QUERIES_DIR = ANALYTICS / "queries"
GOLD_SQL = ROOT / "data" / "schemas" / "sql" / "gold.sql"
MAIN_TF = ANALYTICS / "main.tf"

RAW_TABLES = {
    "raw_order_events": {
        "schema": ROOT / "data" / "schemas" / "json" / "order_event.schema.json",
        "prefix": "raw/order-events/",
        "sample": ROOT / "data" / "samples" / "order_events.ndjson",
        "avro": ROOT / "data" / "schemas" / "avro" / "order_event.avsc",
    },
    "raw_product_catalog": {
        "schema": ROOT / "data" / "schemas" / "json" / "product_catalog_item.schema.json",
        "prefix": "raw/product-catalog/",
        "sample": ROOT / "data" / "samples" / "product_catalog.ndjson",
        "avro": None,
    },
    "raw_reviews": {
        "schema": ROOT / "data" / "schemas" / "json" / "review.schema.json",
        "prefix": "raw/reviews/",
        "sample": ROOT / "data" / "samples" / "reviews.ndjson",
        "avro": None,
    },
}

GOLD_LOCATIONS = {
    "gold_orders": "gold/orders/",
    "gold_reviews": "gold/reviews/",
}

SQL_TYPE_TO_HIVE = {
    "STRING": "string",
    "TIMESTAMP": "timestamp",
    "INT": "int",
}

SERDE = "org.apache.hive.hcatalog.data.JsonSerDe"
INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
TIMESTAMP_FORMATS = "yyyy-MM-dd'T'HH:mm:ss'Z',yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
PROJECTION_RANGE = "2020-01-01,NOW"
ENGINE = "Athena engine version 3"
CUTOFF = "10485760"

IAM_RESOURCE_TYPES = (
    "aws_iam_role",
    "aws_iam_role_policy",
    "aws_iam_policy",
    "aws_iam_user",
    "aws_iam_instance_profile",
    "aws_iam_openid_connect_provider",
    "aws_iam_saml_provider",
)

READ_ONLY_SQL = re.compile(
    r"\b(DROP|INSERT|DELETE|UPDATE|CREATE|ALTER|MSCK|MERGE|UNLOAD)\b",
    re.IGNORECASE,
)
FROM_TABLE = re.compile(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE)
GOLD_TABLE = re.compile(
    r"CREATE EXTERNAL TABLE (\w+) \((.*?)\)\s*"
    r"PARTITIONED BY \((\w+) (\w+)\)\s*"
    r"ROW FORMAT SERDE '([^']+)'\s*"
    r"LOCATION 's3://LAKE_BUCKET/([^']+)';",
    re.DOTALL,
)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve(schema: dict, node: dict) -> dict:
    if "$ref" not in node:
        return node
    ref = node["$ref"]
    if not ref.startswith("#/"):
        raise ValueError(f"unsupported schema ref {ref}")
    current = schema
    for part in ref[2:].split("/"):
        current = current[part]
    return current


def athena_type(schema: dict, node: dict) -> str:
    node = _resolve(schema, node)
    kind = node["type"]
    if kind == "string":
        return "string"
    if kind == "integer":
        return "int"
    if kind == "array":
        item = _resolve(schema, node["items"])
        if item.get("type") != "object":
            raise ValueError("array items must be an object")
        fields = [
            f"{name}:{athena_type(schema, field)}"
            for name, field in item["properties"].items()
        ]
        return "array<struct<" + ",".join(fields) + ">>"
    raise ValueError(f"unsupported JSON Schema type {kind}")


def columns_from_schema(path: Path) -> list[tuple[str, str]]:
    schema = _load_json(path)
    return [
        (name, athena_type(schema, prop))
        for name, prop in schema["properties"].items()
    ]


def avro_names(path: Path) -> list[str]:
    record = _load_json(path)
    names = []
    for field in record["fields"]:
        names.append(field["name"])
        kind = field["type"]
        if isinstance(kind, dict) and kind.get("type") == "array":
            item = kind["items"]
            names.extend(nested["name"] for nested in item["fields"])
    return names


def parse_gold_sql(text: str) -> dict[str, dict]:
    found = {}
    for match in GOLD_TABLE.finditer(text):
        name, body, partition, partition_type, serde, location = match.groups()
        columns = []
        for raw in body.splitlines():
            line = raw.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            column_name, column_type = line.split()
            columns.append((column_name, SQL_TYPE_TO_HIVE[column_type]))
        found[name] = {
            "columns": columns,
            "partition": (partition, SQL_TYPE_TO_HIVE[partition_type]),
            "serde": serde,
            "location": location,
        }
    return found


def sample_key_problems(path: Path, columns: list[str]) -> list[str]:
    problems = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        record = json.loads(line)
        keys = list(record)
        if keys != columns:
            problems.append(
                f"{path.name} line {number} keys {keys} != catalog columns {columns}"
            )
        items = record.get("items")
        if isinstance(items, list):
            for item in items:
                if list(item) != ["product_id", "quantity", "unit_price"]:
                    problems.append(f"{path.name} line {number} line item keys drifted")
    return problems


def contract_problems(
    tables: dict,
    gold_sql: str,
    terraform: str,
    query_descriptions: dict,
    query_sql: dict[str, str],
) -> list[str]:
    problems: list[str] = []
    if set(tables) != set(RAW_TABLES) | set(GOLD_LOCATIONS):
        problems.append(
            "table set "
            f"{sorted(tables)} != {sorted(set(RAW_TABLES) | set(GOLD_LOCATIONS))}"
        )

    for name, spec in RAW_TABLES.items():
        table = tables.get(name)
        if not isinstance(table, dict):
            problems.append(f"missing raw table {name}")
            continue
        expected = columns_from_schema(spec["schema"])
        actual = [(column["name"], column["type"]) for column in table.get("columns", [])]
        if actual != expected:
            problems.append(f"{name} columns {actual} != schema {expected}")
        if table.get("prefix") != spec["prefix"]:
            problems.append(f"{name} prefix {table.get('prefix')!r} != {spec['prefix']!r}")
        if table.get("partition_keys") not in ([], None):
            problems.append(f"{name} must not be partitioned yet")
        if table.get("projection") is not None:
            problems.append(f"{name} must not set partition projection")
        if not str(table.get("description", "")).strip():
            problems.append(f"{name} needs a description")
        for column in table.get("columns", []):
            if not str(column.get("comment", "")).strip():
                problems.append(f"{name}.{column.get('name')} needs a comment")
        if spec["sample"] is not None and actual:
            problems.extend(
                sample_key_problems(spec["sample"], [column[0] for column in actual])
            )
        if spec["avro"] is not None and actual:
            schema_names = [column[0] for column in actual]
            schema_names.extend(["product_id", "quantity", "unit_price"])
            if avro_names(spec["avro"]) != schema_names:
                problems.append(
                    f"Avro field order {avro_names(spec['avro'])} != catalog {schema_names}"
                )

    gold = parse_gold_sql(gold_sql)
    if set(gold) != set(GOLD_LOCATIONS):
        problems.append(f"gold.sql tables {sorted(gold)} != {sorted(GOLD_LOCATIONS)}")
    for name, location in GOLD_LOCATIONS.items():
        table = tables.get(name)
        parsed = gold.get(name)
        if not isinstance(table, dict) or parsed is None:
            problems.append(f"missing gold table {name}")
            continue
        actual = [(column["name"], column["type"]) for column in table.get("columns", [])]
        if actual != parsed["columns"]:
            problems.append(f"{name} columns {actual} != gold.sql {parsed['columns']}")
        partitions = [
            (column["name"], column["type"]) for column in table.get("partition_keys", [])
        ]
        if partitions != [parsed["partition"]]:
            problems.append(f"{name} partitions {partitions} != gold.sql {[parsed['partition']]}")
        if table.get("prefix") != location or parsed["location"] != location:
            problems.append(
                f"{name} location tables={table.get('prefix')!r} sql={parsed['location']!r} expected={location!r}"
            )
        if parsed["serde"] != SERDE:
            problems.append(f"gold.sql serde for {name} is {parsed['serde']!r}")
        projection = table.get("projection") or {}
        partition_name = parsed["partition"][0]
        if projection.get("column") != partition_name:
            problems.append(f"{name} projection column {projection.get('column')!r}")
        if projection.get("format") != "yyyy-MM-dd":
            problems.append(f"{name} projection format {projection.get('format')!r}")
        if projection.get("range") != PROJECTION_RANGE:
            problems.append(f"{name} projection range {projection.get('range')!r}")
        if projection.get("interval") != "1" or projection.get("interval_unit") != "DAYS":
            problems.append(f"{name} projection interval drifted")
        expected_suffix = f"{partition_name}=${{{partition_name}}}/"
        if projection.get("template_suffix") != expected_suffix:
            problems.append(
                f"{name} template suffix {projection.get('template_suffix')!r} != {expected_suffix!r}"
            )
        if not str(table.get("description", "")).strip():
            problems.append(f"{name} needs a description")

    if "DECIMAL(12, 2)" not in gold_sql or "total_amount STRING" not in gold_sql:
        problems.append("gold.sql must keep the DECIMAL(12, 2) cast note and a STRING total_amount column")

    sys.path.insert(0, str(ROOT))
    from pipelines.orders import GOLD_FIELDS, SAMPLE_GOLD_ROW

    gold_orders = tables.get("gold_orders", {})
    registered = [column["name"] for column in gold_orders.get("columns", [])]
    registered += [column["name"] for column in gold_orders.get("partition_keys", [])]
    if registered != list(GOLD_FIELDS):
        problems.append(f"gold_orders field order {registered} != pipelines GOLD_FIELDS {list(GOLD_FIELDS)}")
    if list(SAMPLE_GOLD_ROW) != list(GOLD_FIELDS):
        problems.append("SAMPLE_GOLD_ROW keys drifted from GOLD_FIELDS")
    amount = next(
        (column for column in gold_orders.get("columns", []) if column["name"] == "total_amount"),
        None,
    )
    count = next(
        (column for column in gold_orders.get("columns", []) if column["name"] == "item_count"),
        None,
    )
    if amount is None or amount.get("type") != "string":
        problems.append("gold_orders.total_amount must stay string so the JSON decimal text is readable")
    if count is None or count.get("type") != "int":
        problems.append("gold_orders.item_count must stay int")

    for token in (SERDE, INPUT_FORMAT, OUTPUT_FORMAT, TIMESTAMP_FORMATS, ENGINE, "bytes_scanned_cutoff_per_query"):
        if token not in terraform:
            problems.append(f"analytics module is missing {token}")
    if "tables.json" not in terraform or "queries.json" not in terraform:
        problems.append("analytics module must load tables.json and queries.json")
    if str(CUTOFF) not in (ROOT / "infra" / "envs" / "dev" / "terraform.tfvars").read_text(encoding="utf-8"):
        problems.append("dev tfvars is missing the bytes scanned cutoff")
    for resource_type in IAM_RESOURCE_TYPES:
        if f'resource "{resource_type}"' in terraform or f"resource \"{resource_type}\"" in terraform:
            problems.append(f"analytics module declares {resource_type}")
    for banned in ("aws_glue_crawler", "aws_glue_job", "aws_glue_trigger", "aws_lakeformation_"):
        if banned in terraform:
            problems.append(f"analytics module mentions {banned}")

    if set(query_descriptions) != set(query_sql):
        problems.append(
            f"query descriptions {sorted(query_descriptions)} != sql files {sorted(query_sql)}"
        )
    known_tables = set(tables)
    for name, sql in query_sql.items():
        description = str(query_descriptions.get(name, "")).strip()
        if not description:
            problems.append(f"named query {name} needs a description")
        sql_without_comments = "\n".join(
            line.split("--", 1)[0] for line in sql.splitlines()
        )
        if READ_ONLY_SQL.search(sql_without_comments):
            problems.append(f"named query {name} is not a read-only example")
        if not re.search(r"\bLIMIT\s+20\b", sql):
            problems.append(f"named query {name} must LIMIT 20")
        tables_used = FROM_TABLE.findall(sql_without_comments)
        if len(tables_used) != 1 or tables_used[0] not in known_tables:
            problems.append(f"named query {name} must read exactly one registered table, found {tables_used}")
            continue
        table_name = tables_used[0]
        if table_name in GOLD_LOCATIONS:
            partition = tables[table_name]["partition_keys"][0]["name"]
            if f"WHERE {partition} = " not in sql:
                problems.append(f"named query {name} must filter {partition}")
        if table_name == "gold_orders" and "CAST(total_amount AS DECIMAL(12, 2))" not in sql:
            problems.append("gold order example must cast total_amount to DECIMAL(12, 2)")
        if table_name == "gold_orders" and "2026-01-15" not in sql:
            problems.append("gold order example must use the sample order_date")
        if table_name == "gold_reviews" and "2026-01-16" not in sql:
            problems.append("gold review example must use the sample created_date")
    return problems


def load_problems() -> list[str]:
    tables = _load_json(TABLES_PATH)
    descriptions = _load_json(QUERIES_JSON)
    sql = {
        path.stem: path.read_text(encoding="utf-8")
        for path in sorted(QUERIES_DIR.glob("*.sql"))
    }
    return contract_problems(
        tables,
        GOLD_SQL.read_text(encoding="utf-8"),
        MAIN_TF.read_text(encoding="utf-8"),
        descriptions,
        sql,
    )


def self_test() -> list[str]:
    errors = []
    current = load_problems()
    if current:
        errors.append("committed catalog contract failed: " + "; ".join(current))

    tables = _load_json(TABLES_PATH)
    descriptions = _load_json(QUERIES_JSON)
    sql = {path.stem: path.read_text(encoding="utf-8") for path in QUERIES_DIR.glob("*.sql")}
    gold_sql = GOLD_SQL.read_text(encoding="utf-8")
    terraform = MAIN_TF.read_text(encoding="utf-8")

    drifted = json.loads(json.dumps(tables))
    drifted["raw_order_events"]["columns"] = drifted["raw_order_events"]["columns"][:-1]
    if not contract_problems(drifted, gold_sql, terraform, descriptions, sql):
        errors.append("self-test allowed a raw table missing a column")

    relocated = json.loads(json.dumps(tables))
    relocated["gold_orders"]["prefix"] = "gold/somewhere-else/"
    if not contract_problems(relocated, gold_sql, terraform, descriptions, sql):
        errors.append("self-test allowed a gold prefix change")

    role = terraform + '\nresource "aws_iam_role" "glue" {}\n'
    if not any("aws_iam_role" in item for item in contract_problems(tables, gold_sql, role, descriptions, sql)):
        errors.append("self-test allowed an IAM role in the analytics module")

    unfiltered = dict(sql)
    unfiltered["gold_orders_sample_date"] = "SELECT order_id FROM gold_orders LIMIT 20\n"
    if not any("order_date" in item for item in contract_problems(tables, gold_sql, terraform, descriptions, unfiltered)):
        errors.append("self-test allowed a gold query without the partition filter")
    return errors


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        errors = self_test()
        if errors:
            print("Catalog contract self-test failed:")
            for error in errors:
                print(f"  {error}")
            return 1
        print("Catalog contract self-test ok.")
        return 0
    if argv:
        print("usage: check_catalog_contract.py [--self-test]", file=sys.stderr)
        return 2
    problems = load_problems()
    if problems:
        print("Catalog contract check failed:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    print("Catalog contract check ok.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
