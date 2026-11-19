"""Collapse raw order events into Parquet gold orders.

The row rules live in ``pipelines.orders``. This module is the Spark runner:
read newline-delimited JSON (gzip included), group by ``order_id``, call
``collapse_events``, and write Snappy Parquet partitioned by ``order_date``.

Glue runs ``glue_job_main.py`` with this file and ``orders.py`` on the
worker path. Local tests import this module as ``pipelines.glue_gold_orders``.
Workers must import the functions from that module name, not from ``__main__``,
or Spark cannot unpickle them.
"""

from __future__ import annotations

import json
from datetime import timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("total_amount", StringType(), nullable=False),
        StructField("placed_at", TimestampType(), nullable=False),
        StructField("item_count", IntegerType(), nullable=False),
        StructField("order_date", StringType(), nullable=False),
    ]
)


def _rules():
    """Return the gold-rule module. Glue ships it as top-level ``orders``."""
    try:
        from pipelines import orders as rules
    except ImportError:
        import orders as rules
    return rules


def _row_text(row) -> str:
    return row[0]


def _parse_lines(lines):
    rules = _rules()
    for line in lines:
        if line is None:
            continue
        text = line if isinstance(line, str) else str(line)
        if not text.strip():
            continue
        try:
            record = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON ({exc.msg}): {text[:160]}") from exc
        try:
            rules.parse_event(record)
        except ValueError as exc:
            raise ValueError(f"order event rejected: {exc}") from exc
        yield record


def _key_by_order(record: dict) -> tuple[str, dict]:
    return (record["order_id"], record)


def _collapse_groups(items):
    rules = _rules()
    for _order_id, events in items:
        yield from rules.collapse_events(list(events))


def _spark_tuple(row: dict) -> tuple:
    rules = _rules()
    placed = rules.parse_occurred_at(row["placed_at"]).astimezone(timezone.utc).replace(tzinfo=None)
    return (
        row["order_id"],
        row["customer_id"],
        row["status"],
        row["currency"],
        row["total_amount"],
        placed,
        int(row["item_count"]),
        row["order_date"],
    )


def _configure(spark: SparkSession) -> None:
    # UTC so placed_at round-trips to the same Zulu timestamp the Python
    # transform writes. Dynamic overwrite replaces only order_date directories
    # present in this run. TIMESTAMP_MICROS keeps fractional seconds.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Keep order_date a string. The catalog partition type is string, and the
    # directory name is yyyy-MM-dd. Inference would turn that into a date.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")


def collapse_raw(spark: SparkSession, source: str) -> DataFrame:
    """Read raw events at ``source`` and return one gold row per order.

    ``source`` is a file or a directory of NDJSON. A ``.gz`` suffix is
    decompressed. Blank lines are ignored. A contract failure fails the job
    before any gold write.
    """
    _configure(spark)
    parsed = spark.read.text(source).rdd.map(_row_text).mapPartitions(_parse_lines)
    grouped = parsed.map(_key_by_order).groupByKey()
    tuples = grouped.mapPartitions(_collapse_groups).map(_spark_tuple)
    frame = spark.createDataFrame(tuples, schema=GOLD_SCHEMA)
    # Compute once so a bad event raises before the writer creates files.
    frame.cache()
    frame.count()
    return frame


def write_gold(frame: DataFrame, destination: str) -> None:
    """Overwrite the ``order_date`` partitions present in ``frame``.

    Partitions that this run does not contain are left as they are. A second
    run of the same raw input replaces those same directories, so the write
    is idempotent for the dates still present.
    """
    if frame.count() == 0:
        return
    (
        frame.write.mode("overwrite")
        .option("compression", "snappy")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("order_date")
        .parquet(destination)
    )


def read_gold_rows(spark: SparkSession, destination: str) -> list[dict]:
    """Read Parquet gold back into the Python transform's row shape."""
    rules = _rules()
    frame = spark.read.parquet(destination)
    rows: list[dict] = []
    for record in frame.orderBy("order_id").collect():
        placed = record.placed_at
        if placed.tzinfo is None:
            placed = placed.replace(tzinfo=timezone.utc)
        order_date = record.order_date
        if not isinstance(order_date, str):
            order_date = order_date.isoformat()
        rows.append(
            {
                "order_id": record.order_id,
                "customer_id": record.customer_id,
                "status": record.status,
                "currency": record.currency,
                "total_amount": record.total_amount,
                "placed_at": rules.format_timestamp(placed),
                "item_count": int(record.item_count),
                "order_date": order_date,
            }
        )
    return rows


def transform_path(spark: SparkSession, source: str, destination: str) -> list[dict]:
    """Collapse ``source`` and write Snappy Parquet under ``destination``."""
    frame = collapse_raw(spark, source)
    if frame.count() == 0:
        print(f"Wrote 0 gold order row(s) to {destination}")
        return []
    write_gold(frame, destination)
    rows = read_gold_rows(spark, destination)
    print(f"Wrote {len(rows)} gold order row(s) to {destination}")
    return rows


def run_job() -> None:
    """Entrypoint used by the Glue job. Not called by the local tests."""
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_prefix", "gold_prefix"])
    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    transform_path(spark, args["raw_prefix"], args["gold_prefix"])
    job.commit()
