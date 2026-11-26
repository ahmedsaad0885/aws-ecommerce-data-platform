"""Generate the synthetic e-commerce and airport datasets.

Examples:

    python -m pipelines.datasets ecom --scale beefy --seed 42 --output data/generated/ecom
    python -m pipelines.datasets airports --scale sample --output data/samples/airports

Flags override environment variables, which override the scale preset.
``DATASET_SEED`` sets the seed. ``ECOM_ORDERS``, ``ECOM_PRODUCTS``,
``ECOM_CUSTOMERS``, ``ECOM_DAYS``, and ``ECOM_REVIEW_PROBABILITY`` override
the e-commerce preset. ``AIRPORT_DAYS``, ``AIRPORT_FLIGHTS_PER_DAY``, and
``AIRPORT_PAX_PER_FLIGHT`` override the airport preset. ``ECOM_SCALE`` and
``AIRPORT_SCALE`` choose the preset when ``--scale`` is omitted.

The airport ``mega`` preset writes hive-style day partitions. Pass
``--day-index``, ``--date``, or ``--all-days``. ``--reference-only`` writes
the shared airports, airlines, aircraft types, and tails.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from pipelines.datasets.airports import (
    AIRPORT_SCALES,
    AirportParams,
    airport_params,
    generate_airports,
    write_partitioned_airports,
)
from pipelines.datasets.common import parse_ts, repo_path, write_dataset
from pipelines.datasets.ecom import EcomParams, ecom_params, generate_ecom


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "ecom":
            params = _ecom_params(args)
            streams, manifest = generate_ecom(params)
            output = args.output or repo_path("data", "generated", "ecom")
        else:
            params = _airport_params(args)
            output = args.output or repo_path("data", "generated", "airports")
            if _partitioned(args, params):
                manifest = write_partitioned_airports(
                    params,
                    Path(output),
                    day_index=_day_index(args, params),
                    reference_only=args.reference_only,
                    skip_reference=args.skip_reference,
                    part_bytes=_part_bytes(args),
                )
                _print_partition(args.command, manifest, output)
                return 0
            streams, manifest = generate_airports(params)
        write_dataset(Path(output), streams, manifest)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    counts = manifest["counts"]
    summary = ", ".join(f"{name}={count}" for name, count in counts.items())
    print(f"Wrote {args.command} scale={manifest['scale']} seed={manifest['seed']} to {output}")
    print(summary)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m pipelines.datasets")
    sub = parser.add_subparsers(dest="command", required=True)

    ecom = sub.add_parser("ecom", help="Generate customers, catalog, order events, and reviews.")
    _add_common(ecom)
    ecom.add_argument("--orders", type=int)
    ecom.add_argument("--products", type=int)
    ecom.add_argument("--customers", type=int)
    ecom.add_argument("--days", type=int)
    ecom.add_argument("--review-probability")

    airports = sub.add_parser("airports", help="Generate the airport traffic simulation.")
    _add_common(airports, tuple(sorted(AIRPORT_SCALES)))
    airports.add_argument("--days", type=int)
    airports.add_argument("--flights-per-day", type=int)
    airports.add_argument("--pax-per-flight", type=int)
    day = airports.add_mutually_exclusive_group()
    day.add_argument("--day-index", type=int, help="Zero-based day inside the preset horizon.")
    day.add_argument("--date", help="Calendar day inside the horizon, YYYY-MM-DD.")
    day.add_argument("--all-days", action="store_true", help="Write every day in the horizon.")
    day.add_argument("--reference-only", action="store_true", help="Write the shared reference tables and stop.")
    airports.add_argument(
        "--skip-reference",
        action="store_true",
        help="Write the day partitions and skip the reference tables.",
    )
    airports.add_argument(
        "--part-bytes",
        type=int,
        help="Roll a new NDJSON part at this many bytes. Default is 128 MiB.",
    )
    return parser


def _add_common(parser: argparse.ArgumentParser, scales: tuple[str, ...] = ("test", "sample", "beefy")) -> None:
    parser.add_argument("--scale", choices=scales)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--output", type=Path)


def _ecom_params(args: argparse.Namespace) -> EcomParams:
    scale = args.scale or os.environ.get("ECOM_SCALE") or "beefy"
    params = ecom_params(scale, _seed(args))
    return replace(
        params,
        orders=_override(args.orders, "ECOM_ORDERS", params.orders, int),
        products=_override(args.products, "ECOM_PRODUCTS", params.products, int),
        customers=_override(args.customers, "ECOM_CUSTOMERS", params.customers, int),
        days=_override(args.days, "ECOM_DAYS", params.days, int),
        review_probability=_override(
            args.review_probability,
            "ECOM_REVIEW_PROBABILITY",
            params.review_probability,
            str,
        ),
    )


def _airport_params(args: argparse.Namespace) -> AirportParams:
    scale = args.scale or os.environ.get("AIRPORT_SCALE") or "beefy"
    params = airport_params(scale, _seed(args))
    return replace(
        params,
        days=_override(args.days, "AIRPORT_DAYS", params.days, int),
        flights_per_day=_override(
            args.flights_per_day,
            "AIRPORT_FLIGHTS_PER_DAY",
            params.flights_per_day,
            int,
        ),
        pax_per_flight=_override(
            args.pax_per_flight,
            "AIRPORT_PAX_PER_FLIGHT",
            params.pax_per_flight,
            int,
        ),
    )


def _partitioned(args: argparse.Namespace, params: AirportParams) -> bool:
    selected = any(
        (
            args.day_index is not None,
            bool(args.date),
            args.all_days,
            args.reference_only,
        )
    )
    if params.scale == "mega" and not selected:
        raise ValueError(
            "mega writes one day at a time; pass --day-index, --date, --all-days, or --reference-only"
        )
    if args.skip_reference and args.reference_only:
        raise ValueError("reference-only and skip-reference cannot be combined")
    if args.skip_reference and not selected:
        raise ValueError("--skip-reference needs --day-index, --date, or --all-days")
    return selected


def _day_index(args: argparse.Namespace, params: AirportParams) -> int | None:
    if args.reference_only or args.all_days:
        return None
    if args.date:
        return _index_for_date(params, args.date)
    if args.day_index is None:
        return None
    if args.day_index < 0 or args.day_index >= params.days:
        raise ValueError(f"day-index {args.day_index} is outside 0..{params.days - 1}")
    return args.day_index


def _index_for_date(params: AirportParams, text: str) -> int:
    try:
        day = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD, got {text!r}") from exc
    start = parse_ts(params.start).date()
    index = (day - start).days
    if index < 0 or index >= params.days:
        last = start + timedelta(days=params.days - 1)
        raise ValueError(f"{text} is outside {start.isoformat()}..{last.isoformat()}")
    return index


def _part_bytes(args: argparse.Namespace) -> int:
    if args.part_bytes is not None:
        return args.part_bytes
    env = os.environ.get("AIRPORT_PART_BYTES")
    if env:
        return int(env)
    return 128 * 1024 * 1024


def _print_partition(command: str, manifest: dict, output: Path) -> None:
    scope = "reference" if manifest.get("reference_only") else manifest.get("date") or "all-days"
    print(
        f"Wrote {command} scale={manifest['scale']} seed={manifest['seed']} "
        f"day={scope} to {output}"
    )
    counts = manifest["counts"]
    print(", ".join(f"{name}={count}" for name, count in counts.items()))
    sizes = manifest["bytes"]
    print(", ".join(f"{name}_bytes={size}" for name, size in sizes.items()))


def _seed(args: argparse.Namespace) -> int:
    if args.seed is not None:
        return args.seed
    return int(os.environ.get("DATASET_SEED", "42"))


def _override(flag: object, env_name: str, preset: object, cast: type) -> object:
    if flag is not None:
        return cast(flag)
    env = os.environ.get(env_name)
    if env is not None and env != "":
        return cast(env)
    return preset


if __name__ == "__main__":
    raise SystemExit(main())
