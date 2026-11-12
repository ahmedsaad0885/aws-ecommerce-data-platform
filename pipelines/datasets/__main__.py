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
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace
from pathlib import Path

from pipelines.datasets.airports import AirportParams, airport_params, generate_airports
from pipelines.datasets.common import repo_path, write_dataset
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
            streams, manifest = generate_airports(params)
            output = args.output or repo_path("data", "generated", "airports")
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
    _add_common(airports)
    airports.add_argument("--days", type=int)
    airports.add_argument("--flights-per-day", type=int)
    airports.add_argument("--pax-per-flight", type=int)
    return parser


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--scale", choices=("test", "sample", "beefy"))
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
