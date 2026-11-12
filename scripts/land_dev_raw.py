#!/usr/bin/env python3
"""Plan or upload gzip NDJSON for the dev lake raw tables.

``plan`` generates a scale and prints the S3 keys and object sizes. It does
not call AWS.

``upload`` reads a generator directory and puts the three raw objects. It
refuses unless ``LAND_DEV_RAW_CONFIRM`` is ``land-dev-raw``, the manifest is
the beefy e-commerce dataset at seed 42, and every object fits under the
Athena scan cap. It lists each prefix first and does not put if any other
object is there. It never deletes.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.datasets.ecom import ecom_params, generate_ecom  # noqa: E402
from pipelines.datasets.landing import (  # noqa: E402
    CONFIRM,
    LANDINGS,
    argv_errors,
    cap_errors,
    format_plan,
    head_errors,
    head_object_argv,
    layout_errors,
    list_prefix_argv,
    objects_from_directory,
    plan_streams,
    put_object_argv,
    read_cutoff,
    unexpected_object_keys,
    upload_guard_errors,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts/land_dev_raw.py")
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Print the keys and sizes. Do not call AWS.")
    plan.add_argument("--scale", choices=("test", "sample", "beefy"), default="test")
    plan.add_argument("--seed", type=int, default=42)

    upload = sub.add_parser("upload", help="Put the three raw objects. Requires the confirm env var.")
    upload.add_argument("--input", type=Path, required=True)

    sub.add_parser("self-test", help="Check the layout and a test-scale plan. No AWS.")

    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            return cmd_plan(args.scale, args.seed)
        if args.command == "upload":
            return cmd_upload(args.input)
        if args.command == "self-test":
            return cmd_self_test()
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"unknown command {args.command}", file=sys.stderr)
    return 1


def cmd_plan(scale: str, seed: int) -> int:
    streams, manifest = generate_ecom(ecom_params(scale, seed))
    if manifest.get("scale") != scale or manifest.get("seed") != seed:
        print("generator manifest does not match the requested scale and seed", file=sys.stderr)
        return 1
    planned, errors = plan_streams(streams)
    cutoff = read_cutoff()
    errors.extend(cap_errors(planned, cutoff))
    sys.stdout.write(
        format_plan(
            planned,
            label="dry-run",
            scale=scale,
            seed=seed,
            aws_write="no",
            cutoff=cutoff,
        )
    )
    return _finish(errors)


def cmd_upload(directory: Path) -> int:
    confirm = os.environ.get("LAND_DEV_RAW_CONFIRM", "")
    if confirm != CONFIRM:
        print(
            f"refusing upload: LAND_DEV_RAW_CONFIRM must be {CONFIRM!r}",
            file=sys.stderr,
        )
        return 1
    planned, manifest, errors = objects_from_directory(directory)
    errors.extend(upload_guard_errors(manifest) if manifest else [])
    cutoff = read_cutoff()
    errors.extend(cap_errors(planned, cutoff))
    scale = str(manifest.get("scale", ""))
    seed = manifest.get("seed", "")
    sys.stdout.write(
        format_plan(
            planned,
            label="upload",
            scale=scale,
            seed=seed if isinstance(seed, int) else -1,
            aws_write="pending",
            cutoff=cutoff,
        )
    )
    if errors:
        return _finish(errors)
    return _put_all(planned, _subprocess_runner)


def cmd_self_test() -> int:
    errors = layout_errors()
    streams, _manifest = generate_ecom(ecom_params("test", 42))
    planned, plan_errors = plan_streams(streams)
    errors.extend(plan_errors)
    errors.extend(cap_errors(planned, read_cutoff()))
    landed = {obj.landing.stream for obj in planned}
    if landed != {item.stream for item in LANDINGS}:
        errors.append(f"self-test landed {sorted(landed)}")
    if "customers" in landed:
        errors.append("self-test landed customers")
    if errors:
        return _finish(errors)
    print("land-dev-raw self-test ok")
    return 0


def _put_all(planned, runner) -> int:
    """List every prefix, then put, then head. A bad list blocks every put."""
    if len(planned) != len(LANDINGS):
        return _finish([f"refusing upload: expected {len(LANDINGS)} objects, got {len(planned)}"])
    by_prefix = {obj.landing.prefix: obj for obj in planned}
    errors: list[str] = []
    list_argvs = []
    for prefix, obj in by_prefix.items():
        argv = list_prefix_argv(prefix)
        errors.extend(argv_errors(argv))
        list_argvs.append((obj, argv))
    if errors:
        return _finish(errors)
    for obj, argv in list_argvs:
        payload = json.loads(runner(argv))
        if not isinstance(payload, dict):
            return _finish([f"{obj.landing.prefix} list did not return a JSON object"])
        unexpected = unexpected_object_keys(obj.landing.prefix, payload, obj.landing)
        if unexpected:
            errors.append(
                "refusing to upload; these keys are outside the single object this run overwrites, "
                f"and this run will not delete them: {unexpected}"
            )
    if errors:
        return _finish(errors)

    with tempfile.TemporaryDirectory(prefix="land-dev-raw-") as tmp:
        root = Path(tmp)
        staged = []
        for obj in planned:
            path = root / obj.landing.key.replace("/", "__")
            path.write_bytes(obj.body)
            put_argv = put_object_argv(path, obj.landing.key)
            head_argv = head_object_argv(obj.landing.key)
            errors.extend(argv_errors(put_argv))
            errors.extend(argv_errors(head_argv))
            staged.append((obj, put_argv, head_argv))
        if errors:
            return _finish(errors)
        for obj, put_argv, head_argv in staged:
            runner(put_argv)
            print(f"put {obj.uri} bytes={obj.gzip_bytes} rows={obj.rows}")
            head = json.loads(runner(head_argv))
            if not isinstance(head, dict):
                return _finish([f"{obj.uri} head did not return a JSON object"])
            checked = head_errors(head, obj.body)
            if checked:
                errors.extend(f"{obj.uri}: {item}" for item in checked)
                return _finish(errors)
            print(f"head {obj.uri} content_length={head.get('ContentLength')} match=yes")
    print("upload complete aws_write=yes")
    return 0


def _subprocess_runner(argv: list[str]) -> str:
    completed = subprocess.run(argv, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"aws command failed ({completed.returncode}): {' '.join(argv)}\n{completed.stderr.strip()}"
        )
    return completed.stdout


def _finish(errors: list[str]) -> int:
    if not errors:
        return 0
    print("landing check failed:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
