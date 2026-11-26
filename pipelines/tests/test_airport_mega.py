"""Day-sharded mega preset: determinism, layout, and the existing contracts.

Parameters stay small. A full mega day is a benchmark, not a unit test.
"""

import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from pipelines.datasets.airports import (
    FLIGHT_EVENT_BLOCK,
    FUEL_EVENT_BLOCK,
    PASSENGER_EVENT_BLOCK,
    airport_params,
    generate_airports,
    write_partitioned_airports,
)
from pipelines.datasets.common import parse_ts, read_ndjson, repo_path
from pipelines.datasets.contracts import load_schema, validate
from pipelines.datasets.fuel import compute_fuel, headwind_kt
from pipelines.datasets.reference import AIRPORTS, SYNTHETIC_AIRPORTS, mega_network

STREAMS = {
    "airports": "airport.schema.json",
    "airlines": "airline.schema.json",
    "aircraft_types": "aircraft_type.schema.json",
    "aircraft": "aircraft.schema.json",
    "weather_reports": "weather_report.schema.json",
    "flight_events": "flight_event.schema.json",
    "fuel_records": "fuel_record.schema.json",
    "passenger_events": "passenger_event.schema.json",
}
EVENT_STREAMS = ("weather_reports", "flight_events", "fuel_records", "passenger_events")
REFERENCE_STREAMS = ("airports", "airlines", "aircraft_types", "aircraft")
PART_RE = re.compile(r"^part-\d{5}\.ndjson$")
DATE_RE = re.compile(r"^dt=\d{4}-\d{2}-\d{2}$")


def _params(**overrides):
    params = replace(
        airport_params("mega", 42),
        days=3,
        flights_per_day=8,
        pax_per_flight=2,
    )
    if overrides:
        params = replace(params, **overrides)
    return params


def _files(root: Path) -> dict[str, bytes]:
    found = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            found[str(path.relative_to(root)).replace("\\", "/")] = path.read_bytes()
    return found


def _records(root: Path, stream: str) -> list[dict]:
    folder = root / stream
    rows: list[dict] = []
    if not folder.is_dir():
        return rows
    for path in sorted(folder.rglob("*.ndjson")):
        rows.extend(read_ndjson(path))
    return rows


def test_mega_preset_and_network_shape():
    params = airport_params("mega")
    assert params.days == 60
    assert params.flights_per_day == 20_000
    assert params.pax_per_flight == 25
    assert params.airport_count == 100
    network = mega_network(100)
    assert len(network) == 100
    assert len(SYNTHETIC_AIRPORTS) == 78
    codes = [airport.iata for airport in network]
    assert len(codes) == len(set(codes))
    assert codes[:22] == [airport.iata for airport in AIRPORTS]
    weights = {airport.iata: airport.departure_weight for airport in network}
    small = min(airport.departure_weight for airport in SYNTHETIC_AIRPORTS)
    assert weights["ATL"] >= 8 * small
    assert weights["ORD"] >= 8 * small
    assert weights["ATL"] > weights["MCI"]
    try:
        generate_airports(params)
    except ValueError as exc:
        assert "one day at a time" in str(exc)
    else:
        raise AssertionError("generate_airports must refuse the mega preset")


def test_same_seed_and_day_are_byte_identical(tmp_path):
    params = _params()
    first = tmp_path / "a"
    second = tmp_path / "b"
    write_partitioned_airports(params, first, day_index=1, part_bytes=4096)
    write_partitioned_airports(params, second, day_index=1, part_bytes=4096)
    assert _files(first) == _files(second)


def test_one_day_matches_that_day_of_a_multi_day_run(tmp_path):
    params = _params()
    alone = tmp_path / "alone"
    together = tmp_path / "together"
    write_partitioned_airports(params, alone, day_index=1, part_bytes=8192)
    write_partitioned_airports(params, together, part_bytes=8192)
    alone_files = _files(alone)
    together_files = _files(together)
    day_files = [name for name in alone_files if "/dt=2026-04-02/" in name]
    assert day_files
    for name in day_files:
        assert alone_files[name] == together_files[name]
    for stream in REFERENCE_STREAMS:
        alone_parts = [name for name in alone_files if name.startswith(f"{stream}/")]
        together_parts = [name for name in together_files if name.startswith(f"{stream}/")]
        assert alone_parts == together_parts
        for name in alone_parts:
            assert alone_files[name] == together_files[name]
    # The other days exist only on the multi-day run.
    assert any("/dt=2026-04-01/" in name for name in together_files)
    assert any("/dt=2026-04-03/" in name for name in together_files)
    assert not any("/dt=2026-04-01/" in name for name in alone_files)


def test_ids_are_unique_across_shards_and_block_aligned(tmp_path):
    params = _params()
    root = tmp_path / "out"
    write_partitioned_airports(params, root, part_bytes=65_536)
    flight_ids = []
    flight_event_ids = []
    fuel_ids = []
    passenger_event_ids = []
    passenger_identity = defaultdict(set)
    report_ids = []
    tails = {}
    for record in _records(root, "flight_events"):
        if record["event_type"] == "FlightScheduled":
            flight_ids.append(record["flight_id"])
        flight_event_ids.append(record["event_id"])
        tails.setdefault(record["tail_number"], (record["aircraft_type"], record["airline_iata"]))
        assert tails[record["tail_number"]] == (record["aircraft_type"], record["airline_iata"])
    for record in _records(root, "fuel_records"):
        fuel_ids.append(record["event_id"])
    for record in _records(root, "passenger_events"):
        passenger_event_ids.append(record["event_id"])
        passenger_identity[record["passenger_id"]].add((record["passenger_name"], record["flight_id"]))
    for record in _records(root, "weather_reports"):
        report_ids.append(record["report_id"])
    for name, values in (
        ("flight_id", flight_ids),
        ("flight event", flight_event_ids),
        ("fuel", fuel_ids),
        ("passenger event", passenger_event_ids),
        ("weather", report_ids),
    ):
        assert len(values) == len(set(values)), name
    assert passenger_identity
    for identities in passenger_identity.values():
        assert len(identities) == 1
    assert any(event_id.startswith("fev_002") for event_id in flight_event_ids)
    assert any(event_id.startswith("fue_0005") or event_id.startswith("fue_001") for event_id in fuel_ids)
    # Day index 1 occupies the second block. Day 2 occupies the third.
    numbers = sorted(int(event_id.removeprefix("fev_")) for event_id in flight_event_ids)
    assert numbers[0] == 1
    assert any(FLIGHT_EVENT_BLOCK < number <= 2 * FLIGHT_EVENT_BLOCK for number in numbers)
    assert any(2 * FLIGHT_EVENT_BLOCK < number <= 3 * FLIGHT_EVENT_BLOCK for number in numbers)
    fuel_numbers = [int(event_id.removeprefix("fue_")) for event_id in fuel_ids]
    assert max(fuel_numbers) <= 3 * FUEL_EVENT_BLOCK
    pax_numbers = [int(event_id.removeprefix("pev_")) for event_id in passenger_event_ids]
    assert max(pax_numbers) <= 3 * PASSENGER_EVENT_BLOCK
    for record in _records(root, "aircraft"):
        seen = tails.get(record["tail_number"])
        if seen is not None:
            assert seen == (record["aircraft_type"], record["airline_iata"])
    # flight_id is what must be unique. The 4-digit flight number may repeat tomorrow.
    assert len(flight_ids) == len(set(flight_ids))


def test_partition_layout_and_file_rolling(tmp_path):
    params = _params()
    part_bytes = 1500
    root = tmp_path / "out"
    write_partitioned_airports(params, root, day_index=0, part_bytes=part_bytes)
    passenger_parts = sorted((root / "passenger_events" / "dt=2026-04-01").glob("part-*.ndjson"))
    assert len(passenger_parts) >= 2
    for folder in (root / "weather_reports", root / "flight_events", root / "fuel_records", root / "passenger_events"):
        dates = [path for path in folder.iterdir() if path.is_dir()]
        assert dates
        for date_dir in dates:
            assert DATE_RE.match(date_dir.name)
            parts = sorted(path for path in date_dir.iterdir() if path.is_file())
            assert parts
            for index, path in enumerate(parts):
                assert PART_RE.match(path.name)
                assert path.name == f"part-{index:05d}.ndjson"
                body = path.read_bytes()
                assert body.endswith(b"\n")
                assert b"\n\n" not in body
                lines = body.splitlines()
                assert lines
                for line in lines:
                    json.loads(line)
                if index < len(parts) - 1:
                    assert len(body) >= part_bytes
    for stream in REFERENCE_STREAMS:
        parts = sorted((root / stream).glob("part-*.ndjson"))
        assert parts
        assert not any((root / stream).glob("dt=*"))
        for index, path in enumerate(parts):
            assert path.name == f"part-{index:05d}.ndjson"


def test_records_match_existing_stream_contracts(tmp_path):
    params = _params()
    root = tmp_path / "out"
    write_partitioned_airports(params, root, part_bytes=65_536)
    for stream, schema_name in STREAMS.items():
        schema = load_schema(schema_name)
        rows = _records(root, stream)
        assert rows, stream
        for record in rows:
            validate(record, schema)
            assert list(record) == list(schema["properties"])
    _assert_operations(root)
    _assert_fuel(root)
    _assert_passengers(root)
    _assert_tails(root)


def test_last_day_owns_the_next_morning_and_a_middle_day_does_not(tmp_path):
    params = _params()
    root = tmp_path / "out"
    write_partitioned_airports(params, root, part_bytes=200_000)
    middle = _weather_on(root, "2026-04-02")
    last = _weather_on(root, "2026-04-03")
    assert middle
    assert all(row["observed_at"].startswith("2026-04-02T") for row in middle)
    assert any(row["observed_at"].startswith("2026-04-04T") for row in last)
    referenced = {
        event["weather_report_id"]
        for event in _records(root, "flight_events")
        if event["weather_report_id"]
    }
    stored = {row["report_id"] for row in _records(root, "weather_reports")}
    assert referenced <= stored


def test_hubs_draw_several_times_the_traffic_of_a_small_field(tmp_path):
    params = _params(days=3, flights_per_day=1600, pax_per_flight=1)
    root = tmp_path / "out"
    write_partitioned_airports(params, root, day_index=0, skip_reference=True, part_bytes=8_000_000)
    origins = defaultdict(int)
    for event in _records(root, "flight_events"):
        if event["event_type"] == "FlightScheduled":
            origins[event["origin_iata"]] += 1
    assert origins["ATL"] >= 4 * origins["ZAA"]
    assert origins["ORD"] >= 4 * origins["ZAA"]
    assert origins["ATL"] > origins["ZAA"]


def test_reference_only_matches_a_shard_and_the_cli_round_trips(tmp_path):
    params = _params()
    ref = tmp_path / "ref"
    day = tmp_path / "day"
    skipped = tmp_path / "skip"
    write_partitioned_airports(params, ref, reference_only=True, part_bytes=65_536)
    write_partitioned_airports(params, day, day_index=0, part_bytes=65_536)
    write_partitioned_airports(params, skipped, day_index=0, skip_reference=True, part_bytes=65_536)
    ref_files = _files(ref)
    day_files = _files(day)
    assert any(name.startswith("airports/") for name in ref_files)
    assert not any("/dt=" in name for name in ref_files)
    for name, body in ref_files.items():
        if name == "manifest.json":
            continue
        assert day_files[name] == body
    skip_files = _files(skipped)
    assert not any(name.startswith("airports/") for name in skip_files)
    for name, body in skip_files.items():
        if name == "manifest.json":
            continue
        assert day_files[name] == body
    env = {"PATH": os.environ["PATH"]}
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipelines.datasets",
            "airports",
            "--scale",
            "mega",
            "--seed",
            "42",
            "--days",
            "3",
            "--flights-per-day",
            "8",
            "--pax-per-flight",
            "2",
            "--date",
            "2026-04-02",
            "--part-bytes",
            "65536",
            "--output",
            str(tmp_path / "cli"),
        ],
        cwd=repo_path(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    dated = tmp_path / "dated"
    write_partitioned_airports(params, dated, day_index=1, part_bytes=65_536)
    assert _files(tmp_path / "cli") == _files(dated)
    refused = subprocess.run(
        [sys.executable, "-m", "pipelines.datasets", "airports", "--scale", "mega", "--output", str(tmp_path / "nope")],
        cwd=repo_path(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert refused.returncode == 1
    assert "one day at a time" in refused.stderr


def _weather_rows(folder: Path) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(folder.glob("*.ndjson")):
        rows.extend(read_ndjson(path))
    return rows


def _weather_on(root: Path, date: str) -> list[dict]:
    return _weather_rows(root / "weather_reports" / f"dt={date}")


def _assert_operations(root: Path) -> None:
    grouped = defaultdict(list)
    for event in _records(root, "flight_events"):
        grouped[event["flight_id"]].append(event)
    diverted = False
    rerouted_before = False
    cancels = 0
    for events in grouped.values():
        times = [event["occurred_at"] for event in events]
        assert times == sorted(times)
        kinds = [event["event_type"] for event in events]
        if "FlightCancelled" in kinds:
            cancels += 1
            assert "FlightAirborne" not in kinds
            continue
        index = {kind: position for position, kind in enumerate(kinds)}
        assert index["FlightDeparted"] < index["FlightAirborne"] < index["FlightEnRoute"]
        assert index["FlightEnRoute"] < index["FlightLanded"] < index["FlightArrived"]
        if "FlightRerouted" in kinds:
            if index["FlightRerouted"] < index["FlightDeparted"]:
                rerouted_before = True
            else:
                diverted = True
    assert cancels >= 1
    assert diverted and rerouted_before


def _assert_fuel(root: Path) -> None:
    for record in _records(root, "fuel_records"):
        assert Decimal(record["remaining_fuel_kg"]) >= 0
        recomputed = compute_fuel(
            distance_nm=Decimal(record["distance_nm"]),
            cruise_tas_kt=record["cruise_tas_kt"],
            headwind_kt=record["wind_component_kt"],
            burn_kg_per_hour=Decimal(record["burn_kg_per_hour"]),
            taxi_fuel_kg=Decimal(record["taxi_fuel_kg"]),
            alternate_distance_nm=Decimal(record["alternate_distance_nm"]),
            reserve_minutes=record["reserve_minutes"],
            burn_so_far_kg=Decimal(record["burn_so_far_kg"]),
            contingency_rate=Decimal(record["contingency_rate"]),
        )
        assert record["trip_fuel_kg"] == format(recomputed.trip_fuel_kg, ".2f")
        assert record["planned_fuel_kg"] == format(recomputed.planned_fuel_kg, ".2f")
        assert record["wind_component_kt"] == headwind_kt(
            record["wind_dir_deg"], record["wind_speed_kt"], record["course_deg"]
        )


def _assert_passengers(root: Path) -> None:
    departed = {
        event["flight_id"]: event
        for event in _records(root, "flight_events")
        if event["event_type"] == "FlightDeparted"
    }
    people = defaultdict(list)
    # Each operating day is its own folder. Check-in can fall on the previous
    # calendar day, so folders overlap in time. Inside one folder the rows are sorted.
    for date_dir in sorted((root / "passenger_events").iterdir()):
        stamps = [event["occurred_at"] for event in _weather_rows(date_dir)]
        assert stamps == sorted(stamps)
    for event in _records(root, "passenger_events"):
        people[event["passenger_id"]].append(event)
    assert people
    for events in people.values():
        kinds = {event["event_type"] for event in events}
        assert not ({"PassengerBoarded", "PassengerNoShow"} <= kinds)
        off = parse_ts(departed[events[0]["flight_id"]]["occurred_at"])
        if "PassengerBoarded" in kinds:
            boarded = next(event for event in events if event["event_type"] == "PassengerBoarded")
            assert parse_ts(boarded["occurred_at"]) < off


def _assert_tails(root: Path) -> None:
    grouped = defaultdict(list)
    for event in _records(root, "flight_events"):
        grouped[event["flight_id"]].append(event)
    by_tail = defaultdict(list)
    for events in grouped.values():
        kinds = {event["event_type"]: event for event in events}
        if "FlightDeparted" not in kinds:
            continue
        # A tail is scheduled again the next morning from a planned start,
        # so continuity is per operating day, not across shards.
        day = kinds["FlightDeparted"]["scheduled_departure_at"][:10]
        by_tail[(kinds["FlightDeparted"]["tail_number"], day)].append(
            {
                "origin": kinds["FlightDeparted"]["origin_iata"],
                "arrival": kinds["FlightLanded"]["destination_iata"],
                "off": parse_ts(kinds["FlightDeparted"]["occurred_at"]),
                "on": parse_ts(kinds["FlightArrived"]["occurred_at"]),
            }
        )
    for legs in by_tail.values():
        legs.sort(key=lambda leg: leg["off"])
        for previous, nxt in zip(legs, legs[1:]):
            assert nxt["origin"] == previous["arrival"]
            assert nxt["off"] >= previous["on"] + timedelta(minutes=45)
