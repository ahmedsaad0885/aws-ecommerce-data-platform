"""Invariants for the airport traffic simulation."""

import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from pipelines.datasets.airports import airport_params, generate_airports
from pipelines.datasets.common import load_json, parse_ts, repo_path
from pipelines.datasets.contracts import load_schema, validate
from pipelines.datasets.fuel import (
    compute_fuel,
    great_circle_nm,
    ground_speed_kt,
    headwind_kt,
    initial_course_deg,
    intermediate_point,
)

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
AVRO = {
    "flight_event.schema.json": "flight_event.avsc",
    "passenger_event.schema.json": "passenger_event.avsc",
    "weather_report.schema.json": "weather_report.avsc",
    "fuel_record.schema.json": "fuel_record.avsc",
    "order_event.schema.json": "order_event.avsc",
}


def _batch():
    return generate_airports(airport_params("test", 42))


def test_records_match_schemas_and_avro_field_order():
    streams, _manifest = _batch()
    for name, schema_name in STREAMS.items():
        schema = load_schema(schema_name)
        assert streams[name]
        for record in streams[name]:
            validate(record, schema)
            assert list(record) == list(schema["properties"])
    for schema_name, avro_name in AVRO.items():
        properties = list(load_schema(schema_name)["properties"])
        avro = load_json(repo_path("data", "schemas", "avro", avro_name))
        assert [field["name"] for field in avro["fields"]] == properties


def test_operations_stay_in_order_and_reroutes_are_a_minority():
    streams, _manifest = _batch()
    _assert_sorted(streams["flight_events"], "occurred_at")
    _assert_sorted(streams["fuel_records"], "occurred_at")
    _assert_sorted(streams["passenger_events"], "occurred_at")
    _assert_sorted(streams["weather_reports"], "observed_at")
    grouped = defaultdict(list)
    for event in streams["flight_events"]:
        grouped[event["flight_id"]].append(event)
    reroutes = 0
    cancels = 0
    reasons = set()
    diverted = False
    rerouted_before_departure = False
    for events in grouped.values():
        kinds = [event["event_type"] for event in events]
        times = [parse_ts(event["occurred_at"]) for event in events]
        assert times == sorted(times)
        if "FlightCancelled" in kinds:
            cancels += 1
            assert "FlightAirborne" not in kinds
            assert "FlightLanded" not in kinds
            reasons.add(events[kinds.index("FlightCancelled")]["delay_reason"])
            continue
        index = {kind: position for position, kind in enumerate(kinds)}
        assert index["FlightDeparted"] < index["FlightAirborne"] < index["FlightEnRoute"]
        assert index["FlightEnRoute"] < index["FlightLanded"] < index["FlightArrived"]
        departed = events[index["FlightDeparted"]]
        landed = events[index["FlightLanded"]]
        assert parse_ts(departed["occurred_at"]) < parse_ts(events[index["FlightAirborne"]]["occurred_at"])
        assert parse_ts(events[index["FlightAirborne"]]["occurred_at"]) < parse_ts(landed["occurred_at"])
        if "FlightRerouted" in kinds:
            reroutes += 1
            reroute = events[index["FlightRerouted"]]
            assert reroute["previous_destination_iata"] != reroute["destination_iata"]
            reasons.add(reroute["delay_reason"])
            if index["FlightRerouted"] < index["FlightDeparted"]:
                rerouted_before_departure = True
                assert departed["destination_iata"] == landed["destination_iata"]
                assert events[0]["destination_iata"] != departed["destination_iata"]
            else:
                diverted = True
                assert departed["destination_iata"] != landed["destination_iata"]
                assert landed["destination_iata"] == reroute["destination_iata"]
    assert cancels >= 1
    assert reroutes >= 1
    assert reroutes / len(grouped) < 0.5
    assert cancels / len(grouped) < 0.5
    assert diverted and rerouted_before_departure
    assert {"airport_closure", "weather", "fuel"} <= reasons


def test_fuel_records_recompute_and_stay_non_negative():
    streams, _manifest = _batch()
    airports = {row["iata"]: row for row in streams["airports"]}
    weather = {row["report_id"]: row for row in streams["weather_reports"]}
    cancelled = {
        event["flight_id"]
        for event in streams["flight_events"]
        if event["event_type"] == "FlightCancelled"
    }
    calculated = {}
    for record in streams["fuel_records"]:
        assert Decimal(record["remaining_fuel_kg"]) >= 0
        report = weather[record["weather_report_id"]]
        assert record["wind_dir_deg"] == report["wind_dir_deg"]
        assert record["wind_speed_kt"] == report["wind_speed_kt"]
        assert record["wind_component_kt"] == headwind_kt(
            report["wind_dir_deg"], report["wind_speed_kt"], record["course_deg"]
        )
        assert record["ground_speed_kt"] == ground_speed_kt(
            record["cruise_tas_kt"], record["wind_component_kt"]
        )
        _assert_distance_and_course(record, airports)
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
        assert record["contingency_fuel_kg"] == format(recomputed.contingency_fuel_kg, ".2f")
        assert record["alternate_fuel_kg"] == format(recomputed.alternate_fuel_kg, ".2f")
        assert record["reserve_fuel_kg"] == format(recomputed.reserve_fuel_kg, ".2f")
        assert record["trip_minutes"] == recomputed.trip_minutes
        if record["flight_id"] in cancelled and record["event_type"] == "FuelCalculated":
            assert record["uplift_kg"] == "0.00"
            assert record["estimated_burn_kg"] == "0.00"
            assert record["remaining_fuel_kg"] == "0.00"
        else:
            assert record["estimated_burn_kg"] == format(recomputed.estimated_burn_kg, ".2f")
            remaining = Decimal(record["uplift_kg"]) - Decimal(record["estimated_burn_kg"])
            assert Decimal(record["remaining_fuel_kg"]) == remaining
            if record["stage"] == "preflight":
                assert Decimal(record["uplift_kg"]) == (
                    Decimal(record["planned_fuel_kg"]) + Decimal(record["extra_fuel_kg"])
                )
        if record["event_type"] == "FuelCalculated":
            calculated[record["flight_id"]] = record
        else:
            assert record["reroute_event_id"]
            assert record["previous_destination_iata"] not in {None, record["destination_iata"]}
            if record["stage"] == "airborne":
                assert record["uplift_kg"] == calculated[record["flight_id"]]["uplift_kg"]
                assert record["taxi_fuel_kg"] == "0.00"


def test_weather_and_passengers_line_up_with_flights():
    streams, _manifest = _batch()
    weather = {row["report_id"]: row for row in streams["weather_reports"]}
    by_airport_time = {
        (row["airport_iata"], row["observed_at"]): row for row in streams["weather_reports"]
    }
    for report in streams["weather_reports"]:
        assert report["flight_category"] == _category(report["ceiling_ft"], report["visibility_sm"])
        assert Decimal(report["dewpoint_c"]) <= Decimal(report["temp_c"])
        if report["airport_closed"]:
            assert report["flight_category"] == "LIFR"
    flights = defaultdict(list)
    for event in streams["flight_events"]:
        flights[event["flight_id"]].append(event)
        if event["weather_report_id"] is None:
            continue
        report = weather[event["weather_report_id"]]
        delta = parse_ts(event["occurred_at"]) - parse_ts(report["observed_at"])
        assert timedelta(0) <= delta < timedelta(hours=1)
    for events in flights.values():
        for event in events:
            if event["event_type"] == "FlightDeparted":
                _assert_nearby_weather(by_airport_time, event["origin_iata"], event["occurred_at"])
            if event["event_type"] == "FlightLanded":
                _assert_nearby_weather(by_airport_time, event["destination_iata"], event["occurred_at"])
    departed = {
        event["flight_id"]: event
        for event in streams["flight_events"]
        if event["event_type"] == "FlightDeparted"
    }
    landed = {
        event["flight_id"]: event
        for event in streams["flight_events"]
        if event["event_type"] == "FlightLanded"
    }
    people = defaultdict(list)
    for event in streams["passenger_events"]:
        people[event["passenger_id"]].append(event)
    saw_divert_passengers = False
    for events in people.values():
        kinds = {event["event_type"] for event in events}
        assert not ({"PassengerBoarded", "PassengerNoShow"} <= kinds)
        flight_id = events[0]["flight_id"]
        off = parse_ts(departed[flight_id]["occurred_at"])
        assert events[0]["destination_iata"] == departed[flight_id]["destination_iata"]
        if departed[flight_id]["destination_iata"] != landed[flight_id]["destination_iata"]:
            saw_divert_passengers = True
        if "PassengerBoarded" in kinds:
            checkin = next(event for event in events if event["event_type"] == "PassengerCheckedIn")
            boarded = next(event for event in events if event["event_type"] == "PassengerBoarded")
            assert parse_ts(checkin["occurred_at"]) < parse_ts(boarded["occurred_at"]) < off
        if "BagDropped" in kinds:
            checkin = next(event for event in events if event["event_type"] == "PassengerCheckedIn")
            bag = next(event for event in events if event["event_type"] == "BagDropped")
            assert parse_ts(checkin["occurred_at"]) < parse_ts(bag["occurred_at"]) < off
            assert bag["bags"] == checkin["bags"] >= 1
        if "PassengerNoShow" in kinds:
            assert "PassengerBoarded" not in kinds
    assert saw_divert_passengers


def test_a_tail_does_not_overlap_and_moves_from_where_it_landed():
    streams, _manifest = _batch()
    legs = []
    grouped = defaultdict(list)
    for event in streams["flight_events"]:
        grouped[event["flight_id"]].append(event)
    for events in grouped.values():
        kinds = {event["event_type"]: event for event in events}
        if "FlightDeparted" not in kinds:
            continue
        legs.append(
            {
                "tail": kinds["FlightDeparted"]["tail_number"],
                "origin": kinds["FlightDeparted"]["origin_iata"],
                "arrival": kinds["FlightLanded"]["destination_iata"],
                "off": parse_ts(kinds["FlightDeparted"]["occurred_at"]),
                "on": parse_ts(kinds["FlightArrived"]["occurred_at"]),
            }
        )
    by_tail = defaultdict(list)
    for leg in legs:
        by_tail[leg["tail"]].append(leg)
    assert by_tail
    for tail_legs in by_tail.values():
        tail_legs.sort(key=lambda leg: leg["off"])
        for previous, nxt in zip(tail_legs, tail_legs[1:]):
            assert nxt["origin"] == previous["arrival"]
            assert nxt["off"] >= previous["on"] + timedelta(minutes=45)


def test_same_seed_is_stable_and_the_cli_writes(tmp_path):
    first, first_manifest = generate_airports(airport_params("test", 42))
    second, second_manifest = generate_airports(airport_params("test", 42))
    assert first == second
    assert first_manifest == second_manifest
    env = {"PATH": os.environ["PATH"]}
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipelines.datasets",
            "airports",
            "--scale",
            "test",
            "--seed",
            "42",
            "--output",
            str(tmp_path),
        ],
        cwd=repo_path(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["counts"]["flight_events"] == len(first["flight_events"])


def _assert_sorted(records: list[dict], field: str) -> None:
    stamps = [record[field] for record in records]
    assert stamps == sorted(stamps)


def _assert_nearby_weather(index: dict, iata: str, occurred_at: str) -> None:
    moment = parse_ts(occurred_at).replace(minute=0, second=0, microsecond=0)
    key = (iata, moment.strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert key in index


def _assert_distance_and_course(record: dict, airports: dict[str, dict]) -> None:
    origin = airports[record["origin_iata"]]
    destination = airports[record["destination_iata"]]
    alternate = airports[record["alternate_iata"]]
    if record["stage"] == "airborne":
        previous = airports[record["previous_destination_iata"]]
        point = intermediate_point(
            float(origin["latitude"]),
            float(origin["longitude"]),
            float(previous["latitude"]),
            float(previous["longitude"]),
            float(Decimal(record["decision_fraction"])),
        )
        start = point
    else:
        start = (float(origin["latitude"]), float(origin["longitude"]))
    distance = great_circle_nm(start[0], start[1], float(destination["latitude"]), float(destination["longitude"]))
    course = initial_course_deg(start[0], start[1], float(destination["latitude"]), float(destination["longitude"]))
    assert record["distance_nm"] == format(distance, ".2f")
    assert record["course_deg"] == course
    alternate_distance = great_circle_nm(
        float(destination["latitude"]),
        float(destination["longitude"]),
        float(alternate["latitude"]),
        float(alternate["longitude"]),
    )
    assert record["alternate_distance_nm"] == format(alternate_distance, ".2f")


def _category(ceiling: int | None, visibility: str) -> str:
    miles = Decimal(visibility)
    if miles < Decimal("1.0") or (ceiling is not None and ceiling < 500):
        return "LIFR"
    if miles < Decimal("3.0") or (ceiling is not None and ceiling < 1000):
        return "IFR"
    if miles < Decimal("5.0") or (ceiling is not None and ceiling <= 3000):
        return "MVFR"
    return "VFR"
