"""Synthetic airport traffic for a US network.

The simulation is a history, not a live feed. One seeded run writes reference
data plus four event streams:

- ``flight_events`` — schedule, delay, departure, airborne, en-route, landing,
  arrival, cancellation, and reroute
- ``passenger_events`` — check-in, bag drop, boarding, and no-show
- ``weather_reports`` — one METAR-inspired report per airport per hour
- ``fuel_records`` — the inputs and the rounded result of ``fuel.compute_fuel``

Default beefy scale is 30 days from 2026-04-01, 800 flights a day, and a
cabin sample of 8 fictional passengers on each flight that departs. Full
manifests are not generated. Tail numbers are synthetic. See
``docs/airport-data-model.md`` for the assumptions and the fuel formula.

The mega preset is a separate, day-sharded writer (about 20,000 flights a
day for 60 days on 100 airports). It does not go through ``generate_airports``,
so the test, sample, and beefy files stay byte-identical. See
``docs/airport-mega-dataset.md``.

A few scripted flights (one cancellation, one delay, one pre-departure
reroute, one airborne diversion) are always present so a small seed still
covers every subject. Further delays, cancellations, and reroutes come from
the weather at the scheduled hour.
"""

from __future__ import annotations

import hashlib
import json
import random
import tempfile
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from pipelines.datasets.common import (
    SCHEMA_VERSION,
    dumps_record,
    floor_hour,
    format_ts,
    money,
    parse_ts,
    quantize,
)
from pipelines.datasets.fuel import (
    burn_so_far,
    compute_fuel,
    great_circle_nm,
    headwind_kt,
    initial_course_deg,
    intermediate_point,
)
from pipelines.datasets.reference import (
    AIRCRAFT_BY_ICAO,
    AIRLINES,
    AIRPORTS,
    TAXI_IN_MINUTES,
    TAXI_OUT_MINUTES,
    TURNAROUND_MINUTES,
    AircraftType,
    Airline,
    Airport,
    mega_network,
)

EXTRA_FUEL = ("80.00", "120.00", "150.00", "200.00", "250.00", "400.00")
FRACTIONS = (Decimal("0.35"), Decimal("0.40"), Decimal("0.45"), Decimal("0.50"))
MIN_STAGE_NM = Decimal("180")
MAX_STAGE_NM = Decimal("2400")
PASSENGER_NAMES_FIRST = (
    "Avery", "Blake", "Casey", "Drew", "Ellis", "Finley", "Gray", "Harper",
    "Indigo", "Jules", "Kai", "Logan", "Marlow", "Noah", "Oakley", "Parker",
    "Quinn", "Reese", "Sawyer", "Tatum",
)
PASSENGER_NAMES_LAST = (
    "Birch", "Cobalt", "Dune", "Ember", "Fern", "Grove", "Hale", "Ivy",
    "Juniper", "Keene", "Lark", "Moss", "North", "Pebble", "Quill", "Ridge",
    "Stone", "Vale", "Wynn", "York",
)


@dataclass(frozen=True)
class AirportParams:
    scale: str
    seed: int
    days: int
    flights_per_day: int
    pax_per_flight: int
    start: str = "2026-04-01T00:00:00Z"
    airport_count: int = 22


AIRPORT_SCALES: dict[str, AirportParams] = {
    "test": AirportParams("test", 42, 1, 12, 4),
    "sample": AirportParams("sample", 42, 2, 24, 5),
    "beefy": AirportParams("beefy", 42, 30, 800, 8),
    # 20_000 flights/day * 60 days. Passenger sample is ~25 per departed flight.
    # airport_count includes the original 22 plus synthetic fields.
    "mega": AirportParams("mega", 42, 60, 20_000, 25, airport_count=100),
}

# One shard may emit at most this many rows. The id is
# ``day_index * BLOCK + seq`` (seq starts at 1), so two days never share an id.
# Flight and fuel ids stay inside 8 digits at day 59. Passenger event ids use
# 9 digits. Passenger ids use 8 digits.
FLIGHT_EVENT_BLOCK = 200_000
FUEL_EVENT_BLOCK = 50_000
PASSENGER_EVENT_BLOCK = 3_000_000
PASSENGER_ID_BLOCK = 800_000
PART_BYTES = 128 * 1024 * 1024
_MIN_FLIGHTS_PER_SHARD = 4


def airport_params(scale: str, seed: int | None = None) -> AirportParams:
    if scale not in AIRPORT_SCALES:
        known = ", ".join(sorted(AIRPORT_SCALES))
        raise ValueError(f"unknown airport scale {scale!r} (expected {known})")
    params = AIRPORT_SCALES[scale]
    if seed is not None:
        params = replace(params, seed=seed)
    return params


def flight_category(ceiling_ft: int | None, visibility_sm: str) -> str:
    """METAR-inspired flight category from ceiling and statute-mile visibility.

    Unlimited ceiling is ``None``. Closed airports still use this function and
    are marked with ``airport_closed`` separately. Thresholds: LIFR below 500 ft
    or 1 mile, IFR below 1000 ft or 3 miles, MVFR at or below 3000 ft or below
    5 miles, otherwise VFR.
    """
    visibility = Decimal(visibility_sm)
    if visibility < Decimal("1.0") or (ceiling_ft is not None and ceiling_ft < 500):
        return "LIFR"
    if visibility < Decimal("3.0") or (ceiling_ft is not None and ceiling_ft < 1000):
        return "IFR"
    if visibility < Decimal("5.0") or (ceiling_ft is not None and ceiling_ft <= 3000):
        return "MVFR"
    return "VFR"


@dataclass
class FuelPicture:
    stage: str
    event_type: str
    occurred_at: datetime
    origin: Airport
    destination: Airport
    previous: Airport | None
    alternate: Airport
    distance_nm: Decimal
    course_deg: int
    wind_dir_deg: int
    wind_speed_kt: int
    wind_component_kt: int
    weather_report_id: str
    decision_fraction: str
    burn_so_far_kg: Decimal
    taxi_fuel_kg: Decimal
    extra_fuel_kg: Decimal
    uplift_kg: Decimal
    remaining_fuel_kg: Decimal
    estimated_burn_kg: Decimal
    computation_ground_speed: int
    computation_trip_minutes: int
    computation_trip_fuel: Decimal
    computation_contingency: Decimal
    computation_alternate_fuel: Decimal
    computation_reserve: Decimal
    computation_planned: Decimal
    aircraft: AircraftType


@dataclass
class Flight:
    flight_id: str
    flight_number: str
    airline_iata: str
    aircraft_type: str
    origin: Airport
    scheduled_destination: Airport
    scheduled_departure: datetime
    planned_distance_nm: Decimal
    planned_course_deg: int
    scenario: str = "normal"
    locked: bool = False
    delay_minutes: int = 0
    delay_reason: str | None = None
    cancel_reason: str | None = None
    reroute_reason: str | None = None
    decision_fraction: Decimal = Decimal("0.40")
    extra_fuel_kg: Decimal = Decimal("150.00")
    divert_to: Airport | None = None
    tail_number: str = ""
    departure_destination: Airport | None = None
    arrival_destination: Airport | None = None
    departure_alternate: Airport | None = None
    arrival_alternate: Airport | None = None
    planned_alternate: Airport | None = None
    actual_departure: datetime | None = None
    airborne_at: datetime | None = None
    enroute_at: datetime | None = None
    decision_at: datetime | None = None
    landed_at: datetime | None = None
    arrived_at: datetime | None = None
    published_arrival: datetime | None = None
    revised_arrival: datetime | None = None
    preflight: FuelPicture | None = None
    revised: FuelPicture | None = None


@dataclass
class Tail:
    tail_number: str
    airline_iata: str
    aircraft_type: str
    seats: int
    location: str
    next_free: datetime
    legs: list[Flight] = field(default_factory=list)


def generate_airports(params: AirportParams) -> tuple[dict[str, list[dict]], dict]:
    """Return stream name to records, plus a manifest.

    The mega preset is not built here. A full mega run is tens of gigabytes
    and is written one day at a time by ``write_partitioned_airports``.
    """
    if params.scale == "mega":
        raise ValueError(
            "mega is written one day at a time; use write_partitioned_airports or --day-index"
        )
    rng = random.Random(params.seed)
    start = parse_ts(params.start)
    routes = _routes()
    weather, weather_index = _weather(rng, start, params.days * 24 + 30)
    flights = _draft_flights(rng, params, start, routes)
    _script_and_assign(flights, weather_index, rng)
    tails = _assign_tails(flights, start, weather_index)
    flight_events = _flight_events(flights, weather_index)
    fuel_records = _fuel_records(flights, flight_events)
    passenger_events = _passengers(rng, params, flights, flight_events)
    streams = {
        "airports": _airport_rows(),
        "airlines": _airline_rows(),
        "aircraft_types": _aircraft_type_rows(),
        "aircraft": _aircraft_rows(tails),
        "weather_reports": weather,
        "flight_events": flight_events,
        "fuel_records": fuel_records,
        "passenger_events": passenger_events,
    }
    scenarios = {name: 0 for name in ("normal", "delay", "cancel", "reroute_pre", "divert")}
    for flight in flights:
        scenarios[flight.scenario] = scenarios.get(flight.scenario, 0) + 1
    manifest = {
        "dataset": "airports",
        "synthetic": True,
        "seed": params.seed,
        "scale": params.scale,
        "start": params.start,
        "days": params.days,
        "flights_per_day": params.flights_per_day,
        "pax_per_flight": params.pax_per_flight,
        "notes": [
            "Fictional passengers and tail numbers. Airport and airline codes are public identifiers.",
            "Passenger rows are a cabin sample, not a full manifest.",
            "Fuel figures follow pipelines/datasets/fuel.py and are rounded half-up to 0.01 kg.",
        ],
        "counts": {name: len(records) for name, records in streams.items()},
        "scenarios": scenarios,
    }
    return streams, manifest


def _routes(
    airports: tuple[Airport, ...] | None = None,
) -> dict[str, list[tuple[Airport, Decimal, int]]]:
    airports = AIRPORTS if airports is None else airports
    by_origin: dict[str, list[tuple[Airport, Decimal, int]]] = {airport.iata: [] for airport in airports}
    for origin in airports:
        for destination in airports:
            if origin.iata == destination.iata:
                continue
            distance = great_circle_nm(origin.lat, origin.lon, destination.lat, destination.lon)
            if distance < MIN_STAGE_NM or distance > MAX_STAGE_NM:
                continue
            course = initial_course_deg(origin.lat, origin.lon, destination.lat, destination.lon)
            by_origin[origin.iata].append((destination, distance, course))
    empty = [iata for iata, options in by_origin.items() if not options]
    if empty:
        raise RuntimeError(f"airports with no routes: {empty}")
    return by_origin


def _weather(
    rng: random.Random,
    start: datetime,
    hours: int,
) -> tuple[list[dict], dict[tuple[str, str], dict]]:
    reports: list[dict] = []
    index: dict[tuple[str, str], dict] = {}
    for airport in AIRPORTS:
        category = "VFR"
        wind_dir = rng.randrange(0, 360)
        for hour in range(hours):
            roll = rng.random()
            if roll > 0.75:
                category = rng.choices(
                    ["VFR", "MVFR", "IFR", "LIFR", "CLOSED"],
                    weights=(88, 7, 3, 1.5, 0.5),
                    k=1,
                )[0]
            wind_dir = (wind_dir + rng.randint(-12, 12)) % 360
            wind_speed = rng.randint(4, 24)
            observed = start + timedelta(hours=hour)
            report = _weather_report(airport, observed, category, wind_dir, wind_speed, rng)
            reports.append(report)
            index[(airport.iata, report["observed_at"])] = report
    reports.sort(key=lambda record: (record["observed_at"], record["airport_iata"]))
    return reports, index


def _weather_report(
    airport: Airport,
    observed: datetime,
    category: str,
    wind_dir: int,
    wind_speed: int,
    rng: random.Random,
) -> dict:
    visibility, ceiling, precip, closed = _category_numbers(category, airport, rng)
    resolved = flight_category(ceiling, visibility)
    base = 18 - (airport.lat - 33) * 0.45
    temperature = _one_decimal(base + rng.uniform(-3, 3))
    dewpoint = _one_decimal(float(temperature) - rng.uniform(2, 9))
    altimeter_cents = rng.randint(2970, 3050)
    gust = wind_speed + rng.randint(6, 12) if wind_speed >= 18 else None
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"wx_{airport.icao}_{observed.strftime('%Y%m%d%H')}",
        "airport_iata": airport.iata,
        "airport_icao": airport.icao,
        "observed_at": format_ts(observed),
        "wind_dir_deg": wind_dir,
        "wind_speed_kt": wind_speed,
        "wind_gust_kt": gust,
        "visibility_sm": visibility,
        "ceiling_ft": ceiling,
        "precip": precip,
        "temp_c": temperature,
        "dewpoint_c": dewpoint,
        "altimeter_inhg": f"{altimeter_cents // 100}.{altimeter_cents % 100:02d}",
        "flight_category": resolved,
        "airport_closed": closed,
    }


def _category_numbers(
    category: str,
    airport: Airport,
    rng: random.Random,
) -> tuple[str, int | None, str, bool]:
    snow = airport.lat >= 36
    if category == "MVFR":
        return "4.0", 2500, rng.choice(("none", "rain")), False
    if category == "IFR":
        return "2.0", 800, "rain", False
    if category == "LIFR":
        return "0.5", 300, "snow" if snow else "thunderstorm", False
    if category == "CLOSED":
        return "0.2", 200, "snow" if snow else "thunderstorm", True
    return "10.0", None, "none", False


def _one_decimal(value: float) -> str:
    rounded = Decimal(f"{value:.6f}").quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if rounded == 0:
        return "0.0"
    return format(rounded, ".1f")


def _patch(index: dict[tuple[str, str], dict], airport: Airport, when: datetime, kind: str) -> None:
    report = _wx(index, airport.iata, when)
    snow = airport.lat >= 36
    if kind == "closed":
        visibility, ceiling, precip, closed = "0.2", 200, "snow" if snow else "thunderstorm", True
    elif kind == "thunderstorm":
        visibility, ceiling, precip, closed = "2.0", 800, "thunderstorm", False
    elif kind == "ifr":
        visibility, ceiling, precip, closed = "2.0", 800, "rain", False
    else:
        raise ValueError(kind)
    report["visibility_sm"] = visibility
    report["ceiling_ft"] = ceiling
    report["precip"] = precip
    report["airport_closed"] = closed
    report["flight_category"] = flight_category(ceiling, visibility)


def _wx(index: dict[tuple[str, str], dict], iata: str, when: datetime) -> dict:
    key = (iata, format_ts(floor_hour(when)))
    try:
        return index[key]
    except KeyError as exc:
        raise RuntimeError(f"no weather for {iata} at {key[1]}") from exc


def _draft_flights(
    rng: random.Random,
    params: AirportParams,
    start: datetime,
    routes: dict[str, list[tuple[Airport, Decimal, int]]],
    airports: tuple[Airport, ...] | None = None,
    only_day: int | None = None,
) -> list[Flight]:
    airports = AIRPORTS if airports is None else airports
    weights = [airport.departure_weight for airport in airports]
    counters = {airline.iata: 1000 for airline in AIRLINES}
    flights: list[Flight] = []
    day_range = range(params.days) if only_day is None else (only_day,)
    for day in day_range:
        day_start = start + timedelta(days=day)
        for slot in range(params.flights_per_day):
            minute = _departure_minute(rng, slot, params.flights_per_day)
            scheduled = day_start + timedelta(minutes=minute)
            origin, destination, distance, course, airline, aircraft = _pick_leg(
                rng, routes, weights, airports
            )
            number = counters[airline.iata]
            counters[airline.iata] = number + 1
            flight_number = f"{airline.iata}{number:04d}"
            flights.append(
                Flight(
                    flight_id=(
                        f"flt_{scheduled.strftime('%Y%m%d')}_{flight_number}_"
                        f"{origin.iata}_{destination.iata}"
                    ),
                    flight_number=flight_number,
                    airline_iata=airline.iata,
                    aircraft_type=aircraft,
                    origin=origin,
                    scheduled_destination=destination,
                    scheduled_departure=scheduled,
                    planned_distance_nm=distance,
                    planned_course_deg=course,
                    extra_fuel_kg=Decimal(rng.choice(EXTRA_FUEL)),
                    decision_fraction=rng.choice(FRACTIONS),
                )
            )
    return flights


def _departure_minute(rng: random.Random, slot: int, flights_per_day: int) -> int:
    span_start = 12 * 60
    span_end = 23 * 60 + 30
    span = span_end - span_start
    width = max(1, span // flights_per_day)
    base = span_start + (slot * span) // flights_per_day
    return min(span_end, base + rng.randint(0, width))


def _pick_leg(
    rng: random.Random,
    routes: dict[str, list[tuple[Airport, Decimal, int]]],
    weights: list[int],
    airports: tuple[Airport, ...] | None = None,
) -> tuple[Airport, Airport, Decimal, int, Airline, str]:
    airports = AIRPORTS if airports is None else airports
    for _ in range(80):
        origin = rng.choices(airports, weights=weights, k=1)[0]
        destination, distance, course = rng.choice(routes[origin.iata])
        picked = _pick_airline(rng, origin, distance)
        if picked is not None:
            airline, aircraft = picked
            return origin, destination, distance, course, airline, aircraft
    raise RuntimeError("could not draft a flight on the network")


def _pick_airline(
    rng: random.Random,
    origin: Airport,
    distance: Decimal,
) -> tuple[Airline, str] | None:
    options: list[tuple[Airline, list[str], int]] = []
    for airline in AIRLINES:
        fleet = [
            icao
            for icao in airline.fleet
            if AIRCRAFT_BY_ICAO[icao].max_stage_nm >= distance
        ]
        if fleet:
            weight = 5 if origin.iata in airline.hubs else 1
            options.append((airline, fleet, weight))
    if not options:
        return None
    airline = rng.choices(
        [item[0] for item in options],
        weights=[item[2] for item in options],
        k=1,
    )[0]
    fleet = next(item[1] for item in options if item[0] is airline)
    return airline, rng.choice(fleet)


def _script_and_assign(
    flights: list[Flight],
    weather_index: dict[tuple[str, str], dict],
    rng: random.Random,
    airports: tuple[Airport, ...] | None = None,
) -> None:
    airports = AIRPORTS if airports is None else airports
    _script(flights, weather_index)
    for flight in flights:
        if flight.locked:
            continue
        _assign_from_weather(flight, weather_index, rng)
    for flight in flights:
        if flight.scenario == "reroute_pre":
            flight.divert_to = _pick_reroute_destination(flight, weather_index, airports)


def _script(flights: list[Flight], weather_index: dict[tuple[str, str], dict]) -> None:
    longest = sorted(flights, key=lambda flight: (flight.planned_distance_nm, flight.flight_id), reverse=True)
    divert, reroute = longest[0], longest[1]
    divert.scenario = "divert"
    divert.locked = True
    divert.reroute_reason = "fuel"
    divert.decision_fraction = Decimal("0.40")
    reroute.scenario = "reroute_pre"
    reroute.locked = True
    reroute.reroute_reason = "weather"
    reroute.delay_minutes = 25
    reroute.delay_reason = "weather"
    used = {_hour_key(divert), _hour_key(reroute)}
    cancel = None
    delay = None
    for flight in sorted(flights, key=lambda item: item.flight_id):
        if flight.locked or _hour_key(flight) in used:
            continue
        if cancel is None:
            cancel = flight
            used.add(_hour_key(flight))
            continue
        delay = flight
        used.add(_hour_key(flight))
        break
    if cancel is None or delay is None:
        raise RuntimeError("not enough flights to script a cancellation and a delay")
    cancel.scenario = "cancel"
    cancel.locked = True
    cancel.cancel_reason = "airport_closure"
    delay.scenario = "delay"
    delay.locked = True
    delay.delay_minutes = 45
    delay.delay_reason = "weather"
    _patch(weather_index, cancel.origin, cancel.scheduled_departure, "closed")
    _patch(weather_index, delay.origin, delay.scheduled_departure, "ifr")
    _patch(weather_index, reroute.scheduled_destination, _rough_eta(reroute), "thunderstorm")


def _hour_key(flight: Flight) -> tuple[str, str]:
    return flight.origin.iata, format_ts(floor_hour(flight.scheduled_departure))


def _rough_eta(flight: Flight) -> datetime:
    aircraft = AIRCRAFT_BY_ICAO[flight.aircraft_type]
    hours = flight.planned_distance_nm / Decimal(aircraft.cruise_tas_kt)
    minutes = int((hours * Decimal(60)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return flight.scheduled_departure + timedelta(minutes=TAXI_OUT_MINUTES + minutes + TAXI_IN_MINUTES)


def _assign_from_weather(
    flight: Flight,
    weather_index: dict[tuple[str, str], dict],
    rng: random.Random,
) -> None:
    origin_wx = _wx(weather_index, flight.origin.iata, flight.scheduled_departure)
    dest_wx = _wx(weather_index, flight.scheduled_destination.iata, _rough_eta(flight))
    origin_bad = origin_wx["airport_closed"] or origin_wx["flight_category"] in {"IFR", "LIFR"}
    dest_bad = (
        dest_wx["airport_closed"]
        or dest_wx["flight_category"] in {"IFR", "LIFR"}
        or dest_wx["precip"] == "thunderstorm"
    )
    if origin_wx["airport_closed"] or dest_wx["airport_closed"]:
        flight.scenario = "cancel"
        flight.cancel_reason = "airport_closure"
        return
    if origin_bad or dest_bad:
        roll = rng.random()
        if roll < 0.10:
            flight.scenario = "cancel"
            flight.cancel_reason = "weather"
        elif roll < 0.16 and dest_bad:
            flight.scenario = "divert"
            flight.reroute_reason = "weather"
        elif roll < 0.22 and dest_bad:
            flight.scenario = "reroute_pre"
            flight.reroute_reason = "weather"
            flight.delay_minutes = max(flight.delay_minutes, 20)
            flight.delay_reason = "weather"
        elif roll < 0.62:
            flight.scenario = "delay"
            flight.delay_minutes = rng.choice((20, 35, 50, 75))
            flight.delay_reason = "weather"
        return
    if rng.random() < 0.12:
        flight.scenario = "delay"
        flight.delay_minutes = rng.choice((15, 25, 40))
        flight.delay_reason = rng.choice(("atc", "crew", "mechanical"))


def _pick_reroute_destination(
    flight: Flight,
    weather_index: dict[tuple[str, str], dict],
    airports: tuple[Airport, ...] | None = None,
) -> Airport:
    airports = AIRPORTS if airports is None else airports
    aircraft = AIRCRAFT_BY_ICAO[flight.aircraft_type]
    when = flight.scheduled_departure + timedelta(minutes=flight.delay_minutes + 90)
    ranked: list[tuple[int, Decimal, str, Airport]] = []
    for airport in airports:
        if airport.iata in {flight.origin.iata, flight.scheduled_destination.iata}:
            continue
        stage = great_circle_nm(flight.origin.lat, flight.origin.lon, airport.lat, airport.lon)
        if stage > Decimal(aircraft.max_stage_nm):
            continue
        closed = _wx(weather_index, airport.iata, when)["airport_closed"]
        proximity = great_circle_nm(
            flight.scheduled_destination.lat,
            flight.scheduled_destination.lon,
            airport.lat,
            airport.lon,
        )
        ranked.append((1 if closed else 0, proximity, airport.iata, airport))
    if not ranked:
        raise RuntimeError(f"no reroute destination for {flight.flight_id}")
    ranked.sort()
    open_fields = [item for item in ranked if item[0] == 0]
    chosen = (open_fields or ranked)[0][3]
    if chosen.iata == flight.scheduled_destination.iata:
        raise RuntimeError("reroute destination did not change")
    return chosen


def _assign_tails(
    flights: list[Flight],
    start: datetime,
    weather_index: dict[tuple[str, str], dict],
) -> list[Tail]:
    tails: list[Tail] = []
    counters = {airline.iata: 1 for airline in AIRLINES}
    ordered = sorted(
        flights,
        key=lambda flight: (
            flight.scheduled_departure + timedelta(minutes=flight.delay_minutes),
            flight.flight_id,
        ),
    )
    for flight in ordered:
        desired = flight.scheduled_departure + timedelta(minutes=flight.delay_minutes)
        tail = _take_tail(tails, counters, flight, desired, start)
        if flight.scenario != "cancel" and tail.next_free > desired:
            slip = int((tail.next_free - flight.scheduled_departure).total_seconds() // 60)
            if flight.delay_minutes < slip:
                flight.delay_minutes = slip
                if flight.delay_minutes >= 15 and flight.delay_reason is None:
                    flight.delay_reason = "late_inbound"
        flight.tail_number = tail.tail_number
        _realize(flight, weather_index)
        if flight.scenario == "cancel":
            continue
        if flight.arrived_at is None or flight.arrival_destination is None:
            raise RuntimeError(f"{flight.flight_id} did not arrive")
        tail.location = flight.arrival_destination.iata
        tail.next_free = flight.arrived_at + timedelta(minutes=TURNAROUND_MINUTES)
        tail.legs.append(flight)
    return sorted(tails, key=lambda tail: tail.tail_number)


def _spawn(tails: list[Tail], counters: dict[str, int], flight: Flight, start: datetime) -> Tail:
    aircraft = AIRCRAFT_BY_ICAO[flight.aircraft_type]
    number = counters[flight.airline_iata]
    counters[flight.airline_iata] = number + 1
    tail = Tail(
        tail_number=f"T-{flight.airline_iata}{number:04d}",
        airline_iata=flight.airline_iata,
        aircraft_type=flight.aircraft_type,
        seats=aircraft.seats,
        location=flight.origin.iata,
        next_free=start,
    )
    tails.append(tail)
    return tail


def _take_tail(
    tails: list[Tail],
    counters: dict[str, int],
    flight: Flight,
    desired: datetime,
    start: datetime,
) -> Tail:
    matching = [
        tail
        for tail in tails
        if tail.airline_iata == flight.airline_iata
        and tail.aircraft_type == flight.aircraft_type
        and tail.location == flight.origin.iata
    ]
    if not matching:
        return _spawn(tails, counters, flight, start)
    ready = [tail for tail in matching if tail.next_free <= desired]
    pool = ready or matching
    return min(pool, key=lambda tail: (tail.next_free, tail.tail_number))


def _realize(
    flight: Flight,
    weather_index: dict[tuple[str, str], dict],
    airports: tuple[Airport, ...] | None = None,
) -> None:
    """Fill clock times and fuel pictures from the weather index."""
    airports = AIRPORTS if airports is None else airports
    aircraft = AIRCRAFT_BY_ICAO[flight.aircraft_type]
    flight.actual_departure = flight.scheduled_departure + timedelta(minutes=flight.delay_minutes)
    original_alternate = _nearest(
        flight.scheduled_destination,
        {flight.origin.iata, flight.scheduled_destination.iata},
        airports,
    )
    scheduled_wx = _wx(weather_index, flight.origin.iata, flight.scheduled_departure)
    flight.preflight = _leg_picture(
        stage="preflight",
        event_type="FuelCalculated",
        occurred_at=flight.scheduled_departure - timedelta(hours=6),
        origin=flight.origin,
        destination=flight.scheduled_destination,
        previous=None,
        alternate=original_alternate,
        distance_nm=flight.planned_distance_nm,
        course_deg=flight.planned_course_deg,
        wind_report=scheduled_wx,
        fraction="0.00",
        burn_already=Decimal("0.00"),
        taxi=aircraft.taxi,
        extra=Decimal("0.00") if flight.scenario == "cancel" else flight.extra_fuel_kg,
        uplift_override=Decimal("0.00") if flight.scenario == "cancel" else None,
        aircraft=aircraft,
        cancelled=flight.scenario == "cancel",
    )
    flight.published_arrival = flight.scheduled_departure + timedelta(
        minutes=TAXI_OUT_MINUTES + flight.preflight.computation_trip_minutes + TAXI_IN_MINUTES
    )
    flight.planned_alternate = original_alternate
    flight.departure_alternate = original_alternate
    flight.arrival_alternate = original_alternate
    flight.departure_destination = flight.scheduled_destination
    flight.arrival_destination = flight.scheduled_destination
    flight.revised_arrival = flight.published_arrival
    if flight.scenario == "cancel":
        return
    if flight.scenario == "reroute_pre":
        if flight.divert_to is None:
            raise RuntimeError(f"{flight.flight_id} has no reroute destination")
        new_dest = flight.divert_to
        distance = great_circle_nm(flight.origin.lat, flight.origin.lon, new_dest.lat, new_dest.lon)
        course = initial_course_deg(flight.origin.lat, flight.origin.lon, new_dest.lat, new_dest.lon)
        alternate = _nearest(new_dest, {flight.origin.iata, new_dest.iata}, airports)
        wind_report = _wx(weather_index, flight.origin.iata, flight.actual_departure)
        flight.revised = _leg_picture(
            stage="preflight",
            event_type="FuelRevised",
            occurred_at=flight.scheduled_departure - timedelta(hours=4),
            origin=flight.origin,
            destination=new_dest,
            previous=flight.scheduled_destination,
            alternate=alternate,
            distance_nm=distance,
            course_deg=course,
            wind_report=wind_report,
            fraction="0.00",
            burn_already=Decimal("0.00"),
            taxi=aircraft.taxi,
            extra=flight.extra_fuel_kg,
            uplift_override=None,
            aircraft=aircraft,
            cancelled=False,
        )
        flight.departure_destination = new_dest
        flight.arrival_destination = new_dest
        flight.departure_alternate = alternate
        flight.arrival_alternate = alternate
        trip_minutes = flight.revised.computation_trip_minutes
        flight.revised_arrival = flight.scheduled_departure + timedelta(
            minutes=TAXI_OUT_MINUTES + trip_minutes + TAXI_IN_MINUTES
        )
        _set_airborne_times(flight, trip_minutes, decision_minutes=None)
        return
    if flight.scenario == "divert":
        _realize_diversion(flight, aircraft, weather_index, airports)
        return
    _set_airborne_times(flight, flight.preflight.computation_trip_minutes, decision_minutes=None)


def _realize_diversion(
    flight: Flight,
    aircraft: AircraftType,
    weather_index: dict[tuple[str, str], dict],
    airports: tuple[Airport, ...] | None = None,
) -> None:
    airports = AIRPORTS if airports is None else airports
    if flight.preflight is None or flight.actual_departure is None:
        raise RuntimeError(f"{flight.flight_id} is missing its preflight plan")
    fraction = flight.decision_fraction
    trip_minutes = flight.preflight.computation_trip_minutes
    decision_minutes = int(
        (Decimal(trip_minutes) * fraction).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    decision_minutes = min(max(decision_minutes, 2), max(2, trip_minutes - 1))
    airborne = flight.actual_departure + timedelta(minutes=TAXI_OUT_MINUTES)
    decision_at = airborne + timedelta(minutes=decision_minutes)
    burned = burn_so_far(aircraft.taxi, flight.preflight.computation_trip_fuel, fraction)
    remaining = quantize(flight.preflight.uplift_kg - burned)
    point = intermediate_point(
        flight.origin.lat,
        flight.origin.lon,
        flight.scheduled_destination.lat,
        flight.scheduled_destination.lon,
        float(fraction),
    )
    chosen = _choose_diversion(
        flight,
        point,
        decision_at,
        remaining,
        aircraft,
        weather_index,
        airports,
    )
    if chosen is None and flight.locked:
        raise RuntimeError(f"scripted diversion {flight.flight_id} has no reachable airport")
    if chosen is None:
        flight.scenario = "delay"
        if flight.delay_minutes < 30:
            flight.delay_minutes = 30
            flight.delay_reason = flight.delay_reason or "weather"
        _set_airborne_times(flight, trip_minutes, decision_minutes=None)
        return
    new_dest, distance, course, wind_report = chosen
    alternate = _nearest(
        new_dest,
        {flight.origin.iata, new_dest.iata, flight.scheduled_destination.iata},
        airports,
    )
    flight.revised = _leg_picture(
        stage="airborne",
        event_type="FuelRevised",
        occurred_at=decision_at,
        origin=flight.origin,
        destination=new_dest,
        previous=flight.scheduled_destination,
        alternate=alternate,
        distance_nm=distance,
        course_deg=course,
        wind_report=wind_report,
        fraction=format(fraction, ".2f"),
        burn_already=burned,
        taxi=Decimal("0.00"),
        extra=Decimal("0.00"),
        uplift_override=flight.preflight.uplift_kg,
        aircraft=aircraft,
        cancelled=False,
    )
    if flight.revised.remaining_fuel_kg < 0:
        raise RuntimeError(f"negative remaining fuel on {flight.flight_id}")
    flight.divert_to = new_dest
    flight.departure_destination = flight.scheduled_destination
    flight.arrival_destination = new_dest
    flight.arrival_alternate = alternate
    flight.decision_at = decision_at
    flight.airborne_at = airborne
    flight.enroute_at = airborne + timedelta(minutes=max(1, decision_minutes // 2))
    flight.landed_at = decision_at + timedelta(minutes=max(1, flight.revised.computation_trip_minutes))
    flight.arrived_at = flight.landed_at + timedelta(minutes=TAXI_IN_MINUTES)


def _choose_diversion(
    flight: Flight,
    point: tuple[float, float],
    when: datetime,
    remaining_kg: Decimal,
    aircraft: AircraftType,
    weather_index: dict[tuple[str, str], dict],
    airports: tuple[Airport, ...] | None = None,
) -> tuple[Airport, Decimal, int, dict] | None:
    airports = AIRPORTS if airports is None else airports
    ranked: list[tuple[int, int, Decimal, str, Airport, Decimal, int, dict]] = []
    for airport in airports:
        if airport.iata == flight.scheduled_destination.iata:
            continue
        distance = great_circle_nm(point[0], point[1], airport.lat, airport.lon)
        course = initial_course_deg(point[0], point[1], airport.lat, airport.lon)
        try:
            wind_report = _wx(weather_index, airport.iata, when)
        except RuntimeError:
            continue
        if wind_report["airport_closed"]:
            continue
        component = headwind_kt(wind_report["wind_dir_deg"], wind_report["wind_speed_kt"], course)
        picture = compute_fuel(
            distance_nm=distance,
            cruise_tas_kt=aircraft.cruise_tas_kt,
            headwind_kt=component,
            burn_kg_per_hour=aircraft.burn,
            taxi_fuel_kg=Decimal("0.00"),
            alternate_distance_nm=Decimal("1.00"),
            reserve_minutes=aircraft.reserve_minutes,
            burn_so_far_kg=Decimal("0.00"),
        )
        if remaining_kg - picture.trip_fuel_kg < 0:
            continue
        origin_penalty = 0 if airport.iata != flight.origin.iata else 1
        category_rank = {"VFR": 0, "MVFR": 1, "IFR": 2, "LIFR": 3}[wind_report["flight_category"]]
        ranked.append(
            (origin_penalty, category_rank, distance, airport.iata, airport, distance, course, wind_report)
        )
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    _penalty, _rank, _distance, _iata, airport, distance, course, wind_report = ranked[0]
    return airport, distance, course, wind_report


def _set_airborne_times(flight: Flight, trip_minutes: int, decision_minutes: int | None) -> None:
    if flight.actual_departure is None:
        raise RuntimeError(f"{flight.flight_id} has no departure time")
    flight.airborne_at = flight.actual_departure + timedelta(minutes=TAXI_OUT_MINUTES)
    midpoint = max(1, trip_minutes // 2)
    flight.enroute_at = flight.airborne_at + timedelta(minutes=midpoint)
    flight.landed_at = flight.airborne_at + timedelta(minutes=max(trip_minutes, midpoint + 1))
    flight.arrived_at = flight.landed_at + timedelta(minutes=TAXI_IN_MINUTES)
    flight.decision_at = None if decision_minutes is None else flight.airborne_at + timedelta(minutes=decision_minutes)


def _leg_picture(
    *,
    stage: str,
    event_type: str,
    occurred_at: datetime,
    origin: Airport,
    destination: Airport,
    previous: Airport | None,
    alternate: Airport,
    distance_nm: Decimal,
    course_deg: int,
    wind_report: dict,
    fraction: str,
    burn_already: Decimal,
    taxi: Decimal,
    extra: Decimal,
    uplift_override: Decimal | None,
    aircraft: AircraftType,
    cancelled: bool,
) -> FuelPicture:
    component = headwind_kt(wind_report["wind_dir_deg"], wind_report["wind_speed_kt"], course_deg)
    alternate_distance = great_circle_nm(
        destination.lat, destination.lon, alternate.lat, alternate.lon
    )
    computed = compute_fuel(
        distance_nm=distance_nm,
        cruise_tas_kt=aircraft.cruise_tas_kt,
        headwind_kt=component,
        burn_kg_per_hour=aircraft.burn,
        taxi_fuel_kg=taxi,
        alternate_distance_nm=alternate_distance,
        reserve_minutes=aircraft.reserve_minutes,
        burn_so_far_kg=burn_already,
    )
    if cancelled or uplift_override == Decimal("0.00") and extra == Decimal("0.00") and cancelled:
        uplift = Decimal("0.00")
        estimated = Decimal("0.00")
        remaining = Decimal("0.00")
        extra = Decimal("0.00")
    elif uplift_override is None:
        uplift = quantize(computed.planned_fuel_kg + extra)
        estimated = computed.estimated_burn_kg
        remaining = quantize(uplift - estimated)
    else:
        uplift = uplift_override
        estimated = computed.estimated_burn_kg
        remaining = quantize(uplift - estimated)
    if not cancelled and remaining < 0:
        raise RuntimeError("remaining fuel went negative")
    return FuelPicture(
        stage=stage,
        event_type=event_type,
        occurred_at=occurred_at,
        origin=origin,
        destination=destination,
        previous=previous,
        alternate=alternate,
        distance_nm=distance_nm,
        course_deg=course_deg,
        wind_dir_deg=wind_report["wind_dir_deg"],
        wind_speed_kt=wind_report["wind_speed_kt"],
        wind_component_kt=component,
        weather_report_id=wind_report["report_id"],
        decision_fraction=fraction,
        burn_so_far_kg=burn_already,
        taxi_fuel_kg=taxi,
        extra_fuel_kg=extra,
        uplift_kg=uplift,
        remaining_fuel_kg=remaining,
        estimated_burn_kg=estimated,
        computation_ground_speed=computed.ground_speed_kt,
        computation_trip_minutes=computed.trip_minutes,
        computation_trip_fuel=computed.trip_fuel_kg,
        computation_contingency=computed.contingency_fuel_kg,
        computation_alternate_fuel=computed.alternate_fuel_kg,
        computation_reserve=computed.reserve_fuel_kg,
        computation_planned=computed.planned_fuel_kg,
        aircraft=aircraft,
    )


def _nearest(
    destination: Airport,
    exclude: set[str],
    airports: tuple[Airport, ...] | None = None,
) -> Airport:
    airports = AIRPORTS if airports is None else airports
    best: tuple[Decimal, str, Airport] | None = None
    banned = exclude | {destination.iata}
    for airport in airports:
        if airport.iata in banned:
            continue
        distance = great_circle_nm(destination.lat, destination.lon, airport.lat, airport.lon)
        if best is None or (distance, airport.iata) < (best[0], best[1]):
            best = (distance, airport.iata, airport)
    if best is None:
        raise RuntimeError(f"no alternate for {destination.iata}")
    return best[2]


def _stamp(
    records: list[dict],
    prefix: str,
    *,
    first: int = 1,
    width: int = 8,
) -> list[dict]:
    """Sort by time and sequence, then assign zero-padded ids in that order."""
    records.sort(key=lambda record: (record["occurred_at"], record["_seq"]))
    limit = 10**width
    for number, record in enumerate(records, start=first):
        if number >= limit:
            raise RuntimeError(f"{prefix} id {number} does not fit in {width} digits")
        record["event_id"] = f"{prefix}_{number:0{width}d}"
        del record["_seq"]
    return records


def _flight_events(
    flights: list[Flight],
    weather_index: dict[tuple[str, str], dict],
    *,
    first: int = 1,
    width: int = 8,
) -> list[dict]:
    records: list[dict] = []
    seq = 1
    for flight in sorted(flights, key=lambda item: item.flight_id):
        scheduled = flight.scheduled_departure - timedelta(hours=12)
        records.append(
            _flight_record(
                seq,
                "FlightScheduled",
                scheduled,
                flight,
                flight.scheduled_destination,
                flight.published_arrival,
                flight.planned_alternate,
                None,
                None,
            )
        )
        seq += 1
        if flight.scenario == "cancel":
            records.append(
                _flight_record(
                    seq,
                    "FlightCancelled",
                    flight.scheduled_departure - timedelta(hours=2),
                    flight,
                    flight.scheduled_destination,
                    flight.published_arrival,
                    flight.departure_alternate,
                    None,
                    _wx_id_for(weather_index, flight.origin, flight.scheduled_departure - timedelta(hours=2)),
                )
            )
            seq += 1
            continue
        if flight.scenario == "reroute_pre":
            records.append(
                _flight_record(
                    seq,
                    "FlightRerouted",
                    flight.scheduled_departure - timedelta(hours=4),
                    flight,
                    flight.departure_destination,
                    flight.revised_arrival,
                    flight.departure_alternate,
                    flight.scheduled_destination,
                    _wx_id_for(
                        weather_index,
                        flight.scheduled_destination,
                        flight.scheduled_departure - timedelta(hours=4),
                    ),
                )
            )
            seq += 1
        if flight.delay_minutes >= 15:
            records.append(
                _flight_record(
                    seq,
                    "FlightDelayed",
                    flight.actual_departure - timedelta(minutes=45),
                    flight,
                    flight.departure_destination,
                    flight.revised_arrival if flight.scenario == "reroute_pre" else flight.published_arrival,
                    flight.departure_alternate,
                    None,
                    _wx_id_for(
                        weather_index,
                        flight.origin,
                        flight.actual_departure - timedelta(minutes=45),
                    ),
                )
            )
            seq += 1
        departure_arrival = flight.revised_arrival if flight.scenario == "reroute_pre" else flight.published_arrival
        movement = (
            ("FlightDeparted", flight.actual_departure, flight.departure_destination, flight.origin),
            ("FlightAirborne", flight.airborne_at, flight.departure_destination, flight.origin),
            ("FlightEnRoute", flight.enroute_at, flight.departure_destination, flight.origin),
        )
        for event_type, occurred, dest, wx_airport in movement:
            records.append(
                _flight_record(
                    seq,
                    event_type,
                    occurred,
                    flight,
                    dest,
                    departure_arrival,
                    flight.departure_alternate,
                    None,
                    _wx_id_for(weather_index, wx_airport, occurred),
                )
            )
            seq += 1
        if flight.scenario == "divert":
            records.append(
                _flight_record(
                    seq,
                    "FlightRerouted",
                    flight.decision_at,
                    flight,
                    flight.arrival_destination,
                    flight.published_arrival,
                    flight.arrival_alternate,
                    flight.scheduled_destination,
                    _wx_id_for(weather_index, flight.scheduled_destination, flight.decision_at),
                )
            )
            seq += 1
        arrival_clock = flight.published_arrival if flight.scenario == "divert" else departure_arrival
        records.append(
            _flight_record(
                seq,
                "FlightLanded",
                flight.landed_at,
                flight,
                flight.arrival_destination,
                arrival_clock,
                flight.arrival_alternate,
                None,
                _wx_id_for(weather_index, flight.arrival_destination, flight.landed_at),
            )
        )
        seq += 1
        records.append(
            _flight_record(
                seq,
                "FlightArrived",
                flight.arrived_at,
                flight,
                flight.arrival_destination,
                arrival_clock,
                flight.arrival_alternate,
                None,
                _wx_id_for(weather_index, flight.arrival_destination, flight.arrived_at),
            )
        )
        seq += 1
    return _stamp(records, "fev", first=first, width=width)


def _wx_id_for(
    weather_index: dict[tuple[str, str], dict],
    airport: Airport | None,
    when: datetime | None,
) -> str | None:
    if airport is None or when is None:
        return None
    return _wx(weather_index, airport.iata, when)["report_id"]


def _flight_record(
    seq: int,
    event_type: str,
    occurred_at: datetime | None,
    flight: Flight,
    destination: Airport | None,
    scheduled_arrival: datetime | None,
    alternate: Airport | None,
    previous: Airport | None,
    weather_report_id: str | None,
) -> dict:
    if occurred_at is None or destination is None or scheduled_arrival is None or alternate is None:
        raise RuntimeError(f"incomplete {event_type} for {flight.flight_id}")
    status = {
        "FlightScheduled": "scheduled",
        "FlightDelayed": "delayed",
        "FlightDeparted": "departed",
        "FlightAirborne": "airborne",
        "FlightEnRoute": "enroute",
        "FlightLanded": "landed",
        "FlightArrived": "arrived",
        "FlightCancelled": "cancelled",
        "FlightRerouted": "diverted" if flight.scenario == "divert" and event_type == "FlightRerouted" else "rerouted",
    }[event_type]
    if event_type == "FlightRerouted" and flight.scenario == "reroute_pre":
        status = "rerouted"
    reason = None
    if event_type == "FlightCancelled":
        reason = flight.cancel_reason
    elif event_type == "FlightRerouted":
        reason = flight.reroute_reason
    elif event_type == "FlightDelayed" or flight.delay_minutes >= 15:
        if event_type != "FlightScheduled":
            reason = flight.delay_reason
    return {
        "_seq": seq,
        "schema_version": SCHEMA_VERSION,
        "event_id": "",
        "event_type": event_type,
        "occurred_at": format_ts(occurred_at),
        "flight_id": flight.flight_id,
        "flight_number": flight.flight_number,
        "airline_iata": flight.airline_iata,
        "tail_number": flight.tail_number,
        "aircraft_type": flight.aircraft_type,
        "origin_iata": flight.origin.iata,
        "origin_icao": flight.origin.icao,
        "destination_iata": destination.iata,
        "destination_icao": destination.icao,
        "scheduled_departure_at": format_ts(flight.scheduled_departure),
        "scheduled_arrival_at": format_ts(scheduled_arrival),
        "delay_minutes": flight.delay_minutes,
        "delay_reason": reason,
        "status": status,
        "previous_destination_iata": None if previous is None else previous.iata,
        "alternate_iata": alternate.iata,
        "route": f"{flight.origin.icao} DCT {destination.icao}",
        "weather_report_id": weather_report_id,
    }


def _fuel_records(
    flights: list[Flight],
    flight_events: list[dict],
    *,
    first: int = 1,
    width: int = 8,
) -> list[dict]:
    reroutes = {
        event["flight_id"]: event["event_id"]
        for event in flight_events
        if event["event_type"] == "FlightRerouted"
    }
    records: list[dict] = []
    seq = 1
    for flight in sorted(flights, key=lambda item: item.flight_id):
        if flight.preflight is None:
            raise RuntimeError(f"{flight.flight_id} has no fuel plan")
        records.append(_fuel_record(seq, flight, flight.preflight, None))
        seq += 1
        if flight.revised is not None:
            records.append(_fuel_record(seq, flight, flight.revised, reroutes.get(flight.flight_id)))
            seq += 1
    return _stamp(records, "fue", first=first, width=width)


def _fuel_record(seq: int, flight: Flight, picture: FuelPicture, reroute_event_id: str | None) -> dict:
    alternate_distance = great_circle_nm(
        picture.destination.lat,
        picture.destination.lon,
        picture.alternate.lat,
        picture.alternate.lon,
    )
    return {
        "_seq": seq,
        "schema_version": SCHEMA_VERSION,
        "event_id": "",
        "event_type": picture.event_type,
        "occurred_at": format_ts(picture.occurred_at),
        "flight_id": flight.flight_id,
        "tail_number": flight.tail_number,
        "aircraft_type": flight.aircraft_type,
        "stage": picture.stage,
        "origin_iata": picture.origin.iata,
        "origin_icao": picture.origin.icao,
        "destination_iata": picture.destination.iata,
        "destination_icao": picture.destination.icao,
        "previous_destination_iata": None if picture.previous is None else picture.previous.iata,
        "alternate_iata": picture.alternate.iata,
        "alternate_icao": picture.alternate.icao,
        "distance_nm": money(picture.distance_nm),
        "course_deg": picture.course_deg,
        "cruise_tas_kt": picture.aircraft.cruise_tas_kt,
        "wind_dir_deg": picture.wind_dir_deg,
        "wind_speed_kt": picture.wind_speed_kt,
        "wind_component_kt": picture.wind_component_kt,
        "ground_speed_kt": picture.computation_ground_speed,
        "burn_kg_per_hour": picture.aircraft.burn_kg_per_hour,
        "trip_minutes": picture.computation_trip_minutes,
        "trip_fuel_kg": money(picture.computation_trip_fuel),
        "taxi_fuel_kg": money(picture.taxi_fuel_kg),
        "contingency_rate": "0.05",
        "contingency_fuel_kg": money(picture.computation_contingency),
        "alternate_distance_nm": money(alternate_distance),
        "alternate_fuel_kg": money(picture.computation_alternate_fuel),
        "reserve_minutes": picture.aircraft.reserve_minutes,
        "reserve_fuel_kg": money(picture.computation_reserve),
        "burn_so_far_kg": money(picture.burn_so_far_kg),
        "planned_fuel_kg": money(picture.computation_planned),
        "extra_fuel_kg": money(picture.extra_fuel_kg),
        "uplift_kg": money(picture.uplift_kg),
        "estimated_burn_kg": money(picture.estimated_burn_kg),
        "remaining_fuel_kg": money(picture.remaining_fuel_kg),
        "decision_fraction": picture.decision_fraction,
        "weather_report_id": picture.weather_report_id,
        "reroute_event_id": reroute_event_id,
    }


def _passengers(
    rng: random.Random,
    params: AirportParams,
    flights: list[Flight],
    flight_events: list[dict],
) -> list[dict]:
    departed = {
        event["flight_id"]
        for event in flight_events
        if event["event_type"] == "FlightDeparted"
    }
    records: list[dict] = []
    seq = 1
    passenger_number = 1_000_001
    for flight in sorted(flights, key=lambda item: item.flight_id):
        if flight.flight_id not in departed or flight.actual_departure is None or flight.departure_destination is None:
            continue
        for _ in range(params.pax_per_flight):
            passenger_id = f"pax_{passenger_number:07d}"
            passenger_number += 1
            name = f"{rng.choice(PASSENGER_NAMES_FIRST)} {rng.choice(PASSENGER_NAMES_LAST)}"
            seq = _passenger_story(rng, flight, passenger_id, name, records, seq)
    return _stamp(records, "pev")


def _passenger_story(
    rng: random.Random,
    flight: Flight,
    passenger_id: str,
    name: str,
    records: list[dict],
    seq: int,
) -> int:
    off = flight.actual_departure
    destination = flight.departure_destination
    if off is None or destination is None:
        raise RuntimeError(f"{flight.flight_id} is missing a departure")
    if rng.random() < 0.06:
        bags = 0
        seat = None
        if rng.random() < 0.5:
            checkin = _between(rng, off - timedelta(hours=12), off - timedelta(minutes=50))
            seat = _seat(rng)
            bags = rng.choice((0, 1, 1, 2))
            bag_at = checkin + timedelta(minutes=10)
            if not (bags and bag_at < off - timedelta(minutes=20)):
                bags = 0
            records.append(_passenger_record(seq, "PassengerCheckedIn", checkin, flight, passenger_id, name, destination, bags, seat))
            seq += 1
            if bags:
                records.append(_passenger_record(seq, "BagDropped", bag_at, flight, passenger_id, name, destination, bags, seat))
                seq += 1
        records.append(
            _passenger_record(
                seq,
                "PassengerNoShow",
                off - timedelta(minutes=15),
                flight,
                passenger_id,
                name,
                destination,
                bags,
                seat,
            )
        )
        return seq + 1
    if flight.scenario == "reroute_pre":
        earliest = flight.scheduled_departure - timedelta(hours=3)
    else:
        earliest = off - timedelta(hours=18)
    latest = off - timedelta(minutes=40)
    if earliest >= latest:
        earliest = off - timedelta(minutes=120)
    checkin = _between(rng, earliest, latest)
    board = _between(rng, off - timedelta(minutes=35), off - timedelta(minutes=8))
    if board <= checkin:
        board = checkin + timedelta(minutes=5)
    if board >= off or checkin >= board:
        checkin = off - timedelta(minutes=70)
        board = off - timedelta(minutes=20)
    bags = rng.choice((0, 0, 1, 1, 2))
    seat = _seat(rng)
    bag_at = checkin + timedelta(minutes=10)
    if not (checkin < bag_at < board):
        bags = 0
    records.append(_passenger_record(seq, "PassengerCheckedIn", checkin, flight, passenger_id, name, destination, bags, seat))
    seq += 1
    if bags:
        records.append(_passenger_record(seq, "BagDropped", bag_at, flight, passenger_id, name, destination, bags, seat))
        seq += 1
    records.append(_passenger_record(seq, "PassengerBoarded", board, flight, passenger_id, name, destination, bags, seat))
    return seq + 1


def _between(rng: random.Random, start: datetime, end: datetime) -> datetime:
    minutes = int((end - start).total_seconds() // 60)
    if minutes < 0:
        return start
    return (start + timedelta(minutes=rng.randint(0, minutes))).replace(second=0, microsecond=0)


def _seat(rng: random.Random) -> str:
    return f"{rng.randint(1, 32)}{rng.choice('ABCDEF')}"


def _passenger_record(
    seq: int,
    event_type: str,
    occurred_at: datetime,
    flight: Flight,
    passenger_id: str,
    name: str,
    destination: Airport,
    bags: int,
    seat: str | None,
) -> dict:
    return {
        "_seq": seq,
        "schema_version": SCHEMA_VERSION,
        "event_id": "",
        "event_type": event_type,
        "occurred_at": format_ts(occurred_at),
        "passenger_id": passenger_id,
        "passenger_name": name,
        "flight_id": flight.flight_id,
        "airline_iata": flight.airline_iata,
        "origin_iata": flight.origin.iata,
        "destination_iata": destination.iata,
        "bags": bags,
        "seat": seat,
    }


def _airport_rows(airports: tuple[Airport, ...] | None = None) -> list[dict]:
    airports = AIRPORTS if airports is None else airports
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "iata": airport.iata,
            "icao": airport.icao,
            "name": airport.name,
            "city": airport.city,
            "latitude": airport.latitude,
            "longitude": airport.longitude,
            "elevation_ft": airport.elevation_ft,
        }
        for airport in airports
    ]


def _airline_rows() -> list[dict]:
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "iata": airline.iata,
            "icao": airline.icao,
            "name": airline.name,
            "hub_iata": airline.hub_iata,
        }
        for airline in AIRLINES
    ]


def _aircraft_type_rows() -> list[dict]:
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "icao": aircraft.icao,
            "name": aircraft.name,
            "seats": aircraft.seats,
            "cruise_tas_kt": aircraft.cruise_tas_kt,
            "burn_kg_per_hour": aircraft.burn_kg_per_hour,
            "taxi_fuel_kg": aircraft.taxi_fuel_kg,
            "reserve_minutes": aircraft.reserve_minutes,
        }
        for aircraft in AIRCRAFT_BY_ICAO.values()
    ]


def _aircraft_rows(tails: list[Tail]) -> list[dict]:
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "tail_number": tail.tail_number,
            "aircraft_type": tail.aircraft_type,
            "airline_iata": tail.airline_iata,
            "seats": tail.seats,
        }
        for tail in tails
    ]


PART_NAME = "part-{number:05d}.ndjson"
REFERENCE_STREAMS = ("airports", "airlines", "aircraft_types", "aircraft")
EVENT_STREAMS = ("weather_reports", "flight_events", "fuel_records", "passenger_events")
STREAM_ORDER = REFERENCE_STREAMS + EVENT_STREAMS

_PARTITION_NOTES = [
    "Fictional passengers and tail numbers. The original 22 airport and airline codes are public identifiers.",
    "Airports beyond those 22 are synthetic fields placed near US cities. Their codes are not real IATA codes.",
    "Passenger rows are a cabin sample, not a full manifest.",
    "Fuel figures follow pipelines/datasets/fuel.py and are rounded half-up to 0.01 kg.",
    "A day shard is a pure function of the seed, the day index, and the full day count. Keep --days at the preset horizon on every shard.",
    "Event ids reserve a fixed numeric block per day, so shards do not collide and do not need to see each other.",
    "Tail numbers name a fixed fleet that every shard writes the same way. A tail is not parked overnight where the previous shard left it.",
    "Flight numbers restart each day. flight_id includes the scheduled date, so the flight is still unique.",
    "Weather is one report per airport-hour. A shard writes the hours whose observed date is that day. The last day also writes the next morning.",
    "Event files are uncompressed NDJSON under dt=YYYY-MM-DD, rolled at part_bytes. Reference tables have no date partition.",
]


def write_partitioned_airports(
    params: AirportParams,
    directory: Path,
    *,
    day_index: int | None = None,
    reference_only: bool = False,
    skip_reference: bool = False,
    part_bytes: int = PART_BYTES,
) -> dict:
    """Write hive-style day partitions.

    ``day_index`` None writes every day in the horizon. Pass one index to
    write that day alone. The bytes under ``dt=`` match either way.

    ``params.days`` is the full horizon, not the number of days this call
    writes. The last day of the horizon owns the next morning's weather.
    """
    if params.flights_per_day < _MIN_FLIGHTS_PER_SHARD:
        raise ValueError(
            f"partitioned generation needs at least {_MIN_FLIGHTS_PER_SHARD} flights per day"
        )
    if params.days < 1:
        raise ValueError("days must be positive")
    if params.pax_per_flight < 0:
        raise ValueError("pax_per_flight cannot be negative")
    if part_bytes < 1:
        raise ValueError("part_bytes must be positive")
    if reference_only and skip_reference:
        raise ValueError("reference-only and skip-reference cannot be combined")
    if day_index is not None and (day_index < 0 or day_index >= params.days):
        raise ValueError(f"day_index {day_index} is outside 0..{params.days - 1}")
    directory.mkdir(parents=True, exist_ok=True)
    airports = _network_for(params)
    routes = _routes(airports)
    templates = _fleet_templates(params)
    counts = {name: 0 for name in STREAM_ORDER}
    byte_counts = {name: 0 for name in STREAM_ORDER}
    files: dict[str, list[str]] = {name: [] for name in STREAM_ORDER}
    scenarios = {name: 0 for name in ("normal", "delay", "cancel", "reroute_pre", "divert")}
    tail_waits = 0
    tail_repositions = 0
    if not skip_reference:
        _write_reference(directory, airports, templates, part_bytes, counts, byte_counts, files)
    dates: list[str] = []
    if not reference_only:
        indexes = range(params.days) if day_index is None else (day_index,)
        for index in indexes:
            dates.append(_shard_date(params, index))
            waits, moves = _write_one_day(
                params,
                directory,
                index,
                airports,
                routes,
                templates,
                part_bytes,
                counts,
                byte_counts,
                files,
                scenarios,
            )
            tail_waits += waits
            tail_repositions += moves
    manifest = _partition_manifest(
        params,
        day_index=day_index,
        dates=dates,
        part_bytes=part_bytes,
        reference_only=reference_only,
        counts=counts,
        byte_counts=byte_counts,
        files=files,
        scenarios=scenarios,
        tail_waits=tail_waits,
        tail_repositions=tail_repositions,
        airport_total=len(airports),
    )
    text = json.dumps(manifest, indent=2, ensure_ascii=True) + "\n"
    (directory / "manifest.json").write_text(text, encoding="utf-8", newline="\n")
    return manifest


def _network_for(params: AirportParams) -> tuple[Airport, ...]:
    if params.airport_count == len(AIRPORTS):
        return AIRPORTS
    return mega_network(params.airport_count)


def _shard_date(params: AirportParams, day_index: int) -> str:
    start = parse_ts(params.start)
    return (start + timedelta(days=day_index)).strftime("%Y-%m-%d")


def _seed_rng(seed: int, *parts: object) -> random.Random:
    text = ":".join((str(seed), *(str(part) for part in parts)))
    digest = hashlib.sha256(text.encode("ascii")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _fleet_templates(params: AirportParams) -> list[Tail]:
    """A fleet big enough for one busy day, identical for every shard.

    Each airline gets up to 2,200 tails of each type it flies, and never
    more than 8,999 tails total (the tail number is four digits).
    """
    per_type = min(2200, max(params.flights_per_day, _MIN_FLIGHTS_PER_SHARD))
    tails: list[Tail] = []
    for airline in AIRLINES:
        number = 1
        for icao in airline.fleet:
            aircraft = AIRCRAFT_BY_ICAO[icao]
            for _ in range(per_type):
                if number > 8999:
                    raise RuntimeError(f"{airline.iata} fleet does not fit in 4-digit tail numbers")
                tails.append(
                    Tail(
                        tail_number=f"T-{airline.iata}{number:04d}",
                        airline_iata=airline.iata,
                        aircraft_type=aircraft.icao,
                        seats=aircraft.seats,
                        location="",
                        next_free=parse_ts(params.start),
                    )
                )
                number += 1
    tails.sort(key=lambda tail: tail.tail_number)
    return tails


class _FleetPool:
    """One operating day's view of the fixed fleet. Tails start unplaced."""

    def __init__(self, templates: list[Tail], day_start: datetime) -> None:
        self.unplaced: dict[tuple[str, str], deque[Tail]] = defaultdict(deque)
        self.at: dict[tuple[str, str, str], list[Tail]] = defaultdict(list)
        for template in templates:
            tail = Tail(
                tail_number=template.tail_number,
                airline_iata=template.airline_iata,
                aircraft_type=template.aircraft_type,
                seats=template.seats,
                location="",
                next_free=day_start,
            )
            self.unplaced[(tail.airline_iata, tail.aircraft_type)].append(tail)

    def take(self, flight: Flight, desired: datetime) -> Tail:
        key = (flight.airline_iata, flight.aircraft_type)
        origin = flight.origin.iata
        at_origin = self.at[(key[0], key[1], origin)]
        ready = [tail for tail in at_origin if tail.next_free <= desired]
        if ready:
            return min(ready, key=lambda tail: (tail.next_free, tail.tail_number))
        pool = self.unplaced[key]
        if pool:
            return pool.popleft()
        if at_origin:
            return min(at_origin, key=lambda tail: (tail.next_free, tail.tail_number))
        candidates = [
            tail
            for located in self.at.values()
            if located
            for tail in located
            if tail.airline_iata == flight.airline_iata and tail.aircraft_type == flight.aircraft_type
        ]
        if not candidates:
            raise RuntimeError(
                f"no {flight.aircraft_type} tail for {flight.airline_iata} on {flight.flight_id}"
            )
        return min(candidates, key=lambda tail: (tail.next_free, tail.tail_number))

    def note(self, tail: Tail, flight: Flight) -> None:
        """Park an unplaced tail at the departure airport without moving the clock."""
        if tail.location != "":
            return
        tail.location = flight.origin.iata
        self.at[(tail.airline_iata, tail.aircraft_type, tail.location)].append(tail)

    def park(self, tail: Tail, flight: Flight) -> None:
        if flight.arrived_at is None or flight.arrival_destination is None:
            raise RuntimeError(f"{flight.flight_id} did not arrive")
        old = tail.location
        bucket = self.at[(tail.airline_iata, tail.aircraft_type, old)]
        _drop_tail(bucket, tail)
        tail.location = flight.arrival_destination.iata
        tail.next_free = flight.arrived_at + timedelta(minutes=TURNAROUND_MINUTES)
        tail.legs.append(flight)
        self.at[(tail.airline_iata, tail.aircraft_type, tail.location)].append(tail)


def _drop_tail(bucket: list[Tail], tail: Tail) -> None:
    for index, item in enumerate(bucket):
        if item is tail:
            bucket.pop(index)
            return


def _assign_fixed_fleet(
    flights: list[Flight],
    templates: list[Tail],
    day_start: datetime,
    weather_index: dict[tuple[str, str], dict],
    airports: tuple[Airport, ...],
) -> tuple[int, int]:
    pool = _FleetPool(templates, day_start)
    ordered = sorted(
        flights,
        key=lambda flight: (
            flight.scheduled_departure + timedelta(minutes=flight.delay_minutes),
            flight.flight_id,
        ),
    )
    waits = 0
    repositions = 0
    for flight in ordered:
        desired = flight.scheduled_departure + timedelta(minutes=flight.delay_minutes)
        tail = pool.take(flight, desired)
        if tail.location not in {"", flight.origin.iata}:
            repositions += 1
        pool.note(tail, flight)
        if flight.scenario != "cancel" and tail.next_free > desired:
            slip = int((tail.next_free - flight.scheduled_departure).total_seconds() // 60)
            if slip > 2000:
                raise RuntimeError(
                    f"{flight.flight_id} would slip {slip} minutes for {tail.tail_number}"
                )
            if flight.delay_minutes < slip:
                flight.delay_minutes = slip
                waits += 1
                if flight.delay_minutes >= 15 and flight.delay_reason is None:
                    flight.delay_reason = "late_inbound"
        flight.tail_number = tail.tail_number
        _realize(flight, weather_index, airports)
        if flight.scenario == "cancel":
            continue
        pool.park(tail, flight)
    return waits, repositions


def _weather_bounds(start: datetime, day_index: int, days: int) -> tuple[datetime, datetime]:
    begin = start + timedelta(days=max(day_index - 1, 0))
    if day_index + 1 >= days:
        end = start + timedelta(days=day_index, hours=48)
    else:
        end = start + timedelta(days=day_index + 2, hours=18)
    return begin, end


def _walk_reports(
    seed: int,
    start: datetime,
    airports: tuple[Airport, ...],
    begin: datetime,
    end: datetime,
) -> dict[tuple[str, str], dict]:
    """Random-walk weather from the dataset start, keeping only ``[begin, end)``.

    Each airport has its own stream, so a shard can fast-forward without
    replaying another airport. Reports are the same objects ``_script`` patches.
    """
    total_hours = int((end - start).total_seconds() // 3600)
    if total_hours < 1:
        raise RuntimeError("weather window is empty")
    index: dict[tuple[str, str], dict] = {}
    for airport in airports:
        rng = _seed_rng(seed, "weather", airport.iata)
        category = "VFR"
        wind_dir = rng.randrange(0, 360)
        for hour in range(total_hours):
            roll = rng.random()
            if roll > 0.75:
                category = rng.choices(
                    ["VFR", "MVFR", "IFR", "LIFR", "CLOSED"],
                    weights=(88, 7, 3, 1.5, 0.5),
                    k=1,
                )[0]
            wind_dir = (wind_dir + rng.randint(-12, 12)) % 360
            wind_speed = rng.randint(4, 24)
            observed = start + timedelta(hours=hour)
            report = _weather_report(airport, observed, category, wind_dir, wind_speed, rng)
            if begin <= observed < end:
                index[(airport.iata, report["observed_at"])] = report
    return index


def _owned_weather(
    index: dict[tuple[str, str], dict],
    start: datetime,
    day_index: int,
    days: int,
) -> list[dict]:
    day_start = start + timedelta(days=day_index)
    next_day = day_start + timedelta(days=1)
    spill_end = day_start + timedelta(hours=48)
    last = day_index == days - 1
    owned: list[dict] = []
    for report in index.values():
        observed = parse_ts(report["observed_at"])
        if day_start <= observed < next_day or (last and next_day <= observed < spill_end):
            owned.append(report)
    owned.sort(key=lambda record: (record["observed_at"], record["airport_iata"]))
    return owned


def _prepare_flights(
    params: AirportParams,
    day_index: int,
    airports: tuple[Airport, ...],
    routes: dict[str, list[tuple[Airport, Decimal, int]]],
    weather_index: dict[tuple[str, str], dict],
) -> list[Flight]:
    start = parse_ts(params.start)
    for neighbor in (day_index - 1, day_index + 1):
        if neighbor < 0 or neighbor >= params.days:
            continue
        drafted = _draft_flights(
            _seed_rng(params.seed, "flights", neighbor),
            params,
            start,
            routes,
            airports,
            neighbor,
        )
        _check_flight_numbers(drafted)
        _script(drafted, weather_index)
    flights = _draft_flights(
        _seed_rng(params.seed, "flights", day_index),
        params,
        start,
        routes,
        airports,
        day_index,
    )
    _check_flight_numbers(flights)
    _script_and_assign(
        flights,
        weather_index,
        _seed_rng(params.seed, "ops", day_index),
        airports,
    )
    return flights


def _check_flight_numbers(flights: list[Flight]) -> None:
    for flight in flights:
        number = flight.flight_number[2:]
        if len(number) != 4 or not number.isdigit() or int(number) > 9999:
            raise RuntimeError(f"{flight.flight_number} does not fit the 4-digit flight number")


def _write_one_day(
    params: AirportParams,
    directory: Path,
    day_index: int,
    airports: tuple[Airport, ...],
    routes: dict[str, list[tuple[Airport, Decimal, int]]],
    templates: list[Tail],
    part_bytes: int,
    counts: dict[str, int],
    byte_counts: dict[str, int],
    files: dict[str, list[str]],
    scenarios: dict[str, int],
) -> tuple[int, int]:
    start = parse_ts(params.start)
    begin, end = _weather_bounds(start, day_index, params.days)
    weather_index = _walk_reports(params.seed, start, airports, begin, end)
    flights = _prepare_flights(params, day_index, airports, routes, weather_index)
    waits, repositions = _assign_fixed_fleet(
        flights, templates, start + timedelta(days=day_index), weather_index, airports
    )
    for flight in flights:
        scenarios[flight.scenario] = scenarios.get(flight.scenario, 0) + 1
    date = _shard_date(params, day_index)
    weather_rows = _owned_weather(weather_index, start, day_index, params.days)
    _write_rows(directory, "weather_reports", date, weather_rows, part_bytes, counts, byte_counts, files)
    flight_events = _flight_events(
        flights,
        weather_index,
        first=day_index * FLIGHT_EVENT_BLOCK + 1,
    )
    if len(flight_events) > FLIGHT_EVENT_BLOCK:
        raise RuntimeError(
            f"day {day_index} produced {len(flight_events)} flight events, "
            f"above the block of {FLIGHT_EVENT_BLOCK}"
        )
    fuel_records = _fuel_records(
        flights,
        flight_events,
        first=day_index * FUEL_EVENT_BLOCK + 1,
    )
    if len(fuel_records) > FUEL_EVENT_BLOCK:
        raise RuntimeError(
            f"day {day_index} produced {len(fuel_records)} fuel records, "
            f"above the block of {FUEL_EVENT_BLOCK}"
        )
    _write_rows(directory, "flight_events", date, flight_events, part_bytes, counts, byte_counts, files)
    _write_rows(directory, "fuel_records", date, fuel_records, part_bytes, counts, byte_counts, files)
    departed = {
        event["flight_id"]
        for event in flight_events
        if event["event_type"] == "FlightDeparted"
    }
    _stream_passengers(
        params,
        directory,
        day_index,
        date,
        flights,
        departed,
        part_bytes,
        counts,
        byte_counts,
        files,
    )
    return waits, repositions


def _stream_passengers(
    params: AirportParams,
    directory: Path,
    day_index: int,
    date: str,
    flights: list[Flight],
    departed: set[str],
    part_bytes: int,
    counts: dict[str, int],
    byte_counts: dict[str, int],
    files: dict[str, list[str]],
) -> None:
    rng = _seed_rng(params.seed, "passengers", day_index)
    base = day_index * PASSENGER_ID_BLOCK
    passenger_number = 0
    seq = 1
    with tempfile.TemporaryDirectory(prefix="airport-pax-") as temp_name:
        spill = _HourSpill(Path(temp_name))
        for flight in sorted(flights, key=lambda item: item.flight_id):
            if flight.flight_id not in departed or flight.actual_departure is None:
                continue
            if flight.departure_destination is None:
                continue
            for _ in range(params.pax_per_flight):
                passenger_number += 1
                if passenger_number > PASSENGER_ID_BLOCK:
                    raise RuntimeError(
                        f"day {day_index} has more than {PASSENGER_ID_BLOCK} passengers"
                    )
                passenger_id = f"pax_{base + passenger_number:08d}"
                name = f"{rng.choice(PASSENGER_NAMES_FIRST)} {rng.choice(PASSENGER_NAMES_LAST)}"
                bucket: list[dict] = []
                seq = _passenger_story(rng, flight, passenger_id, name, bucket, seq)
                for record in bucket:
                    spill.add(record)
        event_count = spill.count
        if event_count > PASSENGER_EVENT_BLOCK:
            raise RuntimeError(
                f"day {day_index} produced {event_count} passenger events, "
                f"above the block of {PASSENGER_EVENT_BLOCK}"
            )
        first = day_index * PASSENGER_EVENT_BLOCK + 1

        def _numbered():
            number = first
            for record in spill.iter_sorted():
                if number >= 10**9:
                    raise RuntimeError(f"passenger event id {number} does not fit in 9 digits")
                record["event_id"] = f"pev_{number:09d}"
                del record["_seq"]
                number += 1
                yield record

        _write_rows(
            directory,
            "passenger_events",
            date,
            _numbered(),
            part_bytes,
            counts,
            byte_counts,
            files,
        )


class _HourSpill:
    """Append records to one file per hour, then read those hours in order."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.count = 0
        self._handles: dict[str, object] = {}

    def add(self, record: dict) -> None:
        hour = record["occurred_at"][:13].replace("-", "").replace(":", "")
        handle = self._handles.get(hour)
        if handle is None:
            handle = (self.root / f"{hour}.ndjson").open("w", encoding="utf-8", newline="\n")
            self._handles[hour] = handle
        handle.write(dumps_record(record))
        handle.write("\n")
        self.count += 1

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()

    def iter_sorted(self):
        self.close()
        for path in sorted(self.root.glob("*.ndjson")):
            rows: list[dict] = []
            with path.open(encoding="utf-8", newline="\n") as handle:
                for line in handle:
                    stripped = line.strip()
                    if stripped:
                        rows.append(json.loads(stripped))
            rows.sort(key=lambda record: (record["occurred_at"], record["_seq"]))
            yield from rows
            path.unlink()


class _RollingNdjson:
    def __init__(self, directory: Path, part_bytes: int) -> None:
        self.directory = directory
        self.part_bytes = part_bytes
        self.part = 0
        self.size = 0
        self.rows = 0
        self.written = 0
        self.files: list[Path] = []
        self._handle = None

    def write_record(self, record: dict) -> None:
        line = dumps_record(record) + "\n"
        encoded = len(line.encode("utf-8"))
        if self._handle is None or self.size >= self.part_bytes:
            self._open_next()
        self._handle.write(line)
        self.size += encoded
        self.written += encoded
        self.rows += 1

    def _open_next(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self.part += 1
            self.size = 0
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / PART_NAME.format(number=self.part)
        self._handle = path.open("w", encoding="utf-8", newline="\n")
        self.files.append(path)

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _write_reference(
    directory: Path,
    airports: tuple[Airport, ...],
    templates: list[Tail],
    part_bytes: int,
    counts: dict[str, int],
    byte_counts: dict[str, int],
    files: dict[str, list[str]],
) -> None:
    rows = {
        "airports": _airport_rows(airports),
        "airlines": _airline_rows(),
        "aircraft_types": _aircraft_type_rows(),
        "aircraft": _aircraft_rows(templates),
    }
    for name in REFERENCE_STREAMS:
        _write_rows(directory, name, None, rows[name], part_bytes, counts, byte_counts, files)


def _write_rows(
    directory: Path,
    stream: str,
    date: str | None,
    records,
    part_bytes: int,
    counts: dict[str, int],
    byte_counts: dict[str, int],
    files: dict[str, list[str]],
) -> None:
    folder = directory / stream if date is None else directory / stream / f"dt={date}"
    if folder.exists():
        _remove_tree(folder)
    writer = _RollingNdjson(folder, part_bytes)
    for record in records:
        writer.write_record(record)
    writer.close()
    if writer.rows == 0:
        _remove_tree(folder)
        parent = directory / stream
        if date is not None and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
        return
    counts[stream] += writer.rows
    byte_counts[stream] += writer.written
    files[stream].extend(_posix(directory, path) for path in writer.files)


def _remove_tree(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        for child in path.iterdir():
            _remove_tree(child)
        path.rmdir()
    else:
        path.unlink()


def _posix(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _partition_manifest(
    params: AirportParams,
    *,
    day_index: int | None,
    dates: list[str],
    part_bytes: int,
    reference_only: bool,
    counts: dict[str, int],
    byte_counts: dict[str, int],
    files: dict[str, list[str]],
    scenarios: dict[str, int],
    tail_waits: int,
    tail_repositions: int,
    airport_total: int,
) -> dict:
    written_counts = {name: counts[name] for name in STREAM_ORDER if counts[name] or files[name]}
    written_bytes = {name: byte_counts[name] for name in STREAM_ORDER if name in written_counts}
    written_files = {name: files[name] for name in STREAM_ORDER if name in written_counts}
    return {
        "dataset": "airports",
        "synthetic": True,
        "layout": "hive-dt",
        "seed": params.seed,
        "scale": params.scale,
        "start": params.start,
        "days": params.days,
        "day_index": day_index,
        "date": dates[0] if day_index is not None and dates else None,
        "dates": dates,
        "reference_only": reference_only,
        "flights_per_day": params.flights_per_day,
        "pax_per_flight": params.pax_per_flight,
        "airports": airport_total,
        "part_bytes": part_bytes,
        "tail_waits": tail_waits,
        "tail_repositions": tail_repositions,
        "notes": list(_PARTITION_NOTES),
        "counts": written_counts,
        "bytes": written_bytes,
        "files": written_files,
        "scenarios": scenarios,
    }

