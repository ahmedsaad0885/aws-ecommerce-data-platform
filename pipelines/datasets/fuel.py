"""Teachable fuel arithmetic for the airport simulation.

This is a planning sketch, not a flight-manual model. Distances use a
spherical Earth of 3440.065 nautical miles. Each fuel component is rounded
half-up to 0.01 kg before it is added, so the stored components sum to the
stored total.

Definitions, with the sign convention used on the records:

- ``wind_component_kt`` is the headwind along the course. A tailwind is negative.
- Ground speed is cruise TAS minus that headwind, and never below
  ``MIN_GROUND_SPEED_KT``.
- Trip fuel burns at ``burn_kg_per_hour`` for ``distance_nm / ground_speed``.
- Alternate fuel burns the same hourly rate at cruise TAS, with no wind,
  for the alternate distance.
- Contingency is 5% of trip fuel.
- Reserve is ``reserve_minutes`` of the hourly burn (a final-reserve allowance).
- Planned fuel is burn already behind the aircraft, plus trip, taxi,
  contingency, alternate, and reserve.
- Estimated burn is burn already behind the aircraft, plus trip, plus taxi.
  Contingency, alternate, and reserve stay in the tanks in this sketch.
- Remaining fuel is uplift minus estimated burn.

Preflight uplift is planned fuel plus an extra uplift. A cancelled flight
keeps the planned components and stores uplift, estimated burn, and remaining
as zero because the aircraft was not fueled. An airborne revision keeps the
original uplift (no fuel is added in the air) and sets taxi to zero because
taxi was already counted in ``burn_so_far_kg``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from pipelines.datasets.common import quantize

EARTH_RADIUS_NM = 3440.065
MIN_GROUND_SPEED_KT = 220
DEFAULT_CONTINGENCY = Decimal("0.05")


def great_circle_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> Decimal:
    """Great-circle distance in nautical miles, rounded half-up to 0.01."""
    phi1, lambda1, phi2, lambda2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = phi2 - phi1
    dlon = lambda2 - lambda1
    haversine = (
        math.sin(dlat / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlon / 2) ** 2
    )
    central = 2 * math.asin(min(1.0, math.sqrt(haversine)))
    return quantize(Decimal(f"{EARTH_RADIUS_NM * central:.6f}"))


def initial_course_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Initial true course in degrees, rounded half-up to a whole degree."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    east = math.sin(dlon) * math.cos(phi2)
    north = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    degrees = (math.degrees(math.atan2(east, north)) + 360.0) % 360.0
    rounded = int(Decimal(f"{degrees:.6f}").quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return rounded % 360


def intermediate_point(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    fraction: float,
) -> tuple[float, float]:
    """Point ``fraction`` of the way from point 1 to point 2 on a great circle."""
    if fraction <= 0:
        return lat1, lon1
    if fraction >= 1:
        return lat2, lon2
    phi1, lambda1, phi2, lambda2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = phi2 - phi1
    dlon = lambda2 - lambda1
    haversine = (
        math.sin(dlat / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlon / 2) ** 2
    )
    central = 2 * math.asin(min(1.0, math.sqrt(haversine)))
    if central < 1e-9:
        return lat1, lon1
    share_start = math.sin((1 - fraction) * central) / math.sin(central)
    share_end = math.sin(fraction * central) / math.sin(central)
    x_coord = share_start * math.cos(phi1) * math.cos(lambda1) + share_end * math.cos(phi2) * math.cos(lambda2)
    y_coord = share_start * math.cos(phi1) * math.sin(lambda1) + share_end * math.cos(phi2) * math.sin(lambda2)
    z_coord = share_start * math.sin(phi1) + share_end * math.sin(phi2)
    latitude = math.degrees(math.atan2(z_coord, math.sqrt(x_coord**2 + y_coord**2)))
    longitude = math.degrees(math.atan2(y_coord, x_coord))
    return latitude, longitude


def headwind_kt(wind_from_deg: int, wind_speed_kt: int, course_deg: int) -> int:
    """Headwind component in knots. Meteorological wind is the direction it comes from.

    A wind from the same direction as the course is a headwind and returns a
    positive number. A tailwind returns a negative number.
    """
    delta = math.radians((wind_from_deg - course_deg) % 360)
    component = wind_speed_kt * math.cos(delta)
    return int(Decimal(f"{component:.6f}").quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def ground_speed_kt(cruise_tas_kt: int, wind_component_kt: int) -> int:
    speed = cruise_tas_kt - wind_component_kt
    if speed < MIN_GROUND_SPEED_KT:
        return MIN_GROUND_SPEED_KT
    return speed


@dataclass(frozen=True)
class FuelComputation:
    ground_speed_kt: int
    trip_minutes: int
    trip_fuel_kg: Decimal
    contingency_fuel_kg: Decimal
    alternate_fuel_kg: Decimal
    reserve_fuel_kg: Decimal
    planned_fuel_kg: Decimal
    estimated_burn_kg: Decimal


def compute_fuel(
    *,
    distance_nm: Decimal,
    cruise_tas_kt: int,
    headwind_kt: int,
    burn_kg_per_hour: Decimal,
    taxi_fuel_kg: Decimal,
    alternate_distance_nm: Decimal,
    reserve_minutes: int,
    burn_so_far_kg: Decimal,
    contingency_rate: Decimal = DEFAULT_CONTINGENCY,
) -> FuelComputation:
    """Compute one fuel picture from already-rounded inputs.

    ``distance_nm`` is the distance still to fly, not necessarily the whole
    scheduled route. ``burn_so_far_kg`` is fuel already burned (zero on the
    ground). ``taxi_fuel_kg`` is zero on an airborne revision.
    """
    if cruise_tas_kt <= 0:
        raise ValueError("cruise TAS must be positive")
    if reserve_minutes < 0:
        raise ValueError("reserve minutes must be non-negative")
    speed = ground_speed_kt(cruise_tas_kt, headwind_kt)
    hours = distance_nm / Decimal(speed)
    trip_fuel = quantize(burn_kg_per_hour * hours)
    trip_minutes = int((hours * Decimal(60)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if trip_minutes < 1:
        trip_minutes = 1
    contingency = quantize(trip_fuel * contingency_rate)
    alternate = quantize(burn_kg_per_hour * alternate_distance_nm / Decimal(cruise_tas_kt))
    reserve = quantize(burn_kg_per_hour * Decimal(reserve_minutes) / Decimal(60))
    planned = quantize(
        burn_so_far_kg + trip_fuel + taxi_fuel_kg + contingency + alternate + reserve
    )
    estimated = quantize(burn_so_far_kg + trip_fuel + taxi_fuel_kg)
    return FuelComputation(
        ground_speed_kt=speed,
        trip_minutes=trip_minutes,
        trip_fuel_kg=trip_fuel,
        contingency_fuel_kg=contingency,
        alternate_fuel_kg=alternate,
        reserve_fuel_kg=reserve,
        planned_fuel_kg=planned,
        estimated_burn_kg=estimated,
    )


def burn_so_far(taxi_fuel_kg: Decimal, trip_fuel_kg: Decimal, fraction: Decimal) -> Decimal:
    """Fuel burned from brake release through ``fraction`` of planned trip fuel.

    Taxi is fully burned by the time the aircraft is airborne. ``fraction`` is
    the share of planned trip fuel, not a share of clock time. The simulation
    places the decision event at that same fraction of planned airborne
    minutes, rounded to the nearest minute.
    """
    return quantize(taxi_fuel_kg + trip_fuel_kg * fraction)
