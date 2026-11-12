"""The fuel formula is fixed arithmetic, not a hidden constant."""

from decimal import Decimal

from pipelines.datasets.fuel import (
    burn_so_far,
    compute_fuel,
    great_circle_nm,
    ground_speed_kt,
    headwind_kt,
    initial_course_deg,
    intermediate_point,
)
from pipelines.datasets.reference import AIRPORT_BY_IATA


def test_known_inputs_match_the_worked_example():
    plan = compute_fuel(
        distance_nm=Decimal("606.00"),
        cruise_tas_kt=453,
        headwind_kt=20,
        burn_kg_per_hour=Decimal("2600.00"),
        taxi_fuel_kg=Decimal("220.00"),
        alternate_distance_nm=Decimal("18.00"),
        reserve_minutes=45,
        burn_so_far_kg=Decimal("0.00"),
    )
    assert plan.ground_speed_kt == 433
    assert plan.trip_minutes == 84
    assert plan.trip_fuel_kg == Decimal("3638.80")
    assert plan.contingency_fuel_kg == Decimal("181.94")
    assert plan.alternate_fuel_kg == Decimal("103.31")
    assert plan.reserve_fuel_kg == Decimal("1950.00")
    assert plan.planned_fuel_kg == Decimal("6094.05")
    assert plan.estimated_burn_kg == Decimal("3858.80")
    uplift = plan.planned_fuel_kg + Decimal("150.00")
    assert uplift - plan.estimated_burn_kg == Decimal("2385.25")


def test_components_sum_to_planned_fuel():
    plan = compute_fuel(
        distance_nm=Decimal("420.50"),
        cruise_tas_kt=447,
        headwind_kt=-15,
        burn_kg_per_hour=Decimal("2100.00"),
        taxi_fuel_kg=Decimal("180.00"),
        alternate_distance_nm=Decimal("40.00"),
        reserve_minutes=45,
        burn_so_far_kg=Decimal("500.00"),
    )
    assert plan.planned_fuel_kg == (
        Decimal("500.00")
        + plan.trip_fuel_kg
        + Decimal("180.00")
        + plan.contingency_fuel_kg
        + plan.alternate_fuel_kg
        + plan.reserve_fuel_kg
    )
    assert plan.estimated_burn_kg == Decimal("500.00") + plan.trip_fuel_kg + Decimal("180.00")
    assert plan.ground_speed_kt == ground_speed_kt(447, -15)


def test_headwind_sign_and_ground_speed_floor():
    assert headwind_kt(0, 20, 0) == 20
    assert headwind_kt(180, 20, 0) == -20
    assert headwind_kt(90, 20, 0) == 0
    assert ground_speed_kt(300, 200) == 220


def test_burn_so_far_includes_taxi_and_a_fraction_of_trip_fuel():
    assert burn_so_far(Decimal("220.00"), Decimal("1000.00"), Decimal("0.40")) == Decimal("620.00")


def test_great_circle_is_symmetric_and_atl_ord_is_in_range():
    atlanta = AIRPORT_BY_IATA["ATL"]
    chicago = AIRPORT_BY_IATA["ORD"]
    outbound = great_circle_nm(atlanta.lat, atlanta.lon, chicago.lat, chicago.lon)
    inbound = great_circle_nm(chicago.lat, chicago.lon, atlanta.lat, atlanta.lon)
    assert outbound == inbound
    assert Decimal("500") < outbound < Decimal("560")
    course = initial_course_deg(atlanta.lat, atlanta.lon, chicago.lat, chicago.lon)
    assert 0 <= course <= 359


def test_intermediate_point_endpoints():
    start = intermediate_point(33.0, -84.0, 42.0, -88.0, 0.0)
    end = intermediate_point(33.0, -84.0, 42.0, -88.0, 1.0)
    assert start == (33.0, -84.0)
    assert end == (42.0, -88.0)
