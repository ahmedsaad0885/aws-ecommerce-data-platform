"""Public reference facts used by the airport simulation.

Airport coordinates and elevations are rounded public figures so distances
are teachable. They are not a survey. Aircraft cruise speeds and fuel flows
are ballpark planning rates for this simulation, not approved flight-manual
data. Tail numbers are synthetic (``T-`` plus the airline code). They are
not FAA registrations.

Airline and airport identifiers are the public IATA and ICAO codes.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Airport:
    iata: str
    icao: str
    name: str
    city: str
    latitude: str
    longitude: str
    elevation_ft: int
    departure_weight: int

    @property
    def lat(self) -> float:
        return float(self.latitude)

    @property
    def lon(self) -> float:
        return float(self.longitude)


@dataclass(frozen=True)
class Airline:
    iata: str
    icao: str
    name: str
    hub_iata: str
    hubs: tuple[str, ...]
    fleet: tuple[str, ...]


@dataclass(frozen=True)
class AircraftType:
    icao: str
    name: str
    seats: int
    cruise_tas_kt: int
    burn_kg_per_hour: str
    taxi_fuel_kg: str
    reserve_minutes: int
    max_stage_nm: int

    @property
    def burn(self) -> Decimal:
        return Decimal(self.burn_kg_per_hour)

    @property
    def taxi(self) -> Decimal:
        return Decimal(self.taxi_fuel_kg)


AIRPORTS: tuple[Airport, ...] = (
    Airport("ATL", "KATL", "Hartsfield-Jackson Atlanta International", "Atlanta", "33.6407", "-84.4277", 1026, 18),
    Airport("ORD", "KORD", "Chicago O'Hare International", "Chicago", "41.9742", "-87.9073", 680, 16),
    Airport("DFW", "KDFW", "Dallas Fort Worth International", "Dallas", "32.8998", "-97.0403", 607, 16),
    Airport("DEN", "KDEN", "Denver International", "Denver", "39.8561", "-104.6737", 5434, 14),
    Airport("LAX", "KLAX", "Los Angeles International", "Los Angeles", "33.9416", "-118.4085", 128, 15),
    Airport("SEA", "KSEA", "Seattle-Tacoma International", "Seattle", "47.4502", "-122.3088", 433, 9),
    Airport("SFO", "KSFO", "San Francisco International", "San Francisco", "37.6213", "-122.3790", 13, 10),
    Airport("LAS", "KLAS", "Harry Reid International", "Las Vegas", "36.0801", "-115.1523", 2181, 10),
    Airport("PHX", "KPHX", "Phoenix Sky Harbor International", "Phoenix", "33.4373", "-112.0078", 1135, 9),
    Airport("MSP", "KMSP", "Minneapolis-Saint Paul International", "Minneapolis", "44.8848", "-93.2223", 841, 8),
    Airport("DTW", "KDTW", "Detroit Metropolitan Wayne County", "Detroit", "42.2162", "-83.3554", 645, 8),
    Airport("BOS", "KBOS", "Boston Logan International", "Boston", "42.3656", "-71.0096", 19, 8),
    Airport("EWR", "KEWR", "Newark Liberty International", "Newark", "40.6895", "-74.1745", 18, 9),
    Airport("MIA", "KMIA", "Miami International", "Miami", "25.7959", "-80.2870", 8, 10),
    Airport("CLT", "KCLT", "Charlotte Douglas International", "Charlotte", "35.2140", "-80.9431", 748, 11),
    Airport("SLC", "KSLC", "Salt Lake City International", "Salt Lake City", "40.7899", "-111.9791", 4227, 7),
    Airport("SAN", "KSAN", "San Diego International", "San Diego", "32.7338", "-117.1933", 17, 6),
    Airport("PDX", "KPDX", "Portland International", "Portland", "45.5898", "-122.5951", 31, 6),
    Airport("AUS", "KAUS", "Austin-Bergstrom International", "Austin", "30.1945", "-97.6699", 542, 6),
    Airport("BNA", "KBNA", "Nashville International", "Nashville", "36.1263", "-86.6774", 599, 7),
    Airport("MCI", "KMCI", "Kansas City International", "Kansas City", "39.2976", "-94.7139", 1026, 5),
    Airport("STL", "KSTL", "St. Louis Lambert International", "St. Louis", "38.7487", "-90.3700", 618, 6),
)

AIRLINES: tuple[Airline, ...] = (
    Airline("DL", "DAL", "Delta Air Lines", "ATL", ("ATL", "MSP", "DTW", "SLC", "SEA", "LAX"), ("B738", "A321", "A320", "B739")),
    Airline("AA", "AAL", "American Airlines", "DFW", ("DFW", "CLT", "ORD", "MIA", "PHX"), ("A321", "B738", "A320")),
    Airline("UA", "UAL", "United Airlines", "ORD", ("ORD", "DEN", "EWR", "SFO", "LAX"), ("B738", "B739", "A320")),
    Airline("WN", "SWA", "Southwest Airlines", "DEN", ("DEN", "LAS", "BNA", "STL", "PHX", "AUS", "SAN"), ("B738", "B38M")),
    Airline("AS", "ASA", "Alaska Airlines", "SEA", ("SEA", "PDX", "SFO", "LAX", "SAN"), ("B738", "B739", "E75L")),
    Airline("B6", "JBU", "JetBlue Airways", "BOS", ("BOS", "EWR", "MIA", "LAX"), ("A320", "A321", "A20N")),
    Airline("NK", "NKS", "Spirit Airlines", "ATL", ("ATL", "DFW", "LAS", "MIA", "ORD"), ("A320", "A20N")),
    Airline("F9", "FFT", "Frontier Airlines", "DEN", ("DEN", "LAS", "MIA", "ORD", "PHX"), ("A20N", "A321")),
)

AIRCRAFT_TYPES: tuple[AircraftType, ...] = (
    AircraftType("A320", "Airbus A320", 180, 447, "2500.00", "200.00", 45, 2600),
    AircraftType("A20N", "Airbus A320neo", 186, 447, "2100.00", "180.00", 45, 2600),
    AircraftType("A321", "Airbus A321", 196, 447, "2700.00", "230.00", 45, 2600),
    AircraftType("B738", "Boeing 737-800", 175, 453, "2600.00", "220.00", 45, 2600),
    AircraftType("B739", "Boeing 737-900", 178, 453, "2750.00", "230.00", 45, 2600),
    AircraftType("B38M", "Boeing 737 MAX 8", 178, 453, "2200.00", "190.00", 45, 2600),
    AircraftType("E75L", "Embraer 175", 76, 430, "1100.00", "120.00", 45, 900),
)

TAXI_OUT_MINUTES = 12
TAXI_IN_MINUTES = 8
TURNAROUND_MINUTES = 45

AIRPORT_BY_IATA = {airport.iata: airport for airport in AIRPORTS}
AIRLINE_BY_IATA = {airline.iata: airline for airline in AIRLINES}
AIRCRAFT_BY_ICAO = {aircraft.icao: aircraft for aircraft in AIRCRAFT_TYPES}
