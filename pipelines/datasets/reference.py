"""Public reference facts used by the airport simulation.

Airport coordinates and elevations are rounded public figures so distances
are teachable. They are not a survey. Aircraft cruise speeds and fuel flows
are ballpark planning rates for this simulation, not approved flight-manual
data. Tail numbers are synthetic (``T-`` plus the airline code). They are
not FAA registrations.

Airline and airport identifiers are the public IATA and ICAO codes.

The mega preset adds ``SYNTHETIC_AIRPORTS``: fictional fields with made-up
codes, sited near real US cities so stage lengths still look like a national
network. They are not those cities' real airports.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
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

# Hubs carry several times the departures of a small synthetic field.
# Weights apply only when the mega network is built. The classic 22 keep
# the weights on ``AIRPORTS`` so beefy and sample stay byte-identical.
MEGA_HUB_WEIGHTS: dict[str, int] = {
    "ATL": 36,
    "ORD": 32,
    "DFW": 30,
    "DEN": 24,
    "LAX": 24,
    "CLT": 14,
    "EWR": 12,
    "MIA": 12,
    "LAS": 12,
    "PHX": 12,
    "SFO": 12,
    "SEA": 12,
}

# (city, latitude, longitude, elevation_ft, departure_weight)
# Codes are assigned in order: ZAA, ZAB, ... so the table stays stable.
_SYNTHETIC_FIELDS: tuple[tuple[str, str, str, int, int], ...] = (
    ("Birmingham", "33.5629", "-86.7535", 650, 2),
    ("Mobile", "30.6912", "-88.2428", 26, 2),
    ("Huntsville", "34.6372", "-86.7751", 629, 2),
    ("Little Rock", "34.7294", "-92.2243", 262, 2),
    ("Fayetteville", "36.2819", "-94.3068", 1287, 2),
    ("Oklahoma City", "35.3931", "-97.6007", 1295, 2),
    ("Tulsa", "36.1984", "-95.8881", 677, 2),
    ("San Antonio", "29.5337", "-98.4698", 809, 6),
    ("Houston", "29.6454", "-95.2789", 46, 6),
    ("El Paso", "31.8072", "-106.3778", 3959, 2),
    ("Albuquerque", "35.0402", "-106.6092", 5355, 2),
    ("Tucson", "32.1161", "-110.9410", 2643, 2),
    ("Reno", "39.4991", "-119.7681", 4415, 2),
    ("Sacramento", "38.6954", "-121.5908", 27, 2),
    ("Oakland", "37.7213", "-122.2207", 9, 2),
    ("Santa Ana", "33.6757", "-117.8682", 56, 2),
    ("Ontario", "34.0560", "-117.6012", 944, 2),
    ("San Jose", "37.3639", "-121.9289", 62, 2),
    ("Fresno", "36.7762", "-119.7181", 336, 2),
    ("Boise", "43.5644", "-116.2228", 2871, 2),
    ("Spokane", "47.6199", "-117.5338", 2376, 2),
    ("Billings", "45.8077", "-108.5429", 3652, 2),
    ("Colorado Springs", "38.8058", "-104.7008", 6187, 2),
    ("Omaha", "41.3032", "-95.8941", 984, 2),
    ("Des Moines", "41.5340", "-93.6631", 958, 2),
    ("Madison", "43.1399", "-89.3375", 887, 2),
    ("Milwaukee", "42.9472", "-87.8966", 723, 2),
    ("Grand Rapids", "42.8808", "-85.5228", 794, 2),
    ("Indianapolis", "39.7173", "-86.2944", 797, 2),
    ("Cincinnati", "39.0488", "-84.6678", 896, 2),
    ("Columbus", "39.9980", "-82.8919", 815, 2),
    ("Cleveland", "41.4117", "-81.8498", 791, 2),
    ("Pittsburgh", "40.4915", "-80.2329", 1203, 2),
    ("Buffalo", "42.9405", "-78.7322", 728, 2),
    ("Rochester", "43.1189", "-77.6724", 559, 2),
    ("Syracuse", "43.1112", "-76.1063", 421, 2),
    ("Albany", "42.7483", "-73.8017", 285, 2),
    ("Hartford", "41.9389", "-72.6832", 173, 2),
    ("Providence", "41.7240", "-71.4282", 55, 2),
    ("Portland", "43.6462", "-70.3093", 76, 2),
    ("Norfolk", "36.8946", "-76.2012", 27, 2),
    ("Richmond", "37.5052", "-77.3197", 167, 2),
    ("Dulles", "38.9531", "-77.4565", 313, 2),
    ("Baltimore", "39.1754", "-76.6683", 146, 6),
    ("Philadelphia", "39.8744", "-75.2424", 36, 6),
    ("Raleigh", "35.8801", "-78.7880", 435, 2),
    ("Charleston", "32.8986", "-80.0405", 46, 2),
    ("Savannah", "32.1276", "-81.2021", 50, 2),
    ("Jacksonville", "30.4941", "-81.6879", 30, 2),
    ("Tampa", "27.9755", "-82.5332", 26, 6),
    ("Orlando", "28.4312", "-81.3081", 96, 6),
    ("West Palm Beach", "26.6832", "-80.0956", 19, 2),
    ("Fort Myers", "26.5362", "-81.7552", 30, 2),
    ("Pensacola", "30.4734", "-87.1866", 121, 2),
    ("Jackson", "32.3112", "-90.0759", 346, 2),
    ("New Orleans", "29.9934", "-90.2580", 4, 6),
    ("Shreveport", "32.4466", "-93.8256", 258, 2),
    ("Memphis", "35.0424", "-89.9767", 341, 6),
    ("Knoxville", "35.8110", "-83.9940", 981, 2),
    ("Louisville", "38.1744", "-85.7360", 501, 2),
    ("Lexington", "38.0365", "-84.6059", 979, 2),
    ("Dayton", "39.9024", "-84.2194", 1009, 2),
    ("Moline", "41.4485", "-90.5075", 764, 2),
    ("Fargo", "46.9207", "-96.8158", 902, 2),
    ("Sioux Falls", "43.5813", "-96.7419", 1429, 2),
    ("Wichita", "37.6499", "-97.4331", 1333, 2),
    ("Lubbock", "33.6636", "-101.8228", 3282, 2),
    ("Corpus Christi", "27.7704", "-97.5012", 44, 2),
    ("Amarillo", "35.2194", "-101.7059", 3607, 2),
    ("Green Bay", "44.4851", "-88.1296", 695, 2),
    ("Duluth", "46.8421", "-92.1936", 1428, 2),
    ("Springfield", "37.2457", "-93.3886", 1268, 2),
    ("Bozeman", "45.7775", "-111.1530", 4473, 2),
    ("Eugene", "44.1246", "-123.2119", 374, 2),
    ("Bakersfield", "35.4336", "-119.0568", 507, 2),
    ("Santa Barbara", "34.4262", "-119.8404", 13, 2),
    ("Missoula", "46.9163", "-114.0906", 3206, 2),
    ("Grand Forks", "47.9493", "-97.1761", 845, 2),
)


def _synthetic_code(index: int) -> str:
    first = chr(ord("A") + index // 26)
    second = chr(ord("A") + index % 26)
    return f"Z{first}{second}"


def _build_synthetic_airports() -> tuple[Airport, ...]:
    built: list[Airport] = []
    for index, (city, latitude, longitude, elevation, weight) in enumerate(_SYNTHETIC_FIELDS):
        iata = _synthetic_code(index)
        built.append(
            Airport(
                iata,
                f"K{iata}",
                f"{city} Synthetic Field",
                city,
                latitude,
                longitude,
                elevation,
                weight,
            )
        )
    return tuple(built)


SYNTHETIC_AIRPORTS: tuple[Airport, ...] = _build_synthetic_airports()


def mega_network(count: int = 100) -> tuple[Airport, ...]:
    """The classic 22, with hub weights raised, plus synthetic fields.

    ``count`` includes the original 22. 100 is the mega preset.
    """
    if count < len(AIRPORTS):
        raise ValueError(f"mega network needs at least {len(AIRPORTS)} airports")
    extra = count - len(AIRPORTS)
    if extra > len(SYNTHETIC_AIRPORTS):
        raise ValueError(
            f"only {len(SYNTHETIC_AIRPORTS)} synthetic airports are defined"
        )
    boosted: list[Airport] = []
    for airport in AIRPORTS:
        weight = MEGA_HUB_WEIGHTS.get(airport.iata, airport.departure_weight)
        if weight == airport.departure_weight:
            boosted.append(airport)
        else:
            boosted.append(replace(airport, departure_weight=weight))
    return tuple(boosted) + SYNTHETIC_AIRPORTS[:extra]
