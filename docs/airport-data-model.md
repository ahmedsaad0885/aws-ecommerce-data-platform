# Airport traffic model

A second raw source beside the orders domain: a history of flights on a 22-airport US network. It is a simulation captured as files, not a live feed. Nothing here is a real passenger, a real tail number, or a copied weather product.

Contracts:

| Stream | JSON Schema | Avro |
| --- | --- | --- |
| Flight events | [flight_event.schema.json](../data/schemas/json/flight_event.schema.json) | [flight_event.avsc](../data/schemas/avro/flight_event.avsc) |
| Passenger events | [passenger_event.schema.json](../data/schemas/json/passenger_event.schema.json) | [passenger_event.avsc](../data/schemas/avro/passenger_event.avsc) |
| Weather | [weather_report.schema.json](../data/schemas/json/weather_report.schema.json) | [weather_report.avsc](../data/schemas/avro/weather_report.avsc) |
| Fuel | [fuel_record.schema.json](../data/schemas/json/fuel_record.schema.json) | [fuel_record.avsc](../data/schemas/avro/fuel_record.avsc) |
| Airports, airlines, aircraft types, aircraft | JSON Schema only | reference extracts, not a bus |

How to generate the files: [datasets.md](datasets.md). The arithmetic is [pipelines/datasets/fuel.py](../pipelines/datasets/fuel.py). The network and the ballpark performance tables are [pipelines/datasets/reference.py](../pipelines/datasets/reference.py).

## Identifiers and time

`flight_id` is stable for the life of the flight, including after a reroute. It contains the scheduled departure date, the flight number, the origin, and the originally scheduled destination. `tail_number` looks like `T-DL0001`. The `T-` prefix marks it as synthetic. It is not an FAA N-number.

Airport and airline codes are the public IATA and ICAO identifiers. Aircraft types use the public ICAO designators (`B738`, `A320`, `E75L`, and the others in the reference table).

Every timestamp is UTC. Event files are sorted by that timestamp. `delay_minutes` is the off-block slip against `scheduled_departure_at`. Departures are spread from 12:00Z through 23:30Z so the clock overlaps the North American operating day. Local airport hours are not modeled.

## Reference data

Twenty-two airports: ATL, ORD, DFW, DEN, LAX, SEA, SFO, LAS, PHX, MSP, DTW, BOS, EWR, MIA, CLT, SLC, SAN, PDX, AUS, BNA, MCI, STL. Coordinates and elevations are rounded public figures. Distance is a great circle on a sphere of 3440.065 nautical miles, rounded half-up to 0.01 nm. In this model ATL–ORD is about 527 nm.

Airlines are DL, AA, UA, WN, AS, B6, NK, and F9, with their public names and a primary hub inside this network. A flight is more likely to be assigned to an airline that lists the origin as a hub, but every airline can operate from any airport in the set.

Cruise speed, hourly fuel flow, taxi fuel, and seat count are planning ballparks for teaching. They are not approved flight-manual data. The Embraer 175 is only scheduled on stages of 900 nm or less. The narrowbodies are limited to 2600 nm. Those limits are scheduling rules for the simulation.

A tail has a location. It only departs the airport where it last arrived, and not until 45 minutes after arrival. If that airline has no aircraft of the required type at the origin, the generator creates one. It does not ferry a tail in from another airport, so the beefy fleet is several thousand tails rather than a tightly rotated one. Cancelled flights name a tail and do not move it.

## Flights

Each operating flight emits, in time order:

1. `FlightScheduled`, twelve hours before the scheduled departure. This event keeps the published destination.
2. `FlightDelayed`, when the off-block delay is at least 15 minutes.
3. `FlightDeparted`, `FlightAirborne` (twelve minutes later), `FlightEnRoute`, `FlightLanded`, `FlightArrived` (eight minutes after landing).

`FlightCancelled` replaces the movement. The reason is `airport_closure` or `weather`. A cancelled flight has no airborne event and no passengers.

`FlightRerouted` changes `destination_iata`. `previous_destination_iata` is the destination it had before the event, and the two differ.

- Before departure (`status` `rerouted`), every later event uses the new destination. Passengers are checked in after the reroute, so their destination matches the departure.
- After takeoff (`status` `diverted`), the aircraft lands at the new airport. Passenger events keep the destination from departure. Join them to the landing on `flight_id`, not on destination.

`scheduled_arrival_at` stays the original published arrival on a diversion, so the slip is visible. A pre-departure reroute updates the scheduled arrival on events after the reroute. `alternate_iata` is the nearest other airport in the network to the destination on that event. It is the planned alternate, not necessarily the field the flight diverts to. The diversion field is `destination_iata` on the reroute event.

The route string is `KATL DCT KORD` style: origin ICAO, direct, destination ICAO.

Weather drives most irregular operations. A closed origin or destination cancels the flight. IFR, LIFR, or thunderstorms at either end can cancel, delay, reroute before departure, or divert after takeoff. A minority of flights also take a delay for crew, ATC, or mechanical reasons, or wait for a tail (`late_inbound`). One cancellation, one delay, one pre-departure reroute, and one airborne diversion are always present so a small seed still covers each case. On the beefy run those scripted flights are a negligible share. Reroutes and diversions together stay a minority of flights.

## Passengers

`pax_per_flight` is a cabin sample, not a sold load. Beefy defaults to 8 people on each flight that actually departs. Cancelled flights have no passenger events.

About 6% are no-shows. A no-show is not boarded. Some of them check in first. Everyone else checks in, optionally drops a bag, and boards. Check-in is before bag drop, bag drop is before boarding, and boarding is before off-block. Names are two words from a fixed list. There is no document number, email, phone, or date of birth. The id is `pax_` plus a counter.

## Weather

One report per airport per hour, from the start of the window through 30 hours after the last scheduled day, so late arrivals still have a destination observation.

The report is METAR-inspired: wind direction and speed, optional gust, visibility in statute miles, ceiling in feet (`null` when unlimited), precipitation (`none`, `rain`, `snow`, `thunderstorm`), temperature, dewpoint, altimeter, and a closed flag. `flight_category` is derived:

| Category | Rule |
| --- | --- |
| LIFR | visibility below 1 mile, or ceiling below 500 ft |
| IFR | visibility below 3 miles, or ceiling below 1000 ft |
| MVFR | visibility below 5 miles, or ceiling at or below 3000 ft |
| VFR | otherwise |

A closed airport is also LIFR. Dewpoint is at or below the temperature. The series is a seeded random walk with a bias toward VFR. It is not an archive of real METARs.

`weather_report_id` on a flight event, when present, is the observation for the hour containing that event, so it is less than 60 minutes old. Departure, airborne, and en-route events point at the origin. Landing and arrival point at the destination on that event. A reroute points at the destination being left. A fuel record points at the observation that supplied the wind, which for a preflight plan is the origin at the scheduled departure hour. A ground delay does not by itself refresh that plan.

## Fuel

Each record stores the inputs and the rounded outputs. Components are rounded half-up to 0.01 kg before they are added, so the stored pieces sum to the stored total.

- `wind_component_kt` is the headwind along the course. Meteorological wind is the direction it comes from. A tailwind is negative.
- Ground speed is cruise true airspeed minus that component, and never below 220 kt.
- Trip fuel is the hourly burn times `distance_nm / ground_speed`.
- Alternate fuel uses the hourly burn at cruise true airspeed, with no wind, over the distance from the destination to the nearest other airport in the network.
- Contingency is 5% of trip fuel.
- Reserve is 45 minutes of the hourly burn.
- Planned fuel is burn already behind the aircraft, plus trip, taxi, contingency, alternate, and reserve.
- Estimated burn is burn already behind the aircraft, plus trip, plus taxi. Contingency, alternate, and reserve stay in the tanks in this sketch.
- Remaining fuel is uplift minus estimated burn, and it is never negative.

Preflight uplift is planned fuel plus an extra uplift (a small set of round kilogram amounts). A cancelled flight keeps the planned components, so the calculation is still visible, and stores uplift, estimated burn, and remaining as zero because the aircraft was not fueled.

A pre-departure reroute writes `FuelRevised` with a new destination, a new planned fuel, and a new uplift. An airborne diversion keeps the original uplift, because no fuel is added in the air. Taxi on that revision is zero: taxi was already included in `burn_so_far_kg`, which is the taxi allowance plus `decision_fraction` of the original trip fuel. The decision point is that same fraction of the way along the planned great circle. The remaining distance is from that point to the new airport. The new airport is the best reachable field that is not closed: visual conditions preferred, then nearest, and a return to the origin is allowed when nothing else fits. The generator will not emit a diversion whose remaining fuel would go negative.

`FuelCalculated.occurred_at` is six hours before scheduled departure. `FuelRevised` shares the reroute event's time, and `reroute_event_id` points at that flight event. The latest fuel record for a flight is the one that describes the fuel on board.

Worked example, locked by the unit test, for a 606.00 nm leg at 453 kt with a 20 kt headwind, 2600.00 kg/h, 220.00 kg taxi, an 18.00 nm alternate, 45 minutes reserve, and 150.00 kg extra:

| Piece | kg |
| --- | --- |
| Trip fuel | 3638.80 |
| Contingency | 181.94 |
| Alternate | 103.31 |
| Reserve | 1950.00 |
| Taxi | 220.00 |
| Planned | 6094.05 |
| Uplift | 6244.05 |
| Estimated burn | 3858.80 |
| Remaining | 2385.25 |

Ground speed is 433 kt and the planned airborne time is 84 minutes.

## What is not modeled

Crew legality, real airway routes, step climbs, holding, de-ice, payload, tank capacity, cost index, and a full passenger manifest. Wind is one airport observation, not a forecast grid. The numbers are for joins and for reading a fuel calculation, not for dispatch.
