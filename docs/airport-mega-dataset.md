# Mega airport dataset

The `mega` preset is a large synthetic history for trying out a Spark job on airport data. It is still fictional. Names, tail numbers, and the extra airports are made up. The original 22 airport codes and the airline codes are the public ones.

`test`, `sample`, and `beefy` still write one flat `*.ndjson` file per stream, in memory, through `generate_airports`. Mega uses `write_partitioned_airports` instead. A full mega run is about 29 GB, so the generator writes one day at a time and streams the passenger rows to disk.

The rules for a single flight (events, fuel math, passengers) are the same ones in [airport-data-model.md](airport-data-model.md). This page is about size, files, and how to run a day.

## What you get

| Knob | Value |
| --- | --- |
| Days | 60, starting `2026-04-01` |
| Flights per day | 20,000 |
| Passengers per departed flight | 25 (a cabin sample, not a full aircraft) |
| Airports | 100 |

The 100 airports are the original 22 plus 78 synthetic fields. A synthetic field has a made-up code (`ZAA`, `ZAB`, ...) and sits near a real US city so the distances still look like a national network. It is not that city's real airport.

Departures are skewed the way a real network is skewed. A few hubs carry many times the traffic of a small field. On the measured day (1 April 2026, seed 42) ATL had 1,505 departures, ORD 1,320, and the quietest synthetic field 60. ATL's weight in the picker is 36. A small synthetic field's weight is 2.

## Why the files look like this

### Uncompressed NDJSON

Each row is one JSON object and one line. The bytes are not gzipped.

Spark can split that kind of file. A task may start at any newline, so several tasks can read one file at the same time. Gzip is different: a gzip member has to be inflated from the first byte, so a large `.gz` object becomes a single task. For a multi-gigabyte passenger file, that one task is the whole job. Leave these files uncompressed. Compress later, in the Spark write, if you want smaller Parquet.

### `dt=YYYY-MM-DD` folders

Event streams are laid out like this:

```text
out/passenger_events/dt=2026-04-01/part-00000.ndjson
out/passenger_events/dt=2026-04-01/part-00001.ndjson
out/flight_events/dt=2026-04-01/part-00000.ndjson
out/fuel_records/dt=2026-04-01/part-00000.ndjson
out/weather_reports/dt=2026-04-01/part-00000.ndjson
```

`dt=2026-04-01` is a Hive partition. The date is the flight's scheduled day, not the clock time on every row. A check-in can fall on the evening before, and a late landing can fall after midnight, and both rows still sit in the departure day's folder. Inside that folder the rows are ordered by time.

When a query asks for one date, Spark, Hive, or Athena read that folder and skip the other 59. Skipping unread folders is partition pruning. You do not pay to scan days you filtered out.

Reference tables have no date. They are the same for every day:

```text
out/airports/part-00000.ndjson
out/airlines/part-00000.ndjson
out/aircraft_types/part-00000.ndjson
out/aircraft/part-00000.ndjson
```

### About 128 MB per file

A part file rolls at 128 MiB (134,217,728 bytes). The row that crosses the line stays in the current file, and the next row opens `part-00001.ndjson`. On the measured day the first passenger part is 134,217,803 bytes, 75 bytes over the mark.

That size is a practical Spark task: big enough that the scheduler is not handed thousands of tiny files, small enough that one task is not a multi-gigabyte read. A day of passengers is three files. Flight, fuel, and weather are each one file per day, because each is under 128 MiB.

## How a shard works

One day does not read the other days. Day 17 with seed 42 is the same bytes whether you generate it alone or inside a 60-day run. Sixty jobs can run together.

```bash
python3 -m pipelines.datasets airports --scale mega --seed 42 --day-index 0 --output data/generated/airports-mega
python3 -m pipelines.datasets airports --scale mega --seed 42 --date 2026-04-15 --output data/generated/airports-mega
make dataset-airports-mega-day AIRPORT_DAY=17
```

`--day-index` is zero-based. `0` is 1 April 2026 and `59` is 30 May 2026. `--days` stays 60 on every job. That number is the length of the whole history. The last index (59) also writes the next 24 hours of weather, so a landing after midnight still has a report. If you pass a different `--days`, you change which day owns that extra slice.

Give each job its own output directory, then copy the `dt=` folders together. Every job also writes `manifest.json` for what that job wrote, and the same reference tables. Two jobs writing one directory will overwrite `manifest.json`. If you would rather write the reference tables once:

```bash
python3 -m pipelines.datasets airports --scale mega --seed 42 --reference-only --output data/generated/airports-mega
python3 -m pipelines.datasets airports --scale mega --seed 42 --day-index 0 --skip-reference --output data/generated/airports-mega
```

Ids are a block per day, so the jobs never pick the same one:

| Id | Block per day | Digits | Day 0 starts at |
| --- | --- | --- | --- |
| `fev_` flight event | 200,000 | 8 | `fev_00000001` |
| `fue_` fuel event | 50,000 | 8 | `fue_00000001` |
| `pev_` passenger event | 3,000,000 | 9 | `pev_000000001` |
| `pax_` passenger | 800,000 | 8 | `pax_00000001` |

The number is `day_index * block + sequence`, and the sequence starts at 1. Classic presets still use 8-digit `pev_` ids and 7-digit `pax_` ids. The JSON Schema accepts both widths. Weather ids stay `wx_` plus the ICAO code and `YYYYMMDDHH`, so the same hour is the same id in every shard, and each hour is written by only one shard.

`flight_id` contains the date, the flight number, the origin, and the scheduled destination. Flight numbers start again at 1000 each day (a 4-digit number cannot cover 1.2 million flights). The date keeps the id unique.

Tails are one fixed fleet: up to 2,200 aircraft of each type an airline flies, 48,400 tails in the reference file. Every shard writes that same file. A tail number always means the same airline and type. Each morning the tail is unassigned. The day does not start from where the previous shard parked it, because that shard may not have run. Within the day a tail does not overlap itself. On the measured day no flight had to wait for a tail and none was repositioned mid-day.

Passenger rows are written as they are generated, spilled by hour, then read back in time order. A day is about 1.3 million passenger events. They are not held as one Python list.

## Run it on Windows

The generator is the Python standard library. Python 3.10 or 3.12 is enough. From the repo root, with the `py` launcher:

```text
py -3.10 -m pipelines.datasets airports --scale mega --seed 42 --day-index 0 --output data\generated\airports-mega
```

`mingw32-make` is the stand-in for `make`:

```text
mingw32-make dataset-airports-mega-day AIRPORT_DAY=0
mingw32-make dataset-airports-mega-reference
```

The Makefile recipe calls `python3`. That works when `python3` is on `PATH`. If the machine only has the `py` launcher, use the `py -3.10 -m ...` command above. One day is about half a gigabyte and about half a minute.

## Measured day

Measured on this generator, Python 3.12.3, seed 42, mega defaults, one process. Day index 0 is the full run (reference tables included). Day index 59 was timed as well because it writes 48 hours of weather; the runtime matched.

| | Day 0 | Day 59 |
| --- | --- | --- |
| Wall time | 33.04 s | 33.07 s |
| Peak resident set | 444,116 KiB (434 MiB) | 437,988 KiB (428 MiB) |
| Passenger events | 1,265,941 | 1,245,728 |
| Flight events | 122,022 | 121,269 |
| Fuel records | 20,077 | 20,088 |
| Weather reports | 2,400 | 4,800 |

Day 0 bytes, and the file sizes Spark would see:

| Stream | Rows | Bytes | Files |
| --- | --- | --- | --- |
| `passenger_events` | 1,265,941 | 387,212,350 | 3 |
| `flight_events` | 122,022 | 74,562,220 | 1 |
| `fuel_records` | 20,077 | 21,155,756 | 1 |
| `weather_reports` | 2,400 | 852,520 | 1 |
| `aircraft` | 48,400 | 4,886,200 | 1 |
| `airports` | 100 | 16,524 | 1 |
| `airlines` | 8 | 711 | 1 |
| `aircraft_types` | 7 | 1,147 | 1 |
| Day 0 total | | 488,687,428 | 10 |

The three passenger parts are 134,217,803, 134,217,930, and 118,776,617 bytes.

Day 0 scenarios, out of 20,000 flights: 17,062 normal, 2,671 delayed, 190 cancelled, 34 rerouted before departure, 43 diverted. Cancelled flights have no passengers. Departed flights average about 2.56 passenger events each (check-in, often a bag, then boarding, with some no-shows).

## Sixty days, extrapolated

Flight, fuel, and passenger volume move about 1–2% from day to day. The total below is day 0's flight, fuel, and passenger bytes times 60. Weather is day 0 times 59, plus the measured day 59 file (4,800 reports, 1,705,205 bytes). Reference tables are counted once.

| Stream | Rows | Bytes |
| --- | --- | --- |
| `passenger_events` | 75,956,460 | 23,232,741,000 |
| `flight_events` | 7,321,320 | 4,473,733,200 |
| `fuel_records` | 1,204,620 | 1,269,345,360 |
| `weather_reports` | 146,400 | 52,003,885 |
| Reference tables | 48,515 | 4,904,582 |
| Total | | 29,032,728,027 |

That is 29.03 GB (decimal, 10^9) and 27.04 GiB. About 364 files if the reference tables are stored once and each day keeps the same part counts as the measured days (passengers 3, everything else 1).

Sixty jobs at once finish in about half a minute of wall time, plus copying the folders together. One process walking all 60 days is about 33 minutes.

The sketch for this preset was ~30 GB, with ~75 million passenger events (~22 GB), ~7.4 million flight events (~4.5 GB), ~1.2 million fuel rows (~1.3 GB), and ~260,000 weather rows (~0.1 GB). The measured total is inside 25–35 GB, so passengers per flight stayed at 25.

Weather is the one count that does not match the sketch. 146,400 rows is about 52 MB, not ~260,000 rows and ~0.1 GB. Each airport-hour is written once, so `report_id` stays unique: 100 airports × 24 hours × 60 days is 144,000, and the last day adds the next 24 hours (2,400 reports) for landings after midnight. Writing a long lookback into every shard would have landed near 260,000 rows by repeating the same hours.

Passenger events came out at 76.0 million and 23.2 GB, flight events at 7.32 million and 4.47 GB, fuel at 1.20 million and 1.27 GB. No knob was changed after the run.

## Catalog, workgroup, and how long the files live

Registering the tables does not copy the 29 GB. Terraform writes catalog entries, one Athena workgroup, and lifecycle rules. Those are configuration. The Glue Data Catalog's first million objects a month are free, and this adds eight tables. A workgroup with no queries has no Athena charge. A lifecycle rule has no charge of its own. The apply is about $0 until someone lands files or runs a query.

The database is the one that already exists, `ecom_737334244495_dev`. There is no crawler. Athena is told the date folders in advance, which is partition projection.

### Partition projection

The event files sit in folders named `dt=2026-04-01`. `dt` is not a column inside the JSON line. It is the folder.

Projection means Athena already knows every folder from 1 April 2026 through 30 May 2026 (the 60 operating days). You do not run `MSCK REPAIR TABLE`, and you do not start a crawler, when a new day lands. A query that says `WHERE dt = '2026-04-01'` reads that folder and skips the other 59. A query that forgets `dt` plans all 60 days. That is the whole dataset, about 29 GB, and the workgroup below will cancel it.

The projection type is date, formatted `yyyy-MM-dd`. The Glue partition column is a string, the same way `order_date` is a string on `gold_orders`. Hive stores the folder name as text.

Reference tables are not partitioned. They live at `raw/airports/reference/<name>/` because the generator writes them once, with no `dt=` folder. The event prefixes are:

| Table | S3 prefix |
| --- | --- |
| `raw_airport_passenger_events` | `raw/airports/passenger_events/` |
| `raw_airport_flight_events` | `raw/airports/flight_events/` |
| `raw_airport_fuel_records` | `raw/airports/fuel_records/` |
| `raw_airport_weather_reports` | `raw/airports/weather_reports/` |
| `raw_airport_airports` | `raw/airports/reference/airports/` |
| `raw_airport_airlines` | `raw/airports/reference/airlines/` |
| `raw_airport_aircraft_types` | `raw/airports/reference/aircraft_types/` |
| `raw_airport_aircraft` | `raw/airports/reference/aircraft/` |

Columns match the JSON Schema field names and order. JSON SerDe, uncompressed NDJSON. Money and distances stay decimal strings.

### The 5 GiB workgroup

Airport SQL uses a second workgroup, `ecom-737334244495-dev-airports`. The orders workgroup stays at 10 MiB. This one allows 5 GiB (5,368,709,120 bytes) per query.

The workgroup forces its own settings. A query cannot raise the cap or point results somewhere else from the console. Results go to `s3://ecom-737334244495-dev-lake/athena-results/airports/` with SSE-S3. The existing rule that deletes `athena-results/` after 7 days covers that prefix too.

Pick this workgroup in the Athena console before you run. A query started in the orders workgroup is still capped at 10 MiB, which is smaller than one passenger file.

Athena in `eu-north-1` is $5.00 per TB scanned. 5 GiB is about two and a half cents if a query is canceled at the cap. Scanning one passenger day (about 0.39 GB) is a fraction of a cent. The cap is what keeps a missing `dt` filter from reading all 29 GB.

### Lifecycle

The bucket already has versioning, a 90-day move of `raw/` to Standard-IA, and a 7-day expiry of `athena-results/`. Those rules stay. One lifecycle configuration holds every rule, so the new rules are added on that same resource. The bucket, encryption, and public-access block are not edited.

| Rule | What it does |
| --- | --- |
| `raw-to-ia` | Unchanged. Other `raw/` objects move to Standard-IA after 90 days. |
| `expire-athena-results` | Unchanged. `athena-results/` expires after 7 days. |
| `abort-multipart` | Incomplete multipart uploads anywhere in the bucket are aborted after 1 day. This used to be 7 days. |
| `expire-raw-airports` | Objects under `raw/airports/` expire after 30 days. The previous version is deleted after 30 days too, because versioning would otherwise keep the bytes. |
| `expire-glue-temp` | Objects under `glue-temp/` expire after 7 days, including the previous version. |

`raw/airports/` is also under `raw/`, so the 90-day Standard-IA rule matches it as well. The 30-day expiry happens first, so those objects are removed before they would have changed storage class.

This apply does not upload the NDJSON. Until a later job lands files, the expiry rules have nothing to delete.

### Glue role, later

This pull request does not create an IAM role. The job that reads these tables still needs one.

Glue 5 uses EMRFS for `s3://` paths. EMRFS lists parent prefixes, not only the leaf. `StringLike` value `raw/airports/*` already matches a list of exactly `raw/airports/`, so you do not add a second pattern for the trailing slash on top of `/*`. You do still list the parents themselves. The airport role's `s3:ListBucket` condition should include:

- `raw`
- `raw/`
- `raw/airports`
- `raw/airports/`
- `gold`
- `gold/`
- `glue-temp`
- `glue-temp/`

The gold-orders role on `main` already lists `gold`, `gold/`, `glue-temp`, and `glue-temp/` for the orders job. The airport role is a separate change. Do not widen the orders role in that step unless the airport job assumes that same role.
