# Land the mega airport files

This page is how the 60-day airport dataset gets into the dev lake so Athena can read it. The tables already exist. Building them was a different step, described in [airport-mega-dataset.md](airport-mega-dataset.md). This workflow only copies files. Merging the workflow file does not copy anything.

## The words, briefly

The longer definitions (GitHub Actions, a workflow_dispatch button, the IAM user, S3 prefixes, NDJSON, Glue, Athena) are in [landing-data.md](landing-data.md). The same account, region, bucket, and access-key secrets are used here.

A few details are different from the e-commerce landing:

- The files are **uncompressed** NDJSON. Gzip is not used. Spark can split an uncompressed file. It cannot split a gzip file. The catalog SerDe reads one JSON object per line either way.
- Each operating day is its own folder, named `dt=YYYY-MM-DD`. Athena uses that folder name as the partition value. It does not open the other days.
- A rerun of one day **replaces that day's folder**. The e-commerce landing never deletes. This one deletes only the exact prefix it is about to write, then copies the new files, then checks each object's size.

The bucket is `ecom-737334244495-dev-lake`. The account is `737334244495`. The region is `eu-north-1` (Stockholm).

## What the button does

The workflow file is [`.github/workflows/land-dev-airports.yml`](../.github/workflows/land-dev-airports.yml). Its name on the Actions page is **Dev airports land**.

It does not run on a pull request. There is no dry-run job. Opening or merging the pull request uploads nothing.

A person runs it from the `main` branch and types `land-dev-airports` in the confirm box. Any other text prints a short note and uploads nothing. The same confirm typed on a branch other than `main` fails the run before any file is copied.

Two other boxes:

| Input | Default | Meaning |
| --- | --- | --- |
| `start_date` | `2026-04-01` | The first operating day to upload. |
| `days` | `2` | How many days to upload, from 1 to 60. This is not the generator's horizon. |

The generator always builds the mega preset: seed 42, 60 days starting 2026-04-01, 20,000 flights a day. A 2-day run still uses that preset and writes only the two dates you asked for. The last day of the 60-day horizon is 2026-05-30. A start date plus a day count that would pass 2026-05-30 fails before any upload.

Three jobs run when the confirm is right:

1. **Reference.** One runner writes the four shared tables (airports, airlines, aircraft types, aircraft) and uploads them. It also computes the list of dates for the next job.
2. **One job per day.** Up to 20 of those jobs run at the same time. Each runner generates only its own date, copies that date's four streams, and checks sizes. A failure in one day does not cancel the other days. The summary still waits until every day has finished, and it runs only when every day succeeded.
3. **Summary.** Adds the rows, bytes, and object counts from every job and prints the totals.

Each day is generated on the runner's disk and then copied. The runner does not keep the other days. One day is about half a gigabyte, mostly passengers.

## Where the files go

These prefixes are the same prefixes the Glue tables already point at.

| Glue table | S3 prefix |
| --- | --- |
| `raw_airport_passenger_events` | `raw/airports/passenger_events/dt=YYYY-MM-DD/` |
| `raw_airport_flight_events` | `raw/airports/flight_events/dt=YYYY-MM-DD/` |
| `raw_airport_fuel_records` | `raw/airports/fuel_records/dt=YYYY-MM-DD/` |
| `raw_airport_weather_reports` | `raw/airports/weather_reports/dt=YYYY-MM-DD/` |
| `raw_airport_airports` | `raw/airports/reference/airports/` |
| `raw_airport_airlines` | `raw/airports/reference/airlines/` |
| `raw_airport_aircraft_types` | `raw/airports/reference/aircraft_types/` |
| `raw_airport_aircraft` | `raw/airports/reference/aircraft/` |

Each prefix receives only `part-00000.ndjson` and, when a stream is larger than 128 MiB, the next part files. The generator's `manifest.json` stays on the runner. It is pretty-printed JSON, and the table would not read it as a row.

A second run of the same date deletes the objects under that date's four prefixes and writes the new parts. It does not touch another date, the reference tables (unless the reference job runs again), `raw/order-events/`, `gold/`, `athena-results/`, or the state bucket. The reference job replaces only the four `raw/airports/reference/<name>/` prefixes.

Before the first delete, the job calls `aws s3api head-bucket` on `ecom-737334244495-dev-lake` with `--expected-bucket-owner 737334244495`. That call fails if the bucket belongs to another account, and the job stops before it deletes anything. `aws s3 cp` and `aws s3 rm` do not accept `--expected-bucket-owner` (it is an `s3api` flag), so those copy and delete commands do not carry it.

After the copy, the job calls `aws s3api head-object` on every file, again with `--expected-bucket-owner`, and requires the S3 `ContentLength` to equal the local file size. A mismatch fails the job. The log prints rows, bytes, and object count for each stream.

## How to run it

Run the 2-day test first. Sixty days is the same button with a different number, after the test looks right.

1. Open the repository on GitHub. Choose **Actions**.
2. In the left list, choose **Dev airports land**.
3. Choose **Run workflow**.
4. The branch box must be `main`. A tag or another branch is refused.
5. Confirm: type `land-dev-airports`.
6. Start date: leave `2026-04-01`.
7. Days: leave `2` for the test. After that test, run again with `60`.
8. Choose **Run workflow** and wait. The 2-day test is three jobs that generate data (reference, April 1, April 2) plus the summary. Each day takes about half a minute to generate, then the copy. Sixty days is the same work, about 20 days at a time, so the clock time is a handful of waves rather than 60 times one day.

The workflow uses the existing secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. It does not create a user or a role. It does not assume a role. It does not run Terraform.

Do not set `AIRPORT_DAYS` anywhere in this workflow. That environment variable would change the generator's horizon. The horizon stays 60 so the last day's weather stays on 2026-05-30.

## Check one day in Athena

Use the workgroup `ecom-737334244495-dev-airports`. The orders workgroup is still capped at 10 MiB, which is smaller than one passenger file, so a query started there is canceled. The airport workgroup cap is 5 GiB.

In the Athena console, region Europe (Stockholm), set the workgroup before you run. Then, in database `ecom_737334244495_dev`:

```sql
SELECT dt, count(*) AS rows
FROM raw_airport_passenger_events
WHERE dt = '2026-04-01';
```

`dt` is a string. The quotes are required. Partition projection turns that filter into the single prefix `raw/airports/passenger_events/dt=2026-04-01/`. Athena does not list or read the other days. The query result should be one row, `dt` = `2026-04-01`, and about 1,265,941 passenger rows for seed 42. The bytes scanned should be about 0.39 GB, the size of that day's passenger files, not the size of every day.

The same shape works for the other event tables. Change the table name and keep the `dt` filter:

```sql
SELECT dt, count(*) AS rows
FROM raw_airport_flight_events
WHERE dt = '2026-04-01';
```

Reference tables have no `dt`. They are small:

```sql
SELECT count(*) AS rows
FROM raw_airport_airports;
```

That count is 100.

A count of passengers with no `dt` filter asks Athena to read every landed day. Sixty days is about 23 GB of passenger files, which is past the 5 GiB cap, so Athena cancels it. That cancel is the cap working. Use `dt` (or a short `dt BETWEEN` that stays under 5 GiB) when you query the event tables.

## What it costs

S3 Standard in `eu-north-1` is about **$0.023 per GB-month**. You pay for the days the objects exist. The lifecycle rule `expire-raw-airports` deletes `raw/airports/` after 30 days, including the previous version, so a one-time landing does not sit there for a year.

| Run | About how much data | About the S3 bill while it exists |
| --- | --- | --- |
| 2 days, plus the small reference tables | about 1 GB | about $0.02 for the month, then the 30-day rule deletes it |
| 60 days, plus reference | about 29 GB | about $0.70 for the month, then the same rule deletes it |

The measured 60-day total is 29.03 GB (29,032,728,027 bytes), from the benchmark in [airport-mega-dataset.md](airport-mega-dataset.md). Two days are a bit under 1 GB because each day is about 0.49 GB including its share of the streams, and the reference tables are about 5 MB.

Athena is $5.00 per TB scanned. The one-day passenger count above reads about 0.39 GB, which is a fraction of a cent. A query that hits the 5 GiB cap and is canceled costs about two and a half cents.

The GitHub runners are the free minutes that come with the repository. Generating one day is about half a minute of Python. Copying it is the S3 PUT requests for a handful of objects per stream. That request charge is cents, not dollars.

## How to clean up

The 30-day lifecycle rule is the normal cleanup. You do not have to delete by hand. If you want the bytes gone sooner, delete only the airport prefix:

```bash
aws s3api head-bucket \
  --bucket ecom-737334244495-dev-lake \
  --expected-bucket-owner 737334244495 \
  --region eu-north-1

aws s3 rm s3://ecom-737334244495-dev-lake/raw/airports/ \
  --recursive \
  --region eu-north-1
```

Run `head-bucket` first and stop if it fails. `aws s3 rm` cannot take `--expected-bucket-owner`; the owner check is the `s3api` call above.

The trailing slash after `raw/airports` matters. `raw/` is the parent of the order files and the catalog files. This command must not be pointed at `raw/` alone, at `gold/`, at `athena-results/`, or at the state bucket `ecom-737334244495-dev-tfstate`.

Versioning is on, so that command writes delete markers. The lifecycle rule also expires noncurrent versions under `raw/airports/` after 30 days. Until then, a delete marker hides the objects from Athena and from a normal list. The storage for the old versions remains until the noncurrent expiry.

Dropping a Glue table does not delete these files. Leaving the files and dropping nothing is also fine: the lifecycle rule removes them.

## What this workflow does not do

- It does not run on a pull request, and merging it does not upload.
- It does not run Terraform, and it does not change the catalog, the workgroup, or the lifecycle rules.
- It does not create or change IAM.
- It does not write `gold/`, `athena-results/`, the orders prefixes, or the state bucket.
- It does not start a Glue job or a Spark job.
- It does not land the e-commerce files. That button is **Land dev raw data**.
