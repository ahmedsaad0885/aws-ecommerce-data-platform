# Land raw files in the dev lake

This page is how the synthetic e-commerce files get into the dev lake so Athena has something to read. The catalog and the workgroup are a different manual step, described in [analytics.md](analytics.md). Landing does not create them, and it does not wait for them. The files can sit in the bucket before the catalog exists. Queries cannot see those files until the catalog exists.

Every term is defined the first time it is used. The longer catalog write-up is [analytics.md](analytics.md).

## The words

**GitHub Actions** is the automation runner attached to this git repository. A **workflow** is one automation file under `.github/workflows/`. A **job** is one unit of work inside that workflow. A **workflow_dispatch** trigger means the workflow does not run because someone merged code. A person opens the Actions page and presses a button. A **confirm input** is a text box on that button. This workflow uploads only when that box is exactly `land-dev-raw` and the branch is `main`.

A **pull request** is a proposed change. On a pull request this workflow generates a small dataset and prints the object names and sizes it would store. That job does not contact AWS.

**AWS** is the cloud account. This platform uses account `737334244495` in region `eu-north-1` (Stockholm). The region control in the console is the top bar. A console left on another region is looking at a different, empty set of resources.

**IAM** is AWS's permission system. An **IAM user** is an identity with long-lived keys. The user for this account is `grok_bot_admin`. An **access key** is the pair `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Those two values are stored as GitHub **secrets** (values Actions can read and the git history cannot). This workflow uses the same secrets as the lake workflow. It does not create a user, a role, or a policy. **OIDC** (a way for GitHub to borrow a role without a long-lived key) is not used. The account's free plan cannot create the identity provider that OIDC needs.

**Amazon S3** is object storage. A **bucket** is a named container. An **object** is a file plus a **key**, which is its path inside the bucket. A **prefix** is the leading part of a key, used like a folder. S3 does not have real folders. The console draws them from prefixes. The lake bucket is `ecom-737334244495-dev-lake`. The state bucket `ecom-737334244495-dev-tfstate` holds Terraform state. This workflow never writes to the state bucket.

**NDJSON** means newline-delimited JSON. Each line is one JSON object. Pretty-printed JSON, with a line break inside one object, is a different layout and the tables will not read it.

**Gzip** is a compression wrapper. The file name ends in `.gz`. The bytes inside, once unwrapped, are NDJSON. Gzip makes the stored file smaller. Athena's bill follows the smaller size, which is what keeps these files under the query cap below.

**AWS Glue Data Catalog** is a phone book of tables. It records column names and the S3 prefix where the files live. It does not store the rows. A **database** in that catalog is a folder of table names, not a server. The dev database is `ecom_737334244495_dev` (underscores, not hyphens). A **table** is one phone-book entry. Dropping the table does not delete the files.

A **SerDe** (serializer/deserializer) is the library that turns file bytes into columns. These tables use the Hive JSON SerDe. It expects one JSON object per line. Gzip is not a SerDe setting. Athena unwraps `.gz` first, then the SerDe reads the lines. The [Hive JSON SerDe](https://docs.aws.amazon.com/athena/latest/ug/hive-json-serde.html) page describes the one-line rule. The [compression](https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html) page says that for JSON, Athena chooses the compression from the file extension, and that the extension must be lowercase. `.GZ` is ignored. `tar.gz` is not supported.

**Amazon Athena** is serverless SQL over files in S3. Serverless means there is no cluster to start. You send a SQL statement. Athena reads the catalog, reads the objects the statement needs, and writes the result to S3. A **workgroup** is a saved profile for those queries: engine, result location, encryption, and the maximum amount of data one query may scan. The dev workgroup is `ecom-737334244495-dev`. It is not a database.

**Bytes scanned** is how much file content Athena had to read. That number is the Athena bill for the query. The [optimize data](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html) page says you pay for the bytes scanned before decompression, so a gzip object is billed at its compressed size. A **per-query data usage control**, also called the scan cap, is a ceiling on that number. Past the ceiling, Athena cancels the query. The dev cap is `10485760` bytes (10 MiB). A `LIMIT` clause does not shrink the scan of a JSON file. JSON is not columnar, so Athena reads the whole object even if the statement asks for 20 rows.

A **version** is an older copy of an object. The lake bucket has versioning turned on. Overwriting a key keeps the previous bytes as a noncurrent version. Athena reads the latest version only, so a rerun does not duplicate rows in a query. This workflow does not delete old versions.

## What this workflow does

The workflow file is [`.github/workflows/land-dev-raw.yml`](../.github/workflows/land-dev-raw.yml). Its name on the Actions page is **Land dev raw data**.

On a pull request, the job `dry-run dev raw landing` runs:

```bash
python3 scripts/land_dev_raw.py plan --scale test --seed 42
```

That generates the small `test` scale (16 products, 20 customers, 40 orders) and prints the three keys and the gzip sizes it would write. The log line says `aws_write=no`. The job does not configure AWS credentials. The sizes in that log are the small scale, not the beefy files. The keys are the same keys the manual run uses.

On a manual run from `main` with confirm `land-dev-raw`, the upload job does two things:

```bash
python3 -m pipelines.datasets ecom --scale beefy --seed 42 --output data/generated/ecom
python3 scripts/land_dev_raw.py upload --input data/generated/ecom
```

The generator is the standard-library program from [datasets.md](datasets.md). Seed 42 is fixed. The upload script gzips three of the files and puts those three objects. It lists each prefix first. If it finds any key other than the one it is about to overwrite (or a zero-byte prefix placeholder), it stops and does not delete the stranger. It checks the object after the put and fails the job if the stored size, the checksum, the encryption, or a content encoding does not match. A failed check does not delete the object.

Customers are generated and then left on the runner's disk. There is no Glue table for customers, so they are not uploaded. `manifest.json` is not uploaded either. It is pretty-printed JSON, and the SerDe would reject it. Airport files are not generated by this workflow.

## What it does not do

- It does not merge itself, and opening the pull request does not upload.
- It does not run Terraform, and it does not change the lake bucket, the catalog, or the workgroup.
- It does not create or change IAM.
- It does not delete objects. A rerun overwrites the same three keys.
- It does not write `gold/`, `bronze/`, `silver/`, or `athena-results/`.
- It does not write the state bucket `ecom-737334244495-dev-tfstate`.
- It does not upload customers or any airport stream.
- It does not start a Glue job, a crawler, or a Spark job. Gold files are a later change.

## The objects

The bucket is `ecom-737334244495-dev-lake`. These keys are fixed. A second run replaces the object at that key. It does not add `order_events-2.ndjson.gz` beside it. Two files under one prefix would both be scanned, and the rows would appear twice.

| Glue table | Object key | Rows (beefy, seed 42) | Uncompressed NDJSON | Gzip object |
| --- | --- | --- | --- | --- |
| `raw_order_events` | `raw/order-events/order_events.ndjson.gz` | 264,094 | 97,225,822 bytes | 10,115,450 bytes |
| `raw_product_catalog` | `raw/product-catalog/product_catalog.ndjson.gz` | 3,000 | 550,926 bytes | 38,466 bytes |
| `raw_reviews` | `raw/reviews/reviews.ndjson.gz` | 52,343 | 12,522,033 bytes | 957,566 bytes |

The three gzip objects together are 11,111,482 bytes. Each object is `Content-Type: application/gzip`, encrypted with SSE-S3 (Amazon S3's own AES-256 key, not a key you manage), and has no `Content-Encoding` header. The `.gz` suffix is what tells Athena to unwrap the file. Setting `Content-Encoding: gzip` as well can make a tool unwrap the file before Athena does, so the upload refuses an object that comes back with that header.

Order-event types inside the 264,094 rows: `OrderPlaced` 100,000, `OrderPaid` 86,022, `OrderCancelled` 9,954, `OrderShipped` 68,118. Catalog `status`: `active` 2,850, `discontinued` 150. Those counts come from `python3 scripts/land_dev_raw.py plan --scale beefy --seed 42` and the generator manifest. Re-run that command locally to print the sizes again. It does not call AWS. It takes a few tens of seconds.

The pull-request dry-run uses the `test` scale, so its printed sizes are much smaller. The keys in that log are still the three keys in the table.

## Why the files are gzipped

The workgroup cancels a query that would scan more than 10,485,760 bytes. A full scan of a raw table reads every object under that table's prefix. With one object per table, the scan is that object's size. `LIMIT 20` does not reduce it.

Uncompressed, two of the three files do not fit:

| File | Uncompressed bytes | Over the 10,485,760 cap? | Gzip bytes | Over the cap? |
| --- | --- | --- | --- | --- |
| Order events | 97,225,822 | yes, by 86,740,062 bytes | 10,115,450 | no, under by 370,310 bytes |
| Reviews | 12,522,033 | yes, by 2,036,273 bytes | 957,566 | no |
| Product catalog | 550,926 | no | 38,466 | no |

Gzip is the recommendation. The Hive JSON SerDe does not need a table change: Athena unwraps `.gz` using the suffix, then the SerDe reads one JSON object per line. The existing table definitions already use that SerDe and `TextInputFormat`. No Terraform change is required, so the lake bucket stays as it is and the catalog plan does not gain a new edit from this change.

Raising the cap high enough for the uncompressed order file would mean a ceiling above 97,225,822 bytes, about nine times the current rail, and every query of that file could scan the larger size. The cap exists so a mistake stays cheap. Gzip keeps the rail and still lets a full scan of each raw table finish.

The order-event margin is 370,310 bytes, about 3.5 percent. The upload measures the gzip it just built and refuses to put any object larger than the cap. If a later generator change grows the file past the cap, the manual run fails before the first put. Raising the cap would then be a separate, deliberate edit of `athena_bytes_scanned_cutoff_per_query` in the dev tfvars, followed by the analytics apply. This change does not do that.

## How to run the upload

Do this only when you want the three objects in the dev bucket. Merging the pull request does not do it.

1. Open the repository on GitHub.
2. Click **Actions**.
3. In the left list, click **Land dev raw data**. Do not click **Dev lake and analytics**. That other workflow registers the catalog. It does not upload these files.
4. Click **Run workflow**.
5. Set **Use workflow from** to `main`. Any other branch is refused, even if the confirm text is right.
6. In the confirm box, replace the default word `plan` with `land-dev-raw`. The letters, the hyphen, and the lack of spaces all matter. `plan`, `Land-dev-raw`, and `apply-dev-analytics` do not upload.
7. Press the green **Run workflow** button.
8. Open the run. The job that writes is **upload dev raw landing**. The log prints each key, the byte size, and `upload complete aws_write=yes` at the end.
9. A red run means you should read the log before trying again. A wrong branch or a wrong confirm string fails before any upload. A file over the scan cap fails before any upload. A stranger object already under one of the three prefixes fails before any upload, and the log names the key. This workflow will not delete that stranger.

The dry-run you see on a pull request is not this button. That job is there so a reviewer can see the keys. Its sizes are the small scale.

## What it costs

Figures below are from AWS price lists read for this change, plus the Athena rate already recorded in [analytics.md](analytics.md). They are not a guess at a monthly bill for a larger dataset.

**S3 storage and requests in Stockholm.** AWS Price List offer for Amazon S3, region `eu-north-1`, publication `2026-11-09T15:43:10Z`:

| What | Price | SKU | Usage type |
| --- | --- | --- | --- |
| S3 Standard, first 50 TB | $0.023 per GB-month | `W3FCBWN4J47P9GU4` | `EUN1-TimedStorage-ByteHrs` |
| PUT, COPY, POST, or LIST | $0.005 per 1,000 requests | `GRMX7KKGXZWXWYA7` | `EUN1-Requests-Tier1` |
| GET and other reads | $0.004 per 10,000 requests | `78XM3R8BT46YZKTU` | `EUN1-Requests-Tier2` |
| S3 Standard-IA storage | $0.0125 per GB-month | `EHYYY83E32UJWDAQ` | `EUN1-TimedStorage-SIA-ByteHrs` |
| Standard-IA retrieval | $0.01 per GB | `U8KZASK8A6XZUHT4` | `EUN1-Retrieval-SIA` |
| Lifecycle transition into Standard-IA | $0.01 per 1,000 requests | `WKJGWH3XQKHMK94Q` | `EUN1-Requests-Tier4` |

The public rate card is the [S3 pricing page](https://aws.amazon.com/s3/pricing/). The offer file is the source of the Stockholm SKUs above.

One upload stores 11,111,482 bytes. At $0.023 per GB-month that is about $0.00024 for a month if a GB is treated as 2^30 bytes, and about $0.00026 if a GB is 10^9 bytes. Either way it is a fraction of a cent. The upload itself is three PUT requests (about $0.000015) plus three LIST requests before the puts (the same $0.005 per 1,000) and three HEAD reads after (GET price). A rerun writes three new versions and leaves the previous versions in the bucket. Versioning is already on. This workflow does not turn it off and does not expire old versions, so a second run roughly doubles the stored bytes until someone deletes the old versions by hand. Two copies are still under a cent per month.

The lake already moves objects under `raw/` to S3 Standard-IA after 90 days. That rule is not changed here. [S3 Lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html) does not transition an object smaller than 128 KB by default, so the catalog object (38,466 bytes) stays in Standard. The order-event and review objects are larger than 128 KB, so they move. After the move, reading them adds the $0.01 per GB retrieval fee. A full read of those two objects is about a hundredth of a cent. Standard-IA also has a 30-day minimum storage duration, described on the [storage class page](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html). Overwriting them again within 30 days of the transition can add a small early-delete charge. For this dataset that charge is still well under a cent.

**Athena.** [analytics.md](analytics.md) records the Stockholm SQL price: $5.00 per TB scanned, with a 10 MB minimum on a successful or canceled query (offer publication `2026-09-11T12:44:08Z`, SKU `UJ5FAC3WF3PAZKV9`). The [Athena pricing page](https://aws.amazon.com/athena/pricing/) is the public page. There is no charge for the workgroup while it is idle. A query of `raw_order_events` scans 10,115,450 compressed bytes. At $5 per TB that is about $0.00005, the same neighborhood as the 10 MB minimum. The catalog and review objects are smaller, so those queries sit on the minimum. Three such queries are about $0.00015, plus the S3 GETs Athena makes to read the objects. A query Athena cancels for crossing the cap is billed for the bytes read until the cancel. The cap is what keeps that bounded. Failed queries, as opposed to canceled ones, are not charged under that price-list description.

Nothing in this workflow starts a Glue job, a crawler, a NAT gateway, or a database. Those are the things that bill while idle, and they stay off.

## Check the files in the S3 console

1. Sign in as the IAM user that owns the access keys (`grok_bot_admin`).
2. Set the region to **Europe (Stockholm)** `eu-north-1`.
3. Open S3: [https://eu-north-1.console.aws.amazon.com/s3/buckets/ecom-737334244495-dev-lake?region=eu-north-1](https://eu-north-1.console.aws.amazon.com/s3/buckets/ecom-737334244495-dev-lake?region=eu-north-1).
4. Open the bucket `ecom-737334244495-dev-lake`.
5. Open the prefix `raw/`, then `order-events/`. You should see `order_events.ndjson.gz` at about 9.6 MiB (10,115,450 bytes). The console may show mebibytes rather than the exact byte count. The object's properties should say the storage class is Standard (until the 90-day lifecycle moves it), encryption is SSE-S3, and there is no content encoding.
6. The same check for `raw/product-catalog/product_catalog.ndjson.gz` (38,466 bytes) and `raw/reviews/reviews.ndjson.gz` (957,566 bytes).
7. Leave `gold/`, `athena-results/`, `bronze/`, and `silver/` alone. This run does not add files there. `gold/` and the other top-level prefixes may already contain a zero-byte placeholder from the lake apply. That placeholder is not a data file.
8. Do not open the bucket `ecom-737334244495-dev-tfstate`. That is Terraform state, not lake data.

If **Show versions** is on, you may see older copies of the same key after a rerun. Athena uses the latest. This workflow does not delete the older copies.

## First queries in Athena

Run these only after both manual workflows have succeeded: the catalog apply (`apply-dev-analytics`, see [analytics.md](analytics.md)) and this upload (`land-dev-raw`). Order does not matter. The files can land first.

1. Open Athena in Stockholm: [https://eu-north-1.console.aws.amazon.com/athena/home?region=eu-north-1](https://eu-north-1.console.aws.amazon.com/athena/home?region=eu-north-1).
2. If Athena asks where to store results, that prompt is the built-in **primary** workgroup. Do not create a new bucket. Choose the workgroup `ecom-737334244495-dev`.
3. Choose the database `ecom_737334244495_dev`. The underscores are required. The hyphenated name is the workgroup, not the database.
4. Paste one of the statements below and run it. Stay on this workgroup. The primary workgroup does not have the 10 MiB cap.

Raw order events have no `status` column. `status` on an order (`placed`, `paid`, `shipped`, `cancelled`) is the gold row, and this workflow does not write gold. The raw column is `event_type`.

Count events by type:

```sql
SELECT event_type, COUNT(*) AS events
FROM raw_order_events
GROUP BY event_type
ORDER BY event_type;
```

Expect four rows: `OrderCancelled` 9954, `OrderPaid` 86022, `OrderPlaced` 100000, `OrderShipped` 68118. The scan is the 10,115,450 byte gzip object, under the cap. The saved query `raw_order_events_preview` returns 20 of those rows and scans the same object, because `LIMIT` does not skip the rest of a JSON file.

Count catalog rows by `status`. This is the column named status:

```sql
SELECT status, COUNT(*) AS products
FROM raw_product_catalog
GROUP BY status
ORDER BY status;
```

Expect `active` 2850 and `discontinued` 150.

Count reviews:

```sql
SELECT COUNT(*) AS reviews
FROM raw_reviews;
```

Expect one row, `52343`.

Gold stays empty. The saved query `gold_orders_sample_date` filters `order_date = '2026-01-15'` and should return zero rows. That is a successful empty result. Do not run an unfiltered `SELECT` on `gold_orders`. The table projects every day from 2020 through today, and a query without a date lists all of those days. The [analytics](analytics.md) page explains projection. A future job that writes gold should produce one row per order, with `status` counts `placed` 8067, `paid` 13861, `shipped` 68118, and `cancelled` 9954 (100,000 orders). Those numbers are the generator's manifest. They are not in the bucket today.

## If the catalog is not there yet

The upload does not create the database, the tables, or the workgroup. If that apply has not been run:

- The workgroup menu does not list `ecom-737334244495-dev`. Only the primary workgroup is there. Do not accept a prompt that creates a new results bucket.
- A query fails. The message says the database or the table does not exist. The spelling to look for is `ecom_737334244495_dev` and the table name, for example `raw_order_events`.
- The three objects are still in S3. Landing does not depend on the catalog. Fix the catalog by running **Dev lake and analytics** from `main` and typing `apply-dev-analytics`, then run the query again on the dev workgroup.

If the catalog exists and the upload has not been run, the same queries succeed and return zero rows. Zero rows means the prefix has no data file yet.

## When something looks wrong

| What you see | What it usually means |
| --- | --- |
| Actions log says `aws_write=no` and the sizes are tiny | That was the pull-request dry-run, or a manual run whose confirm box was not `land-dev-raw`. The beefy upload did not run. |
| The run is red before any `put` line | The branch was not `main`, the confirm string did not match, a gzip object was over the scan cap, or a stranger key is already under the prefix. Read the log. Nothing was deleted. |
| `put` printed and then the job failed the head check | That one object was written. The log names it. A later rerun overwrites the same key. This workflow will not delete it. |
| Query canceled, bytes scanned limit | The statement was going to read more than 10 MiB. An extra file under the prefix, or an uncompressed object beside the `.gz`, does that. The upload refuses to add a second file, but it will not remove one that is already there. |
| `HIVE_CURSOR_ERROR` | The file is not one JSON object per line, or a non-NDJSON object (the manifest, a folder marker with contents) was read. The landing objects are one object per line inside gzip. |
| Zero rows | The catalog exists and the prefix Athena read is empty, or the gold date filter does not match a directory. Raw counts above are the result after a successful upload. |
| Table or database does not exist | The analytics apply has not been run, or the console region is not `eu-north-1`, or the database name still has hyphens. The files can still be in S3. |
| Results landed in an unexpected bucket | The query ran in the primary workgroup. Switch to `ecom-737334244495-dev`. |
| `AccessDenied` | The error names the missing action. Report it. Do not create a role or a new user. |

## What a later gold job would hook onto

A later pull request can add a Glue PySpark job. The input keys are the three objects in the table above. The job would gunzip `raw/order-events/order_events.ndjson.gz`, collapse events with the same rules as [pipelines/orders.py](../pipelines/orders.py), and write Parquet (or NDJSON) under `gold/orders/order_date=YYYY-MM-DD/`. Catalog and review gold writes are separate. That job needs an IAM role. This account is not creating roles yet, and this workflow does not add one. The local transform still reads plain NDJSON, not gzip, and it still writes a file on disk. Nothing in this change starts that job or writes `gold/`.
