# Gold orders Glue job

This page is the batch job that reads raw order events from the dev lake and writes gold orders as Parquet. It does not run when you merge. A person runs it by hand from `main`.

The account is `737334244495`. The region is `eu-north-1` (Stockholm). The lake bucket is `ecom-737334244495-dev-lake`. If a word below is new, it is defined the first time it is used.

The catalog and Athena words (database, table, workgroup, bytes scanned, partition projection) are defined in [analytics.md](analytics.md). This page defines the job words.

## The words

**AWS Glue** is a serverless data-processing service. Serverless means AWS starts the computers when a job runs and stops them when it finishes. You do not keep a cluster sitting on. **A Glue job** is a saved program plus its settings: which script to run, which identity it runs as, how many computers, and how long it is allowed to run. This repository has one job, `ecom-737334244495-dev-gold-orders`.

**PySpark** is Python sitting on **Apache Spark**. Spark reads many files at once, groups rows, and writes the result. A **worker** is one of those computers. The job uses worker type **G.1X**. One G.1X worker is 4 vCPUs and 16 GB of memory. vCPU means a slice of a processor. The [worker type page](https://docs.aws.amazon.com/glue/latest/dg/worker-types.html) is the source for those sizes.

A **DPU** (data processing unit) is Glue's billing unit for that computer. One DPU is 4 vCPUs and 16 GB of memory, so one G.1X worker is 1 DPU. The [Glue job API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html) says a Spark ETL job (`glueetl`) needs at least 2 DPUs. Two G.1X workers is that minimum: one runs the driver (the coordinator) and one runs the executor (the worker that processes partitions). **G.025X** is a quarter DPU and is only for streaming jobs, so it is not a cheaper option for this batch job.

**Glue 5.0** is the Glue runtime this job pins. It runs Spark 3.5 and Python 3.11. Glue 4.0 is the previous runtime (Spark 3.3, Python 3.10). Both use the same standard ETL price and the same 2-DPU minimum. Glue 5.0 is the one chosen here because the local test installs PySpark 3.5, so the engine in CI and the engine in AWS are the same generation. A newer Glue 6.0 meter exists and is priced separately. This job does not use it. The price section names the meter that does apply.

**Parquet** is a columnar file format. Columnar means the values of one column are stored together. A query that counts orders can read that column and skip the rest. **Snappy** is the compression codec on those files. It is fast to decompress. The files land at:

```text
s3://ecom-737334244495-dev-lake/gold/orders/order_date=YYYY-MM-DD/part-....snappy.parquet
```

A **partition** is that `order_date=YYYY-MM-DD/` directory. `order_date` is the UTC calendar date of `placed_at`. A query that names the date reads that directory and not the others. The catalog still uses partition projection, so nobody runs `MSCK REPAIR TABLE` and there is still no crawler.

**IAM** is AWS's permission system. An **IAM role** is an identity that a service assumes for a little while. It is not a person and it has no password. This job's role is `ecom-737334244495-dev-glue-gold-orders`. The **trust policy** is the document that says who may assume the role. Here the only principal is `glue.amazonaws.com`, and only when the request is for this account and this job. That pair of conditions (`aws:SourceAccount`, `aws:SourceArn`) is the usual guard against a confused deputy: another account's Glue service should not be able to assume this role. The **permissions policy** is a different document. It lists what the role may do after it has been assumed. This one is an **inline policy**, which means it is stuck to this role and cannot be attached to a second identity.

The raw file is **NDJSON**: one JSON object per line. The object in the lake is gzip-compressed (`.gz`). Spark decompresses that while it reads. Gzip is a compression format. It makes the file smaller. It does not change the JSON.

A **bookmark** is Glue's memory of which files it already processed. This job turns bookmarks off. Every run reads all of `raw/order-events/` again and overwrites the `order_date` directories it produces. A second run of the same input writes the same rows. That is the idempotent behavior. A date that no longer has any order is left in place, because the writer only replaces dates that are in the new result. Deleting raw events for a day does not delete that day's old Parquet until a later change does it on purpose.

## Why Spark, when the Python transform already exists

[pipelines/orders.py](../pipelines/orders.py) is the gold rule. It runs on one computer, reads one NDJSON file, and writes one NDJSON file. The tests around it are the contract: one row per order, latest event wins, `item_count` is the number of lines, `placed_at` does not move, `order_date` is the UTC date of `placed_at`, money stays a two-place decimal string.

The lake object is not that sample file. It is `raw/order-events/order_events.ndjson.gz`, 264,094 events, about 10.1 MB gzipped. The Python program can fold a file you already have on disk. It does not start computers in `eu-north-1`, and it does not write a partitioned Parquet dataset to S3.

The Glue job is the same rules on the lake. The script groups events by `order_id` in Spark, then calls `collapse_events` from `orders.py`. That file is zipped and given to every worker (`glue-scripts/gold_orders_lib.zip`), so the job cannot grow a second copy of the rules. Spark's job is the I/O: read the gzip NDJSON from S3, shuffle rows that share an `order_id` onto the same worker, and write Snappy Parquet partitioned by `order_date`. Local tests build a small Spark session, run that path, and require the Parquet rows to equal `collapse_events` on the same events. No AWS account is involved in those tests.

## What the apply creates

The flag is `enable_glue_jobs`. The committed dev tfvars sets it to true, next to the lake and the catalog. Example tfvars still default the rest of the stack on. Dev still leaves the VPC, database, stream, API, Bedrock, and alarms off.

| Terraform resource | What it is |
| --- | --- |
| `aws_s3_object.script` | `glue-scripts/gold_orders.py`, the entry script. |
| `aws_s3_object.library` | `glue-scripts/gold_orders_lib.zip`, `orders.py` plus the Spark runner. |
| `aws_iam_role.job` | The one role. Trust is `glue.amazonaws.com` for this job only. |
| `aws_iam_role_policy.job` | The inline permissions policy. There is no customer-managed policy and no second attachment. |
| `aws_glue_job.gold_orders` | Glue 5.0, G.1X, 2 workers, 15 minute timeout, 0 retries, bookmarks off. |
| `aws_glue_catalog_table.this["gold_orders"]` | Updated in the analytics module from the JSON SerDe to Parquet. Same columns, same `order_date` projection. |

`gold_reviews` stays newline-delimited JSON. Nothing writes it. The raw tables stay JSON. There is no crawler, no Glue trigger, and no Lake Formation change.

The lake bucket itself must stay unchanged. The plan guard rejects a plan that creates, replaces, or deletes the bucket. An in-place edit of the lifecycle configuration is the airport expiry rules, not this job. The script objects live in `module.glue_jobs`, not in the lake module, so uploading the script does not edit encryption or lifecycle.

### What the role is allowed to do

| Need | Allowed action | Resource |
| --- | --- | --- |
| Find the bucket | `s3:GetBucketLocation` | The lake bucket only |
| List the prefixes the job uses | `s3:ListBucket`, and only when the prefix is raw orders, gold orders, the script prefix, or the temp prefix | The lake bucket only |
| Read events and code | `s3:GetObject` | `raw/order-events/*` and `glue-scripts/*` |
| Write the result and Spark's temp files | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload` | `gold/orders/*` and `glue-temp/gold-orders/*` |
| Read the catalog entries it needs | `glue:GetDatabase`, `glue:GetTable`, `glue:GetTables`, `glue:GetPartition`, `glue:GetPartitions` | The dev database, `raw_order_events`, and `gold_orders` |
| Update one table | `glue:UpdateTable` and the partition create/update/delete calls | `gold_orders` only |
| Write logs | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | `/aws-glue/*` in this account and region |

`DeleteObject` is what an overwrite needs. Spark replaces the Parquet files in a partition; that deletes the previous objects (versioning keeps an old version as a delete marker). The role cannot delete the raw events, cannot read `raw/reviews/` or `raw/product-catalog/` object bodies, and cannot update `gold_reviews` or the raw tables.

The job writes files straight to S3. Terraform changes the catalog table from JSON to Parquet. The role's `glue:UpdateTable` is there so the job cannot be pointed at a second table later without a policy edit, and so a catalog call from the runtime is limited to `gold_orders`. It is not permission to change `raw_order_events`.

CloudWatch is AWS's log and metric service. A **log group** is a named stream of text. Glue writes this job's output under `/aws-glue/jobs` in `eu-north-1`.

## What the apply does not create

- No second role, no IAM user, no OIDC provider, no instance profile.
- No crawler, no schedule, no Glue trigger.
- No change to the lake bucket's settings. A correct plan shows `no-op` for `module.data_lake`.
- No VPC, NAT, RDS, DynamoDB, Kinesis, Lambda, or Bedrock.
- No query execution. Athena still runs as the IAM user who starts the query.

## If apply fails on IAM

This account is on the AWS free project-account plan. That plan already blocked creating a GitHub OIDC identity provider, which is why CI uses access keys. Creating a role is a different API (`iam:CreateRole` and `iam:PutRolePolicy`). It may work. It may be denied.

If the apply stops with `AccessDenied`, an explicit deny, or a message that mentions the free plan, IAM, `iam:CreateRole`, `iam:PutRolePolicy`, or `iam:TagRole`:

1. Stop.
2. Copy the error text.
3. Do not attach a broader policy to the CI user, do not add `AdministratorAccess`, do not create another user, and do not create an identity provider to get around it.
4. Report the error. Widening the caller is a separate decision, not part of this job.

A missing `glue:TagResource` or `iam:TagRole` on the caller is the same kind of stop. The provider tags resources with `Project=ecommerce-data-platform`, `Environment=dev`, and `ManagedBy=terraform`. Do not remove the tags to make apply succeed.

## Cost

Numbers are from AWS publications read for this change.

**The meter.** AWS Price List offer for AWS Glue, region `eu-north-1`, publication `2026-11-17T20:15:46Z`, SKU `NCNRTQBEK8K7PZBD`, usage type `EUN1-ETL-DPU-Hour`, group "ETL Job run": **$0.44 per DPU-hour**. The offer description is "$0.44 per Data Processing Unit-Hour for AWS Glue ETL job". That is the standard ETL meter. It is the one Glue 4.0 and Glue 5.0 use. The same file's Glue 6.0+ meter (SKU `KCSGWQC59PC5ZH7H`, `EUN1-ETL-DPU-Hour-Gen2`) is $0.308 and does not apply to this job. Flex (SKU `6YSZ9V6PKSR3KDDM`, `EUN1-ETL-Flex-DPU-Hour`) is $0.29 and can wait for spare capacity. This job is `STANDARD`, so a manual run does not sit in that queue.

**How the bill is counted.** The [Glue FAQ](https://aws.amazon.com/glue/faqs/) says you pay an hourly rate, billed per second, for the ETL job, with a 1-minute minimum. Billing starts when the job is scheduled and stops when it finishes. Startup that is inside the run counts. Time after the job completes does not. Crawlers have a 10-minute minimum. This stack creates no crawler.

This job is 2 workers × 1 DPU = 2 DPUs, timeout 15 minutes, `max_retries = 0` (a failure is not run again automatically).

| Run | DPU-hours | At $0.44 |
| --- | --- | --- |
| Hits the 1-minute minimum and stops | 2 × 1/60 = 0.0333 | about $0.015 |
| Runs 4 minutes, a plausible length for this file | 2 × 4/60 = 0.1333 | about $0.059 |
| Runs until the 15-minute timeout | 2 × 15/60 = 0.5 | $0.22 |

The 264,094-row file is small. Expect a few minutes, closer to the 4-minute row than to the timeout, plus whatever startup Glue counts inside the run. The API field `ExecutionTime` on the finished run is the number of seconds Glue reports. Multiply by 2 DPUs and by $0.44, then divide by 3600, and that is the job charge. If `ExecutionTime` is under 60, the 1-minute minimum still applies.

S3 storage and requests for the Parquet and for `glue-temp/` are on top of that, at [S3 prices](https://aws.amazon.com/s3/pricing/). Continuous logging is on so the run writes text to CloudWatch while it goes. That ingest is small next to a DPU-hour. The temp prefix is scratch. It is not a second copy of gold.

There is no charge while the job is not running. The role and the job definition are configuration.

## Run it

Pull requests only plan. Two workflows plan this stack on a pull request: **Dev lake and analytics** and **Dev Glue gold orders**. Neither applies. They share one state file, so do not start both apply buttons together. The state lock lets one through and makes the other wait.

After this change is on `main`:

1. Open Actions → **Dev Glue gold orders** → Run workflow. The branch must be `main`.
2. Type `apply-dev-glue-job` in confirm. Any other text plans and does not apply. The old string `apply-dev-analytics` is a different workflow. It applies the same state, including this job if the job is still pending. Prefer `apply-dev-glue-job` so the confirm string matches the change.
3. Wait for the apply job to finish green. If it fails on IAM, follow the stop-and-report section above. Do not re-run it with a wider policy.
4. Open Actions → **Dev Glue gold orders run** → Run workflow, on `main`, and type `start-dev-gold-orders`.
5. The run starts job `ecom-737334244495-dev-gold-orders` and prints the state until it is `SUCCEEDED`, `FAILED`, `TIMEOUT`, `STOPPED`, or `ERROR`. `ExecutionTime` is seconds. A pull request of this workflow only prints the job name. It does not start a run.

The job reads `s3://ecom-737334244495-dev-lake/raw/order-events/` and writes `s3://ecom-737334244495-dev-lake/gold/orders/`. The log line `Wrote N gold order row(s)` is the gold row count. N is the number of distinct orders, which is smaller than 264,094 events.

Running the start workflow a second time is safe. It reads the raw prefix again and replaces the date directories it writes.

## Check the result in Athena

Use the workgroup `ecom-737334244495-dev` in `eu-north-1`, database `ecom_737334244495_dev`. The console steps for finding that workgroup are in [analytics.md](analytics.md). Stay on that workgroup. The 10 MiB cap (`10485760` bytes) is enforced there. 10 MiB is 10 × 1024 × 1024 bytes.

Gold count, across the projected dates:

```sql
SELECT count(*) AS gold_orders
FROM gold_orders
WHERE order_date >= '2020-01-01';
```

The filter matters. Partition projection invents a prefix for every day from `2020-01-01` through today. The `WHERE` tells Athena which of those prefixes are in play. Empty days add no file bytes. After a successful job, `gold_orders` should equal the `Wrote N` line from the job log.

Money, still a string in the file, cast the same way as the saved query:

```sql
SELECT
  count(*) AS gold_orders,
  sum(CAST(total_amount AS DECIMAL(12, 2))) AS amount
FROM gold_orders
WHERE order_date >= '2020-01-01';
```

Raw event count, for the comparison:

```sql
SELECT count(*) AS raw_events
FROM raw_order_events;
```

The gzip object is about 10.1 MB, roughly 370 KB under the workgroup cap. A raw count can finish near that cap or be canceled if the scanned bytes cross it. Open the query in Athena's recent queries and read **Data scanned**.

Do the same for the gold count. Parquet stores columns, not whole JSON lines, and gold has one row per order instead of one row per event. The gold scanned figure should be far below the raw scanned figure, and comfortably under 10 MiB. That gap is why gold is Parquet. The cap stays at 10 MiB. Do not raise it to make the raw scan fit.

The saved query `gold_orders_sample_date` still filters `order_date = '2026-01-15'`. Its text is left exactly as it was deployed. Athena replaces a named query when the text changes, and that delete-and-create is rejected by the plan guard. The comment inside that saved query still says newline-delimited JSON. The catalog table is Parquet. The landed raw file is the large generated set, not the two-line sample. That saved query returns rows only when some order's `placed_at` is that UTC date. Zero rows there does not mean the job failed. Use the count above.

`gold_reviews` is unchanged and still empty.

## When something looks wrong

| What you see | What it usually means |
| --- | --- |
| Apply says `AccessDenied` on `iam:` or mentions the free plan | Stop. Report the error. Do not widen the caller. |
| `SUCCEEDED` and `Wrote 0` | `raw/order-events/` had no events the contract accepts. Check the prefix in the S3 console. |
| `FAILED` and the log says `order event rejected` | A line broke the gold contract (bad amount, unknown event type, empty items). The job does not write a partial gold result for that failure. Fix the raw line. Do not relax the rule in the job only. |
| Job fails with `sts:AssumeRole` | The trust policy refused the call. Stop and report the error. Do not delete the `aws:SourceAccount` / `aws:SourceArn` conditions, and do not point the job at the CI user's keys. |
| Job fails with `s3:ListBucket` denied | The role may list only the four prefixes in the policy. Stop and report the error. Do not grant `s3:*`. |
| Athena error about the JSON SerDe on `gold_orders` | The catalog update did not apply, or an old NDJSON object is still in `gold/orders/`. The table is Parquet now. Old JSON objects in that prefix will not read. |
| Gold count is canceled for the scan cap | The `WHERE order_date` filter is missing, or unexpected files landed under `gold/orders/`. The cap is doing its job. |
| Lake Formation denial | Stop. This job does not grant Lake Formation admin. |
| Second apply wants to change the lake bucket | Stop. The guard should have rejected that plan. Do not apply it by hand. |

## What is left

Reviews are not folded. Silver lines are not written. The job is manual. Nothing schedules it. The Python file remains the rule and the local tool.
