# Glue Data Catalog and Athena

This page is the analytical layer on the dev lake. The lake bucket is already in account `737334244495`, region `eu-north-1` (Stockholm). The catalog and the query settings are declared in Terraform and are not created until someone runs the manual workflow from `main` and types `apply-dev-analytics`. A pull request plans. It does not apply. Merging does not apply.

The long version is here on purpose. Every term is defined the first time it is used.

## The words

**Amazon S3** is object storage. A **bucket** is a named container. An **object** is a file plus a **key**, which is its path inside the bucket. A **prefix** is the leading part of a key, used like a folder. The lake bucket is `ecom-737334244495-dev-lake`. `raw/order-events/events.ndjson` is an object key. `raw/order-events/` is a prefix. S3 does not have real folders. The console draws them from prefixes.

**AWS Glue Data Catalog** is a phone book of tables. Each entry records a table name, the column names and types, and the S3 prefix where the files live. It does not store the order rows. The rows stay in S3. Athena and other tools read the phone book to learn how to read those files. This repository registers that phone book from the contracts under [data/schemas](../data/schemas). It does not use a crawler.

A **crawler** is a Glue program that samples files and guesses a table definition. This platform does not create one. A second, guessed schema would drift from the contracts.

A **database** in the Glue Data Catalog is a folder of table names. It is not a server, and it is not the RDS PostgreSQL instance elsewhere in this repository. The dev database name is `ecom_737334244495_dev`. Hyphens are not allowed in a Glue database name, so the name prefix `ecom-737334244495-dev` is stored with underscores.

A **table** in the catalog is one phone-book entry: a name, columns, a file format, and an S3 location. Dropping the table removes the entry. It does not delete the files.

A **column** is one field. A **data type** says what kind of value the field holds. `string` is text. `int` is a whole number. `timestamp` is an instant in time. `array<struct<...>>` is a list of objects, which is how an order's lines are stored. Athena is **schema-on-read**: the types are applied when a query runs. A wrong type does not rewrite the file. It makes the query fail or return null.

**NDJSON** means newline-delimited JSON. Each line is one JSON object. The next line is the next row. Pretty-printed JSON, with a line break inside one object, is a different layout and these tables will not read it.

A **SerDe** (serializer/deserializer) is the library that turns file bytes into columns and columns back into bytes. These tables use the **Hive JSON SerDe**, class `org.apache.hive.hcatalog.data.JsonSerDe`. Athena documents two JSON libraries. The [OpenX JSON SerDe](https://docs.aws.amazon.com/athena/latest/ug/openx-json-serde.html) page says its results can be non-deterministic and names the Hive library as the workaround. The [Hive JSON SerDe](https://docs.aws.amazon.com/athena/latest/ug/hive-json-serde.html) page says each JSON object must be one line. That is the library registered here.

**Amazon Athena** is serverless SQL over files in S3. Serverless means there is no cluster to start, size, or stop. You send a SQL statement. Athena reads the catalog, reads the objects the statement needs, and writes the result to S3. You pay for the amount of data scanned, not for an idle server. The engine for this workgroup is **Athena engine version 3**, which runs Trino SQL. DDL (the statements that create and alter tables) is the Hive-style catalog language. Day-to-day questions are `SELECT`.

A **workgroup** is a saved profile for those queries: which engine, where results are written, how they are encrypted, and the maximum amount of data one query may scan. It is not a database and it is not a role. The dev workgroup is named `ecom-737334244495-dev`.

A **query result** is the file Athena writes after a query. It is not the lake data. Results for this workgroup go to `s3://ecom-737334244495-dev-lake/athena-results/`. The lake lifecycle rule deletes objects under that prefix 7 days after they are written.

**Bytes scanned** is how much file content Athena had to read to answer the statement. That number is the Athena bill for the query. A **per-query data usage control** is a ceiling on that number. Past the ceiling, Athena cancels the query. The cancel is the only action. It cannot be changed to "allow and warn."

A **named query** is a SQL statement saved in the workgroup so a person can open it in the console. Saving it does not run it. Terraform does not run these statements.

A **partition** is a slice of a table stored under its own prefix, usually `column=value/`. A query that names the slice reads that prefix and skips the others. That is how a scan stays small.

**Partition projection** is Athena calculating those slice names from table properties instead of looking up each slice in the catalog. No crawler, and no `MSCK REPAIR TABLE`, is required. `MSCK REPAIR TABLE` is the statement that walks S3 and adds Hive-style partitions to the catalog. We do not use it. Projection covers dates from `2020-01-01` through the current day.

**IAM** is AWS's permission system. An **IAM user** is a person or a tool with long-lived access keys. An **IAM role** is an identity that a service or a user assumes for a session. This apply does not create a user, a role, a policy, or an identity provider. Queries run as the IAM user who starts them (`grok_bot_admin`). A future Glue job would need a role. That role waits until the account can create roles.

## Names

| Thing | Name |
| --- | --- |
| Account | `737334244495` |
| Region | `eu-north-1` (Stockholm). The catalog is regional. A console set to another region will not show it. |
| Lake bucket | `ecom-737334244495-dev-lake` |
| State bucket | `ecom-737334244495-dev-tfstate` (not a Terraform resource) |
| State key | `ecommerce-data-platform/dev/terraform.tfstate` |
| Glue database | `ecom_737334244495_dev` |
| Athena workgroup | `ecom-737334244495-dev` |
| Result prefix | `s3://ecom-737334244495-dev-lake/athena-results/` |
| Scan cap | `10485760` bytes (10 MiB) |

## What the apply creates

These are the only new resources:

| Terraform resource | What it is |
| --- | --- |
| `aws_glue_catalog_database` | The database `ecom_737334244495_dev`. |
| `aws_glue_catalog_table` `raw_order_events` | NDJSON order events at `raw/order-events/`. Not partitioned. |
| `aws_glue_catalog_table` `raw_product_catalog` | NDJSON catalog file at `raw/product-catalog/`. Not partitioned. This is not DynamoDB. |
| `aws_glue_catalog_table` `raw_reviews` | NDJSON reviews at `raw/reviews/`. Not partitioned. |
| `aws_glue_catalog_table` `gold_orders` | NDJSON gold orders under `gold/orders/`, partitioned by `order_date`. |
| `aws_glue_catalog_table` `gold_reviews` | NDJSON gold reviews under `gold/reviews/`, partitioned by `created_date`. |
| `aws_athena_workgroup` | Engine v3, enforced settings, SSE-S3 results, 10 MiB scan cap. |
| `aws_athena_named_query` | Five saved `SELECT` statements. They are not executed. |

Column definitions live in [infra/modules/analytics/tables.json](../infra/modules/analytics/tables.json). Terraform reads that file. The checker rebuilds the expected columns from the JSON Schemas and from [data/schemas/sql/gold.sql](../data/schemas/sql/gold.sql).

The existing lake bucket, its encryption, its lifecycle rules, and the empty prefix markers stay as they are. A correct plan shows no create, change, or destroy for `module.data_lake`.

## What the apply does not create

- No IAM role, IAM user, IAM policy, instance profile, or OIDC provider.
- No Glue crawler, Glue job, or Glue trigger.
- No Lake Formation permissions or Lake Formation admin.
- No VPC, NAT gateway, RDS database, DynamoDB table, Kinesis stream, Firehose delivery stream, Lambda function, API Gateway, Bedrock role, or CloudWatch alarm.
- No rows copied by Terraform. Sample files stay in git. A separate manual workflow can land gzipped NDJSON under the three raw prefixes. See [landing-data.md](landing-data.md). That workflow is not this apply, and it does not write `gold/`.
- No query execution. Named queries are saved text.

Bronze and silver are prefixes in the bucket and are not tables yet. There is no separate file contract for those layers.

## Tables

### Raw order events

Contract: [data/schemas/json/order_event.schema.json](../data/schemas/json/order_event.schema.json). The Avro file is the same field names for a future stream consumer.

Location: `s3://ecom-737334244495-dev-lake/raw/order-events/`.

Put one object directly in that prefix. The landing workflow uses the fixed key `raw/order-events/order_events.ndjson.gz`. One JSON object per line, UTF-8, inside the gzip. The [Hive JSON SerDe](https://docs.aws.amazon.com/athena/latest/ug/hive-json-serde.html) rejects pretty-printed objects with the error `HIVE_CURSOR_ERROR`. A lowercase `.gz` suffix is how Athena knows to unwrap gzip before the SerDe sees the line. Do not also set `Content-Encoding: gzip` on the object.

The table is not partitioned. [docs/architecture.md](architecture.md) describes a future Firehose layout, `raw/order-events/year=YYYY/month=MM/day=DD/`, partitioned by arrival time. Streaming is not enabled, those prefixes are not in the catalog, and this table does not pretend they are. When Firehose exists, adding `year`, `month`, and `day` projection is a separate change. Until then, land flat NDJSON in `raw/order-events/` itself.

Column types follow the JSON Schema. Money (`total_amount`, and `unit_price` inside `items`) stays `string` because the contract stores a decimal string such as `"24.00"`, not a JSON number and not a float. `occurred_at` stays `string` for the same reason: the schema says text, an ISO-8601 UTC timestamp. `items` is `array<struct<product_id:string,quantity:int,unit_price:string>>`.

### Raw product catalog

Contract: [data/schemas/json/product_catalog_item.schema.json](../data/schemas/json/product_catalog_item.schema.json).

Location: `s3://ecom-737334244495-dev-lake/raw/product-catalog/`. The landing workflow writes `raw/product-catalog/product_catalog.ndjson.gz`.

The architecture says catalog files land under `raw/` until they have their own events. This prefix is that landing spot, next to `raw/order-events/`. It is not the DynamoDB table. DynamoDB is a different module and it is off. `price` is a decimal string. `status` is `active` or `discontinued`. Not partitioned.

### Raw reviews

Contract: [data/schemas/json/review.schema.json](../data/schemas/json/review.schema.json).

Location: `s3://ecom-737334244495-dev-lake/raw/reviews/`. The landing workflow writes `raw/reviews/reviews.ndjson.gz`.

`rating` is an integer from 1 to 5. `created_at` is ISO-8601 text. `summary` is not a column here. The JSON Schema does not include it. Not partitioned.

### Gold orders

Contract: [data/schemas/sql/gold.sql](../data/schemas/sql/gold.sql) and the transform in [pipelines/README.md](../pipelines/README.md).

Location root: `s3://ecom-737334244495-dev-lake/gold/orders/`.

One row per `order_id`. The latest event, ordered by `occurred_at` and then `event_id`, supplies `status`, `customer_id`, `currency`, `total_amount`, and the line list.

| Column | Type in the catalog | Meaning |
| --- | --- | --- |
| `order_id` | string | Order id. |
| `customer_id` | string | Customer on the latest event. |
| `status` | string | `placed`, `paid`, `shipped`, or `cancelled`. |
| `currency` | string | Three-letter code from the latest event. |
| `total_amount` | string | Decimal string. Cast to `DECIMAL(12, 2)` in the query. |
| `placed_at` | timestamp | Earliest `OrderPlaced` time, UTC. Later events do not move it. |
| `item_count` | int | Number of lines on the winning event. Not the sum of `quantity`. Not the number of events. |
| `order_date` | string, partition | UTC calendar date of `placed_at`. |

`item_count` is the rule that is easy to get wrong. The sample order `ord_1001` has one line, `prd_2001`, with quantity 2. Two events (`OrderPlaced`, then `OrderPaid`) collapse to one gold row. `item_count` is 1.

`placed_at` is a real timestamp column. The SerDe property `timestamp.formats` is `yyyy-MM-dd'T'HH:mm:ss'Z'` and the same pattern with exactly three fractional digits. The sample value `2026-01-15T14:03:00Z` matches the first pattern. A timestamp with one, two, or six fractional digits does not. Write whole seconds, or exactly milliseconds, when you land a file.

`total_amount` stays `string` in the catalog even though the logical mart type is `DECIMAL(12, 2)`. The file stores `"24.00"` so the cents cannot become a binary float. [Athena engine version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html) tightened type checks. Its migration notes say a string-to-decimal coercion that sometimes worked on older engines now fails. The catalog therefore matches the bytes. The saved query does `CAST(total_amount AS DECIMAL(12, 2))`. A later Parquet file can store a real decimal. This registration matches the NDJSON the local transform writes today.

`order_date` is not read from the JSON line. Athena takes it from the directory name. Land a file at:

```text
s3://ecom-737334244495-dev-lake/gold/orders/order_date=2026-01-15/part.ndjson
```

The JSON line may still contain `order_date`. The SerDe ignores a field that is not a data column. The local test file keeps `order_date` on the row because that file has no directory layout. The lake object uses the directory.

Partition projection generates `order_date` from `2020-01-01` through today. A query should still say `WHERE order_date = '2026-01-15'` (or a small range). Without that filter Athena lists every projected day. The scan cap stops a large read. It does not make the unfiltered listing a good idea. Dates before `2020-01-01` are outside the projection and will not be found until the range in `tables.json` is widened.

The sample date is `2026-01-15`. The saved query uses that date. Until an object exists at the prefix above, the query returns zero rows. That is a successful empty result, not a broken table.

### Gold reviews

Same SQL file. Location root: `s3://ecom-737334244495-dev-lake/gold/reviews/`.

Partition: `created_date`, the UTC date of `created_at`, directory `created_date=YYYY-MM-DD/`. The sample review time `2026-01-16T09:00:00Z` is the date `2026-01-16`.

`summary` is a column because the gold contract reserves it. The raw file does not have it. Nothing in this apply writes a summary, and nothing calls a model. The column is null until a later write. No local job produces gold review files yet, so this table stays empty after apply.

## Workgroup settings

| Setting | Value | Why |
| --- | --- | --- |
| Engine | `Athena engine version 3` | Pinned. `AUTO` would let Athena move the engine later. |
| `enforce_workgroup_configuration` | true | A client cannot point results at a different bucket or turn off encryption for this workgroup. |
| Result location | `s3://ecom-737334244495-dev-lake/athena-results/` | Same bucket. The lifecycle rule expires this prefix after 7 days. |
| Encryption | `SSE_S3` | Amazon S3-managed keys (AES-256). No customer-managed KMS key, so no key policy. |
| CloudWatch metrics | enabled | Query metrics for this workgroup. This does not create an alarm or an SNS topic. |
| Bytes scanned cutoff | `10485760` | Athena cancels a query that would scan more than this. |

The cutoff needs three numbers, because the documents do not use one unit:

- The Athena API [WorkGroupConfiguration](https://docs.aws.amazon.com/athena/latest/APIReference/API_WorkGroupConfiguration.html) says `BytesScannedCutoffPerQuery` minimum is `10000000`.
- The console [per-query control](https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html) says the minimum you can enter is 10 MB.
- The Terraform AWS provider rejects values below `10485760` (10 × 1024 × 1024). That is the value in dev tfvars, and it is the smallest value this stack can set.

A query that crosses the cap is canceled. Canceled queries are billed for the data scanned up to the cancel. The cap is what keeps that number bounded. The lake already aborts incomplete multipart uploads after 7 days, which is the cleanup the [per-query control page](https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html) asks for when a canceled query leaves a partial result.

The cap is a dev safety rail. Raising it is a one-line change to `athena_bytes_scanned_cutoff_per_query` in the dev tfvars, then another manual apply. Do not raise it so a query can scan the account by accident.

The beefy raw files fit under this cap when they are stored as gzip. Athena charges the compressed size: the [optimize data](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html) page says you pay for the bytes scanned before decompression. For JSON, the [compression](https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html) page says Athena picks gzip from a lowercase `.gz` suffix. The Hive JSON SerDe still sees one JSON object per line after that. The table definitions do not change. Measured sizes and the decision to keep `10485760` are in [landing-data.md](landing-data.md).

## Saved queries

They live under [infra/modules/analytics/queries](../infra/modules/analytics/queries). Each one is a `SELECT` with `LIMIT 20`. The gold queries also filter the partition column. Terraform stores them. It does not start them.

| Name | Reads | What you should see before any files exist |
| --- | --- | --- |
| `raw_order_events_preview` | `raw_order_events` | Zero rows. |
| `raw_product_catalog_preview` | `raw_product_catalog` | Zero rows. `price` is cast to `DECIMAL(12, 2)`. |
| `raw_reviews_preview` | `raw_reviews` | Zero rows. |
| `gold_orders_sample_date` | `gold_orders` where `order_date = '2026-01-15'` | Zero rows. `total_amount` is cast to `DECIMAL(12, 2)`. |
| `gold_reviews_sample_date` | `gold_reviews` where `created_date = '2026-01-16'` | Zero rows. |

Zero rows is the correct result. The prefixes are empty. A query that errors is a different outcome. Read the error text before changing a table.

## Run a query in the console

1. Sign in to the AWS console as the user that holds the access keys (the IAM user `grok_bot_admin`).
2. Set the region to **Europe (Stockholm)** `eu-north-1`. The region control is in the top bar. Athena and the Glue catalog in this account are in that region. Another region has a different, empty catalog.
3. Open the Athena console: [https://eu-north-1.console.aws.amazon.com/athena/home?region=eu-north-1](https://eu-north-1.console.aws.amazon.com/athena/home?region=eu-north-1).
4. If Athena asks where to store results, that prompt is the built-in **primary** workgroup. Do not accept a new bucket. Open the workgroup menu and choose `ecom-737334244495-dev`.
5. Confirm the workgroup. Settings on this workgroup are enforced: results go to `s3://ecom-737334244495-dev-lake/athena-results/`, encrypted with SSE-S3, and a query that would scan more than 10 MiB is canceled. The primary workgroup does not have that cap. Stay on `ecom-737334244495-dev`.
6. Choose the database `ecom_737334244495_dev`. The underscores are required. The hyphenated form is the workgroup name, not the database name.
7. Open **Saved queries** and run `gold_orders_sample_date`, or paste the statement from [gold_orders_sample_date.sql](../infra/modules/analytics/queries/gold_orders_sample_date.sql).
8. A finished query with zero rows means the catalog is visible and the prefix is empty. Download the result only if you want to see the column headers Athena wrote. That small file is under `athena-results/` and expires after 7 days.
9. To see a row, land the gzipped raw files with the manual workflow in [landing-data.md](landing-data.md), then run the saved query again. Terraform does not upload those files. A query with `LIMIT` still reads the whole JSON object, so the object has to fit under the 10 MiB cap. The gzipped beefy files do.

The editor's database and workgroup are part of the request. A saved query run from the primary workgroup ignores this stack's result location and scan cap.

## Permissions for the existing IAM user

Queries and Terraform both use the caller. For dev, that caller is the IAM user `grok_bot_admin`, through the access keys already stored as GitHub secrets `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. This stack does not create a role and does not attach a policy. If a plan or a query returns `AccessDenied`, the error names the missing action. Add that action to the existing user if you choose to. Do not create a new role, a new user, or an identity provider to get around it, and do not turn on account features that cannot be turned off.

The lists below are a checklist, taken from AWS's own examples. They are not a policy document to paste onto a new identity.

**Catalog, for apply and for a later update.** From [Configure access to databases and tables in the AWS Glue Data Catalog](https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html). Replace the sample account, region, and names with `eu-north-1`, `737334244495`, database `ecom_737334244495_dev`, and the table names above. The resource shapes on that page are `catalog`, `database/<name>`, and `table/<database>/<table>`.

- `glue:CreateDatabase`, `glue:GetDatabase`, `glue:GetDatabases`, `glue:UpdateDatabase`, `glue:DeleteDatabase`
- `glue:CreateTable`, `glue:GetTable`, `glue:GetTables`, `glue:DeleteTable`
- `glue:GetPartition`, `glue:GetPartitions` (the gold tables are projected, and the page still lists these on table access)
- `glue:UpdateTable` for the next apply that changes a column or a property. The page shows `glue:UpdateDatabase` for `ALTER DATABASE`. `glue:UpdateTable` is the matching table call. The action catalog is the [Glue API permissions reference](https://docs.aws.amazon.com/glue/latest/dg/api-permissions-reference.html) linked from that page.

**Workgroup and named queries.** From [Example workgroup policies](https://docs.aws.amazon.com/athena/latest/ug/example-policies-workgroup.html), using the workgroup ARN `arn:aws:athena:eu-north-1:737334244495:workgroup/ecom-737334244495-dev`.

- Apply: `athena:CreateWorkGroup`, `athena:GetWorkGroup`, `athena:UpdateWorkGroup`, `athena:DeleteWorkGroup`, `athena:ListWorkGroups`, `athena:ListEngineVersions`, `athena:CreateNamedQuery`, `athena:GetNamedQuery`, `athena:DeleteNamedQuery`, `athena:ListNamedQueries`, `athena:BatchGetNamedQuery`
- Run a query: `athena:StartQueryExecution`, `athena:StopQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`, `athena:GetQueryResultsStream`, `athena:BatchGetQueryExecution`, `athena:ListQueryExecutions`, plus `athena:GetTableMetadata` and `athena:ListTableMetadata` if the console lists tables through Athena rather than only through Glue

**S3.** From [Control access to Amazon S3 from Athena](https://docs.aws.amazon.com/athena/latest/ug/s3-permissions.html). That page's identity-based example is `s3:ListBucket` on the bucket and `s3:*Object` on the objects. For this bucket the practical split is:

- `s3:ListBucket` on `arn:aws:s3:::ecom-737334244495-dev-lake`
- `s3:GetObject` on the data prefixes `raw/*` and `gold/*`
- `s3:GetObject` and `s3:PutObject` on `athena-results/*`
- The state bucket is separate. Terraform already uses it for the lake. This apply reuses that same state key.

The bucket policy already denies requests that are not HTTPS. The console and the AWS CLI use HTTPS.

**Tags.** The provider adds the same tags as the rest of the stack: `Project=ecommerce-data-platform`, `Environment=dev`, `ManagedBy=terraform`. If apply fails on `glue:TagResource` or `athena:TagResource`, that action is the gap. Do not remove the tags and do not invent a role to hold them.

**Do not use** the Spark notebook example on the workgroup policy page. That example includes `iam:CreateRole`. This account is not creating roles.

Lake Formation is a second permission layer on the catalog. This module does not call it and does not grant admin. If a query fails with a Lake Formation denial, stop and report the message. Do not click "grant admin" as part of this apply.

## Cost

Numbers below are from AWS publications read for this change. They are not estimates invented for the page.

**Athena SQL in Stockholm.** The AWS Price List offer file for Amazon Athena, region `eu-north-1`, publication `2026-09-11T12:44:08Z`, SKU `UJ5FAC3WF3PAZKV9`, usage type `EUN1-DataScannedInTB`, is **$5.00 per terabyte** scanned. The offer description says: "Charged for total data scanned per query with a minimum 10MB for each successful or cancelled queries." The public examples on the [Athena pricing page](https://aws.amazon.com/athena/pricing/) use the same $5 per TB rate. The [Athena FAQ](https://aws.amazon.com/athena/faqs/) says failed queries are not charged under per-query pricing, and a query you cancel is charged for the data scanned until the cancel.

There is no cluster charge. A workgroup is configuration. You pay when a query scans data. DDL that only changes the catalog is not a data scan. An empty `SELECT` that succeeds still sits on the 10 MB minimum in that price-list description, which at $5 per TB is well under one cent. The 10 MiB workgroup cap is the same order of magnitude as that minimum, so a runaway query is canceled around the smallest scan Athena bills.

This workgroup does not use provisioned capacity and does not use Athena Spark. Those are different meters. They are not turned on. The pricing page also says S3 charges for storage, requests, and the result files, on top of the Athena scan price. S3 rates are on the [S3 pricing page](https://aws.amazon.com/s3/pricing/). Result objects expire after 7 days.

**Glue Data Catalog in Stockholm.** The [Glue pricing page](https://aws.amazon.com/glue/pricing/) includes a million metadata objects stored and a million metadata requests each month before the paid rates. The AWS Price List offer for AWS Glue, region `eu-north-1`, publication `2026-11-08T17:23:58Z`, then charges **$1 per 100,000 objects per month** for catalog storage (SKU `ZZGHEV6BARPHXPK8`, usage type `EUN1-Catalog-Storage`) and **$1 per 1,000,000 requests** (SKU `B7AB9XZF8FCFJUDN`, usage type `EUN1-Catalog-Request`). A metadata object is a database, a table, a partition, and similar catalog records. This apply adds one database and five tables: six objects, and no partitions, because gold uses projection instead of stored partitions. That is inside the free allowance. Crawlers are a separate DPU-hour charge. This apply creates none.

**What stays free of new fixed cost.** No NAT gateway, no RDS instance, no Kinesis stream, no interface VPC endpoint. Those are the resources that bill while idle, and they stay off.

## When something looks wrong

| What you see | What it usually means |
| --- | --- |
| Zero rows | The prefix is empty, or the gold date filter does not match a directory. This is expected today. |
| Query canceled, bytes scanned limit | The workgroup cap fired. The statement was going to read more than 10 MiB. Narrow the `WHERE` or, later, raise the cap on purpose. |
| `HIVE_CURSOR_ERROR` / not a valid JSON object | The file is pretty-printed, or more than one JSON value was placed on one line. Use NDJSON. |
| `placed_at` fails to parse | The timestamp is not whole seconds and not exactly three fractional digits. See the formats above. |
| Table missing | The console region is not `eu-north-1`, or the database name still has hyphens. |
| Results landed in an unexpected bucket | The query ran in the primary workgroup. Switch to `ecom-737334244495-dev`. |
| `AccessDenied` | The error names an action from the checklist. Report it. Do not create a role. |
| Lake Formation denial | Stop. This apply does not change Lake Formation. |

## What is left for later

A Glue job that reads `raw/order-events/` and writes `gold/orders/order_date=.../` needs an IAM role. That role is not in this module. The comment in the module says why: role creation waits until the account can create roles.

Firehose arrival-time prefixes are not catalog partitions yet. Parquet is not the storage of these tables yet. The gold SQL file says both are later, and that the columns can stay. Sample NDJSON is not copied into the bucket by Terraform. The manual landing workflow can put gzipped raw NDJSON. It does not write `gold/`, and it does not add the Glue job that would. That job is a later change. The hook is described in [landing-data.md](landing-data.md).
