.PHONY: fmt validate check pipeline dataset dataset-ecom dataset-airports dataset-sample land-dry-run

TF_DIR := infra
DATASET_SEED ?= 42
ECOM_SCALE ?= beefy
AIRPORT_SCALE ?= beefy

fmt:
	terraform fmt -recursive

validate:
	terraform -chdir=$(TF_DIR) init -backend=false -input=false
	terraform -chdir=$(TF_DIR) validate

check: fmt validate
	python3 scripts/check_repo.py
	python3 -m compileall -q services pipelines
	npx --yes markdownlint-cli2

pipeline:
	python3 -m pip install -r pipelines/requirements-dev.txt
	python3 -m pytest pipelines/tests -q
	python3 -m pipelines.orders transform --input data/samples/order_events.ndjson --output pipelines/out/gold_orders.ndjson
	python3 -m pipelines.orders check --output pipelines/out/gold_orders.ndjson

dataset: dataset-ecom dataset-airports

dataset-ecom:
	python3 -m pipelines.datasets ecom --scale $(ECOM_SCALE) --seed $(DATASET_SEED) --output data/generated/ecom

dataset-airports:
	python3 -m pipelines.datasets airports --scale $(AIRPORT_SCALE) --seed $(DATASET_SEED) --output data/generated/airports

# Print the dev-lake keys and gzip sizes for the small test scale. No AWS calls.
land-dry-run:
	python3 scripts/land_dev_raw.py plan --scale test --seed 42

# Committed snapshots. Seed 42 and the sample preset, with count overrides cleared.
dataset-sample:
	env -u ECOM_SCALE -u ECOM_ORDERS -u ECOM_PRODUCTS -u ECOM_CUSTOMERS -u ECOM_DAYS -u ECOM_REVIEW_PROBABILITY \
		python3 -m pipelines.datasets ecom --scale sample --seed 42 --output data/samples/ecom
	env -u AIRPORT_SCALE -u AIRPORT_DAYS -u AIRPORT_FLIGHTS_PER_DAY -u AIRPORT_PAX_PER_FLIGHT \
		python3 -m pipelines.datasets airports --scale sample --seed 42 --output data/samples/airports
