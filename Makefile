.PHONY: fmt validate check pipeline

TF_DIR := infra

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
