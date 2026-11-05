.PHONY: fmt validate check

TF_DIR := infra

fmt:
	terraform fmt -recursive

validate:
	terraform -chdir=$(TF_DIR) init -backend=false -input=false
	terraform -chdir=$(TF_DIR) validate

check: fmt validate
	python3 scripts/check_repo.py
	python3 -m compileall -q services
	npx --yes markdownlint-cli2
