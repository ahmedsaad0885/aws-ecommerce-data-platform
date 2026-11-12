#!/usr/bin/env bash
# Dev Terraform state bucket only. No DynamoDB table.
#   parse   Print "<bucket> <region>" and exit. Does not call AWS.
#   status  Print "missing" or "exists". Does not create the bucket.
#   ensure  Create the bucket in eu-north-1 when it is missing, then set
#           versioning, SSE-S3, Block Public Access, BucketOwnerEnforced,
#           and a deny-insecure-transport policy.
# Do not run this under bash -x. That would trace credentials.
set -euo pipefail

export AWS_PAGER=""
export AWS_EC2_METADATA_DISABLED=true

mode="${1:-}"
if [ "$mode" != "parse" ] && [ "$mode" != "status" ] && [ "$mode" != "ensure" ]; then
  echo "usage: $0 parse|status|ensure" >&2
  exit 2
fi

file="${DEV_BACKEND_HCL:-infra/envs/dev/backend.hcl}"
expected_bucket="ecom-737334244495-dev-tfstate"
expected_region="eu-north-1"
expected_account="737334244495"

read_hcl() {
  local key="$1"
  awk -v key="$key" '
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      line = $0
      sub(/#.*/, "", line)
      sub(/^[^=]*=[[:space:]]*/, "", line)
      gsub(/"/, "", line)
      gsub(/[[:space:]]/, "", line)
      print line
      exit
    }
  ' "$file"
}

bucket="$(read_hcl bucket)"
region="$(read_hcl region)"
if [ "$bucket" != "$expected_bucket" ] || [ "$region" != "$expected_region" ]; then
  echo "Refusing: ${file} must set bucket=${expected_bucket} and region=${expected_region}." >&2
  exit 1
fi

if [ "$mode" = "parse" ]; then
  printf '%s %s\n' "$bucket" "$region"
  exit 0
fi

account="$(aws sts get-caller-identity --query Account --output text)"
if [ "$account" != "$expected_account" ]; then
  echo "Refusing: caller account is ${account}, expected ${expected_account}." >&2
  exit 1
fi

head_err="$(mktemp)"
policy_file=""
cleanup() {
  rm -f "$head_err"
  if [ -n "$policy_file" ]; then
    rm -f "$policy_file"
  fi
}
trap cleanup EXIT

set +e
aws s3api head-bucket --bucket "$bucket" --region "$region" >/dev/null 2>"$head_err"
head_status=$?
set -e

if [ "$head_status" -ne 0 ]; then
  if grep -Eq '404|Not Found|NoSuchBucket' "$head_err"; then
    if [ "$mode" = "status" ]; then
      echo "missing"
      exit 0
    fi
    echo "Creating state bucket ${bucket} in ${region}" >&2
    # eu-north-1 is not us-east-1, so LocationConstraint is required.
    aws s3api create-bucket \
      --bucket "$bucket" \
      --region "$region" \
      --create-bucket-configuration "LocationConstraint=${region}"
  else
    echo "head-bucket failed for ${bucket}" >&2
    cat "$head_err" >&2
    exit 1
  fi
elif [ "$mode" = "status" ]; then
  location="$(aws s3api get-bucket-location --bucket "$bucket" --region "$region" --query LocationConstraint --output text)"
  if [ "$location" != "$expected_region" ]; then
    echo "State bucket ${bucket} is in ${location}, expected ${expected_region}." >&2
    exit 1
  fi
  echo "exists"
  exit 0
fi

location="$(aws s3api get-bucket-location --bucket "$bucket" --region "$region" --query LocationConstraint --output text)"
if [ "$location" != "$expected_region" ]; then
  echo "State bucket ${bucket} is in ${location}, expected ${expected_region}." >&2
  exit 1
fi

aws s3api put-public-access-block \
  --bucket "$bucket" \
  --region "$region" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-ownership-controls \
  --bucket "$bucket" \
  --region "$region" \
  --ownership-controls '{"Rules":[{"ObjectOwnership":"BucketOwnerEnforced"}]}'

aws s3api put-bucket-versioning \
  --bucket "$bucket" \
  --region "$region" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "$bucket" \
  --region "$region" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

policy_file="$(mktemp)"
cat >"$policy_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${bucket}",
        "arn:aws:s3:::${bucket}/*"
      ],
      "Condition": {
        "Bool": {"aws:SecureTransport": "false"}
      }
    }
  ]
}
EOF

aws s3api put-bucket-policy \
  --bucket "$bucket" \
  --region "$region" \
  --policy "file://${policy_file}"

aws s3api put-bucket-tagging \
  --bucket "$bucket" \
  --region "$region" \
  --tagging 'TagSet=[{Key=Project,Value=ecommerce-data-platform},{Key=Environment,Value=dev},{Key=ManagedBy,Value=bootstrap},{Key=Purpose,Value=terraform-state}]'

echo "ensured ${bucket} in ${region}" >&2
