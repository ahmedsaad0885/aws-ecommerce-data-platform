# Services

Two application stubs sit on top of the data stores. Both are dependency-free so CI can compile them with no install and no AWS credentials.

## API

`api/src/handler.py` is the file the Terraform `api` module packages. `GET /health` returns 200. Any other route returns 501.

Routes to add with authentication, not before:

- `GET /orders/{order_id}` reads RDS once a network path to the instance exists. This Lambda is outside the VPC.
- `GET /products/{product_id}` reads the DynamoDB catalog table. The execution role already allows `GetItem` and `Query` on that table and its indexes.

The API module registers only the health route.

## Review summaries

`llm/src/summarize.py` is the prompt contract: at most 20 review bodies in, one summary out. It raises `NotImplementedError` rather than calling Bedrock. The IAM role that a future function can assume is the Terraform `llm` module. The default model id is `amazon.nova-micro-v1:0`.
