"""HTTP API stub packaged by the Terraform api module.

Dependency-free so CI can compile it without an install and without AWS
credentials. It does not read a database.
"""

import json


def handler(event, context):
    route = event.get("routeKey", "")
    if route == "GET /health":
        return _json(200, {"status": "ok"})
    return _json(
        501,
        {
            "error": "not_implemented",
            "message": "Entity routes stay off until authentication exists.",
        },
    )


def _json(status, payload):
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(payload),
    }
