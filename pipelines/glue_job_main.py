"""Glue job entry. Uploaded to s3://<lake>/glue-scripts/gold_orders.py.

The runnable logic is ``glue_gold_orders.run_job``. Glue executes this file
as ``__main__``. Spark workers import ``glue_gold_orders`` from the library
zip, so the collapse functions are not pickled out of ``__main__``.
"""

from __future__ import annotations


def main() -> None:
    try:
        from pipelines.glue_gold_orders import run_job
    except ImportError:
        from glue_gold_orders import run_job

    run_job()


if __name__ == "__main__":
    main()
