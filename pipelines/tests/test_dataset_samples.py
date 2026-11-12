"""The committed snapshots are the sample scale at seed 42."""

from pipelines.datasets.airports import airport_params, generate_airports
from pipelines.datasets.common import repo_path, write_dataset
from pipelines.datasets.contracts import load_schema, validate
from pipelines.datasets.ecom import ecom_params, generate_ecom

ECOM_SCHEMAS = {
    "customers.ndjson": "customer.schema.json",
    "product_catalog.ndjson": "product_catalog_item.schema.json",
    "order_events.ndjson": "order_event.schema.json",
    "reviews.ndjson": "review.schema.json",
}
AIRPORT_SCHEMAS = {
    "airports.ndjson": "airport.schema.json",
    "airlines.ndjson": "airline.schema.json",
    "aircraft_types.ndjson": "aircraft_type.schema.json",
    "aircraft.ndjson": "aircraft.schema.json",
    "weather_reports.ndjson": "weather_report.schema.json",
    "flight_events.ndjson": "flight_event.schema.json",
    "fuel_records.ndjson": "fuel_record.schema.json",
    "passenger_events.ndjson": "passenger_event.schema.json",
}


def test_committed_ecom_sample_matches_seed_42(tmp_path):
    streams, manifest = generate_ecom(ecom_params("sample", 42))
    _assert_snapshot(tmp_path, streams, manifest, "ecom", ECOM_SCHEMAS)


def test_committed_airport_sample_matches_seed_42(tmp_path):
    streams, manifest = generate_airports(airport_params("sample", 42))
    _assert_snapshot(tmp_path, streams, manifest, "airports", AIRPORT_SCHEMAS)


def _assert_snapshot(tmp_path, streams, manifest, folder: str, schemas: dict[str, str]) -> None:
    for filename, schema_name in schemas.items():
        schema = load_schema(schema_name)
        stem = filename.removesuffix(".ndjson")
        for record in streams[stem]:
            validate(record, schema)
    write_dataset(tmp_path, streams, manifest)
    committed = repo_path("data", "samples", folder)
    for path in sorted(tmp_path.iterdir()):
        assert path.read_bytes() == (committed / path.name).read_bytes()
