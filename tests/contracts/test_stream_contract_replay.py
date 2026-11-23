from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import vcr
from parameterized import parameterized

from tap_qualtrics.client import Client
from tap_qualtrics.streams import STREAMS
from tests.contracts.contract_harness import (
    assert_stream_fixture_coverage,
    build_selected_catalog,
    run_sync_collect_records,
    static_stream_names,
    validate_emitted_records_against_schemas,
)

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "contracts"
STREAM_NAMES = static_stream_names(STREAMS)


def _scrub_request(request):
    headers = dict(request.headers or {})
    if "authorization" in {key.lower() for key in headers}:
        for key in list(headers.keys()):
            if key.lower() == "authorization":
                headers[key] = "<REDACTED>"
    request.headers = headers
    return request


def _scrub_response(response):
    headers = response.get("headers") or {}
    for key in list(headers.keys()):
        if key.lower() in {"set-cookie", "authorization"}:
            headers[key] = ["<REDACTED>"]
    response["headers"] = headers
    return response


CONTRACT_VCR = vcr.VCR(
    cassette_library_dir=str(FIXTURES_DIR),
    decode_compressed_response=True,
    match_on=["method", "scheme", "host", "port", "path", "query"],
    filter_headers=["authorization"],
    record_mode="none",
    before_record_request=_scrub_request,
    before_record_response=_scrub_response,
)


def _build_replay_client() -> Client:
    config = {
        "client_id": "fixture-client-id",
        "client_secret": "fixture-client-secret",
        "data_center": "fixturedc",
        "auth_method": "client_credentials",
        "start_date": "2020-01-01T00:00:00Z",
        "page_size": 100,
    }
    client = Client(config)
    client.access_token = "fixture-token"
    client._Client__expires = datetime.now(UTC) + timedelta(hours=1)  # pylint: disable=protected-access
    return client


@pytest.mark.contract
def test_contract_fixture_coverage_can_be_enforced():
    if os.environ.get("ENFORCE_CONTRACT_FIXTURE_COVERAGE", "0") not in ("1", "true", "yes"):
        pytest.skip("Set ENFORCE_CONTRACT_FIXTURE_COVERAGE=1 to enforce full registry fixture coverage.")

    missing = assert_stream_fixture_coverage(FIXTURES_DIR, STREAM_NAMES)
    assert not missing, f"Missing contract fixtures for stream(s): {', '.join(missing)}"


class TestStreamContractReplay:

    @parameterized.expand([(stream_name,) for stream_name in STREAM_NAMES])
    @pytest.mark.contract
    def test_stream_sync_replay_from_sanitized_fixture(self, stream_name):
        cassette_name = f"{stream_name}.yaml"
        cassette_path = FIXTURES_DIR / cassette_name
        if not cassette_path.exists():
            pytest.skip(
                f"No contract fixture found for stream '{stream_name}'. "
                f"Record one with scripts/record_contract_fixtures.py"
            )

        client = _build_replay_client()
        with CONTRACT_VCR.use_cassette(cassette_name):
            catalog = build_selected_catalog(client, stream_name)
            emitted, _state = run_sync_collect_records(
                client=client,
                catalog=catalog,
                state={"bookmarks": {}},
            )

        validate_emitted_records_against_schemas(emitted=emitted, catalog=catalog)
