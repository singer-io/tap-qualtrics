from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from parameterized import parameterized

from tap_qualtrics.client import Client
from tap_qualtrics.streams import STREAMS

from tests.contracts.contract_harness import (
    build_selected_catalog,
    run_sync_collect_records,
    static_stream_names,
    validate_emitted_records_against_schemas,
)


STREAM_NAMES = static_stream_names(STREAMS)


def _load_live_config() -> tuple[dict, Path]:
    config_path = os.environ.get("QUALTRICS_CONTRACT_CONFIG")
    if not config_path:
        pytest.skip("Set QUALTRICS_CONTRACT_CONFIG to run live schema contract checks.")

    config_file = Path(config_path)
    if not config_file.exists():
        pytest.skip(f"QUALTRICS_CONTRACT_CONFIG path not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    required = {"client_id", "client_secret", "data_center", "auth_method", "start_date"}
    missing = sorted(required.difference(set(config.keys())))
    if missing:
        raise AssertionError(
            "Live contract config is missing required key(s): " + ", ".join(missing)
        )

    return config, config_file


def _selected_live_streams() -> tuple[str, ...]:
    configured = os.environ.get("QUALTRICS_LIVE_CONTRACT_STREAMS", "").strip()
    if not configured:
        return STREAM_NAMES
    selected = tuple(stream_name.strip() for stream_name in configured.split(",") if stream_name.strip())
    invalid = sorted(set(selected).difference(set(STREAM_NAMES)))
    if invalid:
        raise AssertionError(
            "Unknown stream(s) in QUALTRICS_LIVE_CONTRACT_STREAMS: " + ", ".join(invalid)
        )
    return selected


class TestLiveSchemaContract:

    @parameterized.expand([(stream_name,) for stream_name in _selected_live_streams()])
    @pytest.mark.live_contract
    def test_live_sync_records_match_stream_schema(self, stream_name):
        config, config_file = _load_live_config()
        with Client(config, config_path=str(config_file)) as client:
            catalog = build_selected_catalog(client, stream_name)
            emitted, _state = run_sync_collect_records(
                client=client,
                catalog=catalog,
                state={"bookmarks": {}},
            )

        if not emitted:
            pytest.skip(f"No records emitted for stream '{stream_name}' in this environment.")

        validate_emitted_records_against_schemas(emitted=emitted, catalog=catalog)
