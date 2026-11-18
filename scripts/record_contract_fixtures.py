#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable
from unittest.mock import patch

import vcr

from tap_qualtrics.client import Client
from tap_qualtrics.discover import discover
from tap_qualtrics.streams import STREAMS
from tap_qualtrics.sync import sync


FIXTURE_DIR = Path("tests") / "fixtures" / "contracts"
DYNAMIC_STREAM_BASES = {"audit_export", "survey_response_export"}


def _static_stream_names() -> list[str]:
    return sorted(
        stream_name
        for stream_name in STREAMS
        if stream_name not in DYNAMIC_STREAM_BASES
    )


def _scrub_request(request):
    headers = dict(request.headers or {})
    for key in list(headers.keys()):
        if key.lower() == "authorization":
            headers[key] = "<REDACTED>"
    request.headers = headers
    return request


def _scrub_json(value):
    if isinstance(value, dict):
        redacted_keys = {
            "access_token",
            "refresh_token",
            "client_secret",
            "client_id",
            "authorization",
        }
        return {
            key: "<REDACTED>" if key in redacted_keys else _scrub_json(nested)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_scrub_json(item) for item in value]
    return value


def _scrub_response(response):
    headers = response.get("headers") or {}
    for key in list(headers.keys()):
        if key.lower() in {"set-cookie", "authorization"}:
            headers[key] = ["<REDACTED>"]
    response["headers"] = headers

    body = response.get("body") or {}
    body_string = body.get("string")
    if isinstance(body_string, bytes):
        try:
            payload = json.loads(body_string.decode("utf-8"))
            body["string"] = json.dumps(_scrub_json(payload)).encode("utf-8")
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
    elif isinstance(body_string, str):
        try:
            payload = json.loads(body_string)
            body["string"] = json.dumps(_scrub_json(payload))
        except json.JSONDecodeError:
            pass

    response["body"] = body
    return response


def _selected_catalog(client, selected_stream: str):
    from singer import metadata

    catalog = discover(client)
    dynamic_prefix = f"{selected_stream}__"

    for entry in catalog.streams:
        selected = (
            entry.tap_stream_id == selected_stream
            or entry.tap_stream_id.startswith(dynamic_prefix)
        )
        mdata = metadata.to_map(entry.metadata)
        mdata = metadata.write(mdata, (), "selected", selected)
        entry.metadata = metadata.to_list(mdata)

    return catalog


def _record_stream_fixture(
    stream_name: str,
    config: dict,
    config_path: Path,
    recorder: vcr.VCR,
) -> None:
    cassette_name = f"{stream_name}.yaml"
    print(f"Recording fixture for stream: {stream_name} -> {cassette_name}")

    with recorder.use_cassette(cassette_name):
        with Client(config, config_path=str(config_path)) as client:
            catalog = _selected_catalog(client, stream_name)
            state = {"bookmarks": {}}
            with patch("tap_qualtrics.streams.abstracts.write_record"), patch(
                "singer.write_state"
            ):
                sync(client=client, config=config, catalog=catalog, state=state)


def _parse_streams_argument(streams: str | None) -> Iterable[str]:
    if not streams:
        return _static_stream_names()
    selected = [stream_name.strip() for stream_name in streams.split(",") if stream_name.strip()]
    unknown = sorted(set(selected).difference(set(STREAMS.keys())))
    if unknown:
        raise ValueError(f"Unknown stream(s): {', '.join(unknown)}")
    return selected


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Record sanitized VCR fixtures for tap-qualtrics stream contract tests."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to live Qualtrics tap config.json",
    )
    parser.add_argument(
        "--streams",
        default=None,
        help="Comma-separated stream tap_stream_ids (default: all non-dynamic streams)",
    )
    parser.add_argument(
        "--record-mode",
        default="once",
        choices=["once", "all", "new_episodes"],
        help="VCR record mode (default: once)",
    )
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    streams = list(_parse_streams_argument(args.streams))
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    recorder = vcr.VCR(
        cassette_library_dir=str(FIXTURE_DIR),
        decode_compressed_response=True,
        match_on=["method", "scheme", "host", "port", "path", "query"],
        record_mode=args.record_mode,
        filter_headers=["authorization"],
        filter_post_data_parameters=["client_id", "client_secret", "refresh_token", "access_token"],
        before_record_request=_scrub_request,
        before_record_response=_scrub_response,
    )

    for stream_name in streams:
        _record_stream_fixture(
            stream_name=stream_name,
            config=config,
            config_path=config_path,
            recorder=recorder,
        )

    print("Done recording contract fixtures.")


if __name__ == "__main__":
    main()
