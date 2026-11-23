from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft4Validator, FormatChecker
from singer import metadata

from tap_qualtrics.discover import discover
from tap_qualtrics.schema import get_schemas
from tap_qualtrics.sync import sync

DYNAMIC_STREAM_BASES = {"audit_export", "survey_response_export"}


def _set_selected_flag(catalog_entry, selected: bool) -> None:
    mdata = metadata.to_map(catalog_entry.metadata)
    mdata = metadata.write(mdata, (), "selected", selected)
    catalog_entry.metadata = metadata.to_list(mdata)


def build_selected_catalog(client, stream_name: str):
    catalog = discover(client)
    dynamic_prefix = f"{stream_name}__"

    for entry in catalog.streams:
        selected = (
            entry.tap_stream_id == stream_name
            or entry.tap_stream_id.startswith(dynamic_prefix)
        )
        _set_selected_flag(entry, selected)

    return catalog


def run_sync_collect_records(
    client,
    catalog,
    state: dict | None = None,
) -> tuple[dict[str, list[dict]], dict]:
    state = state or {}
    state.setdefault("bookmarks", {})

    emitted: dict[str, list[dict]] = defaultdict(list)

    def _capture(stream_id, record):
        emitted[stream_id].append(record)

    with patch("tap_qualtrics.streams.abstracts.write_record", side_effect=_capture), patch(
        "singer.write_state"
    ):
        sync(client=client, config=client.config, catalog=catalog, state=state)

    return emitted, state


def static_stream_names(streams_map: Mapping[str, object]) -> tuple[str, ...]:
    return tuple(
        name for name in sorted(streams_map.keys())
        if name not in DYNAMIC_STREAM_BASES
    )


def validate_emitted_records_against_schemas(
    emitted: Mapping[str, Iterable[dict]],
    catalog,
) -> None:
    static_schemas, _ = get_schemas()
    validators = {
        stream_name: Draft4Validator(schema, format_checker=FormatChecker())
        for stream_name, schema in static_schemas.items()
    }

    for stream_name, records in emitted.items():
        entry = catalog.get_stream(stream_name)

        if stream_name in validators:
            validator = validators[stream_name]
        elif entry:
            validator = Draft4Validator(
                entry.schema.to_dict(),
                format_checker=FormatChecker(),
            )
        else:
            raise AssertionError(f"No catalog entry found for stream '{stream_name}'.")

        for record in records:
            validator.validate(record)


def cassette_file_path(fixtures_dir: Path, stream_name: str) -> Path:
    return fixtures_dir / f"{stream_name}.yaml"


def assert_stream_fixture_coverage(fixtures_dir: Path, stream_names: Iterable[str]) -> list[str]:
    missing = []
    for stream_name in stream_names:
        if not cassette_file_path(fixtures_dir, stream_name).exists():
            missing.append(stream_name)
    return missing

