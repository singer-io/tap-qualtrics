from typing import Dict

import singer
from singer import metadata
from tap_qualtrics.client import Client
from tap_qualtrics.streams import STREAMS

LOGGER = singer.get_logger()

# Base tap_stream_ids whose catalog entries are named <base>__<parent_key> at runtime.
_DYNAMIC_SCHEMA_BASES = {"audit_export", "survey_response_export"}


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def _has_selected_dynamic_entries(child_name: str, catalog: singer.Catalog) -> bool:
    """Return True if any catalog entry named <child_name>__* is selected."""
    prefix = f"{child_name}__"
    return any(
        metadata.get(metadata.to_map(s.metadata), (), "selected")
        for s in catalog.streams
        if s.tap_stream_id.startswith(prefix)
    )


def write_schema(
    stream,
    client: Client,
    streams_to_sync: list,
    catalog: singer.Catalog,
) -> None:
    if stream.is_selected():
        stream.write_schema()

    for child_name in stream.children:
        child_entry = catalog.get_stream(child_name)
        if child_entry is None:
            # For dynamic-schema streams the catalog has <child>__<key> entries, not <child>.
            if (
                child_name in _DYNAMIC_SCHEMA_BASES
                and _has_selected_dynamic_entries(child_name, catalog)
            ):
                child_obj = STREAMS[child_name](client=client, catalog_entry=None)
                child_obj.catalog = catalog
                stream.child_to_sync.append(child_obj)
            continue
        child_obj = STREAMS[child_name](
            client=client,
            catalog_entry=child_entry,
        )
        child_obj.catalog = catalog
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child_name in streams_to_sync:
            stream.child_to_sync.append(child_obj)


def sync(
    client: Client,
    config: Dict,
    catalog: singer.Catalog,
    state: Dict,
) -> None:  # pylint: disable=unused-argument
    _ = config
    streams_to_sync = [s.stream for s in catalog.get_selected_streams(state)]
    LOGGER.info("Selected streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("Currently syncing: %s", last_stream)

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            if stream_name not in STREAMS:
                # Dynamic stream name (e.g. audit_export__login) — ensure its parent is queued.
                base_name = stream_name.split("__")[0] if "__" in stream_name else None
                if base_name and base_name in STREAMS:
                    parent_name = STREAMS[base_name].parent
                    if parent_name and parent_name not in streams_to_sync:
                        streams_to_sync.append(parent_name)
                else:
                    LOGGER.warning(
                        "Stream %s not in STREAMS registry - skipping",
                        stream_name,
                    )
                continue

            stream = STREAMS[stream_name](
                client=client,
                catalog_entry=catalog.get_stream(stream_name),
            )

            if stream.parent:
                # Auto-add parent so it drives this child; child is synced via parent
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            write_schema(stream, client, streams_to_sync, catalog)

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)

            try:
                total = stream.sync(state=state, transformer=transformer)
                singer.write_state(state)
                update_currently_syncing(state, None)
                LOGGER.info("FINISHED Syncing: %s - %s records", stream_name, total)
            except Exception:
                singer.write_state(state)
                raise
