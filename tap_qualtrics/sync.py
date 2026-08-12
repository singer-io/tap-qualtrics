import singer
from typing import Dict
from tap_qualtrics.streams import STREAMS
from tap_qualtrics.client import Client

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def _attach_children(stream, streams_to_sync: list, catalog: singer.Catalog, client: Client) -> None:
    """Recursively attach selected child stream objects to a parent stream."""
    for child_name in stream.children:
        if child_name not in STREAMS:
            continue
        child_entry = catalog.get_stream(child_name)
        if child_entry is None:
            continue
        child_obj = STREAMS[child_name](client=client, catalog_entry=child_entry)
        if child_name in streams_to_sync or child_obj.parent:
            if child_obj.is_selected() or child_name in streams_to_sync:
                child_obj.write_schema()
            _attach_children(child_obj, streams_to_sync, catalog, client)
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state: Dict) -> None:
    streams_to_sync = [s.stream for s in catalog.get_selected_streams(state)]
    LOGGER.info("Selected streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("Currently syncing: %s", last_stream)

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            if stream_name not in STREAMS:
                LOGGER.warning("Stream %s not in STREAMS registry – skipping", stream_name)
                continue

            stream_entry = catalog.get_stream(stream_name)
            if stream_entry is None:
                continue

            stream = STREAMS[stream_name](client=client, catalog_entry=stream_entry)

            # Skip child-only streams; they are synced via their parent
            if stream.parent and stream.parent in streams_to_sync:
                continue

            _attach_children(stream, streams_to_sync, catalog, client)

            stream.write_schema()
            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)

            total = stream.sync(state=state, transformer=transformer)
            singer.write_state(state)

            update_currently_syncing(state, None)
            LOGGER.info("FINISHED Syncing: %s – %s records", stream_name, total)


