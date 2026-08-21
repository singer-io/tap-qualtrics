import datetime
import io
import json
import zipfile
from typing import Any, Dict, Iterator

from singer import Transformer, get_bookmark, get_logger, metrics, write_bookmark, write_record

LOGGER = get_logger()

from tap_qualtrics.streams.abstracts import IncrementalStream


class TicketsExport(IncrementalStream):
    tap_stream_id = "tickets_export"
    key_properties = ["ticketId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updatedAt"]
    data_key = ""
    children = ["ticket_relative_events", "ticket_root_causes"]

    def _download_file(self, file_id: str) -> list:
        resp = self.client.get_file(f"ticket-exports/{file_id}/file")
        try:
            records = resp.json()
        except Exception:
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            records = []
            for name in zf.namelist():
                records.extend(json.loads(zf.read(name)))
        if isinstance(records, list):
            return records
        return records.get("tickets", [])

    def get_records(self, parent_id: Any = None, start_date: str = "") -> Iterator[Dict]:
        now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:00:00Z")
        sd = (start_date or self.client.start_date or "")[:10]
        body = {
            "fileType": "json",
            "filename": "Tap_Export",
            "updatedAtDates": {"startDate": f"{sd}T00:00:00Z", "endDate": now},
        }
        start = self.client.post("ticket-exports", body)
        export_id = (start.get("result") or {}).get("exportId", "")
        if not export_id:
            return

        final = self.client.poll_export(f"ticket-exports/{export_id}/status")
        file_id = (final.get("result") or {}).get("fileId", export_id)

        yield from self._download_file(file_id)

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        bookmark = get_bookmark(
            state, self.tap_stream_id, self.replication_keys[0], self.client.start_date
        )
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(start_date=bookmark):
                transformed = transformer.transform(record, self.schema, self.mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    if record_bk > max_bk:
                        max_bk = record_bk
                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_id=record)
        state = write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bk)
        return counter.value

