import io
import json
import zipfile
from typing import Any, Dict, Iterator

from singer import Transformer, get_bookmark, metrics, write_bookmark, write_record

from tap_qualtrics.streams.abstracts import FullTableStream


class AuditExport(FullTableStream):
    tap_stream_id = "audit_export"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["eventDate"]
    data_key = ""

    def get_records(self, parent_id: Any = None, start_date: str = "") -> Iterator[Dict]:
        body = {"startDate": start_date or self.client.start_date}
        start = self.client.post("audit-exports", body)
        export_id = (start.get("result") or {}).get("exportId", "")
        if not export_id:
            return

        final = self.client.poll_export(f"audit-exports/{export_id}")
        file_id = (final.get("result") or {}).get("fileId", export_id)

        resp = self.client.get_file(f"audit-exports/{export_id}/files/{file_id}")
        try:
            records = resp.json()
        except Exception:
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            records = []
            for name in zf.namelist():
                records.extend(json.loads(zf.read(name)))
        if isinstance(records, list):
            yield from records
        elif isinstance(records, dict):
            yield from records.get("events", [records])

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        bookmark = get_bookmark(
            state, self.tap_stream_id, self.replication_keys[0], self.client.start_date
        )
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(start_date=bookmark):
                transformed = transformer.transform(record, self.schema, self.mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                if record_bk and record_bk > max_bk:
                    max_bk = record_bk
        state = write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bk)
        return counter.value
