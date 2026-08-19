import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Iterator

from singer import Transformer, get_bookmark, get_logger, metrics, write_bookmark, write_record

from tap_qualtrics.exceptions import QualtricsBadRequestError
from tap_qualtrics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class AuditExport(IncrementalStream):
    tap_stream_id = "audit_export"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["eventDate"]
    data_key = ""
    # parent = "audit_events_types"

    def _month_windows(self, start_date: str):
        """Yield (start, end) month-boundary pairs from start_date up to now."""
        from dateutil.relativedelta import relativedelta
        from dateutil.parser import parse as parse_date
        start = parse_date(start_date).replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        while start < now:
            end = start + relativedelta(months=1)
            yield (
                start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                min(end, now).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            start = end

    def get_records(self, parent_id: Any = None, start_date: str = "") -> Iterator[Dict]:
        sd = start_date or self.client.start_date
        event_name = (parent_id or {}).get("name") if isinstance(parent_id, dict) else parent_id
        if not event_name:
            return

        for window_start, window_end in self._month_windows(sd):
                body = {"eventName": event_name, "startDate": window_start, "endDate": window_end}
                try:
                    start = self.client.post("audit-exports", body)
                except QualtricsBadRequestError:
                    LOGGER.warning("Skipping unsupported audit export eventName: %s", event_name)
                    return  # skip remaining windows for this event_name
                # API returns 'id', not 'exportId'
                export_id = (start.get("result") or {}).get("id", "")
                if not export_id:
                    continue

                final = self.client.poll_export(f"audit-exports/{export_id}")
                file_id = (final.get("result") or {}).get("fileId", export_id)

                resp = self.client.get_file(f"audit-exports/{export_id}/files/{file_id}")
                if not resp.content:
                    continue
                try:
                    # File is NDJSON: one JSON object per line
                    for line in resp.content.splitlines():
                        line = line.strip()
                        if line:
                            yield json.loads(line)
                    continue
                except (json.JSONDecodeError, ValueError):
                    pass
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
            for record in self.get_records(parent_id=parent_id, start_date=bookmark):
                transformed = transformer.transform(record, self.schema, self.mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    if record_bk > max_bk:
                        max_bk = record_bk
        state = write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bk)
        return counter.value
