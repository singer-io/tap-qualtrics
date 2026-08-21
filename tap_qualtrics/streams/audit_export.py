import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List

import backoff

from singer import Transformer, get_bookmark, get_logger, metadata, metrics, write_bookmark, write_record, write_schema

from tap_qualtrics.exceptions import QualtricsBadRequestError, QualtricsBackoffError
from tap_qualtrics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class AuditExport(IncrementalStream):
    tap_stream_id = "audit_export"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["timestamp"]
    data_key = ""
    parent = "audit_export_event_types"
    dynamic_schema = True

    @classmethod
    def discover_dynamic_entries(cls, client) -> List[Dict]:
        """Return (stream_name, schema, key_properties) for each event type that has data."""
        from tap_qualtrics.schema import infer_schema

        discovery_start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        discovery_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        resp = client.get("audit-events")
        event_types = (resp.get("result") or {}).get("elements", [])

        @backoff.on_exception(backoff.expo, QualtricsBackoffError, max_tries=5, jitter=backoff.full_jitter)
        def _fetch_records(event_name):
            """Run one full export cycle; QualtricsBackoffError triggers exponential retry."""
            body = {"eventName": event_name, "startDate": discovery_start, "endDate": discovery_end}
            start = client.post("audit-exports", body)
            export_id = (start.get("result") or {}).get("id", "")
            if not export_id:
                return []
            final = client.poll_export(f"audit-exports/{export_id}")
            file_id = (final.get("result") or {}).get("fileId", export_id)
            resp_file = client.get_file(f"audit-exports/{export_id}/files/{file_id}")
            if not resp_file.content:
                return []
            try:
                return [json.loads(l) for l in resp_file.content.splitlines() if l.strip()]
            except (json.JSONDecodeError, ValueError):
                try:
                    raw = resp_file.json()
                    return raw if isinstance(raw, list) else raw.get("events", [])
                except Exception:
                    return []

        entries = []
        for event_type in event_types:
            event_name = event_type.get("name") if isinstance(event_type, dict) else event_type
            if not event_name:
                continue

            try:
                records = _fetch_records(event_name)
            except QualtricsBadRequestError:
                LOGGER.warning("Skipping '%s' from catalog: event type not supported by the API.", event_name)
                continue
            except QualtricsBackoffError:
                LOGGER.warning("Skipping '%s' from catalog: still rate limited after retries.", event_name)
                continue

            if not records:
                LOGGER.info("Skipping '%s' from catalog: no data available in the discovery window.", event_name)
                continue

            schema = infer_schema(records)
            entries.append((f"audit_export__{event_name}", schema, cls.key_properties))
            LOGGER.info("Discovered schema for audit_export__%s (%d sample records).", event_name, len(records))

        return entries


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

        # # skip all event types not matching the filter when set in config
        # event_filter = self.client.config.get("audit_event_filter")
        # if event_filter and event_name != event_filter:
        #     return

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
        event_name = (parent_id or {}).get("name") if isinstance(parent_id, dict) else parent_id
        if not event_name:
            return 0

        dynamic_id = f"{self.tap_stream_id}__{event_name}"

        # Only sync event types that were discovered and selected in the catalog.
        catalog_entry = self.catalog.get_stream(dynamic_id) if self.catalog else None
        if not catalog_entry:
            LOGGER.debug("Skipping %s: not in catalog (no data in discovery window).", dynamic_id)
            return 0
        if not metadata.get(metadata.to_map(catalog_entry.metadata), (), "selected"):
            LOGGER.debug("Skipping %s: not selected.", dynamic_id)
            return 0

        schema = catalog_entry.schema.to_dict()
        mdata = metadata.to_map(catalog_entry.metadata)
        write_schema(dynamic_id, schema, self.key_properties)

        bookmark = get_bookmark(state, dynamic_id, self.replication_keys[0], self.client.start_date)
        max_bk = bookmark
        with metrics.record_counter(dynamic_id) as counter:
            for record in self.get_records(parent_id=parent_id, start_date=bookmark):
                transformed = transformer.transform(record, schema, mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    write_record(dynamic_id, transformed)
                    counter.increment()
                    if record_bk > max_bk:
                        max_bk = record_bk
        state = write_bookmark(state, dynamic_id, self.replication_keys[0], max_bk)
        return counter.value
