import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Iterator

import backoff

from singer import Transformer, get_bookmark, get_logger, metadata, metrics, write_bookmark, write_record, write_schema

from tap_qualtrics.exceptions import QualtricsBadRequestError, QualtricsBackoffError, QualtricsError
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
    def discover_dynamic_entries(cls, client):
        """Return ((stream_name, schema, key_properties)[], skipped_names[]) for each event type."""
        from tap_qualtrics.schema import infer_schema  # pylint: disable=import-outside-toplevel

        try:
            resp = client.get("audit-events")
        except QualtricsError as exc:
            LOGGER.warning("Cannot list audit event types during discovery: %s", exc)
            return [], []
        event_types = (resp.get("result") or {}).get("elements", [])

        discovery_start = client.start_date
        discovery_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        @backoff.on_exception(backoff.expo, QualtricsBackoffError, max_tries=5, jitter=backoff.full_jitter)
        def _fetch_records(event_name):
            """Fetch all records in a single request; retries on rate limit."""
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
                except Exception:  # pylint: disable=broad-exception-caught
                    return []

        entries = []
        skipped = []
        for event_type in event_types:
            event_name = event_type.get("name") if isinstance(event_type, dict) else event_type
            if not event_name:
                continue

            try:
                records = _fetch_records(event_name)
            except QualtricsBadRequestError:
                LOGGER.warning("Skipping '%s' from catalog: event type not supported by the API.", event_name)
                skipped.append(event_name)
                continue
            except QualtricsBackoffError:
                LOGGER.warning("Skipping '%s' from catalog: still rate limited after retries.", event_name)
                skipped.append(event_name)
                continue

            if not records:
                LOGGER.info("Skipping '%s' from catalog: no data found in discovery window.", event_name)
                skipped.append(event_name)
                continue

            schema = infer_schema(records)
            entries.append((f"audit_export__{event_name}", schema, cls.key_properties))
            LOGGER.info("Discovered schema for audit_export__%s (%d sample records).", event_name, len(records))

        return entries, skipped


    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        event_name = (parent_id or {}).get("name") if isinstance(parent_id, dict) else parent_id
        if not event_name:
            return

        start_date = bookmark or self.client.start_date
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        body = {"eventName": event_name, "startDate": start_date, "endDate": end_date}
        try:
            start = self.client.post("audit-exports", body)
        except QualtricsBadRequestError:
            LOGGER.warning("Skipping unsupported audit export eventName: %s", event_name)
            return

        # API returns 'id', not 'exportId'
        export_id = (start.get("result") or {}).get("id", "")
        if not export_id:
            return

        final = self.client.poll_export(f"audit-exports/{export_id}")
        file_id = (final.get("result") or {}).get("fileId", export_id)

        resp = self.client.get_file(f"audit-exports/{export_id}/files/{file_id}")
        if not resp.content:
            return
        try:
            # File is NDJSON: one JSON object per line
            for line in resp.content.splitlines():
                line = line.strip()
                if line:
                    yield json.loads(line)
            return
        except (json.JSONDecodeError, ValueError):
            pass
        try:
            records = resp.json()
        except Exception:  # pylint: disable=broad-exception-caught
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
            for record in self.get_records(parent_id=parent_id, bookmark=bookmark):
                transformed = transformer.transform(record, schema, mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    write_record(dynamic_id, transformed)
                    counter.increment()
                    if record_bk > max_bk:
                        max_bk = record_bk
            state = write_bookmark(state, dynamic_id, self.replication_keys[0], max_bk)
            return counter.value
