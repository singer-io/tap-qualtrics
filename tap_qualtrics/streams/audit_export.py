import io
import json
import hashlib
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Iterator

import backoff
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)

from tap_qualtrics.client import Client
from tap_qualtrics.exceptions import (
    QualtricsBackoffError,
    QualtricsBadRequestError,
    QualtricsError,
)
from tap_qualtrics.streams.abstracts import IncrementalStream

LOGGER = get_logger()
DISCOVERY_MAX_WORKERS = 4
DISCOVERY_SAMPLE_RECORD_LIMIT = 50


def _canonical_json(value: Any) -> str:
    """Return deterministic JSON for hashing, tolerating non-serializable values."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _ensure_record_id(record: Dict) -> Dict:
    """Guarantee a stable string id for Singer key_properties compatibility."""
    if record.get("id"):
        return record
    digest = hashlib.sha256(_canonical_json(record).encode("utf-8")).hexdigest()
    record["id"] = digest
    return record


class AuditExport(IncrementalStream):
    tap_stream_id = "audit_export"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["timestamp"]
    data_key = ""
    parent = "audit_export_event_types"
    dynamic_schema = True

    @classmethod
    def discover_dynamic_entries(cls, client):  # pylint: disable=too-many-locals
        """Return ((stream_name, schema, key_properties)[], skipped_names[]) for each event type."""
        from tap_qualtrics.schema import infer_schema  # pylint: disable=import-outside-toplevel

        def _worker_client():
            if isinstance(client, Client):
                return client.fork()
            return client

        try:
            resp = client.get("audit-events")
        except QualtricsError as exc:
            LOGGER.warning("Cannot list audit event types during discovery: %s", exc)
            return [], []
        event_types = (resp.get("result") or {}).get("elements", [])
        unique_event_names = []
        seen = set()
        for event_type in event_types:
            event_name = event_type.get("name") if isinstance(event_type, dict) else event_type
            if not event_name or event_name in seen:
                continue
            seen.add(event_name)
            unique_event_names.append(event_name)

        discovery_start = client.start_date
        discovery_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Qualtrics has no documented cancel/delete endpoint for these export jobs.
        # Discovery therefore creates provider-managed jobs solely to infer
        # per-event schemas. This side effect is unavoidable provider behavior.
        # Worker concurrency is capped and local schema inference uses a bounded sample.
        @backoff.on_exception(
            backoff.expo,
            QualtricsBackoffError,
            max_tries=5,
            jitter=backoff.full_jitter,
        )
        def _fetch_records(worker_client, event_name):
            """Fetch all records in a single request; retries on rate limit."""
            body = {
                "eventName": event_name,
                "startDate": discovery_start,
                "endDate": discovery_end,
            }
            start = worker_client.post("audit-exports", body)
            export_id = (start.get("result") or {}).get("id", "")
            if not export_id:
                return []
            final = worker_client.poll_export(f"audit-exports/{export_id}")
            file_id = (final.get("result") or {}).get("fileId", export_id)
            resp_file = worker_client.get_file(f"audit-exports/{export_id}/files/{file_id}")
            if not resp_file.content:
                return []
            try:
                records = [json.loads(l) for l in resp_file.content.splitlines() if l.strip()]
                return records[:DISCOVERY_SAMPLE_RECORD_LIMIT]
            except (json.JSONDecodeError, ValueError):
                try:
                    raw = resp_file.json()
                    records = raw if isinstance(raw, list) else raw.get("events", [])
                    return records[:DISCOVERY_SAMPLE_RECORD_LIMIT]
                except Exception:  # pylint: disable=broad-exception-caught
                    return []

        def _discover_event(event_name):
            try:
                records = _fetch_records(_worker_client(), event_name)
            except QualtricsBackoffError:
                return (event_name, "rate_limited", None)
            except QualtricsError as exc:
                return (event_name, "error", exc)
            if not records:
                return (event_name, "empty", None)

            records = [_ensure_record_id(record or {}) for record in records]
            schema = infer_schema(records)
            schema["properties"]["event_type"] = {"type": ["null", "string"]}
            schema.setdefault("properties", {}).setdefault(
                "id",
                {"type": ["null", "string"]},
            )
            return (
                event_name,
                "entry",
                (f"audit_export__{event_name}", schema, ["id"], len(records)),
            )

        entries = []
        skipped = []
        with ThreadPoolExecutor(max_workers=min(DISCOVERY_MAX_WORKERS, len(unique_event_names)) or 1) as executor:
            for event_name, status, payload in executor.map(_discover_event, unique_event_names):
                if status == "entry":
                    stream_name, schema, key_props, record_count = payload
                    entries.append((stream_name, schema, key_props))
                    LOGGER.info(
                        "Discovered schema for audit_export__%s (%d sample records).",
                        event_name,
                        record_count,
                    )
                    continue
                if status == "rate_limited":
                    LOGGER.warning(
                        "Skipping '%s' from catalog: discovery retries were exhausted because the Qualtrics API remained rate limited, so there is no reliable schema to expose for this event type.",
                        event_name,
                    )
                elif status == "empty":
                    LOGGER.info(
                        "Skipping '%s' from catalog: the export returned no records in the discovery window, so this event type would create an empty schema with no usable data to sync.",
                        event_name,
                    )
                else:
                    LOGGER.warning(
                        "Skipping '%s' from catalog: export discovery failed with %s. This provider-side discovery job cannot be canceled by the tap.",
                        event_name,
                        payload,
                    )
                skipped.append(event_name)

        return entries, skipped

    # pylint: disable=too-many-branches
    def get_records(
        self,
        parent_id: Any = None,
        bookmark: str = "",
    ) -> Iterator[Dict]:
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
                    record = json.loads(line)
                    record["event_type"] = event_name
                    yield _ensure_record_id(record)
            return
        except (json.JSONDecodeError, ValueError):
            pass
        try:
            records = resp.json()
        except Exception:  # pylint: disable=broad-exception-caught
            records = []
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zip_file:
                for name in zip_file.namelist():
                    records.extend(json.loads(zip_file.read(name)))
        if isinstance(records, list):
            for record in records:
                record["event_type"] = event_name
                _ensure_record_id(record)
            yield from records
        elif isinstance(records, dict):
            for record in records.get("events", [records]):
                record["event_type"] = event_name
                yield _ensure_record_id(record)

    def sync(  # pylint: disable=too-many-branches
        self,
        state: Dict,
        transformer: Transformer,
        parent_id: Any = None,
    ) -> int:
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

        bookmark = get_bookmark(
            state,
            dynamic_id,
            self.replication_keys[0],
            self.client.start_date,
        )
        max_bk = bookmark
        with metrics.record_counter(dynamic_id) as counter:
            for record in self.get_records(parent_id=parent_id, bookmark=bookmark):
                transformed = transformer.transform(record, schema, mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    write_record(dynamic_id, transformed)
                    counter.increment()
                    max_bk = max(max_bk, record_bk)
            state = write_bookmark(state, dynamic_id, self.replication_keys[0], max_bk)
            return counter.value
