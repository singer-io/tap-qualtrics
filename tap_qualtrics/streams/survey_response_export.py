import json
from concurrent.futures import ThreadPoolExecutor
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
from tap_qualtrics.exceptions import QualtricsBackoffError, QualtricsError
from tap_qualtrics.streams.abstracts import IncrementalStream

LOGGER = get_logger()
DISCOVERY_MAX_WORKERS = 4

class SurveyResponseExport(IncrementalStream):
    tap_stream_id = "survey_response_export"
    key_properties = ["responseId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["recorded_date"]
    data_key = "responses"
    parent = "surveys"
    dynamic_schema = True

    @classmethod
    def discover_dynamic_entries(cls, client):  # pylint: disable=too-many-locals
        """Return ((stream_name, schema, key_properties)[], skipped_ids[]) for each survey."""
        from tap_qualtrics.schema import infer_schema  # pylint: disable=import-outside-toplevel

        def _worker_client():
            if isinstance(client, Client):
                return client.fork()
            return client

        try:
            surveys_resp = client.get("surveys")
        except QualtricsError as exc:
            LOGGER.warning("Cannot list surveys during discovery: %s", exc)
            return [], []
        surveys = (surveys_resp.get("result") or {}).get("elements", [])

        # Discovery intentionally starts a temporary export job for each survey so
        # we can inspect one payload and infer the dynamic schema. This is a
        # discovery-only probe, not a regular sync run; if Qualtrics exposes a
        # documented cleanup/cancel API for these temporary exports, they should be
        # deleted immediately after schema inference to avoid quota churn.
        @backoff.on_exception(
            backoff.expo,
            QualtricsBackoffError,
            max_tries=5,
            jitter=backoff.full_jitter,
        )
        def _fetch_records(worker_client, survey_id):
            """Run one full export cycle; QualtricsBackoffError triggers exponential retry."""
            body = {
                "startDate": client.start_date,
                "format": "json",
                "compress": False,
                "limit": 50,
                "sortByLastModifiedDate": True,
            }
            start = worker_client.post(f"surveys/{survey_id}/export-responses", body)
            export_id = (start.get("result") or {}).get("progressId", "")
            if not export_id:
                return []
            final = worker_client.poll_export(f"surveys/{survey_id}/export-responses/{export_id}")
            file_id = (final.get("result") or {}).get("fileId", "")
            if not file_id:
                return []
            resp = worker_client.get_file(f"surveys/{survey_id}/export-responses/{file_id}/file")
            try:
                data = json.loads(resp.content)
                return data.get("responses", [])
            except (json.JSONDecodeError, ValueError):
                return []

        def _discover_survey(survey):
            survey_id = survey.get("id") if isinstance(survey, dict) else survey
            if not survey_id:
                return (None, "skip", None)
            try:
                records = _fetch_records(_worker_client(), survey_id)
            except QualtricsBackoffError:
                return (survey_id, "rate_limited", None)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return (survey_id, "error", exc)
            if not records:
                return (survey_id, "empty", None)

            for record in records:
                record["survey_id"] = survey_id
                record["recorded_date"] = (record.get("values") or {}).get("recordedDate", "")

            schema = infer_schema(records)
            return (
                survey_id,
                "entry",
                (f"survey_response_export__{survey_id}", schema, cls.key_properties, len(records)),
            )

        entries = []
        skipped = []
        with ThreadPoolExecutor(max_workers=min(DISCOVERY_MAX_WORKERS, len(surveys)) or 1) as executor:
            for survey_id, status, payload in executor.map(_discover_survey, surveys):
                if status == "skip":
                    continue
                if status == "entry":
                    stream_name, schema, key_properties, record_count = payload
                    entries.append((stream_name, schema, key_properties))
                    LOGGER.info(
                        "Discovered schema for survey_response_export__%s (%d sample records).",
                        survey_id,
                        record_count,
                    )
                    continue
                if status == "rate_limited":
                    LOGGER.warning(
                        "Skipping survey '%s' from catalog: discovery retries were exhausted because the Qualtrics API remained rate limited, so this survey cannot be safely exposed as a syncable export schema.",
                        survey_id,
                    )
                elif status == "empty":
                    LOGGER.info(
                        "Skipping survey '%s' from catalog: the export returned no records in the discovery window, so this survey would create an empty schema with no usable data to sync.",
                        survey_id,
                    )
                else:
                    LOGGER.warning(
                        "Skipping survey '%s' from catalog: export discovery failed with %s, so this survey is not safe to expose as a schema until the underlying API issue is resolved.",
                        survey_id,
                        payload,
                    )
                skipped.append(survey_id)

        return entries, skipped


    def get_records(
        self,
        parent_id: Any = None,
        bookmark: str = "",
    ) -> Iterator[Dict]:
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        body = {
            "startDate": bookmark or self.client.start_date,
            "format": "json",
            "compress": False,
            "limit": 50000,
            "sortByLastModifiedDate": True,
        }
        LOGGER.info("Starting export for survey %s", survey_id)
        start = self.client.post(f"surveys/{survey_id}/export-responses", body)
        export_id = (start.get("result") or {}).get("progressId", "")
        if not export_id:
            return

        final = self.client.poll_export(f"surveys/{survey_id}/export-responses/{export_id}")
        file_id = (final.get("result") or {}).get("fileId", "")
        if not file_id:
            return

        resp = self.client.get_file(f"surveys/{survey_id}/export-responses/{file_id}/file")
        data = json.loads(resp.content)
        for response in data.get("responses", []):
            response["survey_id"] = survey_id
            response["recorded_date"] = (response.get("values") or {}).get("recordedDate", "")
            yield response

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return 0

        dynamic_id = f"{self.tap_stream_id}__{survey_id}"

        # Only sync surveys that were discovered and selected in the catalog.
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
        count = 0

        try:
            with metrics.record_counter(dynamic_id) as counter:
                for record in self.get_records(parent_id, bookmark=bookmark):
                    transformed = transformer.transform(record, schema, mdata)
                    rec_bk = transformed.get(self.replication_keys[0], "")
                    if rec_bk >= bookmark:
                        write_record(dynamic_id, transformed)
                        counter.increment()
                        max_bk = max(max_bk, rec_bk)
                count = counter.value
        except QualtricsError as exc:
            LOGGER.warning("Skipping %s due to API error: %s", dynamic_id, exc)
        finally:
            state = write_bookmark(state, dynamic_id, self.replication_keys[0], max_bk)

        return count
