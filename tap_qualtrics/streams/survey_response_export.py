import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

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
    QualtricsBadGatewayError,
    QualtricsError,
    QualtricsForbiddenError,
    QualtricsPaginationError,
    QualtricsRateLimitError,
)
from tap_qualtrics.streams.abstracts import IncrementalStream

LOGGER = get_logger()
DISCOVERY_MAX_WORKERS = 4
DISCOVERY_EXPORT_LIMIT = 50
# Safety cap used only to convert looping/non-terminating pagination into an explicit failure.
DISCOVERY_MAX_SURVEY_PAGES = 100_000


def _reported_total_from_export_status(status_response: dict) -> int | None:
    """Best-effort extraction of API-reported record totals from export status payloads."""
    result = (status_response or {}).get("result") or {}
    for key in ("recordCount", "totalResponses", "totalCount", "count"):
        value = result.get(key)
        if isinstance(value, int) and value >= 0:
            return value
    return None

class SurveyResponseExport(IncrementalStream):
    tap_stream_id = "survey_response_export"
    key_properties: ClassVar = ["responseId"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["recorded_date"]
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

        # Discovery must follow all survey pages and fail loudly on truncation or malformed pagination state.
        page_size = getattr(client, "page_size", 100)
        surveys = []
        next_url = None
        seen_urls = set()
        page_count = 0
        has_more_pages = True
        while has_more_pages:
            if next_url is not None and next_url in seen_urls:
                raise QualtricsPaginationError(
                    f"GET /surveys returned a repeated nextPage URL ({next_url}) during discovery "
                    f"after {page_count} page(s); refusing to loop forever or silently return a partial "
                    "survey listing."
                )
            if page_count >= DISCOVERY_MAX_SURVEY_PAGES:
                raise QualtricsPaginationError(
                    "GET /surveys did not finish paginating during discovery after the safety "
                    f"cap of {DISCOVERY_MAX_SURVEY_PAGES} pages; refusing to silently return a partial survey listing. "
                    "This likely indicates the API is not terminating pagination as expected."
                )
            if next_url is not None:
                seen_urls.add(next_url)
            try:
                if next_url:
                    surveys_resp = client.get("surveys", full_url=next_url)
                else:
                    surveys_resp = client.get("surveys", params={"pageSize": page_size})
            except QualtricsForbiddenError as exc:
                if page_count == 0:
                    LOGGER.warning("Cannot list surveys during discovery: %s", exc)
                    return [], []
                raise QualtricsPaginationError(
                    "GET /surveys returned forbidden access while paginating during discovery "
                    f"after {page_count} page(s); refusing to continue with a partial survey listing: {exc}"
                )
            page_count += 1
            result = surveys_resp.get("result") or {}
            surveys.extend(result.get("elements") or [])
            next_url = result.get("nextPage")
            if next_url is not None and not isinstance(next_url, str):
                raise QualtricsPaginationError(
                    f"GET /surveys returned invalid nextPage type during discovery on page {page_count}: "
                    f"expected string or null, got {type(next_url).__name__}."
                )
            has_more_pages = bool(next_url)

        unique_survey_ids = []
        seen = set()
        for survey in surveys:
            survey_id = survey.get("id") if isinstance(survey, dict) else survey
            if not survey_id or survey_id in seen:
                continue
            seen.add(survey_id)
            unique_survey_ids.append(survey_id)

        # Qualtrics has no documented cancel/delete endpoint for these export jobs.
        # Discovery therefore creates provider-managed jobs solely to infer
        # per-survey schemas. This side effect is unavoidable provider behavior.
        # The probe is bounded to a small sample and worker concurrency is capped.
        @backoff.on_exception(
            backoff.expo,
            (QualtricsBackoffError, QualtricsBadGatewayError),
            max_tries=5,
            jitter=backoff.full_jitter,
        )
        def _fetch_records(worker_client, survey_id):
            """Run one full export cycle; QualtricsBackoffError triggers exponential retry."""
            body = {
                "startDate": client.start_date,
                "format": "json",
                "compress": False,
                "limit": DISCOVERY_EXPORT_LIMIT,
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

        def _discover_survey(survey_id):
            try:
                records = _fetch_records(_worker_client(), survey_id)
            except QualtricsRateLimitError:
                return (survey_id, "rate_limited", None)
            except QualtricsError as exc:
                return (survey_id, "error", exc)
            # Without sample records, we cannot infer a dynamic schema for this survey.
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
        with ThreadPoolExecutor(max_workers=min(DISCOVERY_MAX_WORKERS, len(unique_survey_ids)) or 1) as executor:
            for survey_id, status, payload in executor.map(_discover_survey, unique_survey_ids):
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
    ) -> Iterator[dict]:
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
        api_total = _reported_total_from_export_status(final)
        file_id = (final.get("result") or {}).get("fileId", "")
        if not file_id:
            return

        resp = self.client.get_file(f"surveys/{survey_id}/export-responses/{file_id}/file", stream=True)
        emitted_count = 0
        content_type = ((getattr(resp, "headers", {}) or {}).get("Content-Type") or "").lower()

        if "ndjson" in content_type or "jsonl" in content_type:
            for line in resp.iter_lines(decode_unicode=True):
                line = (line or "").strip()
                if not line:
                    continue
                response = json.loads(line)
                response["survey_id"] = survey_id
                response["recorded_date"] = (response.get("values") or {}).get("recordedDate", "")
                emitted_count += 1
                yield response

            if api_total is not None and emitted_count != api_total:
                LOGGER.warning(
                    "Record-count reconciliation mismatch for survey %s: emitted=%d, api_total=%d",
                    survey_id,
                    emitted_count,
                    api_total,
                )
            return

        if not resp.content:
            return
        data = json.loads(resp.content)
        for response in data.get("responses", []):
            response["survey_id"] = survey_id
            response["recorded_date"] = (response.get("values") or {}).get("recordedDate", "")
            emitted_count += 1
            yield response

        if api_total is not None and emitted_count != api_total:
            LOGGER.warning(
                "Record-count reconciliation mismatch for survey %s: emitted=%d, api_total=%d",
                survey_id,
                emitted_count,
                api_total,
            )

    def sync(self, state: dict, transformer: Transformer, parent_id: Any = None) -> int:
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
