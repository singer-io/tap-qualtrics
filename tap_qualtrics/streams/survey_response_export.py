import json
from typing import Any, Dict, Iterator

from singer import Transformer, metrics, write_record

from tap_qualtrics.streams.abstracts import FullTableStream
from singer import get_logger
LOGGER = get_logger()

class SurveyResponseExport(FullTableStream):
    tap_stream_id = "survey_response_export"
    key_properties = ["responseId"]
    replication_method = "FULL_TABLE"
    data_key = "responses"
    parent = "surveys"

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        body = {
            "startDate": self.client.start_date,
            "format": "json",
            "compress": False,
            "limit": 50000,
            "sortByLastModifiedDate": True,
        }
        LOGGER.info(f"Starting export for survey {survey_id}")
        start = self.client.post(f"surveys/{survey_id}/export-responses", body)
        export_id = (start.get("result") or {}).get("progressId", "")
        if not export_id:
            return

        final = self.client.poll_export(f"surveys/{survey_id}/export-responses/{export_id}")
        file_id = (final.get("result") or {}).get("fileId", "")
        if not file_id:
            return

        resp = self.client.get_file(f"surveys/{survey_id}/export-responses/{file_id}/file")
        LOGGER.info("File response status: %s, content-type: %s, size: %d bytes",
                    resp.status_code, resp.headers.get("Content-Type"), len(resp.content))
        LOGGER.info("File response preview: %s", resp.content[:500])
        data = json.loads(resp.content)
        for response in data.get("responses", []):
            response["survey_id"] = survey_id
            yield response

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id):
                transformed = transformer.transform(record, self.schema, self.mdata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
        return counter.value
