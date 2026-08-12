import io
import json
import zipfile
from typing import Any, Dict, Iterator

from singer import Transformer, get_bookmark, metrics, write_bookmark, write_record

from tap_qualtrics.streams.abstracts import FullTableStream


class SurveyResponseExport(FullTableStream):
    tap_stream_id = "survey_response_export"
    key_properties = ["responseId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["recordedDate"]
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
        start = self.client.post(f"surveys/{survey_id}/export-responses", body)
        export_id = (start.get("result") or {}).get("progressId", "")
        if not export_id:
            return

        final = self.client.poll_export(f"surveys/{survey_id}/export-responses/{export_id}")
        file_id = (final.get("result") or {}).get("fileId", "")
        if not file_id:
            return

        resp = self.client.get_file(f"surveys/{survey_id}/export-responses/{export_id}/file")
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        for name in zf.namelist():
            data = json.loads(zf.read(name))
            for response in data.get("responses", []):
                response["survey_id"] = survey_id
                yield response

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        bookmark = get_bookmark(
            state, self.tap_stream_id, self.replication_keys[0], self.client.start_date
        )
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id):
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
