"""Unit tests for stream pagination and async export flows."""
import io
import json
import unittest
import zipfile
from unittest.mock import MagicMock, patch, call

from tap_qualtrics.streams.abstracts import FullTableStream, _get_nested
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport as SurveyResponseExportStream
from tap_qualtrics.streams.tickets_export import TicketsExport as TicketsExportStream


# ---------------------------------------------------------------------------
# Helper: build a mock catalog entry
# ---------------------------------------------------------------------------

def _make_catalog_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {"type": "object", "properties": {}, "additionalProperties": True}
    entry.metadata = []
    return entry


# ---------------------------------------------------------------------------
# _get_nested
# ---------------------------------------------------------------------------

class TestGetNested(unittest.TestCase):

    def test_shallow(self):
        assert _get_nested({"a": 1}, "a") == 1

    def test_nested(self):
        assert _get_nested({"result": {"elements": [1, 2]}}, "result.elements") == [1, 2]

    def test_missing_key(self):
        assert _get_nested({"a": 1}, "b") is None

    def test_non_dict_midpath(self):
        assert _get_nested({"a": "str"}, "a.b") is None


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class TestFullTableStreamPagination(unittest.TestCase):

    def _make_stream(self, pages):
        class ConcreteStream(FullTableStream):
            tap_stream_id = "test_stream"
            key_properties = ["id"]
            replication_method = "FULL_TABLE"
            data_key = "result.elements"
            path = "test-resource"
            page_size = 2

        stream = ConcreteStream(client=MagicMock(), catalog_entry=_make_catalog_entry())
        stream.client.get.side_effect = pages
        return stream

    def test_single_page(self):
        page = {"result": {"elements": [{"id": 1}, {"id": 2}], "nextPage": None}}
        stream = self._make_stream([page])
        records = list(stream.get_records())
        assert records == [{"id": 1}, {"id": 2}]

    def test_multi_page(self):
        page1 = {"result": {"elements": [{"id": 1}], "nextPage": "https://q.com/next"}}
        page2 = {"result": {"elements": [{"id": 2}], "nextPage": None}}
        stream = self._make_stream([page1, page2])
        records = list(stream.get_records())
        assert records == [{"id": 1}, {"id": 2}]
        # second call should use full_url
        _, kwargs = stream.client.get.call_args_list[1]
        assert kwargs.get("full_url") == "https://q.com/next"


# ---------------------------------------------------------------------------
# Survey Response Export (POST → poll → GET file)
# ---------------------------------------------------------------------------

class TestSurveyResponseExport(unittest.TestCase):

    def _make_zip(self, responses):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("responses.json", json.dumps({"responses": responses}))
        buf.seek(0)
        return buf

    def test_full_flow(self):
        stream = SurveyResponseExportStream(
            client=MagicMock(), catalog_entry=_make_catalog_entry()
        )
        stream.client.start_date = "2020-01-01"
        stream.client.post.return_value = {"result": {"progressId": "pid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = self._make_zip([{"responseId": "R_1", "recordedDate": "2021-01-01"}]).read()
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id={"id": "SV_123"}))

        assert len(records) == 1
        assert records[0]["responseId"] == "R_1"
        assert records[0]["survey_id"] == "SV_123"
        stream.client.post.assert_called_once()
        stream.client.poll_export.assert_called_once_with("surveys/SV_123/export-responses/pid1")
        stream.client.get_file.assert_called_once()

    def test_no_export_id_returns_empty(self):
        stream = SurveyResponseExportStream(
            client=MagicMock(), catalog_entry=_make_catalog_entry()
        )
        stream.client.start_date = "2020-01-01"
        stream.client.post.return_value = {"result": {}}  # no progressId
        records = list(stream.get_records(parent_id={"id": "SV_123"}))
        assert records == []


# ---------------------------------------------------------------------------
# Tickets Export (POST → poll → GET file)
# ---------------------------------------------------------------------------

class TestTicketsExport(unittest.TestCase):

    def test_full_flow(self):
        stream = TicketsExportStream(
            client=MagicMock(), catalog_entry=_make_catalog_entry()
        )
        stream.client.start_date = "2020-01-01"
        stream.client.post.return_value = {"result": {"exportId": "eid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.json.return_value = [{"ticketId": "T_1", "updatedAt": "2021-01-01"}]
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(start_date="2020-01-01"))

        assert len(records) == 1
        assert records[0]["ticketId"] == "T_1"
        stream.client.poll_export.assert_called_once_with("ticket-exports/eid1/status")


if __name__ == "__main__":
    unittest.main()
