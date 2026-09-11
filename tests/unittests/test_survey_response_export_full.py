"""Unit tests for tap_qualtrics/streams/survey_response_export.py."""
import json
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.streams.survey_response_export import SurveyResponseExport
from tap_qualtrics.exceptions import QualtricsBackoffError, QualtricsError


def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry(schema=None):
    entry = MagicMock()
    entry.schema.to_dict.return_value = schema or {
        "type": "object",
        "properties": {"responseId": {"type": "string"}, "recorded_date": {"type": "string"}},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


def _make_catalog(stream_name, selected=True):
    catalog = MagicMock()
    entry = _make_entry()
    from singer import metadata as md
    entry.metadata = md.to_list(md.write(md.new(), (), "selected", selected))
    catalog.get_stream.return_value = entry
    return catalog


# ---------------------------------------------------------------------------
# get_records
# ---------------------------------------------------------------------------

class TestSurveyResponseExportGetRecords(unittest.TestCase):

    def _stream(self):
        return SurveyResponseExport(client=_make_client(), catalog_entry=_make_entry())

    def test_no_survey_id_yields_nothing(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_no_export_id_yields_nothing(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {}}
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(records, [])
        stream.client.poll_export.assert_not_called()

    def test_no_file_id_yields_nothing(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"progressId": "pid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete"}}  # no fileId
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(records, [])

    def test_full_flow_with_responses(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"progressId": "pid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        responses = [{"responseId": "R_1", "values": {"recordedDate": "2021-01-01"}}]
        file_resp.content = json.dumps({"responses": responses}).encode()
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["survey_id"], "SV_1")
        self.assertEqual(records[0]["recorded_date"], "2021-01-01")

    def test_survey_id_from_string_via_poll(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"progressId": "pid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": []}).encode()
        stream.client.get_file.return_value = file_resp
        records = list(stream.get_records(parent_id={"id": "SV_abc"}))
        self.assertEqual(records, [])

    def test_bookmark_passed_to_post(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"progressId": "pid1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": []}).encode()
        stream.client.get_file.return_value = file_resp

        list(stream.get_records(parent_id={"id": "SV_1"}, bookmark="2021-06-01"))
        call_body = stream.client.post.call_args[0][1]
        self.assertEqual(call_body["startDate"], "2021-06-01")


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

class TestSurveyResponseExportSync(unittest.TestCase):

    def _stream(self, catalog=None, selected=True):
        stream = SurveyResponseExport(client=_make_client(), catalog_entry=_make_entry())
        stream.catalog = catalog or _make_catalog("survey_response_export__SV_1", selected=selected)
        return stream

    def test_no_survey_id_returns_zero(self):
        stream = self._stream()
        count = stream.sync(state={}, transformer=MagicMock(), parent_id=None)
        self.assertEqual(count, 0)

    def test_catalog_entry_missing_returns_zero(self):
        stream = self._stream()
        stream.catalog.get_stream.return_value = None
        count = stream.sync(state={}, transformer=MagicMock(), parent_id={"id": "SV_1"})
        self.assertEqual(count, 0)

    def test_not_selected_returns_zero(self):
        stream = self._stream(selected=False)
        count = stream.sync(state={}, transformer=MagicMock(), parent_id={"id": "SV_1"})
        self.assertEqual(count, 0)

    @patch("tap_qualtrics.streams.survey_response_export.write_record")
    @patch("tap_qualtrics.streams.survey_response_export.write_schema")
    def test_sync_writes_records_above_bookmark(self, mock_ws, mock_wr):
        stream = self._stream()
        records = [{"responseId": "R_1", "recorded_date": "2021-06-01"}]

        with patch("tap_qualtrics.streams.survey_response_export.get_bookmark", return_value="2021-01-01"):
            with patch("singer.write_bookmark") as mock_wb:
                with patch.object(stream, "get_records", return_value=iter(records)):
                    transformer = MagicMock()
                    transformer.transform.side_effect = lambda r, *a, **kw: r
                    stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})

        mock_wr.assert_called_once()

    @patch("tap_qualtrics.streams.survey_response_export.write_record")
    @patch("tap_qualtrics.streams.survey_response_export.write_schema")
    def test_sync_skips_old_records(self, mock_ws, mock_wr):
        stream = self._stream()
        records = [{"responseId": "R_1", "recorded_date": "2020-01-01"}]

        with patch("tap_qualtrics.streams.survey_response_export.get_bookmark", return_value="2021-06-01"):
            with patch("singer.write_bookmark"):
                with patch.object(stream, "get_records", return_value=iter(records)):
                    transformer = MagicMock()
                    transformer.transform.side_effect = lambda r, *a, **kw: r
                    stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})

        mock_wr.assert_not_called()


# ---------------------------------------------------------------------------
# discover_dynamic_entries
# ---------------------------------------------------------------------------

class TestSurveyResponseExportDiscover(unittest.TestCase):

    @patch("tap_qualtrics.streams.survey_response_export.ThreadPoolExecutor")
    def test_discovery_uses_thread_pool(self, mock_executor):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": [{"responseId": "R_1", "values": {"recordedDate": "2021-01-01"}}]}).encode()
        client.get_file.return_value = file_resp
        executor = MagicMock()
        executor.__enter__.return_value = executor
        executor.__exit__.return_value = False
        executor.map.return_value = [("SV_1", "entry", ("survey_response_export__SV_1", {"type": "object", "properties": {}}, ["responseId"], 1))]
        mock_executor.return_value = executor

        entries, skipped = SurveyResponseExport.discover_dynamic_entries(client)

        self.assertEqual(len(entries), 1)
        self.assertEqual(skipped, [])
        mock_executor.assert_called_once()
        executor.map.assert_called_once()

    def test_get_error_returns_empty(self):
        client = _make_client()
        client.get.side_effect = QualtricsError("error")
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_deduplicates_survey_jobs_within_discovery(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}, {"id": "SV_1"}, {"id": "SV_2"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": [{"responseId": "R_1", "values": {"recordedDate": "2021-01-01"}}]}).encode()
        client.get_file.return_value = file_resp

        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)

        self.assertEqual(len(entries), 2)
        self.assertEqual(client.post.call_count, 2)

    @patch("tap_qualtrics.streams.survey_response_export.ThreadPoolExecutor")
    def test_worker_count_is_capped(self, mock_executor):
        client = _make_client()
        client.get.return_value = {
            "result": {
                "elements": [{"id": f"SV_{idx}"} for idx in range(10)]
            }
        }
        executor = MagicMock()
        executor.__enter__.return_value = executor
        executor.__exit__.return_value = False
        executor.map.return_value = []
        mock_executor.return_value = executor

        SurveyResponseExport.discover_dynamic_entries(client)

        self.assertEqual(mock_executor.call_args.kwargs["max_workers"], 4)

    def test_qualtrics_error_is_skipped(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.side_effect = QualtricsError("known api error")

        entries, skipped = SurveyResponseExport.discover_dynamic_entries(client)

        self.assertEqual(entries, [])
        self.assertEqual(skipped, ["SV_1"])

    def test_no_surveys_returns_empty(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": []}}
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_no_records_skips_survey(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": []}).encode()
        client.get_file.return_value = file_resp
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_backoff_error_skips_survey(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.side_effect = QualtricsBackoffError("rate limited")
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_successful_discovery(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        responses = [{"responseId": "R_1", "values": {"recordedDate": "2021-01-01"}}]
        file_resp = MagicMock()
        file_resp.content = json.dumps({"responses": responses}).encode()
        client.get_file.return_value = file_resp

        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(len(entries), 1)
        stream_name, schema, key_props = entries[0]
        self.assertEqual(stream_name, "survey_response_export__SV_1")

    def test_no_export_id_in_discover_skips(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {}}  # no progressId
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        client.poll_export.assert_not_called()

    def test_no_file_id_in_discover_skips(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete"}}  # no fileId
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_json_parse_error_in_discover_skips(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "fid1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-valid-json"
        client.get_file.return_value = file_resp
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_generic_exception_fails_discovery(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
        client.post.side_effect = Exception("unexpected")
        with self.assertRaises(Exception):
            SurveyResponseExport.discover_dynamic_entries(client)

    def test_survey_with_no_id_skipped(self):
        client = _make_client()
        # One survey with empty id (hits `if not survey_id: continue` branch)
        client.get.return_value = {"result": {"elements": [{"id": ""}]}}
        entries, _ = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        client.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
