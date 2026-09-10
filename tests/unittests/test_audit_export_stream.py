"""Unit tests for tap_qualtrics/streams/audit_export.py."""
import io
import json
import zipfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

from tap_qualtrics.streams.audit_export import AuditExport
from tap_qualtrics.exceptions import QualtricsBadRequestError, QualtricsBackoffError, QualtricsError


def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry(schema=None):
    entry = MagicMock()
    entry.schema.to_dict.return_value = schema or {
        "type": "object",
        "properties": {"id": {"type": "string"}, "timestamp": {"type": "string"}},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


def _make_catalog(stream_name, selected=True):
    catalog = MagicMock()
    entry = _make_entry()
    from singer import metadata as md
    entry.metadata = md.to_list(
        md.write(md.new(), (), "selected", selected)
    )
    catalog.get_stream.return_value = entry
    return catalog


# ---------------------------------------------------------------------------
# get_records
# ---------------------------------------------------------------------------

class TestAuditExportGetRecords(unittest.TestCase):

    def _stream(self):
        return AuditExport(client=_make_client(), catalog_entry=_make_entry())

    def test_no_event_name_yields_nothing(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_ndjson_response(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b'{"id": "e1", "timestamp": "2021-01-01"}\n{"id": "e2", "timestamp": "2021-01-02"}\n'
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id={"name": "login"}))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], "e1")

    def test_empty_content_skipped(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b""
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id={"name": "login"}))

        self.assertEqual(records, [])

    def test_bad_request_returns_empty(self):
        stream = self._stream()
        stream.client.post.side_effect = QualtricsBadRequestError("bad request")

        records = list(stream.get_records(parent_id="login"))

        self.assertEqual(stream.client.post.call_count, 1)
        self.assertEqual(records, [])

    def test_no_export_id_returns_empty(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {}}  # no id

        records = list(stream.get_records(parent_id="login"))

        self.assertEqual(records, [])
        stream.client.poll_export.assert_not_called()

    def test_json_list_fallback(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-json"
        file_resp.json.return_value = [{"id": "e1", "timestamp": "2021-01-01"}]
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id="login"))

        self.assertEqual(len(records), 1)

    def test_dict_response_uses_events_key(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-json"
        file_resp.json.return_value = {"events": [{"id": "e1"}]}
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id="login"))

        self.assertEqual(len(records), 1)

    def test_zip_fallback(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("events.json", json.dumps([{"id": "e1"}]))
        buf.seek(0)

        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = buf.read()
        file_resp.json.side_effect = Exception("not json")
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id="login"))

        self.assertEqual(len(records), 1)

    def test_ndjson_missing_id_gets_hashed_id(self):
        stream = self._stream()
        stream.client.post.return_value = {"result": {"id": "exp1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b'{"timestamp": "2021-01-01", "actor": {"id": "u1"}}\n'
        stream.client.get_file.return_value = file_resp

        records = list(stream.get_records(parent_id={"name": "login"}))

        self.assertEqual(len(records), 1)
        self.assertIn("id", records[0])
        self.assertTrue(records[0]["id"])


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

class TestAuditExportSync(unittest.TestCase):

    def _stream(self, catalog=None, selected=True):
        stream = AuditExport(client=_make_client(), catalog_entry=_make_entry())
        stream.catalog = catalog or _make_catalog("audit_export__login", selected=selected)
        return stream

    def test_no_event_name_returns_zero(self):
        stream = self._stream()
        count = stream.sync(state={}, transformer=MagicMock(), parent_id=None)
        self.assertEqual(count, 0)

    def test_catalog_entry_missing_skips(self):
        stream = self._stream()
        stream.catalog.get_stream.return_value = None
        count = stream.sync(state={}, transformer=MagicMock(), parent_id="login")
        self.assertEqual(count, 0)

    def test_not_selected_skips(self):
        stream = self._stream(selected=False)
        count = stream.sync(state={}, transformer=MagicMock(), parent_id="login")
        self.assertEqual(count, 0)

    @patch("tap_qualtrics.streams.audit_export.write_record")
    @patch("tap_qualtrics.streams.audit_export.write_schema")
    @patch("tap_qualtrics.streams.audit_export.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.audit_export.write_bookmark")
    def test_sync_writes_records(self, mock_wb, mock_bk, mock_ws, mock_wr):
        stream = self._stream()
        records = [{'id': 'e1', 'timestamp': '2021-06-01'}]
        with patch.object(stream, "get_records", return_value=iter(records)):
            transformer = MagicMock()
            transformer.transform.side_effect = lambda r, *a, **kw: r
            stream.sync(state={}, transformer=transformer, parent_id="login")

        mock_wr.assert_called_once()
        mock_wb.assert_called_once()

    @patch("tap_qualtrics.streams.audit_export.write_record")
    @patch("tap_qualtrics.streams.audit_export.write_schema")
    @patch("tap_qualtrics.streams.audit_export.get_bookmark", return_value="2021-06-01")
    @patch("tap_qualtrics.streams.audit_export.write_bookmark")
    def test_sync_skips_old_records(self, mock_wb, mock_bk, mock_ws, mock_wr):
        stream = self._stream()
        records = [{"id": "e1", "timestamp": "2020-01-01"}]
        with patch.object(stream, "get_records", return_value=iter(records)):
            transformer = MagicMock()
            transformer.transform.side_effect = lambda r, *a, **kw: r
            count = stream.sync(state={}, transformer=transformer, parent_id="login")

        self.assertEqual(count, 0)
        mock_wr.assert_not_called()


# ---------------------------------------------------------------------------
# discover_dynamic_entries
# ---------------------------------------------------------------------------

class TestAuditExportDiscoverDynamicEntries(unittest.TestCase):

    def test_no_event_types_returns_empty(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": []}}
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_get_error_returns_empty(self):
        client = _make_client()
        client.get.side_effect = QualtricsError("error")
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_bad_request_event_type_raises(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.side_effect = QualtricsBadRequestError("not supported")
        with self.assertRaises(QualtricsBadRequestError):
            AuditExport.discover_dynamic_entries(client)

    def test_no_records_skips_event_type(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b""
        client.get_file.return_value = file_resp
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_successful_discovery(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b'{"id": "e1", "timestamp": "2021-01-01"}\n'
        client.get_file.return_value = file_resp

        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(len(entries), 1)
        stream_name, schema, key_props = entries[0]
        self.assertEqual(stream_name, "audit_export__login")
        self.assertIn("id", schema["properties"])
        self.assertEqual(key_props, ["id"])

    def test_successful_discovery_without_id_still_sets_key_props(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b'{"timestamp": "2021-01-01", "actor": {"id": "u1"}}\n'
        client.get_file.return_value = file_resp

        entries, _ = AuditExport.discover_dynamic_entries(client)

        self.assertEqual(len(entries), 1)
        _, schema, key_props = entries[0]
        self.assertEqual(key_props, ["id"])
        self.assertIn("id", schema["properties"])

    def test_no_export_id_in_discover_skips(self):
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {}}  # no id returned
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        client.poll_export.assert_not_called()

    def test_ndjson_parse_fails_json_list_fallback(self):
        """Lines 56-59: NDJSON fails, resp.json() returns a list."""
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-ndjson"
        file_resp.json.return_value = [{"id": "e1"}]
        client.get_file.return_value = file_resp
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(len(entries), 1)

    def test_ndjson_parse_fails_json_dict_fallback(self):
        """Lines 56-59: NDJSON fails, resp.json() returns a dict with events key."""
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-ndjson"
        file_resp.json.return_value = {"events": [{"id": "e1"}]}
        client.get_file.return_value = file_resp
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(len(entries), 1)

    def test_all_json_fallbacks_fail_returns_empty(self):
        """Lines 60-61: both NDJSON and json() fallback fail."""
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        file_resp = MagicMock()
        file_resp.content = b"not-ndjson"
        file_resp.json.side_effect = Exception("bad json")
        client.get_file.return_value = file_resp
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])

    def test_event_type_with_no_name_skipped(self):
        """Line 67: event_name is falsy → continue."""
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": ""}]}}
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        client.post.assert_not_called()

    def test_backoff_error_skips_event_type(self):
        """Lines 74-76: QualtricsBackoffError from _fetch_records → skip."""
        from tap_qualtrics.exceptions import QualtricsBackoffError
        client = _make_client()
        client.get.return_value = {"result": {"elements": [{"name": "login"}]}}
        client.post.side_effect = QualtricsBackoffError("rate limited")
        entries, _ = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])


if __name__ == "__main__":
    unittest.main()
