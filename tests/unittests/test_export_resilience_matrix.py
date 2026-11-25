import json
import unittest
import zipfile
from unittest.mock import MagicMock

from parameterized import parameterized

from tap_qualtrics.exceptions import (
    QualtricsBadGatewayError,
    QualtricsError,
    QualtricsRateLimitError,
)
from tap_qualtrics.streams.audit_export import AuditExport
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport


def _make_client():
    client = MagicMock()
    client.page_size = 100
    client.start_date = "2020-01-01T00:00:00Z"
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    return client


def _make_entry(schema=None):
    entry = MagicMock()
    entry.schema.to_dict.return_value = schema or {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


def _build_stream(stream_type):
    if stream_type == "audit_export":
        stream = AuditExport(client=_make_client(), catalog_entry=_make_entry())
        parent = {"name": "login"}
        id_key = "id"
    elif stream_type == "survey_response_export":
        stream = SurveyResponseExport(client=_make_client(), catalog_entry=_make_entry())
        parent = {"id": "SV_1"}
        id_key = "progressId"
    else:
        raise AssertionError(f"unknown stream_type={stream_type}")
    return stream, parent, id_key


class RefreshingExportClient:
    def __init__(self, stream_type):
        self.stream_type = stream_type
        self.page_size = 100
        self.start_date = "2020-01-01T00:00:00Z"
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.refresh_count = 0
        self.poll_calls = 0

    def post(self, _path, _body=None):
        if self.stream_type == "audit_export":
            return {"result": {"id": "exp-1"}}
        return {"result": {"progressId": "prog-1"}}

    def poll_export(self, _path):
        # Simulate token expiry mid-export that gets refreshed inside client layer.
        if self.poll_calls == 0:
            self.refresh_count += 1
        self.poll_calls += 1
        if self.stream_type == "audit_export":
            return {"result": {"status": "complete", "fileId": "exp-1"}}
        return {"result": {"status": "complete", "fileId": "file-1"}}

    def get_file(self, _path, stream=False):  # pylint: disable=unused-argument
        resp = MagicMock()
        resp.headers = {}
        if self.stream_type == "audit_export":
            resp.content = b'{"id":"a1","timestamp":"2026-11-01T00:00:00Z"}\n'
        else:
            payload = {
                "responses": [
                    {
                        "responseId": "R_1",
                        "values": {"recordedDate": "2026-11-01T00:00:00Z"},
                    }
                ]
            }
            resp.content = json.dumps(payload).encode("utf-8")
        return resp


class TestExportResilienceMatrix(unittest.TestCase):

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_429_retry_after_exhaustion_propagates(self, stream_type):
        stream, parent, _ = _build_stream(stream_type)
        stream.client.post.side_effect = QualtricsRateLimitError("429")

        with self.assertRaises(QualtricsRateLimitError):
            list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_5xx_backoff_exhaustion_propagates(self, stream_type):
        stream, parent, _ = _build_stream(stream_type)
        stream.client.post.side_effect = QualtricsBadGatewayError("502")

        with self.assertRaises(QualtricsBadGatewayError):
            list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_export_timeout_or_failed_status_raises(self, stream_type):
        stream, parent, id_key = _build_stream(stream_type)
        stream.client.post.return_value = {"result": {id_key: "exp-or-progress"}}
        stream.client.poll_export.side_effect = QualtricsError("Export did not complete after max attempts")

        with self.assertRaises(QualtricsError):
            list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_empty_file_returns_empty_records(self, stream_type):
        stream, parent, id_key = _build_stream(stream_type)
        stream.client.post.return_value = {"result": {id_key: "id-1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "file-1"}}

        resp = MagicMock()
        resp.content = b""
        stream.client.get_file.return_value = resp

        self.assertEqual(list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z")), [])

    def test_survey_response_export_malformed_or_truncated_json_raises(self):
        stream, parent, _ = _build_stream("survey_response_export")
        stream.client.post.return_value = {"result": {"progressId": "p1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}

        malformed = MagicMock()
        malformed.content = b'{"responses": [ { "responseId": "R_1" '
        stream.client.get_file.return_value = malformed

        with self.assertRaises(json.JSONDecodeError):
            list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

    def test_audit_export_malformed_or_truncated_payload_raises_bad_zip(self):
        stream, parent, _ = _build_stream("audit_export")
        stream.client.post.return_value = {"result": {"id": "e1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1"}}

        malformed = MagicMock()
        malformed.content = b"{bad-json-line"
        malformed.json.side_effect = ValueError("invalid json")
        stream.client.get_file.return_value = malformed

        with self.assertRaises(zipfile.BadZipFile):
            list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_unicode_and_very_large_payload_records_are_processed(self, stream_type):
        stream, parent, id_key = _build_stream(stream_type)
        stream.client.post.return_value = {"result": {id_key: "id-1"}}
        stream.client.poll_export.return_value = {"result": {"status": "complete", "fileId": "file-1"}}

        huge_text = "🚀" * 200000

        response = MagicMock()
        if stream_type == "audit_export":
            response.content = (
                json.dumps(
                    {
                        "id": "A_1",
                        "timestamp": "2026-11-01T00:00:00Z",
                        "message": huge_text,
                        "unicode": "こんにちは-مرحبا",
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                + b"\n"
            )
        else:
            payload = {
                "responses": [
                    {
                        "responseId": "R_1",
                        "values": {
                            "recordedDate": "2026-11-01T00:00:00Z",
                            "comments": huge_text,
                            "unicode": "こんにちは-مرحبا",
                        },
                    }
                ]
            }
            response.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        stream.client.get_file.return_value = response

        records = list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))
        self.assertEqual(len(records), 1)
        if stream_type == "audit_export":
            self.assertEqual(records[0]["unicode"], "こんにちは-مرحبا")
            self.assertEqual(len(records[0]["message"]), len(huge_text))
        else:
            self.assertEqual(records[0]["values"]["unicode"], "こんにちは-مرحبا")
            self.assertEqual(len(records[0]["values"]["comments"]), len(huge_text))

    @parameterized.expand([
        ("audit_export",),
        ("survey_response_export",),
    ])
    def test_oauth_token_expiry_mid_export_is_recovered_by_client(self, stream_type):
        stream, parent, _ = _build_stream(stream_type)
        stream.client = RefreshingExportClient(stream_type)

        records = list(stream.get_records(parent_id=parent, bookmark="2026-01-01T00:00:00Z"))

        self.assertEqual(stream.client.refresh_count, 1)
        self.assertEqual(len(records), 1)


if __name__ == "__main__":
    unittest.main()
