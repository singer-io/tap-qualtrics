import json
import time
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from tap_qualtrics.streams.audit_export import AuditExport
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport


BASELINES_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "nonfunctional_export_baselines.json"
BASELINES = json.loads(BASELINES_PATH.read_text(encoding="utf-8"))


class _StreamingNDJSONResponse:
    def __init__(self, records):
        self._records = records
        self.headers = {"Content-Type": "application/x-ndjson"}

    @property
    def content(self):
        raise AssertionError("content should not be materialized for NDJSON streaming path")

    def iter_lines(self, decode_unicode=True):
        for record in self._records:
            line = json.dumps(record)
            yield line if decode_unicode else line.encode("utf-8")


def _make_client(start_date="2020-01-01T00:00:00Z"):
    client = MagicMock()
    client.start_date = start_date
    client.page_size = 100
    return client


class TestExportStreamingAndReconciliation(unittest.TestCase):

    def test_audit_export_streams_ndjson_without_materializing_content(self):
        client = _make_client()
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1", "recordCount": 3}}
        client.get_file.return_value = _StreamingNDJSONResponse(
            [
                {"id": "e1", "timestamp": "2021-01-01T00:00:00Z"},
                {"id": "e2", "timestamp": "2021-01-02T00:00:00Z"},
                {"id": "e3", "timestamp": "2021-01-03T00:00:00Z"},
            ]
        )

        stream = AuditExport(client=client)
        records = list(stream.get_records(parent_id={"name": "login"}))

        self.assertEqual(3, len(records))
        self.assertEqual("login", records[0]["event_type"])
        client.get_file.assert_called_once()
        self.assertEqual(True, client.get_file.call_args.kwargs.get("stream"))

    def test_survey_export_streams_ndjson_without_materializing_content(self):
        client = _make_client()
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {
            "result": {"status": "complete", "fileId": "fid1", "totalResponses": 2}
        }
        client.get_file.return_value = _StreamingNDJSONResponse(
            [
                {"responseId": "R1", "values": {"recordedDate": "2021-01-01T00:00:00Z"}},
                {"responseId": "R2", "values": {"recordedDate": "2021-01-02T00:00:00Z"}},
            ]
        )

        stream = SurveyResponseExport(client=client)
        records = list(stream.get_records(parent_id={"id": "SV_1"}))

        self.assertEqual(2, len(records))
        self.assertEqual("SV_1", records[0]["survey_id"])
        client.get_file.assert_called_once()
        self.assertEqual(True, client.get_file.call_args.kwargs.get("stream"))

    def test_audit_export_logs_reconciliation_mismatch(self):
        client = _make_client()
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {"result": {"status": "complete", "fileId": "f1", "recordCount": 5}}
        client.get_file.return_value = _StreamingNDJSONResponse(
            [{"id": "e1", "timestamp": "2021-01-01T00:00:00Z"}]
        )

        stream = AuditExport(client=client)
        with self.assertLogs(level="WARNING") as logs:
            records = list(stream.get_records(parent_id={"name": "login"}))

        self.assertEqual(1, len(records))
        self.assertTrue(any("Record-count reconciliation mismatch" in log for log in logs.output))

    def test_survey_export_logs_reconciliation_mismatch(self):
        client = _make_client()
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {
            "result": {"status": "complete", "fileId": "fid1", "totalResponses": 4}
        }
        client.get_file.return_value = _StreamingNDJSONResponse(
            [{"responseId": "R1", "values": {"recordedDate": "2021-01-01T00:00:00Z"}}]
        )

        stream = SurveyResponseExport(client=client)
        with self.assertLogs(level="WARNING") as logs:
            records = list(stream.get_records(parent_id={"id": "SV_1"}))

        self.assertEqual(1, len(records))
        self.assertTrue(any("Record-count reconciliation mismatch" in log for log in logs.output))


class TestExportPerformanceBaselines(unittest.TestCase):

    @staticmethod
    def _make_ndjson_records(count, response_prefix):
        return [
            {
                "id": f"{response_prefix}-{idx}",
                "responseId": f"{response_prefix}-{idx}",
                "timestamp": "2021-01-01T00:00:00Z",
                "values": {"recordedDate": "2021-01-01T00:00:00Z"},
            }
            for idx in range(count)
        ]

    def _assert_export_performance(self, stream_name, stream, parent_id):
        baseline = BASELINES[stream_name]
        records = self._make_ndjson_records(
            baseline["records"],
            response_prefix=stream_name,
        )
        stream.client.get_file.return_value = _StreamingNDJSONResponse(records)

        tracemalloc.start()
        start = time.perf_counter()
        record_count = sum(1 for _ in stream.get_records(parent_id=parent_id))
        elapsed = time.perf_counter() - start
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / (1024 * 1024)

        self.assertEqual(baseline["records"], record_count)
        self.assertLessEqual(
            elapsed,
            baseline["max_seconds"],
            msg=f"{stream_name} exceeded runtime baseline: {elapsed:.3f}s > {baseline['max_seconds']}s",
        )
        self.assertLessEqual(
            peak_mb,
            baseline["max_peak_mb"],
            msg=f"{stream_name} exceeded peak-memory baseline: {peak_mb:.2f}MB > {baseline['max_peak_mb']}MB",
        )

    def test_audit_export_performance_baseline(self):
        client = _make_client()
        client.post.return_value = {"result": {"id": "exp1"}}
        client.poll_export.return_value = {
            "result": {
                "status": "complete",
                "fileId": "f1",
                "recordCount": BASELINES["audit_export"]["records"],
            }
        }
        stream = AuditExport(client=client)
        self._assert_export_performance("audit_export", stream, parent_id={"name": "login"})

    def test_survey_export_performance_baseline(self):
        client = _make_client()
        client.post.return_value = {"result": {"progressId": "pid1"}}
        client.poll_export.return_value = {
            "result": {
                "status": "complete",
                "fileId": "fid1",
                "totalResponses": BASELINES["survey_response_export"]["records"],
            }
        }
        stream = SurveyResponseExport(client=client)
        self._assert_export_performance("survey_response_export", stream, parent_id={"id": "SV_1"})


if __name__ == "__main__":
    unittest.main()
