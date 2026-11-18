import unittest
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import Catalog

from tap_qualtrics.client import Client
from tap_qualtrics.discover import _add_dynamic_entries, discover
from tap_qualtrics.exceptions import QualtricsError, QualtricsForbiddenError
from tap_qualtrics.streams.audit_export import AuditExport
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport
from tap_qualtrics.sync import sync


def _make_client():
    client = MagicMock()
    client.page_size = 100
    client.start_date = "2020-01-01T00:00:00Z"
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    return client


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


class TestDiscoverAndSyncBranchBehaviors(unittest.TestCase):

    def test_add_dynamic_entries_marks_replication_key_automatic(self):
        catalog = Catalog([])

        with patch("tap_qualtrics.discover.AuditExport") as mock_audit_export, patch(
            "tap_qualtrics.discover.SurveyResponseExport"
        ) as mock_sre:
            mock_audit_export.tap_stream_id = "audit_export"
            mock_audit_export.replication_method = "INCREMENTAL"
            mock_audit_export.replication_keys = ["eventTime"]
            mock_audit_export.parent = "audit_events_types"
            mock_audit_export.discover_dynamic_entries.return_value = (
                [
                    (
                        "audit_export__login",
                        {
                            "type": "object",
                            "properties": {"eventTime": {"type": "string"}},
                        },
                        ["id"],
                    )
                ],
                [],
            )

            mock_sre.tap_stream_id = "survey_response_export"
            mock_sre.replication_method = "INCREMENTAL"
            mock_sre.replication_keys = ["recordedDate"]
            mock_sre.parent = "surveys"
            mock_sre.discover_dynamic_entries.return_value = ([], [])

            _add_dynamic_entries(MagicMock(), catalog)

        entry = catalog.get_stream("audit_export__login")
        mdata = metadata.to_map(entry.metadata)
        self.assertEqual(
            metadata.get(mdata, ("properties", "eventTime"), "inclusion"),
            "automatic",
        )

    @patch("tap_qualtrics.discover._add_dynamic_entries", return_value=["x"])
    @patch("tap_qualtrics.discover._apply_access_checks", return_value=["y"])
    @patch("tap_qualtrics.discover.LOGGER")
    def test_discover_logs_skipped_summary(self, mock_logger, _mock_access, _mock_dynamic):
        discover(client=MagicMock())
        self.assertTrue(mock_logger.info.called)

    @patch("tap_qualtrics.sync.write_schema")
    @patch("tap_qualtrics.sync.update_currently_syncing")
    @patch("singer.write_state")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_sync_exception_path_writes_state_then_raises(
        self,
        _mock_currently_syncing,
        mock_transformer_cls,
        mock_write_state,
        _mock_update_syncing,
        _mock_write_schema,
    ):
        failing_cls = MagicMock()
        failing_stream = MagicMock()
        failing_stream.parent = None
        failing_stream.children = []
        failing_stream.child_to_sync = []
        failing_stream.sync.side_effect = RuntimeError("boom")
        failing_cls.return_value = failing_stream

        catalog = MagicMock()
        selected = MagicMock()
        selected.stream = "users"
        catalog.get_selected_streams.return_value = [selected]
        catalog.get_stream.return_value = MagicMock()

        with patch("tap_qualtrics.sync.STREAMS", {"users": failing_cls}):
            with self.assertRaises(RuntimeError):
                sync(MagicMock(), {}, catalog, {})

        self.assertTrue(mock_write_state.called)

    @patch("tap_qualtrics.streams.survey_response_export.LOGGER")
    @patch("singer.write_bookmark")
    @patch("tap_qualtrics.streams.survey_response_export.get_bookmark", return_value="")
    @patch("tap_qualtrics.streams.survey_response_export.write_schema")
    def test_survey_response_export_sync_handles_qualtrics_error(
        self,
        _mock_write_schema,
        _mock_get_bookmark,
        _mock_write_bookmark,
        mock_logger,
    ):
        stream = SurveyResponseExport(client=_make_client(), catalog_entry=_make_entry())
        cat_entry = MagicMock()
        cat_entry.schema.to_dict.return_value = {"type": "object", "properties": {}}
        cat_entry.metadata = metadata.to_list(metadata.write(metadata.new(), (), "selected", True))

        stream.catalog = MagicMock()
        stream.catalog.get_stream.return_value = cat_entry

        transformer = MagicMock()
        with patch.object(stream, "get_records", side_effect=QualtricsError("api-error")):
            count = stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})

        self.assertEqual(count, 0)
        self.assertTrue(mock_logger.warning.called)


class TestDynamicDiscoveryClientBranches(unittest.TestCase):

    def _real_client_mock(self, list_response):
        client = MagicMock(spec=Client)
        client.start_date = "2020-01-01T00:00:00Z"
        client.get.return_value = list_response
        worker = MagicMock()
        worker.post.return_value = {"result": {}}
        client.fork.return_value = worker
        return client, worker

    def test_audit_export_forks_worker_client(self):
        client, worker = self._real_client_mock({"result": {"elements": [{"name": "login"}]}})
        entries, skipped = AuditExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        self.assertEqual(skipped, ["login"])
        client.fork.assert_called_once()
        worker.post.assert_called_once()

    @patch("tap_qualtrics.streams.audit_export.LOGGER")
    def test_audit_export_forbidden_returns_empty(self, mock_logger):
        client = _make_client()
        client.get.side_effect = QualtricsForbiddenError("403")
        self.assertEqual(AuditExport.discover_dynamic_entries(client), ([], []))
        mock_logger.warning.assert_called_once()

    def test_survey_response_export_forks_worker_client(self):
        client, worker = self._real_client_mock({"result": {"elements": [{"id": "SV_1"}]}})
        entries, skipped = SurveyResponseExport.discover_dynamic_entries(client)
        self.assertEqual(entries, [])
        self.assertEqual(skipped, ["SV_1"])
        client.fork.assert_called_once()
        worker.post.assert_called_once()

    @patch("tap_qualtrics.streams.survey_response_export.LOGGER")
    def test_survey_response_export_forbidden_returns_empty(self, mock_logger):
        client = _make_client()
        client.get.side_effect = QualtricsForbiddenError("403")
        self.assertEqual(SurveyResponseExport.discover_dynamic_entries(client), ([], []))
        mock_logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
