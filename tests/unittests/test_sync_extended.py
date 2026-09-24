"""Extended tests for tap_qualtrics/sync.py."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.sync import (
    _has_selected_dynamic_entries,
    sync,
    update_currently_syncing,
    write_schema,
)


# ---------------------------------------------------------------------------
# _has_selected_dynamic_entries
# ---------------------------------------------------------------------------

class TestHasSelectedDynamicEntries(unittest.TestCase):

    def _make_catalog(self, stream_ids_selected):
        catalog = MagicMock()
        streams = []
        from singer import metadata as md
        for sid, selected in stream_ids_selected.items():
            entry = MagicMock()
            entry.tap_stream_id = sid
            entry.metadata = md.to_list(md.write(md.new(), (), "selected", selected))
            streams.append(entry)
        catalog.streams = streams
        return catalog

    def test_true_when_matching_selected_entry(self):
        catalog = self._make_catalog({
            "audit_export__login": True,
            "users": True,
        })
        self.assertTrue(_has_selected_dynamic_entries("audit_export", catalog))

    def test_false_when_no_matching_prefix(self):
        catalog = self._make_catalog({"users": True})
        self.assertFalse(_has_selected_dynamic_entries("audit_export", catalog))

    def test_false_when_matching_entry_not_selected(self):
        catalog = self._make_catalog({"audit_export__login": False})
        self.assertFalse(_has_selected_dynamic_entries("audit_export", catalog))


# ---------------------------------------------------------------------------
# write_schema
# ---------------------------------------------------------------------------

class TestWriteSchema(unittest.TestCase):

    def _make_stream(self, stream_name, children=None, selected=True, parent=None):
        stream = MagicMock()
        stream.tap_stream_id = stream_name
        stream.is_selected.return_value = selected
        stream.children = children or []
        stream.child_to_sync = []
        stream.parent = parent
        return stream

    def test_write_schema_not_selected_skips(self):
        stream = self._make_stream("users", selected=False)
        catalog = MagicMock()
        catalog.get_stream.return_value = None
        write_schema(stream, MagicMock(), [], catalog)
        stream.write_schema.assert_not_called()

    def test_write_schema_selected_calls_write_schema(self):
        stream = self._make_stream("users", selected=True)
        catalog = MagicMock()
        catalog.get_stream.return_value = None
        write_schema(stream, MagicMock(), [], catalog)
        stream.write_schema.assert_called_once()

    def test_child_in_streams_to_sync_appended(self):
        from tap_qualtrics.streams import STREAMS
        stream = self._make_stream("surveys", children=["survey_quotas"], selected=True)
        catalog = MagicMock()
        child_entry = MagicMock()
        child_entry.metadata = []
        child_entry.schema.to_dict.return_value = {"type": "object", "properties": {}}
        catalog.get_stream.return_value = child_entry

        client = MagicMock()
        client.page_size = 100
        with patch("tap_qualtrics.sync.STREAMS", {**STREAMS}):
            write_schema(stream, client, ["survey_quotas"], catalog)

        self.assertEqual(len(stream.child_to_sync), 1)

    def test_dynamic_child_without_catalog_entry(self):
        stream = self._make_stream("audit_export_event_types", children=["audit_export"], selected=True)
        catalog = MagicMock()
        catalog.get_stream.return_value = None
        # Simulate a dynamic entry existing
        from singer import metadata as md
        dynamic_entry = MagicMock()
        dynamic_entry.tap_stream_id = "audit_export__login"
        dynamic_entry.metadata = md.to_list(md.write(md.new(), (), "selected", True))
        catalog.streams = [dynamic_entry]
        client = MagicMock()
        client.page_size = 100

        from tap_qualtrics.streams.audit_export import AuditExport
        with patch("tap_qualtrics.sync.STREAMS", {"audit_export": AuditExport}):
            write_schema(stream, client, ["audit_export"], catalog)

        self.assertEqual(len(stream.child_to_sync), 1)


# ---------------------------------------------------------------------------
# sync (edge cases)
# ---------------------------------------------------------------------------

class TestSyncEdgeCases(unittest.TestCase):

    @patch("singer.write_state")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_dynamic_stream_adds_parent(self, *_):
        parent_cls = MagicMock()
        parent_inst = MagicMock()
        parent_inst.parent = None
        parent_inst.children = []
        parent_inst.child_to_sync = []
        parent_inst.sync.return_value = 0
        parent_cls.return_value = parent_inst
        parent_cls.parent = None

        # audit_export__login → base=audit_export → parent=audit_export_event_types
        audit_export_cls = MagicMock()
        audit_export_cls.parent = "audit_export_event_types"

        mock_catalog = MagicMock()
        s1 = MagicMock()
        s1.stream = "audit_export__login"
        mock_catalog.get_selected_streams.return_value = [s1]
        mock_catalog.get_stream.return_value = MagicMock()

        streams = {
            "audit_export": audit_export_cls,
            "audit_export_event_types": parent_cls,
        }
        with patch("tap_qualtrics.sync.STREAMS", streams):
            sync(MagicMock(), {}, mock_catalog, {})

        parent_inst.sync.assert_called_once()

    @patch("singer.write_state")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_unknown_dynamic_stream_with_no_base_skipped(self, *_):
        mock_catalog = MagicMock()
        s1 = MagicMock()
        s1.stream = "completely_unknown__whatever"
        mock_catalog.get_selected_streams.return_value = [s1]
        mock_catalog.get_stream.return_value = MagicMock()

        with patch("tap_qualtrics.sync.STREAMS", {}):
            # Should not raise
            sync(MagicMock(), {}, mock_catalog, {})

    @patch("singer.write_state")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_child_stream_auto_adds_parent(self, *_):
        parent_cls = MagicMock()
        parent_inst = MagicMock()
        parent_inst.parent = None
        parent_inst.children = []
        parent_inst.child_to_sync = []
        parent_inst.sync.return_value = 0
        parent_cls.return_value = parent_inst
        parent_cls.parent = None

        child_cls = MagicMock()
        child_inst = MagicMock()
        child_inst.parent = "surveys"
        child_cls.return_value = child_inst
        child_cls.parent = "surveys"

        mock_catalog = MagicMock()
        s1 = MagicMock()
        s1.stream = "survey_quotas"
        mock_catalog.get_selected_streams.return_value = [s1]
        mock_catalog.get_stream.return_value = MagicMock()

        streams = {"surveys": parent_cls, "survey_quotas": child_cls}
        with patch("tap_qualtrics.sync.STREAMS", streams):
            sync(MagicMock(), {}, mock_catalog, {})

        # Parent was added and synced
        parent_inst.sync.assert_called_once()


if __name__ == "__main__":
    unittest.main()
