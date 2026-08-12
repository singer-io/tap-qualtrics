import unittest
from unittest.mock import patch, MagicMock, call
from tap_qualtrics.sync import sync, update_currently_syncing


class TestUpdateCurrentlySyncing(unittest.TestCase):

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set, mock_get):
        mock_get.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}
        update_currently_syncing(state, None)
        mock_set.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set, mock_get):
        mock_get.return_value = None
        state = {}
        update_currently_syncing(state, "new_stream")
        mock_set.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)


class TestSync(unittest.TestCase):

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    @patch("tap_qualtrics.streams.STREAMS", {})
    def test_sync_skips_unknown_stream(self, *_):
        mock_catalog = MagicMock()
        selected = MagicMock()
        selected.stream = "unknown_stream"
        mock_catalog.get_selected_streams.return_value = [selected]
        client = MagicMock()
        sync(client, {}, mock_catalog, {})

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_sync_calls_stream_sync(self, *_):
        mock_stream_cls = MagicMock()
        mock_stream_instance = MagicMock()
        mock_stream_instance.parent = None
        mock_stream_instance.children = []
        mock_stream_instance.child_to_sync = []
        mock_stream_instance.sync.return_value = 5
        mock_stream_cls.return_value = mock_stream_instance

        mock_catalog = MagicMock()
        selected = MagicMock()
        selected.stream = "users"
        mock_catalog.get_selected_streams.return_value = [selected]
        mock_catalog.get_stream.return_value = MagicMock()

        with patch("tap_qualtrics.sync.STREAMS", {"users": mock_stream_cls}):
            sync(MagicMock(), {}, mock_catalog, {})

        mock_stream_instance.sync.assert_called_once()

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_sync_skips_child_when_parent_selected(self, *_):
        parent_cls = MagicMock()
        parent_inst = MagicMock()
        parent_inst.parent = None
        parent_inst.children = ["contacts"]
        parent_inst.child_to_sync = []
        parent_inst.sync.return_value = 0
        parent_cls.return_value = parent_inst

        child_cls = MagicMock()
        child_inst = MagicMock()
        child_inst.parent = "directories"
        child_inst.children = []
        child_inst.child_to_sync = []
        child_cls.return_value = child_inst

        mock_catalog = MagicMock()
        s1 = MagicMock(); s1.stream = "directories"
        s2 = MagicMock(); s2.stream = "contacts"
        mock_catalog.get_selected_streams.return_value = [s1, s2]
        mock_catalog.get_stream.return_value = MagicMock()

        streams = {"directories": parent_cls, "contacts": child_cls}
        with patch("tap_qualtrics.sync.STREAMS", streams):
            sync(MagicMock(), {}, mock_catalog, {})

        # child should not be synced directly; only through parent
        child_inst.sync.assert_not_called()
