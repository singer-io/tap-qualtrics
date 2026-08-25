"""Tests for parent-child stream bookmark propagation via sync.py."""
import unittest
from unittest.mock import MagicMock, patch
from tap_qualtrics.streams.abstracts import FullTableStream, IncrementalStream


class ConcreteParent(FullTableStream):
    tap_stream_id = "parent_stream"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "parent-resource"
    children = ["child_stream"]


class ConcreteChild(FullTableStream):
    tap_stream_id = "child_stream"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    parent = "parent_stream"

    def get_records(self, parent_id=None):
        parent_id_val = (parent_id or {}).get("id", "unknown")
        yield {"id": f"child_of_{parent_id_val}"}


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {"type": "object", "properties": {}, "additionalProperties": True}
    entry.metadata = []
    return entry


class TestParentChildSync(unittest.TestCase):

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_child_sync_called_per_parent_record(self, mock_selected, mock_write_record):
        client = MagicMock()
        client.page_size = 100
        parent = ConcreteParent(client=client, catalog_entry=_make_entry())
        parent.client.get.return_value = {
            "result": {"elements": [{"id": "P1"}, {"id": "P2"}], "nextPage": None}
        }

        child_client = MagicMock()
        child_client.page_size = 100
        child = ConcreteChild(client=child_client, catalog_entry=_make_entry())
        parent.child_to_sync = [child]

        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        parent.sync(state={}, transformer=transformer)

        written_ids = [c.args[1]["id"] for c in mock_write_record.call_args_list]
        assert "child_of_P1" in written_ids
        assert "child_of_P2" in written_ids

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_parent_emits_own_records(self, mock_selected, mock_write_record):
        client = MagicMock()
        client.page_size = 100
        parent = ConcreteParent(client=client, catalog_entry=_make_entry())
        parent.client.get.return_value = {
            "result": {"elements": [{"id": "P1"}], "nextPage": None}
        }
        parent.child_to_sync = []

        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        parent.sync(state={}, transformer=transformer)

        mock_write_record.assert_called_once_with("parent_stream", {"id": "P1"})

