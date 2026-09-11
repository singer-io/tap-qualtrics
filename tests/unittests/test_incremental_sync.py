import unittest
from unittest.mock import patch, MagicMock
from tap_qualtrics.streams.abstracts import IncrementalStream


class ConcreteIncrementalStream(IncrementalStream):
    tap_stream_id = "stream_1"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"


class TestIncrementalSync(unittest.TestCase):

    def setUp(self):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"type": "object", "properties": {}}
        mock_catalog.metadata = []
        mock_client = MagicMock()
        mock_client.page_size = 100
        mock_client.config = {"start_date": "2020-01-01"}
        self.stream = ConcreteIncrementalStream(
            client=mock_client, catalog_entry=mock_catalog
        )

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2020-06-01")
    def test_get_bookmark_returns_value(self, mock_bk):
        result = self.stream.get_bookmark({}, self.stream.tap_stream_id)
        assert result == "2020-06-01"

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2020-06-01")
    def test_write_bookmark_larger_value(self, mock_bk):
        state = {}
        result = self.stream.write_bookmark(
            state, self.stream.tap_stream_id, value="2021-01-01"
        )
        assert result["bookmarks"]["stream_1"]["updated_at"] == "2021-01-01"

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-06-01")
    def test_write_bookmark_keeps_larger_existing(self, mock_bk):
        state = {}
        result = self.stream.write_bookmark(
            state, self.stream.tap_stream_id, value="2020-01-01"
        )
        # existing bookmark (2021-06-01) > new value (2020-01-01) → keep existing
        assert result["bookmarks"]["stream_1"]["updated_at"] == "2021-06-01"
