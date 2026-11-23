import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest
import singer

from tap_qualtrics.exceptions import QualtricsError
from tap_qualtrics.streams.abstracts import IncrementalStream


class ConcreteIncrementalStream(IncrementalStream):
    tap_stream_id = "stream_1"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["updated_at"]
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


@pytest.mark.parametrize("current,value,expected", [
    ("2025-01-01T00:00:00Z", "2024-12-31T23:30:00-01:00", "2024-12-31T23:30:00-01:00"),
    ("2024-12-31T23:30:00-01:00", "2025-01-01T00:00:00Z", "2024-12-31T23:30:00-01:00"),
    ("2025-01-01T00:00:00Z", "2025-01-01T00:00:00.000000Z", "2025-01-01T00:00:00Z"),
])
def test_bookmark_maximum_uses_chronological_order(current, value, expected):
    client = MagicMock()
    client.page_size = 100
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    stream = ConcreteIncrementalStream(client=client)
    state = {"bookmarks": {"stream_1": {"updated_at": current}}}
    stream.write_bookmark(state, "stream_1", value=value)
    assert singer.get_bookmark(state, "stream_1", "updated_at") == expected


@pytest.mark.parametrize("record_timestamp", [
    "2025-01-01T00:00:00.000000Z", "2024-12-31T19:00:00-05:00",
])
def test_incremental_filter_includes_equivalent_timestamp(record_timestamp):
    client = MagicMock()
    client.page_size = 100
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    client.get.return_value = {"result": {"elements": [
        {"id": "1", "updated_at": record_timestamp},
    ]}}
    stream = ConcreteIncrementalStream(client=client)
    stream.mdata = {(): {"selected": True}}
    state = {"bookmarks": {"stream_1": {"updated_at": "2025-01-01T00:00:00Z"}}}
    transformer = MagicMock()
    transformer.transform.side_effect = lambda record, *args: record
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        assert stream.sync(state, transformer) == 1
    write_record.assert_called_once()


@pytest.mark.parametrize("state", [
    [], {"bookmarks": None}, {"bookmarks": []}, {"bookmarks": {"stream_1": []}},
])
@pytest.mark.parametrize("operation", ["read", "write"])
def test_invalid_bookmark_state_is_rejected(state, operation):
    client = MagicMock()
    client.page_size = 100
    stream = ConcreteIncrementalStream(client=client)
    with pytest.raises(QualtricsError, match="Invalid bookmark state"):
        if operation == "read":
            stream.get_bookmark(state, "stream_1")
        else:
            stream.write_bookmark(state, "stream_1", value="2025-01-01T00:00:00Z")
