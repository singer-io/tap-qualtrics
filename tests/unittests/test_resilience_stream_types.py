import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from tap_qualtrics.exceptions import (
    QualtricsBadGatewayError,
    QualtricsRateLimitError,
)
from tap_qualtrics.streams.abstracts import (
    ChildBaseStream,
    FullTableStream,
    IncrementalStream,
)


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
        "properties": {
            "id": {"type": "string"},
            "updatedAt": {"type": ["null", "string"]},
        },
    }
    entry.metadata = []
    return entry


class ConcreteFullTable(FullTableStream):
    tap_stream_id = "resilience_full_table"
    key_properties: ClassVar = ["id"]
    data_key = "result.elements"
    path = "resilience/full"


class ConcreteIncremental(IncrementalStream):
    tap_stream_id = "resilience_incremental"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["updatedAt"]
    data_key = "result.elements"
    path = "resilience/incremental"


class ConcreteChildBase(ChildBaseStream):
    tap_stream_id = "resilience_child_base"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["updatedAt"]
    data_key = "result.elements"

    def get_url_endpoint(self, _parent_obj=None):
        return "resilience/child"


class RefreshingClient:
    def __init__(self, record):
        self.page_size = 100
        self.start_date = "2020-01-01T00:00:00Z"
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self._record = record
        self.refresh_count = 0
        self.call_count = 0

    def get(self, _path, params=None, full_url=None):
        _ = (params, full_url)
        self.call_count += 1
        if self.call_count == 1:
            # Simulate an internal 401 + token refresh handled in the client layer.
            self.refresh_count += 1
        return {"result": {"elements": [dict(self._record)], "nextPage": None}}


class TestResilienceAcrossStreamTypes(unittest.TestCase):

    def _stream_case(self, stream_type):
        client = _make_client()
        entry = _make_entry()
        if stream_type == "full_table":
            stream = ConcreteFullTable(client=client, catalog_entry=entry)
            parent = None
        elif stream_type == "incremental":
            stream = ConcreteIncremental(client=client, catalog_entry=entry)
            parent = None
        elif stream_type == "child_base":
            stream = ConcreteChildBase(client=client, catalog_entry=entry)
            parent = {"id": "P1"}
        else:
            raise AssertionError(f"unknown stream_type={stream_type}")
        return stream, parent

    @parameterized.expand([
        ("full_table",),
        ("incremental",),
        ("child_base",),
    ])
    def test_429_after_client_retries_exhausted_propagates(self, stream_type):
        stream, parent = self._stream_case(stream_type)
        stream.client.get.side_effect = QualtricsRateLimitError("429")

        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_a, **_k: record

        with self.assertRaises(QualtricsRateLimitError):
            stream.sync(state={"bookmarks": {}}, transformer=transformer, parent_id=parent)

    @parameterized.expand([
        ("full_table",),
        ("incremental",),
        ("child_base",),
    ])
    def test_5xx_after_client_backoff_exhausted_propagates(self, stream_type):
        stream, parent = self._stream_case(stream_type)
        stream.client.get.side_effect = QualtricsBadGatewayError("502")

        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_a, **_k: record

        with self.assertRaises(QualtricsBadGatewayError):
            stream.sync(state={"bookmarks": {}}, transformer=transformer, parent_id=parent)

    @parameterized.expand([
        ("full_table", {"id": "R1"}),
        ("incremental", {"id": "R1", "updatedAt": "2026-11-10T00:00:00Z"}),
        ("child_base", {"id": "R1", "updatedAt": "2026-11-10T00:00:00Z"}),
    ])
    def test_401_mid_sync_recovered_by_client_refresh(self, stream_type, record):
        refreshing_client = RefreshingClient(record=record)
        entry = _make_entry()

        if stream_type == "full_table":
            stream = ConcreteFullTable(client=refreshing_client, catalog_entry=entry)
            parent = None
        elif stream_type == "incremental":
            stream = ConcreteIncremental(client=refreshing_client, catalog_entry=entry)
            parent = None
        else:
            stream = ConcreteChildBase(client=refreshing_client, catalog_entry=entry)
            parent = {"id": "P1"}

        transformer = MagicMock()
        transformer.transform.side_effect = lambda input_record, *_a, **_k: input_record

        with patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True), patch(
            "tap_qualtrics.streams.abstracts.write_record"
        ) as mock_write_record:
            count = stream.sync(state={"bookmarks": {}}, transformer=transformer, parent_id=parent)

        self.assertEqual(refreshing_client.refresh_count, 1)
        self.assertEqual(count, 1)
        self.assertTrue(mock_write_record.called)


if __name__ == "__main__":
    unittest.main()
