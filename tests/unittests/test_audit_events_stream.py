"""Unit tests for tap_qualtrics/streams/audit_events.py."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.streams.audit_events import AuditEvents


def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


class TestAuditEventsGetRecords(unittest.TestCase):

    def _stream(self):
        return AuditEvents(client=_make_client(), catalog_entry=_make_entry())

    def test_no_activity_type_yields_nothing(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_empty_string_activity_type_yields_nothing(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=""))
        self.assertEqual(records, [])

    def test_with_activity_type_dict(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "E1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"name": "login"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["activity_type"], "login")

    def test_with_activity_type_string(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "E1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id="login"))
        self.assertEqual(records[0]["activity_type"], "login")

    def test_pagination_with_token(self):
        stream = self._stream()
        stream.client.get.side_effect = [
            {"result": {"elements": [{"id": "E1"}], "nextPage": "tok1"}},
            {"result": {"elements": [{"id": "E2"}], "nextPage": None}},
        ]
        records = list(stream.get_records(parent_id="login"))
        self.assertEqual(len(records), 2)
        # Second call should include pageToken
        _, kw = stream.client.get.call_args
        self.assertIn("pageToken", kw["params"])

    def test_pagination_activityType_in_all_calls(self):
        stream = self._stream()
        stream.client.get.side_effect = [
            {"result": {"elements": [{"id": "E1"}], "nextPage": "tok1"}},
            {"result": {"elements": [], "nextPage": None}},
        ]
        list(stream.get_records(parent_id="login"))
        for mock_call in stream.client.get.call_args_list:
            _, kw = mock_call
            self.assertEqual(kw["params"]["activityType"], "login")

    def test_empty_elements_list(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        records = list(stream.get_records(parent_id="login"))
        self.assertEqual(records, [])

    def test_none_elements_treated_as_empty(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": None, "nextPage": None}
        }
        records = list(stream.get_records(parent_id="login"))
        self.assertEqual(records, [])


class TestAuditEventsSync(unittest.TestCase):

    def _stream(self):
        return AuditEvents(client=_make_client(), catalog_entry=_make_entry())

    @patch("tap_qualtrics.streams.audit_events.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_writes_records(self, _, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "E1"}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer, parent_id="login")
        mock_wr.assert_called_once()

    @patch("tap_qualtrics.streams.audit_events.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=False)
    def test_sync_not_selected_no_write(self, _, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "E1"}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        count = stream.sync(state={}, transformer=transformer, parent_id="login")
        self.assertEqual(count, 0)
        mock_wr.assert_not_called()

    @patch("tap_qualtrics.streams.audit_events.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_no_activity_type(self, _, mock_wr):
        stream = self._stream()
        count = stream.sync(state={}, transformer=MagicMock(), parent_id=None)
        self.assertEqual(count, 0)
        mock_wr.assert_not_called()


if __name__ == "__main__":
    unittest.main()
