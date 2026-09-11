"""Unit tests for tap_qualtrics/streams/distributions.py and distribution child streams."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.streams.distributions import Distributions
from tap_qualtrics.streams.distribution_history import DistributionHistory
from tap_qualtrics.streams.distribution_links import DistributionLinks


def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "modifiedDate": {"type": "string"}},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


class TestDistributionsGetRecords(unittest.TestCase):

    def _stream(self):
        return Distributions(client=_make_client(), catalog_entry=_make_entry())

    def test_no_survey_id_yields_nothing(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_survey_id_dict(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "D_1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["survey_id"], "SV_1")

    def test_survey_id_passed_as_param(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        list(stream.get_records(parent_id={"id": "SV_1"}, bookmark="2021-01-01"))
        _, kw = stream.client.get.call_args
        self.assertEqual(kw["params"]["surveyId"], "SV_1")


class TestDistributionsSync(unittest.TestCase):

    def _stream(self):
        return Distributions(client=_make_client(), catalog_entry=_make_entry())

    @patch("tap_qualtrics.streams.distributions.write_record")
    @patch("tap_qualtrics.streams.distributions.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.distributions.write_bookmark")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_writes_records_above_bookmark(self, _, mock_wb, mock_bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"id": "D_1", "modifiedDate": "2021-06-01", "survey_id": "SV_1"}],
                "nextPage": None,
            }
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})
        mock_wr.assert_called_once()

    @patch("tap_qualtrics.streams.distributions.write_record")
    @patch("tap_qualtrics.streams.distributions.get_bookmark", return_value="2021-06-01")
    @patch("tap_qualtrics.streams.distributions.write_bookmark")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_skips_old_records(self, _, mock_wb, mock_bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"id": "D_1", "modifiedDate": "2020-01-01", "survey_id": "SV_1"}],
                "nextPage": None,
            }
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        count = stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})
        self.assertEqual(count, 0)
        mock_wr.assert_not_called()

    @patch("tap_qualtrics.streams.distributions.write_record")
    @patch("tap_qualtrics.streams.distributions.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.distributions.write_bookmark")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_calls_children(self, _, mock_wb, mock_bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"id": "D_1", "modifiedDate": "2021-06-01", "survey_id": "SV_1"}],
                "nextPage": None,
            }
        }
        child = MagicMock()
        child.sync.return_value = 0
        stream.child_to_sync = [child]
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})
        child.sync.assert_called_once()

    @patch("tap_qualtrics.streams.distributions.write_record")
    @patch("tap_qualtrics.streams.distributions.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.distributions.write_bookmark")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_empty_modified_date(self, _, mock_wb, mock_bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"id": "D_1", "modifiedDate": "", "survey_id": "SV_1"}],
                "nextPage": None,
            }
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        # empty modifiedDate < bookmark "2021-01-01" → not written
        count = stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})
        self.assertEqual(count, 0)


class TestDistributionHistory(unittest.TestCase):

    def _stream(self):
        return DistributionHistory(client=_make_client(), catalog_entry=_make_entry())

    def test_no_distribution_id_yields_nothing(self):
        stream = self._stream()
        stream.url_endpoint = stream.get_url_endpoint(None)
        records = list(stream.get_records())
        self.assertEqual(records, [])

    def test_with_distribution_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"contactId": "C1"}], "nextPage": None}
        }
        stream.url_endpoint = stream.get_url_endpoint({"id": "D_1"})
        records = list(stream.get_records())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["contactId"], "C1")

    def test_with_string_parent_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"contactId": "C1"}], "nextPage": None}
        }
        stream.url_endpoint = stream.get_url_endpoint({"id": "D_1"})
        records = list(stream.get_records())
        self.assertEqual(records[0]["contactId"], "C1")


if __name__ == "__main__":
    unittest.main()
