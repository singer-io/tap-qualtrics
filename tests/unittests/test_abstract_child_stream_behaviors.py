import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

from tap_qualtrics.exceptions import QualtricsInternalServerError
from tap_qualtrics.streams.abstracts import (
    ChildBaseStream,
    IncrementalMailingListChildStream,
    IncrementalSegmentChildStream,
    IncrementalStream,
    IncrementalSurveyChildStream,
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
        "properties": {},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


class ConcreteIncrementalSurveyChild(IncrementalSurveyChildStream):
    tap_stream_id = "incr_survey_child_test"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["lastModified"]
    data_key = "result.elements"
    path = "surveys/{survey_id}/children"


class ConcreteIncrementalMailingListChild(IncrementalMailingListChildStream):
    tap_stream_id = "incr_mailing_child_test"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/records"


class ConcreteIncrementalSegmentChild(IncrementalSegmentChildStream):
    tap_stream_id = "incr_segment_child_test"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segment_id}/records"


class ConcreteChildBaseDefault(ChildBaseStream):
    tap_stream_id = "child_base_default"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["updatedAt"]
    data_key = "result.elements"


class ConcreteChildBaseSync(ChildBaseStream):
    tap_stream_id = "child_base_sync"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = ["updatedAt"]
    data_key = "result.elements"

    def get_url_endpoint(self, _parent_obj=None):
        return "child-base-endpoint"


class NoReplicationIncremental(IncrementalStream):
    tap_stream_id = "no_replication"
    key_properties: ClassVar = ["id"]
    replication_keys: ClassVar = []
    data_key = "result.elements"
    path = "x"

    def sync(self, state, transformer, parent_id=None):
        _ = (state, transformer, parent_id)
        return 0


class TestAbstractChildBehaviors(unittest.TestCase):

    def test_incremental_survey_child_sets_fields(self):
        stream = ConcreteIncrementalSurveyChild(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "R1"}], "nextPage": None}
        }

        records = list(
            stream.get_records(parent_id={"id": "SV_1", "lastModified": "2024-01-01T00:00:00Z"})
        )

        self.assertEqual(records[0]["survey_id"], "SV_1")
        self.assertEqual(records[0]["lastModified"], "2024-01-01T00:00:00Z")
        self.assertEqual(
            stream._make_probe_path({"id": "SV_1"}),
            "surveys/SV_1/children",
        )

    def test_incremental_survey_child_returns_empty_without_id(self):
        stream = ConcreteIncrementalSurveyChild(client=_make_client(), catalog_entry=_make_entry())
        records = list(stream.get_records(parent_id={}))
        self.assertEqual(records, [])

    def test_incremental_mailing_list_child_sets_parent_bookmark(self):
        stream = ConcreteIncrementalMailingListChild(
            client=_make_client(), catalog_entry=_make_entry()
        )
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "X1"}], "nextPage": None}
        }

        records = list(
            stream.get_records(
                parent_id={
                    "_directory_id": "DIR_1",
                    "mailingListId": "ML_1",
                    "lastModifiedDate": "2024-02-01T00:00:00Z",
                }
            )
        )

        self.assertEqual(records[0]["lastModifiedDate"], "2024-02-01T00:00:00Z")
        self.assertEqual(
            stream._make_probe_path({"_directory_id": "DIR_1", "mailingListId": "ML_1"}),
            "directories/DIR_1/mailinglists/ML_1/records",
        )

    def test_incremental_mailing_list_child_missing_inputs(self):
        stream = ConcreteIncrementalMailingListChild(
            client=_make_client(), catalog_entry=_make_entry()
        )
        self.assertEqual(list(stream.get_records(parent_id=None)), [])
        self.assertEqual(
            list(stream.get_records(parent_id={"_directory_id": "DIR_1", "mailingListId": ""})),
            [],
        )
        self.assertEqual(stream._make_probe_path({}), "")

    def test_incremental_segment_child_sets_parent_bookmark(self):
        stream = ConcreteIncrementalSegmentChild(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "X1"}], "nextPage": None}
        }

        records = list(
            stream.get_records(
                parent_id={
                    "_directory_id": "DIR_1",
                    "segmentId": "SEG_1",
                    "lastModifiedDate": "2024-03-01T00:00:00Z",
                }
            )
        )

        self.assertEqual(records[0]["segmentId"], "SEG_1")
        self.assertEqual(records[0]["lastModifiedDate"], "2024-03-01T00:00:00Z")
        self.assertEqual(
            stream._make_probe_path({"_directory_id": "DIR_1", "segmentId": "SEG_1"}),
            "directories/DIR_1/segments/SEG_1/records",
        )

    def test_incremental_segment_child_missing_inputs(self):
        stream = ConcreteIncrementalSegmentChild(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(list(stream.get_records(parent_id=None)), [])
        self.assertEqual(
            list(stream.get_records(parent_id={"_directory_id": "DIR_1", "segmentId": ""})),
            [],
        )
        self.assertEqual(stream._make_probe_path({}), "")

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value=None)
    def test_child_base_bookmark_cached_and_defaults_empty(self, mock_get_bookmark):
        stream = ConcreteChildBaseDefault(client=_make_client(), catalog_entry=_make_entry())

        first = stream.get_bookmark({}, stream.tap_stream_id)
        second = stream.get_bookmark({}, stream.tap_stream_id)

        self.assertEqual(first, "")
        self.assertEqual(second, "")
        self.assertEqual(mock_get_bookmark.call_count, 1)

    @patch("tap_qualtrics.streams.abstracts.LOGGER")
    def test_child_base_get_records_warns_on_internal_server_error(self, mock_logger):
        stream = ConcreteChildBaseDefault(client=_make_client(), catalog_entry=_make_entry())
        stream.url_endpoint = "broken"
        with patch.object(
            stream,
            "_paginate",
            side_effect=QualtricsInternalServerError("500"),
        ), self.assertRaises(QualtricsInternalServerError):
            list(stream.get_records(parent_id={"id": "P1"}, bookmark=""))

        self.assertTrue(mock_logger.warning.called)

    def test_child_base_default_methods(self):
        stream = ConcreteChildBaseDefault(client=_make_client(), catalog_entry=_make_entry())
        sample = {"id": "R1"}

        self.assertEqual(stream.get_url_endpoint({"id": "P1"}), "")
        self.assertEqual(stream.modify_object(sample, {"id": "P1"}), sample)

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=False)
    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="")
    def test_child_base_sync_propagates_bookmark_to_matching_child(
        self,
        _mock_get_bookmark,
        _mock_is_selected,
        mock_write_record,
    ):
        stream = ConcreteChildBaseSync(client=_make_client(), catalog_entry=_make_entry())
        child_same_key = MagicMock()
        child_same_key.replication_keys = ["updatedAt"]
        child_same_key.tap_stream_id = "child_same"
        child_same_key.write_bookmark.return_value = {"bookmarks": {}}

        child_other_key = MagicMock()
        child_other_key.replication_keys = ["lastModifiedDate"]
        child_other_key.tap_stream_id = "child_other"
        child_other_key.write_bookmark.return_value = {"bookmarks": {}}

        stream.child_to_sync = [child_same_key, child_other_key]
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "1", "updatedAt": "2024-04-01T00:00:00Z"}], "nextPage": None}
        }

        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *_a, **_k: r

        count = stream.sync(state={}, transformer=transformer, parent_id={"id": "P1"})

        self.assertEqual(count, 1)
        self.assertFalse(mock_write_record.called)
        child_same_key.write_bookmark.assert_called_once()
        child_other_key.write_bookmark.assert_not_called()

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="")
    def test_child_base_sync_writes_record_when_selected(
        self,
        _mock_get_bookmark,
        _mock_is_selected,
        mock_write_record,
    ):
        stream = ConcreteChildBaseSync(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "1", "updatedAt": "2024-05-01T00:00:00Z"}], "nextPage": None}
        }

        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *_a, **_k: r
        count = stream.sync(state={}, transformer=transformer, parent_id={"id": "P1"})

        self.assertEqual(count, 1)
        mock_write_record.assert_called_once()

    def test_incremental_write_bookmark_without_replication_key(self):
        stream = NoReplicationIncremental(client=_make_client(), catalog_entry=_make_entry())
        state = {"bookmarks": {}}

        returned = stream.write_bookmark(state, stream.tap_stream_id, key=None, value="x")

        self.assertIs(returned, state)


if __name__ == "__main__":
    unittest.main()
