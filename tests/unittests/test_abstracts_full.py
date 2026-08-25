"""Comprehensive unit tests for all abstract stream base classes."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.streams.abstracts import (
    ContactChildStream,
    DirectoryChildStream,
    FullTableStream,
    GroupChildStream,
    IncrementalDirectoryChildStream,
    IncrementalStream,
    LibraryChildStream,
    MailingListChildStream,
    SampleChildStream,
    SegmentChildStream,
    SurveyChildStream,
    TicketChildStream,
    _get_nested,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {
        "type": "object",
        "properties": {},
        "additionalProperties": True,
    }
    entry.metadata = []
    return entry


# ---------------------------------------------------------------------------
# _get_nested
# ---------------------------------------------------------------------------

class TestGetNested(unittest.TestCase):

    def test_top_level_key(self):
        self.assertEqual(_get_nested({"a": 1}, "a"), 1)

    def test_nested_path(self):
        self.assertEqual(_get_nested({"result": {"elements": [1]}}, "result.elements"), [1])

    def test_missing_key_returns_none(self):
        self.assertIsNone(_get_nested({"a": 1}, "b"))

    def test_non_dict_in_midpath_returns_none(self):
        self.assertIsNone(_get_nested({"a": "string"}, "a.b"))

    def test_none_input_returns_none(self):
        self.assertIsNone(_get_nested(None, "a"))


# ---------------------------------------------------------------------------
# BaseStream helpers
# ---------------------------------------------------------------------------

class ConcreteFullTable(FullTableStream):
    tap_stream_id = "test_full"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "test-resource"
    page_size = 100


class TestBaseStreamHelpers(unittest.TestCase):

    def _stream(self):
        return ConcreteFullTable(client=_make_client(), catalog_entry=_make_entry())

    @patch("tap_qualtrics.streams.abstracts.write_schema")
    def test_write_schema_calls_singer(self, mock_ws):
        stream = self._stream()
        stream.write_schema()
        mock_ws.assert_called_once_with("test_full", stream.schema, stream.key_properties)

    @patch("tap_qualtrics.streams.abstracts.write_schema", side_effect=OSError("disk full"))
    def test_write_schema_re_raises_os_error(self, _):
        with self.assertRaises(OSError):
            self._stream().write_schema()

    def test_is_selected_false_by_default(self):
        # default metadata is empty list; no "selected" key → False
        self.assertFalse(self._stream().is_selected())

    def test_check_access_no_path(self):
        stream = self._stream()
        del stream.__class__.path  # remove path attribute
        try:
            result = stream.check_access()
        finally:
            stream.__class__.path = "test-resource"
        self.assertTrue(result)

    def test_check_access_parent_stream_no_error(self):
        stream = self._stream()
        stream.client.get.return_value = {"result": {"elements": []}}
        self.assertTrue(stream.check_access())

    def test_check_access_forbidden_returns_false(self):
        from tap_qualtrics.exceptions import QualtricsForbiddenError
        stream = self._stream()
        stream.client.get.side_effect = QualtricsForbiddenError("403")
        self.assertFalse(stream.check_access())

    def test_check_access_empty_probe_path_returns_true(self):
        """When _make_probe_path returns empty string, check_access returns True."""
        stream = self._stream()
        # Provide a non-None parent_record so _make_probe_path is called
        stream._make_probe_path = lambda rec: ""
        result = stream.check_access(parent_record={"id": "P1"})
        self.assertTrue(result)
        stream.client.get.assert_not_called()

    def test_make_probe_path_returns_path(self):
        stream = self._stream()
        self.assertEqual(stream._make_probe_path({}), "test-resource")

    def test_enrich_sample_identity(self):
        stream = self._stream()
        sample = {"id": 1}
        self.assertEqual(stream._enrich_sample(sample, {}), {"id": 1})

    def test_page_size_capped_at_stream_class(self):
        class SmallStream(FullTableStream):
            tap_stream_id = "small"
            key_properties = ["id"]
            data_key = "result.elements"
            path = "x"
            page_size = 5
        client = _make_client()
        client.page_size = 1000
        stream = SmallStream(client=client, catalog_entry=_make_entry())
        self.assertEqual(stream.page_size, 5)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class TestPagination(unittest.TestCase):

    def _full_table(self, data_key="result.elements"):
        class S(FullTableStream):
            tap_stream_id = "pag_test"
            key_properties = ["id"]
            page_size = 10
            path = "res"

        S.data_key = data_key
        stream = S(client=_make_client(), catalog_entry=_make_entry())
        return stream

    def test_single_page_list(self):
        stream = self._full_table()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1}, {"id": 2}], "nextPage": None}
        }
        records = list(stream._paginate("res"))
        self.assertEqual(len(records), 2)

    def test_multi_page_follows_next_url(self):
        stream = self._full_table()
        stream.client.get.side_effect = [
            {"result": {"elements": [{"id": 1}], "nextPage": "https://q.com/next"}},
            {"result": {"elements": [{"id": 2}], "nextPage": None}},
        ]
        records = list(stream._paginate("res"))
        self.assertEqual(len(records), 2)
        # Second call must use full_url keyword
        _, kw = stream.client.get.call_args_list[1]
        self.assertEqual(kw.get("full_url"), "https://q.com/next")

    def test_dict_record_yielded_as_single(self):
        stream = self._full_table(data_key="result")
        stream.client.get.return_value = {"result": {"id": 99}, "nextPage": None}
        records = list(stream._paginate("res"))
        self.assertEqual(records, [{"id": 99}])

    def test_empty_elements(self):
        stream = self._full_table()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        records = list(stream._paginate("res"))
        self.assertEqual(records, [])


# ---------------------------------------------------------------------------
# FullTableStream.sync
# ---------------------------------------------------------------------------

class TestFullTableSync(unittest.TestCase):

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_writes_records(self, _, mock_wr):
        stream = ConcreteFullTable(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer)
        mock_wr.assert_called_once_with("test_full", {"id": 1})

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=False)
    def test_sync_not_selected_skips_write(self, _, mock_wr):
        stream = ConcreteFullTable(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer)
        mock_wr.assert_not_called()


# ---------------------------------------------------------------------------
# IncrementalStream
# ---------------------------------------------------------------------------

class ConcreteIncremental(IncrementalStream):
    tap_stream_id = "incr_test"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "result.elements"
    path = "res"
    page_size = 100


class TestIncrementalStream(unittest.TestCase):

    def _stream(self):
        return ConcreteIncremental(client=_make_client(), catalog_entry=_make_entry())

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-01-01")
    def test_get_bookmark(self, _):
        self.assertEqual(self._stream().get_bookmark({}), "2021-01-01")

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2020-01-01")
    def test_write_bookmark_advances(self, _):
        state = self._stream().write_bookmark({}, "2021-06-01")
        self.assertEqual(state["bookmarks"]["incr_test"]["updated_at"], "2021-06-01")

    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-06-01")
    def test_write_bookmark_does_not_go_back(self, _):
        state = self._stream().write_bookmark({}, "2020-01-01")
        self.assertEqual(state["bookmarks"]["incr_test"]["updated_at"], "2021-06-01")

    def test_get_records_no_bookmark_no_start_date_param(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1}], "nextPage": None}
        }
        records = list(stream.get_records(bookmark=""))
        self.assertEqual(len(records), 1)
        _, kw = stream.client.get.call_args
        self.assertNotIn("startDate", kw.get("params", {}))

    def test_get_records_with_bookmark_sends_start_date(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        list(stream.get_records(bookmark="2021-01-01"))
        _, kw = stream.client.get.call_args
        self.assertEqual(kw["params"]["startDate"], "2021-01-01")

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_writes_record_above_bookmark(self, _, _bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1, "updated_at": "2021-06-01"}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer)
        mock_wr.assert_called_once()

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-06-01")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_skips_old_record(self, _, _bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1, "updated_at": "2020-01-01"}], "nextPage": None}
        }
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer)
        mock_wr.assert_not_called()

    @patch("tap_qualtrics.streams.abstracts.write_record")
    @patch("tap_qualtrics.streams.abstracts.get_bookmark", return_value="2021-01-01")
    @patch("tap_qualtrics.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_sync_with_child(self, _, _bk, mock_wr):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": 1, "updated_at": "2021-06-01"}], "nextPage": None}
        }
        child = MagicMock()
        child.sync.return_value = 0
        stream.child_to_sync = [child]
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, *a, **kw: r
        stream.sync(state={}, transformer=transformer)
        child.sync.assert_called_once()


# ---------------------------------------------------------------------------
# SurveyChildStream
# ---------------------------------------------------------------------------

class ConcreteSurveyChild(SurveyChildStream):
    tap_stream_id = "survey_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "surveys/{survey_id}/questions"
    page_size = 100


class TestSurveyChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteSurveyChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_parent_id_dict(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "Q1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "SV_abc"}))
        self.assertEqual(records[0]["survey_id"], "SV_abc")

    def test_probe_path_with_id(self):
        stream = self._stream()
        self.assertEqual(stream._make_probe_path({"id": "SV_abc"}), "surveys/SV_abc/questions")

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# DirectoryChildStream
# ---------------------------------------------------------------------------

class ConcreteDirectoryChild(DirectoryChildStream):
    tap_stream_id = "dir_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts"
    page_size = 100


class TestDirectoryChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteDirectoryChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_directory_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_with_directory_id_dict(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "C1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"directoryId": "DIR_1"}))
        self.assertEqual(records[0]["directoryId"], "DIR_1")

    def test_probe_path(self):
        self.assertEqual(
            self._stream()._make_probe_path({"directoryId": "DIR_1"}),
            "directories/DIR_1/contacts",
        )

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")

    def test_enrich_sample(self):
        result = self._stream()._enrich_sample({"id": "C1"}, {"directoryId": "DIR_1"})
        self.assertEqual(result["_directory_id"], "DIR_1")


# ---------------------------------------------------------------------------
# IncrementalDirectoryChildStream
# ---------------------------------------------------------------------------

class ConcreteIncrementalDirChild(IncrementalDirectoryChildStream):
    tap_stream_id = "incr_dir_child_test"
    key_properties = ["id"]
    replication_keys = ["lastModified"]
    data_key = "result.elements"
    path = "directories/{directory_id}/lists"
    page_size = 100


class TestIncrementalDirectoryChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteIncrementalDirChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_directory_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_with_bookmark(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "L1"}], "nextPage": None}
        }
        records = list(
            stream.get_records(parent_id={"directoryId": "DIR_1"}, bookmark="2021-01-01")
        )
        self.assertEqual(records[0]["directoryId"], "DIR_1")
        self.assertEqual(records[0]["_directory_id"], "DIR_1")
        _, kw = stream.client.get.call_args
        self.assertIn("startDate", kw["params"])

    def test_probe_path(self):
        self.assertEqual(
            self._stream()._make_probe_path({"directoryId": "DIR_1"}),
            "directories/DIR_1/lists",
        )

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")

    def test_enrich_sample(self):
        result = self._stream()._enrich_sample({"id": "L1"}, {"directoryId": "DIR_1"})
        self.assertEqual(result["_directory_id"], "DIR_1")


# ---------------------------------------------------------------------------
# MailingListChildStream
# ---------------------------------------------------------------------------

class ConcreteMailingListChild(MailingListChildStream):
    tap_stream_id = "ml_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
    page_size = 100


class TestMailingListChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteMailingListChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_missing_directory_id_empty(self):
        records = list(
            self._stream().get_records(parent_id={"_directory_id": "", "mailingListId": "ML_1"})
        )
        self.assertEqual(records, [])

    def test_missing_mailing_list_id_empty(self):
        records = list(
            self._stream().get_records(parent_id={"_directory_id": "DIR_1", "mailingListId": ""})
        )
        self.assertEqual(records, [])

    def test_with_ids(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "MC1"}], "nextPage": None}
        }
        records = list(
            stream.get_records(parent_id={"_directory_id": "DIR_1", "mailingListId": "ML_1"})
        )
        self.assertEqual(len(records), 1)

    def test_probe_path_with_ids(self):
        path = self._stream()._make_probe_path({"_directory_id": "DIR_1", "mailingListId": "ML_1"})
        self.assertIn("DIR_1", path)
        self.assertIn("ML_1", path)

    def test_probe_path_no_ids(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# GroupChildStream
# ---------------------------------------------------------------------------

class ConcreteGroupChild(GroupChildStream):
    tap_stream_id = "grp_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "groups/{group_id}/members"
    page_size = 100


class TestGroupChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteGroupChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_with_group_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "U1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "GRP_1"}))
        self.assertEqual(records[0]["groupId"], "GRP_1")

    def test_probe_path_with_id(self):
        self.assertEqual(self._stream()._make_probe_path({"id": "GRP_1"}), "groups/GRP_1/members")

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# LibraryChildStream
# ---------------------------------------------------------------------------

class ConcreteLibraryChild(LibraryChildStream):
    tap_stream_id = "lib_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "libraries/{library_id}/messages"
    page_size = 100


class TestLibraryChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteLibraryChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_library_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_with_library_id_dict(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "M1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"libraryId": "LIB_1"}))
        self.assertEqual(records[0]["libraryId"], "LIB_1")

    def test_probe_path(self):
        self.assertEqual(
            self._stream()._make_probe_path({"libraryId": "LIB_1"}),
            "libraries/LIB_1/messages",
        )

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# SampleChildStream
# ---------------------------------------------------------------------------

class ConcreteSampleChild(SampleChildStream):
    tap_stream_id = "sample_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "directories/{directory_id}/samples/{sample_id}/contacts"
    page_size = 100


class TestSampleChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteSampleChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_missing_ids_empty(self):
        records = list(
            self._stream().get_records(parent_id={"_directory_id": "", "sampleId": ""})
        )
        self.assertEqual(records, [])

    def test_with_ids(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "SC1"}], "nextPage": None}
        }
        records = list(
            stream.get_records(parent_id={"_directory_id": "DIR_1", "sampleId": "SMP_1"})
        )
        self.assertEqual(records[0]["sampleId"], "SMP_1")

    def test_probe_path(self):
        path = self._stream()._make_probe_path({"_directory_id": "DIR_1", "sampleId": "SMP_1"})
        self.assertIn("DIR_1", path)
        self.assertIn("SMP_1", path)

    def test_probe_path_no_ids(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# SegmentChildStream
# ---------------------------------------------------------------------------

class ConcreteSegmentChild(SegmentChildStream):
    tap_stream_id = "seg_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segment_id}/contacts"
    page_size = 100


class TestSegmentChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteSegmentChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_missing_ids_empty(self):
        records = list(
            self._stream().get_records(parent_id={"_directory_id": "", "segmentId": ""})
        )
        self.assertEqual(records, [])

    def test_with_ids(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "C1"}], "nextPage": None}
        }
        records = list(
            stream.get_records(parent_id={"_directory_id": "DIR_1", "segmentId": "SEG_1"})
        )
        self.assertEqual(records[0]["segmentId"], "SEG_1")

    def test_probe_path(self):
        path = self._stream()._make_probe_path({"_directory_id": "DIR_1", "segmentId": "SEG_1"})
        self.assertIn("DIR_1", path)
        self.assertIn("SEG_1", path)

    def test_probe_path_no_ids(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# ContactChildStream
# ---------------------------------------------------------------------------

class ConcreteContactChild(ContactChildStream):
    tap_stream_id = "contact_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts/{contact_id}/transactions"
    page_size = 100


class TestContactChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteContactChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_missing_ids_empty(self):
        records = list(
            self._stream().get_records(parent_id={"_directory_id": "", "contactId": ""})
        )
        self.assertEqual(records, [])

    def test_with_ids(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "TX1"}], "nextPage": None}
        }
        records = list(
            stream.get_records(parent_id={"_directory_id": "DIR_1", "contactId": "CT_1"})
        )
        self.assertEqual(len(records), 1)

    def test_probe_path(self):
        path = self._stream()._make_probe_path({"_directory_id": "DIR_1", "contactId": "CT_1"})
        self.assertIn("DIR_1", path)
        self.assertIn("CT_1", path)

    def test_probe_path_no_ids(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


# ---------------------------------------------------------------------------
# TicketChildStream
# ---------------------------------------------------------------------------

class ConcreteTicketChild(TicketChildStream):
    tap_stream_id = "ticket_child_test"
    key_properties = ["id"]
    data_key = "result.elements"
    path = "tickets/{ticket_id}/root-causes"
    page_size = 100


class TestTicketChildStream(unittest.TestCase):

    def _stream(self):
        return ConcreteTicketChild(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_id_empty(self):
        self.assertEqual(list(self._stream().get_records(parent_id=None)), [])

    def test_key_field(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "RC1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"key": "TKT_1"}))
        self.assertEqual(len(records), 1)

    def test_ticket_id_field(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "RC1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"ticketId": "TKT_2"}))
        self.assertEqual(len(records), 1)

    def test_id_field_fallback(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "RC1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "TKT_3"}))
        self.assertEqual(len(records), 1)

    def test_probe_path_key(self):
        path = self._stream()._make_probe_path({"key": "TKT_1"})
        self.assertIn("TKT_1", path)

    def test_probe_path_no_id(self):
        self.assertEqual(self._stream()._make_probe_path({}), "")


if __name__ == "__main__":
    unittest.main()
