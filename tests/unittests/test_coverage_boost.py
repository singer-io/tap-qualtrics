import runpy
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import Catalog

import tap_qualtrics
from tap_qualtrics.discover import _add_dynamic_entries, discover
from tap_qualtrics.exceptions import QualtricsError, QualtricsInternalServerError
from tap_qualtrics.streams.abstracts import (
    ChildBaseStream,
    IncrementalMailingListChildStream,
    IncrementalSegmentChildStream,
    IncrementalStream,
    IncrementalSurveyChildStream,
)
from tap_qualtrics.streams.audit_events import AuditEvents
from tap_qualtrics.streams.distribution_history import DistributionHistory
from tap_qualtrics.streams.distribution_links import DistributionLinks
from tap_qualtrics.streams.distributions import Distributions
from tap_qualtrics.streams.mailing_lists import MailingLists
from tap_qualtrics.streams.segment_contacts import SegmentContacts
from tap_qualtrics.streams.segments import Segments
from tap_qualtrics.streams.sms_distributions import SmsDistributions
from tap_qualtrics.streams.survey import Survey
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport
from tap_qualtrics.sync import sync


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


class TestInitEntrypoints(unittest.TestCase):

    @patch("tap_qualtrics.json.dump")
    @patch("tap_qualtrics.discover")
    def test_do_discover_emits_catalog(self, mock_discover, mock_dump):
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        tap_qualtrics.do_discover(client=MagicMock())

        mock_discover.assert_called_once()
        mock_dump.assert_called_once()

    @patch("tap_qualtrics.do_discover")
    @patch("tap_qualtrics.Client")
    @patch("singer.utils.parse_args")
    def test_main_discover_branch(self, mock_parse_args, mock_client_cls, mock_do_discover):
        mock_parse_args.return_value = SimpleNamespace(
            config={"x": 1},
            state={"bookmarks": {}},
            discover=True,
            catalog=None,
        )
        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        tap_qualtrics.main()

        mock_do_discover.assert_called_once_with(client)

    @patch("tap_qualtrics.sync")
    @patch("tap_qualtrics.Client")
    @patch("singer.utils.parse_args")
    def test_main_catalog_branch_uses_empty_state_when_missing(
        self, mock_parse_args, mock_client_cls, mock_sync
    ):
        catalog = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        mock_parse_args.return_value = SimpleNamespace(
            config=config,
            state=None,
            discover=False,
            catalog=catalog,
        )
        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        tap_qualtrics.main()

        mock_sync.assert_called_once_with(
            client=client,
            config=config,
            catalog=catalog,
            state={},
        )

    @patch("json.dump")
    @patch("tap_qualtrics.discover.discover")
    @patch("tap_qualtrics.client.Client")
    @patch("singer.utils.parse_args")
    def test_module_main_invokes_main(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        _mock_json_dump,
    ):
        mock_parse_args.return_value = SimpleNamespace(
            config={"x": 1},
            state={},
            discover=True,
            catalog=None,
        )
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        runpy.run_path("tap_qualtrics/__init__.py", run_name="__main__")

        mock_parse_args.assert_called_once()


class ConcreteIncrementalSurveyChild(IncrementalSurveyChildStream):
    tap_stream_id = "incr_survey_child_test"
    key_properties = ["id"]
    replication_keys = ["lastModified"]
    data_key = "result.elements"
    path = "surveys/{survey_id}/children"


class ConcreteIncrementalMailingListChild(IncrementalMailingListChildStream):
    tap_stream_id = "incr_mailing_child_test"
    key_properties = ["id"]
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/records"


class ConcreteIncrementalSegmentChild(IncrementalSegmentChildStream):
    tap_stream_id = "incr_segment_child_test"
    key_properties = ["id"]
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segment_id}/records"


class ConcreteChildBaseDefault(ChildBaseStream):
    tap_stream_id = "child_base_default"
    key_properties = ["id"]
    replication_keys = ["updatedAt"]
    data_key = "result.elements"


class ConcreteChildBaseSync(ChildBaseStream):
    tap_stream_id = "child_base_sync"
    key_properties = ["id"]
    replication_keys = ["updatedAt"]
    data_key = "result.elements"

    def get_url_endpoint(self, _parent_obj=None):
        return "child-base-endpoint"


class NoReplicationIncremental(IncrementalStream):
    tap_stream_id = "no_replication"
    key_properties = ["id"]
    replication_keys = []
    data_key = "result.elements"
    path = "x"

    def sync(self, state, transformer, parent_id=None):
        _ = (state, transformer, parent_id)
        return 0


class TestAbstractCoverageGaps(unittest.TestCase):

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
    def test_child_base_get_records_handles_internal_server_error(self, mock_logger):
        stream = ConcreteChildBaseDefault(client=_make_client(), catalog_entry=_make_entry())
        stream.url_endpoint = "broken"
        with patch.object(
            stream,
            "_paginate",
            side_effect=QualtricsInternalServerError("500"),
        ):
            records = list(stream.get_records(parent_id={"id": "P1"}, bookmark=""))

        self.assertEqual(records, [])
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


class TestStreamEdgeCoverage(unittest.TestCase):

    def test_distribution_history_helpers(self):
        stream = DistributionHistory(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            stream.get_url_endpoint({"id": "D_1"}),
            "distributions/D_1/history",
        )
        record = stream.modify_object({"contactId": "C1"}, {"id": "D_1", "modifiedDate": "2024-01-01"})
        self.assertEqual(record["distributionId"], "D_1")
        self.assertEqual(record["modifiedDate"], "2024-01-01")
        self.assertEqual(stream._make_probe_path({"id": "D_1"}), "distributions/D_1/history")

    def test_distributions_enrich_sample_adds_survey_id_for_children(self):
        stream = Distributions(client=_make_client(), catalog_entry=_make_entry())
        sample = stream._enrich_sample({"id": "D_1"}, {"id": "SV_1"})
        self.assertEqual(sample["survey_id"], "SV_1")

    @patch("tap_qualtrics.streams.distribution_links.LOGGER")
    def test_distribution_links_probe_path_and_500_handling(self, mock_logger):
        stream = DistributionLinks(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(stream._make_probe_path({"id": "D_2"}), "distributions/D_2/links")
        self.assertEqual(stream._make_probe_path({}), "")

        stream.url_endpoint = "distributions/D_2/links"
        with patch.object(stream, "_paginate", side_effect=QualtricsInternalServerError("500")):
            records = list(stream.get_records())

        self.assertEqual(records, [])
        self.assertTrue(mock_logger.warning.called)

    def test_distribution_links_modify_object(self):
        stream = DistributionLinks(client=_make_client(), catalog_entry=_make_entry())
        record = stream.modify_object(
            {"contactId": "C1"},
            {"id": "D_7", "modifiedDate": "2024-06-01T00:00:00Z"},
        )
        self.assertEqual(record["distributionId"], "D_7")
        self.assertEqual(record["modifiedDate"], "2024-06-01T00:00:00Z")

    def test_distribution_links_probe_path_with_survey(self):
        stream = DistributionLinks(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            stream._make_probe_path({"id": "D_2", "survey_id": "SV_2"}),
            "distributions/D_2/links",
        )

    def test_mailing_lists_date_normalization(self):
        stream = MailingLists(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {
                "elements": [
                    {
                        "mailingListId": "ML_1",
                        "creationDate": 1704067200000,
                        "lastModifiedDate": 1706745600000,
                    }
                ],
                "nextPage": None,
            }
        }

        records = list(stream.get_records(parent_id={"directoryId": "DIR_1"}, bookmark=""))

        self.assertEqual(records[0]["creationDate"], "2024-01-01T00:00:00Z")
        self.assertEqual(records[0]["lastModifiedDate"], "2024-02-01T00:00:00Z")

    def test_segments_and_segment_contacts_helpers(self):
        segments = Segments(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            segments.get_url_endpoint({"directoryId": "DIR_9"}),
            "directories/DIR_9/segments",
        )
        seg_record = segments.modify_object({"segmentId": "SEG_1"}, {"directoryId": "DIR_9"})
        self.assertEqual(seg_record["directoryId"], "DIR_9")
        self.assertEqual(seg_record["_directory_id"], "DIR_9")
        self.assertEqual(
            segments._make_probe_path({"directoryId": "DIR_9"}),
            "directories/DIR_9/segments",
        )

        seg_contacts = SegmentContacts(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            seg_contacts.get_url_endpoint({"_directory_id": "DIR_9", "segmentId": "SEG_1"}),
            "directories/DIR_9/segments/SEG_1/contacts",
        )
        self.assertEqual(seg_contacts.get_url_endpoint({"segmentId": "SEG_1"}), "")
        c_record = seg_contacts.modify_object(
            {"contactId": "C1"},
            {"segmentId": "SEG_1", "lastModifiedDate": "2024-01-01"},
        )
        self.assertEqual(c_record["segmentId"], "SEG_1")
        self.assertEqual(c_record["lastModifiedDate"], "2024-01-01")
        self.assertEqual(
            seg_contacts._make_probe_path({"_directory_id": "DIR_9", "segmentId": "SEG_1"}),
            "directories/DIR_9/segments/SEG_1/contacts",
        )
        self.assertEqual(seg_contacts._make_probe_path({"segmentId": "SEG_1"}), "")

    def test_survey_and_distributions_helpers(self):
        survey = Survey(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(survey.get_url_endpoint("SV_1"), "surveys/SV_1")
        self.assertEqual(survey.get_url_endpoint(None), "")

        enriched = survey.modify_object({"id": "SV_1"}, {"id": "SV_1"})
        self.assertEqual(enriched["_survey_id"], "SV_1")
        unchanged = survey.modify_object({"id": "SV_1"}, {})
        self.assertNotIn("_survey_id", unchanged)
        self.assertEqual(survey._make_probe_path({"id": "SV_1"}), "surveys/SV_1")

        distributions = Distributions(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            distributions._make_probe_path({"id": "SV_99"}),
            "distributions?surveyId=SV_99",
        )

    def test_audit_events_accepts_string_parent(self):
        stream = AuditEvents(client=_make_client(), catalog_entry=_make_entry())
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "A1"}], "nextPage": None}
        }

        rows = list(stream.get_records(parent_id="login"))

        self.assertEqual(rows[0]["activity_type"], "login")

    def test_audit_events_probe_path_empty_without_activity_type(self):
        stream = AuditEvents(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(stream._make_probe_path({}), "")

    def test_sms_distributions_probe_path_variants(self):
        stream = SmsDistributions(client=_make_client(), catalog_entry=_make_entry())
        self.assertEqual(
            stream._make_probe_path({"id": "SV_1"}),
            "distributions/sms?surveyId=SV_1",
        )
        self.assertEqual(stream._make_probe_path({}), "")


class TestDiscoverAndSyncGapBranches(unittest.TestCase):

    def test_add_dynamic_entries_marks_replication_key_automatic(self):
        catalog = Catalog([])

        with patch("tap_qualtrics.discover.AuditExport") as mock_audit_export, patch(
            "tap_qualtrics.discover.SurveyResponseExport"
        ) as mock_sre:
            mock_audit_export.tap_stream_id = "audit_export"
            mock_audit_export.replication_method = "INCREMENTAL"
            mock_audit_export.replication_keys = ["eventTime"]
            mock_audit_export.parent = "audit_events_types"
            mock_audit_export.discover_dynamic_entries.return_value = (
                [
                    (
                        "audit_export__login",
                        {
                            "type": "object",
                            "properties": {"eventTime": {"type": "string"}},
                        },
                        ["id"],
                    )
                ],
                [],
            )

            mock_sre.tap_stream_id = "survey_response_export"
            mock_sre.replication_method = "INCREMENTAL"
            mock_sre.replication_keys = ["recordedDate"]
            mock_sre.parent = "surveys"
            mock_sre.discover_dynamic_entries.return_value = ([], [])

            _add_dynamic_entries(MagicMock(), catalog)

        entry = catalog.get_stream("audit_export__login")
        mdata = metadata.to_map(entry.metadata)
        self.assertEqual(
            metadata.get(mdata, ("properties", "eventTime"), "inclusion"),
            "automatic",
        )

    @patch("tap_qualtrics.discover._add_dynamic_entries", return_value=["x"])
    @patch("tap_qualtrics.discover._apply_access_checks", return_value=["y"])
    @patch("tap_qualtrics.discover.LOGGER")
    def test_discover_logs_skipped_summary(self, mock_logger, _mock_access, _mock_dynamic):
        discover(client=MagicMock())
        self.assertTrue(mock_logger.info.called)

    @patch("tap_qualtrics.sync.write_schema")
    @patch("tap_qualtrics.sync.update_currently_syncing")
    @patch("singer.write_state")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_sync_exception_path_writes_state_then_raises(
        self,
        _mock_currently_syncing,
        mock_transformer_cls,
        mock_write_state,
        _mock_update_syncing,
        _mock_write_schema,
    ):
        failing_cls = MagicMock()
        failing_stream = MagicMock()
        failing_stream.parent = None
        failing_stream.children = []
        failing_stream.child_to_sync = []
        failing_stream.sync.side_effect = RuntimeError("boom")
        failing_cls.return_value = failing_stream

        catalog = MagicMock()
        selected = MagicMock()
        selected.stream = "users"
        catalog.get_selected_streams.return_value = [selected]
        catalog.get_stream.return_value = MagicMock()

        with patch("tap_qualtrics.sync.STREAMS", {"users": failing_cls}):
            with self.assertRaises(RuntimeError):
                sync(MagicMock(), {}, catalog, {})

        self.assertTrue(mock_write_state.called)

    @patch("tap_qualtrics.streams.survey_response_export.LOGGER")
    @patch("singer.write_bookmark")
    @patch("tap_qualtrics.streams.survey_response_export.get_bookmark", return_value="")
    @patch("tap_qualtrics.streams.survey_response_export.write_schema")
    def test_survey_response_export_sync_handles_qualtrics_error(
        self,
        _mock_write_schema,
        _mock_get_bookmark,
        _mock_write_bookmark,
        mock_logger,
    ):
        stream = SurveyResponseExport(client=_make_client(), catalog_entry=_make_entry())
        dynamic_id = "survey_response_export__SV_1"
        cat_entry = MagicMock()
        cat_entry.schema.to_dict.return_value = {"type": "object", "properties": {}}
        cat_entry.metadata = metadata.to_list(metadata.write(metadata.new(), (), "selected", True))

        stream.catalog = MagicMock()
        stream.catalog.get_stream.return_value = cat_entry

        transformer = MagicMock()
        with patch.object(stream, "get_records", side_effect=QualtricsError("api-error")):
            count = stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})

        self.assertEqual(count, 0)
        self.assertTrue(mock_logger.warning.called)


if __name__ == "__main__":
    unittest.main()
