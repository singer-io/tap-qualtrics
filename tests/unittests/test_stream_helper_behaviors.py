import unittest
from unittest.mock import MagicMock, patch

from singer import metadata

from tap_qualtrics.exceptions import QualtricsInternalServerError
from tap_qualtrics.streams.audit_events import AuditEvents
from tap_qualtrics.streams.distribution_history import DistributionHistory
from tap_qualtrics.streams.distribution_links import DistributionLinks
from tap_qualtrics.streams.distributions import Distributions
from tap_qualtrics.streams.mailing_lists import MailingLists
from tap_qualtrics.streams.segment_contacts import SegmentContacts
from tap_qualtrics.streams.segments import Segments
from tap_qualtrics.streams.sms_distributions import SmsDistributions
from tap_qualtrics.streams.survey import Survey
from tap_qualtrics.streams.whatsapp_distributions import WhatsappDistributions


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


class TestStreamHelperBehaviors(unittest.TestCase):

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
            with self.assertRaises(QualtricsInternalServerError):
                list(stream.get_records())

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


class TestWhatsappDistributions(unittest.TestCase):

    def _stream(self):
        return WhatsappDistributions(client=_make_client(), catalog_entry=_make_entry())

    def test_get_records_without_survey_yields_nothing(self):
        stream = self._stream()
        with patch.object(stream, "_paginate") as mock_paginate:
            self.assertEqual(list(stream.get_records(None)), [])
        mock_paginate.assert_not_called()

    def test_get_records_adds_survey_id_and_start_date(self):
        stream = self._stream()
        with patch.object(stream, "_paginate", return_value=iter([{"id": "W1"}])) as mock_paginate:
            records = list(stream.get_records({"id": "SV_1"}, "2024-01-01T00:00:00Z"))

        self.assertEqual(records, [{"id": "W1", "survey_id": "SV_1"}])
        mock_paginate.assert_called_once_with(
            "distributions/whatsapp",
            {"surveyId": "SV_1", "pageSize": 100, "startDate": "2024-01-01T00:00:00Z"},
        )

    def test_get_records_omits_start_date_without_bookmark(self):
        stream = self._stream()
        with patch.object(stream, "_paginate", return_value=iter([])) as mock_paginate:
            list(stream.get_records({"id": "SV_2"}))
        mock_paginate.assert_called_once_with(
            "distributions/whatsapp", {"surveyId": "SV_2", "pageSize": 100}
        )

    def test_sync_passes_bookmark_to_get_records(self):
        stream = self._stream()
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_: record
        records = [{"id": "W1", "sendDate": "2024-02-01T00:00:00Z"}]
        with patch.object(stream, "_paginate", return_value=iter(records)):
            count = stream.sync(state={}, transformer=transformer, parent_id={"id": "SV_1"})
        self.assertEqual(count, 1)

    def test_make_probe_path(self):
        stream = self._stream()
        self.assertEqual(
            stream._make_probe_path({"id": "SV_1"}),
            "distributions/whatsapp?surveyId=SV_1",
        )
        self.assertEqual(stream._make_probe_path({}), "")


class TestDistributionLinksCheckAccess(unittest.TestCase):

    def _stream(self):
        return DistributionLinks(client=_make_client(), catalog_entry=_make_entry())

    def test_no_parent_returns_true_without_probe(self):
        stream = self._stream()
        self.assertTrue(stream.check_access(None))
        stream.client.get.assert_not_called()

    def test_probe_uses_parent_survey_id(self):
        stream = self._stream()
        self.assertTrue(stream.check_access({"id": "D_1", "survey_id": "SV_1"}))
        stream.client.get.assert_called_once_with(
            "distributions/D_1/links", params={"pageSize": 1, "surveyId": "SV_1"}
        )

    def test_probe_falls_back_to_cached_survey_id(self):
        stream = self._stream()
        stream._survey_id = "SV_9"
        stream.check_access({"id": "D_1"})
        stream.client.get.assert_called_once_with(
            "distributions/D_1/links", params={"pageSize": 1, "surveyId": "SV_9"}
        )

    def test_probe_without_survey_id(self):
        stream = self._stream()
        stream.check_access({"id": "D_1"})
        stream.client.get.assert_called_once_with(
            "distributions/D_1/links", params={"pageSize": 1}
        )

    @patch("tap_qualtrics.streams.distribution_links.LOGGER")
    def test_probe_500_is_warning_only(self, mock_logger):
        stream = self._stream()
        stream.client.get.side_effect = QualtricsInternalServerError("500")
        self.assertTrue(stream.check_access({"id": "D_1", "survey_id": "SV_1"}))
        mock_logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
