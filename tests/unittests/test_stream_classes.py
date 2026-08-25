"""Tests for specific stream get_records implementations that need coverage."""
import unittest
from unittest.mock import MagicMock

from tap_qualtrics.streams.directories_contacts import DirectoryContacts
from tap_qualtrics.streams.distribution_links import DistributionLinks
from tap_qualtrics.streams.libraries_survey_questions import LibrariesSurveyQuestions
from tap_qualtrics.streams.library_blocks import LibraryBlocks
from tap_qualtrics.streams.library_surveys import LibrarySurveys
from tap_qualtrics.streams.opted_out_contacts import OptedOutContacts
from tap_qualtrics.streams.samples import Samples
from tap_qualtrics.streams.sms_distributions import SmsDistributions
from tap_qualtrics.streams.survey import Survey
from tap_qualtrics.streams.user import User


def _make_client():
    c = MagicMock()
    c.page_size = 100
    c.start_date = "2020-01-01"
    return c


def _make_entry():
    entry = MagicMock()
    entry.schema.to_dict.return_value = {"type": "object", "properties": {}, "additionalProperties": True}
    entry.metadata = []
    return entry


class TestDirectoryContactsGetRecords(unittest.TestCase):

    def _stream(self):
        return DirectoryContacts(client=_make_client(), catalog_entry=_make_entry())

    def test_no_directory_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_directory_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"contactId": "C1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"directoryId": "DIR_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["directoryId"], "DIR_1")
        self.assertEqual(records[0]["_directory_id"], "DIR_1")


class TestDistributionLinksGetRecords(unittest.TestCase):

    def _stream(self):
        return DistributionLinks(client=_make_client(), catalog_entry=_make_entry())

    def test_no_distribution_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_distribution_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"contactId": "C1", "link": "http://..."}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "D_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["distributionId"], "D_1")

    def test_with_survey_id_adds_param(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        list(stream.get_records(parent_id={"id": "D_1", "_survey_id": "SV_1"}))
        _, kw = stream.client.get.call_args
        self.assertEqual(kw["params"].get("surveyId"), "SV_1")


class TestOptedOutContactsGetRecords(unittest.TestCase):

    def _stream(self):
        return OptedOutContacts(client=_make_client(), catalog_entry=_make_entry())

    def test_no_directory_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_directory_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"contactId": "C1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"directoryId": "DIR_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["directoryId"], "DIR_1")


class TestSamplesGetRecords(unittest.TestCase):

    def _stream(self):
        return Samples(client=_make_client(), catalog_entry=_make_entry())

    def test_no_directory_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_directory_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"sampleId": "SMP_1"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"directoryId": "DIR_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["directoryId"], "DIR_1")
        self.assertEqual(records[0]["_directory_id"], "DIR_1")


class TestSmsDistributionsGetRecords(unittest.TestCase):

    def _stream(self):
        return SmsDistributions(client=_make_client(), catalog_entry=_make_entry())

    def test_no_survey_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_survey_id(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [{"id": "SMS_1", "sendDate": "2021-01-01"}], "nextPage": None}
        }
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["survey_id"], "SV_1")

    def test_with_bookmark_adds_start_date(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {"elements": [], "nextPage": None}
        }
        list(stream.get_records(parent_id={"id": "SV_1"}, bookmark="2021-01-01"))
        _, kw = stream.client.get.call_args
        self.assertIn("startDate", kw["params"])


class TestSurveyGetRecords(unittest.TestCase):

    def _stream(self):
        return Survey(client=_make_client(), catalog_entry=_make_entry())

    def test_no_survey_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_survey_id(self):
        stream = self._stream()
        stream.client.get.return_value = {"result": {"SurveyID": "SV_1", "SurveyName": "Test"}}
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_survey_id"], "SV_1")

    def test_empty_result_yields_nothing(self):
        stream = self._stream()
        stream.client.get.return_value = {"result": {}}
        records = list(stream.get_records(parent_id={"id": "SV_1"}))
        self.assertEqual(records, [])


class TestUserGetRecords(unittest.TestCase):

    def _stream(self):
        return User(client=_make_client(), catalog_entry=_make_entry())

    def test_no_user_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_with_user_id(self):
        stream = self._stream()
        stream.client.get.return_value = {"result": {"id": "U1", "email": "test@test.com"}}
        records = list(stream.get_records(parent_id={"id": "U1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "U1")

    def test_empty_result_yields_nothing(self):
        stream = self._stream()
        stream.client.get.return_value = {"result": {}}
        records = list(stream.get_records(parent_id={"id": "U1"}))
        self.assertEqual(records, [])


class TestLibrariesSurveyQuestionsGetRecords(unittest.TestCase):

    def _stream(self):
        return LibrariesSurveyQuestions(client=_make_client(), catalog_entry=_make_entry())

    def test_no_library_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_groups_by_category(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"CategoryA": {"Q1": "What?", "Q2": "Who?"}}],
                "nextPage": None,
            }
        }
        records = list(stream.get_records(parent_id={"libraryId": "LIB_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["category"], "CategoryA")
        self.assertEqual(records[0]["libraryId"], "LIB_1")
        self.assertEqual(len(records[0]["questions"]), 2)

    def test_non_dict_category_skipped(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"SomeKey": "not_a_dict"}],
                "nextPage": None,
            }
        }
        records = list(stream.get_records(parent_id={"libraryId": "LIB_1"}))
        self.assertEqual(records, [])


class TestLibraryBlocksGetRecords(unittest.TestCase):

    def _stream(self):
        return LibraryBlocks(client=_make_client(), catalog_entry=_make_entry())

    def test_no_library_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_groups_by_category(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"BlockCat": {"B1": "Block1"}}],
                "nextPage": None,
            }
        }
        records = list(stream.get_records(parent_id={"libraryId": "LIB_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["category"], "BlockCat")


class TestLibrarySurveysGetRecords(unittest.TestCase):

    def _stream(self):
        return LibrarySurveys(client=_make_client(), catalog_entry=_make_entry())

    def test_no_library_id_empty(self):
        stream = self._stream()
        records = list(stream.get_records(parent_id=None))
        self.assertEqual(records, [])

    def test_groups_by_category(self):
        stream = self._stream()
        stream.client.get.return_value = {
            "result": {
                "elements": [{"SurveyCat": {"SV_1": "Survey 1"}}],
                "nextPage": None,
            }
        }
        records = list(stream.get_records(parent_id={"libraryId": "LIB_1"}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["category"], "SurveyCat")


if __name__ == "__main__":
    unittest.main()
