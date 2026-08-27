import os
from datetime import datetime as dt

from tap_tester import connections, menagerie, runner
from tap_tester.base_suite_tests.base_case import BaseCase


class QualtricsBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2026-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-qualtrics"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.qualtrics"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "user": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "groups": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "group_users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "audit_events_types": {
                cls.PRIMARY_KEYS: {"name"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "audit_events": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000
            },
            "audit_export_event_types": {
                cls.PRIMARY_KEYS: {"name"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "audit_export": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "libraries": {
                cls.PRIMARY_KEYS: {"libraryId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "library_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "libraries_survey_questions": {
                cls.PRIMARY_KEYS: {"libraryId", "category"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "library_blocks": {
                cls.PRIMARY_KEYS: {"libraryId", "category"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "library_surveys": {
                cls.PRIMARY_KEYS: {"libraryId", "category"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "event_subscriptions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "directories": {
                cls.PRIMARY_KEYS: {"directoryId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 5
            },
            "directories_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "contact_frequency_rules": {
                cls.PRIMARY_KEYS: {"ruleId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 500
            },
            "contact_transactions": {
                cls.PRIMARY_KEYS: {"transactionId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 200
            },
            "opted_out_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "transaction_batches": {
                cls.PRIMARY_KEYS: {"batchId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 200
            },
            "samples": {
                cls.PRIMARY_KEYS: {"sampleId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "sample_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "sample_definitions": {
                cls.PRIMARY_KEYS: {"sampleDefinitionId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 10
            },
            "mailing_lists": {
                cls.PRIMARY_KEYS: {"mailingListId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModifiedDate"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1000
            },
            "mailing_list_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1000
            },
            "mailing_list_bounced_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 50
            },
            "mailing_list_opted_out_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 50
            },
            "segments": {
                cls.PRIMARY_KEYS: {"segmentId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModifiedDate"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 10
            },
            "segment_contacts": {
                cls.PRIMARY_KEYS: {"contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 50
            },
            "surveys": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey": {
                cls.PRIMARY_KEYS: {"SurveyID"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "survey_quotas": {
                cls.PRIMARY_KEYS: {"quotaId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "survey_response_export": {
                cls.PRIMARY_KEYS: {"responseId"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"recorded_date"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "distributions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"modifiedDate"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "distribution_history": {
                cls.PRIMARY_KEYS: {"distributionId", "contactId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "distribution_links": {
                cls.PRIMARY_KEYS: {"contactId", "link"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "sms_distributions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"sendDate"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "whatsapp_distributions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "erasure_requests": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "tickets": {
                cls.PRIMARY_KEYS: {"key"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 50
            },
            "ticket_groups": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 50
            },
            "ticket_statuses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "ticket_teams": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 25
            },
            "ticket_root_causes": {
                cls.PRIMARY_KEYS: {"ticketId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "ticket_retrieve_events": {
                cls.PRIMARY_KEYS: {"ticketId"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
        }



    # Five reliably-accessible streams covering FULL_TABLE and INCREMENTAL types.
    _STREAMS_FOR_TESTING = {
        'users',
        'groups',
        'tickets',
        'surveys',
        'mailing_lists',
    }

    @classmethod
    def expected_stream_names(cls):
        return cls._STREAMS_FOR_TESTING

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {'clientId': 'TAP_QUALTRICS_CLIENT_ID', 'clientSecret': 'TAP_QUALTRICS_CLIENT_SECRET', 
                 'scope': 'TAP_QUALTRICS_SCOPE', 'grant_type': 'TAP_QUALTRICS_GRANT_TYPE', 'dataCenter': 'TAP_QUALTRICS_DATA_CENTER'}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def run_and_verify_check_mode(self, conn_id):
        """Override to filter discovered catalog to the selected test streams."""
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        all_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(all_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))
        selected = self.expected_stream_names()
        found_catalogs = [c for c in all_catalogs if c['stream_name'] in selected]
        missing = selected - {c['stream_name'] for c in found_catalogs}
        self.assertEqual(set(), missing,
                         msg="Streams missing from catalog: {}".format(missing))
        return found_catalogs
