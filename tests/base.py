import os
from typing import ClassVar

from tap_tester import menagerie, runner
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
            "survey_versions": {
                cls.PRIMARY_KEYS: {"surveyID", "versionID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey_definitions": {
                cls.PRIMARY_KEYS: {"SurveyID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"LastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey_questions": {
                cls.PRIMARY_KEYS: {"survey_id", "QuestionID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey_flows": {
                cls.PRIMARY_KEYS: {"survey_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey_options": {
                cls.PRIMARY_KEYS: {"survey_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1
            },
            "survey_languages": {
                cls.PRIMARY_KEYS: {"survey_id", "language_code"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100
            },
            "survey_translations": {
                cls.PRIMARY_KEYS: {"survey_id", "language_code", "element_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"lastModified"},
                cls.OBEYS_START_DATE: True,
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



    # Fast PR gate: small, stable coverage across FULL_TABLE and INCREMENTAL.
    _STREAMS_FOR_TESTING_PR: ClassVar = {
        'users',
        'groups',
        'tickets',
        'surveys',
        'mailing_lists',
    }

    # Nightly tier: broader coverage for higher confidence and cross-domain behavior.
    _STREAMS_FOR_TESTING_NIGHTLY: ClassVar = {
        'users',
        'groups',
        'tickets',
        'surveys',
        'mailing_lists',
        'audit_events_types',
        'libraries',
        'directories',
        'mailing_list_contacts',
        'segments',
        'survey_definitions',
        'survey_questions',
        'survey_response_export',
        'distributions',
        'erasure_requests',
    }

    _TEST_STREAM_TIERS: ClassVar = {
        'pr': _STREAMS_FOR_TESTING_PR,
        'nightly': _STREAMS_FOR_TESTING_NIGHTLY,
    }

    _TEST_STREAM_GROUPS: ClassVar = {
        'pr-core': {
            'users',
            'groups',
            'surveys',
        },
        'pr-stateful': {
            'mailing_lists',
            'tickets',
        },
        'nightly-core': {
            'users',
            'groups',
            'surveys',
            'mailing_lists',
            'tickets',
        },
        'nightly-catalog': {
            'audit_events_types',
            'libraries',
            'directories',
            'mailing_list_contacts',
            'segments',
        },
        'nightly-incremental': {
            'survey_definitions',
            'survey_questions',
            'survey_response_export',
            'distributions',
            'erasure_requests',
        },
    }

    @classmethod
    def stream_test_tier(cls):
        return os.getenv("TAP_QUALTRICS_TEST_STREAM_TIER", "pr").strip().lower()

    @classmethod
    def stream_exclusion_reasons(cls):
        """Streams excluded for account/entitlement reasons across suites."""
        return {
            'tickets': 'Shared integration account has restricted ticketing access in this environment.',
            'ticket_groups': 'Shared integration account has restricted ticketing access in this environment.',
            'ticket_statuses': 'Shared integration account has restricted ticketing access in this environment.',
            'ticket_teams': 'Shared integration account has restricted ticketing access in this environment.',
            'ticket_root_causes': 'Shared integration account has restricted ticketing access in this environment.',
            'ticket_retrieve_events': 'Shared integration account has restricted ticketing access in this environment.',
        }

    @classmethod
    def apply_stream_exclusions(cls, streams, exclusions):
        missing_reasons = {stream for stream, reason in exclusions.items() if not reason}
        if missing_reasons:
            raise AssertionError(
                f"Every excluded stream must include a non-empty reason: {sorted(missing_reasons)}"
            )
        unknown_streams = set(exclusions).difference(streams)
        if unknown_streams:
            raise AssertionError(
                f"Exclusions reference streams outside the selected set: {sorted(unknown_streams)}"
            )
        return set(streams).difference(exclusions)

    @classmethod
    def expected_stream_names(cls):
        tier = cls.stream_test_tier()
        if tier not in cls._TEST_STREAM_TIERS:
            raise ValueError(
                f"Unsupported TAP_QUALTRICS_TEST_STREAM_TIER '{tier}'. Expected one of: {sorted(cls._TEST_STREAM_TIERS)}"
            )

        selected = set(cls._TEST_STREAM_TIERS[tier])
        exclusions = {
            stream: reason
            for stream, reason in cls.stream_exclusion_reasons().items()
            if stream in selected
        }
        selected = cls.apply_stream_exclusions(selected, exclusions)

        group = os.getenv("TAP_QUALTRICS_TEST_STREAM_GROUP", "").strip().lower()
        if group:
            if group not in cls._TEST_STREAM_GROUPS:
                raise ValueError(
                    f"Unsupported TAP_QUALTRICS_TEST_STREAM_GROUP '{group}'. Expected one of: {sorted(cls._TEST_STREAM_GROUPS)}"
                )
            selected = selected.intersection(cls._TEST_STREAM_GROUPS[group])
            if not selected:
                raise ValueError(
                    f"No streams selected after applying TAP_QUALTRICS_TEST_STREAM_GROUP='{group}' for tier '{tier}'"
                )

        return selected

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            'client_id': 'TAP_QUALTRICS_CLIENT_ID',
            'client_secret': 'TAP_QUALTRICS_CLIENT_SECRET',
            'scope': 'TAP_QUALTRICS_SCOPE',
            'auth_method': 'TAP_QUALTRICS_GRANT_TYPE',
            'data_center': 'TAP_QUALTRICS_DATA_CENTER',
            'access_token': 'TAP_QUALTRICS_ACCESS_TOKEN',
            'refresh_token': 'TAP_QUALTRICS_REFRESH_TOKEN',
            'redirect_uri': 'TAP_QUALTRICS_REDIRECT_URI',
        }

        for cred, env_var in creds.items():
            credentials_dict[cred] = os.getenv(env_var)

        overrides = getattr(QualtricsBaseTest, "_credential_overrides", None) or {}
        for key, value in overrides.items():
            if value is None:
                credentials_dict.pop(key, None)
            else:
                credentials_dict[key] = value

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date
        }

        overrides = getattr(self, "_property_overrides", None) or {}
        for key, value in overrides.items():
            if value is None:
                return_value.pop(key, None)
            else:
                return_value[key] = value

        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @classmethod
    def set_test_config_overrides(cls, credential_overrides=None, property_overrides=None):
        cls._credential_overrides = credential_overrides or {}
        cls._property_overrides = property_overrides or {}

    @classmethod
    def clear_test_config_overrides(cls):
        cls._credential_overrides = {}
        cls._property_overrides = {}

    def run_and_verify_check_mode(self, conn_id):
        """Override to filter discovered catalog to the selected test streams."""
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        all_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(all_catalogs), 0,
                           msg=f"unable to locate schemas for connection {conn_id}")
        selected = self.expected_stream_names()
        found_catalogs = [c for c in all_catalogs if c['stream_name'] in selected]
        missing = selected - {c['stream_name'] for c in found_catalogs}
        self.assertEqual(set(), missing,
                         msg=f"Streams missing from catalog: {missing}")
        return found_catalogs
