
from base import QualtricsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class QualtricsInterruptedSyncTest(InterruptedSyncTest, QualtricsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_qualtrics_interrupted_sync_test"

    def streams_to_test(self):
        selected = self.expected_stream_names()
        exclusions = {
            stream: reason
            for stream, reason in {
                'users': 'Interrupted sync semantics are only validated on incremental streams.',
                'groups': 'Interrupted sync semantics are only validated on incremental streams.',
                'tickets': 'Interrupted sync semantics are only validated on incremental streams.',
            }.items()
            if stream in selected
        }
        return self.apply_stream_exclusions(selected, exclusions)


    def manipulate_state(self):
        return {
            "currently_syncing": "surveys",
            "bookmarks": {
                "surveys": { "lastModified" : "2026-08-19T00:00:00Z"},
                "mailing_lists": {"lastModifiedDate": "2026-08-19T00:00:00.00Z"}
        }
    }
