"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import QualtricsBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class QualtricsAutomaticFields(MinimumSelectionTest, QualtricsBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_qualtrics_automatic_fields_test"

    def streams_to_test(self):
        selected = self.expected_stream_names()
        exclusions = {
            stream: reason
            for stream, reason in {
                'tickets': 'Ticket fields are not consistently selectable in the shared integration account.',
            }.items()
            if stream in selected
        }
        return self.apply_stream_exclusions(selected, exclusions)
