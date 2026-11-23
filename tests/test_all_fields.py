from base import QualtricsBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

KNOWN_MISSING_FIELDS = {

}


class QualtricsAllFields(AllFieldsTest, QualtricsBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = KNOWN_MISSING_FIELDS

    @staticmethod
    def name():
        return "tap_tester_qualtrics_all_fields_test"

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

    def test_emitted_records_only_contain_catalog_fields(self):
        """Fail when API-emitted top-level fields are missing from catalog/schema selection."""
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                selected_fields = set(self.selected_fields.get(stream, set()))
                stream_messages = self.synced_records.get(stream, {}).get("messages", [])

                observed_fields = set()
                for message in stream_messages:
                    if message.get("action") != "upsert":
                        continue
                    data = message.get("data")
                    if isinstance(data, dict):
                        observed_fields.update(data.keys())

                unexpected_fields = observed_fields.difference(selected_fields)
                self.assertSetEqual(
                    set(),
                    unexpected_fields,
                    msg=(
                        f"Stream '{stream}' emitted field(s) not present in the selected catalog/schema: {sorted(unexpected_fields)}"
                    ),
                )
