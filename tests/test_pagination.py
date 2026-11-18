import math
import re

from tap_tester import connections, runner
from base import QualtricsBaseTest


class QualtricsPaginationTest(QualtricsBaseTest):
    """Custom pagination test — one sync, two focused assertions.

    Assertions:
      1. No duplicate records (by primary key) in any tested stream.
      2. For streams where record count exceeds the API page limit,
         confirm pagination actually occurred.
    """

    @staticmethod
    def name():
        return "tap_tester_qualtrics_pagination_test"

    @staticmethod
    def test_page_size():
        return 2

    def streams_to_test(self):
        return self.expected_stream_names().intersection({"users", "groups", "surveys", "mailing_lists"})

    def get_properties(self, original: bool = True):
        properties = super().get_properties(original=original)
        properties["page_size"] = self.test_page_size()
        return properties

    @staticmethod
    def _stream_request_pattern(stream):
        patterns = {
            "users": re.compile(r"HTTP_REQUEST method=GET url=.*/API/v3/users(?:\?|$)"),
            "groups": re.compile(r"HTTP_REQUEST method=GET url=.*/API/v3/groups(?:\?|$)"),
            "surveys": re.compile(r"HTTP_REQUEST method=GET url=.*/API/v3/surveys(?:\?|$)"),
            "mailing_lists": re.compile(
                r"HTTP_REQUEST method=GET url=.*/API/v3/directories/[^/]+/mailinglists(?:\?|$)"
            ),
        }
        return patterns[stream]

    def _count_api_page_requests(self, logs, stream):
        pattern = self._stream_request_pattern(stream)
        return sum(1 for line in logs if pattern.search(line))

    def test_pagination(self):
        """Verify no duplicate records and assert API pagination requests are exercised."""
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [c for c in found_catalogs
                         if c['stream_name'] in self.streams_to_test()]
        self.assertGreater(len(test_catalogs), 0, msg="No test catalogs found")

        self.select_all_streams_and_fields(conn_id, test_catalogs)

        with self.assertLogs("tap-tester", level="INFO") as captured_logs:
            record_count_by_stream = self.run_and_verify_sync_mode(conn_id)

        api_logs = captured_logs.output
        synced_records = runner.get_records_from_target_output()
        insufficient_streams = []
        exercised_streams = []

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                upserts = [
                    msg['data']
                    for msg in synced_records.get(stream, {}).get('messages', [])
                    if msg.get('action') == 'upsert'
                ]
                pkeys = self.expected_primary_keys(stream)
                pk_tuples = [tuple(r[pk] for pk in sorted(pkeys)) for r in upserts]

                # No duplicate records across pages.
                self.assertEqual(
                    len(pk_tuples), len(set(pk_tuples)),
                    msg=f"Duplicate records found in '{stream}'")

                page_limit = min(self.expected_page_size(stream), self.test_page_size())
                record_count = record_count_by_stream.get(stream, 0)
                if record_count <= page_limit:
                    insufficient_streams.append(
                        "{}: {} record(s) with effective page size {}".format(
                            stream, record_count, page_limit
                        )
                    )
                    continue

                expected_min_pages = math.ceil(record_count / page_limit)
                api_page_requests = self._count_api_page_requests(api_logs, stream)

                self.assertGreaterEqual(
                    api_page_requests,
                    expected_min_pages,
                    msg=(
                        "Expected at least {} API page requests for stream '{}' "
                        "(record_count={}, effective_page_size={}) but observed {}."
                    ).format(
                        expected_min_pages,
                        stream,
                        record_count,
                        page_limit,
                        api_page_requests,
                    ),
                )
                exercised_streams.append(stream)

        if insufficient_streams:
            self.fail(
                "Pagination was not exercised for stream(s): {}. "
                "Seed additional records for these streams or reduce page_size further."
                .format(insufficient_streams)
            )

        self.assertGreater(
            len(exercised_streams),
            0,
            msg="Pagination test did not exercise any stream with multi-page data.",
        )
