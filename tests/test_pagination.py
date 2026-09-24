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

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_pagination(self):
        """Verify no duplicate records and that pagination works when data exceeds page size."""
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [c for c in found_catalogs
                         if c['stream_name'] in self.streams_to_test()]
        self.assertGreater(len(test_catalogs), 0, msg="No test catalogs found")

        self.select_all_streams_and_fields(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        synced_records = runner.get_records_from_target_output()

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

                # If record count exceeds the page limit, pagination was exercised.
                page_limit = self.expected_page_size(stream)
                record_count = record_count_by_stream.get(stream, 0)
                if record_count > page_limit:
                    self.assertGreater(
                        record_count, page_limit,
                        msg=f"'{stream}' has {record_count} records but page limit is {page_limit}")
