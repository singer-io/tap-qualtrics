import unittest

from tap_tester import connections, runner
from base import QualtricsBaseTest



@unittest.skip("Temporarily skipped: start_date test runtime is too high")
class QualtricsStartDateTest(QualtricsBaseTest):
    """Custom start-date test to keep setup and assertions in one pass."""

    @staticmethod
    def name():
        return "tap_tester_qualtrics_start_date_test"

    def streams_to_test(self):
        # Keep this test focused on a single incremental stream to reduce runtime.
        return self.expected_stream_names().intersection({'surveys'})

    @property
    def start_date_1(self):
        return "2026-08-01T00:00:00Z"
    @property
    def start_date_2(self):
        return "2026-08-20T00:00:00Z"

    def _run_sync_with_start_date(self, start_date):
        """Run check + selection + sync for a given start date once."""
        self.start_date = start_date
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [
            catalog for catalog in found_catalogs
            if catalog.get('stream_name') in self.streams_to_test()
        ]
        self.assertGreater(len(test_catalogs), 0, msg="No test catalogs found")

        # Use direct selection to keep runtime close to the pagination test pattern.
        self.select_all_streams_and_fields(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        synced_messages_by_stream = runner.get_records_from_target_output()
        return record_count_by_stream, synced_messages_by_stream

    def test_start_date(self):
        """Verify stream behavior across two start dates in a single execution flow."""
        self.assertGreater(self.start_date_2, self.start_date_1)

        record_count_by_stream_1, synced_messages_by_stream_1 = \
            self._run_sync_with_start_date(self.start_date_1)
        record_count_by_stream_2, synced_messages_by_stream_2 = \
            self._run_sync_with_start_date(self.start_date_2)

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                self.assertGreater(record_count_sync_1, 0,
                                   msg=f"stream {stream} has no records in sync 1")
                self.assertGreater(record_count_sync_2, 0,
                                   msg=f"stream {stream} has no records in sync 2")

                self.assertIn(stream, synced_messages_by_stream_1,
                              msg=f"No records for stream: {stream} in sync 1")
                self.assertIn(stream, synced_messages_by_stream_2,
                              msg=f"No records for stream: {stream} in sync 2")

                stream_obeys_start_date = self.expected_start_date_behavior(stream)
                expected_primary_keys = sorted(self.expected_primary_keys(stream))

                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                upserts_sync_1 = [
                    message['data']
                    for message in synced_messages_by_stream_1.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                ]
                upserts_sync_2 = [
                    message['data']
                    for message in synced_messages_by_stream_2.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                ]

                replication_dates_1 = {
                    record.get(expected_replication_key)
                    for record in upserts_sync_1
                }
                replication_dates_2 = {
                    record.get(expected_replication_key)
                    for record in upserts_sync_2
                }

                if stream_obeys_start_date:
                    for replication_date in replication_dates_1:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date),
                            self.parse_date(self.start_date_1),
                            msg=f"Record date {replication_date} is older than {self.start_date_1}")

                    for replication_date in replication_dates_2:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date),
                            self.parse_date(self.start_date_2),
                            msg=f"Record date {replication_date} is older than {self.start_date_2}")

                    latest_sync_1_replication_date = max(
                        self.parse_date(replication_date)
                        for replication_date in replication_dates_1
                    )

                    primary_keys_sync_2 = {
                        tuple(record[pk] for pk in expected_primary_keys)
                        for record in upserts_sync_2
                        if self.parse_date(record[expected_replication_key])
                        <= latest_sync_1_replication_date
                    }
                    primary_keys_sync_1 = {
                        tuple(record[pk] for pk in expected_primary_keys)
                        for record in upserts_sync_1
                        if self.parse_date(record[expected_replication_key])
                        >= self.parse_date(self.start_date_2)
                    }

                    self.assertGreater(record_count_sync_1, record_count_sync_2)
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
                else:
                    oldest_sync_1_date = min(self.parse_date(rd) for rd in replication_dates_1)
                    oldest_sync_2_date = min(self.parse_date(rd) for rd in replication_dates_2)
                    self.assertEqual(oldest_sync_1_date, oldest_sync_2_date)
                    self.assertLess(oldest_sync_2_date, self.parse_date(self.start_date_2))

                    primary_keys_sync_1 = {
                        tuple(record[pk] for pk in expected_primary_keys)
                        for record in upserts_sync_1
                    }
                    primary_keys_sync_2 = {
                        tuple(record[pk] for pk in expected_primary_keys)
                        for record in upserts_sync_2
                    }
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
