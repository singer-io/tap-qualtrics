from base import QualtricsBaseTest
from tap_tester import connections, menagerie, runner


class QualtricsBookMarkTest(QualtricsBaseTest):
    """Custom bookmark test for mailing_lists (INCREMENTAL stream).

    Runs two syncs and asserts:
      1. Sync 1 writes a bookmark.
      2. Sync 2 only returns records >= that bookmark.
      3. Sync 2 has fewer or equal records than sync 1.
    """
    # Recent start_date keeps audit_export discovery window small (fast discovery).
    start_date = "2026-08-20T00:00:00Z"

    STREAM = "mailing_lists"
    REPLICATION_KEY = "lastModifiedDate"
    INITIAL_BOOKMARK = "2026-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_qualtrics_bookmark_test"

    def streams_to_test(self):
        return {self.STREAM}

    def test_bookmark(self):
        """Verify mailing_lists writes a bookmark and sync 2 respects it."""
        conn_id = connections.ensure_connection(self)

        # Discovery — our override filters catalog to the 5 selected streams.
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [c for c in found_catalogs
                         if c['stream_name'] in self.streams_to_test()]
        self.assertGreater(len(test_catalogs), 0,
                           msg=f"Stream '{self.STREAM}' not found in catalog")

        self.select_all_streams_and_fields(conn_id, test_catalogs)

        # --- Sync 1: seed with a known start bookmark ---
        menagerie.set_state(conn_id, {
            "bookmarks": {self.STREAM: {self.REPLICATION_KEY: self.INITIAL_BOOKMARK}}
        })
        self.run_sync_mode(conn_id)
        records_1 = self._upsert_records(runner.get_records_from_target_output())
        state_1 = menagerie.get_state(conn_id)

        # Verify a bookmark was written
        bookmark_1 = (state_1.get("bookmarks", {})
                             .get(self.STREAM, {})
                             .get(self.REPLICATION_KEY))
        self.assertIsNotNone(bookmark_1, "Sync 1 did not write a bookmark")
        self.assertGreaterEqual(
            bookmark_1, self.INITIAL_BOOKMARK,
            msg=f"Bookmark {bookmark_1!r} is earlier than initial bookmark {self.INITIAL_BOOKMARK!r}")

        # --- Sync 2: start from the bookmark written by sync 1 ---
        menagerie.set_state(conn_id, state_1)
        self.run_sync_mode(conn_id)
        records_2 = self._upsert_records(runner.get_records_from_target_output())
        state_2 = menagerie.get_state(conn_id)

        # Sync 2 must not return more records than sync 1 (bookmark was respected).
        self.assertLessEqual(
            len(records_2), len(records_1),
            msg=f"Sync 2 ({len(records_2)}) returned more records than sync 1 ({len(records_1)})")

        # Every sync 2 record must have replication key >= bookmark from sync 1.
        bookmark_dt = self.parse_date(bookmark_1)
        for record in records_2:
            record_dt = self.parse_date(record.get(self.REPLICATION_KEY))
            self.assertGreaterEqual(
                record_dt, bookmark_dt,
                msg=f"Sync 2 record pre-dates bookmark: {record_dt} < {bookmark_dt}")

        # Verify a new bookmark was written after sync 2.
        bookmark_2 = (state_2.get("bookmarks", {})
                             .get(self.STREAM, {})
                             .get(self.REPLICATION_KEY))
        self.assertIsNotNone(bookmark_2, "Sync 2 did not write a bookmark")

    # -------------------------------------------------------------------------

    def _upsert_records(self, target_output):
        """Return upsert-action record data list for the test stream."""
        return [
            msg["data"]
            for msg in target_output.get(self.STREAM, {}).get("messages", [])
            if msg.get("action") == "upsert"
        ]
