from base import QualtricsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class QualtricsBookMarkTest(BookmarkTest, QualtricsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "mailinglists": { "creationDate" : "2020-01-01T00:00:00Z"},
            "segments": { "creationDate" : "2020-01-01T00:00:00Z"},
            "surveys": { "lastModified" : "2020-01-01T00:00:00Z"},
            "survey": { "LastModified" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_qualtrics_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)

