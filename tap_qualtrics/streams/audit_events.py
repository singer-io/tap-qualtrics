from tap_qualtrics.streams.abstracts import IncrementalStream


class AuditEvents(IncrementalStream):
    """Audit events from GET /logs with date filtering."""
    tap_stream_id = "audit_events"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["createdDate"]
    data_key = "result.elements"
    path = "logs"
    page_size = 1000

    def get_records(self, parent_id=None, bookmark=""):
        import urllib.parse
        params = {"pageSize": self.page_size}
        if bookmark:
            params["startDate"] = urllib.parse.quote(bookmark)
        yield from self._paginate(self.path, params)

