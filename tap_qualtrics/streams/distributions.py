from singer import get_bookmark, metrics, write_bookmark, write_record
from tap_qualtrics.streams.abstracts import IncrementalStream


class Distributions(IncrementalStream):
    tap_stream_id = "distributions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["modifiedDate"]
    data_key = "result.elements"
    path = "distributions"
    page_size = 100
    parent = "surveys"
    children = ["distribution_history", "distribution_links"]

    def get_records(self, parent_id=None, bookmark=""):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        params = {"surveyId": survey_id, "pageSize": self.page_size}
        for record in self._paginate("distributions", params):
            record["survey_id"] = survey_id
            yield record

    def _make_probe_path(self, parent_record):
        survey_id = (parent_record or {}).get("id", "")
        return f"{self.path}?surveyId={survey_id}" if survey_id else ""

    def sync(self, state, transformer, parent_id=None):
        bookmark = get_bookmark(
            state, self.tap_stream_id, self.replication_keys[0], self.client.start_date
        )
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            try:
                for record in self.get_records(parent_id, bookmark):
                    transformed = transformer.transform(record, self.schema, self.mdata)
                    record_bk = transformed.get(self.replication_keys[0], "")
                    if record_bk >= bookmark:
                        if self.is_selected():
                            write_record(self.tap_stream_id, transformed)
                            counter.increment()
                        if record_bk and record_bk > max_bk:
                            max_bk = record_bk
                        for child in self.child_to_sync:
                            child.sync(state=state, transformer=transformer, parent_id=record)
            finally:
                state = write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bk)
            return counter.value
