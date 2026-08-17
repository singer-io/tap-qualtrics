from tap_qualtrics.streams.abstracts import IncrementalStream


class SmsDistributions(IncrementalStream):
    tap_stream_id = "sms_distributions"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["sendDate"]
    data_key = "result.elements"
    path = "distributions/sms"
    page_size = 100
    parent = "surveys"

    def get_records(self, parent_id=None, bookmark=""):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        params = {"surveyId": survey_id, "pageSize": self.page_size}
        if bookmark:
            params["startDate"] = bookmark
        yield from self._paginate(self.path, params)
