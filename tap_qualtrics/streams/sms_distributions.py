from tap_qualtrics.streams.abstracts import SurveyChildStream


class SmsDistributions(SurveyChildStream):
    tap_stream_id = "sms_distributions"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    page_size = 100
    parent = "surveys"

    def get_records(self, parent_id=None):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        params = {"surveyId": survey_id, "pageSize": self.page_size}
        yield from self._paginate("distributions/sms", params)
