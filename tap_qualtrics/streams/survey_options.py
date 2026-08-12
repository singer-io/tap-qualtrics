from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyOptions(SurveyChildStream):
    tap_stream_id = "survey_options"
    key_properties = ["SurveyID"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "survey-definitions/{survey_id}/options"
    parent = "surveys"

    def get_records(self, parent_id=None):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        resp = self.client.get(f"survey-definitions/{survey_id}/options")
        record = resp.get("result", {})
        if record:
            record["survey_id"] = survey_id
            yield record

