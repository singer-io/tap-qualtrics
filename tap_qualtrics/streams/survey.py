from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyDefinitions(SurveyChildStream):
    """Full survey definition per survey – replaces old 'survey' stream."""
    tap_stream_id = "survey_definitions"
    key_properties = ["SurveyID"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "survey/{survey_id}"
    parent = "surveys"

    def get_records(self, parent_id=None):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        resp = self.client.get(f"survey/{survey_id}")
        record = resp.get("result", {})
        if record:
            record["_survey_id"] = survey_id
            yield record

