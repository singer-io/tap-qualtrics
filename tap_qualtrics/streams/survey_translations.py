from tap_qualtrics.streams.abstracts import FullTableStream


class SurveyTranslations(FullTableStream):
    tap_stream_id = "survey_translations"
    key_properties = ["survey_id", "language_code"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    parent = "survey_languages"

    def get_records(self, parent_id=None):
        if not parent_id:
            return
        survey_id = parent_id.get("survey_id", "")
        language_code = parent_id.get("language_code", "")
        if not survey_id or not language_code:
            return
        resp = self.client.get(f"surveys/{survey_id}/translations/{language_code}")
        record = resp.get("result", {})
        if record:
            record["survey_id"] = survey_id
            record["language_code"] = language_code
            yield record
