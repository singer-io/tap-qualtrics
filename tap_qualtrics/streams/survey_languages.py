from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyLanguages(SurveyChildStream):
    tap_stream_id = "survey_languages"
    key_properties = ["language_code"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "surveys/{survey_id}/languages"
    parent = "surveys"
    children = ["survey_translations"]

    def get_records(self, parent_id=None):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        resp = self.client.get(f"surveys/{survey_id}/languages")
        languages = resp.get("result", {})
        if isinstance(languages, dict):
            for code, name in languages.items():
                yield {"survey_id": survey_id, "language_code": code, "language_name": name}
        elif isinstance(languages, list):
            for lang in languages:
                if isinstance(lang, str):
                    yield {"survey_id": survey_id, "language_code": lang}
                else:
                    lang["survey_id"] = survey_id
                    yield lang
