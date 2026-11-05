from tap_qualtrics.streams.abstracts import ChildBaseStream


class SurveyTranslations(ChildBaseStream):
    tap_stream_id = "survey_translations"
    key_properties = ["survey_id", "language_code", "element_id"]
    path = "surveys/{survey_id}/translations/{language_code}"
    parent = "survey_languages"
    data_key = "result"
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]

    def get_url_endpoint(self, parent_obj=None):
        return self._make_probe_path(parent_obj)

    def _make_probe_path(self, parent_record):
        if not parent_record:
            return ""
        return self.path.format(
            survey_id=parent_record["survey_id"],
            language_code=parent_record["language_code"],
        )

    def get_records(self, parent_id=None, bookmark=""):
        path = self._make_probe_path(parent_id)
        if not path:
            return
        response = self.client.get(path)
        for element_id, value in response["result"].items():
            yield {
                "survey_id": parent_id["survey_id"],
                "language_code": parent_id["language_code"],
                "element_id": element_id,
                "value": value,
                "lastModified": parent_id["lastModified"],
            }
