from tap_qualtrics.streams.abstracts import ChildBaseStream


class SurveyLanguages(ChildBaseStream):
    tap_stream_id = "survey_languages"
    key_properties = ["survey_id", "language_code"]
    path = "surveys/{survey_id}/languages"
    parent = "surveys"
    children = ["survey_translations"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result.AvailableLanguages"

    def get_url_endpoint(self, parent_obj=None):
        survey_id = (parent_obj or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""

    def _make_probe_path(self, parent_record):
        return self.get_url_endpoint(parent_record)

    def get_records(self, parent_id=None, bookmark=""):
        path = self._make_probe_path(parent_id)
        if not path:
            return
        response = self.client.get(path)
        for language_code in response["result"]["AvailableLanguages"]:
            yield {
                "survey_id": parent_id["id"],
                "language_code": language_code,
                "lastModified": parent_id["lastModified"],
            }

    def _enrich_sample(self, sample, parent_record):
        return {"survey_id": parent_record["id"], "language_code": sample}
