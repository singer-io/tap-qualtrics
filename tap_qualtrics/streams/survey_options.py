from tap_qualtrics.streams.abstracts import ChildBaseStream


class SurveyOptions(ChildBaseStream):
    tap_stream_id = "survey_options"
    key_properties = ["survey_id"]
    path = "survey-definitions/{survey_id}/options"
    parent = "surveys"
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result"

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
        yield {
            "survey_id": parent_id["id"],
            "options": response["result"],
            "lastModified": parent_id["lastModified"],
        }
