from tap_qualtrics.streams.abstracts import ChildBaseStream


class SurveyVersions(ChildBaseStream):
    tap_stream_id = "survey_versions"
    key_properties = ["surveyID", "versionID"]
    path = "survey-definitions/{survey_id}/versions"
    parent = "surveys"
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]

    def get_url_endpoint(self, parent_obj=None):
        survey_id = (parent_obj or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""

    def _make_probe_path(self, parent_record):
        return self.get_url_endpoint(parent_record)

    def get_records(self, parent_id=None, bookmark=""):
        path = self.get_url_endpoint(parent_id)
        if not path:
            return
        for record in self._paginate(path):
            yield {
                **record["metadata"],
                "surveyID": parent_id["id"],
                "lastModified": parent_id["lastModified"],
            }
