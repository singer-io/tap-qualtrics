from tap_qualtrics.streams.abstracts import ChildBaseStream


class Survey(ChildBaseStream):
    tap_stream_id = "survey"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result"
    path = "surveys/{survey_id}"
    parent = "surveys"
    bookmark_value = None

    def get_url_endpoint(self, parent_obj=None):
        """Prepare URL endpoint for the survey child stream."""
        survey_id = (parent_obj or {}).get("id") if isinstance(parent_obj, dict) else parent_obj
        return self.path.format(survey_id=survey_id) if survey_id else ""

    def get_records(self, parent_id=None, bookmark=""):
        _ = (parent_id, bookmark)
        if not self.url_endpoint:
            return
        resp = self.client.get(self.url_endpoint)
        record = resp.get(self.data_key, {})
        if record:
            yield record

    def modify_object(self, record, parent_record=None):
        """Inject survey_id from parent into the record."""
        survey_id = (parent_record or {}).get("id", "") if isinstance(parent_record, dict) else ""
        if survey_id:
            record["_survey_id"] = survey_id
        return record

    def _make_probe_path(self, parent_record):
        survey_id = (parent_record or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""
