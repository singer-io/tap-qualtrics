from tap_qualtrics.streams.abstracts import ChildBaseStream


class SurveyDefinitions(ChildBaseStream):
    tap_stream_id = "survey_definitions"
    key_properties: ClassVar = ["SurveyID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["LastModified"]
    data_key = "result"
    response_record_fields = {"SurveyID": str, "LastModified": str}
    path = "survey-definitions/{survey_id}"
    parent = "surveys"
    parent_cursor_key = "parent_lastModified"

    def get_url_endpoint(self, parent_obj=None):
        survey_id = (parent_obj or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""

    def _make_probe_path(self, parent_record):
        return self.get_url_endpoint(parent_record)

    def get_records(self, parent_id=None, bookmark=""):
        self.url_endpoint = self.get_url_endpoint(parent_id)
        survey_id = (parent_id or {}).get("id", "")
        for record in super().get_records(parent_id=parent_id, bookmark=bookmark):
            if survey_id:
                record["survey_id"] = survey_id
            yield record

    def modify_object(self, record, parent_record=None):
        survey_id = (parent_record or {}).get("id", "")
        if survey_id:
            record["survey_id"] = survey_id
        return record
