from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyDefinitions(IncrementalSurveyChildStream):
    tap_stream_id = "survey_definitions"
    key_properties = ["SurveyID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["LastModified"]
    parent_replication_key = None
    data_key = "result"
    response_record_fields = {"SurveyID": str}
    path = "survey-definitions/{survey_id}"
    parent = "surveys"
