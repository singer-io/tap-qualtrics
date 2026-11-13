from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyDefinitions(IncrementalSurveyChildStream):
    tap_stream_id = "survey_definitions"
    key_properties = ["SurveyID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result"
    path = "survey-definitions/{survey_id}"
    parent = "surveys"
