from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyFlows(IncrementalSurveyChildStream):
    tap_stream_id = "survey_flows"
    key_properties = ["survey_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result"
    path = "survey-definitions/{survey_id}/flow"
    parent = "surveys"
