from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyVersions(SurveyChildStream):
    tap_stream_id = "survey_versions"
    key_properties = ["versionId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "survey-definitions/{survey_id}/versions"
    page_size = 100
    parent = "surveys"
