from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyQuotas(SurveyChildStream):
    tap_stream_id = "survey_quotas"
    key_properties = ["quotaId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "survey/{survey_id}/quotas"
    page_size = 100
    parent = "surveys"

