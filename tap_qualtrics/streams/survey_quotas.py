from typing import ClassVar

from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyQuotas(IncrementalSurveyChildStream):
    tap_stream_id = "survey_quotas"
    key_properties: ClassVar = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModified"]
    data_key = "result.elements"
    path = "surveys/{survey_id}/quotas"
    page_size = 100
    parent = "surveys"
