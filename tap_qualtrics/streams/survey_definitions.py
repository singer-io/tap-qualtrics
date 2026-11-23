from typing import ClassVar

from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyDefinitions(IncrementalSurveyChildStream):
    tap_stream_id = "survey_definitions"
    key_properties: ClassVar = ["SurveyID"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModified"]
    data_key = "result"
    response_record_fields: ClassVar = {"SurveyID": str}
    path = "survey-definitions/{survey_id}"
    parent = "surveys"
