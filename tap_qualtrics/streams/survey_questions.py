from typing import ClassVar

from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyQuestions(IncrementalSurveyChildStream):
    tap_stream_id = "survey_questions"
    key_properties: ClassVar = ["survey_id", "QuestionID"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModified"]
    data_key = "result.elements"
    response_record_fields: ClassVar = {"QuestionID": str}
    path = "survey-definitions/{survey_id}/questions"
    parent = "surveys"
