from tap_qualtrics.streams.abstracts import IncrementalSurveyChildStream


class SurveyQuestions(IncrementalSurveyChildStream):
    tap_stream_id = "survey_questions"
    key_properties = ["survey_id", "QuestionID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result.elements"
    response_record_fields = {"QuestionID": str}
    path = "survey-definitions/{survey_id}/questions"
    parent = "surveys"
