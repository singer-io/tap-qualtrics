from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyQuestions(SurveyChildStream):
    tap_stream_id = "survey_questions"
    key_properties = ["QuestionID"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "survey-definitions/{survey_id}/questions"
    page_size = 100
    parent = "surveys"

