from tap_qualtrics.streams.abstracts import FullTableStream

class Questions(FullTableStream):
    tap_stream_id = "questions"
    key_properties = ["QuestionID"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "/survey-definitions/{surveyId}/questions"
    path = "survey"

