from tap_qualtrics.streams.abstracts import FullTableStream

class Question(FullTableStream):
    tap_stream_id = "question"
    key_properties = ["QuestionID"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "/survey-definitions/{surveyId}/questions/{questionId}"
    path = "questions"

