from tap_qualtrics.streams.abstracts import FullTableStream

class LibrariesSurveyQuestions(FullTableStream):
    tap_stream_id = "libraries_survey_questions"
    key_properties = ["questionId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{libraryId}/survey/questions"
    path = "libraries"

