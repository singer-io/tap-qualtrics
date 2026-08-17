from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibrariesSurveyQuestions(LibraryChildStream):
    tap_stream_id = "libraries_survey_questions"
    key_properties = ["questionId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/questions"
    parent = "libraries"

