from tap_qualtrics.streams.abstracts import FullTableStream

class Libraries(FullTableStream):
    tap_stream_id = "libraries"
    key_properties = ["libraryId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries"
    children = "['libraries_messages', 'libraries_survey_questions']"

