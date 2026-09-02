from tap_qualtrics.streams.abstracts import FullTableStream


class Libraries(FullTableStream):
    tap_stream_id = "libraries"
    key_properties = ["libraryId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries"
    page_size = 100
    children = [
        "library_messages",
        "libraries_survey_questions",
        "library_surveys",
        "library_blocks",
    ]
