from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibrarySurveys(LibraryChildStream):
    tap_stream_id = "library_surveys"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/surveys"
    parent = "libraries"
