from tap_qualtrics.streams.abstracts import FullTableStream

class LibrariesMessages(FullTableStream):
    tap_stream_id = "libraries_messages"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{libraryId}/messages"
    path = "libraries"

