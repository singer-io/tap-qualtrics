from tap_qualtrics.streams.abstracts import FullTableStream

class Directories(FullTableStream):
    tap_stream_id = "directories"
    key_properties = ["directoryId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories"
    children = "['directories_contacts', 'mailinglists']"

