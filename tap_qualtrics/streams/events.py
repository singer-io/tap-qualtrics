from tap_qualtrics.streams.abstracts import FullTableStream

class Events(FullTableStream):
    tap_stream_id = "events"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "logs"

