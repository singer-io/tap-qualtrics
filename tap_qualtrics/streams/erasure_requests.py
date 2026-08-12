from tap_qualtrics.streams.abstracts import FullTableStream


class ErasureRequests(FullTableStream):
    tap_stream_id = "erasure_requests"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "op-erase-personal-data"
    page_size = 100
