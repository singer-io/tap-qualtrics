from tap_qualtrics.streams.abstracts import IncrementalStream


class ErasureRequests(IncrementalStream):
    tap_stream_id = "erasure_requests"
    key_properties = ["requestid"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated"]
    data_key = "result.elements"
    path = "op-erase-personal-data"
    page_size = 100
