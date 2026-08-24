from tap_qualtrics.streams.abstracts import IncrementalDirectoryChildStream


class Segments(IncrementalDirectoryChildStream):
    tap_stream_id = "segments"
    key_properties = ["segmentId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments"
    page_size = 10
    parent = "directories"
    children = ["segment_contacts"]

