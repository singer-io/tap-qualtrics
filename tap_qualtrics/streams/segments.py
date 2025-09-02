from tap_qualtrics.streams.abstracts import ChildBaseStream

class Segments(ChildBaseStream):
    tap_stream_id = "segments"
    key_properties = ["segmentId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["creationDate", "lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments"
    parent = "directories"
    bookmark_value = None
    children = ["segment_contacts"]

