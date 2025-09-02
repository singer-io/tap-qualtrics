from tap_qualtrics.streams.abstracts import FullTableStream

class SegmentContacts(FullTableStream):
    tap_stream_id = "segment_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segmentId}/contacts"
    path = "segments"

