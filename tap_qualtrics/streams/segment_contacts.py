from tap_qualtrics.streams.abstracts import SegmentChildStream


class SegmentContacts(SegmentChildStream):
    tap_stream_id = "segment_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segment_id}/contacts"
    page_size = 50
    parent = "segments"

