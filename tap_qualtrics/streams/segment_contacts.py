from tap_qualtrics.streams.abstracts import ChildBaseStream


class SegmentContacts(ChildBaseStream):
    tap_stream_id = "segment_contacts"
    key_properties = ["contactId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments/{segment_id}/contacts"
    page_size = 50
    parent = "segments"

    def get_url_endpoint(self, parent_obj=None):
        directory_id = (parent_obj or {}).get("_directory_id", "")
        segment_id = (parent_obj or {}).get("segmentId", "")
        if not directory_id or not segment_id:
            return ""
        return self.path.format(directory_id=directory_id, segment_id=segment_id)

    def modify_object(self, record, parent_record=None):
        if isinstance(record, dict) and isinstance(parent_record, dict):
            record["segmentId"] = parent_record.get("segmentId", "")
            record["lastModifiedDate"] = parent_record.get("lastModifiedDate", "")
        return record

    def _make_probe_path(self, parent_record):
        directory_id = (parent_record or {}).get("_directory_id", "")
        segment_id = (parent_record or {}).get("segmentId", "")
        if directory_id and segment_id:
            return self.path.format(directory_id=directory_id, segment_id=segment_id)
        return ""
