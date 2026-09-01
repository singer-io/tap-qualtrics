from tap_qualtrics.streams.abstracts import ChildBaseStream


class Segments(ChildBaseStream):
    tap_stream_id = "segments"
    key_properties = ["segmentId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/segments"
    page_size = 10
    parent = "directories"
    children = ["segment_contacts"]

    def get_url_endpoint(self, parent_obj=None):
        directory_id = (parent_obj or {}).get("directoryId", "")
        return f"directories/{directory_id}/segments" if directory_id else ""

    def modify_object(self, record, parent_record=None):
        if isinstance(record, dict) and isinstance(parent_record, dict):
            directory_id = parent_record.get("directoryId", "")
            record["directoryId"] = directory_id
            record["_directory_id"] = directory_id  # grandchild segment_contacts needs _directory_id
        return record

    def _make_probe_path(self, parent_record):
        directory_id = (parent_record or {}).get("directoryId", "")
        return f"directories/{directory_id}/segments" if directory_id else ""
