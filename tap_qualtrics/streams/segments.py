from tap_qualtrics.streams.abstracts import DirectoryChildStream


class Segments(DirectoryChildStream):
    tap_stream_id = "segments"
    key_properties = ["segmentId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/segments"
    page_size = 20
    parent = "directories"
    children = ["segment_contacts"]

    def get_records(self, parent_id=None):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        for record in self._paginate(path, {"pageSize": self.page_size}):
            record["_directory_id"] = directory_id
            yield record

