from tap_qualtrics.streams.abstracts import DirectoryChildStream


class Samples(DirectoryChildStream):
    tap_stream_id = "samples"
    key_properties = ["sampleId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/samples"
    page_size = 100
    parent = "directories"
    children = ["sample_contacts"]

    def get_records(self, parent_id=None):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        for record in self._paginate(path, {"pageSize": self.page_size}):
            record["_directory_id"] = directory_id
            yield record
