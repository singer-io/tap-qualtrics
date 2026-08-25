from tap_qualtrics.streams.abstracts import DirectoryChildStream


class DirectoryContacts(DirectoryChildStream):
    """Contacts in a directory (page size 500); has contact_transactions child."""
    tap_stream_id = "directories_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts"
    page_size = 100
    parent = "directories"
    children = ["contact_transactions"]

    def get_records(self, parent_id=None):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        for record in self._paginate(path, {"pageSize": self.page_size}):
            record["directoryId"] = directory_id
            record["_directory_id"] = directory_id
            yield record
