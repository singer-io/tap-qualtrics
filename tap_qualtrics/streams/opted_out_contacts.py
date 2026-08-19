from tap_qualtrics.streams.abstracts import DirectoryChildStream


class OptedOutContacts(DirectoryChildStream):
    tap_stream_id = "opted_out_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts/optedOutContacts"
    page_size = 100
    parent = "directories"

    def get_records(self, parent_id=None):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        yield from self._paginate(path, {"pageSize": self.page_size})

