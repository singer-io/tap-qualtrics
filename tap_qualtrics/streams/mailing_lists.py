from tap_qualtrics.streams.abstracts import DirectoryChildStream


class MailingLists(DirectoryChildStream):
    tap_stream_id = "mailing_lists"
    key_properties = ["mailingListId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists"
    page_size = 1000
    parent = "directories"
    children = [
        "mailing_list_contacts",
        "mailing_list_bounced_contacts",
        "mailing_list_opted_out_contacts",
    ]

    def get_records(self, parent_id=None):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        for record in self._paginate(path, {"pageSize": self.page_size}):
            record["_directory_id"] = directory_id
            yield record

