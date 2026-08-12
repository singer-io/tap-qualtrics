from tap_qualtrics.streams.abstracts import DirectoryChildStream


class DirectoryContacts(DirectoryChildStream):
    """All contacts in a directory – paginated (page size 1000)."""
    tap_stream_id = "directory_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts"
    page_size = 1000
    parent = "directories"

