from tap_qualtrics.streams.abstracts import FullTableStream

class DirectoriesContacts(FullTableStream):
    tap_stream_id = "directories_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts"
    path = "directories"
    children = "['directories_contact']"

