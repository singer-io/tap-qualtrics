from tap_qualtrics.streams.abstracts import FullTableStream

class DirectoriesContact(FullTableStream):
    tap_stream_id = "directories_contact"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "directories/{directory_id}/contacts/{contactId}"
    path = "directories_contacts"

