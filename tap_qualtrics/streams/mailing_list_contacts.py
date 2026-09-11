from tap_qualtrics.streams.abstracts import IncrementalMailingListChildStream


class MailingListContacts(IncrementalMailingListChildStream):
    tap_stream_id = "mailing_list_contacts"
    key_properties = ["contactId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
    page_size = 1000
    parent = "mailing_lists"
