from tap_qualtrics.streams.abstracts import IncrementalMailingListChildStream


class MailingListBouncedContacts(IncrementalMailingListChildStream):
    tap_stream_id = "mailing_list_bounced_contacts"
    key_properties = ["contactId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/bouncedContacts"
    page_size = 50
    parent = "mailing_lists"
