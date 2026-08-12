from tap_qualtrics.streams.abstracts import MailingListChildStream


class MailingListBouncedContacts(MailingListChildStream):
    tap_stream_id = "mailing_list_bounced_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/bouncedContacts"
    page_size = 1000
    parent = "mailing_lists"
