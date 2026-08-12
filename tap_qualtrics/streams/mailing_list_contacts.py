from tap_qualtrics.streams.abstracts import MailingListChildStream


class MailingListContacts(MailingListChildStream):
    tap_stream_id = "mailing_list_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
    page_size = 1000
    parent = "mailing_lists"
