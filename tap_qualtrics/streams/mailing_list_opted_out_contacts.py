from tap_qualtrics.streams.abstracts import MailingListChildStream


class MailingListOptedOutContacts(MailingListChildStream):
    tap_stream_id = "mailing_list_opted_out_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/optedOutContacts"
    page_size = 1000
    parent = "mailing_lists"
