from typing import ClassVar

from tap_qualtrics.streams.abstracts import IncrementalMailingListChildStream


class MailingListOptedOutContacts(IncrementalMailingListChildStream):
    tap_stream_id = "mailing_list_opted_out_contacts"
    key_properties: ClassVar = ["contactId"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists/{mailing_list_id}/optedOutContacts"
    page_size = 50
    parent = "mailing_lists"
