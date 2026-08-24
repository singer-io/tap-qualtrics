from tap_qualtrics.streams.abstracts import IncrementalDirectoryChildStream


class MailingLists(IncrementalDirectoryChildStream):
    tap_stream_id = "mailing_lists"
    key_properties = ["mailingListId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists"
    page_size = 1000
    parent = "directories"
    children = [
        "mailing_list_contacts",
        "mailing_list_bounced_contacts",
        "mailing_list_opted_out_contacts",
    ]

