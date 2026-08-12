from tap_qualtrics.streams.abstracts import ContactChildStream


class ContactTransactions(ContactChildStream):
    tap_stream_id = "contact_transactions"
    key_properties = ["transactionId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts/{contact_id}/transactions"
    page_size = 1000
    parent = "contacts"
