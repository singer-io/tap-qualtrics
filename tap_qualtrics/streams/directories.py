from tap_qualtrics.streams.abstracts import FullTableStream


class Directories(FullTableStream):
    tap_stream_id = "directories"
    key_properties = ["directoryId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories"
    page_size = 5
    children = [
        "contacts",
        "directory_contacts",
        "mailing_lists",
        "segments",
        "samples",
        "sample_definitions",
        "transaction_batches",
        "contact_frequency_rules",
        "opted_out_contacts",
    ]

