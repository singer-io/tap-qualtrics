from tap_qualtrics.streams.abstracts import FullTableStream


class Directories(FullTableStream):
    tap_stream_id = "directories"
    key_properties = ["directoryId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories"
    page_size = 5
    children = [
        "directories_contacts",
        "contact_frequency_rules",
        "mailing_lists",
        "opted_out_contacts",
        "sample_definitions",
        "samples",
        "segments",
        "transaction_batches"
    ]
