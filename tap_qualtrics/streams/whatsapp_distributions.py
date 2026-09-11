from tap_qualtrics.streams.abstracts import FullTableStream


class WhatsappDistributions(FullTableStream):
    tap_stream_id = "whatsapp_distributions"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "distributions/whatsapp"
    page_size = 100
