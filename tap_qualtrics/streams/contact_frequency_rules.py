from tap_qualtrics.streams.abstracts import DirectoryChildStream


class ContactFrequencyRules(DirectoryChildStream):
    tap_stream_id = "contact_frequency_rules"
    key_properties = ["ruleId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/frequencyrules"
    page_size = 500
    parent = "directories"
