from tap_qualtrics.streams.abstracts import DirectoryChildStream


class SampleDefinitions(DirectoryChildStream):
    tap_stream_id = "sample_definitions"
    key_properties = ["sampleDefinitionId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/samples/definitions"
    page_size = 10
    parent = "directories"
