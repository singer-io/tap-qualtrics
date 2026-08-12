from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibraryBlocks(LibraryChildStream):
    tap_stream_id = "library_blocks"
    key_properties = ["blockId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/blocks"
    parent = "libraries"
