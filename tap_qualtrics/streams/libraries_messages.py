from typing import ClassVar

from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibraryMessages(LibraryChildStream):
    tap_stream_id = "library_messages"
    key_properties: ClassVar = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/messages"
    parent = "libraries"
