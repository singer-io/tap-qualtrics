from typing import ClassVar

from tap_qualtrics.streams.abstracts import FullTableStream


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties: ClassVar = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "users"
    page_size = 100
    children: ClassVar = ["user"]
