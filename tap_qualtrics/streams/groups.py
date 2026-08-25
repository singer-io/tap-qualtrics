from tap_qualtrics.streams.abstracts import FullTableStream


class Groups(FullTableStream):
    tap_stream_id = "groups"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "groups"
    page_size = 100
    children = ["group_users"]
