from tap_qualtrics.streams.abstracts import GroupChildStream


class GroupUsers(GroupChildStream):
    tap_stream_id = "group_users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "groups/{group_id}/members"
    parent = "groups"
