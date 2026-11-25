from typing import ClassVar

from tap_qualtrics.streams.abstracts import GroupChildStream


class GroupUsers(GroupChildStream):
    tap_stream_id = "group_users"
    key_properties: ClassVar = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "groups/{group_id}/members"
    parent = "groups"
