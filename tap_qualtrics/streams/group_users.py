from tap_qualtrics.streams.abstracts import FullTableStream

class GroupUsers(FullTableStream):
    tap_stream_id = "group_users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "groups/{groupId}/members"
    path = "groups"

