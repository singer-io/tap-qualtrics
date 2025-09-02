from tap_qualtrics.streams.abstracts import FullTableStream

class User(FullTableStream):
    tap_stream_id = "user"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "users/{userId}"
    path = "users"

