from tap_qualtrics.streams.abstracts import FullTableStream


class User(FullTableStream):
    """Individual user detail – child of Users."""
    tap_stream_id = "user"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    parent = "users"

    def get_records(self, parent_id=None):
        user_id = (parent_id or {}).get("id") or parent_id
        if not user_id:
            return
        resp = self.client.get(f"users/{user_id}")
        record = resp.get("result", {})
        if record:
            yield record
