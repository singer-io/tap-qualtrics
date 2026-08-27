from tap_qualtrics.streams.abstracts import IncrementalStream


class Survey(IncrementalStream):
    """Full survey definition per survey – replaces old 'survey' stream."""
    tap_stream_id = "survey"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result"
    path = "surveys/{survey_id}"
    parent = "surveys"

    def get_records(self, parent_id=None, bookmark: str = ""):
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        resp = self.client.get(f"surveys/{survey_id}")
        record = resp.get("result", {})
        if record:
            record["_survey_id"] = survey_id
            yield record
