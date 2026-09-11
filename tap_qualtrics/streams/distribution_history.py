from tap_qualtrics.streams.abstracts import ChildBaseStream


class DistributionHistory(ChildBaseStream):
    tap_stream_id = "distribution_history"
    key_properties = ["distributionId", "contactId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["modifiedDate"]
    data_key = "result.elements"
    page_size = 100
    parent = "distributions"

    def get_url_endpoint(self, parent_obj=None):
        distribution_id = (parent_obj or {}).get("id", "")
        return f"distributions/{distribution_id}/history" if distribution_id else ""

    def modify_object(self, record, parent_record=None):
        if isinstance(record, dict) and isinstance(parent_record, dict):
            record["distributionId"] = parent_record.get("id", "")
            record["modifiedDate"] = parent_record.get("modifiedDate")
        return record

    def _make_probe_path(self, parent_record):
        distribution_id = (parent_record or {}).get("id", "")
        return f"distributions/{distribution_id}/history" if distribution_id else ""
