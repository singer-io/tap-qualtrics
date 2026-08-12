from tap_qualtrics.streams.abstracts import FullTableStream


class DistributionHistory(FullTableStream):
    tap_stream_id = "distribution_history"
    key_properties = ["distributionId", "contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    page_size = 100
    parent = "distributions"

    def get_records(self, parent_id=None):
        distribution_id = (parent_id or {}).get("id") or parent_id
        if not distribution_id:
            return
        path = f"distributions/{distribution_id}/history"
        yield from self._paginate(path, {"pageSize": self.page_size})
