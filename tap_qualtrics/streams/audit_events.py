from typing import Any, Dict

from singer import Transformer, get_logger, metrics, write_record

from tap_qualtrics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class AuditEvents(FullTableStream):
    tap_stream_id = "audit_events"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "logs"
    page_size = 1000
    parent = "audit_events_types"

    def get_records(self, parent_id=None):
        activity_type = (parent_id or {}).get("name") if isinstance(parent_id, dict) else parent_id
        if not activity_type:
            return
        params = {"pageSize": self.page_size, "activityType": activity_type}
        yield from self._paginate(self.path, params)

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id):
                transformed = transformer.transform(record, self.schema, self.mdata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
        return counter.value

