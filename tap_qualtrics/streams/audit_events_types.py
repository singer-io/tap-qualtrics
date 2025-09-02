from tap_qualtrics.streams.abstracts import FullTableStream

class AuditEventsTypes(FullTableStream):
    tap_stream_id = "audit_events_types"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "audit-events"

