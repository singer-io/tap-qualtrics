from typing import ClassVar

from tap_qualtrics.streams.abstracts import FullTableStream


class AuditExportEventTypes(FullTableStream):
    tap_stream_id = "audit_export_event_types"
    key_properties: ClassVar = ["name"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "audit-events"
    children: ClassVar = ["audit_export"]
