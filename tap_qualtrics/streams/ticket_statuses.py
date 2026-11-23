from typing import ClassVar

from tap_qualtrics.streams.abstracts import FullTableStream


class TicketStatuses(FullTableStream):
    tap_stream_id = "ticket_statuses"
    key_properties: ClassVar = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "tickets/statuses"
