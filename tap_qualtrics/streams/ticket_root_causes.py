from typing import ClassVar

from tap_qualtrics.streams.abstracts import TicketChildStream


class TicketRootCauses(TicketChildStream):
    tap_stream_id = "ticket_root_causes"
    key_properties: ClassVar = ["ticketId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "tickets/{ticket_id}/rootCauses"
    parent = "tickets"
