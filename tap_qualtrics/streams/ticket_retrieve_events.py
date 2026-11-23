from typing import ClassVar

from tap_qualtrics.streams.abstracts import TicketChildStream


class TicketRetrieveEvents(TicketChildStream):
    tap_stream_id = "ticket_retrieve_events"
    key_properties: ClassVar = ["ownerId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "tickets/{ticket_id}/events"
    parent = "tickets"
