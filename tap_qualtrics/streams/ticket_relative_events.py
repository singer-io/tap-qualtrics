from tap_qualtrics.streams.abstracts import TicketChildStream


class TicketRelativeEvents(TicketChildStream):
    tap_stream_id = "ticket_relative_events"
    key_properties = ["ticketId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "tickets/{ticket_id}/events"
    parent = "tickets_export"
