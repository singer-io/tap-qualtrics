from tap_qualtrics.streams.abstracts import FullTableStream


class TicketGroups(FullTableStream):
    tap_stream_id = "ticket_groups"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "ticket-groups"
    page_size = 50
