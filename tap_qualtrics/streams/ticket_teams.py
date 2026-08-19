from tap_qualtrics.streams.abstracts import FullTableStream


class TicketTeams(FullTableStream):
    tap_stream_id = "ticket_teams"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "ticket-teams"
    page_size = 25
