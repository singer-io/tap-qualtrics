from tap_qualtrics.streams.abstracts import FullTableStream


class Tickets(FullTableStream):
    tap_stream_id = "tickets"
    key_properties = ["key"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "tickets"
    page_size = 50
    children = ["ticket_root_causes", "ticket_relative_events"]

    def get_records(self, parent_id=None):
        yield from self._paginate(self.path, {"allTickets": "true", "pageSize": self.page_size})
