from tap_qualtrics.streams.abstracts import FullTableStream


class PollTicketExport(FullTableStream):
    """Stores ticket-export poll status records (precog dataset: Poll Ticket Export)."""
    tap_stream_id = "poll_ticket_export"
    key_properties = ["exportId"]
    replication_method = "FULL_TABLE"
    data_key = "result"
    path = "ticket-exports"

    def get_records(self, parent_id=None):
        # Returns an empty iterator; poll status is a precog-only concept.
        return iter([])
