from tap_qualtrics.streams.abstracts import FullTableStream


class EventSubscriptions(FullTableStream):
    tap_stream_id = "event_subscriptions"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "eventsubscriptions"
    page_size = 100
