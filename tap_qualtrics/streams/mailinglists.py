from tap_qualtrics.streams.abstracts import ChildBaseStream

class Mailinglists(ChildBaseStream):
    tap_stream_id = "mailinglists"
    key_properties = ["mailingListId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["creationDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists"
    parent = "directories"
    bookmark_value = None

