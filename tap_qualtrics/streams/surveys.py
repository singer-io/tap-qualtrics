from tap_qualtrics.streams.abstracts import ParentBaseStream

class Surveys(ParentBaseStream):
    tap_stream_id = "surveys"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result.elements"
    path = "surveys"
    children = ["survey", "questions"]

