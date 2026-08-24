from tap_qualtrics.streams.abstracts import IncrementalStream


class Surveys(IncrementalStream):
    tap_stream_id = "surveys"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModified"]
    data_key = "result.elements"
    path = "surveys"
    page_size = 100
    children = [
        "survey",
        "survey_quotas",
        "sms_distributions",
        "survey_response_export",
        "distributions",
    ]

