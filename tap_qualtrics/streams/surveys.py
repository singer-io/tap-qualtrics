from tap_qualtrics.streams.abstracts import FullTableStream


class Surveys(FullTableStream):
    """Paginated survey list – each element is emitted; children pull per-survey data."""
    tap_stream_id = "surveys"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "surveys"
    page_size = 100
    children = [
        "survey_definitions",
        "survey_questions",
        "survey_flows",
        "survey_versions",
        "survey_options",
        "survey_quotas",
        "survey_languages",
        "sms_distributions",
        "survey_response_export",
        "distributions",
    ]

