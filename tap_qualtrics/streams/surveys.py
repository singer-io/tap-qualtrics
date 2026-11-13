from copy import deepcopy

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
        "survey_definitions",
        "survey_questions",
        "survey_flows",
        "survey_quotas",
        "survey_versions",
        "survey_options",
        "survey_languages",
        "sms_distributions",
        "whatsapp_distributions",
        "survey_response_export",
        "distributions",
    ]

    def sync(self, state, transformer, parent_id=None):
        detail_streams = {"survey_definitions", "survey_questions", "survey_flows"}
        if not any(child.tap_stream_id in detail_streams for child in self.child_to_sync):
            return super().sync(state, transformer, parent_id)

        # A failed traversal must replay all surveys, including those older than
        # an already processed survey; commit bookmarks only after completion.
        pending_state = deepcopy(state)
        count = super().sync(pending_state, transformer, parent_id)
        state.clear()
        state.update(pending_state)
        return count
