from copy import deepcopy
from typing import ClassVar

from singer import metrics, utils, write_record

from tap_qualtrics.streams.abstracts import IncrementalStream


class Surveys(IncrementalStream):
    tap_stream_id = "surveys"
    key_properties: ClassVar = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModified"]
    data_key = "result.elements"
    response_record_fields: ClassVar = {"id": str, "lastModified": str}
    path = "surveys"
    page_size = 100
    children: ClassVar = [
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
        details = [
            child for child in self.child_to_sync if child.tap_stream_id in detail_streams
        ]
        if not details:
            return super().sync(state, transformer, parent_id)

        # A failed traversal must replay all surveys, including those older than
        # an already processed survey; commit bookmarks only after completion.
        pending_state = deepcopy(state)
        parent_bookmark = self.get_bookmark(state, self.tap_stream_id)
        child_parent_cursors = {}
        for child in details:
            cursor_key = getattr(child, "parent_cursor_key", None)
            if cursor_key:
                child_parent_cursors[child.tap_stream_id] = self.get_bookmark(
                    state, child.tap_stream_id, key=cursor_key
                )
            else:
                child_parent_cursors[child.tap_stream_id] = child.get_bookmark(
                    state, child.tap_stream_id
                )
        traversal_bookmark = min(
            parent_bookmark,
            *child_parent_cursors.values(),
            key=utils.strptime_to_utc,
        )
        max_bookmark = parent_bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id, traversal_bookmark):
                record_bookmark = record["lastModified"]
                record_datetime = utils.strptime_to_utc(record_bookmark)
                if record_datetime < utils.strptime_to_utc(traversal_bookmark):
                    continue
                parent_due = record_datetime >= utils.strptime_to_utc(parent_bookmark)
                if parent_due:
                    transformed = transformer.transform(record, self.schema, self.mdata)
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                    counter.increment()
                for child in self.child_to_sync:
                    if child.tap_stream_id in child_parent_cursors:
                        child_due = record_datetime >= utils.strptime_to_utc(
                            child_parent_cursors[child.tap_stream_id]
                        )
                    else:
                        child_due = parent_due
                    if child_due:
                        child.sync(
                            state=pending_state, transformer=transformer, parent_id=record
                        )
                max_bookmark = max(
                    max_bookmark, record_bookmark, key=utils.strptime_to_utc
                )
            count = counter.value

        self.write_bookmark(pending_state, self.tap_stream_id, value=max_bookmark)
        # Empty question lists still complete the backfill for their surveys.
        for child in details:
            cursor_key = getattr(child, "parent_cursor_key", None)
            if cursor_key:
                self.write_bookmark(
                    pending_state,
                    child.tap_stream_id,
                    key=cursor_key,
                    value=max_bookmark,
                )
            else:
                child.write_bookmark(
                    pending_state, child.tap_stream_id, value=max_bookmark
                )
        state.clear()
        state.update(pending_state)
        return count
