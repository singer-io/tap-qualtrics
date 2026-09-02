from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)

from tap_qualtrics.exceptions import QualtricsError, QualtricsInternalServerError

LOGGER = get_logger()


def _get_nested(data: Any, key_path: str) -> Any:
    """Navigate a dot-separated path through a nested dict."""
    for key in key_path.split("."):
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data


class BaseStream(ABC):
    """Abstract base class for all Singer streams."""

    tap_stream_id: str = ""
    key_properties: List[str] = []
    replication_method: str = "FULL_TABLE"
    replication_keys: List[str] = []
    http_method: str = "GET"
    path: str = ""
    # dot-path into the response JSON where the list of records lives
    data_key: str = "result.elements"
    # dot-path into the response JSON for the next-page URL/token
    next_page_key: str = "result.nextPage"
    page_size: int = 100
    children: List[str] = []
    parent: Optional[str] = None

    def __init__(self, client=None, catalog_entry=None) -> None:
        self.client = client
        self.catalog_entry = catalog_entry
        self.catalog = None  # full singer.Catalog; set for dynamic-schema streams
        if catalog_entry:
            self.schema = catalog_entry.schema.to_dict()
            self.mdata = metadata.to_map(catalog_entry.metadata)
        else:
            self.schema = {}
            self.mdata = {}
        if client:
            # stream's class-level page_size is an endpoint-specific cap; never exceed it
            self.page_size = min(client.page_size, type(self).page_size)
        self.child_to_sync: List["BaseStream"] = []

    # ------------------------------------------------------------------ #
    # Abstract interface                                                   #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        """Perform a full sync of this stream and return the record count."""

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def is_selected(self) -> bool:
        return bool(metadata.get(self.mdata, (), "selected"))

    def write_schema(self) -> None:
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error writing schema for: %s", self.tap_stream_id)
            raise err

    def check_access(self, parent_record: Optional[Dict] = None) -> bool:
        """Return True if credentials can read this stream. Child streams require a
        parent_record to probe; without one they are assumed accessible."""
        path = getattr(self, "path", None)
        if not path:
            return True
        if self.parent and parent_record is None:
            return True
        try:
            probe = self._make_probe_path(parent_record) if parent_record is not None else path
            if not probe:
                return True
            self.client.get(probe, params={"pageSize": 1})
            return True
        except QualtricsError as exc:
            LOGGER.warning("Access check failed for stream '%s': %s", self.tap_stream_id, exc)
            return False

    def _make_probe_path(self, parent_record: Dict) -> str:  # pylint: disable=unused-argument
        """Build the probe URL for a given parent record (top-level: no substitution needed)."""
        return getattr(self, "path", "")

    def _enrich_sample(self, sample: Dict, parent_record: Dict) -> Dict:  # pylint: disable=unused-argument
        """Inject parent context into a fetched sample so grandchild probes have the IDs they need."""
        return sample

    # ------------------------------------------------------------------ #
    # Pagination                                                           #
    # ------------------------------------------------------------------ #

    def _paginate(self, path: str, params: Optional[Dict] = None) -> Iterator[Any]:
        """Yield records from a paginated GET endpoint.

        Qualtrics returns a full URL in ``result.nextPage``; we follow it
        until it is null.
        """
        params = dict(params or {})
        params.setdefault("pageSize", self.page_size)
        next_url: Optional[str] = None

        while True:
            if next_url:
                response = self.client.get(path, full_url=next_url)
            else:
                response = self.client.get(path, params=params)
            # LOGGER.info("Paginating %s: %s", path, response.get("result", {}))
            records = _get_nested(response, self.data_key)
            if isinstance(records, list):
                yield from records
            elif isinstance(records, dict):
                yield records

            next_url = _get_nested(response, self.next_page_key)
            if not next_url:
                break


class FullTableStream(BaseStream):
    """Full-table stream: dumps all records on every sync."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:  # pylint: disable=unused-argument
        """Override in subclasses to customise how records are fetched."""
        yield from self._paginate(self.path)

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id):
                transformed = transformer.transform(record, self.schema, self.mdata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_id=record)
            return counter.value


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""


    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(state, stream, key or self.replication_keys[0], self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )


    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        params: Dict = {}
        if bookmark:
            params["startDate"] = bookmark
        yield from self._paginate(self.path, params)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_id: Any = None,
    ) -> int:
        """Implementation for `type: Incremental` stream."""
        bookmark = self.get_bookmark(state, self.tap_stream_id)
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            try:
                for record in self.get_records(parent_id, bookmark):
                    transformed = transformer.transform(record, self.schema, self.mdata)
                    record_bk = transformed.get(self.replication_keys[0], "")
                    if record_bk >= bookmark:
                        counter.increment()
                        if self.is_selected():
                            write_record(self.tap_stream_id, transformed)
                        if record_bk > max_bk:
                            max_bk = record_bk
                        for child in self.child_to_sync:
                            child.sync(state=state, transformer=transformer, parent_id=record)
            finally:
                state = self.write_bookmark(state, self.tap_stream_id, value=max_bk)
            return counter.value


# ------------------------------------------------------------------ #
# Specialised base classes                                             #
# ------------------------------------------------------------------ #

class SurveyChildStream(FullTableStream):
    """Stream whose records are fetched once per survey."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        path = self.path.format(survey_id=survey_id)
        for record in self._paginate(path):
            record["survey_id"] = survey_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        survey_id = (parent_record or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""


class IncrementalSurveyChildStream(IncrementalStream):
    """Incremental stream fetched once per survey; uses parent's lastModified as replication key."""

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        survey_id = (parent_id or {}).get("id") or parent_id
        if not survey_id:
            return
        parent_bk = (parent_id or {}).get("lastModified", "") if isinstance(parent_id, dict) else ""
        path = self.path.format(survey_id=survey_id)
        for record in self._paginate(path):
            record["survey_id"] = survey_id
            record["lastModified"] = parent_bk
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        survey_id = (parent_record or {}).get("id", "")
        return self.path.format(survey_id=survey_id) if survey_id else ""


class DirectoryChildStream(FullTableStream):
    """Stream whose records are fetched once per directory."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        for record in self._paginate(path):
            record["directoryId"] = directory_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("directoryId", "")
        return self.path.format(directory_id=directory_id) if directory_id else ""

    def _enrich_sample(self, sample: Dict, parent_record: Dict) -> Dict:
        return {**sample, "_directory_id": (parent_record or {}).get("directoryId", "")}


class IncrementalDirectoryChildStream(IncrementalStream):
    """Incremental stream whose records are fetched once per directory."""

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        params: Dict = {"pageSize": self.page_size}
        if bookmark:
            params["startDate"] = bookmark
        for record in self._paginate(path, params):
            record["directoryId"] = directory_id
            record["_directory_id"] = directory_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("directoryId", "")
        return self.path.format(directory_id=directory_id) if directory_id else ""

    def _enrich_sample(self, sample: Dict, parent_record: Dict) -> Dict:
        return {**sample, "_directory_id": (parent_record or {}).get("directoryId", "")}


class MailingListChildStream(FullTableStream):
    """Stream whose records are fetched once per (directory, mailing-list) pair."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        mailing_list_id = parent_id.get("mailingListId", "")
        if not directory_id or not mailing_list_id:
            return
        path = self.path.format(directory_id=directory_id, mailing_list_id=mailing_list_id)
        yield from self._paginate(path)

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        mailing_list_id = (parent_record or {}).get("mailingListId", "")
        if directory_id and mailing_list_id:
            return self.path.format(directory_id=directory_id, mailing_list_id=mailing_list_id)
        return ""


class IncrementalMailingListChildStream(IncrementalStream):
    """Incremental stream fetched once per (directory, mailing-list) pair; uses parent's lastModifiedDate."""

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        mailing_list_id = parent_id.get("mailingListId", "")
        if not directory_id or not mailing_list_id:
            return
        parent_bk = parent_id.get("lastModifiedDate", "")
        path = self.path.format(directory_id=directory_id, mailing_list_id=mailing_list_id)
        for record in self._paginate(path):
            record["lastModifiedDate"] = parent_bk
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        mailing_list_id = (parent_record or {}).get("mailingListId", "")
        if directory_id and mailing_list_id:
            return self.path.format(directory_id=directory_id, mailing_list_id=mailing_list_id)
        return ""


class GroupChildStream(FullTableStream):
    """Stream whose records are fetched once per group."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        group_id = (parent_id or {}).get("id") or parent_id
        if not group_id:
            return
        path = self.path.format(group_id=group_id)
        for record in self._paginate(path):
            record["groupId"] = group_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        group_id = (parent_record or {}).get("id", "")
        return self.path.format(group_id=group_id) if group_id else ""


class LibraryChildStream(FullTableStream):
    """Stream whose records are fetched once per library."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        library_id = (parent_id or {}).get("libraryId") or parent_id
        if not library_id:
            return
        path = self.path.format(library_id=library_id)
        for record in self._paginate(path):
            record["libraryId"] = library_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        library_id = (parent_record or {}).get("libraryId", "")
        return self.path.format(library_id=library_id) if library_id else ""


class SampleChildStream(FullTableStream):
    """Stream whose records are fetched once per (directory, sample) pair."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        sample_id = parent_id.get("sampleId", parent_id.get("id", ""))
        if not directory_id or not sample_id:
            return
        path = self.path.format(directory_id=directory_id, sample_id=sample_id)
        for record in self._paginate(path):
            record["sampleId"] = sample_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        sample_id = (parent_record or {}).get("sampleId", (parent_record or {}).get("id", ""))
        if directory_id and sample_id:
            return self.path.format(directory_id=directory_id, sample_id=sample_id)
        return ""


class SegmentChildStream(FullTableStream):
    """Stream whose records are fetched once per (directory, segment) pair."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        segment_id = parent_id.get("segmentId", "")
        if not directory_id or not segment_id:
            return
        path = self.path.format(directory_id=directory_id, segment_id=segment_id)
        for record in self._paginate(path):
            record["segmentId"] = segment_id
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        segment_id = (parent_record or {}).get("segmentId", "")
        if directory_id and segment_id:
            return self.path.format(directory_id=directory_id, segment_id=segment_id)
        return ""


class IncrementalSegmentChildStream(IncrementalStream):
    """Incremental stream fetched once per (directory, segment) pair; uses parent's lastModifiedDate."""

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        segment_id = parent_id.get("segmentId", "")
        if not directory_id or not segment_id:
            return
        parent_bk = parent_id.get("lastModifiedDate", "")
        path = self.path.format(directory_id=directory_id, segment_id=segment_id)
        for record in self._paginate(path):
            record["segmentId"] = segment_id
            record["lastModifiedDate"] = parent_bk
            yield record

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        segment_id = (parent_record or {}).get("segmentId", "")
        if directory_id and segment_id:
            return self.path.format(directory_id=directory_id, segment_id=segment_id)
        return ""


class ContactChildStream(FullTableStream):
    """Stream whose records are fetched once per (directory, contact) pair."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        if not parent_id:
            return
        directory_id = parent_id.get("_directory_id", "")
        contact_id = parent_id.get("contactId", "")
        if not directory_id or not contact_id:
            return
        path = self.path.format(directory_id=directory_id, contact_id=contact_id)
        yield from self._paginate(path)

    def _make_probe_path(self, parent_record: Dict) -> str:
        directory_id = (parent_record or {}).get("_directory_id", "")
        contact_id = (parent_record or {}).get("contactId", "")
        if directory_id and contact_id:
            return self.path.format(directory_id=directory_id, contact_id=contact_id)
        return ""


class TicketChildStream(FullTableStream):
    """Stream whose records are fetched once per ticket."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        ticket_id = (parent_id or {}).get("key") or (parent_id or {}).get("ticketId") or (parent_id or {}).get("id") or parent_id
        if not ticket_id:
            return
        path = self.path.format(ticket_id=ticket_id)
        yield from self._paginate(path)

    def _make_probe_path(self, parent_record: Dict) -> str:
        ticket_id = (parent_record or {}).get("key") or (parent_record or {}).get("ticketId") or (parent_record or {}).get("id", "")
        return self.path.format(ticket_id=ticket_id) if ticket_id else ""


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream - mirrors tap-gitlab ChildBaseStream pattern."""

    bookmark_value = None

    def get_url_endpoint(self, parent_obj=None) -> str:
        """Override in subclasses to build the child's URL from the parent record."""
        return ""

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Override to inject parent context (IDs, replication key) into each record."""
        return record

    def get_records(self) -> Iterator[Any]:
        """Paginate self.url_endpoint, which sync() sets before calling this."""
        if not self.url_endpoint:
            return
        try:
            yield from self._paginate(self.url_endpoint)
        except QualtricsInternalServerError:
            LOGGER.warning("Skipping %s for %s: API returned 500", self.tap_stream_id, self.url_endpoint)

    # pylint: disable=access-member-before-definition
    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> str:
        """Singleton bookmark; defaults to '' so all records pass on first run without state."""
        if self.bookmark_value is None:
            bk = get_bookmark(state, stream, key or self.replication_keys[0], None)
            self.bookmark_value = bk if bk is not None else ""
        return self.bookmark_value

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        """Tap-gitlab-style: set URL from parent → fetch → modify → filter → write."""
        bookmark = self.get_bookmark(state, self.tap_stream_id)
        max_bk = bookmark
        self.url_endpoint = self.get_url_endpoint(parent_id)

        with metrics.record_counter(self.tap_stream_id) as counter:
            try:
                for record in self.get_records():
                    record = self.modify_object(record, parent_id)
                    transformed = transformer.transform(record, self.schema, self.mdata)
                    record_bk = transformed.get(self.replication_keys[0], "")
                    if record_bk >= bookmark:
                        counter.increment()
                        if self.is_selected():
                            write_record(self.tap_stream_id, transformed)
                        if record_bk > max_bk:
                            max_bk = record_bk
                        for child in self.child_to_sync:
                            child.sync(state=state, transformer=transformer, parent_id=record)
            finally:
                state = self.write_bookmark(state, self.tap_stream_id, value=max_bk)
                # Keep child bookmarks moving with the parent only when both streams
                # use the same replication key (e.g. segments -> segment_contacts).
                for child in self.child_to_sync:
                    child_keys = getattr(child, "replication_keys", []) or []
                    parent_keys = self.replication_keys or []
                    if child_keys and parent_keys and child_keys[0] == parent_keys[0]:
                        state = child.write_bookmark(
                            state,
                            child.tap_stream_id,
                            value=max_bk,
                        )
            return counter.value