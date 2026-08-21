import time
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

from tap_qualtrics.exceptions import QualtricsForbiddenError, QualtricsNotFoundError

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
            self.page_size = client.page_size
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

    def check_access(self) -> bool:
        """Return True if credentials have read access to this stream; False on 403.
        Child streams always return True — access is governed by the parent check.
        Streams with no simple GET path (e.g. async export streams) are assumed accessible."""
        if self.parent:
            return True
        path = getattr(self, "path", None)
        if not path:
            return True
        try:
            self.client.get(path, params={"pageSize": 1})
            return True
        except (QualtricsForbiddenError, QualtricsNotFoundError) as exc:
            LOGGER.warning("Access check failed for stream '%s': %s", self.tap_stream_id, exc)
            return False

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

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        """Override in subclasses to customise how records are fetched."""
        path = self._build_path(parent_id)
        yield from self._paginate(path)

    def _build_path(self, parent_id: Any = None) -> str:
        """Return the URL path for this stream, optionally parameterised."""
        return self.path  # subclasses override or format with parent_id

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
    """Incremental stream: uses a bookmark to track progress."""

    def get_bookmark(self, state: Dict, key: Any = None) -> str:
        return get_bookmark(
            state,
            self.tap_stream_id,
            key or self.replication_keys[0],
            self.client.start_date,
        )

    def write_bookmark(self, state: Dict, value: str, key: Any = None) -> Dict:
        bk = key or self.replication_keys[0]
        current = get_bookmark(state, self.tap_stream_id, bk, self.client.start_date)
        value = max(current, value)
        return write_bookmark(state, self.tap_stream_id, bk, value)

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        path = self._build_path(parent_id)
        params: Dict = {}
        if bookmark:
            params["startDate"] = bookmark
        yield from self._paginate(path, params)

    def _build_path(self, parent_id: Any = None) -> str:
        return self.path

    def sync(self, state: Dict, transformer: Transformer, parent_id: Any = None) -> int:
        bookmark = self.get_bookmark(state)
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id, bookmark):
                transformed = transformer.transform(record, self.schema, self.mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    if record_bk > max_bk:
                        max_bk = record_bk
                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_id=record)
        state = self.write_bookmark(state, max_bk)
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
        yield from self._paginate(path)


class DirectoryChildStream(FullTableStream):
    """Stream whose records are fetched once per directory."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        yield from self._paginate(path)


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
            record["_directory_id"] = directory_id
            yield record


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


class GroupChildStream(FullTableStream):
    """Stream whose records are fetched once per group."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        group_id = (parent_id or {}).get("id") or parent_id
        if not group_id:
            return
        path = self.path.format(group_id=group_id)
        yield from self._paginate(path)


class LibraryChildStream(FullTableStream):
    """Stream whose records are fetched once per library."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        library_id = (parent_id or {}).get("libraryId") or parent_id
        if not library_id:
            return
        path = self.path.format(library_id=library_id)
        yield from self._paginate(path)


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
        yield from self._paginate(path)


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
        yield from self._paginate(path)


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


class TicketChildStream(FullTableStream):
    """Stream whose records are fetched once per ticket."""

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        ticket_id = (parent_id or {}).get("key") or (parent_id or {}).get("ticketId") or (parent_id or {}).get("id") or parent_id
        if not ticket_id:
            return
        path = self.path.format(ticket_id=ticket_id)
        yield from self._paginate(path)

