from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, ClassVar

from tap_qualtrics.streams.abstracts import IncrementalDirectoryChildStream

_DATE_FIELDS = ("creationDate", "lastModifiedDate")


class MailingLists(IncrementalDirectoryChildStream):
    tap_stream_id = "mailing_lists"
    key_properties: ClassVar = ["mailingListId"]
    replication_method = "INCREMENTAL"
    replication_keys: ClassVar = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists"
    page_size = 1000
    parent = "directories"
    children: ClassVar = [
        "mailing_list_contacts",
        "mailing_list_bounced_contacts",
        "mailing_list_opted_out_contacts",
    ]

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[dict]:
        for record in super().get_records(parent_id, bookmark):
            for field in _DATE_FIELDS:
                value = record.get(field)
                if isinstance(value, int):
                    record[field] = datetime.fromtimestamp(
                        value / 1000, tz=UTC
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
            yield record
