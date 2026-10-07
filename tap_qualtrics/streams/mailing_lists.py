from datetime import datetime, timezone
from typing import Any, Dict, Iterator

from tap_qualtrics.streams.abstracts import IncrementalDirectoryChildStream

_DATE_FIELDS = ("creationDate", "lastModifiedDate")


class MailingLists(IncrementalDirectoryChildStream):
    tap_stream_id = "mailing_lists"
    key_properties = ["mailingListId"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDate"]
    data_key = "result.elements"
    path = "directories/{directory_id}/mailinglists"
    page_size = 1000
    parent = "directories"
    children = [
        "mailing_list_contacts",
        "mailing_list_bounced_contacts",
        "mailing_list_opted_out_contacts",
    ]

    def get_records(self, parent_id: Any = None, bookmark: str = "") -> Iterator[Dict]:
        for record in super().get_records(parent_id, bookmark):
            for field in _DATE_FIELDS:
                value = record.get(field)
                if isinstance(value, int):
                    record[field] = datetime.fromtimestamp(
                        value / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
            yield record
