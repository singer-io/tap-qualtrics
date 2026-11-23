from collections.abc import Iterator
from typing import Any, ClassVar

from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibraryBlocks(LibraryChildStream):
    tap_stream_id = "library_blocks"
    key_properties: ClassVar = ["libraryId", "category"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/blocks"
    parent = "libraries"

    def get_records(self, parent_id: Any = None) -> Iterator[dict]:
        library_id = (parent_id or {}).get("libraryId") or parent_id
        if not library_id:
            return
        grouped: dict[str, dict] = {}
        for element in self._paginate(self.path.format(library_id=library_id)):
            for category, blocks in element.items():
                if isinstance(blocks, dict):
                    grouped.setdefault(category, {}).update(blocks)
        for category, blocks in grouped.items():
            yield {
                "libraryId": library_id,
                "category": category,
                "blocks": [
                    {
                        "block_id": block_id,
                        "block_value": block_value if isinstance(block_value, str) else None,
                    }
                    for block_id, block_value in blocks.items()
                ],
            }
