from typing import Any, Dict, Iterator

from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibrarySurveys(LibraryChildStream):
    tap_stream_id = "library_surveys"
    key_properties = ["libraryId", "category"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/surveys"
    parent = "libraries"

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        library_id = (parent_id or {}).get("libraryId") or parent_id
        if not library_id:
            return
        grouped: Dict[str, Dict] = {}
        for element in self._paginate(self.path.format(library_id=library_id)):
            for category, surveys in element.items():
                if isinstance(surveys, dict):
                    grouped.setdefault(category, {}).update(surveys)
        for category, surveys in grouped.items():
            yield {
                "libraryId": library_id,
                "category": category,
                "surveys": surveys,
            }
