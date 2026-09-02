from typing import Any, Dict, Iterator

from tap_qualtrics.streams.abstracts import LibraryChildStream


class LibrariesSurveyQuestions(LibraryChildStream):
    tap_stream_id = "libraries_survey_questions"
    key_properties = ["libraryId", "category"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "libraries/{library_id}/survey/questions"
    parent = "libraries"

    def get_records(self, parent_id: Any = None) -> Iterator[Dict]:
        library_id = (parent_id or {}).get("libraryId") or parent_id
        if not library_id:
            return
        grouped: Dict[str, Dict] = {}
        for element in self._paginate(self.path.format(library_id=library_id)):
            for category, questions in element.items():
                if isinstance(questions, dict):
                    grouped.setdefault(category, {}).update(questions)
        for category, questions in grouped.items():
            yield {
                "libraryId": library_id,
                "category": category,
                "questions": [
                    {
                        "question_id": question_id,
                        "question_value": (
                            question_value
                            if isinstance(question_value, str)
                            else None
                        ),
                    }
                    for question_id, question_value in questions.items()
                ],
            }
