"""Tests for the survey definitions, questions, and flows streams."""
from unittest.mock import MagicMock, patch

import singer
from jsonschema import Draft4Validator, FormatChecker
from singer import metadata

from tap_qualtrics.discover import discover
from tap_qualtrics.schema import get_schemas
from tap_qualtrics.streams import STREAMS
from tap_qualtrics.streams.survey_definitions import SurveyDefinitions
from tap_qualtrics.streams.survey_flows import SurveyFlows
from tap_qualtrics.streams.survey_questions import SurveyQuestions
from tap_qualtrics.sync import sync


SURVEY_ID = "SV_1"
LAST_MODIFIED = "2026-11-04T10:00:00Z"
PARENT = {"id": SURVEY_ID, "lastModified": LAST_MODIFIED}
DETAIL_STREAMS = (SurveyDefinitions, SurveyQuestions, SurveyFlows)


def _client():
    client = MagicMock()
    client.page_size = 100
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    return client


def _selected_catalog(selected):
    catalog = discover()
    for entry in catalog.streams:
        mdata = metadata.to_map(entry.metadata)
        mdata = metadata.write(mdata, (), "selected", entry.stream in selected)
        entry.metadata = metadata.to_list(mdata)
    return catalog


def test_streams_are_registered_as_incremental_survey_children():
    surveys = STREAMS["surveys"]
    expected_keys = {
        "survey_definitions": ["SurveyID"],
        "survey_questions": ["survey_id", "QuestionID"],
        "survey_flows": ["survey_id"],
    }

    for stream_cls in DETAIL_STREAMS:
        stream_id = stream_cls.tap_stream_id
        entry = discover().get_stream(stream_id)
        mdata = metadata.to_map(entry.metadata)
        schema = entry.schema.to_dict()
        Draft4Validator.check_schema(schema)
        assert entry.key_properties == expected_keys[stream_id]
        assert stream_cls.parent == "surveys"
        assert stream_id in surveys.children
        assert metadata.get(mdata, (), "parent-tap-stream-id") == "surveys"
        assert metadata.get(mdata, (), "valid-replication-keys") == ["lastModified"]
        assert metadata.get(mdata, ("properties", "lastModified"), "inclusion") == "automatic"
        for key in expected_keys[stream_id]:
            assert metadata.get(mdata, ("properties", key), "inclusion") == "automatic"


def test_stream_records_use_endpoint_shape_and_parent_replication_key():
    responses = [
        (
            SurveyDefinitions,
            "survey-definitions/SV_1",
            {"result": {"SurveyID": SURVEY_ID, "SurveyName": "Example"}},
            {
                "SurveyID": SURVEY_ID,
                "SurveyName": "Example",
                "survey_id": SURVEY_ID,
                "lastModified": LAST_MODIFIED,
            },
        ),
        (
            SurveyQuestions,
            "survey-definitions/SV_1/questions",
            {"result": {"elements": [{"QuestionID": "QID1", "QuestionText": "Question"}]}},
            {
                "QuestionID": "QID1",
                "QuestionText": "Question",
                "survey_id": SURVEY_ID,
                "lastModified": LAST_MODIFIED,
            },
        ),
        (
            SurveyFlows,
            "survey-definitions/SV_1/flow",
            {"result": {"FlowID": "FL_1", "Type": "Root", "Flow": []}},
            {
                "FlowID": "FL_1",
                "Type": "Root",
                "Flow": [],
                "survey_id": SURVEY_ID,
                "lastModified": LAST_MODIFIED,
            },
        ),
    ]

    for stream_cls, path, response, expected in responses:
        client = _client()
        client.get.return_value = response
        records = list(stream_cls(client=client).get_records(PARENT))
        assert records == [expected]
        client.get.assert_called_once_with(path, params={"pageSize": 100})


def test_questions_retain_repeating_question_ids_per_survey():
    client = _client()
    surveys = [
        {"id": "SV_1", "lastModified": LAST_MODIFIED},
        {"id": "SV_2", "lastModified": "2026-11-05T10:00:00Z"},
    ]

    def get(path, **kwargs):
        _ = kwargs
        if path == "surveys":
            return {"result": {"elements": surveys, "nextPage": None}}
        if path.endswith("/questions"):
            survey_id = path.split("/")[-2]
            return {
                "result": {
                    "elements": [{"QuestionID": "QID1", "QuestionText": survey_id}],
                    "nextPage": None,
                }
            }
        if path.endswith("/flow"):
            survey_id = path.split("/")[-2]
            return {"result": {"FlowID": "FL_1", "Type": survey_id, "Flow": []}}
        if path.startswith("survey-definitions/"):
            survey_id = path.split("/")[-1]
            return {"result": {"SurveyID": survey_id, "SurveyName": survey_id}}
        raise AssertionError(f"Unexpected request: {path}")

    client.get.side_effect = get
    selected = {stream_cls.tap_stream_id for stream_cls in DETAIL_STREAMS}
    catalog = _selected_catalog(selected)
    state = {"bookmarks": {"surveys": {"lastModified": "2020-01-01T00:00:00Z"}}}

    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        sync(client, client.config, catalog, state)

    emitted = {}
    schemas = get_schemas()[0]
    for args, _ in write_record.call_args_list:
        stream_id, record = args
        emitted.setdefault(stream_id, []).append(record)
        Draft4Validator(schemas[stream_id], format_checker=FormatChecker()).validate(record)

    assert set(emitted) == selected
    assert len(emitted["survey_definitions"]) == 2
    assert len(emitted["survey_questions"]) == 2
    assert len(emitted["survey_flows"]) == 2
    assert {
        (record["survey_id"], record["QuestionID"])
        for record in emitted["survey_questions"]
    } == {("SV_1", "QID1"), ("SV_2", "QID1")}
    assert {record["SurveyID"] for record in emitted["survey_definitions"]} == {
        "SV_1", "SV_2",
    }
    assert {record["survey_id"] for record in emitted["survey_flows"]} == {
        "SV_1", "SV_2",
    }
    for survey in surveys:
        for suffix in ("", "/questions", "/flow"):
            client.get.assert_any_call(
                f"survey-definitions/{survey['id']}{suffix}",
                params={"pageSize": 100},
            )
    for stream_cls in DETAIL_STREAMS:
        assert singer.utils.strptime_to_utc(
            state["bookmarks"][stream_cls.tap_stream_id]["lastModified"]
        ) == singer.utils.strptime_to_utc(
            surveys[-1]["lastModified"]
        )
