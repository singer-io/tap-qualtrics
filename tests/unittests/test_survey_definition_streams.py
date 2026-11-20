"""Tests for the survey definitions, questions, and flows streams."""
import copy
import json
from unittest.mock import MagicMock, patch

import pytest
import singer
from jsonschema import Draft4Validator, FormatChecker
from requests import Response
from singer import metadata

from tap_qualtrics.discover import discover
from tap_qualtrics.exceptions import QualtricsError
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


def _detail_response(stream_cls, survey_id):
    if stream_cls is SurveyQuestions:
        return {"result": {"elements": [{"QuestionID": "QID1"}]}}
    if stream_cls is SurveyDefinitions:
        return {"result": {"SurveyID": survey_id, "LastModified": LAST_MODIFIED}}
    return {"result": {"FlowID": "FL_1", "Type": "Root", "Flow": []}}


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
    expected_replication_keys = {
        "survey_definitions": ["LastModified"],
        "survey_questions": ["lastModified"],
        "survey_flows": ["lastModified"],
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
        replication_key = expected_replication_keys[stream_id][0]
        assert metadata.get(mdata, (), "valid-replication-keys") == [replication_key]
        assert metadata.get(
            mdata, ("properties", replication_key), "inclusion"
        ) == "automatic"
        for key in expected_keys[stream_id]:
            assert metadata.get(mdata, ("properties", key), "inclusion") == "automatic"


def test_stream_records_use_endpoint_shape_and_parent_replication_key():
    responses = [
        (
            SurveyDefinitions,
            "survey-definitions/SV_1",
            {
                "result": {
                    "SurveyID": SURVEY_ID,
                    "SurveyName": "Example",
                    "LastModified": LAST_MODIFIED,
                }
            },
            {
                "SurveyID": SURVEY_ID,
                "SurveyName": "Example",
                "survey_id": SURVEY_ID,
                "LastModified": LAST_MODIFIED,
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
            return {"result": {
                "SurveyID": survey_id,
                "SurveyName": survey_id,
                "LastModified": LAST_MODIFIED,
            }}
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
        replication_key = stream_cls.replication_keys[0]
        expected_bookmark = (
            LAST_MODIFIED
            if stream_cls is SurveyDefinitions
            else surveys[-1]["lastModified"]
        )
        assert singer.utils.strptime_to_utc(
            state["bookmarks"][stream_cls.tap_stream_id][replication_key]
        ) == singer.utils.strptime_to_utc(
            expected_bookmark
        )


@pytest.mark.parametrize("stream_cls", DETAIL_STREAMS)
def test_out_of_order_surveys_and_interrupted_sync_resume(stream_cls):
    client = _client()
    surveys = [
        {"id": "SV_new", "lastModified": "2026-02-01T00:00:00Z"},
        {"id": "SV_old", "lastModified": "2026-01-01T00:00:00Z"},
    ]
    fail = [True]

    def get(path, **kwargs):
        _ = kwargs
        if path == "surveys":
            return {"result": {"elements": copy.deepcopy(surveys)}}
        if "SV_old" in path and fail[0]:
            raise RuntimeError("interrupted")
        survey_id = path.split("/")[1]
        if stream_cls is SurveyQuestions:
            return {"result": {"elements": [{"QuestionID": "QID1"}]}}
        if stream_cls is SurveyDefinitions:
            return {"result": {"SurveyID": survey_id, "LastModified": LAST_MODIFIED}}
        return {"result": {"FlowID": "FL_1", "Type": "Root", "Flow": []}}

    client.get.side_effect = get
    catalog = _selected_catalog({stream_cls.tap_stream_id})
    initial_bookmarks = {
        "surveys": {"lastModified": "2020-01-01T00:00:00Z"},
        stream_cls.tap_stream_id: {
            stream_cls.replication_keys[0]: "2020-01-01T00:00:00Z"
        },
    }
    state = {"bookmarks": copy.deepcopy(initial_bookmarks)}
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        with pytest.raises(RuntimeError, match="interrupted"):
            sync(client, client.config, catalog, state)
        assert state["bookmarks"] == initial_bookmarks
        fail[0] = False
        write_record.reset_mock()
        sync(client, client.config, catalog, state)

    assert [args[1]["survey_id"] for args, _ in write_record.call_args_list] == [
        "SV_new", "SV_old",
    ]
    for stream_id in ("surveys", stream_cls.tap_stream_id):
        bookmark_key = (
            "lastModified" if stream_id == "surveys"
            else stream_cls.replication_keys[0]
        )
        expected_bookmark = (
            surveys[0]["lastModified"]
            if stream_id == "surveys" or stream_cls is not SurveyDefinitions
            else LAST_MODIFIED
        )
        assert singer.utils.strptime_to_utc(
            state["bookmarks"][stream_id][bookmark_key]
        ) == singer.utils.strptime_to_utc(expected_bookmark)


def test_question_page_failure_does_not_checkpoint_partial_survey():
    client = _client()
    fail = [True]
    next_page = "https://example.test/questions?page=2"

    def get(path, **kwargs):
        if path == "surveys":
            return {"result": {"elements": [PARENT]}}
        if kwargs.get("full_url") == next_page:
            if fail[0]:
                raise RuntimeError("page failed")
            return {"result": {"elements": [{"QuestionID": "QID2"}]}}
        return {
            "result": {
                "elements": [{"QuestionID": "QID1"}],
                "nextPage": next_page,
            }
        }

    client.get.side_effect = get
    catalog = _selected_catalog({"survey_questions"})
    state = {"bookmarks": {"surveys": {"lastModified": "2020-01-01T00:00:00Z"}}}
    initial_bookmarks = copy.deepcopy(state["bookmarks"])
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        with pytest.raises(RuntimeError, match="page failed"):
            sync(client, client.config, catalog, state)
        assert state["bookmarks"] == initial_bookmarks
        fail[0] = False
        write_record.reset_mock()
        sync(client, client.config, catalog, state)

    assert [args[1]["QuestionID"] for args, _ in write_record.call_args_list] == [
        "QID1", "QID2",
    ]


@pytest.mark.parametrize("stream_id,fields", [
    ("survey_definitions", {
        "SurveyID": "SV_1",
        "Questions": {"QID1": {"translations": {"FR": "Bonjour"}}},
    }),
    ("survey_questions", {
        "QuestionID": "QID1",
        "Choices": {str(i): {"Display": str(i), "extra": {"flag": True}}
                    for i in range(1, 5)},
        "RecodeValues": {str(i): str(i) for i in range(1, 5)},
        "Configuration": {"custom": {"nested": 1}},
        "Validation": {"Settings": {"Logic": {"third": {"operator": "eq"}}}},
    }),
    ("survey_flows", {
        "FlowID": "FL_1",
        "Flow": [{"Type": "Branch", "ID": "BL_1", "Properties": {"flag": True}}],
    }),
])
def test_declared_payload_fields_survive_singer_transformation(stream_id, fields):
    entry = discover().get_stream(stream_id)
    record = {
        "survey_id": "SV_1",
        STREAMS[stream_id].replication_keys[0]: LAST_MODIFIED,
        **fields,
    }
    schema = entry.schema.to_dict()
    with singer.Transformer() as transformer:
        transformed = transformer.transform(copy.deepcopy(record), schema, metadata.to_map(entry.metadata))
    for key, value in fields.items():
        assert transformed[key] == value
    Draft4Validator(schema, format_checker=FormatChecker()).validate(transformed)


@pytest.mark.parametrize("choices", [
    {"1": {"Display": "First"}, "2": {"Display": "Second"}},
    [{"Display": "First", "Details": {"enabled": True}}, {"Display": "Second"}],
    None,
])
def test_question_choice_shapes_and_order_transform_consistently(choices):
    entry = discover().get_stream("survey_questions")
    schema = entry.schema.to_dict()
    record = {
        "survey_id": "SV_1",
        "QuestionID": "QID1",
        "lastModified": LAST_MODIFIED,
        "Choices": choices,
        "ChoiceOrder": [1, "2"],
    }
    Draft4Validator(schema).validate(record)
    with singer.Transformer() as transformer:
        transformed = transformer.transform(
            copy.deepcopy(record), schema, metadata.to_map(entry.metadata)
        )
    assert transformed["Choices"] == choices
    assert transformed["ChoiceOrder"] == ["1", "2"]


@pytest.mark.parametrize("randomization", [
    {"Advanced": {"RandomizeAll": True, "DisplayCount": 2}, "Order": ["1", "2"]},
    "All",
    None,
])
def test_question_randomization_preserves_type_in_emitted_record(randomization, capsys):
    catalog = singer.Catalog.from_dict(json.loads(json.dumps(
        _selected_catalog({"survey_questions"}).to_dict()
    )))
    client = _client()
    response = Response()
    response.status_code = 200
    response._content = json.dumps({"result": {"elements": [{
        "QuestionID": "QID1", "Randomization": randomization,
    }]}}).encode("utf-8")
    client.get.side_effect = lambda path, **kwargs: (
        {"result": {"elements": [copy.deepcopy(PARENT)]}}
        if path == "surveys" else response.json()
    )

    sync(client, client.config, catalog, {})

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    records = [
        message["record"] for message in messages
        if message["type"] == "RECORD" and message["stream"] == "survey_questions"
    ]
    assert len(records) == 1
    assert records[0]["Randomization"] == randomization
    assert type(records[0]["Randomization"]) is type(randomization)
    Draft4Validator(
        catalog.get_stream("survey_questions").schema.to_dict(),
        format_checker=FormatChecker(),
    ).validate(records[0])


@pytest.mark.parametrize("stream_cls", DETAIL_STREAMS)
@pytest.mark.parametrize("child_bookmark", [None, "2026-01-01T00:00:00Z"])
def test_upgrade_backfills_child_without_replaying_parent_or_other_children(
    stream_cls, child_bookmark
):
    client = _client()
    parents = [
        {"id": "SV_old", "lastModified": "2026-01-10T00:00:00Z"},
        {"id": "SV_new", "lastModified": "2026-03-01T00:00:00Z"},
    ]

    def get(path, **kwargs):
        if path == "surveys":
            start = singer.utils.strptime_to_utc(kwargs["params"]["startDate"])
            return {"result": {"elements": [
                copy.deepcopy(parent) for parent in parents
                if singer.utils.strptime_to_utc(parent["lastModified"]) >= start
            ]}}
        if path == "surveys/SV_new":
            return {"result": {
                "id": "SV_new", "lastModifiedDate": parents[-1]["lastModified"],
            }}
        if path.startswith("survey-definitions/"):
            return _detail_response(stream_cls, path.split("/")[1])
        raise AssertionError(f"Unexpected request: {path}")

    client.get.side_effect = get
    state = {"bookmarks": {"surveys": {"lastModified": "2026-02-01T00:00:00Z"}}}
    if child_bookmark:
        state["bookmarks"][stream_cls.tap_stream_id] = {
            stream_cls.replication_keys[0]: child_bookmark
        }
    catalog = _selected_catalog({"surveys", "survey", stream_cls.tap_stream_id})
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.surveys.write_record", new=write_record), \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        sync(client, client.config, catalog, state)

    emitted = {}
    for args, _ in write_record.call_args_list:
        emitted.setdefault(args[0], []).append(args[1])
    assert [record["survey_id"] for record in emitted[stream_cls.tap_stream_id]] == [
        "SV_old", "SV_new",
    ]
    assert [record["id"] for record in emitted["surveys"]] == ["SV_new"]
    assert [record["id"] for record in emitted["survey"]] == ["SV_new"]
    client.get.assert_any_call(
        "surveys", params={
            "startDate": child_bookmark or client.config["start_date"], "pageSize": 100,
        },
    )
    for stream_id in ("surveys", stream_cls.tap_stream_id):
        bookmark_key = (
            "lastModified" if stream_id == "surveys"
            else stream_cls.replication_keys[0]
        )
        expected_bookmark = (
            parents[-1]["lastModified"]
            if stream_id == "surveys" or stream_cls is not SurveyDefinitions
            else LAST_MODIFIED
        )
        assert singer.utils.strptime_to_utc(
            state["bookmarks"][stream_id][bookmark_key]
        ) == singer.utils.strptime_to_utc(expected_bookmark)


@pytest.mark.parametrize("response", [
    None, {}, {"result": None}, {"result": {}},
    {"result": {"elements": None}}, {"result": {"elements": {}}},
    {"result": {"elements": "invalid"}}, {"result": {"elements": [None]}},
    {"result": {"elements": [{}]}},
])
@pytest.mark.parametrize("later_page", [False, True])
def test_incomplete_question_response_preserves_all_bookmarks(response, later_page):
    client = _client()
    next_page = "https://example.test/questions?page=2"
    first_page = {"result": {
        "elements": [{"QuestionID": "QID1"}], "nextPage": next_page,
    }}

    def get(path, **kwargs):
        if path == "surveys":
            return {"result": {"elements": [copy.deepcopy(PARENT)]}}
        if later_page and not kwargs.get("full_url"):
            return first_page
        return response

    client.get.side_effect = get
    state = {"bookmarks": {
        "surveys": {"lastModified": "2026-01-01T00:00:00Z"},
        "survey_questions": {"lastModified": "2026-01-01T00:00:00Z"},
    }}
    initial = copy.deepcopy(state["bookmarks"])
    with patch("tap_qualtrics.streams.abstracts.write_record"), \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state") as write_state:
        with pytest.raises(QualtricsError, match="survey_questions"):
            sync(client, client.config, _selected_catalog({"survey_questions"}), state)
    assert state["bookmarks"] == initial
    assert all(call.args[0]["bookmarks"] == initial for call in write_state.call_args_list)


@pytest.mark.parametrize("stream_cls", [SurveyDefinitions, SurveyFlows])
@pytest.mark.parametrize("result", [None, [], {}, "invalid"])
def test_incomplete_single_record_response_fails(stream_cls, result):
    client = _client()
    client.get.return_value = {"result": result}
    with pytest.raises(QualtricsError, match=stream_cls.tap_stream_id):
        list(stream_cls(client=client).get_records(PARENT))


@pytest.mark.parametrize("timestamp", [
    "2025-01-01T00:00:00.000000Z",
    "2024-12-31T19:00:00-05:00",
    "2025-01-01T01:00:00+01:00",
])
def test_equal_timestamp_formats_do_not_skip_survey_questions(timestamp):
    client = _client()
    parent = {"id": SURVEY_ID, "lastModified": timestamp}
    client.get.side_effect = lambda path, **kwargs: (
        {"result": {"elements": [copy.deepcopy(parent)]}} if path == "surveys"
        else _detail_response(SurveyQuestions, SURVEY_ID)
    )
    bookmark = "2025-01-01T00:00:00Z"
    state = {"bookmarks": {
        stream_id: {"lastModified": bookmark}
        for stream_id in ("surveys", "survey_questions")
    }}
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        sync(client, client.config, _selected_catalog({"survey_questions"}), state)
    assert write_record.call_count == 1
    assert write_record.call_args.args[1]["QuestionID"] == "QID1"


def test_multiple_detail_streams_backfill_from_their_own_bookmarks():
    client = _client()
    parents = [
        {"id": "SV_before_start", "lastModified": "2019-01-01T00:00:00Z"},
        {"id": "SV_old", "lastModified": "2026-01-01T00:00:00Z"},
        {"id": "SV_middle", "lastModified": "2026-03-01T00:00:00Z"},
        {"id": "SV_new", "lastModified": "2026-05-01T00:00:00Z"},
    ]

    def get(path, **kwargs):
        _ = kwargs
        if path == "surveys":
            return {"result": {"elements": copy.deepcopy(parents)}}
        stream_cls = (
            SurveyQuestions if path.endswith("/questions") else
            SurveyFlows if path.endswith("/flow") else SurveyDefinitions
        )
        return _detail_response(stream_cls, path.split("/")[1])

    client.get.side_effect = get
    state = {"bookmarks": {
        "surveys": {"lastModified": "2026-04-01T00:00:00Z"},
        "survey_questions": {"lastModified": "2026-02-01T00:00:00Z"},
        "survey_flows": {"lastModified": "2026-04-01T00:00:00Z"},
    }}
    selected = {stream_cls.tap_stream_id for stream_cls in DETAIL_STREAMS}
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        sync(client, client.config, _selected_catalog(selected), state)
    emitted = {}
    for args, _ in write_record.call_args_list:
        emitted.setdefault(args[0], []).append(args[1]["survey_id"])
    assert emitted == {
        "survey_definitions": ["SV_old", "SV_middle", "SV_new"],
        "survey_questions": ["SV_middle", "SV_new"],
        "survey_flows": ["SV_new"],
    }


@pytest.mark.parametrize("empty_parent_list", [False, True])
def test_empty_question_backfill_catches_up_to_parent_bookmark(empty_parent_list):
    client = _client()
    parent_bookmark = "2026-12-01T00:00:00Z"
    client.get.side_effect = lambda path, **kwargs: {"result": {
        "elements": [copy.deepcopy(PARENT)] if path == "surveys" and not empty_parent_list else [],
    }}
    state = {"bookmarks": {"surveys": {"lastModified": parent_bookmark}}}
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        sync(client, client.config, _selected_catalog({"survey_questions"}), state)
    write_record.assert_not_called()
    assert state["bookmarks"]["surveys"]["lastModified"] == parent_bookmark
    assert state["bookmarks"]["survey_questions"]["lastModified"] == parent_bookmark


def test_failed_historical_backfill_is_replayed_without_advancing_state():
    client = _client()
    parents = [
        {"id": "SV_new", "lastModified": "2026-03-01T00:00:00Z"},
        {"id": "SV_old", "lastModified": "2026-01-01T00:00:00Z"},
    ]
    fail = [True]

    def get(path, **kwargs):
        _ = kwargs
        if path == "surveys":
            return {"result": {"elements": copy.deepcopy(parents)}}
        if "SV_old" in path and fail[0]:
            return {"result": {}}
        return _detail_response(SurveyQuestions, path.split("/")[1])

    client.get.side_effect = get
    state = {"bookmarks": {"surveys": {"lastModified": "2026-02-01T00:00:00Z"}}}
    initial = copy.deepcopy(state["bookmarks"])
    catalog = _selected_catalog({"survey_questions"})
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("singer.write_state"):
        with pytest.raises(QualtricsError, match="survey_questions"):
            sync(client, client.config, catalog, state)
        assert state["bookmarks"] == initial
        fail[0] = False
        write_record.reset_mock()
        sync(client, client.config, catalog, state)
    assert [args[1]["survey_id"] for args, _ in write_record.call_args_list] == [
        "SV_new", "SV_old",
    ]
    assert singer.utils.strptime_to_utc(
        state["bookmarks"]["survey_questions"]["lastModified"]
    ) == singer.utils.strptime_to_utc(parents[0]["lastModified"])
