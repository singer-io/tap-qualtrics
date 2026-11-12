"""Survey metadata extraction, discovery, and Singer output tests."""
import copy
import json
from unittest.mock import MagicMock, call, patch

import pytest
import singer
from jsonschema import Draft4Validator, FormatChecker
from singer import metadata

from tap_qualtrics.discover import _apply_access_checks, discover
from tap_qualtrics.exceptions import QualtricsForbiddenError, QualtricsInternalServerError
from tap_qualtrics.schema import get_schemas
from tap_qualtrics.streams import STREAMS
from tap_qualtrics.streams.survey_languages import SurveyLanguages
from tap_qualtrics.streams.survey_options import SurveyOptions
from tap_qualtrics.streams.survey_translations import SurveyTranslations
from tap_qualtrics.streams.survey_versions import SurveyVersions
from tap_qualtrics.sync import sync


NEW_STREAMS = (
    SurveyVersions, SurveyOptions, SurveyLanguages, SurveyTranslations,
)
LAST_MODIFIED = "2026-11-04T10:00:00Z"
SURVEY_PARENT = {"id": "SV_1", "lastModified": LAST_MODIFIED}
VERSION = {
    "surveyID": "SV_1",
    "versionID": "V_1",
    "versionNumber": 1,
    "description": "First version",
    "userID": "UR_1",
    "creationDate": "2026-11-04T10:00:00Z",
    "published": True,
    "wasPublished": False,
    "publishEvents": [{"date": "2026-11-04T10:00:00Z", "userID": "UR_1"}],
}
OPTIONS = {
    "SurveyTitle": "Example",
    "Limit": 3,
    "Enabled": False,
    "Optional": None,
    "Nested": {"unknown": {"values": [1, "text", None, False]}},
}


def make_client():
    client = MagicMock()
    client.page_size = 100
    client.config = {"start_date": "2020-01-01T00:00:00Z"}
    return client


def selected_catalog(selected):
    catalog = discover()
    for entry in catalog.streams:
        mdata = metadata.to_map(entry.metadata)
        mdata = metadata.write(mdata, (), "selected", entry.stream in selected)
        entry.metadata = metadata.to_list(mdata)
    return catalog


@pytest.mark.parametrize("stream_cls", NEW_STREAMS)
def test_catalog_metadata_and_schema(stream_cls):
    entry = discover().get_stream(stream_cls.tap_stream_id)
    schema = entry.schema.to_dict()
    Draft4Validator.check_schema(schema)
    assert "additionalProperties" not in schema
    assert entry.key_properties == stream_cls.key_properties
    mdata = metadata.to_map(entry.metadata)
    assert metadata.get(mdata, (), "forced-replication-method") == "INCREMENTAL"
    assert metadata.get(mdata, (), "valid-replication-keys") == ["lastModified"]
    assert metadata.get(mdata, ("properties", "lastModified"), "inclusion") == "automatic"
    assert metadata.get(mdata, (), "parent-tap-stream-id") == stream_cls.parent
    for key in stream_cls.key_properties:
        assert metadata.get(mdata, ("properties", key), "inclusion") == "automatic"
    assert stream_cls.tap_stream_id in STREAMS[stream_cls.parent].children


def test_versions_flatten_metadata_and_paginate():
    client = make_client()
    second_version = {**VERSION, "versionID": "V_2", "versionNumber": 2}
    client.get.side_effect = [
        {"result": {"elements": [{"metadata": copy.deepcopy(VERSION)}],
                    "nextPage": "https://example.test/next"}},
        {"result": {"elements": [{"metadata": second_version}], "nextPage": None}},
    ]
    records = list(SurveyVersions(client=client).get_records(SURVEY_PARENT))
    assert records == [
        {**VERSION, "lastModified": LAST_MODIFIED},
        {**second_version, "lastModified": LAST_MODIFIED},
    ]
    assert client.get.call_args_list == [
        call("survey-definitions/SV_1/versions", params={"pageSize": 100}),
        call("survey-definitions/SV_1/versions", full_url="https://example.test/next"),
    ]


@pytest.mark.parametrize("options", [OPTIONS, {}, {"NewKey": {"OtherKey": 4}}])
def test_options_preserved_through_real_transformer(options):
    client = make_client()
    client.get.return_value = {"result": copy.deepcopy(options)}
    entry = discover().get_stream("survey_options")
    stream = SurveyOptions(client=client, catalog_entry=entry)
    records = list(stream.get_records(SURVEY_PARENT))
    expected = {"survey_id": "SV_1", "options": options, "lastModified": LAST_MODIFIED}
    assert records == [expected]
    with singer.Transformer() as transformer:
        transformed = transformer.transform(records[0], stream.schema, stream.mdata)
    assert transformed["options"] == options
    assert singer.utils.strptime_to_utc(transformed["lastModified"]) == (
        singer.utils.strptime_to_utc(LAST_MODIFIED)
    )
    assert json.dumps(transformed["options"], sort_keys=True) == json.dumps(options, sort_keys=True)
    Draft4Validator(stream.schema).validate(transformed)
    client.get.assert_called_once_with("survey-definitions/SV_1/options")


def test_languages_normalized():
    client = make_client()
    client.get.return_value = {"result": {"AvailableLanguages": ["EN", "FR"]}}
    records = list(SurveyLanguages(client=client).get_records(SURVEY_PARENT))
    assert records == [
        {"survey_id": "SV_1", "language_code": "EN", "lastModified": LAST_MODIFIED},
        {"survey_id": "SV_1", "language_code": "FR", "lastModified": LAST_MODIFIED},
    ]
    client.get.assert_called_once_with("surveys/SV_1/languages")


def test_translations_normalized_preserving_empty_and_unicode_text():
    client = make_client()
    translations = {"QID1_QuestionText": "Bonjour \u00e9quipe", "SurveyTitle": "", "QID2": None}
    client.get.return_value = {"result": translations}
    stream = SurveyTranslations(client=client)
    parent = {"survey_id": "SV_1", "language_code": "FR", "lastModified": LAST_MODIFIED}
    assert list(stream.get_records(parent)) == [
        {**parent, "element_id": key, "value": value}
        for key, value in translations.items()
    ]
    client.get.assert_called_once_with("surveys/SV_1/translations/FR")


@pytest.mark.parametrize("stream_cls", NEW_STREAMS)
def test_no_parent_does_not_request(stream_cls):
    client = make_client()
    stream = stream_cls(client=client)
    assert list(stream.get_records()) == []
    client.get.assert_not_called()


@pytest.mark.parametrize("stream_cls,response", [
    (SurveyVersions, {"result": {"elements": [], "nextPage": None}}),
    (SurveyLanguages, {"result": {"AvailableLanguages": []}}),
    (SurveyTranslations, {"result": {}}),
])
def test_empty_collections(stream_cls, response):
    client = make_client()
    client.get.return_value = response
    parent = {"id": "SV_1", "survey_id": "SV_1", "language_code": "EN"}
    assert list(stream_cls(client=client).get_records(parent)) == []


@pytest.mark.parametrize("stream_cls", NEW_STREAMS)
def test_api_errors_propagate(stream_cls):
    client = make_client()
    client.get.side_effect = QualtricsInternalServerError("500")
    parent = {"id": "SV_1", "survey_id": "SV_1", "language_code": "EN"}
    with pytest.raises(QualtricsInternalServerError):
        list(stream_cls(client=client).get_records(parent))


@pytest.mark.parametrize("stream_cls", [SurveyOptions, SurveyLanguages, SurveyTranslations])
def test_missing_result_is_not_successful_empty_sync(stream_cls):
    client = make_client()
    client.get.return_value = {}
    parent = {"id": "SV_1", "survey_id": "SV_1", "language_code": "EN"}
    with pytest.raises(KeyError):
        list(stream_cls(client=client).get_records(parent))


@pytest.mark.parametrize("denied", ["survey_versions", "survey_options",
                                   "survey_languages", "survey_translations"])
def test_discovery_probes_children_and_prunes_denied_streams(denied):
    client = make_client()
    schemas, field_metadata = get_schemas()
    names = {"surveys", *(cls.tap_stream_id for cls in NEW_STREAMS)}
    schemas = {key: value for key, value in schemas.items() if key in names}
    field_metadata = {key: value for key, value in field_metadata.items() if key in names}
    denied_path = {
        "survey_versions": "survey-definitions/SV_1/versions",
        "survey_options": "survey-definitions/SV_1/options",
        "survey_languages": "surveys/SV_1/languages",
        "survey_translations": "surveys/SV_1/translations/FR",
    }[denied]

    def get(path, **_kwargs):
        if path == denied_path:
            raise QualtricsForbiddenError("403")
        if path == "surveys":
            return {"result": {"elements": [{"id": "SV_1"}]}}
        if path.endswith("/languages"):
            return {"result": {"AvailableLanguages": ["FR"]}}
        return {"result": {"elements": []}}

    client.get.side_effect = get
    _apply_access_checks(client, schemas, field_metadata)
    assert denied not in schemas
    if denied == "survey_languages":
        assert "survey_translations" not in schemas
        assert not any("/translations/" in args[0] for args, _ in client.get.call_args_list)
    else:
        assert "survey_languages" in schemas
    assert "surveys" in schemas
    client.get.assert_any_call(denied_path, params={"pageSize": 1})


@pytest.mark.parametrize("selected", [
    {"survey_translations"},
    {"surveys", "survey_translations"},
    {cls.tap_stream_id for cls in NEW_STREAMS},
    {"surveys", *(cls.tap_stream_id for cls in NEW_STREAMS)},
])
def test_incremental_sync_selection_bookmarks_and_parent_traversal(selected):
    client = make_client()
    surveys = [
        {"id": "SV_1", "lastModified": "2026-01-01T00:00:00Z"},
        {"id": "SV_2", "lastModified": "2026-02-01T00:00:00Z"},
    ]

    def get(path, **_kwargs):
        if path == "surveys":
            return {"result": {"elements": copy.deepcopy(surveys), "nextPage": None}}
        if path.endswith("/versions"):
            return {"result": {"elements": [{"metadata": copy.deepcopy(VERSION)}]}}
        if path.endswith("/options"):
            return {"result": copy.deepcopy(OPTIONS)}
        if path.endswith("/languages"):
            return {"result": {"AvailableLanguages": ["EN", "FR"]}}
        if "/translations/" in path:
            return {"result": {"SurveyTitle": "Title", "QID1_QuestionText": "Question"}}
        pytest.fail(f"Unexpected request: {path}")

    client.get.side_effect = get
    catalog = selected_catalog(selected)
    state = {"bookmarks": {"surveys": {"lastModified": "2020-01-01T00:00:00Z"}}}
    with patch("tap_qualtrics.streams.abstracts.write_record") as write_record, \
            patch("tap_qualtrics.streams.abstracts.write_schema") as write_schema, \
            patch("singer.write_state"):
        sync(client, client.config, catalog, state)
    emitted = {}
    schemas = get_schemas()[0]
    for args, _ in write_record.call_args_list:
        stream_id, record = args
        emitted.setdefault(stream_id, []).append(record)
        Draft4Validator(schemas[stream_id], format_checker=FormatChecker()).validate(record)
    assert set(emitted) == selected
    assert len(emitted["survey_translations"]) == 8
    assert len({tuple(record[key] for key in SurveyTranslations.key_properties)
                for record in emitted["survey_translations"]}) == 8
    assert {args[0] for args, _ in write_schema.call_args_list} == selected
    client.get.assert_any_call(
        "surveys", params={"pageSize": 100, "startDate": "2020-01-01T00:00:00Z"},
    )
    for survey in surveys:
        for language in ("EN", "FR"):
            client.get.assert_any_call(f"surveys/{survey['id']}/translations/{language}")
    expected_streams = selected | {"surveys", "survey_languages"}
    for name in expected_streams:
        bookmark = state["bookmarks"][name]["lastModified"]
        assert singer.utils.strptime_to_utc(bookmark) == singer.utils.strptime_to_utc(
            surveys[-1]["lastModified"]
        )


def test_existing_incremental_children_keep_parent_bookmark_filter():
    client = make_client()
    client.get.return_value = {"result": {"elements": [], "nextPage": None}}
    catalog = selected_catalog({"survey_quotas"})
    state = {"bookmarks": {"surveys": {"lastModified": "2026-11-04T00:00:00Z"}}}
    with patch("singer.write_state"):
        sync(client, client.config, catalog, state)
    client.get.assert_called_once_with(
        "surveys", params={"pageSize": 100, "startDate": "2026-11-04T00:00:00Z"},
    )


def test_mixed_selection_only_runs_existing_children_for_modified_surveys():
    client = make_client()
    surveys = [
        {"id": "SV_old", "lastModified": "2026-01-01T00:00:00Z"},
        {"id": "SV_new", "lastModified": "2026-11-05T10:00:00Z"},
    ]

    def get(path, **_kwargs):
        if path == "surveys":
            return {"result": {"elements": copy.deepcopy(surveys), "nextPage": None}}
        if path.endswith("/options"):
            return {"result": {}}
        if path == "surveys/SV_new/quotas":
            return {"result": {"elements": [], "nextPage": None}}
        pytest.fail(f"Unexpected request: {path}")

    client.get.side_effect = get
    catalog = selected_catalog({"surveys", "survey_options", "survey_quotas"})
    state = {"bookmarks": {"surveys": {"lastModified": "2026-11-04T00:00:00Z"}}}
    with patch("singer.write_state"), \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        sync(client, client.config, catalog, state)
    emitted = [args for args, _ in write_record.call_args_list]
    assert len([args for args in emitted if args[0] == "survey_options"]) == 1
    survey_records = [args[1] for args in emitted if args[0] == "surveys"]
    assert [record["id"] for record in survey_records] == ["SV_new"]
    assert state["bookmarks"]["surveys"]["lastModified"] == survey_records[0]["lastModified"]
    client.get.assert_any_call("surveys/SV_new/quotas", params={"pageSize": 100})
    assert not any(args[0] == "surveys/SV_old/quotas" for args, _ in client.get.call_args_list)


def test_translation_selection_with_no_languages_makes_no_translation_requests():
    client = make_client()
    client.get.side_effect = [
        {"result": {"elements": [{"id": "SV_1", "lastModified": "2026-01-01T00:00:00Z"}]}},
        {"result": {"AvailableLanguages": []}},
    ]
    with patch("singer.write_state"), \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        sync(client, client.config, selected_catalog({"survey_translations"}), {})
    write_record.assert_not_called()
    assert client.get.call_args_list == [
        call("surveys", params={"pageSize": 100, "startDate": client.config["start_date"]}),
        call("surveys/SV_1/languages"),
    ]


@pytest.mark.parametrize("selected", [
    {"survey_translations"},
    {cls.tap_stream_id for cls in NEW_STREAMS},
])
def test_unchanged_surveys_skip_new_streams(selected):
    client = make_client()
    client.get.return_value = {
        "result": {"elements": [{"id": "SV_1", "lastModified": "2026-01-01T00:00:00Z"}]},
    }
    state = {"bookmarks": {"surveys": {"lastModified": "2026-11-04T00:00:00Z"}}}
    with patch("singer.write_state"), \
            patch("tap_qualtrics.streams.abstracts.write_schema"), \
            patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        sync(client, client.config, selected_catalog(selected), state)
    write_record.assert_not_called()
    client.get.assert_called_once_with(
        "surveys", params={"pageSize": 100, "startDate": "2026-11-04T00:00:00Z"},
    )


@pytest.mark.parametrize("stream_cls", NEW_STREAMS)
def test_child_bookmark_snapshot_handles_out_of_order_parents(stream_cls):
    client = make_client()
    client.get.return_value = {
        "result": {
            "elements": [{"metadata": copy.deepcopy(VERSION)}],
            "AvailableLanguages": ["EN", "FR"],
            "SurveyTitle": "Title",
        },
    }
    catalog = selected_catalog({stream_cls.tap_stream_id})
    stream = stream_cls(client=client, catalog_entry=catalog.get_stream(stream_cls.tap_stream_id))
    newer = "2026-11-05T10:00:00Z"
    older = "2026-11-04T10:00:00Z"
    state = {"bookmarks": {stream_cls.tap_stream_id: {"lastModified": "2026-01-01T00:00:00Z"}}}
    with singer.Transformer() as transformer, \
            patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        for modified in (newer, older):
            parent = {
                "id": "SV_1", "survey_id": "SV_1", "language_code": "EN",
                "lastModified": modified,
            }
            stream.sync(state, transformer, parent)
    emitted_dates = {
        singer.utils.strptime_to_utc(args[1]["lastModified"])
        for args, _ in write_record.call_args_list
    }
    assert emitted_dates == {
        singer.utils.strptime_to_utc(newer), singer.utils.strptime_to_utc(older),
    }
    assert singer.utils.strptime_to_utc(
        state["bookmarks"][stream_cls.tap_stream_id]["lastModified"]
    ) == singer.utils.strptime_to_utc(newer)
    resumed = stream_cls(
        client=client, catalog_entry=catalog.get_stream(stream_cls.tap_stream_id),
    )
    with singer.Transformer() as transformer, \
            patch("tap_qualtrics.streams.abstracts.write_record") as write_record:
        resumed.sync(state, transformer, {
            "id": "SV_1", "survey_id": "SV_1", "language_code": "EN",
            "lastModified": older,
        })
    write_record.assert_not_called()
