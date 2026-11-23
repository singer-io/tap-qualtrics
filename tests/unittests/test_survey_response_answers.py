import copy
import json
from unittest.mock import MagicMock, patch

from jsonschema import Draft4Validator
from singer import Transformer, metadata, utils
from singer.catalog import CatalogEntry, Schema

from tap_qualtrics.discover import discover
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport
from tap_qualtrics.sync import sync


ANSWER_FIELDS = ("values", "labels", "displayedValues")


def _client():
    client = MagicMock()
    client.page_size = 100
    client.start_date = "2020-01-01T00:00:00Z"
    client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}
    client.post.return_value = {"result": {"progressId": "progress"}}
    client.poll_export.return_value = {"result": {"fileId": "file"}}
    return client


def _discovered_schema(client, responses):
    client.get_file.return_value.content = json.dumps({"responses": responses}).encode()
    entries, skipped = SurveyResponseExport.discover_dynamic_entries(client)
    assert not skipped
    assert len(entries) == 1
    return entries[0][1]


def _sync(schema, records, mdata=None):
    client = _client()
    stream = SurveyResponseExport(client=client)
    entry = MagicMock()
    entry.schema = Schema.from_dict(copy.deepcopy(schema))
    entry.metadata = metadata.to_list(mdata or metadata.write(metadata.new(), (), "selected", True))
    stream.catalog = MagicMock()
    stream.catalog.get_stream.return_value = entry
    state = {}
    with patch.object(stream, "get_records", return_value=iter(copy.deepcopy(records))), \
            patch("tap_qualtrics.streams.survey_response_export.write_schema") as write_schema, \
            patch("tap_qualtrics.streams.survey_response_export.write_record") as write_record:
        stream.sync(state, Transformer(), {"id": "SV_1"})
    return write_schema.call_args.args[1], [call.args[1] for call in write_record.call_args_list], state


def test_discovery_and_sync_preserve_unsampled_and_mixed_type_answers():
    client = _client()
    samples = [
        {"responseId": "R_1", "values": {"recordedDate": "2021-01-01T00:00:00Z", "Q1": 1},
         "labels": {"status": "complete"}},
        {"responseId": "R_2", "values": {"recordedDate": "2021-01-02T00:00:00Z", "Q2": "sample"},
         "labels": {"Q2": "sample label"}, "displayedValues": {"Q2": "sample display"}},
    ]
    schema = _discovered_schema(client, samples)
    record = {
        "responseId": "R_3", "survey_id": "SV_1", "recorded_date": "2021-01-03T00:00:00Z",
        "values": {"recordedDate": "2021-01-03T00:00:00Z", "Q1": 7.5, "Q_new": {"answer": [1, "x"]}},
        "labels": {"Q_new": "new label"},
        "displayedValues": {"Q_new": "new display"},
    }
    text_record = copy.deepcopy(record)
    text_record["responseId"] = "R_4"
    text_record["values"]["Q1"] = "text"
    output_schema, output, state = _sync(schema, [record, text_record])
    assert len(output) == 2
    for emitted, expected in zip(output, [record, text_record]):
        assert {field: emitted[field] for field in ANSWER_FIELDS} == {
            field: expected[field] for field in ANSWER_FIELDS
        }
        assert emitted["responseId"] == expected["responseId"]
        assert utils.strptime_to_utc(emitted["recorded_date"]) == utils.strptime_to_utc(expected["recorded_date"])
    for field in ANSWER_FIELDS:
        assert output_schema["properties"][field]["patternProperties"] == {".*": {}}
    assert "Q2" in schema["properties"]["values"]["properties"]
    for item in output:
        Draft4Validator(output_schema).validate(item)
    assert utils.strptime_to_utc(
        state["bookmarks"]["survey_response_export__SV_1"]["recorded_date"]
    ) == utils.strptime_to_utc(record["recorded_date"])


def test_existing_catalog_is_updated_without_mutation_and_respects_selection():
    schema = {
        "type": "object",
        "properties": {
            "responseId": {"type": "string"}, "survey_id": {"type": "string"},
            "recorded_date": {"type": "string"},
            "values": {"type": ["null", "object"], "properties": {
                "Q1": {"type": ["null", "integer"]}, "Q_hidden": {"type": "string"},
            }},
            "labels": {"type": ["null", "object"], "properties": {"old": {"type": "string"}}},
        },
    }
    original_schema = copy.deepcopy(schema)
    mdata = metadata.write(metadata.new(), (), "selected", True)
    mdata = metadata.write(mdata, ("properties", "values", "properties", "Q_hidden"), "selected", False)
    record = {
        "responseId": "R_1", "survey_id": "SV_1", "recorded_date": "2021-01-01T00:00:00Z",
        "values": {"Q1": 7.5, "Q_hidden": "private", "Q_new": True},
        "labels": {"Q_new": "label"}, "displayedValues": {"Q_new": "display"},
    }
    output_schema, output, _ = _sync(schema, [record], mdata)
    expected = copy.deepcopy(record)
    del expected["values"]["Q_hidden"]
    assert output == [expected]
    assert schema == original_schema
    Draft4Validator(output_schema).validate(output[0])


def test_discovery_includes_answer_maps_missing_from_sample():
    schema = _discovered_schema(_client(), [{"responseId": "R_1", "values": {"recordedDate": "2021-01-01T00:00:00Z"}}])
    for field in ANSWER_FIELDS:
        assert schema["properties"][field]["patternProperties"] == {".*": {}}


def test_unselected_answer_map_remains_unselected():
    schema = {
        "type": "object",
        "properties": {
            "responseId": {"type": "string"},
            "recorded_date": {"type": "string"},
            "values": {"type": ["null", "object"], "properties": {"Q1": {"type": "integer"}}},
        },
    }
    mdata = metadata.write(metadata.new(), (), "selected", True)
    mdata = metadata.write(mdata, ("properties", "values"), "selected", False)
    record = {
        "responseId": "R_1", "recorded_date": "2021-01-01T00:00:00Z",
        "values": {"Q1": 1, "Q_new": "answer"},
    }
    _, output, _ = _sync(schema, [record], mdata)
    assert "values" not in output[0]


def test_selected_dynamic_stream_preserves_answers_through_parent_sync():
    client = _client()
    client.config = {"start_date": client.start_date}
    schema = _discovered_schema(client, [{
        "responseId": "R_sample",
        "values": {"recordedDate": "2021-01-01T00:00:00Z", "Q1": 1},
    }])
    stream_id = "survey_response_export__SV_1"
    mdata = metadata.to_map(metadata.get_standard_metadata(
        schema=schema, key_properties=["responseId"], valid_replication_keys=["recorded_date"],
        replication_method="INCREMENTAL",
    ))
    mdata = metadata.write(mdata, (), "selected", True)
    mdata = metadata.write(mdata, (), "parent-tap-stream-id", "surveys")
    mdata = metadata.write(mdata, ("properties", "recorded_date"), "inclusion", "automatic")
    catalog = discover()
    catalog.streams.append(CatalogEntry(
        stream=stream_id, tap_stream_id=stream_id, schema=Schema.from_dict(schema),
        key_properties=["responseId"], metadata=metadata.to_list(mdata),
    ))
    client.get.return_value = {"result": {"elements": [
        {"id": "SV_1", "lastModified": "2021-01-01T00:00:00Z"},
    ]}}
    record = {
        "responseId": "R_1", "survey_id": "SV_1", "recorded_date": "2021-01-02T00:00:00Z",
        "values": {"Q1": 7.5, "Q_new": "answer"},
        "labels": {"Q_new": "label"}, "displayedValues": {"Q_new": "display"},
    }
    state = {}
    with patch.object(SurveyResponseExport, "get_records", return_value=iter([copy.deepcopy(record)])), \
            patch("tap_qualtrics.streams.survey_response_export.write_schema") as write_schema, \
            patch("tap_qualtrics.streams.survey_response_export.write_record") as write_record, \
            patch("singer.write_state"):
        sync(client, client.config, catalog, state)
    assert write_record.call_count == 1
    emitted = write_record.call_args.args[1]
    for field in ANSWER_FIELDS:
        assert emitted[field] == record[field]
    Draft4Validator(write_schema.call_args.args[1]).validate(emitted)
    assert "survey_response_export__SV_1" in state["bookmarks"]
