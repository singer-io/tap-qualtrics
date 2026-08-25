"""Unit tests for tap_qualtrics/schema.py."""
import os
import unittest
from unittest.mock import patch

from tap_qualtrics.schema import (
    _infer_json_type,
    get_abs_path,
    get_schemas,
    infer_schema,
    load_schema_references,
)


class TestInferJsonType(unittest.TestCase):

    def test_none_returns_null_string(self):
        self.assertEqual(_infer_json_type(None), {"type": ["null", "string"]})

    def test_bool_true(self):
        self.assertEqual(_infer_json_type(True), {"type": ["null", "boolean"]})

    def test_bool_false(self):
        self.assertEqual(_infer_json_type(False), {"type": ["null", "boolean"]})

    def test_integer(self):
        self.assertEqual(_infer_json_type(42), {"type": ["null", "integer"]})

    def test_float(self):
        self.assertEqual(_infer_json_type(3.14), {"type": ["null", "number"]})

    def test_empty_list(self):
        result = _infer_json_type([])
        self.assertEqual(result["type"], ["null", "array"])
        self.assertEqual(result["items"], {})

    def test_list_of_ints(self):
        result = _infer_json_type([1, 2, 3])
        self.assertEqual(result["type"], ["null", "array"])
        self.assertEqual(result["items"], {"type": ["null", "integer"]})

    def test_dict(self):
        result = _infer_json_type({"key": "val"})
        self.assertEqual(result["type"], ["null", "object"])
        self.assertIn("key", result["properties"])

    def test_nested_dict(self):
        result = _infer_json_type({"outer": {"inner": 1}})
        self.assertEqual(result["properties"]["outer"]["type"], ["null", "object"])

    def test_datetime_string(self):
        result = _infer_json_type("2021-01-01T00:00:00Z")
        self.assertEqual(result, {"type": ["null", "string"], "format": "date-time"})

    def test_datetime_string_with_offset(self):
        result = _infer_json_type("2021-06-15T12:30:00+05:30")
        self.assertEqual(result.get("format"), "date-time")

    def test_plain_string(self):
        self.assertEqual(_infer_json_type("hello"), {"type": ["null", "string"]})

    def test_date_only_string_no_datetime(self):
        # "2021-01-01" lacks the T separator, should be plain string
        result = _infer_json_type("2021-01-01")
        self.assertNotEqual(result.get("format"), "date-time")


class TestInferSchema(unittest.TestCase):

    def test_empty_records(self):
        result = infer_schema([])
        self.assertEqual(result, {"type": "object", "properties": {}})

    def test_single_record(self):
        result = infer_schema([{"id": 1, "name": "Alice"}])
        self.assertIn("id", result["properties"])
        self.assertIn("name", result["properties"])

    def test_union_of_fields(self):
        result = infer_schema([{"id": 1}, {"name": "Bob"}])
        self.assertIn("id", result["properties"])
        self.assertIn("name", result["properties"])

    def test_none_record_skipped(self):
        result = infer_schema([None, {"id": 1}])
        self.assertIn("id", result["properties"])

    def test_first_field_type_wins(self):
        # second record has same key with different type; first type is kept
        result = infer_schema([{"x": 1}, {"x": "str"}])
        self.assertEqual(result["properties"]["x"]["type"], ["null", "integer"])


class TestGetAbsPath(unittest.TestCase):

    def test_returns_absolute(self):
        result = get_abs_path("schemas/users.json")
        self.assertTrue(os.path.isabs(result))

    def test_contains_given_path(self):
        result = get_abs_path("schemas/users.json")
        self.assertIn("schemas", result)


class TestLoadSchemaReferences(unittest.TestCase):

    def test_returns_dict(self):
        refs = load_schema_references()
        self.assertIsInstance(refs, dict)

    def test_no_shared_dir_returns_empty(self):
        with patch("tap_qualtrics.schema.os.path.exists", return_value=False):
            refs = load_schema_references()
        self.assertEqual(refs, {})

    def test_shared_dir_with_files_loads_refs(self):
        import json as _json
        from unittest.mock import mock_open
        fake_content = _json.dumps({"type": "object"})
        with patch("tap_qualtrics.schema.os.path.exists", return_value=True):
            with patch("tap_qualtrics.schema.os.listdir", return_value=["shared.json"]):
                with patch("tap_qualtrics.schema.os.path.isfile", return_value=True):
                    with patch("builtins.open", mock_open(read_data=fake_content)):
                        refs = load_schema_references()
        self.assertIn("shared/shared.json", refs)


class TestGetSchemas(unittest.TestCase):

    def test_returns_two_dicts(self):
        schemas, field_metadata = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(field_metadata, dict)

    def test_keys_match(self):
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    def test_common_streams_present(self):
        schemas, _ = get_schemas()
        for stream in ("users", "surveys", "groups", "distributions"):
            self.assertIn(stream, schemas)

    def test_dynamic_schema_streams_excluded(self):
        schemas, _ = get_schemas()
        self.assertNotIn("audit_export", schemas)
        self.assertNotIn("survey_response_export", schemas)

    def test_replication_keys_automatic(self):
        _, field_metadata = get_schemas()
        from singer import metadata as md
        distributions_mdata = md.to_map(field_metadata["distributions"])
        inclusion = md.get(distributions_mdata, ("properties", "modifiedDate"), "inclusion")
        self.assertEqual(inclusion, "automatic")


if __name__ == "__main__":
    unittest.main()
