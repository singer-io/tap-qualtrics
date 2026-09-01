import os
import json
import re
import singer
from typing import Any, Dict, Tuple
from singer import metadata
from tap_qualtrics.streams import STREAMS

LOGGER = singer.get_logger()

_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _infer_json_type(value: Any) -> Dict:
    if value is None:
        return {"type": ["null", "string"]}
    if isinstance(value, bool):
        return {"type": ["null", "boolean"]}
    if isinstance(value, int):
        return {"type": ["null", "integer"]}
    if isinstance(value, float):
        return {"type": ["null", "number"]}
    if isinstance(value, list):
        items_schema = _infer_json_type(value[0]) if value else {"type": ["null", "string"]}
        return {"type": ["null", "array"], "items": items_schema}
    if isinstance(value, dict):
        props = {k: _infer_json_type(v) for k, v in value.items()}
        return {"type": ["null", "object"], "properties": props}
    if isinstance(value, str) and _DATETIME_RE.match(value):
        return {"type": ["null", "string"], "format": "date-time"}
    return {"type": ["null", "string"]}


def infer_schema(records: list) -> Dict:
    """Build a JSON Schema by inspecting the union of fields across all records."""
    properties: Dict[str, Any] = {}
    for record in records:
        for key, value in (record or {}).items():
            if key not in properties:
                properties[key] = _infer_json_type(value)
    return {"type": "object", "properties": properties}


def get_abs_path(path: str) -> str:
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """
    Load the schema files from the schema folder and return the schema references.
    """
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file), encoding='utf-8') as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def get_schemas() -> Tuple[Dict, Dict]:
    """
    Load the schema references, prepare metadata for each streams and return schema and metadata for the catalog.
    """
    schemas = {}
    field_metadata = {}

    refs = load_schema_references()
    for stream_name, stream_obj in STREAMS.items():
        if getattr(stream_obj, "dynamic_schema", False):
            continue  # catalog entries built at runtime via discover_dynamic_entries()
        schema_path = get_abs_path("schemas/{}.json".format(stream_name))
        with open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata = metadata.to_map(mdata)

        if getattr(stream_obj, "parent", None):
            mdata = metadata.write(mdata, (), "parent-tap-stream-id", stream_obj.parent)

        automatic_keys = getattr(stream_obj, "replication_keys") or []
        for field_name in schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
