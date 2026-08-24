import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_qualtrics.exceptions import QualtricsForbiddenError, QualtricsError
from tap_qualtrics.schema import get_schemas
from tap_qualtrics.streams import STREAMS
from tap_qualtrics.streams.audit_export import AuditExport
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport

# Streams whose catalog entries are built at runtime from live API data.
_DYNAMIC_SCHEMA_STREAMS = {AuditExport.tap_stream_id, SurveyResponseExport.tap_stream_id}

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams whose parent was excluded. Repeats until stable."""
    changed = True
    while changed:
        changed = False
        for name, stream_cls in list(STREAMS.items()):
            if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because parent stream '%s' is not accessible.",
                    name,
                    stream_cls.parent,
                )
                schemas.pop(name, None)
                field_metadata.pop(name, None)
                changed = True


def _topological_order(names, streams_map):
    """Yield stream names so every parent comes before its children."""
    ordered, visited = [], set()

    def visit(name):
        if name in visited:
            return
        visited.add(name)
        parent = (streams_map.get(name) or type("_", (), {"parent": None})).parent
        if parent and parent in names:
            visit(parent)
        ordered.append(name)

    for name in names:
        visit(name)
    return ordered


def _fetch_sample_record(client, path):
    """Return the first record from path (pageSize=1), or None on any error."""
    try:
        resp = client.get(path, params={"pageSize": 1})
        result = resp.get("result") or {}
        elements = result.get("elements") or result.get("result") or []
        return elements[0] if elements else None
    except Exception:
        return None


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe every stream (parents and children) and remove inaccessible ones."""
    inaccessible = []
    # stream_name -> one sample record (enriched with parent context for grandchild probes)
    parent_samples = {}

    for name in _topological_order(set(schemas), STREAMS):
        stream_cls = STREAMS.get(name)
        if not stream_cls:
            continue
        parent_name = stream_cls.parent
        if parent_name and parent_name in inaccessible:
            continue  # pruned below with _prune_inaccessible_children

        parent_record = parent_samples.get(parent_name) if parent_name else None
        instance = stream_cls(client=client)

        if not instance.check_access(parent_record):
            inaccessible.append(name)
            continue

        # Fetch a sample record so this stream's children can be probed
        if parent_record is not None:
            probe_path = instance._make_probe_path(parent_record)
        elif not stream_cls.parent:
            probe_path = getattr(instance, "path", None)
        else:
            probe_path = None

        if probe_path:
            sample = _fetch_sample_record(client, probe_path)
            if sample:
                parent_samples[name] = instance._enrich_sample(sample, parent_record or {})

    for name in inaccessible:
        schemas.pop(name, None)
        field_metadata.pop(name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise QualtricsForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    elif inaccessible:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible),
        )


def discover(client=None) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    When a client is provided, streams that return 403 are excluded.
    """
    schemas, field_metadata = get_schemas()

    if client is not None:
        _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if stream_name in _DYNAMIC_SCHEMA_STREAMS:
            continue  # replaced below by per-parent entries
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    if client is not None:
        _add_dynamic_entries(client, catalog)

    return catalog


def _add_dynamic_entries(client, catalog: Catalog) -> None:
    """Append per-parent catalog entries for dynamic-schema streams."""
    for stream_cls in (AuditExport, SurveyResponseExport):
        try:
            entries = stream_cls.discover_dynamic_entries(client)
        except QualtricsError as exc:
            LOGGER.warning("Skipping dynamic entries for '%s' during discovery: %s", stream_cls.tap_stream_id, exc)
            continue
        for stream_name, schema_dict, key_props in entries:
            mdata = metadata.to_list(
                metadata.write(
                    metadata.write(
                        metadata.new(),
                        (),
                        "table-key-properties",
                        key_props,
                    ),
                    (),
                    "selected",
                    False,
                )
            )
            catalog.streams.append(
                CatalogEntry(
                    stream=stream_name,
                    tap_stream_id=stream_name,
                    key_properties=key_props,
                    schema=Schema.from_dict(schema_dict),
                    metadata=mdata,
                )
            )

