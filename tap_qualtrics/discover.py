import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_qualtrics.exceptions import QualtricsForbiddenError
from tap_qualtrics.schema import get_schemas
from tap_qualtrics.streams import STREAMS

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


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe each parent stream for read access and remove inaccessible ones in place."""
    inaccessible = [
        name
        for name, stream_cls in STREAMS.items()
        if name in schemas and not stream_cls(client=client).check_access()
    ]

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

    return catalog

