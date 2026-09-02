from singer import get_logger
from tap_qualtrics.exceptions import QualtricsInternalServerError
from tap_qualtrics.streams.abstracts import ChildBaseStream

LOGGER = get_logger()


class DistributionLinks(ChildBaseStream):
    tap_stream_id = "distribution_links"
    key_properties = ["contactId", "link"]
    replication_method = "INCREMENTAL"
    replication_keys = ["modifiedDate"]
    data_key = "result.elements"
    page_size = 100
    parent = "distributions"

    def __init__(self, client=None, catalog_entry=None):
        super().__init__(client=client, catalog_entry=catalog_entry)
        self._survey_id = ""

    def get_url_endpoint(self, parent_obj=None):
        distribution_id = (parent_obj or {}).get("id", "")
        # store survey_id as instance attr; get_records reads it for API requirements
        self._survey_id = (parent_obj or {}).get("survey_id", "")
        return f"distributions/{distribution_id}/links" if distribution_id else ""

    def modify_object(self, record, parent_record=None):
        if isinstance(record, dict) and isinstance(parent_record, dict):
            record["distributionId"] = parent_record.get("id", "")
            record["modifiedDate"] = parent_record.get("modifiedDate")
        return record

    def get_records(self, parent_id=None, bookmark=""):
        _ = (parent_id, bookmark)
        if not self.url_endpoint:
            return
        survey_id = getattr(self, "_survey_id", "")
        params = {"surveyId": survey_id} if survey_id else {}
        try:
            yield from self._paginate(self.url_endpoint, params)
        except QualtricsInternalServerError:
            # The /links endpoint only works for link-type distributions; skip others gracefully
            LOGGER.warning(
                (
                    "Skipping distribution_links for %s: API returned 500 "
                    "(not a link distribution)"
                ),
                self.url_endpoint,
            )

    def _make_probe_path(self, parent_record):
        distribution_id = (parent_record or {}).get("id", "")
        survey_id = (parent_record or {}).get("survey_id", "")
        path = f"distributions/{distribution_id}/links"
        if distribution_id and survey_id:
            return f"{path}?surveyId={survey_id}"
        return path if distribution_id else ""
