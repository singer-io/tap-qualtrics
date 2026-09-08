from singer import get_logger
from tap_qualtrics.exceptions import QualtricsError, QualtricsInternalServerError
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
        self._survey_id = (parent_obj or {}).get("survey_id", "")
        return f"distributions/{distribution_id}/links" if distribution_id else ""

    def modify_object(self, record, parent_record=None):
        if isinstance(record, dict) and isinstance(parent_record, dict):
            record["distributionId"] = parent_record.get("id", "")
            record["modifiedDate"] = parent_record.get("modifiedDate")
        return record

    def check_access(self, parent_record=None):
        """Probe distribution links using the required surveyId query param."""
        parent_record = parent_record or {}
        path = self._make_probe_path(parent_record)
        if not path:
            return True

        params = {"pageSize": 1}
        survey_id = (parent_record or {}).get("survey_id") or getattr(self, "_survey_id", "")
        if survey_id:
            params["surveyId"] = survey_id

        try:
            self.client.get(path, params=params)
            return True
        except QualtricsInternalServerError:
            LOGGER.warning(
                "Skipping %s for %s: API returned 500 (not a link distribution)",
                self.tap_stream_id,
                path,
            )
            return False

    def get_records(self, parent_id=None, bookmark=""):
        _ = (parent_id, bookmark)
        if not self.url_endpoint:
            return
        survey_id = getattr(self, "_survey_id", "")
        params = {"surveyId": survey_id} if survey_id else {}
        try:
            yield from self._paginate(self.url_endpoint, params)
        except QualtricsInternalServerError:
            LOGGER.warning(
                "Skipping %s for %s: API returned 500 (not a link distribution)",
                self.tap_stream_id,
                self.url_endpoint,
            )

    def _make_probe_path(self, parent_record):
        distribution_id = (parent_record or {}).get("id", "")
        return f"distributions/{distribution_id}/links" if distribution_id else ""
