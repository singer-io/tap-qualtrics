from tap_qualtrics.streams.abstracts import FullTableStream


class DistributionLinks(FullTableStream):
    tap_stream_id = "distribution_links"
    key_properties = ["contactId", "link"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    page_size = 100
    parent = "distributions"

    def get_records(self, parent_id=None):
        distribution_id = (parent_id or {}).get("id") or parent_id
        survey_id = (parent_id or {}).get("_survey_id", "")
        if not distribution_id:
            return
        params = {"pageSize": self.page_size}
        if survey_id:
            params["surveyId"] = survey_id
        path = f"distributions/{distribution_id}/links"
        yield from self._paginate(path, params)
