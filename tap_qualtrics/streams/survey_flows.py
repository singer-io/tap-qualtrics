from tap_qualtrics.streams.abstracts import SurveyChildStream


class SurveyFlows(SurveyChildStream):
    tap_stream_id = 'survey_flows'
    key_properties = ['FlowID']
    replication_method = 'FULL_TABLE'
    data_key = 'result'
    path = 'survey-definitions/{survey_id}/flow'
    parent = 'surveys'

    def get_records(self, parent_id=None):
        survey_id = (parent_id or {}).get('id') or parent_id
        if not survey_id:
            return
        resp = self.client.get(f'survey-definitions/{survey_id}/flow')
        record = resp.get('result', {})
        if record:
            record['survey_id'] = survey_id
            yield record
