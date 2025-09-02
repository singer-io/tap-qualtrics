from tap_qualtrics.streams.abstracts import ChildBaseStream

class SurveyOptions(ChildBaseStream):
    tap_stream_id = "survey_options"
    key_properties = ["SurveyStartDate"]
    replication_method = "INCREMENTAL"
    data_key = "result"
    path = "/survey-definitions/{surveyId}/options"
    parent = "survey"
    bookmark_value = None

