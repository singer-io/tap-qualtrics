from tap_qualtrics.streams.abstracts import ChildBaseStream

class Survey(ChildBaseStream):
    tap_stream_id = "survey"
    key_properties = ["SurveyID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["LastModified"]
    data_key = "result"
    path = "/survey-definitions/{surveyId}"
    parent = "surveys"
    bookmark_value = None

