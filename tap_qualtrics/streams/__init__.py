from tap_qualtrics.streams.users import Users
from tap_qualtrics.streams.user import User
from tap_qualtrics.streams.groups import Groups
from tap_qualtrics.streams.group_users import GroupUsers
from tap_qualtrics.streams.audit_events_types import AuditEventsTypes
from tap_qualtrics.streams.libraries import Libraries
from tap_qualtrics.streams.libraries_messages import LibrariesMessages
from tap_qualtrics.streams.libraries_survey_questions import LibrariesSurveyQuestions
from tap_qualtrics.streams.events import Events
from tap_qualtrics.streams.directories import Directories
from tap_qualtrics.streams.directories_contacts import DirectoriesContacts
from tap_qualtrics.streams.directories_contact import DirectoriesContact
from tap_qualtrics.streams.mailinglists import Mailinglists
from tap_qualtrics.streams.segments import Segments
from tap_qualtrics.streams.segment_contacts import SegmentContacts
from tap_qualtrics.streams.surveys import Surveys
from tap_qualtrics.streams.survey import Survey
from tap_qualtrics.streams.questions import Questions
from tap_qualtrics.streams.question import Question
from tap_qualtrics.streams.survey_options import SurveyOptions

STREAMS = {
    "users": Users,
    "user": User,
    "groups": Groups,
    "group_users": GroupUsers,
    "audit_events_types": AuditEventsTypes,
    "libraries": Libraries,
    "libraries_messages": LibrariesMessages,
    "libraries_survey_questions": LibrariesSurveyQuestions,
    "events": Events,
    "directories": Directories,
    "directories_contacts": DirectoriesContacts,
    "directories_contact": DirectoriesContact,
    "mailinglists": Mailinglists,
    "segments": Segments,
    "segment_contacts": SegmentContacts,
    "surveys": Surveys,
    "survey": Survey,
    "questions": Questions,
    "question": Question,
    "survey_options": SurveyOptions,
}

