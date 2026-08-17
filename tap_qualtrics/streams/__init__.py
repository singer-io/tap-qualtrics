from tap_qualtrics.streams.directories_contacts import DirectoryContacts
from tap_qualtrics.streams.users import Users
from tap_qualtrics.streams.user import User
from tap_qualtrics.streams.groups import Groups
from tap_qualtrics.streams.group_users import GroupUsers

from tap_qualtrics.streams.audit_events_types import AuditEventsTypes
from tap_qualtrics.streams.audit_events import AuditEvents
from tap_qualtrics.streams.audit_export import AuditExport

from tap_qualtrics.streams.libraries import Libraries
from tap_qualtrics.streams.libraries_messages import LibraryMessages
from tap_qualtrics.streams.libraries_survey_questions import LibrariesSurveyQuestions
from tap_qualtrics.streams.library_blocks import LibraryBlocks
from tap_qualtrics.streams.library_surveys import LibrarySurveys

from tap_qualtrics.streams.directories import Directories
from tap_qualtrics.streams.contact_transactions import ContactTransactions
from tap_qualtrics.streams.contact_frequency_rules import ContactFrequencyRules
from tap_qualtrics.streams.opted_out_contacts import OptedOutContacts

from tap_qualtrics.streams.mailing_lists import MailingLists
from tap_qualtrics.streams.mailing_list_contacts import MailingListContacts
from tap_qualtrics.streams.mailing_list_bounced_contacts import MailingListBouncedContacts
from tap_qualtrics.streams.mailing_list_opted_out_contacts import MailingListOptedOutContacts

from tap_qualtrics.streams.segments import Segments
from tap_qualtrics.streams.segment_contacts import SegmentContacts

from tap_qualtrics.streams.samples import Samples
from tap_qualtrics.streams.sample_contacts import SampleContacts
from tap_qualtrics.streams.sample_definitions import SampleDefinitions
from tap_qualtrics.streams.transaction_batches import TransactionBatches

from tap_qualtrics.streams.surveys import Surveys
from tap_qualtrics.streams.survey import Survey
from tap_qualtrics.streams.survey_quotas import SurveyQuotas
from tap_qualtrics.streams.survey_response_export import SurveyResponseExport

from tap_qualtrics.streams.distributions import Distributions
from tap_qualtrics.streams.distribution_history import DistributionHistory
from tap_qualtrics.streams.distribution_links import DistributionLinks
from tap_qualtrics.streams.sms_distributions import SmsDistributions
from tap_qualtrics.streams.whatsapp_distributions import WhatsappDistributions

from tap_qualtrics.streams.event_subscriptions import EventSubscriptions
from tap_qualtrics.streams.erasure_requests import ErasureRequests

from tap_qualtrics.streams.ticket_groups import TicketGroups
from tap_qualtrics.streams.ticket_teams import TicketTeams
from tap_qualtrics.streams.ticket_statuses import TicketStatuses
from tap_qualtrics.streams.tickets_export import TicketsExport
from tap_qualtrics.streams.ticket_relative_events import TicketRelativeEvents
from tap_qualtrics.streams.ticket_root_causes import TicketRootCauses
from tap_qualtrics.streams.poll_ticket_export import PollTicketExport

STREAMS = {
    # Users / Groups
    "users": Users,
    "user": User,
    "groups": Groups,
    "group_users": GroupUsers,
    # Audit
    "audit_events_types": AuditEventsTypes,
    "audit_events": AuditEvents,
    "audit_export": AuditExport,
    # Libraries
    "libraries": Libraries,
    "library_messages": LibraryMessages,
    "libraries_survey_questions": LibrariesSurveyQuestions,
    "library_blocks": LibraryBlocks,
    "library_surveys": LibrarySurveys,
    # Directories / Contacts
    "directories": Directories,
    "contact_transactions": ContactTransactions,
    "contact_frequency_rules": ContactFrequencyRules,
    "directories_contacts": DirectoryContacts,
    "opted_out_contacts": OptedOutContacts,
    # Mailing Lists
    "mailing_lists": MailingLists,
    "mailing_list_contacts": MailingListContacts,
    "mailing_list_bounced_contacts": MailingListBouncedContacts,
    "mailing_list_opted_out_contacts": MailingListOptedOutContacts,
    # Segments
    "segments": Segments,
    "segment_contacts": SegmentContacts,
    # Samples
    "samples": Samples,
    "sample_contacts": SampleContacts,
    "sample_definitions": SampleDefinitions,
    # Transaction Batches
    "transaction_batches": TransactionBatches,
    # Surveys
    "surveys": Surveys,
    "survey": Survey,
    # "survey_flows": SurveyFlows,
    # "survey_versions": SurveyVersions,
    # "survey_questions": SurveyQuestions,
    # "survey_options": SurveyOptions,
    "survey_quotas": SurveyQuotas,
    # "survey_languages": SurveyLanguages,
    # "survey_translations": SurveyTranslations,
    "survey_response_export": SurveyResponseExport,
    # Distributions
    "distributions": Distributions,
    "distribution_history": DistributionHistory,
    "distribution_links": DistributionLinks,
    "sms_distributions": SmsDistributions,
    "whatsapp_distributions": WhatsappDistributions,
    # Misc
    "event_subscriptions": EventSubscriptions,
    "erasure_requests": ErasureRequests,
    # Tickets
    "ticket_groups": TicketGroups,
    "ticket_teams": TicketTeams,
    "ticket_statuses": TicketStatuses,
    "tickets_export": TicketsExport,
    "ticket_relative_events": TicketRelativeEvents,
    "ticket_root_causes": TicketRootCauses,
    "poll_ticket_export": PollTicketExport,
}


