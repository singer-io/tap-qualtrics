# Per-stream test matrix

Generated from `tests/base.py` and integration test scope conventions.

| stream | in_pr_tier | in_nightly_tier | discovery | all_fields | automatic_fields | interrupted_sync | pagination | bookmark | start_date | auth_config_negative | contract_replay | live_contract |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| audit_events |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| audit_events_types |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  |  |  | ✅ | ✅ |
| audit_export |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| audit_export_event_types |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| contact_frequency_rules |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| contact_transactions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| directories |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  |  |  | ✅ | ✅ |
| directories_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| distribution_history |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| distribution_links |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| distributions |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| erasure_requests |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| event_subscriptions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| group_users |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| groups | ✅ | ✅ | ✅ | ✅ | ✅ |  | ✅ |  |  |  | ✅ | ✅ |
| libraries |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  |  |  | ✅ | ✅ |
| libraries_survey_questions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| library_blocks |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| library_messages |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| library_surveys |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| mailing_list_bounced_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| mailing_list_contacts |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  |  |  | ✅ | ✅ |
| mailing_list_opted_out_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| mailing_lists | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |  | ✅ | ✅ |
| opted_out_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| sample_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| samples |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| segment_contacts |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| segments |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| sms_distributions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_definitions |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| survey_flows |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_languages |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_options |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_questions |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| survey_quotas |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_response_export |  | ✅ | ✅ | ✅ | ✅ | ✅ |  |  | ✅ |  | ✅ | ✅ |
| survey_translations |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| survey_versions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| surveys | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |  | ✅ | ✅ | ✅ | ✅ |
| ticket_groups |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| ticket_retrieve_events |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| ticket_root_causes |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| ticket_statuses |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| ticket_teams |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| tickets |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| transaction_batches |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| user |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
| users | ✅ | ✅ | ✅ | ✅ | ✅ |  | ✅ |  |  | ✅ | ✅ | ✅ |
| whatsapp_distributions |  |  |  |  |  |  |  |  |  |  | ✅ | ✅ |
