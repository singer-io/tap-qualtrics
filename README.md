# tap-qualtrics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Qualtrics API](https://api.qualtrics.com).
- Extracts the following resources (45 streams across 11 groups):
  - Users, Groups, Audit, Libraries, Directories, Mailing Lists, Segments, Samples, Surveys, Distributions, Tickets
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

## Streams

### Users

**[users](https://api.qualtrics.com/c9eeb409d7fe2-list-users)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[user](https://api.qualtrics.com/df14e6c98ea7f-get-user)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

### Groups

**[groups](https://api.qualtrics.com/0f7e43cc91c22-list-groups)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[group_users](https://api.qualtrics.com/1e833350364cf-list-users-in-group)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

### Audit

**[audit_events_types](https://api.qualtrics.com/e15904ed04110-list-event-types)**
- Primary keys: `name`
- Replication strategy: FULL_TABLE

**[audit_events](https://api.qualtrics.com/821e93ec17a62-list-events)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[audit_export_event_types](https://api.qualtrics.com/d2af757a5a36b-list-all-audit-event-types)**
- Primary keys: `name`
- Replication strategy: FULL_TABLE

**[audit_export](https://api.qualtrics.com/b77c1ac41d7db-retrieves-a-download-for-an-export-job)**
- Primary keys: `id`
- Replication strategy: INCREMENTAL
- Replication key: `timestamp`

### Libraries

**[libraries](https://api.qualtrics.com/7f0b7d82d44fc-list-libraries)**
- Primary keys: `libraryId`
- Replication strategy: FULL_TABLE

**[library_messages](https://api.qualtrics.com/1d60a66b340fa-list-library-messages)** *(child of libraries)*
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[libraries_survey_questions](https://api.qualtrics.com/a15d1b5f6d6bb-list-library-questions)** *(child of libraries)*
- Primary keys: `libraryId`, `category`
- Replication strategy: FULL_TABLE

**[library_blocks](https://api.qualtrics.com/0f7b2e8996ed2-list-library-blocks)** *(child of libraries)*
- Primary keys: `libraryId`, `category`
- Replication strategy: FULL_TABLE

**[library_surveys](https://api.qualtrics.com/2c55b7ff8b0c7-list-surveys)** *(child of libraries)*
- Primary keys: `libraryId`, `category`
- Replication strategy: FULL_TABLE

### Directories

**[directories](https://api.qualtrics.com/013df5106e3c7-list-directories-for-a-brand)**
- Primary keys: `directoryId`
- Replication strategy: FULL_TABLE

**[directories_contacts](https://api.qualtrics.com/d326cdc7e69ae-list-directory-contacts)** *(child of directories)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

**[contact_transactions](https://api.qualtrics.com/4d20708240964-list-contact-transactions)** *(child of directories_contacts)*
- Primary keys: `transactionId`
- Replication strategy: FULL_TABLE

**[contact_frequency_rules](https://api.qualtrics.com/4ba9faac17632-list-contact-frequency-rules)** *(child of directories)*
- Primary keys: `ruleId`
- Replication strategy: FULL_TABLE

**[opted_out_contacts](https://api.qualtrics.com/54f50780937be-list-opted-out-directory-contacts)** *(child of directories)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

**[transaction_batches](https://api.qualtrics.com/d476c17b493db-list-transaction-batches)** *(child of directories)*
- Primary keys: `batchId`
- Replication strategy: FULL_TABLE


### Mailing Lists

**[mailing_lists](https://api.qualtrics.com/dd83f1535056c-list-mailing-lists)** *(child of directories)*
- Primary keys: `mailingListId`
- Replication strategy: INCREMENTAL
- Replication key: `lastModifiedDate`

**[mailing_list_contacts](https://api.qualtrics.com/af95194dd116e-list-contacts-in-mailing-list)** *(child of mailing_lists)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

**[mailing_list_bounced_contacts](https://api.qualtrics.com/e254b2bc7d890-list-bounced-contacts-in-mailing-list)** *(child of mailing_lists)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

**[mailing_list_opted_out_contacts](https://api.qualtrics.com/f65f4f88f8c9c-list-opted-out-contacts-in-mailing-list)** *(child of mailing_lists)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

### Segments

**[segments](https://api.qualtrics.com/dd0fc3f656462-list-segments)** *(child of directories)*
- Primary keys: `segmentId`
- Replication strategy: INCREMENTAL
- Replication key: `lastModifiedDate`

**[segment_contacts](https://api.qualtrics.com/9df25057f5f14-list-contacts-in-segment)** *(child of segments)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

### Samples

**[samples](https://api.qualtrics.com/1b6abe392d030-list-samples)** *(child of directories)*
- Primary keys: `sampleId`
- Replication strategy: FULL_TABLE

**[sample_contacts](https://api.qualtrics.com/f7b8d90819090-get-sample-contacts)** *(child of samples)*
- Primary keys: `contactId`
- Replication strategy: FULL_TABLE

### Surveys

**[surveys](https://api.qualtrics.com/2c55b7ff8b0c7-list-surveys)**
- Primary keys: `id`
- Replication strategy: INCREMENTAL
- Replication key: `lastModified`

**[survey](https://api.qualtrics.com/9d0928392673d-get-survey)** *(child of surveys)*
- Primary keys: `SurveyID`
- Replication strategy: FULL_TABLE

**[survey_quotas](https://api.qualtrics.com/371bb797b85f7-get-survey-quotas)** *(child of surveys)*
- Primary keys: `quotaId`
- Replication strategy: FULL_TABLE

**[survey_response_export](https://api.qualtrics.com/6b00592b9c013-start-response-export)** *(child of surveys — one stream per survey)*
- Primary keys: `responseId`
- Replication strategy: INCREMENTAL
- Replication key: `recordedDate`

### Dynamic Export Discovery

The tap includes export-based streams whose schema is not fixed ahead of time. In particular:

- `survey_response_export`
- `audit_export`

These streams are dynamic because each survey or event type can return a different payload shape. During discovery, the tap does not use a static schema file. Instead, it performs the following flow for each parent resource:

1. Lists the available parent objects (surveys or audit event types).
2. Creates an async export job with a `POST` request.
3. Polls the job status until it completes.
4. Downloads the generated export file.
5. Parses a sample payload and infers the schema.
6. Builds a dynamic catalog entry such as `survey_response_export__<survey_id>` or `audit_export__<event_name>`.

This is required for dynamic polling APIs where the schema is only known after data is fetched. Without creating a sample export and inspecting the returned records, the tap cannot infer the correct field structure for each survey or event type.

## Limitations

- Discovery-side impact
    - Each discovery run creates real export jobs against Qualtrics, consuming API quota, creating account-side artifacts, and adding latency.
    - The tap limits this impact by using small sample exports where supported, capping worker concurrency, and deduplicating parent resources so the same survey or event type does not spawn duplicate jobs in one run.

- Dynamic, discovery-time catalog
    - Catalog entries are generated only from what discovery actually finds, not from a static list of all possible objects.
    - A survey or audit event type with no records at discovery time is skipped entirely; no catalog entry is created for it.
    - As a result, new survey data or event activity that appears later will not be selectable or syncable until discovery is re-run and produces a fresh catalog entry.


### Distributions

**[distributions](https://api.qualtrics.com/234bb6b16cf6d-list-distributions)** *(child of surveys)*
- Primary keys: `id`
- Replication strategy: INCREMENTAL
- Replication key: `modifiedDate`

**[distribution_history](https://api.qualtrics.com/8840efb2c71a8-list-distribution-history)** *(child of distributions)*
- Primary keys: `distributionId`, `contactId`
- Replication strategy: FULL_TABLE

**[distribution_links](https://api.qualtrics.com/437447486af95-list-distribution-links)** *(child of distributions)*
- Primary keys: `contactId`, `link`
- Replication strategy: FULL_TABLE

**[sms_distributions](https://api.qualtrics.com/2c09bb20f50cc-list-sms-distribution)** *(child of surveys)*
- Primary keys: `id`
- Replication strategy: INCREMENTAL
- Replication key: `sendDate`

**[whatsapp_distributions](https://api.qualtrics.com/29bef98fe59cd-list-whats-app-distribution)** *(child of surveys)*
- Primary keys: `id`
- Replication strategy: FULL_TABLE

### Tickets

**[tickets](https://api.qualtrics.com/5ba3b0d2c0dda-retrieve-tickets-for-account)**
- Primary keys: `key`
- Replication strategy: FULL_TABLE

**[ticket_groups](https://api.qualtrics.com/c6b7e8f09506f-get-list-of-groups)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[ticket_teams](https://api.qualtrics.com/c37a403d80f13-get-list-of-ticket-teams)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[ticket_statuses](https://api.qualtrics.com/21bcbced156e1-set-statuses)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[ticket_retrieve_events](https://api.qualtrics.com/fd540196c20f7-retrieve-events-for-a-ticket)** *(child of tickets)*
- Primary keys: `ticketId`
- Replication strategy: FULL_TABLE

**[ticket_root_causes](https://api.qualtrics.com/a8894451706f0-retrieve-root-causes-for-a-ticket)** *(child of tickets)*
- Primary keys: `ticketId`
- Replication strategy: FULL_TABLE

### Miscellaneous

**[event_subscriptions](https://api.qualtrics.com/9bf03a83945b9-list-subscriptions)**
- Primary keys: `id`
- Replication strategy: FULL_TABLE

**[erasure_requests](https://api.qualtrics.com/1327c91f8a5a4-list-requests)**
- Primary keys: `id`
- Replication strategy: INCREMENTAL
- Replication key: `updated`

---

## Authentication

The tap uses OAuth2 Client Credentials flow. Add the following to your `config.json`:

```json
{
    "client_id": "your_client_id",
    "client_secret": "your_client_secret",
    "data_center": "sjc1",
    "start_date": "2024-01-01T00:00:00Z"
}
```

## Quick Start

1. **Install**

    ```bash
    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -e .
    ```

2. **Create `config.json`**

    ```json
    {
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "data_center": "sjc1",
        "start_date": "2024-01-01T00:00:00Z"
    }
    ```

3. **Discovery**

    ```bash
    tap-qualtrics --config config.json --discover > catalog.json
    ```

4. **Sync**

    ```bash
    tap-qualtrics --config config.json --catalog catalog.json > output.json
    ```

5. **Sync with state**

    ```bash
    tap-qualtrics --config config.json --catalog catalog.json --state state.json > output.json
    ```

---

Copyright &copy; 2019 Stitch
