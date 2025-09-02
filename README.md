# tap-qualtrics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Qualtrics API].
- Extracts the following resources:
    - [Users](https://api.qualtrics.com/c9eeb409d7fe2-list-users)

    - [User](https://api.qualtrics.com/df14e6c98ea7f-get-user)

    - [Groups](https://api.qualtrics.com/0f7e43cc91c22-list-groups)

    - [GroupUsers](https://api.qualtrics.com/1e833350364cf-list-users-in-group)

    - [AuditEventsTypes](https://api.qualtrics.com/d2af757a5a36b-list-all-audit-event-types)

    - [Libraries](https://api.qualtrics.com/7f0b7d82d44fc-list-libraries)

    - [LibrariesMessages](https://api.qualtrics.com/1d60a66b340fa-list-library-messages)

    - [LibrariesSurveyQuestions](https://api.qualtrics.com/a15d1b5f6d6bb-list-library-questions)

    - [Events](https://api.qualtrics.com/821e93ec17a62-list-events)

    - [Directories](https://api.qualtrics.com/013df5106e3c7-list-directories-for-a-brand)

    - [DirectoriesContacts](https://api.qualtrics.com/d326cdc7e69ae-list-directory-contacts)

    - [DirectoriesContact](https://api.qualtrics.com/dd105bd826317-get-directory-contact)

    - [Mailinglists](https://api.qualtrics.com/dd83f1535056c-list-mailing-lists)

    - [Segments](https://api.qualtrics.com/dd0fc3f656462-list-segments)

    - [SegmentContacts](https://api.qualtrics.com/9df25057f5f14-list-contacts-in-segment)

    - [Surveys](https://api.qualtrics.com/2c55b7ff8b0c7-list-surveys)

    - [Survey](https://api.qualtrics.com/9d0928392673d-get-survey)

    - [Questions](https://api.qualtrics.com/957c5f8a4604b-get-questions)

    - [Question](https://api.qualtrics.com/1ebd0bb7008f8-get-question)

    - [SurveyOptions](https://api.qualtrics.com/021740be5b5b6-get-options)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[users](https://api.qualtrics.com/c9eeb409d7fe2-list-users)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[user](https://api.qualtrics.com/df14e6c98ea7f-get-user)**
- Data Key = result
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[groups](https://api.qualtrics.com/0f7e43cc91c22-list-groups)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[group_users](https://api.qualtrics.com/1e833350364cf-list-users-in-group)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[audit_events_types](https://api.qualtrics.com/d2af757a5a36b-list-all-audit-event-types)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[libraries](https://api.qualtrics.com/7f0b7d82d44fc-list-libraries)**
- Data Key = result.elements
- Primary keys: ['libraryId']
- Replication strategy: FULL_TABLE

**[libraries_messages](https://api.qualtrics.com/1d60a66b340fa-list-library-messages)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[libraries_survey_questions](https://api.qualtrics.com/a15d1b5f6d6bb-list-library-questions)**
- Data Key = result.elements
- Primary keys: ['questionId']
- Replication strategy: FULL_TABLE

**[events](https://api.qualtrics.com/821e93ec17a62-list-events)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[directories](https://api.qualtrics.com/013df5106e3c7-list-directories-for-a-brand)**
- Data Key = result.elements
- Primary keys: ['directoryId']
- Replication strategy: FULL_TABLE

**[directories_contacts](https://api.qualtrics.com/d326cdc7e69ae-list-directory-contacts)**
- Data Key = result.elements
- Primary keys: ['contactId']
- Replication strategy: FULL_TABLE

**[directories_contact](https://api.qualtrics.com/dd105bd826317-get-directory-contact)**
- Data Key = result
- Primary keys: ['contactId']
- Replication strategy: FULL_TABLE

**[mailinglists](https://api.qualtrics.com/dd83f1535056c-list-mailing-lists)**
- Data Key = result.elements
- Primary keys: ['mailingListId']
- Replication strategy: INCREMENTAL

**[segments](https://api.qualtrics.com/dd0fc3f656462-list-segments)**
- Data Key = result.elements
- Primary keys: ['segmentId']
- Replication strategy: INCREMENTAL

**[segment_contacts](https://api.qualtrics.com/9df25057f5f14-list-contacts-in-segment)**
- Data Key = result.elements
- Primary keys: ['contactId']
- Replication strategy: FULL_TABLE

**[surveys](https://api.qualtrics.com/2c55b7ff8b0c7-list-surveys)**
- Data Key = result.elements
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[survey](https://api.qualtrics.com/9d0928392673d-get-survey)**
- Data Key = result
- Primary keys: ['SurveyID']
- Replication strategy: INCREMENTAL

**[questions](https://api.qualtrics.com/957c5f8a4604b-get-questions)**
- Data Key = result.elements
- Primary keys: ['QuestionID']
- Replication strategy: FULL_TABLE

**[question](https://api.qualtrics.com/1ebd0bb7008f8-get-question)**
- Data Key = result
- Primary keys: ['QuestionID']
- Replication strategy: FULL_TABLE

**[survey_options](https://api.qualtrics.com/021740be5b5b6-get-options)**
- Data Key = result
- Primary keys: ['SurveyStartDate']
- Replication strategy: INCREMENTAL



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-qualtrics
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json

    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-qualtrics <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-qualtrics <api_user_email@your_company.com>",
        "request_timeout": 300
    }```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-qualtrics --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-qualtrics --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-qualtrics --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-qualtrics --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    While developing the qualtrics tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_qualtrics -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap_qualtrics --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2019 Stitch
