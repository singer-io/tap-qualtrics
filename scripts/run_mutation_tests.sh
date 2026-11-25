#!/usr/bin/env bash
set -euo pipefail

echo "Running mutation tests for client behaviors"
mutmut run \
  --paths-to-mutate tap_qualtrics/client.py \
  --runner "python -m pytest tests/unittests/test_client.py tests/unittests/test_client_extended.py tests/unittests/test_client_oauth2.py tests/unittests/test_request_safety.py -q"

echo "Running mutation tests for abstract stream/bookmark behaviors"
mutmut run \
  --paths-to-mutate tap_qualtrics/streams/abstracts.py \
  --runner "python -m pytest tests/unittests/test_abstracts_full.py tests/unittests/test_abstract_child_stream_behaviors.py tests/unittests/test_parent_child_bookmark.py tests/unittests/test_survey_metadata_streams.py -q"

echo "Running mutation tests for bookmark-focused sync behaviors"
mutmut run \
  --paths-to-mutate tap_qualtrics/sync.py \
  --runner "python -m pytest tests/unittests/test_sync.py tests/unittests/test_sync_extended.py tests/unittests/test_discovery_sync_branch_behaviors.py -q"

echo "Mutation test runs completed. Use 'mutmut results' and 'mutmut show <id>' to inspect survivors."