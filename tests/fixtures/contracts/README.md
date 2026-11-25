# Stream Contract Fixtures

This directory stores sanitized VCR cassettes used by `tests/contracts/test_stream_contract_replay.py`.

## Record fixtures from live Qualtrics API

Run from repository root:

```bash
python scripts/record_contract_fixtures.py --config ./config.json
```

Record a subset of streams:

```bash
python scripts/record_contract_fixtures.py --config ./config.json --streams surveys,survey,users
```

Notes:

- Fixtures are sanitized before writing (authorization, tokens, credentials, cookies).
- Cassettes are stream-scoped (`<tap_stream_id>.yaml`).
- Dynamic stream bases (`audit_export`, `survey_response_export`) are excluded by default and can be recorded explicitly with `--streams`.

## Replay fixtures offline

```bash
python -m pytest tests/contracts/test_stream_contract_replay.py -q
```

To enforce fixture coverage for all static streams in CI/local:

```bash
ENFORCE_CONTRACT_FIXTURE_COVERAGE=1 python -m pytest tests/contracts/test_stream_contract_replay.py -q
```
