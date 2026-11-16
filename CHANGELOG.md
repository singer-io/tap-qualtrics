# Changelog

## 2.0.0
  * Add incremental `survey_definitions`, `survey_questions`, and `survey_flows` streams.
  * Automatically backfill newly selected or lagging survey detail streams without replaying historical records in `surveys` or unrelated child streams.
  * Validate survey detail response shapes and commit survey traversal bookmarks only after all selected children succeed.
  * Compare incremental timestamps chronologically, including fractional seconds and equivalent timezone offsets.[#6](https://github.com/singer-io/tap-qualtrics/pull/6)

## 1.0.1
  * `survey_response_export` discovery: follow `GET /surveys` pagination so surveys beyond the first page are considered for a dataset. Fails explicitly if pagination cycles or reaches the 100,000-page safety cap; if a later page is forbidden, logs a warning and proceeds with surveys already listed. [#5](https://github.com/singer-io/tap-qualtrics/pull/5)

## 1.0.0
  * Validate Qualtrics request origins, reject redirects, and detect pagination cycles.
  * Add incremental survey versions, options, languages and translations streams.[#4](https://github.com/singer-io/tap-qualtrics/pull/4)

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit