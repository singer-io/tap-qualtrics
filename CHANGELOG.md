# Changelog

## 2.0.0
  * Add incremental `survey_definitions`, `survey_questions`, and `survey_flows` streams.
  * Preserve object-valued question `Randomization` fields without converting them to strings.
  * Automatically backfill newly selected or lagging survey detail streams without replaying historical records in `surveys` or unrelated child streams.
  * Validate survey detail response shapes and commit survey traversal bookmarks only after all selected children succeed.
  * Compare incremental timestamps chronologically, including fractional seconds and equivalent timezone offsets.[#6](https://github.com/singer-io/tap-qualtrics/pull/6)

## 1.0.1
  *`survey_response_export` discovery: follow `GET /surveys` pagination (`result.nextPage`) so every survey the account can see is considered for a dataset, not only the first page of 100. Discovery now fails with an explicit error, instead of returning a partial catalog, if a `nextPage` URL repeats, if three consecutive pages return no new survey IDs while still reporting more pages, if `nextPage` is not a URL string, or if a page after the first returns 403. If the first page returns 403, discovery still logs a warning and returns no `survey_response_export` streams. Discovery now creates one sample export job per survey, so large accounts take longer and create more provider-side jobs [#5](https://github.com/singer-io/tap-qualtrics/pull/5)

## 1.0.0
  * Validate Qualtrics request origins, reject redirects, and detect pagination cycles.
  * Add incremental survey versions, options, languages and translations streams.[#4](https://github.com/singer-io/tap-qualtrics/pull/4)

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit