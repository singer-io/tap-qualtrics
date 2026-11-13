# Changelog

## 1.0.1
  * `survey_response_export` discovery: follow `GET /surveys` pagination so surveys beyond the first page are considered for a dataset. Fails explicitly if pagination cycles or reaches the 100,000-page safety cap; if a later page is forbidden, logs a warning and proceeds with surveys already listed. [#5](https://github.com/singer-io/tap-qualtrics/pull/5)

## 1.0.0
  * Validate Qualtrics request origins, reject redirects, and detect pagination cycles.
  * Add incremental survey versions, options, languages and translations streams.[#4](https://github.com/singer-io/tap-qualtrics/pull/4)

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit