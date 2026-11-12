# Changelog

## 1.0.1
  * `survey_response_export` discovery: paginate through all of `GET /surveys` instead of only the first page, so every survey is considered for a dataset. Fails explicitly instead of silently truncating if pagination loops or can't complete.

## 1.0.0
  * Validate Qualtrics request origins, reject redirects, and detect pagination cycles.
  * Add incremental survey versions, options, languages and translations streams.[#4](https://github.com/singer-io/tap-qualtrics/pull/4)

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit