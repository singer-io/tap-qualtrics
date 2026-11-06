# Changelog

## 0.0.3
  * `survey_response_export` discovery: paginate through all of `GET /surveys` instead of only the first page, so every survey is considered for a dataset. Fails explicitly instead of silently truncating if pagination loops or can't complete.

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit