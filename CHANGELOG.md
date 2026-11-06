# Changelog

## 0.0.3
  * `survey_response_export` discovery: follow `result.nextPage` when listing surveys via `GET /surveys`, so surveys past the first page (previously silently never considered for a dataset) are now discovered the same as any other survey.

## 0.0.2
  * Allow null `embeddedData` values in the distributions schema.

## 0.0.1
  * Initial Commit