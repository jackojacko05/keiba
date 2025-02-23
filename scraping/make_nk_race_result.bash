# GCSからBigQueryにデータをロード
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  --encoding=UTF-8 \
  keiba.nk_race_results \
  gs://nk_race_result/race_result_consolidated.csv \
  nk_race_result_schema.json