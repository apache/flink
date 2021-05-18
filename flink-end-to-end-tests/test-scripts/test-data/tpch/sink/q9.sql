CREATE TABLE q9(
  nation VARCHAR,
  o_year BIGINT,
  sum_profit DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q9',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
