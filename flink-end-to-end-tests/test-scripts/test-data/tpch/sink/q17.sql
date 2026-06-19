CREATE TABLE q17(
  avg_yearly DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q17',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
