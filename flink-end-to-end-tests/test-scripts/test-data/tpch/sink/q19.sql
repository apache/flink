CREATE TABLE q19(
  revenue DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q19',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
