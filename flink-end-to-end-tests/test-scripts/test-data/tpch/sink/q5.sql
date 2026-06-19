CREATE TABLE q5(
  n_name VARCHAR,
  revenue DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q5',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
