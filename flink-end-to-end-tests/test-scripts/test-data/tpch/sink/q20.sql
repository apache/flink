CREATE TABLE q20(
  s_name VARCHAR,
  s_address VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q20',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
