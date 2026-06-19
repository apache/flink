CREATE TABLE q13(
  c_count BIGINT,
  custdist BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q13',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
