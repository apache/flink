CREATE TABLE q11(
  ps_partkey BIGINT,
  `value` DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q11',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
