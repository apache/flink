CREATE TABLE q21(
  s_name VARCHAR,
  numwait BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q21',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
