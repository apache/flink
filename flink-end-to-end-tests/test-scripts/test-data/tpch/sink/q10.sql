CREATE TABLE q10(
  c_custkey BIGINT,
  c_name VARCHAR,
  revenue DOUBLE,
  c_acctbal DOUBLE,
  n_name VARCHAR,
  c_address VARCHAR,
  c_phone VARCHAR,
  c_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q10',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
