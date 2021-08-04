CREATE TABLE q2(
  s_acctbal DOUBLE,
  s_name VARCHAR,
  n_name VARCHAR,
  p_partkey BIGINT,
  p_mfgr VARCHAR,
  s_addres VARCHAR,
  s_phone VARCHAR,
  s_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q2',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
