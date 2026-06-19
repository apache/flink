CREATE TABLE q3(
  l_orderkey BIGINT,
  revenue DOUBLE,
  o_orderdate DATE,
  o_shippriority INT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q3',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
