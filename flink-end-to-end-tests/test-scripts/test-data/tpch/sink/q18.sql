CREATE TABLE q18(
  c_name VARCHAR,
  c_custkey BIGINT,
  o_orderkey BIGINT,
  o_orderdate DATE,
  o_totalprice DOUBLE,
  `sum(l_quantity)` DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q18',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
