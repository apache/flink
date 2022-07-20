CREATE TABLE q1 (
  l_returnflag VARCHAR,
  l_linestatus VARCHAR,
  sum_qty DOUBLE,
  sum_base_price DOUBLE,
  sum_disc_price DOUBLE,
  sum_charge DOUBLE,
  avg_qty DOUBLE,
  avg_price DOUBLE,
  avg_disc DOUBLE,
  count_order BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q1',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
