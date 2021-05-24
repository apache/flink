CREATE TABLE q7(
  supp_nation VARCHAR,
  cust_nation VARCHAR,
  l_year BIGINT,
  revenue DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q7',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
