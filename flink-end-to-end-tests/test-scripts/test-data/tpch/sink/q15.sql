CREATE TABLE q15(
  s_suppkey BIGINT,
  s_name VARCHAR,
  s_address VARCHAR,
  s_phone VARCHAR,
  total_revenue DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q15',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
