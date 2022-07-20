CREATE TABLE q4(
  o_orderpriority VARCHAR,
  order_count BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q4',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
