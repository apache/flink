CREATE TABLE q8(
  o_year BIGINT,
  mkt_share DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q8',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
