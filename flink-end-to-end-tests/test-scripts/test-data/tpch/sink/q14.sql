CREATE TABLE q14(
  promo_revenue DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q14',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
