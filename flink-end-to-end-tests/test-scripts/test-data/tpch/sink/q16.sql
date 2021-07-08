CREATE TABLE q16(
  p_brand VARCHAR,
  p_type VARCHAR,
  p_size INT,
  supplier_cnt BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q16',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
