CREATE TABLE q12(
  l_shipmode VARCHAR,
  high_line_count INT,
  low_line_count INT
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q12',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
