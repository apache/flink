CREATE TABLE q22(
  cntrycode VARCHAR,
  numcust BIGINT,
  totacctbal DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '$RESULT_DIR/q22',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'sink.parallelism' = '1'
);
