
CREATE TABLE customer (
  c_custkey BIGINT,
  c_name VARCHAR,
  c_address VARCHAR,
  c_nationkey BIGINT,
  c_phone VARCHAR,
  c_acctbal DOUBLE,
  c_mktsegment VARCHAR,
  c_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$TABLE_DIR/customer.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE lineitem (
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INT,
  l_quantity DOUBLE,
  l_extendedprice DOUBLE,
  l_discount DOUBLE,
  l_tax DOUBLE,
  l_returnflag VARCHAR,
  l_linestatus VARCHAR,
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct VARCHAR,
  l_shipmode VARCHAR,
  l_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$TABLE_DIR/lineitem.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE nation (
  n_nationkey BIGINT,
  n_name VARCHAR,
  n_regionkey BIGINT,
  n_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = '$TABLE_DIR/nation.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus VARCHAR,
  o_totalprice DOUBLE,
  o_orderdate DATE,
  o_orderpriority VARCHAR,
  o_clerk VARCHAR,
  o_shippriority INT,
  o_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' ='$TABLE_DIR/orders.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE part (
  p_partkey BIGINT,
  p_name VARCHAR,
  p_mfgr VARCHAR,
  p_brand VARCHAR,
  p_type VARCHAR,
  p_size INT,
  p_container VARCHAR,
  p_retailprice DOUBLE,
  p_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' ='$TABLE_DIR/part.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DOUBLE,
  ps_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' ='$TABLE_DIR/partsupp.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE region(
  r_regionkey BIGINT,
  r_name VARCHAR,
  r_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' ='$TABLE_DIR/region.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);

CREATE TABLE supplier (
  s_suppkey BIGINT,
  s_name VARCHAR,
  s_address VARCHAR,
  s_nationkey BIGINT,
  s_phone VARCHAR,
  s_acctbal DOUBLE,
  s_comment VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' ='$TABLE_DIR/supplier.csv',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.allow-comments' = 'true'
);
