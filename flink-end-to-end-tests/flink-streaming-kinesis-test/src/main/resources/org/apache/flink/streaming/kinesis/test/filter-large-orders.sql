CREATE TABLE orders (
  `code` STRING,
  `quantity` BIGINT
) WITH (
  'connector' = 'kinesis',
  'stream' = 'orders',
  'aws.endpoint' = 'https://kinesalite:4567',
  'scan.stream.initpos' = 'TRIM_HORIZON',
  'scan.shard.discovery.intervalmillis' = '1000',
  'scan.shard.adaptivereads' = 'true',
  'format' = 'json'
);

CREATE TABLE large_orders (
  `code` STRING,
  `quantity` BIGINT
) WITH (
  'connector' = 'kinesis',
  'stream' = 'large_orders',
  'aws.region' = 'us-east-1',
  'sink.producer.verify-certificate' = 'false',
  'sink.producer.kinesis-port' = '4567',
  'sink.producer.kinesis-endpoint' = 'kinesalite',
  'sink.producer.aggregation-enabled' = 'false',
  'format' = 'json'
);

INSERT INTO large_orders SELECT * FROM orders WHERE quantity > 10;
