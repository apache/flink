--/*
-- * Licensed to the Apache Software Foundation (ASF) under one
-- * or more contributor license agreements.  See the NOTICE file
-- * distributed with this work for additional information
-- * regarding copyright ownership.  The ASF licenses this file
-- * to you under the Apache License, Version 2.0 (the
-- * "License"); you may not use this file except in compliance
-- * with the License.  You may obtain a copy of the License at
-- *
-- *     http://www.apache.org/licenses/LICENSE-2.0
-- *
-- * Unless required by applicable law or agreed to in writing, software
-- * distributed under the License is distributed on an "AS IS" BASIS,
-- * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- * See the License for the specific language governing permissions and
-- * limitations under the License.
-- */

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
