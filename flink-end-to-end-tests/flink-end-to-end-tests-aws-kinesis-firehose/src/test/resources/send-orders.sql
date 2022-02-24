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
  'connector' = 'firehose',
  'delivery-stream' = 's3-stream',
  'aws.region' = 'ap-southeast-1',
  'aws.endpoint' = 'https://localstack:4566',
  'aws.credentials.provider' = 'BASIC',
  'aws.credentials.basic.accesskeyid' = 'accessKeyId',
  'aws.credentials.basic.secretkey' = 'secretAccessKey',
  'aws.trust.all.certificates' = 'true',
  'sink.http-client.protocol.version' = 'HTTP1_1',
  'sink.batch.max-size' = '1',
  'format' = 'json'
);

INSERT INTO orders VALUES ('A',1),('B',2),('C',3),('D',4),('E',5);
