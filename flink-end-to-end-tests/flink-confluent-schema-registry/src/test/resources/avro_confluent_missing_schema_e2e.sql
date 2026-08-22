/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

SET 'table.dml-sync' = 'true';

CREATE TABLE avro_output_missing_schema (
  name STRING,
  favoriteNumber STRING,
  favoriteColor STRING,
  eventType STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '$OUTPUT_TOPIC',
  'properties.bootstrap.servers' = '$BOOTSTRAP_SERVERS',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '$SCHEMA_REGISTRY_URL',
  'avro-confluent.auto.register.schemas' = 'false'
);

INSERT INTO avro_output_missing_schema VALUES ('Grace', '1', 'black', 'INSERT');
