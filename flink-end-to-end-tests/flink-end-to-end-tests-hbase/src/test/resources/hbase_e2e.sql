-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE MyHBaseSource (
  rowkey STRING,
  family1 ROW<f1c1 STRING>,
  family2 ROW<f2c1 STRING, f2c2 STRING>
) WITH (
  'connector' = '$HBASE_CONNECTOR',
  'table-name' = 'source',
  'zookeeper.quorum' = 'localhost:2181',
  'zookeeper.znode.parent' = '/hbase'
);

CREATE TABLE MyHBaseSink (
  rowkey STRING,
  family1 ROW<f1c1 STRING>,
  family2 ROW<f2c1 STRING, f2c2 STRING>
) WITH (
  'connector' = '$HBASE_CONNECTOR',
  'table-name' = 'sink',
  'zookeeper.quorum' = 'localhost:2181',
  'zookeeper.znode.parent' = '/hbase',
  'sink.buffer-flush.max-rows' = '1',
  'sink.buffer-flush.interval' = '2s'
);

CREATE FUNCTION RegReplace AS 'org.apache.flink.table.toolbox.StringRegexReplaceFunction';

INSERT INTO MyHBaseSink
SELECT
  rowkey,
  ROW(a),
  ROW(b, c)
FROM (
  SELECT
    rowkey,
    RegReplace(family1.f1c1, 'v', 'value') as a,
    family2.f2c1 as b,
    family2.f2c2 as c
  FROM MyHBaseSource)
source;

