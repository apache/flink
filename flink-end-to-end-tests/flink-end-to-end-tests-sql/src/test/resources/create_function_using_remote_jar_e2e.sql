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

CREATE TABLE JsonTable (
   user_name STRING,
   order_cnt BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = '$RESULT',
    'sink.rolling-policy.rollover-interval' = '2s',
    'sink.rolling-policy.check-interval' = '2s',
    'format' = 'debezium-json'
);

CREATE $TEMPORARY FUNCTION count_agg AS 'org.apache.flink.table.toolbox.CountAggFunction'
    LANGUAGE JAVA USING JAR '$JAR_PATH';

SET execution.runtime-mode = $MODE;
SET table.exec.mini-batch.enabled = true;
SET table.exec.mini-batch.size = 5;
SET table.exec.mini-batch.allow-latency = 2s;

INSERT INTO JsonTable
SELECT user_name, count_agg(order_id)
FROM (VALUES (1, 'Bob'), (2, 'Bob'), (1, 'Alice')) T(order_id, user_name)
GROUP BY user_name;
