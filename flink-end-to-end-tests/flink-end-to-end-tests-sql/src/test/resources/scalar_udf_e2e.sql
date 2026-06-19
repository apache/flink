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
    id INT,
    str VARCHAR
) WITH (
    'connector' = 'filesystem',
    'path' = '$RESULT',
    'sink.rolling-policy.rollover-interval' = '2s',
    'sink.rolling-policy.check-interval' = '2s',
    'format' = 'debezium-json'
);

ADD JAR '$JAR_PATH';
create function func1 as 'org.apache.flink.table.toolbox.StringRegexReplaceFunction' LANGUAGE JAVA;

SET execution.runtime-mode = $MODE;

INSERT INTO JsonTable
SELECT id, func1(str, 'World', 'Flink') FROM (VALUES (1, 'Hello World')) AS T(id, str);

