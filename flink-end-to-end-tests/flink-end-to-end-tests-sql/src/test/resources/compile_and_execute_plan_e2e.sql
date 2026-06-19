/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

SET 'execution.runtime-mode' = 'streaming';
SET 'table.dml-sync' = 'true';

CREATE TABLE $TABLE1 (
    msg_id INTEGER,
    msg STRING
) WITH (
    'connector' = 'filesystem',
    'sink.rolling-policy.rollover-interval' = '2s',
    'sink.rolling-policy.check-interval' = '2s',
    'path' = '$RESULT/$TABLE1',
    'format' = 'csv');

CREATE TABLE $TABLE2 (
    emp_id INTEGER,
    emp_name STRING
) WITH (
    'connector' = 'filesystem',
    'sink.rolling-policy.rollover-interval' = '2s',
    'sink.rolling-policy.check-interval' = '2s',
    'path' = '$RESULT/$TABLE2',
    'format' = 'csv');

COMPILE PLAN '$REMOTE_PLAN_DIR/$TABLE1.json' FOR INSERT INTO $TABLE1 SELECT * FROM (VALUES (1, 'Hello'), (2, 'Flink'), (3, 'Bye')) T(msg_id, msg);

SET 'table.plan.force-recompile' = 'true';

COMPILE PLAN '$REMOTE_PLAN_DIR/$TABLE1.json' FOR INSERT INTO $TABLE1 SELECT * FROM (VALUES (1, 'Meow'), (2, 'Purr')) T(msg_id, msg);

EXECUTE PLAN '$REMOTE_PLAN_DIR/$TABLE1.json';

COMPILE AND EXECUTE PLAN'$REMOTE_PLAN_DIR/$TABLE2.json' FOR INSERT INTO $TABLE2 SELECT * FROM (VALUES (1, 'Tom'), (2, 'Jerry')) T(emp_id, emp_name);
