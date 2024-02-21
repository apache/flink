# insert.q - insert
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SET 'table.dml-sync' = 'true';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# ==========================================================================
# test streaming insert
# ==========================================================================

SET 'execution.runtime-mode' = 'streaming';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table StreamingTable (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH',
  'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
!output
Job ID:
!info

RESET '$internal.pipeline.job-id';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SELECT * FROM StreamingTable;
!output
+----+----+-------------+
| op | id |         str |
+----+----+-------------+
| +I |  1 | Hello World |
| +I |  2 |          Hi |
| +I |  2 |          Hi |
| +I |  3 |       Hello |
| +I |  3 |       World |
| +I |  4 |         ADD |
| +I |  5 |        LINE |
+----+----+-------------+
7 rows in set
!ok

# ==========================================================================
# test streaming insert through compiled plan
# ==========================================================================

create table StreamingTableForPlan (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH3',
  'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# test path with scheme
COMPILE AND EXECUTE PLAN 'file://$VAR_STREAMING_PLAN_PATH/plan1.json' FOR INSERT INTO StreamingTableForPlan SELECT * FROM (VALUES (1, 'Hello'));
!output
Job ID:
!info

RESET '$internal.pipeline.job-id';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SELECT * FROM StreamingTableForPlan;
!output
+----+----+-------+
| op | id |   str |
+----+----+-------+
| +I |  1 | Hello |
+----+----+-------+
1 row in set
!ok


# test absolute path without scheme
COMPILE PLAN '$VAR_STREAMING_PLAN_PATH/plan2.json' FOR INSERT INTO StreamingTableForPlan SELECT * FROM (VALUES (1, 'Hello'));
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# test relative path without scheme
EXECUTE PLAN '$VAR_STREAMING_PLAN_RELATIVE_PATH/plan2.json';
!output
Job ID:
!info

RESET '$internal.pipeline.job-id';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SELECT * FROM StreamingTableForPlan;
!output
+----+----+-------+
| op | id |   str |
+----+----+-------+
| +I |  1 | Hello |
| +I |  1 | Hello |
+----+----+-------+
2 rows in set
!ok

# ==========================================================================
# test batch insert
# ==========================================================================

SET 'execution.runtime-mode' = 'batch';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

create table BatchTable (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_BATCH_PATH',
  'format' = 'csv'
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
!output
Job ID:
!info

SELECT * FROM BatchTable;
!output
+----+-------------+
| id |         str |
+----+-------------+
|  1 | Hello World |
|  2 |          Hi |
|  2 |          Hi |
|  3 |       Hello |
|  3 |       World |
|  4 |         ADD |
|  5 |        LINE |
+----+-------------+
7 rows in set
!ok

CREATE TABLE CtasTable
WITH (
  'connector' = 'filesystem',
  'path' = '$VAR_BATCH_CTAS_PATH',
  'format' = 'csv'
)
AS SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE')) T(id, str);
!output
Job ID:
!info

SELECT * FROM CtasTable;
!output
+----+-------------+
| id |         str |
+----+-------------+
|  1 | Hello World |
|  2 |          Hi |
|  2 |          Hi |
|  3 |       Hello |
|  3 |       World |
|  4 |         ADD |
|  5 |        LINE |
+----+-------------+
7 rows in set
!ok
