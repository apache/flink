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

# test only to verify the test job id.
SET '$internal.pipeline.job-id' = 'e68e7fabddfade4f42910980652582dc';
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
+----------------------------------+
|                           job id |
+----------------------------------+
| e68e7fabddfade4f42910980652582dc |
+----------------------------------+
1 row in set
!ok

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

# test only to verify the test job id.
SET '$internal.pipeline.job-id' = '29ba2263b9b86bd8a14b91487941bfe7';
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
+----------------------------------+
|                           job id |
+----------------------------------+
| 29ba2263b9b86bd8a14b91487941bfe7 |
+----------------------------------+
1 row in set
!ok

RESET '$internal.pipeline.job-id';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

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
