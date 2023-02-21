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

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
!info

SET 'table.dml-sync' = 'true';
[INFO] Execute statement succeed.
!info

# ==========================================================================
# test streaming insert
# ==========================================================================

SET 'execution.runtime-mode' = 'streaming';
[INFO] Execute statement succeed.
!info

create table StreamingTable (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH',
  'format' = 'csv'
);
[INFO] Execute statement succeed.
!info

INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
[INFO] Complete execution of the SQL update statement.
!info

SELECT * FROM StreamingTable;
+----+-------------+--------------------------------+
| op |          id |                            str |
+----+-------------+--------------------------------+
| +I |           1 |                    Hello World |
| +I |           2 |                             Hi |
| +I |           2 |                             Hi |
| +I |           3 |                          Hello |
| +I |           3 |                          World |
| +I |           4 |                            ADD |
| +I |           5 |                           LINE |
+----+-------------+--------------------------------+
Received a total of 7 rows
!ok

# ==========================================================================
# test batch insert
# ==========================================================================

SET 'execution.runtime-mode' = 'batch';
[INFO] Execute statement succeed.
!info

create table BatchTable (
  id int,
  str string
) with (
  'connector' = 'filesystem',
  'path' = '$VAR_BATCH_PATH',
  'format' = 'csv'
);
[INFO] Execute statement succeed.
!info

INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
[INFO] Complete execution of the SQL update statement.
!info

SELECT * FROM BatchTable;
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
