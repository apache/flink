# statement-set.q - BEGIN STATEMENT SET, END
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
# test statement set with streaming insert (with some negative cases)
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
SET '$internal.pipeline.job-id' = 'c6d2eb2ade68485fb4d1848294150861';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

BEGIN STATEMENT SET;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

BEGIN STATEMENT SET;
!output
org.apache.flink.table.gateway.service.utils.SqlExecutionException: Wrong statement after 'BEGIN STATEMENT SET'.
Only 'INSERT/CREATE TABLE AS' statement is allowed in Statement Set or use 'END' statement to terminate Statement Set.
!error

create table src (
  id int,
  str string
) with (
  'connector' = 'values'
);
!output
org.apache.flink.table.gateway.service.utils.SqlExecutionException: Wrong statement after 'BEGIN STATEMENT SET'.
Only 'INSERT/CREATE TABLE AS' statement is allowed in Statement Set or use 'END' statement to terminate Statement Set.
!error

SELECT id, str FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str);
!output
org.apache.flink.table.gateway.service.utils.SqlExecutionException: Wrong statement after 'BEGIN STATEMENT SET'.
Only 'INSERT/CREATE TABLE AS' statement is allowed in Statement Set or use 'END' statement to terminate Statement Set.
!error

INSERT INTO StreamingTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

END;
!output
+----------------------------------+
|                           job id |
+----------------------------------+
| c6d2eb2ade68485fb4d1848294150861 |
+----------------------------------+
1 row in set
!ok

END;
!output
org.apache.flink.table.gateway.service.utils.SqlExecutionException: No Statement Set to submit. 'END' statement should be used after 'BEGIN STATEMENT SET'.
!error

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
# test statement set with batch inserts
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
SET '$internal.pipeline.job-id' = 'e66a9bbf66c04a41b4d687e26c8296a0';
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

BEGIN STATEMENT SET;
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
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

END;
!output
+----------------------------------+
|                           job id |
+----------------------------------+
| e66a9bbf66c04a41b4d687e26c8296a0 |
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

# ==========================================================================
# special case test
# ==========================================================================

BEGIN STATEMENT SET;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

END;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

END;
!output
org.apache.flink.table.gateway.service.utils.SqlExecutionException: No Statement Set to submit. 'END' statement should be used after 'BEGIN STATEMENT SET'.
!error

BEGIN STATEMENT SET;
INSERT INTO BatchTable SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi'), (3, 'Hello'), (3, 'World'), (4, 'ADD'), (5, 'LINE'));
END;
!output
java.lang.IllegalArgumentException: only single statement supported
!error
