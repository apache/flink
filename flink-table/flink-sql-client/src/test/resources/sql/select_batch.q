# select.q - SELECT query
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

# set default streaming mode and tableau result mode

SET 'execution.runtime-mode' = 'batch';
[INFO] Execute statement succeed.
!info

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
!info

SELECT id, COUNT(*) as cnt, COUNT(DISTINCT str) as uv
FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str)
GROUP BY id;
+----+-----+----+
| id | cnt | uv |
+----+-----+----+
|  1 |   1 |  1 |
|  2 |   2 |  1 |
+----+-----+----+
2 rows in set
!ok

SET 'table.local-time-zone' = 'UTC';
[INFO] Execute statement succeed.
!info

SELECT TIME '20:12:11' as time0,
 ts0, ts3, ts9,
 TO_TIMESTAMP_LTZ(1, 0) AS ts_ltz0,
 TO_TIMESTAMP_LTZ(1, 3) AS ts_ltz3,
 CAST(ts9 as TIMESTAMP_LTZ(9)) AS ts_ltz9
FROM (VALUES
  (1, TIMESTAMP '2021-04-13 20:12:11', TIMESTAMP '2021-04-13 20:12:11.123', TIMESTAMP '2021-04-13 20:12:11.123456789'),
  (2, TIMESTAMP '2021-04-13 21:12:11', TIMESTAMP '2021-04-13 21:12:11.001', TIMESTAMP '2021-04-13 21:12:11.1'))
   as T(id, ts0, ts3, ts9);
+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
|    time0 |                 ts0 |                     ts3 |                           ts9 |                 ts_ltz0 |                 ts_ltz3 |                       ts_ltz9 |
+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
| 20:12:11 | 2021-04-13 20:12:11 | 2021-04-13 20:12:11.123 | 2021-04-13 20:12:11.123456789 | 1970-01-01 00:00:01.000 | 1970-01-01 00:00:00.001 | 2021-04-13 20:12:11.123456789 |
| 20:12:11 | 2021-04-13 21:12:11 | 2021-04-13 21:12:11.001 | 2021-04-13 21:12:11.100000000 | 1970-01-01 00:00:01.000 | 1970-01-01 00:00:00.001 | 2021-04-13 21:12:11.100000000 |
+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
2 rows in set
!ok

# ==========================================================================
# Testing behavior of sql-client.display.max-column-width
# Only variable width columns are impacted at the moment => STRING, but not TIMESTAMP nor BOOLEAN
# ==========================================================================

CREATE TEMPORARY VIEW
  testUserData(name, dob, isHappy)
AS (VALUES
  ('30b5c1bb-0ac0-43d3-b812-fcb649fd2b07', TIMESTAMP '2001-01-13 20:11:11.123', true),
  ('91170c98-2cc5-4935-9ea6-12b72d32fb3c', TIMESTAMP '1994-02-14 21:12:11.123', true),
  ('8b012d93-6ece-48ad-a2ea-aa75ef7b1d60', TIMESTAMP '1979-03-15 22:13:11.123', false),
  ('09969d9e-d584-11eb-b8bc-0242ac130003', TIMESTAMP '1985-04-16 23:14:11.123', true)
);
[INFO] Execute statement succeed.
!info

SELECT * from testUserData;
+--------------------------------+-------------------------+---------+
|                           name |                     dob | isHappy |
+--------------------------------+-------------------------+---------+
| 30b5c1bb-0ac0-43d3-b812-fcb... | 2001-01-13 20:11:11.123 |    TRUE |
| 91170c98-2cc5-4935-9ea6-12b... | 1994-02-14 21:12:11.123 |    TRUE |
| 8b012d93-6ece-48ad-a2ea-aa7... | 1979-03-15 22:13:11.123 |   FALSE |
| 09969d9e-d584-11eb-b8bc-024... | 1985-04-16 23:14:11.123 |    TRUE |
+--------------------------------+-------------------------+---------+
4 rows in set
!ok

SET 'sql-client.display.max-column-width' = '10';
[INFO] Execute statement succeed.
!info

SELECT * from testUserData;
+------------+------------+---------+
|       name |        dob | isHappy |
+------------+------------+---------+
| 30b5c1b... | 2001-01... |    TRUE |
| 91170c9... | 1994-02... |    TRUE |
| 8b012d9... | 1979-03... |   FALSE |
| 09969d9... | 1985-04... |    TRUE |
+------------+------------+---------+
4 rows in set
!ok

SET 'sql-client.display.max-column-width' = '40';
[INFO] Execute statement succeed.
!info

SELECT * from testUserData;
+--------------------------------------+-------------------------+---------+
|                                 name |                     dob | isHappy |
+--------------------------------------+-------------------------+---------+
| 30b5c1bb-0ac0-43d3-b812-fcb649fd2b07 | 2001-01-13 20:11:11.123 |    TRUE |
| 91170c98-2cc5-4935-9ea6-12b72d32fb3c | 1994-02-14 21:12:11.123 |    TRUE |
| 8b012d93-6ece-48ad-a2ea-aa75ef7b1d60 | 1979-03-15 22:13:11.123 |   FALSE |
| 09969d9e-d584-11eb-b8bc-0242ac130003 | 1985-04-16 23:14:11.123 |    TRUE |
+--------------------------------------+-------------------------+---------+
4 rows in set
!ok

-- post-test cleanup + setting back default max width value
DROP TEMPORARY VIEW testUserData;
[INFO] Execute statement succeed.
!info

SET 'sql-client.display.max-column-width' = '30';
[INFO] Execute statement succeed.
!info

SELECT INTERVAL '1' DAY as dayInterval, INTERVAL '1' YEAR as yearInterval;
+-----------------+--------------+
|     dayInterval | yearInterval |
+-----------------+--------------+
| +1 00:00:00.000 |        +1-00 |
+-----------------+--------------+
1 row in set
!ok
