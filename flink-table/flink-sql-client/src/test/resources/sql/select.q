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

SET execution.runtime-mode = streaming;
[INFO] Session property has been set.
!info

SET sql-client.execution.result-mode = tableau;
[INFO] Session property has been set.
!info

# ==========================================================================
# validation test
# ==========================================================================

create table src (
  id int,
  str string
) with (
  'connector' = 'values',
  'data-id' = 'non-exist',
  'failing-source' = 'true'
);
[INFO] Execute statement succeed.
!info

SELECT UPPER(str), id FROM src;
[ERROR] Could not execute SQL statement. Reason:
java.lang.IllegalArgumentException: testing elements of values source shouldn't be empty.
!error

# ==========================================================================
# test stream query with tableau result mode
# (we can't test changelog mode and table mode in IT case)
# ==========================================================================

SELECT id, COUNT(*) as cnt, COUNT(DISTINCT str) as uv, max(ts) as max_ts
FROM (VALUES
  (1, 'Hello World', TIMESTAMP '2021-04-13 20:12:11.123456789'),
  (2, 'Hi', TIMESTAMP '2021-04-13 19:12:11.123456789'),
  (2, 'Hi', TIMESTAMP '2021-04-13 21:12:11.123456789')) as T(id, str, ts)
GROUP BY id;
+----+-------------+----------------------+----------------------+-------------------------------+
| op |          id |                  cnt |                   uv |                        max_ts |
+----+-------------+----------------------+----------------------+-------------------------------+
| +I |           1 |                    1 |                    1 | 2021-04-13 20:12:11.123456789 |
| +I |           2 |                    1 |                    1 | 2021-04-13 19:12:11.123456789 |
| -U |           2 |                    1 |                    1 | 2021-04-13 19:12:11.123456789 |
| +U |           2 |                    2 |                    1 | 2021-04-13 21:12:11.123456789 |
+----+-------------+----------------------+----------------------+-------------------------------+
Received a total of 4 rows
!ok

# ==========================================================================
# test TIMESTAMP and TIMESTAMP_LTZ type display with tableau result mode
# ==========================================================================

SET table.local-time-zone = Asia/Shanghai;
[INFO] Session property has been set.
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
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
| op |    time0 |                 ts0 |                     ts3 |                           ts9 |                 ts_ltz0 |                 ts_ltz3 |                       ts_ltz9 |
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
| +I | 20:12:11 | 2021-04-13 20:12:11 | 2021-04-13 20:12:11.123 | 2021-04-13 20:12:11.123456789 | 1970-01-01 08:00:01.000 | 1970-01-01 08:00:00.001 | 2021-04-13 20:12:11.123456789 |
| +I | 20:12:11 | 2021-04-13 21:12:11 | 2021-04-13 21:12:11.001 | 2021-04-13 21:12:11.100000000 | 1970-01-01 08:00:01.000 | 1970-01-01 08:00:00.001 | 2021-04-13 21:12:11.100000000 |
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
Received a total of 2 rows
!ok

SET table.local-time-zone = UTC;
[INFO] Session property has been set.
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
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
| op |    time0 |                 ts0 |                     ts3 |                           ts9 |                 ts_ltz0 |                 ts_ltz3 |                       ts_ltz9 |
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
| +I | 20:12:11 | 2021-04-13 20:12:11 | 2021-04-13 20:12:11.123 | 2021-04-13 20:12:11.123456789 | 1970-01-01 00:00:01.000 | 1970-01-01 00:00:00.001 | 2021-04-13 20:12:11.123456789 |
| +I | 20:12:11 | 2021-04-13 21:12:11 | 2021-04-13 21:12:11.001 | 2021-04-13 21:12:11.100000000 | 1970-01-01 00:00:01.000 | 1970-01-01 00:00:00.001 | 2021-04-13 21:12:11.100000000 |
+----+----------+---------------------+-------------------------+-------------------------------+-------------------------+-------------------------+-------------------------------+
Received a total of 2 rows
!ok


# ==========================================================================
# test batch query
# ==========================================================================

SET execution.runtime-mode = batch;
[INFO] Session property has been set.
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
