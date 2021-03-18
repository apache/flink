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
[INFO] Table has been created.
!info

SELECT UPPER(str), id FROM src;
[ERROR] Could not execute SQL statement. Reason:
java.lang.IllegalArgumentException: testing elements of values source shouldn't be empty.
!error

# ==========================================================================
# test stream query with tableau result mode
# (we can't test changelog mode and table mode in IT case)
# ==========================================================================

SELECT id, COUNT(*) as cnt, COUNT(DISTINCT str) as uv
FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str)
GROUP BY id;
+----+-------------+----------------------+----------------------+
| op |          id |                  cnt |                   uv |
+----+-------------+----------------------+----------------------+
| +I |           1 |                    1 |                    1 |
| +I |           2 |                    1 |                    1 |
| -U |           2 |                    1 |                    1 |
| +U |           2 |                    2 |                    1 |
+----+-------------+----------------------+----------------------+
Received a total of 4 rows
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
+-------------+----------------------+----------------------+
|          id |                  cnt |                   uv |
+-------------+----------------------+----------------------+
|           1 |                    1 |                    1 |
|           2 |                    2 |                    1 |
+-------------+----------------------+----------------------+
Received a total of 2 rows
!ok
