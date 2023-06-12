# delete.q - test delete statement
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

# first set batch mode
SET 'execution.runtime-mode' = 'batch';
[INFO] Execute statement succeed.
!info

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
!info

SET 'table.dml-sync' = 'true';
[INFO] Execute statement succeed.
!info

# create a table first
CREATE TABLE t (a int PRIMARY KEY NOT ENFORCED, b string, c double)
WITH (
  'connector' = 'test-update-delete',
  'data-id' = '$VAR_DELETE_TABLE_DATA_ID',
  'mix-delete' = 'true'
);
[INFO] Execute statement succeed.
!info

# query the table first
SELECT * FROM t;
+---+-----+-----+
| a |   b |   c |
+---+-----+-----+
| 0 | b_0 | 0.0 |
| 1 | b_1 | 2.0 |
| 2 | b_2 | 4.0 |
| 3 | b_3 | 6.0 |
| 4 | b_4 | 8.0 |
+---+-----+-----+
5 rows in set
!ok

# delete the table with condition containing subquery which can't be push down, so that it'll submit a job;
DELETE FROM t WHERE a >= (SELECT COUNT(1) FROM t WHERE c > 2);
[INFO] Complete execution of the SQL update statement.
!info

# query the table
SELECT * FROM t;
+---+-----+-----+
| a |   b |   c |
+---+-----+-----+
| 0 | b_0 | 0.0 |
| 1 | b_1 | 2.0 |
| 2 | b_2 | 4.0 |
+---+-----+-----+
3 rows in set
!ok

# delete the table with filter push down
DELETE FROM t;
+---------------+
| rows affected |
+---------------+
|             3 |
+---------------+
1 row in set
!ok
