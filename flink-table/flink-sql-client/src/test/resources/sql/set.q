# set.q - SET/RESET configuration
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

# test set a configuration
SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
!info

SET 'table.dml-sync' = 'true';
[INFO] Execute statement succeed.
!info

# test "ctas"
CREATE TABLE foo with(
  'connector' = 'filesystem',
  'path' = '$VAR_STREAMING_PATH',
  'format' = 'csv'
) as select id FROM (VALUES (1)) T(id);
[INFO] Complete execution of the SQL update statement.
!info

RESET 'table.dml-sync';
[INFO] Execute statement succeed.
!info

SELECT * from foo;
+----+-------------+
| op |          id |
+----+-------------+
| +I |           1 |
+----+-------------+
Received a total of 1 row
!ok

# test add jar
ADD JAR '$VAR_UDF_JAR_PATH';
[INFO] Execute statement succeed.
!info

SHOW JARS;
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH_SPACEjars |
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH |
+-$VAR_UDF_JAR_PATH_DASH-----+
1 row in set
!ok

REMOVE JAR '$VAR_UDF_JAR_PATH';
[INFO] Execute statement succeed.
!info

SHOW JARS;
Empty set
!ok

reset 'table.resources.download-dir';
[INFO] Execute statement succeed.
!info

# list the configured configuration
set;
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | $VAR_JOBMANAGER_RPC_ADDRESS |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
|           sql-client.execution.result-mode |   tableau |
|           table.exec.legacy-cast-behaviour |  DISABLED |
+--------------------------------------------+-----------+
11 rows in set
!ok

# reset the configuration
reset;
[INFO] Execute statement succeed.
!info

set;
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | $VAR_JOBMANAGER_RPC_ADDRESS |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
+--------------------------------------------+-----------+
9 rows in set
!ok

# should fail because default dialect doesn't support hive dialect
CREATE TABLE hive_table2 (
  product_id STRING,
  product_name STRING,
  unit_price DECIMAL(10, 4),
  pv_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  update_time TIMESTAMP(3),
  update_user STRING
) PARTITIONED BY (pt_year STRING, pt_month STRING, pt_day STRING) TBLPROPERTIES (
  'streaming-source.enable' = 'true'
);
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Encountered "STRING" at line 10, column 27.
Was expecting one of:
    ")" ...
    "," ...
!error

set 'sql-client.verbose' = 'true';
[INFO] Execute statement succeed.
!info

set;
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | $VAR_JOBMANAGER_RPC_ADDRESS |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
|                         sql-client.verbose |      true |
+--------------------------------------------+-----------+
10 rows in set
!ok

set 'execution.attached' = 'false';
[INFO] Execute statement succeed.
!info

reset 'execution.attached';
[INFO] Execute statement succeed.
!info

set;
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | $VAR_JOBMANAGER_RPC_ADDRESS |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
|                         sql-client.verbose |      true |
+--------------------------------------------+-----------+
10 rows in set
!ok

# test reset can work with add jar
ADD JAR '$VAR_UDF_JAR_PATH';
[INFO] Execute statement succeed.
!info

SHOW JARS;
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH_SPACEjars |
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH |
+-$VAR_UDF_JAR_PATH_DASH-----+
1 row in set
!ok

set;
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | $VAR_JOBMANAGER_RPC_ADDRESS |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
|                         sql-client.verbose |      true |
+--------------------------------------------+-----------+
10 rows in set
!ok

reset;
[INFO] Execute statement succeed.
!info

SHOW JARS;
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH_SPACEjars |
+-$VAR_UDF_JAR_PATH_DASH-----+
| $VAR_UDF_JAR_PATH |
+-$VAR_UDF_JAR_PATH_DASH-----+
1 row in set
!ok

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
!info

create function func1 as 'LowerUDF' LANGUAGE JAVA;
[INFO] Execute statement succeed.
!info

SELECT id, func1(str) FROM (VALUES (1, 'Hello World')) AS T(id, str) ;
+----+-------------+--------------------------------+
| op |          id |                         EXPR$1 |
+----+-------------+--------------------------------+
| +I |           1 |                    hello world |
+----+-------------+--------------------------------+
Received a total of 1 row
!ok

REMOVE JAR '$VAR_UDF_JAR_PATH';
[INFO] Execute statement succeed.
!info

SHOW JARS;
Empty set
!ok
