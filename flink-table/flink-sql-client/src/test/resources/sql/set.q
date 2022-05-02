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
[INFO] Session property has been set.
!info

SET 'table.sql-dialect' = 'hive';
[INFO] Session property has been set.
!info

create catalog hivecatalog with (
 'type' = 'hive-test',
 'hive-version' = '2.3.4'
);
[INFO] Execute statement succeed.
!info

use catalog hivecatalog;
[INFO] Execute statement succeed.
!info

# test create a hive table to verify the configuration works
CREATE TABLE hive_table (
  product_id STRING,
  product_name STRING,
  unit_price DECIMAL(10, 4),
  pv_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  update_time TIMESTAMP,
  update_user STRING
) PARTITIONED BY (pt_year STRING, pt_month STRING, pt_day STRING) TBLPROPERTIES (
  'streaming-source.enable' = 'true'
);
[INFO] Execute statement succeed.
!info

# test "ctas" only supported in Hive Dialect
CREATE TABLE foo as select 1;
+-------------------------+
| hivecatalog.default.foo |
+-------------------------+
|                      -1 |
+-------------------------+
1 row in set
!ok

# list the configured configuration
set;
'execution.attached' = 'true'
'execution.savepoint-restore-mode' = 'NO_CLAIM'
'execution.savepoint.ignore-unclaimed-state' = 'false'
'execution.shutdown-on-attached-exit' = 'false'
'execution.target' = 'remote'
'jobmanager.rpc.address' = '$VAR_JOBMANAGER_RPC_ADDRESS'
'pipeline.classpaths' = ''
'pipeline.jars' = ''
'rest.port' = '$VAR_REST_PORT'
'sql-client.execution.result-mode' = 'tableau'
'table.sql-dialect' = 'hive'
!ok

# reset the configuration
reset;
[INFO] All session properties have been set to their default values.
!info

set;
'execution.attached' = 'true'
'execution.savepoint-restore-mode' = 'NO_CLAIM'
'execution.savepoint.ignore-unclaimed-state' = 'false'
'execution.shutdown-on-attached-exit' = 'false'
'execution.target' = 'remote'
'jobmanager.rpc.address' = '$VAR_JOBMANAGER_RPC_ADDRESS'
'pipeline.classpaths' = ''
'pipeline.jars' = ''
'rest.port' = '$VAR_REST_PORT'
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

set;
'execution.attached' = 'true'
'execution.savepoint-restore-mode' = 'NO_CLAIM'
'execution.savepoint.ignore-unclaimed-state' = 'false'
'execution.shutdown-on-attached-exit' = 'false'
'execution.target' = 'remote'
'jobmanager.rpc.address' = '$VAR_JOBMANAGER_RPC_ADDRESS'
'pipeline.classpaths' = ''
'pipeline.jars' = ''
'rest.port' = '$VAR_REST_PORT'
!ok

set 'execution.attached' = 'false';
[INFO] Session property has been set.
!info

reset 'execution.attached';
[INFO] Session property has been reset.
!info

set;
'execution.attached' = 'true'
'execution.savepoint-restore-mode' = 'NO_CLAIM'
'execution.savepoint.ignore-unclaimed-state' = 'false'
'execution.shutdown-on-attached-exit' = 'false'
'execution.target' = 'remote'
'jobmanager.rpc.address' = '$VAR_JOBMANAGER_RPC_ADDRESS'
'pipeline.classpaths' = ''
'pipeline.jars' = ''
'rest.port' = '$VAR_REST_PORT'
!ok

# test reset can work with add jar
ADD JAR '$VAR_UDF_JAR_PATH';
[INFO] The specified jar is added into session classloader.
!info

SHOW JARS;
$VAR_UDF_JAR_PATH
!ok

set;
'execution.attached' = 'true'
'execution.savepoint-restore-mode' = 'NO_CLAIM'
'execution.savepoint.ignore-unclaimed-state' = 'false'
'execution.shutdown-on-attached-exit' = 'false'
'execution.target' = 'remote'
'jobmanager.rpc.address' = 'localhost'
'pipeline.classpaths' = ''
'pipeline.jars' = '$VAR_PIPELINE_JARS_URL'
'rest.port' = '$VAR_REST_PORT'
!ok

reset;
[INFO] All session properties have been set to their default values.
!info

SHOW JARS;
$VAR_UDF_JAR_PATH
!ok

SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Session property has been set.
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
[INFO] The specified jar is removed from session classloader.
!info

SHOW JARS;
Empty set
!ok
