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

# validation test
set execution.parallelism = 10a;
[ERROR] Could not execute SQL statement. Reason:
java.lang.NumberFormatException: For input string: "10a"
!error

# test set the removed key
SET execution.max-idle-state-retention=1000;
[WARNING] The specified key is not supported anymore.
!warning

# test set the deprecated key
SET execution.planner=blink;
[WARNING] The specified key 'execution.planner' is deprecated. Please use 'table.planner' instead.
[INFO] Session property has been set.
!warning

# test set a configuration
SET table.sql-dialect=hive;
[INFO] Session property has been set.
!info

# test create a hive table to verify the configuration works
CREATE TABLE hive_table (
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
[INFO] Table has been created.
!info

# list the configured configuration
set;
execution.attached=true
execution.savepoint.ignore-unclaimed-state=false
execution.shutdown-on-attached-exit=false
execution.target=remote
jobmanager.rpc.address=$VAR_JOBMANAGER_RPC_ADDRESS
pipeline.classpaths=
pipeline.jars=$VAR_PIPELINE_JARS
rest.port=$VAR_REST_PORT
table.planner=blink
table.sql-dialect=hive
[DEPRECATED] execution.planner=blink
!ok

# reset the configuration
reset;
[INFO] All session properties have been set to their default values.
!info

set;
execution.attached=true
execution.savepoint.ignore-unclaimed-state=false
execution.shutdown-on-attached-exit=false
execution.target=remote
jobmanager.rpc.address=$VAR_JOBMANAGER_RPC_ADDRESS
pipeline.classpaths=
pipeline.jars=$VAR_PIPELINE_JARS
rest.port=$VAR_REST_PORT
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
