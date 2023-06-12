# set.q - SET, RESET
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

reset table.resources.download-dir;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

set;
!output
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | localhost |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
+--------------------------------------------+-----------+
9 rows in set
!ok

# set illegal value
set 'table.sql-dialect' = 'unknown';
!output
java.lang.IllegalArgumentException: No enum constant org.apache.flink.table.api.SqlDialect.UNKNOWN
!error

set;
!output
+--------------------------------------------+-----------+
|                                        key |     value |
+--------------------------------------------+-----------+
|                         execution.attached |      true |
|           execution.savepoint-restore-mode |  NO_CLAIM |
| execution.savepoint.ignore-unclaimed-state |     false |
|        execution.shutdown-on-attached-exit |     false |
|                           execution.target |    remote |
|                     jobmanager.rpc.address | localhost |
|                        pipeline.classpaths |           |
|                              pipeline.jars |           |
|                                  rest.port |     $VAR_REST_PORT |
+--------------------------------------------+-----------+
9 rows in set
!ok
