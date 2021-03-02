# table.q - Create/Drop/Show Table
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

# regular table registration
create table tbl(a int, b as proctime());
[INFO] Table has been created.
!info

show tables;
tbl
!ok

create table tbl(a int, b as invalid_function());
[ERROR] Could not execute SQL statement. Reason:
org.apache.calcite.sql.validate.SqlValidatorException: No match found for function signature invalid_function()
!error

SET execution.result-mode=tableau;
[INFO] Session property has been set.
!info


SELECT * FROM (VALUES (1, '2', TIMESTAMP '2020-10-10 09:20:22')) as T(a, b, c);
+-------------+----------------------+-----------------------+
|           a |                    b |                     c |
+-------------+----------------------+-----------------------+
|           1 |                    2 |   2020-10-10T09:20:22 |
+-------------+----------------------+-----------------------+
Received a total of 1 row
!ok
