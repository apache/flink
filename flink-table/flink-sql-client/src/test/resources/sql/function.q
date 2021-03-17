# function.q - CREATE/DROP/ALTER FUNCTION
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

# this also tests user classloader because the LowerUDF is in user jar
create function func1 as 'LowerUDF' LANGUAGE JAVA;
[INFO] Function has been created.
!info

show user functions;
func1
!ok

SET execution.result-mode=tableau;
[INFO] Session property has been set.
!info

# run a query to verify the registered UDF works
SELECT id, func1(str) FROM (VALUES (1, 'Hello World'), (2, 'Hi')) as T(id, str);
+----+-------------+----------------------+
| op |          id |               EXPR$1 |
+----+-------------+----------------------+
| +I |           1 |          hello world |
| +I |           2 |                   hi |
+----+-------------+----------------------+
Received a total of 2 rows
!ok

# ====== test temporary function ======

create temporary function if not exists func2 as 'LowerUDF' LANGUAGE JAVA;
[INFO] Function has been created.
!info

show user functions;
func1
func2
!ok

# ====== test function with full qualified name ======

create catalog c1 with ('type'='generic_in_memory');
[INFO] Catalog has been created.
!info

use catalog c1;
[INFO] Catalog changed.
!info

create database db;
[INFO] Database has been created.
!info

use catalog default_catalog;
[INFO] Catalog changed.
!info

create function c1.db.func3 as 'LowerUDF' LANGUAGE JAVA;
[INFO] Function has been created.
!info

create temporary function if not exists c1.db.func4 as 'LowerUDF' LANGUAGE JAVA;
[INFO] Function has been created.
!info

# no func3 and func4 because we are not under catalog c1
show user functions;
func1
func2
!ok

use catalog c1;
[INFO] Catalog changed.
!info

use db;
[INFO] Database changed.
!info

# should show func3 and func4 now
show user functions;
func3
func4
!ok

# test create function with database name
create function `default`.func5 as 'LowerUDF';
[INFO] Function has been created.
!info

create function `default`.func6 as 'LowerUDF';
[INFO] Function has been created.
!info

use `default`;
[INFO] Database changed.
!info

# should show func5 and func6
show user functions;
func5
func6
!ok

# ==========================================================================
# test drop function
# ==========================================================================

create function c1.db.func10 as 'LowerUDF';
[INFO] Function has been created.
!info

create function c1.db.func11 as 'LowerUDF';
[INFO] Function has been created.
!info

drop function if exists c1.db.func10;
[INFO] Function has been removed.
!info

use catalog c1;
[INFO] Catalog changed.
!info

use db;
[INFO] Database changed.
!info

drop function if exists non_func;
[INFO] Function has been removed.
!info

# should contain func11, not contain func10
show user functions;
func11
func3
func4
!ok

# ==========================================================================
# test alter function
# ==========================================================================

alter function func11 as 'org.apache.flink.table.client.gateway.local.LocalExecutorITCase$TestScalaFunction';
[INFO] Alter function succeeded!
!info

# TODO: show func11 when we support DESCRIBE FUNCTION

create temporary function tmp_func as 'LowerUDF';
[INFO] Function has been created.
!info

# should throw unsupported error
alter temporary function tmp_func as 'org.apache.flink.table.client.gateway.local.LocalExecutorITCase$TestScalaFunction';
[ERROR] Could not execute SQL statement. Alter function failed! Reason:
org.apache.flink.table.api.ValidationException: Alter temporary catalog function is not supported
!error


# ==========================================================================
# test function with hive catalog
# ==========================================================================

create catalog hivecatalog with ('type'='hive', 'hive-version'='2.3.4','test'='test');
[INFO] Catalog has been created.
!info

use catalog hivecatalog;
[INFO] Catalog changed.
!info

create function lowerudf AS 'LowerUDF';
[INFO] Function has been created.
!info

show user functions;
lowerudf
!ok
