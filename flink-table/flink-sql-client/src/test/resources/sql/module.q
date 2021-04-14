# module.q - LOAD/UNLOAD MODULE, USE/LIST/LIST FULL MODULES
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

# set tableau result mode
SET sql-client.execution.result-mode = tableau;
[INFO] Session property has been set.
!info

# ==========================================================================
# test load module
# ==========================================================================

# list default loaded and enabled module
SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
+-------------+
1 row in set
!ok

SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | true |
+-------------+------+
1 row in set
!ok

# load core module twice
LOAD MODULE core;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: A module with name 'core' already exists
!error

# use hive built-in function without loading hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World')) AS T(id, str);
[ERROR] Could not execute SQL statement. Reason:
org.apache.calcite.sql.validate.SqlValidatorException: No match found for function signature SUBSTRING_INDEX(<CHARACTER>, <CHARACTER>, <NUMERIC>)
!error

# load hive module with module name as string literal
LOAD MODULE 'hive';
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Encountered "\'hive\'" at line 1, column 13.
Was expecting one of:
    <BRACKET_QUOTED_IDENTIFIER> ...
    <QUOTED_IDENTIFIER> ...
    <BACK_QUOTED_IDENTIFIER> ...
    <HYPHENATED_IDENTIFIER> ...
    <IDENTIFIER> ...
    <UNICODE_QUOTED_IDENTIFIER> ...

!error

# load hive module with module name capitalized
LOAD MODULE Hive;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.factories.ModuleFactory' in
the classpath.

Reason: Required context properties mismatch.

The following properties are requested:
type=Hive

The following factories have been considered:
org.apache.flink.table.client.gateway.local.DependencyTest$TestModuleFactory
org.apache.flink.table.module.CoreModuleFactory
org.apache.flink.table.module.hive.HiveModuleFactory
!error

# load hive module with specifying type
LOAD MODULE myhive WITH ('type' = 'hive');
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Property 'type' = 'hive' is not supported since module name is used to find module
!error

LOAD MODULE hive;
[INFO] Execute statement succeed.
!info

# show enabled modules
SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
|        hive |
+-------------+
2 rows in set
!ok

# show all loaded modules
SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | true |
|        hive | true |
+-------------+------+
2 rows in set
!ok

# use hive built-in function after loading hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World')) AS T(id, str);
+----+--------------------------------+
| op |                         EXPR$0 |
+----+--------------------------------+
| +I |                     www.apache |
+----+--------------------------------+
Received a total of 1 row
!ok

# ==========================================================================
# test use modules
# ==========================================================================

# use duplicate modules
USE MODULES hive, core, hive;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Module 'hive' appears more than once
!error

# change module resolution order
USE MODULES hive, core;
[INFO] Execute statement succeed.
!info

SHOW MODULES;
+-------------+
| module name |
+-------------+
|        hive |
|        core |
+-------------+
2 rows in set
!ok

SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        hive | true |
|        core | true |
+-------------+------+
2 rows in set
!ok

# disable hive module
USE MODULES core;
[INFO] Execute statement succeed.
!info

SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
+-------------+
1 row in set
!ok

SHOW FULL MODULES;
+-------------+-------+
| module name |  used |
+-------------+-------+
|        core |  true |
|        hive | false |
+-------------+-------+
2 rows in set
!ok

# use hive built-in function without using hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World')) AS T(id, str);
[ERROR] Could not execute SQL statement. Reason:
org.apache.calcite.sql.validate.SqlValidatorException: No match found for function signature SUBSTRING_INDEX(<CHARACTER>, <CHARACTER>, <NUMERIC>)
!error

# ==========================================================================
# test unload module
# ==========================================================================

UNLOAD MODULE core;
[INFO] Execute statement succeed.
!info

SHOW MODULES;
Empty set
!ok

SHOW FULL MODULES;
+-------------+-------+
| module name |  used |
+-------------+-------+
|        hive | false |
+-------------+-------+
1 row in set
!ok

# unload core module twice
UNLOAD MODULE core;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: No module with name 'core' exists
!error
