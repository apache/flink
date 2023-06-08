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
SET 'sql-client.execution.result-mode' = 'tableau';
[INFO] Execute statement succeed.
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
|        core | TRUE |
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

# load dummy module with module name as string literal
LOAD MODULE 'dummy';
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.sql.parser.impl.ParseException: Encountered "\'dummy\'" at line 1, column 13.
Was expecting one of:
    <BRACKET_QUOTED_IDENTIFIER> ...
    <QUOTED_IDENTIFIER> ...
    <BACK_QUOTED_IDENTIFIER> ...
    <BIG_QUERY_BACK_QUOTED_IDENTIFIER> ...
    <HYPHENATED_IDENTIFIER> ...
    <IDENTIFIER> ...
    <UNICODE_QUOTED_IDENTIFIER> ...
!error

# load dummy module with module name capitalized
LOAD MODULE Dummy;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'Dummy' that implements 'org.apache.flink.table.factories.ModuleFactory' in the classpath.

Available factory identifiers are:

core
dummy
!error

# load dummy module with specifying type
LOAD MODULE mydummy WITH ('type' = 'dummy');
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Option 'type' = 'dummy' is not supported since module name is used to find module
!error

LOAD MODULE dummy;
[INFO] Execute statement succeed.
!info

# show enabled modules
SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
|       dummy |
+-------------+
2 rows in set
!ok

# show all loaded modules
SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | TRUE |
|       dummy | TRUE |
+-------------+------+
2 rows in set
!ok

# ==========================================================================
# test use modules
# ==========================================================================

# use duplicate modules
USE MODULES dummy, core, dummy;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: Module 'dummy' appears more than once
!error

# change module resolution order
USE MODULES dummy, core;
[INFO] Execute statement succeed.
!info

SHOW MODULES;
+-------------+
| module name |
+-------------+
|       dummy |
|        core |
+-------------+
2 rows in set
!ok

SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|       dummy | TRUE |
|        core | TRUE |
+-------------+------+
2 rows in set
!ok

# disable dummy module
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
|        core |  TRUE |
|       dummy | FALSE |
+-------------+-------+
2 rows in set
!ok

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
|       dummy | FALSE |
+-------------+-------+
1 row in set
!ok

# unload core module twice
UNLOAD MODULE core;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: No module with name 'core' exists
!error
