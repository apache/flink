# hive_module.q - LOAD/UNLOAD MODULE, USE/LIST/LIST FULL MODULES
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

# ==========================================================================
# test load module
# ==========================================================================

# set to default dialect to execute the statements supported only in Flink default dialect
SET table.sql-dialect=default;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# list default loaded and enabled module
SHOW MODULES;
!output
+-------------+
| module name |
+-------------+
|        hive |
|        core |
+-------------+
2 rows in set
!ok

SHOW FULL MODULES;
!output
+-------------+------+
| module name | used |
+-------------+------+
|        hive | TRUE |
|        core | TRUE |
+-------------+------+
2 rows in set
!ok

# load core module twice
LOAD MODULE core;
!output
org.apache.flink.table.api.ValidationException: A module with name 'core' already exists
!error

# use hive built-in function without loading hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World'));
!output
+------------+
|     EXPR$0 |
+------------+
| www.apache |
+------------+
1 row in set
!ok

# ==========================================================================
# test use built-in native agg function of hive module
# ==========================================================================

# set to hive dialect to execute the statements supported in Hive dialect
SET table.sql-dialect = hive;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

CREATE TABLE source (
    a INT
);
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

EXPLAIN SELECT SUM(a) FROM source;
!output
== Abstract Syntax Tree ==
LogicalProject(_o__c0=[$0])
+- LogicalAggregate(group=[{}], agg#0=[sum($0)])
   +- LogicalProject($f0=[$0])
      +- LogicalTableScan(table=[[hive, default, source]])

== Optimized Physical Plan ==
SortAggregate(isMerge=[false], select=[sum(a) AS $f0])
+- Exchange(distribution=[single])
   +- TableSourceScan(table=[[hive, default, source]], fields=[a])

== Optimized Execution Plan ==
SortAggregate(isMerge=[false], select=[sum(a) AS $f0])
+- Exchange(distribution=[single])
   +- TableSourceScan(table=[[hive, default, source]], fields=[a])
!ok

# enable hive native agg function that use hash-agg strategy
SET table.exec.hive.native-agg-function.enabled = true;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

EXPLAIN SELECT SUM(a) FROM source;
!output
== Abstract Syntax Tree ==
LogicalProject(_o__c0=[$0])
+- LogicalAggregate(group=[{}], agg#0=[sum($0)])
   +- LogicalProject($f0=[$0])
      +- LogicalTableScan(table=[[hive, default, source]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[false], select=[sum(a) AS $f0])
+- Exchange(distribution=[single])
   +- TableSourceScan(table=[[hive, default, source]], fields=[a])

== Optimized Execution Plan ==
HashAggregate(isMerge=[false], select=[sum(a) AS $f0])
+- Exchange(distribution=[single])
   +- TableSourceScan(table=[[hive, default, source]], fields=[a])
!ok

# load hive module with module name as string literal
LOAD MODULE 'hive';
!output
org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseException: line 1:5 mismatched input 'MODULE' expecting DATA near 'LOAD' in load statement
!error

# set to default dialect to execute the statements supported in Flink default dialect
SET table.sql-dialect = default;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

# load hive module with module name capitalized
LOAD MODULE Hive;
!output
org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'Hive' that implements 'org.apache.flink.table.factories.ModuleFactory' in the classpath.

Available factory identifiers are:

core
dummy
hive
!error

# load hive module with specifying type
LOAD MODULE myhive WITH ('type' = 'hive');
!output
org.apache.flink.table.api.ValidationException: Option 'type' = 'hive' is not supported since module name is used to find module
!error

LOAD MODULE hive;
!output
org.apache.flink.table.api.ValidationException: A module with name 'hive' already exists
!error

# show enabled modules
SHOW MODULES;
!output
+-------------+
| module name |
+-------------+
|        hive |
|        core |
+-------------+
2 rows in set
!ok

# show all loaded modules
SHOW FULL MODULES;
!output
+-------------+------+
| module name | used |
+-------------+------+
|        hive | TRUE |
|        core | TRUE |
+-------------+------+
2 rows in set
!ok

# use hive built-in function after loading hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World'));
!output
+------------+
|     EXPR$0 |
+------------+
| www.apache |
+------------+
1 row in set
!ok

# ==========================================================================
# test use modules
# ==========================================================================

# use duplicate modules
USE MODULES hive, core, hive;
!output
org.apache.flink.table.api.ValidationException: Module 'hive' appears more than once
!error

# change module resolution order
USE MODULES hive, core;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SHOW MODULES;
!output
+-------------+
| module name |
+-------------+
|        hive |
|        core |
+-------------+
2 rows in set
!ok

SHOW FULL MODULES;
!output
+-------------+------+
| module name | used |
+-------------+------+
|        hive | TRUE |
|        core | TRUE |
+-------------+------+
2 rows in set
!ok

# disable hive module
USE MODULES core;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SHOW MODULES;
!output
+-------------+
| module name |
+-------------+
|        core |
+-------------+
1 row in set
!ok

SHOW FULL MODULES;
!output
+-------------+-------+
| module name |  used |
+-------------+-------+
|        core |  TRUE |
|        hive | FALSE |
+-------------+-------+
2 rows in set
!ok

# use hive built-in function without using hive module
SELECT SUBSTRING_INDEX('www.apache.org', '.', 2) FROM (VALUES (1, 'Hello World'));
!output
org.apache.calcite.sql.validate.SqlValidatorException: No match found for function signature SUBSTRING_INDEX(<CHARACTER>, <CHARACTER>, <NUMERIC>)
!error

# ==========================================================================
# test unload module
# ==========================================================================

UNLOAD MODULE core;
!output
+--------+
| result |
+--------+
|     OK |
+--------+
1 row in set
!ok

SHOW MODULES;
!output
Empty set
!ok

SHOW FULL MODULES;
!output
+-------------+-------+
| module name |  used |
+-------------+-------+
|        hive | FALSE |
+-------------+-------+
1 row in set
!ok

# unload core module twice
UNLOAD MODULE core;
!output
org.apache.flink.table.api.ValidationException: No module with name 'core' exists
!error
