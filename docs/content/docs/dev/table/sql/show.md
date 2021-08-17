---
title: "SHOW Statements"
weight: 11
type: docs
aliases:
  - /dev/table/sql/show.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SHOW Statements

SHOW statements are used to list all catalogs, or list all databases in the current catalog, or list all tables/views in the current catalog and the current database, or show current catalog and database, or show create statement for specified table, or list all functions including system functions and user-defined functions in the current catalog and current database, or list only user-defined functions in the current catalog and current database, or list enabled module names, or list all loaded modules with enabled status in the current session.

Flink SQL supports the following SHOW statements for now:
- SHOW CATALOGS
- SHOW CURRENT CATALOG
- SHOW DATABASES
- SHOW CURRENT DATABASE
- SHOW TABLES
- SHOW CREATE TABLE
- SHOW VIEWS
- SHOW FUNCTIONS
- SHOW MODULES
- SHOW JARS

## Run a SHOW statement

{{< tabs "show" >}}
{{< tab "Java" >}}

SHOW statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns objects for a successful SHOW operation, otherwise will throw an exception.

The following examples show how to run a SHOW statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}

SHOW statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns objects for a successful SHOW operation, otherwise will throw an exception.

The following examples show how to run a SHOW statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Python" >}}

SHOW statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns objects for a successful SHOW operation, otherwise will throw an exception.

The following examples show how to run a SHOW statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

SHOW statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a SHOW statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "37a50b24-61f5-46a3-a382-db3fbb613358" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// show catalogs
tEnv.executeSql("SHOW CATALOGS").print();
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// +-----------------+

// show current catalog
tEnv.executeSql("SHOW CURRENT CATALOG").print();
// +----------------------+
// | current catalog name |
// +----------------------+
// |      default_catalog |
// +----------------------+

// show databases
tEnv.executeSql("SHOW DATABASES").print();
// +------------------+
// |    database name |
// +------------------+
// | default_database |
// +------------------+

// show current database
tEnv.executeSql("SHOW CURRENT DATABASE").print();
// +-----------------------+
// | current database name |
// +-----------------------+
// |      default_database |
// +-----------------------+

// create a table
tEnv.executeSql("CREATE TABLE my_table (...) WITH (...)");
// show tables
tEnv.executeSql("SHOW TABLES").print();
// +------------+
// | table name |
// +------------+
// |   my_table |
// +------------+

// show create table
tEnv.executeSql("SHOW CREATE TABLE my_table").print();
// CREATE TABLE `default_catalog`.`default_db`.`my_table` (
//   ...
// ) WITH (
//   ...
// )


// create a view
tEnv.executeSql("CREATE VIEW my_view AS ...");
// show views
tEnv.executeSql("SHOW VIEWS").print();
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show functions
tEnv.executeSql("SHOW FUNCTIONS").print();
// +---------------+
// | function name |
// +---------------+
// |           mod |
// |        sha256 |
// |           ... |
// +---------------+

// create a user defined function
tEnv.executeSql("CREATE FUNCTION f1 AS ...");
// show user defined functions
tEnv.executeSql("SHOW USER FUNCTIONS").print();
// +---------------+
// | function name |
// +---------------+
// |            f1 |
// |           ... |
// +---------------+

// show modules
tEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+

// show full modules
tEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        core |  true |
// |        hive | false |
// +-------------+-------+

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// show catalogs
tEnv.executeSql("SHOW CATALOGS").print()
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// +-----------------+

// show databases
tEnv.executeSql("SHOW DATABASES").print()
// +------------------+
// |    database name |
// +------------------+
// | default_database |
// +------------------+

// create a table
tEnv.executeSql("CREATE TABLE my_table (...) WITH (...)")
// show tables
tEnv.executeSql("SHOW TABLES").print()
// +------------+
// | table name |
// +------------+
// |   my_table |
// +------------+

// show create table
tEnv.executeSql("SHOW CREATE TABLE my_table").print()
// CREATE TABLE `default_catalog`.`default_db`.`my_table` (
//  ...
// ) WITH (
//  ...
// )
// create a view
tEnv.executeSql("CREATE VIEW my_view AS ...")
// show views
tEnv.executeSql("SHOW VIEWS").print()
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show functions
tEnv.executeSql("SHOW FUNCTIONS").print()
// +---------------+
// | function name |
// +---------------+
// |           mod |
// |        sha256 |
// |           ... |
// +---------------+

// create a user defined function
tEnv.executeSql("CREATE FUNCTION f1 AS ...")
// show user defined functions
tEnv.executeSql("SHOW USER FUNCTIONS").print()
// +---------------+
// | function name |
// +---------------+
// |            f1 |
// |           ... |
// +---------------+

// show modules
tEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        core |
// +-------------+

// show full modules
tEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        core |  true |
// |        hive | false |
// +-------------+-------+

```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = StreamTableEnvironment.create(...)

# show catalogs
table_env.execute_sql("SHOW CATALOGS").print()
# +-----------------+
# |    catalog name |
# +-----------------+
# | default_catalog |
# +-----------------+

# show databases
table_env.execute_sql("SHOW DATABASES").print()
# +------------------+
# |    database name |
# +------------------+
# | default_database |
# +------------------+

# create a table
table_env.execute_sql("CREATE TABLE my_table (...) WITH (...)")
# show tables
table_env.execute_sql("SHOW TABLES").print()
# +------------+
# | table name |
# +------------+
# |   my_table |
# +------------+
# show create table
table_env.executeSql("SHOW CREATE TABLE my_table").print()
# CREATE TABLE `default_catalog`.`default_db`.`my_table` (
#   ...
# ) WITH (
#   ...
# )

# create a view
table_env.execute_sql("CREATE VIEW my_view AS ...")
# show views
table_env.execute_sql("SHOW VIEWS").print()
# +-----------+
# | view name |
# +-----------+
# |   my_view |
# +-----------+

# show functions
table_env.execute_sql("SHOW FUNCTIONS").print()
# +---------------+
# | function name |
# +---------------+
# |           mod |
# |        sha256 |
# |           ... |
# +---------------+

# create a user defined function
table_env.execute_sql("CREATE FUNCTION f1 AS ...")
# show user defined functions
table_env.execute_sql("SHOW USER FUNCTIONS").print()
# +---------------+
# | function name |
# +---------------+
# |            f1 |
# |           ... |
# +---------------+

# show modules
table_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        core |
# +-------------+

# show full modules
table_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        core |  true |
# |        hive | false |
# +-------------+-------+
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql

Flink SQL> SHOW CATALOGS;
default_catalog

Flink SQL> SHOW DATABASES;
default_database

Flink SQL> CREATE TABLE my_table (...) WITH (...);
[INFO] Table has been created.

Flink SQL> SHOW TABLES;
my_table

Flink SQL> SHOW CREATE TABLE my_table;
CREATE TABLE `default_catalog`.`default_db`.`my_table` (
  ...
) WITH (
  ...
)

Flink SQL> CREATE VIEW my_view AS ...;
[INFO] View has been created.

Flink SQL> SHOW VIEWS;
my_view

Flink SQL> SHOW FUNCTIONS;
mod
sha256
...

Flink SQL> CREATE FUNCTION f1 AS ...;
[INFO] Function has been created.

Flink SQL> SHOW USER FUNCTIONS;
f1
...

Flink SQL> SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
+-------------+
1 row in set


Flink SQL> SHOW FULL MODULES;
+-------------+------+
| module name | used |
+-------------+------+
|        core | true |
+-------------+------+
1 row in set

Flink SQL> SHOW JARS;
/path/to/addedJar.jar

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## SHOW CATALOGS

```sql
SHOW CATALOGS
```

Show all catalogs.

## SHOW CURRENT CATALOG

```sql
SHOW CURRENT CATALOG
```

Show current catalog.

## SHOW DATABASES

```sql
SHOW DATABASES
```

Show all databases in the current catalog.

## SHOW CURRENT DATABASE

```sql
SHOW CURRENT DATABASE
```

Show current database.

## SHOW TABLES

```sql
SHOW TABLES
```

Show all tables in the current catalog and the current database.


## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE
```

Show create table statement for specified table.

<span class="label label-danger">Attention</span> Currently `SHOW CREATE TABLE` only supports table that is created by Flink SQL DDL.

## SHOW VIEWS

```sql
SHOW VIEWS
```

Show all views in the current catalog and the current database.

## SHOW FUNCTIONS

```sql
SHOW [USER] FUNCTIONS
```

Show all functions including system functions and user-defined functions in the current catalog and current database.

**USER**
Show only user-defined functions in the current catalog and current database.

## SHOW MODULES

```sql
SHOW [FULL] MODULES
```

Show all enabled module names with resolution order.

**FULL**
Show all loaded modules and enabled status with resolution order.

## SHOW JARS

```sql
SHOW JARS
```

Show all added jars in the session classloader which are added by [`ADD JAR`]({{< ref "docs/dev/table/sql/jar" >}}#add-jar) statements.

<span class="label label-danger">Attention</span> Currently `SHOW JARS` only works in the [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

{{< top >}}
