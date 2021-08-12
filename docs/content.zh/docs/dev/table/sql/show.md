---
title: "SHOW 语句"
weight: 11
type: docs
aliases:
  - /zh/dev/table/sql/show.html
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

# SHOW 语句



SHOW 语句用于列出所有的 catalog，或者列出当前 catalog 中所有的 database，或者列出当前 catalog 和当前 database 的所有表或视图，或者列出当前正在使用的 catalog 和 database, 或者列出创建指定表的语句，或者列出当前 catalog 和当前 database 中所有的 function，包括：系统 function 和用户定义的 function，或者仅仅列出当前 catalog 和当前 database 中用户定义的 function，或者列出当前环境所有激活的 module，或者列出当前环境所有加载的 module 及激活状态。

目前 Flink SQL 支持下列 SHOW 语句：
- SHOW CATALOGS
- SHOW CURRENT CATALOG
- SHOW DATABASES
- SHOW CURRENT DATABASE
- SHOW TABLES
- SHOW CREATE TABLE
- SHOW VIEWS
- SHOW FUNCTIONS
- SHOW MODULES
- SHOW FULL MODULES
- SHOW JARS


## 执行 SHOW 语句

{{< tabs "execute" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 SHOW 语句。 若 SHOW 操作执行成功，`executeSql()` 方法返回所有对象，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 SHOW 语句。

{{< /tab >}}
{{< tab "Scala" >}}
可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 SHOW 语句。 若 SHOW 操作执行成功，`executeSql()` 方法返回所有对象，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 SHOW 语句。
{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 SHOW 语句。 若 SHOW 操作执行成功，`execute_sql()` 方法返回所有对象，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 SHOW 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 SHOW 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 SHOW 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "bc804bed-4550-4f60-a8c0-17e6d741e08d" >}}
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

展示所有的 catalog。

## SHOW CURRENT CATALOG

```sql
SHOW CURRENT CATALOG
```

显示当前正在使用的 catalog。

## SHOW DATABASES

```sql
SHOW DATABASES
```

展示当前 catalog 中所有的 database。

## SHOW CURRENT DATABASE

```sql
SHOW CURRENT DATABASE
```

显示当前正在使用的 database。

## SHOW TABLES

```sql
SHOW TABLES
```

展示当前 catalog 和当前 database 中所有的表。

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [catalog_name.][db_name.]table_name
```

展示创建指定表的 create 语句。

<span class="label label-danger">Attention</span> 目前 `SHOW CREATE TABLE` 只支持通过 Flink SQL DDL 创建的表。

## SHOW VIEWS

```sql
SHOW VIEWS
```

展示当前 catalog 和当前 database 中所有的视图。

## SHOW FUNCTIONS

```sql
SHOW [USER] FUNCTIONS
```

展示当前 catalog 和当前 database 中所有的 function，包括：系统 function 和用户定义的 function。

**USER**
仅仅展示当前 catalog 和当前 database 中用户定义的 function。

## SHOW MODULES

```sql
SHOW [FULL] MODULES
```

展示当前环境激活的所有 module。

**FULL**
展示当前环境加载的所有 module 及激活状态。

## SHOW JARS

```sql
SHOW JARS
```

展示所有通过 [`ADD JAR`]({{< ref "docs/dev/table/sql/jar" >}}#add-jar) 语句加入到 session classloader 中的 jar。

<span class="label label-danger">Attention</span> 当前 SHOW JARS 命令只能在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中使用。

{{< top >}}
