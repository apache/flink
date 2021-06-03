---
title: "USE 语句"
weight: 10
type: docs
aliases:
  - /zh/dev/table/sql/use.html
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

# USE 语句



USE 语句用来设置当前的 catalog 或者 database。

## 运行一个 USE 语句

{{< tabs "explain" >}}
{{< tab "Java" >}}

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 USE 语句。 若 USE 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 USE 语句。

{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 USE 语句。 若 USE 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 USE 语句。
{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 USE 语句。 若 USE 操作执行成功，`execute_sql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 USE 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 SQL CLI 中执行 USE 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 USE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "9c2050ce-b261-4692-9447-b9b6772c5b38" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// create a catalog
tEnv.executeSql("CREATE CATALOG cat1 WITH (...)");
tEnv.executeSql("SHOW CATALOGS").print();
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// | cat1            |
// +-----------------+

// change default catalog
tEnv.executeSql("USE CATALOG cat1");

tEnv.executeSql("SHOW DATABASES").print();
// databases are empty
// +---------------+
// | database name |
// +---------------+
// +---------------+

// create a database
tEnv.executeSql("CREATE DATABASE db1 WITH (...)");
tEnv.executeSql("SHOW DATABASES").print();
// +---------------+
// | database name |
// +---------------+
// |        db1    |
// +---------------+

// change default database
tEnv.executeSql("USE db1");

// change module resolution order and enabled status
tEnv.executeSql("USE MODULES hive");
tEnv.executeSql("SHOW FULL MODULES").print();
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// create a catalog
tEnv.executeSql("CREATE CATALOG cat1 WITH (...)")
tEnv.executeSql("SHOW CATALOGS").print()
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// | cat1            |
// +-----------------+

// change default catalog
tEnv.executeSql("USE CATALOG cat1")

tEnv.executeSql("SHOW DATABASES").print()
// databases are empty
// +---------------+
// | database name |
// +---------------+
// +---------------+

// create a database
tEnv.executeSql("CREATE DATABASE db1 WITH (...)")
tEnv.executeSql("SHOW DATABASES").print()
// +---------------+
// | database name |
// +---------------+
// |        db1    |
// +---------------+

// change default database
tEnv.executeSql("USE db1")

// change module resolution order and enabled status
tEnv.executeSql("USE MODULES hive")
tEnv.executeSql("SHOW FULL MODULES").print()
// +-------------+-------+
// | module name |  used |
// +-------------+-------+
// |        hive |  true |
// |        core | false |
// +-------------+-------+
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = StreamTableEnvironment.create(...)

# create a catalog
table_env.execute_sql("CREATE CATALOG cat1 WITH (...)")
table_env.execute_sql("SHOW CATALOGS").print()
# +-----------------+
# |    catalog name |
# +-----------------+
# | default_catalog |
# | cat1            |
# +-----------------+

# change default catalog
table_env.execute_sql("USE CATALOG cat1")

table_env.execute_sql("SHOW DATABASES").print()
# databases are empty
# +---------------+
# | database name |
# +---------------+
# +---------------+

# create a database
table_env.execute_sql("CREATE DATABASE db1 WITH (...)")
table_env.execute_sql("SHOW DATABASES").print()
# +---------------+
# | database name |
# +---------------+
# |           db1 |
# +---------------+

# change default database
table_env.execute_sql("USE db1")

# change module resolution order and enabled status
table_env.execute_sql("USE MODULES hive")
table_env.execute_sql("SHOW FULL MODULES").print()
# +-------------+-------+
# | module name |  used |
# +-------------+-------+
# |        hive |  true |
# |        core | false |
# +-------------+-------+
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE CATALOG cat1 WITH (...);
[INFO] Catalog has been created.

Flink SQL> SHOW CATALOGS;
default_catalog
cat1

Flink SQL> USE CATALOG cat1;

Flink SQL> SHOW DATABASES;

Flink SQL> CREATE DATABASE db1 WITH (...);
[INFO] Database has been created.

Flink SQL> SHOW DATABASES;
db1

Flink SQL> USE db1;

Flink SQL> USE MODULES hive;
[INFO] Use modules succeeded!
Flink SQL> SHOW FULL MODULES;
+-------------+-------+
| module name |  used |
+-------------+-------+
|        hive |  true |
|        core | false |
+-------------+-------+
2 rows in set
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## USE CATALOG

```sql
USE CATALOG catalog_name
```

设置当前的 catalog。所有后续命令未显式指定 catalog 的将使用此 catalog。如果指定的的 catalog 不存在，则抛出异常。默认的当前 catalog 是 `default_catalog`。

## USE MODULES
```sql
USE MODULES module_name1[, module_name2, ...]
```
Set the enabled modules with declared order. All subsequent commands will resolve metadata(functions/user-defined types/rules, *etc.*) within enabled modules and follow resolution order. A module is used by default when it is loaded. Loaded modules will become disabled if not used by `USE MODULES` statement. The default loaded and enabled module is `core`.

## USE

```sql
USE [catalog_name.]database_name
```

设置当前的 database。所有后续命令未显式指定 database 的将使用此 database。如果指定的的 database 不存在，则抛出异常。默认的当前 database 是 `default_database`。
