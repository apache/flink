---
title: "ALTER 语句"
weight: 6
type: docs
aliases:
  - /zh/dev/table/sql/alter.html
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

# ALTER 语句



ALTER 语句用于修改一个已经在 [Catalog]({{< ref "docs/dev/table/catalogs" >}}) 中注册的表、视图或函数定义。

Flink SQL 目前支持以下 ALTER 语句：

- ALTER TABLE
- ALTER VIEW
- ALTER DATABASE
- ALTER FUNCTION

## 执行 ALTER 语句

{{< tabs "explain" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 ALTER 语句。 若 ALTER 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 ALTER 语句。
{{< /tab >}}
{{< tab "Scala" >}}
可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 ALTER 语句。 若 ALTER 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 ALTER 语句。
{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 ALTER 语句。 若 ALTER 操作执行成功，`execute_sql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 ALTER 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 ALTER 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 ALTER 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "147c58e0-44d1-4f78-b995-88b3edba7bec" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// 字符串数组： ["Orders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();

// 把 “Orders” 的表名改为 “NewOrders”
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders;");

// 字符串数组：["NewOrders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// 字符串数组： ["Orders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()

// 把 “Orders” 的表名改为 “NewOrders”
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders;")

// 字符串数组：["NewOrders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 字符串数组： ["Orders"]
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()

# 把 “Orders” 的表名改为 “NewOrders”
table_env.execute_sql("ALTER TABLE Orders RENAME TO NewOrders;")

# 字符串数组：["NewOrders"]
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> SHOW TABLES;
Orders

Flink SQL> ALTER TABLE Orders RENAME TO NewOrders;
[INFO] Table has been removed.

Flink SQL> SHOW TABLES;
NewOrders
```
{{< /tab >}}
{{< /tabs >}}

## ALTER TABLE

* 重命名表

```sql
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
```

把原有的表名更改为新的表名。

* 设置或修改表属性

```sql
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
```

为指定的表设置一个或多个属性。若个别属性已经存在于表中，则使用新的值覆盖旧的值。

## ALTER VIEW

```sql
ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name
```

Renames a given view to a new name within the same catalog and database.

```sql
ALTER VIEW [catalog_name.][db_name.]view_name AS new_query_expression
```

Changes the underlying query defining the given view to a new query.

## ALTER DATABASE

```sql
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
```

在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。

## ALTER FUNCTION

```sql
ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION
  [IF EXISTS] [catalog_name.][db_name.]function_name
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
```

修改一个有 catalog 和数据库命名空间的 catalog function ，需要指定一个新的 identifier ，可指定 language tag 。若函数不存在，删除会抛出异常。

如果 language tag 是 JAVA 或者 SCALA ，则 identifier 是 UDF 实现类的全限定名。关于 JAVA/SCALA UDF 的实现，请参考 [自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})。

如果 language tag 是 PYTHON ， 则 identifier 是 UDF 对象的全限定名，例如 `pyflink.table.tests.test_udf.add`。关于 PYTHON UDF 的实现，请参考 [Python UDFs]({{< ref "docs/dev/python/table/udfs/python_udfs" >}})。

**TEMPORARY**

修改一个有 catalog 和数据库命名空间的临时 catalog function ，并覆盖原有的 catalog function 。

**TEMPORARY SYSTEM**

修改一个没有数据库命名空间的临时系统 catalog function ，并覆盖系统内置的函数。

**IF EXISTS**

若函数不存在，则不进行任何操作。

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag 用于指定 Flink runtime 如何执行这个函数。目前，只支持 JAVA，SCALA 和 PYTHON，且函数的默认语言为 JAVA。
