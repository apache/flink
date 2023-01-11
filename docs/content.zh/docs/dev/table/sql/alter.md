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

// 新增列 `order` 并置于第一位
tableEnv.executeSql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST");

// 新增更多列, 以及主键和 watermark
tableEnv.executeSql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)");

// 修改列类型, 注释及 watermark 策略
tableEnv.executeSql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)");

// 删除 watermark
tableEnv.executeSql("ALTER TABLE Orders DROP WATERMARK");

// 删除列
tableEnv.executeSql("ALTER TABLE Orders DROP (amount, ts, category)");

// 重命名列
tableEnv.executeSql("ALTER TABLE Orders RENAME `order` TO order_id");

// 把 “Orders” 的表名改为 “NewOrders”
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders");

// 字符串数组：["NewOrders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// 新增列 `order` 并置于第一位
tableEnv.executeSql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST")

// 新增更多列, 以及主键和 watermark
tableEnv.executeSql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)")

// 修改列类型, 注释, 以及主键和 watermark
tableEnv.executeSql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)")

// 删除 watermark
tableEnv.executeSql("ALTER TABLE Orders DROP WATERMARK")

// 删除列
tableEnv.executeSql("ALTER TABLE Orders DROP (amount, ts, category)")

// 重命名列
tableEnv.executeSql("ALTER TABLE Orders RENAME `order` TO order_id")

// 字符串数组： ["Orders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()

// rename "Orders" to "NewOrders"
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders")

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

# 新增列 `order` 并置于第一位
table_env.execute_sql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST");

# 新增更多列, 主键及 watermark
table_env.execute_sql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)");

# 修改列类型, 列注释, 主键及 watermark
table_env.execute_sql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)");

# 删除 watermark
table_env.execute_sql("ALTER TABLE Orders DROP WATERMARK");

# 删除列
table_env.execute_sql("ALTER TABLE Orders DROP (amount, ts, category)");

# 重命名列
table_env.execute_sql("ALTER TABLE Orders RENAME `order` TO order_id");

# 把 “Orders” 的表名改为 “NewOrders”
table_env.execute_sql("ALTER TABLE Orders RENAME TO NewOrders");

# 字符串数组：["NewOrders"]
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Execute statement succeed.

Flink SQL> ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST;
[INFO] Execute statement succeed.

Flink SQL> DESCRIBE Orders;
+---------+--------+------+-----+--------+-----------+------------------+
|    name |   type | null | key | extras | watermark |          comment |
+---------+--------+------+-----+--------+-----------+------------------+
|   order |    INT | TRUE |     |        |           | order identifier |
|    user | BIGINT | TRUE |     |        |           |                  |
| product | STRING | TRUE |     |        |           |                  |
|  amount |    INT | TRUE |     |        |           |                  |
+---------+--------+------+-----+--------+-----------+------------------+
4 rows in set

Flink SQL> ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR);
[INFO] Execute statement succeed. 

Flink SQL> DESCRIBE Orders;
+----------+------------------------+-------+------------+--------+--------------------------+------------------+
|     name |                   type |  null |        key | extras |                watermark |          comment |
+----------+------------------------+-------+------------+--------+--------------------------+------------------+
|    order |                    INT | FALSE | PRI(order) |        |                          | order identifier |
|     user |                 BIGINT |  TRUE |            |        |                          |                  |
|  product |                 STRING |  TRUE |            |        |                          |                  |
| category |                 STRING |  TRUE |            |        |                          |                  |
|   amount |                    INT |  TRUE |            |        |                          |                  |
|       ts | TIMESTAMP(3) *ROWTIME* |  TRUE |            |        | `ts` - INTERVAL '1' HOUR |                  |
+----------+------------------------+-------+------------+--------+--------------------------+------------------+
6 rows in set

Flink SQL> ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts);
[INFO] Execute statement succeed. 

Flink SQL> DESCRIBE Orders;
+----------+------------------------+-------+------------+--------+-----------+---------------------+
|     name |                   type |  null |        key | extras | watermark |             comment |
+----------+------------------------+-------+------------+--------+-----------+---------------------+
|    order |                    INT | FALSE | PRI(order) |        |           |    order identifier |
| category |                 STRING |  TRUE |            |        |           | category identifier |
|     user |                 BIGINT |  TRUE |            |        |           |                     |
|  product |                 STRING |  TRUE |            |        |           |                     |
|   amount |                 DOUBLE | FALSE |            |        |           |                     |
|       ts | TIMESTAMP(3) *ROWTIME* |  TRUE |            |        |      `ts` |                     |
+----------+------------------------+-------+------------+--------+-----------+---------------------+
6 rows in set

Flink SQL> ALTER TABLE Orders DROP WATERMARK;
[INFO] Execute statement succeed.

Flink SQL> DESCRIBE Orders;
+----------+--------------+-------+------------+--------+-----------+---------------------+
|     name |         type |  null |        key | extras | watermark |             comment |
+----------+--------------+-------+------------+--------+-----------+---------------------+
|    order |          INT | FALSE | PRI(order) |        |           |    order identifier |
| category |       STRING |  TRUE |            |        |           | category identifier |
|     user |       BIGINT |  TRUE |            |        |           |                     |
|  product |       STRING |  TRUE |            |        |           |                     |
|   amount |       DOUBLE | FALSE |            |        |           |                     |
|       ts | TIMESTAMP(3) |  TRUE |            |        |           |                     |
+----------+--------------+-------+------------+--------+-----------+---------------------+
6 rows in set

Flink SQL> ALTER TABLE Orders DROP (amount, ts, category);
[INFO] Execute statement succeed.

Flink SQL> DESCRIBE Orders;
+---------+--------+-------+------------+--------+-----------+------------------+
|    name |   type |  null |        key | extras | watermark |          comment |
+---------+--------+-------+------------+--------+-----------+------------------+
|   order |    INT | FALSE | PRI(order) |        |           | order identifier |
|    user | BIGINT |  TRUE |            |        |           |                  |
| product | STRING |  TRUE |            |        |           |                  |
+---------+--------+-------+------------+--------+-----------+------------------+
3 rows in set

Flink SQL> ALTER TABLE Orders RENAME `order` to `order_id`;
[INFO] Execute statement succeed.

Flink SQL> DESCRIBE Orders;
+----------+--------+-------+---------------+--------+-----------+------------------+
|     name |   type |  null |           key | extras | watermark |          comment |
+----------+--------+-------+---------------+--------+-----------+------------------+
| order_id |    INT | FALSE | PRI(order_id) |        |           | order identifier |
|     user | BIGINT |  TRUE |               |        |           |                  |
|  product | STRING |  TRUE |               |        |           |                  |
+----------+--------+-------+---------------+--------+-----------+------------------+
3 rows in set

Flink SQL> SHOW TABLES;
+------------+
| table name |
+------------+
|     Orders |
+------------+
1 row in set

Flink SQL> ALTER TABLE Orders RENAME TO NewOrders;
[INFO] Execute statement succeed.

Flink SQL> SHOW TABLES;
+------------+
| table name |
+------------+
|  NewOrders |
+------------+
1 row in set
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## ALTER TABLE

当前支持的 ALTER TABLE 语法如下
```text
ALTER TABLE [IF EXISTS] table_name {
    ADD { <schema_component> | (<schema_component> [, ...]) }
  | MODIFY { <schema_component> | (<schema_component> [, ...]) }
  | DROP {column_name | (column_name, column_name, ....) | PRIMARY KEY | CONSTRAINT constraint_name | WATERMARK}
  | RENAME old_column_name TO new_column_name
  | RENAME TO new_table_name
  | SET (key1=val1, ...)
  | RESET (key1, ...)
}

<schema_component>:
  { <column_component> | <constraint_component> | <watermark_component> }

<column_component>:
  column_name <column_definition> [FIRST | AFTER column_name]

<constraint_component>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<watermark_component>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

<column_definition>:
  { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> } [COMMENT column_comment]

<physical_column_definition>:
  column_type

<metadata_column_definition>:
  column_type METADATA [ FROM metadata_key ] [ VIRTUAL ]

<computed_column_definition>:
  AS computed_column_expression
```

<span class="label label-info">注意</span> 如果表不存在且未指定 `IF EXISTS` 时将会抛出 `ValidationException`。

**新增列**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name ADD (<column_component>[, <column_component>, ...])
```

向表中新增一列或多列至指定位置，包括
- 将新增列置于最后
- 将新增列置于最前
- 将新增列置于指定列后

在触发以下任一条件时将抛出 `ValidationException`
- 新增列列名已存在
- `AFTER` 指向不存在的列
- 新增计算列的表达式引用了其它计算列或不存在的列
- 新增计算列的表达式非法

**新增主键约束**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name ADD [CONSTRAINT constraint_name] PRIMARY KEY(col1[, col2, ...]) NOT ENFORCED
```

向表中新增主键约束。当表中已存在主键、指定列不存在或为非物理列时抛出 `ValidationException`。

<span class="label label-danger">注意</span> 指定列为主键列时会隐式修改该列的 nullability 为 false。

**新增 Watermark**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name ADD WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

向表中新增 Watermark。当表中已存在 Watermark、指定的 row-time 列不存在或表达式非法时抛出 `ValidationException`。

**修改列**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name MODIFY (<column_component>[, <column_component>, ...])
```

修改表中已存在的一列或多列，包括
- 修改列的类型
- 修改列的注释
- 修改列的 nullability
- 修改列的位置

在触发以下任一条件时将抛出 `ValidationException`
- 修改不存在的列名
- `AFTER` 指向不存在的列
- 修改列类型，导致引用其的计算列或 watermark 表达式非法
- 将主键列修改为计算列或 metadata 列


**修改主键约束**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name MODIFY [CONSTRAINT constraint_name] PRIMARY KEY(col1[, col2, ...]) NOT ENFORCED
```

修改表的主键约束。在触发以下任一条件时将抛出 `ValidationException`
- 源表未定义主键
- 指定不存在的列
- 指定非物理列

<span class="label label-danger">注意</span> 指定列为主键列时会隐式修改该列的 nullability 为 false。

**修改 Watermark**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name MODIFY WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

修改 watermark 表达式或重新指定 row-time 列。在触发以下任一条件时将抛出 `ValidationException`
- 源表未定义 watermark
- 指定不存在的 row-time 列，或 watermark 表达式非法

**删除列**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name DROP (col1[, col2, ...])
```

删除表中一列或多列。在触发以下任一条件时将抛出 `ValidationException`
- 指定列不存在
- 指定列存在计算列引用，且计算列不随指定列一起删除
- 指定列是主键列
- 指定列定义了 watermark 策略

**删除主键约束**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name DROP <constraint_definition>

<constraint_definition>:
{
    PRIMARY KEY | CONSTRAINT constraint_name
}
```

使用关键字 `PRIMARY KEY` 或约束名删除主键约束。如果表中不存在主键或指定错误的约束名时抛出 `ValidationException`。

**删除 Watermark**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name DROP WATERMARK
```

使用关键字 `WATERMARK` 删除 watermark。如果表中不存在 watermark 时抛出 `ValidationException`。

**重命名列**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name RENAME old_column_name TO new_column_name
```

把原有的列名更改为新的列名。若指定的原有列名不存在或新列名已存在时抛出 `ValidationException`。

**重命名表**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name RENAME TO new_table_name
```

把原有的表名更改为新的表名。 若新表名已存在则抛出 `TableAlreadyExistException`。

**设置或修改表属性**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
```

为指定的表设置一个或多个属性。若个别属性已经存在于表中，则使用新的值覆盖旧的值。

**重置表属性**

```text
ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name RESET (key1, key2, ...)
```

为指定的表重置一个或多个属性。不支持设置 `connector` 属性, 会抛出 `ValidationException`。

{{< top >}}

## ALTER VIEW

```sql
ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name
```

Renames a given view to a new name within the same catalog and database.

```sql
ALTER VIEW [catalog_name.][db_name.]view_name AS new_query_expression
```

Changes the underlying query defining the given view to a new query.

{{< top >}}

## ALTER DATABASE

```sql
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
```

在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。

{{< top >}}

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
