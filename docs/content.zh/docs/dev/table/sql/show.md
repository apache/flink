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


SHOW 语句用于列出其相应父对象中的对象，例如 catalog、database、table 和 view、column、function 和 module。有关详细信息和其他选项，请参见各个命令。

SHOW CREATE 语句用于打印给定对象的创建 DDL 语句。当前的 SHOW CREATE 语句仅在打印给定表和视图的 DDL 语句时可用。

目前 Flink SQL 支持下列 SHOW 语句：
- SHOW CATALOGS
- SHOW CURRENT CATALOG
- SHOW DATABASES
- SHOW CURRENT DATABASE
- SHOW TABLES
- SHOW CREATE TABLE
- SHOW COLUMNS
- SHOW PARTITIONS
- SHOW PROCEDURES
- SHOW VIEWS
- SHOW CREATE VIEW
- SHOW FUNCTIONS
- SHOW MODULES
- SHOW FULL MODULES
- SHOW JARS
- SHOW JOBS


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

// show columns
tEnv.executeSql("SHOW COLUMNS FROM my_table LIKE '%f%'").print();
// +--------+-------+------+-----+--------+-----------+
// |   name |  type | null | key | extras | watermark |
// +--------+-------+------+-----+--------+-----------+
// | field2 | BYTES | true |     |        |           |
// +--------+-------+------+-----+--------+-----------+


// create a view
tEnv.executeSql("CREATE VIEW my_view AS SELECT * FROM my_table");
// show views
tEnv.executeSql("SHOW VIEWS").print();
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show create view
tEnv.executeSql("SHOW CREATE VIEW my_view").print();
// CREATE VIEW `default_catalog`.`default_db`.`my_view`(`field1`, `field2`, ...) as
// SELECT *
// FROM `default_catalog`.`default_database`.`my_table`

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

// show columns
tEnv.executeSql("SHOW COLUMNS FROM my_table LIKE '%f%'").print()
// +--------+-------+------+-----+--------+-----------+
// |   name |  type | null | key | extras | watermark |
// +--------+-------+------+-----+--------+-----------+
// | field2 | BYTES | true |     |        |           |
// +--------+-------+------+-----+--------+-----------+

// create a view
tEnv.executeSql("CREATE VIEW my_view AS SELECT * FROM my_table")
// show views
tEnv.executeSql("SHOW VIEWS").print()
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show create view
tEnv.executeSql("SHOW CREATE VIEW my_view").print();
// CREATE VIEW `default_catalog`.`default_db`.`my_view`(`field1`, `field2`, ...) as
// SELECT *
// FROM `default_catalog`.`default_database`.`my_table`

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

# show columns
table_env.execute_sql("SHOW COLUMNS FROM my_table LIKE '%f%'").print()
# +--------+-------+------+-----+--------+-----------+
# |   name |  type | null | key | extras | watermark |
# +--------+-------+------+-----+--------+-----------+
# | field2 | BYTES | true |     |        |           |
# +--------+-------+------+-----+--------+-----------+

# create a view
table_env.execute_sql("CREATE VIEW my_view AS SELECT * FROM my_table")
# show views
table_env.execute_sql("SHOW VIEWS").print()
# +-----------+
# | view name |
# +-----------+
# |   my_view |
# +-----------+

# show create view
table_env.execute_sql("SHOW CREATE VIEW my_view").print()
# CREATE VIEW `default_catalog`.`default_db`.`my_view`(`field1`, `field2`, ...) as
# SELECT *
# FROM `default_catalog`.`default_database`.`my_table`

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


Flink SQL> SHOW COLUMNS from MyUserTable LIKE '%f%';
+--------+-------+------+-----+--------+-----------+
|   name |  type | null | key | extras | watermark |
+--------+-------+------+-----+--------+-----------+
| field2 | BYTES | true |     |        |           |
+--------+-------+------+-----+--------+-----------+
1 row in set


Flink SQL> CREATE VIEW my_view AS SELECT * from my_table;
[INFO] View has been created.

Flink SQL> SHOW VIEWS;
my_view

Flink SQL> SHOW CREATE VIEW my_view;
CREATE VIEW `default_catalog`.`default_db`.`my_view`(`field1`, `field2`, ...) as
SELECT *
FROM `default_catalog`.`default_database`.`my_table`

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
SHOW TABLES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE <sql_like_pattern> ]
```

展示指定库的所有表，如果没有指定库则展示当前库的所有表。另外返回的结果能被一个可选的匹配字符串过滤。

**LIKE**
根据可选的 `LIKE` 语句展示给定库中与 `<sql_like_pattern>` 是否模糊相似的所有表。

`LIKE` 子句中 sql 正则式的语法与 `MySQL` 方言中的语法相同。
* `%` 匹配任意数量的字符, 也包括0数量字符, `\%` 匹配一个 `%` 字符.
* `_` 只匹配一个字符, `\_` 匹配一个 `_` 字符.


### SHOW TABLES 示例

假定在 `catalog1` 的 `db1` 库有如下表：
* person
* dim

在会话的当前库下有如下表：
* items
* orders

- 显示指定库的所有表。

```sql
show tables from db1;
-- show tables from catalog1.db1;
-- show tables in db1;
-- show tables in catalog1.db1;
+------------+
| table name |
+------------+
|        dim |
|     person |
+------------+
2 rows in set
```

- 显示指定库中相似于指定 SQL 正则式的所有表。

```sql
show tables from db1 like '%n';
-- show tables from catalog1.db1 like '%n';
-- show tables in db1 like '%n';
-- show tables in catalog1.db1 like '%n';
+------------+
| table name |
+------------+
|     person |
+------------+
1 row in set
```

- 显示指定库中不相似于指定 SQL 正则式的所有表。

```sql
show tables from db1 not like '%n';
-- show tables from catalog1.db1 not like '%n';
-- show tables in db1 not like '%n';
-- show tables in catalog1.db1 not like '%n';
+------------+
| table name |
+------------+
|        dim |
+------------+
1 row in set
```

- 显示当前库中的所有表。

```sql
show tables;
+------------+
| table name |
+------------+
|      items |
|     orders |
+------------+
2 rows in set
```

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [[catalog_name.]db_name.]table_name
```

展示创建指定表的 create 语句。

该语句的输出内容包括表名、列名、数据类型、约束、注释和配置。

当您需要了解现有表的结构、配置和约束，或在另一个数据库中重新创建表时，这个语句非常有用。

假设表 `orders` 是按如下方式创建的：
```sql
CREATE TABLE orders (
  order_id BIGINT NOT NULL comment 'this is the primary key, named ''order_id''.',
  product VARCHAR(32),
  amount INT,
  ts TIMESTAMP(3) comment 'notice: watermark, named ''ts''.',
  ptime AS PROCTIME() comment 'notice: computed column, named ''ptime''.',
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'datagen'
);
```
展示表创建语句。
```sql
show create table orders;
+---------------------------------------------------------------------------------------------+
|                                                                                      result |
+---------------------------------------------------------------------------------------------+
| CREATE TABLE `default_catalog`.`default_database`.`orders` (
  `order_id` BIGINT NOT NULL COMMENT 'this is the primary key, named ''order_id''.',
  `product` VARCHAR(32),
  `amount` INT,
  `ts` TIMESTAMP(3) COMMENT 'notice: watermark, named ''ts''.',
  `ptime` AS PROCTIME() COMMENT 'notice: computed column, named ''ptime''.',
  WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND,
  CONSTRAINT `PK_3599338` PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
  'connector' = 'datagen'
)
 |
+---------------------------------------------------------------------------------------------+
1 row in set
```

<span class="label label-danger">Attention</span> 目前 `SHOW CREATE TABLE` 只支持通过 Flink SQL DDL 创建的表。

## SHOW COLUMNS

```sql
SHOW COLUMNS ( FROM | IN ) [[catalog_name.]database.]<table_name> [ [NOT] LIKE <sql_like_pattern>]
```

展示给定表的所有列。

**LIKE**
根据可选的 `LIKE` 语句展示给定表中与 `<sql_like_pattern>` 是否模糊相似的所有列。

`LIKE` 子句中 sql 正则式的语法与 `MySQL` 方言中的语法相同。

<a name="show-columns-examples"></a>

### SHOW COLUMNS 示例

假定在 `catalog1` catalog 中的 `database1` 数据库中有名为 `orders` 的表，其结构如下所示：
```sql
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | false | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  true |           |               |                            |
|  amount |                         INT |  true |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  true |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | false |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
```

- 显示指定表中的所有列。

```sql
show columns from orders;
-- show columns from database1.orders;
-- show columns from catalog1.database1.orders;
-- show columns in orders;
-- show columns in database1.orders;
-- show columns in catalog1.database1.orders;
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
|    user |                      BIGINT | false | PRI(user) |               |                            |
| product |                 VARCHAR(32) |  true |           |               |                            |
|  amount |                         INT |  true |           |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  true |           |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | false |           | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+
5 rows in set
```

- 显示指定表中相似于指定 SQL 正则式的所有列。

```sql
show columns from orders like '%r';
-- show columns from database1.orders like '%r';
-- show columns from catalog1.database1.orders like '%r';
-- show columns in orders like '%r';
-- show columns in database1.orders like '%r';
-- show columns in catalog1.database1.orders like '%r';
+------+--------+-------+-----------+--------+-----------+
| name |   type |  null |       key | extras | watermark |
+------+--------+-------+-----------+--------+-----------+
| user | BIGINT | false | PRI(user) |        |           |
+------+--------+-------+-----------+--------+-----------+
1 row in set
```

- 显示指定表中不相似于指定 SQL 正则式的所有列。

```sql
show columns from orders not like '%_r';
-- show columns from database1.orders not like '%_r';
-- show columns from catalog1.database1.orders not like '%_r';
-- show columns in orders not like '%_r';
-- show columns in database1.orders not like '%_r';
-- show columns in catalog1.database1.orders not like '%_r';
+---------+-----------------------------+-------+-----+---------------+----------------------------+
|    name |                        type |  null | key |        extras |                  watermark |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
| product |                 VARCHAR(32) |  true |     |               |                            |
|  amount |                         INT |  true |     |               |                            |
|      ts |      TIMESTAMP(3) *ROWTIME* |  true |     |               | `ts` - INTERVAL '1' SECOND |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | false |     | AS PROCTIME() |                            |
+---------+-----------------------------+-------+-----+---------------+----------------------------+
4 rows in set
```

## SHOW PARTITIONS

```sql
SHOW PARTITIONS [[catalog_name.]database.]<table_name> [ PARTITION <partition_spec>]

<partition_spec>:
  (key1=val1, key2=val2, ...)
```

展示给定分区表的所有分区。

**PARTITION**
根据可选的 `PARTITION` 语句展示给定分区表中在指定的 `<partition_spec>` 分区下的所有分区。

### SHOW PARTITIONS 示例

假定在 `catalog1` catalog 中的 `database1` 数据库中有名为 `table1` 的分区表，其包含的所有分区如下所示：

```sql
+---------+-----------------------------+
|      id |                        date |
+---------+-----------------------------+
|    1001 |                  2020-01-01 |
|    1002 |                  2020-01-01 |
|    1002 |                  2020-01-02 |
+---------+-----------------------------+
```

- 显示指定分区表中的所有分区。

```sql
show partitions table1;
-- show partitions database1.table1;
-- show partitions catalog1.database1.table1;
+---------+-----------------------------+
|      id |                        date |
+---------+-----------------------------+
|    1001 |                  2020-01-01 |
|    1002 |                  2020-01-01 |
|    1002 |                  2020-01-02 |
+---------+-----------------------------+
3 rows in set
```

- 显示指定分区表在指定分区下的所有分区。

```sql
show partitions table1 partition (id=1002);
-- show partitions database1.table1 partition (id=1002);
-- show partitions catalog1.database1.table1 partition (id=1002);
+---------+-----------------------------+
|      id |                        date |
+---------+-----------------------------+
|    1002 |                  2020-01-01 |
|    1002 |                  2020-01-02 |
+---------+-----------------------------+
2 rows in set
```

## SHOW PROCEDURES

```sql
SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ]	
```

展示指定 catalog 和 database 下的所有 procedure。
如果没有指定 catalog 和 database，则将使用当前 catalog 和 当前 database。另外可以用 `<sql_like_pattern>` 来过滤要返回的 procedure。

**LIKE**
根据可选的 `LIKE` 语句与 `<sql_like_pattern>` 是否模糊匹配的所有 procedure。

`LIKE` 子句中 SQL 正则式的语法与 `MySQL` 方言中的语法相同。
* `%` 匹配任意数量的字符, 也包括0数量字符, `\%` 匹配一个 `%` 字符.
* `_` 只匹配一个字符, `\_` 匹配一个 `_` 字符.

**ILIKE**
它的行为和 LIKE 相同，只是对于大小写是不敏感的。

## SHOW VIEWS

```sql
SHOW VIEWS
```

展示当前 catalog 和当前 database 中所有的视图。

## SHOW CREATE VIEW

```sql
SHOW CREATE VIEW [catalog_name.][db_name.]view_name
```

展示创建指定视图的 create 语句。

## SHOW FUNCTIONS

```sql
SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ]
```

展示指定 catalog 和 database 下的所有 function，包括：系统 function 和用户定义的 function。
如果没有指定 catalog 和 database，则将使用当前 catalog 和 当前 database。另外可以用 `<sql_like_pattern>` 来过滤要返回的 function。

**USER**
仅展示用户定义的 function, 另外可以用 `<sql_like_pattern>` 来过滤要返回的 function。

**LIKE**
根据可选的 `LIKE` 语句与 `<sql_like_pattern>` 是否模糊匹配的所有 function。

`LIKE` 子句中 SQL 正则式的语法与 `MySQL` 方言中的语法相同。
* `%` 匹配任意数量的字符, 也包括0数量字符, `\%` 匹配一个 `%` 字符.
* `_` 只匹配一个字符, `\_` 匹配一个 `_` 字符.

**ILIKE**
它的行为和 LIKE 相同，只是对于大小写是不敏感的。

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

<span class="label label-danger">Attention</span> 当前 SHOW JARS 命令只能在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 或者 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) 中使用.

## SHOW JOBS

```sql
SHOW JOBS
```

展示集群中所有作业。

<span class="label label-danger">Attention</span> 当前 SHOW JOBS 命令只能在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 或者 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) 中使用.

{{< top >}}
