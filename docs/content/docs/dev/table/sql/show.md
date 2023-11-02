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

SHOW statements are used to list objects within their corresponding parent, such as catalogs, databases, tables and views, columns, functions, and modules. See the individual commands for more details and additional options.

SHOW CREATE statements are used to print a DDL statement with which a given object can be created. The currently 'SHOW CREATE' statement is only available in printing DDL statement of the given table and view.

Flink SQL supports the following SHOW statements for now:
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
- SHOW JARS
- SHOW JOBS

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
SHOW TABLES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE <sql_like_pattern> ]
```

Show all tables for an optionally specified database. If no database is specified then the tables are returned from the current database. Additionally, the output of this statement may be filtered by an optional matching pattern.

**LIKE**
Show all tables with given table name and optional `LIKE` clause, whose name is whether similar to the `<sql_like_pattern>`.

The syntax of sql pattern in `LIKE` clause is the same as that of `MySQL` dialect.
* `%` matches any number of characters, even zero characters, `\%` matches one `%` character.
* `_` matches exactly one character, `\_` matches one `_` character.

### SHOW TABLES EXAMPLES

Assumes that the `db1` database located in `catalog1` catalog has the following tables:
* person
* dim

the current database in session has the following tables:
* items
* orders

- Shows all tables of the given database.

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

- Shows all tables of the given database, which are similar to the given sql pattern.

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

- Shows all tables of the given database, which are not similar to the given sql pattern.

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

- Shows all tables of the current database.

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

Show create table statement for specified table.

The output includes the table name, column names, data types, constraints, comments, and configurations.

It is a very useful statement when you need to understand the structure, configuration and constraints of an existing table or to recreate the table in another database.

Assumes that the table `orders` is created as follows:
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
Shows the creation statement.
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

<span class="label label-danger">Attention</span> Currently `SHOW CREATE TABLE` only supports table that is created by Flink SQL DDL.

## SHOW COLUMNS

```sql
SHOW COLUMNS ( FROM | IN ) [[catalog_name.]database.]<table_name> [ [NOT] LIKE <sql_like_pattern>]
```

Show all columns of the table with given table name and optional like clause.

**LIKE**
Show all columns of the table with given table name and optional `LIKE` clause, whose name is whether similar to the `<sql_like_pattern>`.

The syntax of sql pattern in `LIKE` clause is the same as that of `MySQL` dialect.

### SHOW COLUMNS EXAMPLES

Assumes that the table named `orders` in the `database1` database which is located in the `catalog1` catalog has the following structure:

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

- Shows all columns of the given table.

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

- Shows all columns of the given table, which are similar to the given sql pattern.

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

- Shows all columns of the given table, which are not similar to the given sql pattern.

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

Show all partitions of the partitioned table with the given table name and optional partition clause.

**PARTITION**
Show all the partitions which are under the provided <partition_spec> in the given table.

### SHOW PARTITIONS EXAMPLES

Assumes that the partitioned table `table1` in the `database1` database which is located in the `catalog1` catalog has the following partitions:

```sql
+---------+-----------------------------+
|      id |                        date |
+---------+-----------------------------+
|    1001 |                  2020-01-01 |
|    1002 |                  2020-01-01 |
|    1002 |                  2020-01-02 |
+---------+-----------------------------+
```

- Shows all partitions of the given table.

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

- Shows all partitions of the given table with the given partition spec.

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

Show all procedures for an optionally specified database. If no database is specified then the procedures are returned from the current database. Additionally, a `<sql_like_pattern>` can be used to filter the procedures.

**LIKE**
Show all procedures with a `LIKE` clause, whose name is similar to the `<sql_like_pattern>`.

The syntax of the SQL pattern in the `LIKE` clause is the same as that of the `MySQL` dialect.
* `%` matches any number of characters, even zero characters, and `\%` matches one `%` character.
* `_` matches exactly one character, `\_` matches one `_` character.

**ILIKE**
The same behavior as `LIKE` but the SQL pattern is case-insensitive.

## SHOW VIEWS

```sql
SHOW VIEWS
```

Show all views in the current catalog and the current database.

## SHOW CREATE VIEW

```sql
SHOW CREATE VIEW [catalog_name.][db_name.]view_name
```

Show create view statement for specified view.

## SHOW FUNCTIONS

```sql
SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ]	
```

Show all functions including system functions and user-defined functions for an optionally specified database. If no database is specified then the functions are returned from the current database. Additionally, a `<sql_like_pattern>` can be used to filter the functions.

**USER**
Show only user-defined functions for an optionally specified database. If no database is specified then the functions are returned from the current database. Additionally, a `<sql_like_pattern>` can be used to filter the functions.

**LIKE**
Show all functions with a `LIKE` clause, whose name is similar to the `<sql_like_pattern>`.

The syntax of the SQL pattern in the `LIKE` clause is the same as that of the `MySQL` dialect.
* `%` matches any number of characters, even zero characters, and `\%` matches one `%` character.
* `_` matches exactly one character, `\_` matches one `_` character.

**ILIKE**
The same behavior as `LIKE` but the SQL pattern is case-insensitive.

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

<span class="label label-danger">Attention</span> Currently `SHOW JARS` statements only work in the [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) or [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

## SHOW JOBS

```sql
SHOW JOBS
```

Show the jobs in the Flink cluster.

<span class="label label-danger">Attention</span> Currently `SHOW JOBS` statements only work in the [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) or [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}).

{{< top >}}
