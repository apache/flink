---
title: "ALTER Statements"
weight: 6
type: docs
aliases:
  - /dev/table/sql/alter.html
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

# ALTER Statements



ALTER statements are used to modified a registered table/view/function definition in the [Catalog]({{< ref "docs/dev/table/catalogs" >}}).

Flink SQL supports the following ALTER statements for now:

- ALTER TABLE
- ALTER VIEW
- ALTER DATABASE
- ALTER FUNCTION

## Run an ALTER statement

{{< tabs "alter" >}}
{{< tab "Java" >}}
ALTER statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful ALTER operation, otherwise will throw an exception.

The following examples show how to run an ALTER statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Scala" >}}
ALTER statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful ALTER operation, otherwise will throw an exception.

The following examples show how to run an ALTER statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

ALTER statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns 'OK' for a successful ALTER operation, otherwise will throw an exception.

The following examples show how to run an ALTER statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

ALTER statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run an ALTER statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a01e970b-5de2-44de-b5d0-595b53682c64" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// a string array: ["Orders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();

// add a new column `order` to the first position
tableEnv.executeSql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST");

// add more columns, primary key and watermark
tableEnv.executeSql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)");

// modify column type, column comment and watermark
tableEnv.executeSql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)");

// drop watermark
tableEnv.executeSql("ALTER TABLE Orders DROP WATERMARK");

// drop column
tableEnv.executeSql("ALTER TABLE Orders DROP (amount, ts, category)");

// rename column
tableEnv.executeSql("ALTER TABLE Orders RENAME `order` TO order_id");

// rename "Orders" to "NewOrders"
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders");

// a string array: ["NewOrders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// add a new column `order` to the first position
tableEnv.executeSql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST")

// add more columns, primary key and watermark
tableEnv.executeSql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)")

// modify column type, column comment and watermark
tableEnv.executeSql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)")

// drop watermark
tableEnv.executeSql("ALTER TABLE Orders DROP WATERMARK")

// drop column
tableEnv.executeSql("ALTER TABLE Orders DROP (amount, ts, category)")

// rename column
tableEnv.executeSql("ALTER TABLE Orders RENAME `order` TO order_id")

// a string array: ["Orders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()

// rename "Orders" to "NewOrders"
tableEnv.executeSql("ALTER TABLE Orders RENAME TO NewOrders")

// a string array: ["NewOrders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# a string array: ["Orders"]
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()

# add a new column `order` to the first position
table_env.execute_sql("ALTER TABLE Orders ADD `order` INT COMMENT 'order identifier' FIRST");

# add more columns, primary key and watermark
table_env.execute_sql("ALTER TABLE Orders ADD (ts TIMESTAMP(3), category STRING AFTER product, PRIMARY KEY(`order`) NOT ENFORCED, WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)");

# modify column type, column comment and watermark
table_env.execute_sql("ALTER TABLE Orders MODIFY (amount DOUBLE NOT NULL, category STRING COMMENT 'category identifier' AFTER `order`, WATERMARK FOR ts AS ts)");

# drop watermark
table_env.execute_sql("ALTER TABLE Orders DROP WATERMARK");

# drop column
table_env.execute_sql("ALTER TABLE Orders DROP (amount, ts, category)");

# rename column
table_env.execute_sql("ALTER TABLE Orders RENAME `order` TO order_id");

# rename "Orders" to "NewOrders"
table_env.execute_sql("ALTER TABLE Orders RENAME TO NewOrders");

# a string array: ["NewOrders"]
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

The following grammar gives an overview about the available syntax:
```text
ALTER TABLE [IF EXISTS] table_name {
    ADD { <schema_component> | (<schema_component> [, ...]) | [IF NOT EXISTS] <partition_component> [<partition_component> ...]}
  | MODIFY { <schema_component> | (<schema_component> [, ...]) }
  | DROP {column_name | (column_name, column_name, ....) | PRIMARY KEY | CONSTRAINT constraint_name | WATERMARK | [IF EXISTS] <partition_component> [, ...]}
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
  
<partition_component>:
  PARTITION (key1=val1, key2=val2, ...) [WITH (key1=val1, key2=val2, ...)]
```

**IF EXISTS**

If the table does not exist, nothing happens.

### ADD
Use `ADD` clause to add [columns]({{< ref "docs/dev/table/sql/create" >}}#columns), [constraints]({{< ref "docs/dev/table/sql/create" >}}#primary-key), [watermark]({{< ref "docs/dev/table/sql/create" >}}#watermark) and [partitions]({{< ref "docs/dev/table/sql/create" >}}#partitioned-by) to an existing table. 

To add a column at the specified position, use `FIRST` or `AFTER col_name`. By default, the column is appended at last.

The following examples illustrate the usage of the `ADD` statements.

```sql
-- add a new column 
ALTER TABLE MyTable ADD category_id STRING COMMENT 'identifier of the category';

-- add columns, constraint, and watermark
ALTER TABLE MyTable ADD (
    log_ts STRING COMMENT 'log timestamp string' FIRST,
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    PRIMARY KEY (id) NOT ENFORCED,
    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
);

-- add a new partition 
ALTER TABLE MyTable ADD PARTITION (p1=1,p2='a') with ('k1'='v1');

-- add two new partitions
ALTER TABLE MyTable ADD PARTITION (p1=1,p2='a') with ('k1'='v1') PARTITION (p1=1,p2='b') with ('k2'='v2');
```
<span class="label label-danger">Note</span> Add a column to be primary key will change the column's nullability to false implicitly.

### MODIFY
Use `MODIFY` clause to change column's position, type, comment or nullability, change primary key columns and watermark strategy to an existing table. 

To modify an existent column to a new position, use `FIRST` or `AFTER col_name`. By default, the position remains unchanged. 

The following examples illustrate the usage of the `MODIFY` statements.

```sql
-- modify a column type, comment and position
ALTER TABLE MyTable MODIFY measurement double COMMENT 'unit is bytes per second' AFTER `id`;

-- modify definition of column log_ts and ts, primary key, watermark. They must exist in table schema
ALTER TABLE MyTable MODIFY (
    log_ts STRING COMMENT 'log timestamp string' AFTER `id`,  -- reorder columns
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    PRIMARY KEY (id) NOT ENFORCED,
    WATERMARK FOR ts AS ts -- modify watermark strategy
);
```

<span class="label label-danger">Note</span> Modify a column to be primary key will change the column's nullability to false implicitly.

### DROP
Use the `DROP` clause to drop columns, primary key, partitions, and watermark strategy to an existing table.

The following examples illustrate the usage of the `DROP` statements.

```sql
-- drop a column
ALTER TABLE MyTable DROP measurement;

-- drop columns
ALTER TABLE MyTable DROP (col1, col2, col3);

-- drop primary key
ALTER TABLE MyTable DROP PRIMARY KEY;

-- drop a partition
ALTER TABLE MyTable DROP PARTITION (`id` = 1);

-- drop two partitions
ALTER TABLE MyTable DROP PARTITION (`id` = 1), PARTITION (`id` = 2);

-- drop a watermark
ALTER TABLE MyTable DROP WATERMARK;
```

### RENAME
Use `RENAME` clause to rename column or an existing table.

The following examples illustrate the usage of the `RENAME` statements.
```sql
-- rename column
ALTER TABLE MyTable RENAME request_body TO payload;

-- rename table
ALTER TABLE MyTable RENAME TO MyTable2;
```

### SET

Set one or more properties in the specified table. If a particular property is already set in the table, override the old value with the new one.

The following examples illustrate the usage of the `SET` statements.

```sql
-- set 'rows-per-second'
ALTER TABLE DataGenSource SET ('rows-per-second' = '10');
```

### RESET

Reset one or more properties to its default value.

The following examples illustrate the usage of the `RESET` statements.

```sql
-- reset 'rows-per-second' to the default value
ALTER TABLE DataGenSource RESET ('rows-per-second');
```

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

Set one or more properties in the specified database. If a particular property is already set in the database, override the old value with the new one.

{{< top >}}

## ALTER FUNCTION

```sql
ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION 
  [IF EXISTS] [catalog_name.][db_name.]function_name 
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
```

Alter a catalog function with the new identifier and optional language tag. If a function doesn't exist in the catalog, an exception is thrown.

If the language tag is JAVA/SCALA, the identifier is the full classpath of the UDF. For the implementation of Java/Scala UDF, please refer to [User-defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}) for more details.

If the language tag is PYTHON, the identifier is the fully qualified name of the UDF, e.g. `pyflink.table.tests.test_udf.add`. For the implementation of Python UDF, please refer to [Python UDFs]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}) for more details.

**TEMPORARY**

Alter temporary catalog function that has catalog and database namespaces and overrides catalog functions.

**TEMPORARY SYSTEM**

Alter temporary system function that has no namespace and overrides built-in functions

**IF EXISTS**

If the function doesn't exist, nothing happens.

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag to instruct flink runtime how to execute the function. Currently only JAVA, SCALA and PYTHON are supported, the default language for a function is JAVA.

