---
title: "DESCRIBE Statements"
weight: 8
type: docs
aliases:
  - /dev/table/sql/describe.html
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

# DESCRIBE Statements

DESCRIBE statements are used to describe the schema of a table or a view.


## Run a DESCRIBE statement

{{< tabs "describe" >}}
{{< tab "Java" >}}
DESCRIBE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns the schema of given table for a successful DESCRIBE operation, otherwise will throw an exception.

The following examples show how to run a DESCRIBE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Scala" >}}
DESCRIBE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns the schema of given table for a successful DESCRIBE operation, otherwise will throw an exception.

The following examples show how to run a DESCRIBE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

DESCRIBE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns the schema of given table for a successful DESCRIBE operation, otherwise will throw an exception.

The following examples show how to run a DESCRIBE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

DESCRIBE statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a DESCRIBE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// register a table named "Orders"
tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl comment 'this is primary key'," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3) comment 'notice: watermark'," +
        " ptime AS PROCTIME() comment 'this is a computed column'," +
        " PRIMARY KEY(`user`) NOT ENFORCED," +
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
        ") with (...)");

// print the schema
tableEnv.executeSql("DESCRIBE Orders").print();

// print the schema
tableEnv.executeSql("DESC Orders").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// register a table named "Orders"
 tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl comment 'this is primary key'," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3) comment 'notice: watermark'," +
        " ptime AS PROCTIME() comment 'this is a computed column'," +
        " PRIMARY KEY(`user`) NOT ENFORCED," +
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
        ") with (...)")

// print the schema
tableEnv.executeSql("DESCRIBE Orders").print()

// print the schema
tableEnv.executeSql("DESC Orders").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# register a table named "Orders"
table_env.execute_sql( \
        "CREATE TABLE Orders (" 
        " `user` BIGINT NOT NULl comment 'this is primary key'," 
        " product VARCHAR(32),"
        " amount INT,"
        " ts TIMESTAMP(3) comment 'notice: watermark',"
        " ptime AS PROCTIME() comment 'this is a computed column',"
        " PRIMARY KEY(`user`) NOT ENFORCED,"
        " WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS"
        ") with (...)");

# print the schema
table_env.execute_sql("DESCRIBE Orders").print()

# print the schema
table_env.execute_sql("DESC Orders").print()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (
>  `user` BIGINT NOT NULl comment 'this is primary key',
>  product VARCHAR(32),
>  amount INT,
>  ts TIMESTAMP(3) comment 'notice: watermark',
>  ptime AS PROCTIME() comment 'this is a computed column',
>  PRIMARY KEY(`user`) NOT ENFORCED,
>  WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
> ) with (
>  ...
> );
[INFO] Table has been created.

Flink SQL> DESCRIBE Orders;

Flink SQL> DESC Orders;
```
{{< /tab >}}
{{< /tabs >}}

The result of the above example is:
{{< tabs "c20da697-b9fc-434b-b7e5-3b51510eee5b" >}}
{{< tab "Java" >}}
```text

+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |                   comment |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |       this is primary key |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |                           |
|  amount |                         INT |  TRUE |           |               |                            |                           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |         notice: watermark |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            | this is a computed column |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
5 rows in set

```
{{< /tab >}}
{{< tab "Scala" >}}
```text

+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |                   comment |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |       this is primary key |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |                           |
|  amount |                         INT |  TRUE |           |               |                            |                           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |         notice: watermark |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            | this is a computed column |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
5 rows in set

```
{{< /tab >}}
{{< tab "Python" >}}
```text

+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    name |                        type |  null |       key |        extras |                  watermark |                   comment |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
|    user |                      BIGINT | FALSE | PRI(user) |               |                            |       this is primary key |
| product |                 VARCHAR(32) |  TRUE |           |               |                            |                           |
|  amount |                         INT |  TRUE |           |               |                            |                           |
|      ts |      TIMESTAMP(3) *ROWTIME* |  TRUE |           |               | `ts` - INTERVAL '1' SECOND |         notice: watermark |
|   ptime | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |           | AS PROCTIME() |                            | this is a computed column |
+---------+-----------------------------+-------+-----------+---------------+----------------------------+---------------------------+
5 rows in set

```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```text

root
 |-- user: BIGINT NOT NULL COMMENT 'this is primary key'
 |-- product: VARCHAR(32)
 |-- amount: INT
 |-- ts: TIMESTAMP(3) *ROWTIME* COMMENT 'notice: watermark'
 |-- ptime: TIMESTAMP(3) NOT NULL *PROCTIME* AS PROCTIME() COMMENT 'this is a computed column'
 |-- WATERMARK FOR ts AS `ts` - INTERVAL '1' SECOND
 |-- CONSTRAINT PK_3599338 PRIMARY KEY (user)

```
{{< /tab >}}
{{< /tabs >}}


{{< top >}}

## Syntax

```sql
{ DESCRIBE | DESC } [catalog_name.][db_name.]table_name
```
