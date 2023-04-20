---
title: "DESCRIBE 语句"
weight: 8
type: docs
aliases:
  - /zh/dev/table/sql/describe.html
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

<a name="describe-statements"></a>

# DESCRIBE 语句

DESCRIBE 语句用于描述表或视图的 schema。

<a name="run-a-describe-statement"></a>

## 执行 DESCRIBE 语句

{{< tabs "describe" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 DESCRIBE 语句。如果 DESCRIBE 操作执行成功，`executeSql()` 方法会返回给定表的 schema，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 DESCRIBE 语句。
{{< /tab >}}
{{< tab "Scala" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 DESCRIBE 语句。如果 DESCRIBE 操作执行成功，`executeSql()` 方法会返回给定表的 schema，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 DESCRIBE 语句。
{{< /tab >}}
{{< tab "Python" >}}
可以使用 `TableEnvironment` 的 `execute_sql()` 方法执行 DESCRIBE 语句。如果 DESCRIBE 操作执行成功，`execute_sql()` 方法会返回给定表的 schema，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 DESCRIBE 语句。
{{< /tab >}}
{{< tab "SQL CLI" >}}

DESCRIBE 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 DESCRIBE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// 注册名为 “Orders” 的表
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

// 打印 schema
tableEnv.executeSql("DESCRIBE Orders").print();

// 打印 schema
tableEnv.executeSql("DESC Orders").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// 注册名为 “Orders” 的表
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

// 打印 schema
tableEnv.executeSql("DESCRIBE Orders").print()

// 打印 schema
tableEnv.executeSql("DESC Orders").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 注册名为 “Orders” 的表
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

# 打印 schema
table_env.execute_sql("DESCRIBE Orders").print()

# 打印 schema
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

上述示例的结果是：
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

<a name="syntax"></a>

## 语法

```sql
{ DESCRIBE | DESC } [catalog_name.][db_name.]table_name
```
