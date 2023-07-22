---
title: "TRUNCATE TABLE 语句"
weight: 8
type: docs
aliases:
- /zh/dev/table/sql/truncate.html
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

<a name="truncate-statements"></a>

# TRUNCATE TABLE 语句

DESCRIBE 语句用于删除表中的全部数据，但不会删除表本身。TRUNCATE TABLE 语句目前仅支持使用在Batch模式。

<a name="run-a-truncate-statement"></a>

## 执行 TRUNCATE TABLE 语句

{{< tabs "truncate" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 TRUNCATE TABLE 语句。如果 TRUNCATE TABLE 操作执行成功，`executeSql()` 方法不会有异常返回，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 TRUNCATE TABLE 语句。
{{< /tab >}}
{{< tab "SQL CLI" >}}

TRUNCATE TABLE 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 TRUNCATE TABLE 语句。

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

// 清理 Orders 表的数据
tableEnv.executeSql("TRUNCATE TABLE Orders");
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

Flink SQL> TRUNCATE TABLE Orders;
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

<a name="syntax"></a>

## 语法

```sql
TRUNCATE TABLE [catalog_name.][db_name.]table_name
```
