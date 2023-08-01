---
title: 时间旅行
type: docs
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

# 时间旅行

{{< label Batch >}} {{< label Streaming >}}

时间旅行是一种用于查询历史数据的 SQL 语法。它允许用户指定一个时间点，查询对应时间点的表的数据，并使用与该时间相对应的 schema。

<span class="label label-danger">注意</span> 目前, `Time Travel` 语法需要相应的 Catalog 支持 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/catalog/Catalog.java" name="getTable(ObjectPath tablePath, long timestamp)" >}} 接口。

```sql
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TIMESTAMP '2023-07-31 00:00:00'
```

## 表达式说明

<span class="label label-danger">注意</span> 时间旅行当前只能够支持部分常量表达式， 不支持使用函数或自定义函数。

### 常量表达式

```sql
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TIMESTAMP '2023-07-31 00:00:00'
```

### 常量表达式加减运算

```sql
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TIMESTAMP '2023-07-31 00:00:00' - INTERVAL '1' DAY
```

### 时间函数或UDF （不支持）

当使用 UDF 或者函数时， 由于当前框架的限制，无法生成一个合法的时间戳，当执行下面的 query 时，会抛异常。

```sql
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TO_TIMESTAMP_LTZ(0, 3)
```

## 时区的处理

默认情况下， TIMESTAMP 表达式生成的数据类型应该是 TIMESTAMP， 而 Catalog 提供的接口是 `getTable(ObjectPath tablePath, long timestamp)`,
因此框架需要将 TIMESTAMP 类型转换为 LONG 类型。 TIMESTAMP 会先转换为 TIMESTAMP_LTZ 类型， 之后再将 TIMESTAMP_LTZ 转换为 LONG 类型。 
因此，同一个 Time Travel 查询语句在不同时区下查询到的结果是不一样的。
