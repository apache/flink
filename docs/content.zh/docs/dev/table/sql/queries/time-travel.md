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

`时间旅行`语法主要用于查询历史数据。它允许用户指定一个时间点，查询对应时间点 table 的数据。

<span class="label label-danger">注意</span> 目前, `时间旅行`语法需要查询 table 所属的 catalog 实现了 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/catalog/Catalog.java" name="getTable(ObjectPath tablePath, long timestamp)" >}} 接口。
更多详细信息，请参考 [Catalog]({{< ref "docs/dev/table/catalogs" >}}#catalog-中支持时间旅行的接口)。

带有时间旅行子句的语法如下所示：
```sql
SELECT select_list FROM table_name FOR SYSTEM_TIME AS OF timestamp_expression
```

**参数说明：**

- `FOR SYSTEM_TIME AS OF timestamp_expression`：用于特定的时间表达式，用于查询该时间点之前的数据。`timestamp_expression` 用于表示需要查询的时间点。`timestamp_expression` 可以是一个具体的 `TIMESTAMP` 常量或者是可以简化为 `TIMESTAMP` 常量的时间表达式，该表达式只能作用于物理表不能是视图或者子查询。

## 示例
 
```sql
--使用时间常量
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TIMESTAMP '2023-07-31 00:00:00'

--使用可以转换为时间常量的时间表达式
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TIMESTAMP '2023-07-31 00:00:00' - INTERVAL '1' DAY
```

## 限制

<span class="label label-danger">注意</span> `时间旅行` 中使用的时间表达式只支持 `TIMESTAMP` 常量表达式， `TIMESTAMP` 加减运算， 以及部分`内置函数`和 `UDF`

当时间表达式中使用 `UDF` 或者`函数`时， 由于当前框架的限制，部分表达式无法在 SQL 解析时直接换为一个 `TIMESTAMP` 常量，会直接抛出异常。

```sql
--使用无法在 SQL 解析时直接转换为时间常量的时间表达式
SELECT select_list FROM paimon_tb FOR SYSTEM_TIME AS OF TO_TIMESTAMP_LTZ(0, 3)
```

对应的异常如下: 

```bash
Unsupported time travel expression: TO_TIMESTAMP_LTZ(0, 3) for the expression can not be reduced to a constant by Flink.
```

## 时区的处理

`TIMESTAMP` 表达式生成的数据类型是 `TIMESTAMP` 类型，但在时间旅行语句中有一个特殊情况。执行时间旅行语句时，框架会根据`本地时区`将 `TIMESTAMP` 类型转换为 `LONG` 类型。
因此，在不同的时区执行相同的时间旅行查询语句可能会得到不同的结果。
