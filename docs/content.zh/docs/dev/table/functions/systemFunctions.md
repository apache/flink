---
title: "系统（内置）函数"
weight: 32
type: docs
aliases:
  - /zh/dev/table/functions/systemFunctions.html
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

# 系统（内置）函数

Flink Table API & SQL 为用户提供了一组内置的数据转换函数。本页简要介绍了它们。如果你需要的函数尚不支持，你可以实现
[用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})。如果你觉得这个函数够通用，请
<a href="https://issues.apache.org/jira/secure/CreateIssue!default.jspa">创建一个 Jira issue</a>并详细
说明。

标量函数
----------------

标量函数将零、一个或多个值作为输入并返回单个值作为结果。

### 比较函数

{{< sql_functions_zh "comparison" >}}

### 逻辑函数

{{< sql_functions_zh "logical" >}}

### 算术函数

{{< sql_functions_zh "arithmetic" >}}

### 字符串函数

{{< sql_functions_zh "string" >}}

### 时间函数

{{< sql_functions_zh "temporal" >}}

### 条件函数

{{< sql_functions_zh "conditional" >}}

### 类型转换函数

{{< sql_functions_zh "conversion" >}}

### 集合函数

{{< sql_functions_zh "collection" >}}

### JSON Functions

JSON functions make use of JSON path expressions as described in ISO/IEC TR 19075-6 of the SQL
standard. Their syntax is inspired by and adopts many features of ECMAScript, but is neither a
subset nor superset thereof.

Path expressions come in two flavors, lax and strict. When omitted, it defaults to the strict mode.
Strict mode is intended to examine data from a schema perspective and will throw errors whenever
data does not adhere to the path expression. However, functions like `JSON_VALUE` allow defining
fallback behavior if an error is encountered. Lax mode, on the other hand, is more forgiving and
converts errors to empty sequences.

The special character `$` denotes the root node in a JSON path. Paths can access properties (`$.a`),
array elements (`$.a[0].b`), or branch over all elements in an array (`$.a[*].b`).

Known Limitations:
* Not all features of Lax mode are currently supported correctly. This is an upstream bug
  (CALCITE-4717). Non-standard behavior is not guaranteed.

{{< sql_functions "json" >}}

### 值构建函数

{{< sql_functions_zh "valueconstruction" >}}

### 值获取函数

{{< sql_functions_zh "valueaccess" >}}

### 分组函数

{{< sql_functions_zh "grouping" >}}

### 哈希函数

{{< sql_functions_zh "hashfunctions" >}}

### 辅助函数

{{< sql_functions_zh "auxiliary" >}}

聚合函数
-------------------

聚合函数将所有的行作为输入，并返回单个聚合值作为结果。

{{< sql_functions_zh "aggregate" >}}

时间间隔单位和时间点单位标识符
---------------------------------------

下表列出了时间间隔单位和时间点单位标识符。

对于 Table API，请使用 `_` 代替空格（例如 `DAY_TO_HOUR`）。

| 时间间隔单位                | 时间点单位                        |
| :------------------------ | :------------------------------ |
| `MILLENIUM` _（仅适用SQL）_ |                                 |
| `CENTURY` _（仅适用SQL）_   |                                 |
| `YEAR`                    | `YEAR`                          |
| `YEAR TO MONTH`           |                                 |
| `QUARTER`                 | `QUARTER`                       |
| `MONTH`                   | `MONTH`                         |
| `WEEK`                    | `WEEK`                          |
| `DAY`                     | `DAY`                           |
| `DAY TO HOUR`             |                                 |
| `DAY TO MINUTE`           |                                 |
| `DAY TO SECOND`           |                                 |
| `HOUR`                    | `HOUR`                          |
| `HOUR TO MINUTE`          |                                 |
| `HOUR TO SECOND`          |                                 |
| `MINUTE`                  | `MINUTE`                        |
| `MINUTE TO SECOND`        |                                 |
| `SECOND`                  | `SECOND`                        |
|                           | `MILLISECOND`                   |
|                           | `MICROSECOND`                   |
| `DOY` _（仅适用SQL）_       |                                 |
| `DOW` _（仅适用SQL）_       |                                 |
|                           | `SQL_TSI_YEAR` _（仅适用SQL）_    |
|                           | `SQL_TSI_QUARTER` _（仅适用SQL）_ |
|                           | `SQL_TSI_MONTH` _（仅适用SQL）_   |
|                           | `SQL_TSI_WEEK` _（仅适用SQL）_    |
|                           | `SQL_TSI_DAY` _（仅适用SQL）_     |
|                           | `SQL_TSI_HOUR` _（仅适用SQL）_    |
|                           | `SQL_TSI_MINUTE` _（仅适用SQL）_  |
|                           | `SQL_TSI_SECOND ` _（仅适用SQL）_ |

{{< top >}}

列函数
---------------------------------------

列函数用于选择或丢弃表的列。

{{< hint info >}}
列函数仅在 Table API 中使用。
{{< /hint >}}

| 语法                      | 描述                         |
| :----------------------- | :--------------------------- |
| withColumns(...)         | 选择指定的列                   |
| withoutColumns(...)      | 选择除指定列以外的列            |

详细语法如下：

```text
列函数:
    withColumns(columnExprs)
    withoutColumns(columnExprs)

多列表达式:
    columnExpr [, columnExpr]*

单列表达式:
    columnRef | columnIndex to columnIndex | columnName to columnName

列引用:
    columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)
```
列函数的用法如下表所示（假设我们有一个包含 5 列的表：`(a: Int, b: Long, c: String, d:String, e: String)`）：

| 接口 | 用法举例 | 描述 |
|-|-|-|
| withColumns(*)| select("withColumns(*)")  = select("a, b, c, d, e") | 全部列 |
| withColumns(m to n) | select("withColumns(2 to 4)") = select("b, c, d") | 第 m 到第 n 列 |
|  withColumns(m, n, k)  | select("withColumns(1, 3, e)") = select("a, c, e") | 第 m、n、k 列 |
|  withColumns(m, n to k)  | select("withColumns(1, 3 to 5)") = select("a, c, d ,e") |  以上两种用法的混合 |
|  withoutColumns(m to n) | select("withoutColumns(2 to 4)") = select("a, e") |  不选从第 m 到第 n 列 |
|  withoutColumns(m, n, k) | select("withoutColumns(1, 3, 5)") = select("b, d") |  不选第 m、n、k 列 |
|  withoutColumns(m, n to k) | select("withoutColumns(1, 3 to 5)") = select("b") |  以上两种用法的混合 |

列函数可用于所有需要列字段的地方，例如 `select、groupBy、orderBy、UDFs` 等函数，例如：

{{< tabs "402fe551-5fb9-4b17-bd64-e05cbd56b4cc" >}}
{{< tab "Java" >}}
```java
table
   .groupBy("withColumns(1 to 3)")
   .select("withColumns(a to b), myUDAgg(myUDF(withColumns(5 to 20)))")
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table
   .groupBy(withColumns(1 to 3))
   .select(withColumns('a to 'b), myUDAgg(myUDF(withColumns(5 to 20))))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table \
    .group_by("withColumns(1 to 3)") \
    .select("withColumns(a to b), myUDAgg(myUDF(withColumns(5 to 20)))")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
