---
title: "列函数"
weight: 22
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

# 列函数

列函数用于选择或丢弃表的列。

{{< hint info >}}
列函数仅在 Table API 中使用。
{{< /hint >}}

| 语法                      | 描述                         |
| :----------------------- | :--------------------------- |
| withColumns(...)         | 选择指定的列                   |
| withoutColumns(...)      | 选择除指定列以外的列            |
| withAllColumns()    | select all columns (like `SELECT *` in SQL) |

详细语法如下：

```text
列函数:
    withColumns(columnExprs)
    withoutColumns(columnExprs)
    withAllColumns()

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
| withColumns($(*)) | select(withColumns($("*")))  = select($("a"), $("b"), $("c"), $("d"), $("e")) | 全部列 |
| withColumns(m to n) | select(withColumns(range(2, 4))) = select($("b"), $("c"), $("d")) | 第 m 到第 n 列 |
| withColumns(m, n, k)  | select(withColumns(lit(1), lit(3), $("e"))) = select($("a"), $("c"), $("e")) | 第 m、n、k 列 |
| withColumns(m, n to k)  | select(withColumns(lit(1), range(3, 5))) = select($("a"), $("c"), $("d"), $("e")) |  以上两种用法的混合 |
| withoutColumns(m to n) | select(withoutColumns(range(2, 4))) = select($("a"), $("e")) |  不选从第 m 到第 n 列 |
| withoutColumns(m, n, k) | select(withoutColumns(lit(1), lit(3), lit(5))) = select($("b"), $("d")) |  不选第 m、n、k 列 |
| withoutColumns(m, n to k) | select(withoutColumns(lit(1), range(3, 5))) = select($("b")) |  以上两种用法的混合 |

列函数可用于所有需要列字段的地方，例如 `select、groupBy、orderBy、UDFs` 等函数，例如：

{{< tabs "402fe551-5fb9-4b17-bd64-e05cbd56b4cc" >}}
{{< tab "Java" >}}
```java
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns(range("a", "b")), myUDAgg(myUDF(withColumns(range(5, 20)))));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns('a to 'b), myUDAgg(myUDF(withColumns(5 to 20))))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table
    .group_by(with_columns(range_(1, 3)))
    .select(with_columns(range_('a', 'b')), myUDAgg(myUDF(with_columns(range_(5, 20)))))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
