---
title: "分组聚合"
weight: 8
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

# 分组聚合
{{< label Batch >}} {{< label Streaming >}}

像大多数数据系统一样，Apache Flink支持聚合函数；包括内置的和用户定义的。[用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})在使用前必须在目录中注册。

聚合函数把多行输入数据计算为一行结果。例如，有一些聚合函数可以计算一组行的 "COUNT"、"SUM"、"AVG"（平均）、"MAX"（最大）和 "MIN"（最小）。

```sql
SELECT COUNT(*) FROM Orders
```

对于流式查询，重要的是要理解 Flink 运行的是连续查询，永远不会终止。而且它们会根据其输入表的更新来更新其结果表。对于上述查询，每当有新行插入 `Orders` 表时，Flink 都会实时计算并输出更新后的结果。

Apache Flink 支持标准的 `GROUP BY` 子句来聚合数据。

```sql
SELECT COUNT(*)
FROM Orders
GROUP BY order_id
```

对于流式查询，用于计算查询结果的状态可能无限膨胀。状态的大小取决于分组的数量以及聚合函数的数量和类型。例如：`MIN`/`MAX` 的状态是重量级的，`COUNT` 是轻量级的。可以提供一个合适的状态 time-to-live (TTL) 配置来防止状态过大。注意：这可能会影响查询结果的正确性。详情参见：[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)。

Flink 对于分组聚合提供了一系列性能优化的方法。更多参见：[性能优化]({{< ref "docs/dev/table/tuning" >}})。

## DISTINCT 聚合

DISTINCT 聚合在聚合函数前去掉重复的数据。下面的示例计算 Orders 表中不同 order_ids 的数量，而不是总行数。

```sql
SELECT COUNT(DISTINCT order_id) FROM Orders
```

对于流式查询，用于计算查询结果的状态可能无限膨胀。状态的大小大多数情况下取决于去重行的数量和分组持续的时间，持续时间较短的 group 窗口不会产生状态过大的问题。可以提供一个合适的状态 time-to-live (TTL) 配置来防止状态过大。注意：这可能会影响查询结果的正确性。详情参见：[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)。

## GROUPING SETS

Grouping Sets 可以通过一个标准的 `GROUP BY` 语句来描述更复杂的分组操作。数据按每个指定的 Grouping Sets 分别分组，并像简单的 `group by` 子句一样为每个组进行聚合。

```sql
SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())
```

结果：

```
+-------------+--------+-------+
| supplier_id | rating | total |
+-------------+--------+-------+
|   supplier1 |      4 |     1 |
|   supplier1 | (NULL) |     2 |
|      (NULL) | (NULL) |     4 |
|   supplier1 |      3 |     1 |
|   supplier2 |      3 |     1 |
|   supplier2 | (NULL) |     2 |
|   supplier2 |      4 |     1 |
+-------------+--------+-------+
```

`GROUPING SETS` 的每个子列表可以是：空的，多列或表达式，它们的解释方式和直接使用 `GROUP BY` 子句是一样的。一个空的 Grouping Sets 表示所有行都聚合在一个分组下，即使没有数据，也会输出结果。

对于 Grouping Sets 中的空子列表，结果数据中的分组或表达式列会用`NULL`代替。例如，上例中的 `GROUPING SETS ((supplier_id), ())` 里的 `()` 就是空子列表，与其对应的结果数据中的 `supplier_id` 列使用 `NULL` 填充。

对于流式查询，用于计算查询结果的状态可能无限膨胀。状态的大小取决于 Grouping Sets 的数量以及聚合函数的类型。可以提供一个合适的状态 time-to-live (TTL)配置来防止状态过大.注意:这可能会影响查询结果的正确性.详情参见：[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)。

### ROLLUP

`ROLLUP` 是一种特定通用类型 Grouping Sets 的简写。代表着指定表达式和所有前缀的列表，包括空列表。

例如：下面这个查询和上个例子是等效的。

```sql
SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY ROLLUP (supplier_id, rating)
```

### CUBE

`CUBE` 是一种特定通用类型 Grouping Sets 的简写。代表着指定列表以及所有可能的子集和幂集。

例如：下面两个查询是等效的。

```sql
SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY CUBE (supplier_id, rating, product_id)

SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SET (
    ( supplier_id, product_id, rating ),
    ( supplier_id, product_id         ),
    ( supplier_id,             rating ),
    ( supplier_id                     ),
    (              product_id, rating ),
    (              product_id         ),
    (                          rating ),
    (                                 )
)
```

## HAVING

`HAVING` 会删除 group 后不符合条件的行。 `HAVING` 和 `WHERE` 的不同点：`WHERE` 在 `GROUP BY` 之前过滤单独的数据行。`HAVING` 过滤 `GROUP BY` 生成的数据行。 `HAVING` 条件中的每一列引用必须是明确的 grouping 列，除非它出现在聚合函数中。

```sql
SELECT SUM(amount)
FROM Orders
GROUP BY users
HAVING SUM(amount) > 50
```

即使没有 `GROUP BY` 子句，`HAVING` 的存在也会使查询变成一个分组查询。这与查询包含聚合函数但没有 `GROUP BY` 子句时的情况相同。查询认为所有被选中的行形成一个单一的组，并且 `SELECT` 列表和 `HAVING` 子句只能从聚合函数中引用列。如果 `HAVING` 条件为真，这样的查询将发出一条记录，如果不为真，则发出零条记录。

{{< top >}}
