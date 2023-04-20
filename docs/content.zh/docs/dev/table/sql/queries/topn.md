---
title: "Top-N"
weight: 14
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

# Top-N
{{< label Batch >}} {{< label Streaming >}}

Top-N 查询可以根据指定列排序后获得前 N 个最小或最大值。最小值和最大值集都被认为是Top-N查询。在需要从批表或流表中仅显示 N 个底部或 N 个顶部记录时，Top-N 查询是非常有用的。并且该结果集还可用于进一步分析。

Flink 使用 `OVER` 窗口子句和过滤条件的组合来表达一个 Top-N 查询。借助 `OVER` 窗口的 `PARTITION BY` 子句能力，Flink 也能支持分组 Top-N。例如：实时显示每个分类下销售额最高的五个产品。对于批处理和流处理模式的SQL，都支持 Top-N 查询。

下面展示了 Top-N 的语法：

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]
```

**参数说明：**
- `ROW_NUMBER()`：根据分区数据的排序，为每一行分配一个唯一且连续的序号，从 1 开始。目前，只支持 `ROW_NUMBER` 作为 `OVER` 窗口函数。未来会支持 `RANK()` 和 `DENSE_RANK()`。
- `PARTITION BY col1[, col2...]`：指定分区字段。每个分区都会有一个 Top-N 的结果。
- `ORDER BY col1 [asc|desc][, col2 [asc|desc]...]`： 指定排序列。 每个列的排序类型（ASC/DESC）可以不同。
- `WHERE rownum <= N`: Flink 需要 `rownum <= N` 才能识别此查询是 Top-N 查询。 N 表示将要保留 N 个最大或最小数据。
- `[AND conditions]`: 可以在 `WHERE` 子句中添加其他条件，但是这些其他条件和 `rownum <= N` 需要使用 `AND` 结合。

{{< hint info >}}

注意： 必须严格遵循上述模式，否则优化器无法翻译查询。

{{< /hint >}}

{{< hint info >}}

Top-N 查询是<span class="label label-info">结果更新</span>的. Flink SQL会根据`ORDER BY`的字段对输入的数据流进行排序，所以如果前 N 条记录发生了变化，那么变化后的记录将作为回撤/更新记录发送到下游。
建议使用一个支持更新的存储作为 Top-N 查询的结果表。此外，如果 Top-N 条记录需要存储在外部存储中，结果表应该与Top-N查询的唯一键保持一致。

{{< /hint >}}

Top-N 查询的唯一键是分区字段和 rownum 字段的组合。Top-N 查询也可以获取上游的唯一键。用下面的 job 举例:比如 `product_id` 是 `ShopSales` 的唯一键，这时 Top-N 查询的唯一键是[`category`, `rownum`] 和 [`product_id`]。

下面的示例展示了在流式表上指定 Top-N SQL 查询。这也是上面提到的 '实时显示每个分类下销售额最高的五个产品' 的示例。

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```

#### 无排名输出优化

如上所述， `rownum` 将作为唯一键的一个字段写入到结果表，这可能会导致大量数据写入到结果表。例如，排名第九（比如 `product-1001`）的记录更新为 1，排名 1 到 9 的所有记录都会作为更新信息逐条写入到结果表。如果结果表收到太多的数据，它将会成为这个 SQL 任务的瓶颈。

优化的方法是在 Top-N 查询的外层 `SELECT` 子句中省略 `rownum` 字段。因为通常 Top-N 的数据量不大，消费端就可以快速地排序。下面的示例中就没有 `rownum` 字段，只需要发送变更数据（`product-1001`）到下游，这样可以减少结果表很多 IO。

下面的示例展示了用这种方法怎样去优化上面的 Top-N：

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

-- omit row_num field from the output
SELECT product_id, category, product_name, sales
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```
<span class="label label-danger">Attention in Streaming Mode</span> 为了上面的查询输出到外部存储的正确性，外部存储必须和 Top-N 查询拥有相同的唯一键。在上面的示例中，如果 `product_id` 是查询的唯一键，外部表应该也把 `product_id` 作为唯一键。

{{< top >}}
