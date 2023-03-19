---
title: "Over聚合"
weight: 9
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

# Over聚合

{{< label Batch >}} {{< label Streaming >}}

`OVER` 聚合通过排序后的范围数据为每行输入计算出聚合值。和 `GROUP BY` 聚合不同， `OVER` 聚合不会把结果通过分组减少到一行，它会为每行输入增加一个聚合值。

下面这个查询为每个订单计算前一个小时之内接收到的同一产品所有订单的总金额。

```sql
SELECT order_id, order_time, amount,
  SUM(amount) OVER (
    PARTITION BY product
    ORDER BY order_time
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
  ) AS one_hour_prod_amount_sum
FROM Orders
```

下面总结了 `OVER` 窗口的语法。

```sql
SELECT
  agg_func(agg_col) OVER (
    [PARTITION BY col1[, col2, ...]]
    ORDER BY time_col
    range_definition),
  ...
FROM ...
```

你可以在一个 `SELECT` 子句中定义多个 `OVER` 窗口聚合。然而，对于流式查询，由于目前的限制，所有聚合的 `OVER` 窗口必须是相同的。

### ORDER BY

`OVER` 窗口需要数据是有序的。因为表没有固定的排序，所以 `ORDER BY` 子句是强制的。对于流式查询，Flink 目前只支持 `OVER` 窗口定义在升序（asc）的 [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}) 上。其他的排序不支持。

### PARTITION BY

`OVER` 窗口可以定义在一个分区表上。`PARTITION BY` 子句代表着每行数据只在其所属的数据分区进行聚合。

### 范围（RANGE）定义

范围（RANGE）定义指定了聚合中包含了多少行数据。范围通过 `BETWEEN` 子句定义上下边界，其内的所有行都会聚合。Flink 只支持 `CURRENT ROW` 作为上边界。

有两种方法可以定义范围：`ROWS` 间隔 和 `RANGE` 间隔

#### RANGE 间隔

`RANGE` 间隔是定义在排序列值上的，在 Flink 里，排序列总是一个时间属性。下面的 `RANG` 间隔定义了聚合会在比当前行的时间属性小 30 分钟的所有行上进行。

```sql
RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
```

#### ROW 间隔

`ROWS` 间隔基于计数。它定义了聚合操作包含的精确行数。下面的 `ROWS` 间隔定义了当前行 + 之前的 10 行（也就是11行）都会被聚合。

```sql
ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
WINDOW
```

`WINDOW` 子句可用于在 `SELECT` 子句之外定义 `OVER` 窗口。它让查询可读性更好，也允许多个聚合共用一个窗口定义。

```sql
SELECT order_id, order_time, amount,
  SUM(amount) OVER w AS sum_amount,
  AVG(amount) OVER w AS avg_amount
FROM Orders
WINDOW w AS (
  PARTITION BY product
  ORDER BY order_time
  RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
```

{{< top >}}
