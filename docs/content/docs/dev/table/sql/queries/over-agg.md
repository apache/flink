---
title: "Over Aggregation"
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

# Over Aggregation
{{< label Batch >}} {{< label Streaming >}}

`OVER` aggregates compute an aggregated value for every input row over a range of ordered rows. In contrast to `GROUP BY` aggregates, `OVER` aggregates do not reduce the number of result rows to a single row for every group. Instead `OVER` aggregates produce an aggregated value for every input row.

The following query computes for every order the sum of amounts of all orders for the same product that were received within one hour before the current order.

```sql
SELECT order_id, order_time, amount,
  SUM(amount) OVER (
    PARTITION BY product
    ORDER BY order_time
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
  ) AS one_hour_prod_amount_sum
FROM Orders
```

The syntax for an `OVER` window is summarized below.

```sql
SELECT
  agg_func(agg_col) OVER (
    [PARTITION BY col1[, col2, ...]]
    ORDER BY time_col
    range_definition),
  ...
FROM ...
```

You can define multiple `OVER` window aggregates in a `SELECT` clause. However, for streaming queries, the `OVER` windows for all aggregates must be identical due to current limitation.


### ORDER BY

`OVER` windows are defined on an ordered sequence of rows. Since tables do not have an inherent order, the `ORDER BY` clause is mandatory. For streaming queries, Flink currently only supports `OVER` windows that are defined with an ascending [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) order. Additional orderings are not supported.

### PARTITION BY

`OVER` windows can be defined on a partitioned table. In presence of a `PARTITION BY` clause, the aggregate is computed for each input row only over the rows of its partition.

### Range Definitions

The range definition specifies how many rows are included in the aggregate. The range is defined with a `BETWEEN` clause that defines a lower and an upper boundary. All rows between these boundaries are included in the aggregate. Flink only supports `CURRENT ROW` as the upper boundary.

There are two options to define the range, `ROWS` intervals and `RANGE` intervals.

#### RANGE intervals

A `RANGE` interval is defined on the values of the ORDER BY column, which is in case of Flink always a time attribute. The following RANGE interval defines that all rows with a time attribute of at most 30 minutes less than the current row are included in the aggregate.

```sql
RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
```

#### ROW intervals

A `ROWS` interval is a count-based interval. It defines exactly how many rows are included in the aggregate. The following `ROWS` interval defines that the 10 rows preceding the current row and the current row (so 11 rows in total) are included in the aggregate.

```sql
ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
WINDOW
```

The `WINDOW` clause can be used to define an `OVER` window outside of the `SELECT` clause. It can make queries more readable and also allows us to reuse the window definition for multiple aggregates.

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
