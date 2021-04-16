---
title: "窗口 Top-N"
weight: 15
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

# Window Top-N
{{< label Streaming >}}

Window Top-N is a special [Top-N]({{< ref "docs/dev/table/sql/queries/topn" >}}) which returns the N smallest or largest values for each window and other partitioned keys.

For streaming queries, unlike regular Top-N on continuous tables, window Top-N do not emit intermediate results but only a final result, the total top N records at the end of the window. Moreover, window Top-N purge all intermediate state when no longer needed.
Therefore, window Top-N queries have better performance if users don't need results updated per record. Usually, Window Top-N is used with [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}) together.

Window Top-N can be defined in the same syntax as regular Top-N, see [Top-N documentation]({{< ref "docs/dev/table/sql/queries/topn" >}}) for more information.
Besides that, Window Top-N requires the `PARTITION BY` clause contains `window_start` and `window_end` columns of the relation applied [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) or [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}).
Otherwise the optimizer won’t be able to translate the query.


The following shows the syntax of the Window Top-N statement:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name) -- relation applied windowing TVF
WHERE rownum <= N [AND conditions]
```

## Example

The following example shows how to calculate Top 3 suppliers who have the highest sales for every tumbling 10 minutes window.

```sql
-- Note: the bidtime column must have been defined as time attribute
> SELECT * FROM Bid;
--+---------+-------+------+-------------+
--| bidtime | price | item | supplier_id |
--+---------+-------+------+-------------+
--|    8:05 |     4 |    A |   supplier1 |
--|    8:06 |     4 |    C |   supplier2 |
--|    8:07 |     2 |    G |   supplier1 |
--|    8:08 |     2 |    B |   supplier3 |
--|    8:09 |     5 |    D |   supplier4 |
--|    8:11 |     2 |    B |   supplier3 |
--|    8:13 |     1 |    E |   supplier1 |
--|    8:15 |     3 |    H |   supplier2 |
--|    8:17 |     6 |    F |   supplier5 |
--+---------+-------+------+-------------+

> SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
    FROM (
      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt
      FROM TABLE(
        TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
      GROUP BY window_start, window_end, supplier_id
    )
  ) WHERE rownum <= 3;
--+--------------+------------+-------------+-------+-----+--------+
--| window_start | window_end | supplier_id | price | cnt | rownum |
--+--------------+------------+-------------+-------+-----+--------+
--|         8:00 |       8:10 |   supplier1 |     6 |   2 |      1 |
--|         8:00 |       8:10 |   supplier4 |     5 |   1 |      2 |
--|         8:00 |       8:10 |   supplier2 |     4 |   1 |      3 |
--|         8:10 |       8:20 |   supplier5 |     6 |   1 |      1 |
--|         8:10 |       8:20 |   supplier2 |     3 |   1 |      2 |
--|         8:10 |       8:20 |   supplier3 |     2 |   1 |      3 |
--+--------------+------------+-------------+-------+-----+--------+
```

## Limitation

Currently, Flink only supports Window Top-N which follows after [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}). Window Top-N after [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) will be support in the near future.


{{< top >}}
