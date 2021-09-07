---
title: "Window Top-N"
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

For streaming queries, unlike regular Top-N on continuous tables, window Top-N does not emit intermediate results but only a final result, the total top N records at the end of the window. Moreover, window Top-N purges all intermediate state when no longer needed.
Therefore, window Top-N queries have better performance if users don't need results updated per record. Usually, Window Top-N is used with [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) directly. Besides, Window Top-N could be used with other operations based on [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}), such as [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}), [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}) and [Window Join]({{< ref "docs/dev/table/sql/queries/window-join">}}). 

Window Top-N can be defined in the same syntax as regular Top-N, see [Top-N documentation]({{< ref "docs/dev/table/sql/queries/topn" >}}) for more information.
Besides that, Window Top-N requires the `PARTITION BY` clause contains `window_start` and `window_end` columns of the relation applied [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) or [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}).
Otherwise, the optimizer won’t be able to translate the query.


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

### Window Top-N follows after Window Aggregation

The following example shows how to calculate Top 3 suppliers who have the highest sales for every tumbling 10 minutes window.

```sql
-- tables must have time attribute, e.g. `bidtime` in this table
Flink SQL> desc Bid;
+-------------+------------------------+------+-----+--------+---------------------------------+
|        name |                   type | null | key | extras |                       watermark |
+-------------+------------------------+------+-----+--------+---------------------------------+
|     bidtime | TIMESTAMP(3) *ROWTIME* | true |     |        | `bidtime` - INTERVAL '1' SECOND |
|       price |         DECIMAL(10, 2) | true |     |        |                                 |
|        item |                 STRING | true |     |        |                                 |
| supplier_id |                 STRING | true |     |        |                                 |
+-------------+------------------------+------+-----+--------+---------------------------------+

Flink SQL> SELECT * FROM Bid;
+------------------+-------+------+-------------+
|          bidtime | price | item | supplier_id |
+------------------+-------+------+-------------+
| 2020-04-15 08:05 |  4.00 |    A |   supplier1 |
| 2020-04-15 08:06 |  4.00 |    C |   supplier2 |
| 2020-04-15 08:07 |  2.00 |    G |   supplier1 |
| 2020-04-15 08:08 |  2.00 |    B |   supplier3 |
| 2020-04-15 08:09 |  5.00 |    D |   supplier4 |
| 2020-04-15 08:11 |  2.00 |    B |   supplier3 |
| 2020-04-15 08:13 |  1.00 |    E |   supplier1 |
| 2020-04-15 08:15 |  3.00 |    H |   supplier2 |
| 2020-04-15 08:17 |  6.00 |    F |   supplier5 |
+------------------+-------+------+-------------+

Flink SQL> SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
    FROM (
      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt
      FROM TABLE(
        TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
      GROUP BY window_start, window_end, supplier_id
    )
  ) WHERE rownum <= 3;
+------------------+------------------+-------------+-------+-----+--------+
|     window_start |       window_end | supplier_id | price | cnt | rownum |
+------------------+------------------+-------------+-------+-----+--------+
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier1 |  6.00 |   2 |      1 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier4 |  5.00 |   1 |      2 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier2 |  4.00 |   1 |      3 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier5 |  6.00 |   1 |      1 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier2 |  3.00 |   1 |      2 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier3 |  2.00 |   1 |      3 |
+------------------+------------------+-------------+-------+-----+--------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*

### Window Top-N follows after Windowing TVF

The following example shows how to calculate Top 3 items which have the highest price for every tumbling 10 minutes window.

```sql
Flink SQL> SELECT *
  FROM (
    SELECT bidtime, price, item, supplier_id, window_start, window_end, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
    FROM TABLE(
               TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  ) WHERE rownum <= 3;
+------------------+-------+------+-------------+------------------+------------------+--------+
|          bidtime | price | item | supplier_id |     window_start |       window_end | rownum |
+------------------+-------+------+-------------+------------------+------------------+--------+
| 2020-04-15 08:05 |  4.00 |    A |   supplier1 | 2020-04-15 08:00 | 2020-04-15 08:10 |      2 |
| 2020-04-15 08:06 |  4.00 |    C |   supplier2 | 2020-04-15 08:00 | 2020-04-15 08:10 |      3 |
| 2020-04-15 08:09 |  5.00 |    D |   supplier4 | 2020-04-15 08:00 | 2020-04-15 08:10 |      1 |
| 2020-04-15 08:11 |  2.00 |    B |   supplier3 | 2020-04-15 08:10 | 2020-04-15 08:20 |      3 |
| 2020-04-15 08:15 |  3.00 |    H |   supplier2 | 2020-04-15 08:10 | 2020-04-15 08:20 |      2 |
| 2020-04-15 08:17 |  6.00 |    F |   supplier5 | 2020-04-15 08:10 | 2020-04-15 08:20 |      1 |
+------------------+-------+------+-------------+------------------+------------------+--------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*

## Limitation

Currently, Flink only supports Window Top-N follows after [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) with Tumble Windows, Hop Windows and Cumulate Windows. Window Top-N follows after [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) with Session windows will be supported in the near future.


{{< top >}}
