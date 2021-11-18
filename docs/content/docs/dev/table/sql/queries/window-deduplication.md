---
title: "Window Deduplication"
weight: 16
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

# Window Deduplication
{{< label Streaming >}}

Window Deduplication is a special [Deduplication]({{< ref "docs/dev/table/sql/queries/deduplication" >}}) which removes rows that duplicate over a set of columns, keeping the first one or the last one for each window and partitioned keys. 

For streaming queries, unlike regular Deduplicate on continuous tables, Window Deduplication does not emit intermediate results but only a final result at the end of the window. Moreover, window Deduplication purges all intermediate state when no longer needed.
Therefore, Window Deduplication queries have better performance if users don't need results updated per record. Usually, Window Deduplication is used with [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) directly. Besides, Window Deduplication could be used with other operations based on [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}), such as [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}), [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}) and [Window Join]({{< ref "docs/dev/table/sql/queries/window-join">}}). 

Window Deduplication can be defined in the same syntax as regular Deduplication, see [Deduplication documentation]({{< ref "docs/dev/table/sql/queries/deduplication" >}}) for more information.
Besides that, Window Deduplication requires the `PARTITION BY` clause contains `window_start` and `window_end` columns of the relation.
Otherwise, the optimizer won’t be able to translate the query.

Flink uses `ROW_NUMBER()` to remove duplicates, just like the way of [Window Top-N query]({{< ref "docs/dev/table/sql/queries/window-topn" >}}). In theory, Window Deduplication is a special case of Window Top-N in which the N is one and order by the processing time or event time.

The following shows the syntax of the Window Deduplication statement:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name) -- relation applied windowing TVF
WHERE (rownum = 1 | rownum <=1 | rownum < 2) [AND conditions]
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY window_start, window_end [, col_key1...]`: Specifies the partition columns which contain `window_start`, `window_end` and other partition keys.
- `ORDER BY time_attr [asc|desc]`: Specifies the ordering column, it must be a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}). Currently Flink supports [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) and [event time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time). Ordering by ASC means keeping the first row, ordering by DESC means keeping the last row.
- `WHERE (rownum = 1 | rownum <=1 | rownum < 2)`: The `rownum = 1 | rownum <=1 | rownum < 2` is required for the optimizer to recognize the query could be translated to Window Deduplication.

{{< hint info >}}
Note: the above pattern must be followed exactly, otherwise the optimizer won’t translate the query to Window Deduplication.
{{< /hint >}}

## Example

The following example shows how to keep last record for every 10 minutes tumbling window.

```sql
-- tables must have time attribute, e.g. `bidtime` in this table
Flink SQL> DESC Bid;
+-------------+------------------------+------+-----+--------+---------------------------------+
|        name |                   type | null | key | extras |                       watermark |
+-------------+------------------------+------+-----+--------+---------------------------------+
|     bidtime | TIMESTAMP(3) *ROWTIME* | true |     |        | `bidtime` - INTERVAL '1' SECOND |
|       price |         DECIMAL(10, 2) | true |     |        |                                 |
|        item |                 STRING | true |     |        |                                 |
+-------------+------------------------+------+-----+--------+---------------------------------+

Flink SQL> SELECT * FROM Bid;
+------------------+-------+------+
|          bidtime | price | item |
+------------------+-------+------+
| 2020-04-15 08:05 |  4.00 | C    |
| 2020-04-15 08:07 |  2.00 | A    |
| 2020-04-15 08:09 |  5.00 | D    |
| 2020-04-15 08:11 |  3.00 | B    |
| 2020-04-15 08:13 |  1.00 | E    |
| 2020-04-15 08:17 |  6.00 | F    |
+------------------+-------+------+

Flink SQL> SELECT *
  FROM (
    SELECT bidtime, price, item, supplier_id, window_start, window_end, 
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY bidtime DESC) AS rownum
    FROM TABLE(
               TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  ) WHERE rownum <= 1;
+------------------+-------+------+-------------+------------------+------------------+--------+
|          bidtime | price | item | supplier_id |     window_start |       window_end | rownum |
+------------------+-------+------+-------------+------------------+------------------+--------+
| 2020-04-15 08:09 |  5.00 |    D |   supplier4 | 2020-04-15 08:00 | 2020-04-15 08:10 |      1 |
| 2020-04-15 08:17 |  6.00 |    F |   supplier5 | 2020-04-15 08:10 | 2020-04-15 08:20 |      1 |
+------------------+-------+------+-------------+------------------+------------------+--------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*

## Limitation

### Limitation on Window Deduplication which follows after Windowing TVFs directly
Currently, if Window Deduplication follows after [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}), the [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) has to be with Tumble Windows, Hop Windows or Cumulate Windows instead of Session windows. Session windows will be supported in the near future.

### Limitation on time attribute of order key
Currently, Window Deduplication requires order key must be [event time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time) instead of [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time). Ordering by processing-time would be supported in the near future.


{{< top >}}
