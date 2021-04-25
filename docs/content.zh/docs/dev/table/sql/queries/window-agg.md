---
title: "窗口聚合"
weight: 7
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

# Window Aggregation

## Window TVF Aggregation

{{< label Streaming >}}

Window aggregations are defined in the `GROUP BY` clause contains "window_start" and "window_end" columns of the relation applied [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}). Just like queries with regular `GROUP BY` clauses, queries with a group by window aggregation will compute a single result row per group.

```sql
SELECT ...
FROM <windowed_table> -- relation applied windowing TVF
GROUP BY window_start, window_end, ...
```

Unlike other aggregations on continuous tables, window aggregation do not emit intermediate results but only a final result, the total aggregation at the end of the window. Moreover, window aggregations purge all intermediate state when no longer needed.

### Windowing TVFs

Flink supports `TUMBLE`, `HOP` and `CUMULATE` types of window aggregations, which can be defined on either [event or processing time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}). See [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) for more windowing functions information.

Here are some examples for `TUMBLE`, `HOP` and `CUMULATE` window aggregations.

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
| 2020-04-15 08:05 | 4.00  | C    | supplier1   |
| 2020-04-15 08:07 | 2.00  | A    | supplier1   |
| 2020-04-15 08:09 | 5.00  | D    | supplier2   |
| 2020-04-15 08:11 | 3.00  | B    | supplier2   |
| 2020-04-15 08:13 | 1.00  | E    | supplier1   |
| 2020-04-15 08:17 | 6.00  | F    | supplier2   |
+------------------+-------+------+-------------+

-- tumbling window aggregation
Flink SQL> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
+------------------+------------------+-------+
|     window_start |       window_end | price |
+------------------+------------------+-------+
| 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
+------------------+------------------+-------+

-- hopping window aggregation
Flink SQL> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
+------------------+------------------+-------+
|     window_start |       window_end | price |
+------------------+------------------+-------+
| 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
| 2020-04-15 08:05 | 2020-04-15 08:15 | 15.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
| 2020-04-15 08:15 | 2020-04-15 08:25 | 6.00  |
+------------------+------------------+-------+

-- cumulative window aggregation
Flink SQL> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
+------------------+------------------+-------+
|     window_start |       window_end | price |
+------------------+------------------+-------+
| 2020-04-15 08:00 | 2020-04-15 08:06 | 4.00  |
| 2020-04-15 08:00 | 2020-04-15 08:08 | 6.00  |
| 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
| 2020-04-15 08:10 | 2020-04-15 08:12 | 3.00  |
| 2020-04-15 08:10 | 2020-04-15 08:14 | 4.00  |
| 2020-04-15 08:10 | 2020-04-15 08:16 | 4.00  |
| 2020-04-15 08:10 | 2020-04-15 08:18 | 10.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
+------------------+------------------+-------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*

### GROUPING SETS

Window aggregations also support `GROUPING SETS` syntax. Grouping sets allow for more complex grouping operations than those describable by a standard `GROUP BY`. Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple `GROUP BY` clauses.

Window aggregations with `GROUPING SETS` require both the `window_start` and `window_end` columns have to be in the `GROUP BY` clause, but not in the `GROUPING SETS` clause.

```sql
Flink SQL> SELECT window_start, window_end, supplier_id, SUM(price) as price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end, GROUPING SETS ((supplier_id), ());
+------------------+------------------+-------------+-------+
|     window_start |       window_end | supplier_id | price |
+------------------+------------------+-------------+-------+
| 2020-04-15 08:00 | 2020-04-15 08:10 |      (NULL) | 11.00 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier2 |  5.00 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier1 |  6.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |      (NULL) | 10.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier2 |  9.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier1 |  1.00 |
+------------------+------------------+-------------+-------+
```

Each sublist of `GROUPING SETS` may specify zero or more columns or expressions and is interpreted the same way as though used directly in the `GROUP BY` clause. An empty grouping set means that all rows are aggregated down to a single group, which is output even if no input rows were present.

References to the grouping columns or expressions are replaced by null values in result rows for grouping sets in which those columns do not appear.

#### ROLLUP

`ROLLUP` is a shorthand notation for specifying a common type of grouping set. It represents the given list of expressions and all prefixes of the list, including the empty list.

Window aggregations with `ROLLUP` requires both the `window_start` and `window_end` columns have to be in the `GROUP BY` clause, but not in the `ROLLUP` clause.

For example, the following query is equivalent to the one above.

```sql
SELECT window_start, window_end, supplier_id, SUM(price) as price
FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end, ROLLUP (supplier_id);
```

#### CUBE

`CUBE` is a shorthand notation for specifying a common type of grouping set. It represents the given list and all of its possible subsets - the power set.

Window aggregations with `CUBE` requires both the `window_start` and `window_end` columns have to be in the `GROUP BY` clause, but not in the `CUBE` clause.

For example, the following two queries are equivalent.

```sql
SELECT window_start, window_end, item, supplier_id, SUM(price) as price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end, CUBE (supplier_id, item);

SELECT window_start, window_end, item, supplier_id, SUM(price) as price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end, GROUPING SETS (
      (supplier_id, item),
      (supplier_id      ),
      (             item),
      (                 )
)
```

### Selecting Group Window Start and End Timestamps

The start and end timestamps of group windows can be selected with the grouped `window_start` and `window_end` columns.

### Cascading Window Aggregation

The `window_start` and `window_end` columns are regular timestamp columns, not time attributes. Thus they can't be used as time attributes in subsequent time-based operations.
In order to propagate time attributes, you need to additionally add `window_time` column into `GROUP BY` clause. The `window_time` is the third column produced by [Windowing TVFs]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}#window-functions) which is a time attribute of the assigned window.
Adding `window_time` into `GROUP BY` clause makes `window_time` also to be group key that can be selected. Then following queries can use this column for subsequent time-based operations, such as cascading window aggregations and [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}).

The following shows a cascading window aggregation where the first window aggregation propagates the time attribute for the second window aggregation.

```sql
-- tumbling 5 minutes for each supplier_id
CREATE VIEW window1 AS
SELECT window_start, window_end, window_time as rowtime, SUM(price) as partial_price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES))
  GROUP BY supplier_id, window_start, window_end, window_time;

-- tumbling 10 minutes on the first window
SELECT window_start, window_end, SUM(partial_price) as total_price
  FROM TABLE(
      TUMBLE(TABLE window1, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
```

## Group Window Aggregation

{{< label Batch >}} {{< label Streaming >}}

{{< hint warning >}}
Warning: Group Window Aggregation is deprecated. It's encouraged to use Window TVF Aggregation which is more powerful and effective.

Compared to Group Window Aggregation, Window TVF Aggregation have many advantages, including:
- Have all performance optimizations mentioned in [Performance Tuning]({{< ref "docs/dev/table/tuning" >}}).
- Support standard `GROUPING SETS` syntax.
- Can apply [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}) after window aggregation result.
- and so on.
{{< /hint >}}

Group Window Aggregations are defined in the `GROUP BY` clause of a SQL query. Just like queries with regular `GROUP BY` clauses, queries with a `GROUP BY` clause that includes a group window function compute a single result row per group. The following group windows functions are supported for SQL on batch and streaming tables.

### Group Window Functions

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Group Window Function</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>TUMBLE(time_attr, interval)</code></td>
      <td>Defines a tumbling time window. A tumbling time window assigns rows to non-overlapping, continuous windows with a fixed duration (<code>interval</code>). For example, a tumbling window of 5 minutes groups rows in 5 minutes intervals. Tumbling windows can be defined on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
    <tr>
      <td><code>HOP(time_attr, interval, interval)</code></td>
      <td>Defines a hopping time window (called sliding window in the Table API). A hopping time window has a fixed duration (second <code>interval</code> parameter) and hops by a specified hop interval (first <code>interval</code> parameter). If the hop interval is smaller than the window size, hopping windows are overlapping. Thus, rows can be assigned to multiple windows. For example, a hopping window of 15 minutes size and 5 minute hop interval assigns each row to 3 different windows of 15 minute size, which are evaluated in an interval of 5 minutes. Hopping windows can be defined on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
    <tr>
      <td><code>SESSION(time_attr, interval)</code></td>
      <td>Defines a session time window. Session time windows do not have a fixed duration but their bounds are defined by a time <code>interval</code> of inactivity, i.e., a session window is closed if no event appears for a defined gap period. For example a session window with a 30 minute gap starts when a row is observed after 30 minutes inactivity (otherwise the row would be added to an existing window) and is closed if no row is added within 30 minutes. Session windows can work on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
  </tbody>
</table>

### Time Attributes

For SQL queries on streaming tables, the `time_attr` argument of the group window function must refer to a valid time attribute that specifies the processing time or event time of rows. See the [documentation of time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) to learn how to define time attributes.

For SQL on batch tables, the `time_attr` argument of the group window function must be an attribute of type `TIMESTAMP`.

### Selecting Group Window Start and End Timestamps

The start and end timestamps of group windows as well as time attributes can be selected with the following auxiliary functions:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary Function</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <code>TUMBLE_START(time_attr, interval)</code><br/>
        <code>HOP_START(time_attr, interval, interval)</code><br/>
        <code>SESSION_START(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the inclusive lower bound of the corresponding tumbling, hopping, or session window.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_END(time_attr, interval)</code><br/>
        <code>HOP_END(time_attr, interval, interval)</code><br/>
        <code>SESSION_END(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>exclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
        <p><b>Note:</b> The exclusive upper bound timestamp <i>cannot</i> be used as a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a> in subsequent time-based operations, such as <a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> and <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> or <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>inclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
      <p>The resulting attribute is a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a> that can be used in subsequent time-based operations such as <a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> and <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> or <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time">proctime attribute</a> that can be used in subsequent time-based operations such as <a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> and <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> or <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>.</p></td>
    </tr>
  </tbody>
</table>

*Note:* Auxiliary functions must be called with exactly same arguments as the group window function in the `GROUP BY` clause.

The following examples show how to specify SQL queries with group windows on streaming tables.

```sql
CREATE TABLE Orders (
  user       BIGINT,
  product    STIRNG,
  amount     INT,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '1' MINUTE
) WITH (...);

SELECT
  user,
  TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
  SUM(amount) FROM Orders
GROUP BY
  TUMBLE(order_time, INTERVAL '1' DAY),
  user
```

{{< top >}}
