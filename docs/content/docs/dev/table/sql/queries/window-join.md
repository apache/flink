---
title: "Window JOIN"
weight: 10
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

# Window Join
{{< label Streaming >}}

A window join adds the dimension of time into the join criteria themselves. In doing so, the window join joins the elements of two streams that share a common key and lie in the same window. The semantic of window join is same to the [DataStream window join]({{< ref "docs/dev/datastream/operators/joining" >}}#window-join)

For streaming queries, unlike other joins on continuous tables, window join does not emit intermediate results but only emits final results at the end of the window. Moreover, window join purge all intermediate state when no longer needed.

Usually, Window Join is used with [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}). Besides, Window Join could follow after other operations based on [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}), such as [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}), [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}) and [Window Join]({{< ref "docs/dev/table/sql/queries/window-join">}}).

Currently, Window Join requires the join on condition contains window starts equality of input tables and window ends equality of input tables.

Window Join supports INNER/LEFT/RIGHT/FULL OUTER/ANTI/SEMI JOIN.

## INNER/LEFT/RIGHT/FULL OUTER 

The following shows the syntax of the INNER/LEFT/RIGHT/FULL OUTER Window Join statement.

```sql
SELECT ...
FROM L [LEFT|RIGHT|FULL OUTER] JOIN R -- L and R are relations applied windowing TVF
ON L.window_start = R.window_start AND L.window_end = R.window_end AND ...
```

The syntax of INNER/LEFT/RIGHT/FULL OUTER WINDOW JOIN are very similar with each other, we only give an example for FULL OUTER JOIN here.
When performing a window join, all elements with a common key and a common tumbling window are joined together. We only give an example for a Window Join which works on a Tumble Window TVF.
By scoping the region of time for the join into fixed five-minute intervals, we chopped our datasets into two distinct windows of time: [12:00, 12:05) and [12:05, 12:10). The L2 and R2 rows could not join together because they fell into separate windows.

```sql
Flink SQL> desc LeftTable;
+----------+------------------------+------+-----+--------+----------------------------------+
|     name |                   type | null | key | extras |                        watermark |
+----------+------------------------+------+-----+--------+----------------------------------+
| row_time | TIMESTAMP(3) *ROWTIME* | true |     |        | `row_time` - INTERVAL '1' SECOND |
|      num |                    INT | true |     |        |                                  |
|       id |                 STRING | true |     |        |                                  |
+----------+------------------------+------+-----+--------+----------------------------------+

Flink SQL> SELECT * FROM LeftTable;
+------------------+-----+----+
|         row_time | num | id |
+------------------+-----+----+
| 2020-04-15 12:02 |   1 | L1 |
| 2020-04-15 12:06 |   2 | L2 |
| 2020-04-15 12:03 |   3 | L3 |
+------------------+-----+----+

Flink SQL> desc RightTable;
+----------+------------------------+------+-----+--------+----------------------------------+
|     name |                   type | null | key | extras |                        watermark |
+----------+------------------------+------+-----+--------+----------------------------------+
| row_time | TIMESTAMP(3) *ROWTIME* | true |     |        | `row_time` - INTERVAL '1' SECOND |
|      num |                    INT | true |     |        |                                  |
|       id |                 STRING | true |     |        |                                  |
+----------+------------------------+------+-----+--------+----------------------------------+

Flink SQL> SELECT * FROM RightTable;
+------------------+-----+----+
|         row_time | num | id |
+------------------+-----+----+
| 2020-04-15 12:01 |   2 | R2 |
| 2020-04-15 12:04 |   3 | R3 |
| 2020-04-15 12:05 |   4 | R4 |
+------------------+-----+----+

Flink SQL> SELECT L.num as L_Num, L.id as L_Id, R.num as R_Num, R.id as R_Id, L.window_start, L.window_end
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L
           FULL JOIN (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) R
           ON L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end;
+-------+------+-------+------+------------------+------------------+
| L_Num | L_Id | R_Num | R_Id |     window_start |       window_end |
+-------+------+-------+------+------------------+------------------+
|     1 |   L1 |  null | null | 2020-04-15 12:00 | 2020-04-15 12:05 |
|  null | null |     2 |   R2 | 2020-04-15 12:00 | 2020-04-15 12:05 |
|     3 |   L3 |     3 |   R3 | 2020-04-15 12:00 | 2020-04-15 12:05 |
|     2 |   L2 |  null | null | 2020-04-15 12:05 | 2020-04-15 12:10 |
|  null | null |     4 |   R4 | 2020-04-15 12:05 | 2020-04-15 12:10 |
+-------+------+-------+------+------------------+------------------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*


## SEMI
Semi Window Joins returns a row from one left record if there is at least one matching row on the right side within the common window.

```sql
Flink SQL> SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE L.num IN (
             SELECT num FROM (   
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:03 |   3 | L3 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+

Flink SQL> SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE EXISTS (
             SELECT * FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:03 |   3 | L3 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*


## ANTI
Anti Window Joins are the obverse of the Inner Window Join: they contain all of the unjoined rows within each common window.

```sql
Flink SQL> SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE L.num NOT IN (
             SELECT num FROM (   
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:02 |   1 | L1 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:06 |   2 | L2 | 2020-04-15 12:05 | 2020-04-15 12:10 | 2020-04-15 12:09:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+

Flink SQL> SELECT *
           FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
           ) L WHERE NOT EXISTS (
             SELECT * FROM (
               SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
             ) R WHERE L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:02 |   1 | L1 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:06 |   2 | L2 | 2020-04-15 12:05 | 2020-04-15 12:10 | 2020-04-15 12:09:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+
```

*Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. `2020-04-15 08:05` should be displayed as `2020-04-15 08:05:00.000` in Flink SQL Client if the type is `TIMESTAMP(3)`.*


## Limitation

### Limitation on Join clause
Currently, The window join requires the join on condition contains window starts equality of input tables and window ends equality of input tables. In the future, we can also simplify the join on clause to only include the window start equality if the windowing TVF is TUMBLE or HOP. 

### Limitation on windowing TVFs of inputs
Currently, the windowing TVFs must be the same of left and right inputs. This can be extended in the future, for example, tumbling windows join sliding windows with the same window size.

### Limitation on Window Join which follows after Windowing TVFs directly
Currently, if Window Join follows after [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}), the [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) has to be with Tumble Windows, Hop Windows or Cumulate Windows instead of Session windows.

{{< top >}}
