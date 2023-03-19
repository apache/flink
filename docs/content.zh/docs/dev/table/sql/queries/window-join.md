---
title: "窗口关联"
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

# 窗口关联
{{< label Batch >}} {{< label Streaming >}}

窗口关联就是增加时间维度到关联条件中。在此过程中，窗口关联将两个流中在同一窗口且符合 join 条件的元素 join 起来。窗口关联的语义和 [DataStream window join]({{< ref "docs/dev/datastream/operators/joining" >}}#window-join) 相同。

在流式查询中，与其他连续表上的关联不同，窗口关联不产生中间结果，只在窗口结束产生一个最终的结果。另外，窗口关联会清除不需要的中间状态。

通常，窗口关联和 [窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 一起使用。而且，窗口关联可以在其他基于 [窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 的操作后使用，例如 [窗口聚合]({{< ref "docs/dev/table/sql/queries/window-agg" >}})，[窗口 Top-N]({{< ref "docs/dev/table/sql/queries/window-topn">}}) 和 [窗口关联]({{< ref "docs/dev/table/sql/queries/window-join">}})。

目前，窗口关联需要在 join on 条件中包含两个输入表的 `window_start` 等值条件和 `window_end` 等值条件。

窗口关联支持 INNER/LEFT/RIGHT/FULL OUTER/ANTI/SEMI JOIN。

## INNER/LEFT/RIGHT/FULL OUTER 

下面展示了 INNER/LEFT/RIGHT/FULL OUTER 窗口关联的语法：

```sql
SELECT ...
FROM L [LEFT|RIGHT|FULL OUTER] JOIN R -- L and R are relations applied windowing TVF
ON L.window_start = R.window_start AND L.window_end = R.window_end AND ...
```

INNER/LEFT/RIGHT/FULL OUTER 这几种窗口关联的语法非常相似，我们在这里只举一个 FULL OUTER JOIN 的例子。
当执行窗口关联时，所有具有相同 key 和相同滚动窗口的数据会被关联在一起。这里给出一个基于 TUMBLE Window TVF 的窗口连接的例子。
在下面的例子中，通过将 join 的时间区域限定为固定的 5 分钟，数据集被分成两个不同的时间窗口：[12:00,12:05) 和 [12:05,12:10)。L2 和 R2 不能 join 在一起是因为它们不在一个窗口中。

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

Flink SQL> SELECT L.num as L_Num, L.id as L_Id, R.num as R_Num, R.id as R_Id,
           COALESCE(L.window_start, R.window_start) as window_start,
           COALESCE(L.window_end, R.window_end) as window_end
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

*注意：为了更好地理解窗口行为，这里把 timestamp 值后面的 0 去掉了。例如：在 Flink SQL Client 中，如果类型是 `TIMESTAMP(3)`，`2020-04-15 08:05` 应该显示成 `2020-04-15 08:05:00.000`。*


## SEMI
如果在同一个窗口中，左侧记录在右侧至少有一个匹配的记录时，半窗口连接（Semi Window Join）就会输出左侧的记录。

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

*注意：为了更好地理解窗口行为，这里把 timestamp 值后面的 0 去掉了。例如：在 Flink SQL Client 中，如果类型是 `TIMESTAMP(3)`，`2020-04-15 08:05` 应该显示成 `2020-04-15 08:05:00.000`。*


## ANTI
反窗口连接（Anti Window Join）是内窗口连接（Inner Window Join）的相反操作：它包含了每个公共窗口内所有未关联上的行。

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

*注意：为了更好地理解窗口行为，这里把 timestamp 值后面的 0 去掉了。例如：在 Flink SQL Client 中，如果类型是 `TIMESTAMP(3)`，`2020-04-15 08:05` 应该显示成 `2020-04-15 08:05:00.000`。*


## 限制

### Join 子句的限制
目前，窗口关联需要在 join on 条件中包含两个输入表的 `window_start` 等值条件和 `window_end` 等值条件。未来，如果是滚动或滑动窗口，只需要在 join on 条件中包含窗口开始相等即可。

### 输入的窗口表值函数的限制
目前，关联的左右两边必须使用相同的窗口表值函数。这个规则在未来可以扩展，比如：滚动和滑动窗口在窗口大小相同的情况下 join。

### 窗口表值函数之后直接使用窗口关联的限制
目前窗口关联支持作用在滚动（TUMBLE）、滑动（HOP）和累积（CUMULATE）[窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 之上，但是还不支持会话窗口（SESSION）。

{{< top >}}
