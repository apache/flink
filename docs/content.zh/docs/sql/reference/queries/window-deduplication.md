---
title: "窗口去重"
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

# 窗口去重
{{< label Streaming >}}

窗口去重是一种特殊的 [去重]({{< ref "docs/dev/table/sql/queries/deduplication" >}})，它根据指定的多个列来删除重复的行，保留每个窗口和分区键的第一个或最后一个数据。

对于流式查询，与普通去重不同，窗口去重只在窗口的最后返回结果数据，不会产生中间结果。它会清除不需要的中间状态。
因此，窗口去重查询在用户不需要更新结果时，性能较好。通常，窗口去重直接用于 [窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 上。另外，它可以用于基于 [窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 的操作。比如 [窗口聚合]({{< ref "docs/dev/table/sql/queries/window-agg" >}})，[窗口TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}}) 和 [窗口关联]({{< ref "docs/dev/table/sql/queries/window-join">}})。

窗口Top-N的语法和普通的Top-N相同，更多信息参见：[去重文档]({{< ref "docs/dev/table/sql/queries/deduplication" >}})。
除此之外，窗口去重需要 `PARTITION BY` 子句包含表的 `window_start` 和 `window_end` 列。
否则优化器无法翻译。

Flink 使用 `ROW_NUMBER()` 移除重复数据，就像 [窗口 Top-N]({{< ref "docs/dev/table/sql/queries/window-topn" >}}) 一样。理论上，窗口是一种特殊的窗口 Top-N：N是1并且是根据处理时间或事件时间排序的。

下面展示了窗口去重的语法：

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name) -- relation applied windowing TVF
WHERE (rownum = 1 | rownum <=1 | rownum < 2) [AND conditions]
```

**参数说明：**

*   `ROW_NUMBER()`：为每一行分配一个唯一且连续的序号，从1开始。
*   `PARTITION BY window_start, window_end [, col_key1...]`： 指定分区字段，需要包含`window_start`， `window_end`以及其他分区键。
*   `ORDER BY time_attr [asc|desc]`： 指定排序列，必须是 [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})。目前 Flink 支持 [处理时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) 和 [事件时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time)。 Order by ASC 表示保留第一行，Order by DESC 表示保留最后一行。
*   `WHERE (rownum = 1 | rownum <=1 | rownum < 2)`： 优化器通过 `rownum = 1 | rownum <=1 | rownum < 2` 来识别查询能否被翻译成窗口去重。

{{< hint info >}}
注意：必须严格遵循上述模式，否则优化器无法翻译查询。
{{< /hint >}}

## 示例

下面的示例展示了在10分钟的滚动窗口上保持最后一条记录。

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

*注意： 为了更好地理解窗口行为，这里把 timestamp 值后面的0去掉了。例如：在 Flink SQL Client 中，如果类型是 `TIMESTAMP(3)` ，`2020-04-15 08:05` 应该显示成 `2020-04-15 08:05:00.000`。*

## 限制

### 在窗口表值函数后直接进行窗口去重的限制
目前，Flink 只支持在滚动窗口、滑动窗口和累积窗口的[窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}})后进行窗口去重。会话窗口的去重将在未来版本中支持。

### 根据时间属性排序的限制
目前，窗口去重只支持根据[事件时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time)进行排序。根据处理时间排序将在未来版本中支持。


{{< top >}}
