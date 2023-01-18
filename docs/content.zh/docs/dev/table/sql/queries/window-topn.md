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

# 窗口 Top-N

{{< label Batch >}} {{< label Streaming >}}

窗口Top-N是特殊的\[Top-N]\({{< ref "docs/dev/table/sql/queries/topn" >}}),它返回每个窗口和其他分区键的N个最小或最大值.

对于流式查询,与持续查询的普通Top-N不同,它只在窗口最后返回汇总的Top-N数据,不会产生中间结果.窗口Top-N会清除不需要的中间状态.
因此,窗口Top-N查询在用户不需要更新结果时,性能较好.通常,窗口Top-N直接用于\[窗口表值函数]\({{< ref "docs/dev/table/sql/queries/window-tvf" >}})上.另外,窗口Top-N可以用于基于\[窗口表值函数]\({{< ref "docs/dev/table/sql/queries/window-tvf" >}})的操作.比如\[窗口聚合]\({{< ref "docs/dev/table/sql/queries/window-agg" >}}),\[窗口TopN]\({{< ref "docs/dev/table/sql/queries/window-topn">}}) 和 \[窗口关联]\({{< ref "docs/dev/table/sql/queries/window-join">}}).

窗口Top-N的语法和普通的Top-N相同,更多信息参见:\[Top-N文档]\({{< ref "docs/dev/table/sql/queries/topn" >}}).

除此之外,窗口Top-N需要 `PARTITION BY` 子句包含\[窗口表值函数]\({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 或 \[窗口聚合]\({{< ref "docs/dev/table/sql/queries/window-agg" >}})产生的`window_start`和`window_end`.
否则优化器无法翻译.

下面展示了窗口Top-N的语法:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER (PARTITION BY window_start, window_end [, col_key1...]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name) -- relation applied windowing TVF
WHERE rownum <= N [AND conditions]
```

## 示例

### 在窗口聚合后进行窗口Top-N

下面的示例展示了在10分钟的滚动窗口上计算销售额位列前三的供应商.

```sql
-- 表必须有时间属性.这里是`bidtime`字段.
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

*注意: 为了更好地理解窗口行为,这里把timestamp值后面的0去掉了.例如:在Flink SQL Client中,如果类型是`TIMESTAMP(3)`,`2020-04-15 08:05`应该显示成`2020-04-15 08:05:00.000`.*

### 在窗口表值函数后进行窗口Top-N

下面的示例展示了在10分钟的滚动窗口上计算价格位列前三的数据.

```sql
Flink SQL> SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
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

*注意: 为了更好地理解窗口行为,这里把timestamp值后面的0去掉了.例如:在Flink SQL Client中,如果类型是`TIMESTAMP(3)`,`2020-04-15 08:05`应该显示成`2020-04-15 08:05:00.000`.*

## 限制

目前,Flink只支持在滚动,滑动和累计\[窗口表值函数]\({{< ref "docs/dev/table/sql/queries/window-tvf" >}})后进行窗口Top-N.会话窗口不久之后就会支持.

{{< top >}}
