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

# 窗口聚合

## 窗口表值函数（TVF）聚合

{{< label Batch >}} {{< label Streaming >}}

窗口聚合是通过 `GROUP BY` 子句定义的，其特征是包含 [窗口表值函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 产生的 "window_start" 和 "window_end" 列。和普通的 `GROUP BY` 子句一样，窗口聚合对于每个组会计算出一行数据。

```sql
SELECT ...
FROM <windowed_table> -- relation applied windowing TVF
GROUP BY window_start, window_end, ...
```

和其他连续表上的聚合不同，窗口聚合不产生中间结果，只在窗口结束产生一个总的聚合结果，另外，窗口聚合会清除不需要的中间状态。

### 窗口表值函数

Flink 支持在 `TUMBLE`， `HOP` 和 `CUMULATE` 上进行窗口聚合。
在流模式下，窗口表值函数的时间属性字段必须是 [事件时间或处理时间]({{< ref "docs/dev/table/concepts/time_attributes" >}})。关于窗口函数更多信息，参见 [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}})。
在批模式下，窗口表值函数的时间属性字段必须是 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型的。

这里有关于 `TUMBLE`，`HOP` 和 `CUMULATE` 窗口聚合的几个例子：

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

*注意: 为了更好地理解窗口行为，这里把 timestamp 值后面的 0 去掉了，例如：在 Flink SQL Client 中，如果类型是 `TIMESTAMP(3)`，`2020-04-15 08:05` 应该显示成 `2020-04-15 08:05:00.000`。*

### GROUPING SETS

窗口聚合也支持 `GROUPING SETS` 语法。Grouping Sets 可以通过一个标准的 `GROUP BY` 语句来描述更复杂的分组操作。数据按每个指定的 Grouping Sets 分别分组，并像简单的 `GROUP BY` 子句一样为每个组进行聚合。

`GROUPING SETS` 窗口聚合中 `GROUP BY` 子句必须包含 `window_start` 和 `window_end` 列，但 `GROUPING SETS` 子句中不能包含这两个字段。

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

`GROUPING SETS` 的每个子列表可以是：空的，多列或表达式，它们的解释方式和直接使用 `GROUP BY` 子句是一样的。一个空的 Grouping Sets 表示所有行都聚合在一个分组下，即使没有数据，也会输出结果。

对于 Grouping Sets 中的空子列表，结果数据中的分组或表达式列会用`NULL`代替。例如，上例中的 `GROUPING SETS ((supplier_id), ())` 里的 `()` 就是空子列表，与其对应的结果数据中的 `supplier_id` 列使用 `NULL` 填充。

#### ROLLUP

`ROLLUP` 是一种特定通用类型 Grouping Sets 的简写。代表着指定表达式和所有前缀的列表，包括空列表。例如：`ROLLUP (one,two)` 等效于 `GROUPING SET((one,two),(one),())`.

`ROLLUP` 窗口聚合中 `GROUP BY` 子句必须包含 `window_start` 和 `window_end` 列，但 `ROLLUP` 子句中不能包含这两个字段。

例如：下面这个查询和上个例子中的效果是一样的。

```sql
SELECT window_start, window_end, supplier_id, SUM(price) as price
FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end, ROLLUP (supplier_id);
```

#### CUBE

`CUBE` 是一种特定通用类型 Grouping Sets 的简写。代表着指定列表以及所有可能的子集和幂集。

`CUBE` 窗口聚合中 `GROUP BY` 子句必须包含 `window_start` 和 `window_end` 列，但 `CUBE` 子句中不能包含这两个字段。

例如：下面两个查询是等效的。

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

### 选取分组窗口的开始和结束时间戳

分组窗口的开始和结束时间戳可以通过 `window_start` 和 `window_end` 来选定.

### 多级窗口聚合

`window_start` 和 `window_end` 列是普通的时间戳字段，并不是时间属性。因此它们不能在后续的操作中当做时间属性进行基于时间的操作。
为了传递时间属性，需要在 `GROUP BY` 子句中添加 `window_time` 列。`window_time` 是 [Windowing TVFs]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}#window-functions) 产生的三列之一，它是窗口的时间属性。
`window_time` 添加到 `GROUP BY` 子句后就能被选定了。下面的查询可以把它用于后续基于时间的操作，比如：多级窗口聚合 和 [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}})。

下面展示了一个多级窗口聚合：第一个窗口聚合后把时间属性传递给第二个窗口聚合。

```sql
-- tumbling 5 minutes for each supplier_id
CREATE VIEW window1 AS
-- Note: The window start and window end fields of inner Window TVF are optional in the select clause. However, if they appear in the clause, they need to be aliased to prevent name conflicting with the window start and window end of the outer Window TVF.
SELECT window_start as window_5mintumble_start, window_end as window_5mintumble_end, window_time as rowtime, SUM(price) as partial_price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES))
  GROUP BY supplier_id, window_start, window_end, window_time;

-- tumbling 10 minutes on the first window
SELECT window_start, window_end, SUM(partial_price) as total_price
  FROM TABLE(
      TUMBLE(TABLE window1, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
```

## 分组窗口聚合

{{< label Batch >}} {{< label Streaming >}}

{{< hint warning >}}
警告：分组窗口聚合已经过时。推荐使用更加强大和有效的[窗口表值函数聚合](#窗口表值函数tvf聚合)。

"窗口表值函数聚合"相对于"分组窗口聚合"有如下优点：
- 包含 [性能调优]({{< ref "docs/dev/table/tuning" >}}) 中提到的所有性能优化。
- 支持标准的 `GROUPING SETS` 语法。 
- 可以在窗口聚合结果上使用 [窗口 TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}})。
- 等等。
{{< /hint >}}

分组窗口聚合定义在 SQL 的 `GROUP BY` 子句中。和普通的 `GROUP BY` 子句一样，包含分组窗口函数的 `GROUP BY` 子句的查询会对各组分别计算，各产生一个结果行。批处理表和流表上的SQL支持以下分组窗口函数：

### 分组窗口函数

<table>
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Group Window Function</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>TUMBLE(time_attr, interval)</code></td>
      <td>定义一个滚动时间窗口。它把数据分配到连续且不重叠的固定时间区间（<code>interval</code>）,例如：一个5分钟的滚动窗口以5分钟为间隔对数据进行分组。滚动窗口可以被定义在事件时间（流 + 批）或者处理时间（流）上。</td>
    </tr>
    <tr>
      <td><code>HOP(time_attr, interval, interval)</code></td>
      <td>定义一个滑动时间窗口，它有窗口大小（第二个 <code>interval</code> 参数）和滑动间隔（第一个 <code>interval</code> 参数）两个参数。如果滑动间隔小于窗口大小，窗口会产生重叠。所以，数据可以被指定到多个窗口。例如：一个15分钟大小和5分钟滑动间隔的滑动窗口将每一行分配给3个15分钟大小的不同窗口，这些窗口以5分钟的间隔计算。滑动窗口可以被定义在事件时间（流 + 批）或者处理时间（流）上。</td>
    </tr>
    <tr>
      <td><code>SESSION(time_attr, interval)</code></td>
      <td>定义一个会话时间窗口。会话时间窗口没有固定的时间区间，但其边界是通过不活动的时间 <code>interval</code> 定义的，即：一个会话窗口会在指定的时长内没有事件出现时关闭。例如：一个30分钟间隔的会话窗口收到一条数据时，如果之前已经30分钟不活动了（否则，这条数据会被分配到已经存在的窗口中），它会开启一个新窗口，如果30分钟之内没有新数据到来，就会关闭。会话窗口可以被定义在事件时间（流 + 批) 或者处理时间（流）上。</td>
    </tr>
  </tbody>
</table>

### 时间属性

在流处理模式，分组窗口函数的 `time_attr` 属性必须是一个有效的处理或事件时间。更多关于定义时间属性参见：[时间属性文档]({{< ref "docs/dev/table/concepts/time_attributes" >}})

在批处理模式，分组窗口函数的 `time_attr` 参数必须是一个 `TIMESTAMP` 类型的属性。

### 选取分组窗口开始和结束时间戳

分组窗口的开始和结束时间戳以及时间属性也可以通过下列辅助函数的方式获取到：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">辅助函数</th>
      <th class="text-left">描述</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <code>TUMBLE_START(time_attr, interval)</code><br/>
        <code>HOP_START(time_attr, interval, interval)</code><br/>
        <code>SESSION_START(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相应的滚动，滑动或会话窗口的下限的时间戳（inclusive），即窗口开始时间。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_END(time_attr, interval)</code><br/>
        <code>HOP_END(time_attr, interval, interval)</code><br/>
        <code>SESSION_END(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相应滚动窗口，跳跃窗口或会话窗口的上限的时间戳（exclusive），即窗口结束时间。</p>
        <p><b>注意:</b> 上限时间戳（exlusive）<i>不能</i>作为 <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a> 用于后续基于时间的操作，例如：<a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> 和 <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> 或 <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>返回相应滚动窗口，跳跃窗口或会话窗口的上限的时间戳（inclusive），即窗口事件时间，或窗口处理时间。</p>
      <p>返回的值是 <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a>，可以用于后续基于时间的操作，比如：<a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> 和 <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> 或 <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>。</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>返回的值是 <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time">proctime attribute</a>，可以用于后续基于时间的操作，比如： <a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a> 和 <a href="{{< ref "docs/dev/table/sql/queries/window-agg" >}}">group window</a> 或 <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over window aggregations</a>。</p></td>
    </tr>
  </tbody>
</table>

*注意：* 辅助函数的参数必须和 `GROUP BY` 子句中的分组窗口函数一致。

下面的例子展示了在流式表上如何使用分组窗口 SQL 查询：

```sql
CREATE TABLE Orders (
  user       BIGINT,
  product    STRING,
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
