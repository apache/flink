---
title: "Windowing TVF"
weight: 6
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

# Windowing table-valued functions (Windowing TVFs)

{{< label Streaming >}}

Windows are at the heart of processing infinite streams. Windows split the stream into “buckets” of finite size, over which we can apply computations. This document focuses on how windowing is performed in Flink SQL and how the programmer can benefit to the maximum from its offered functionality.

Apache Flink provides several windowing table-valued functions (TVF) to divide the elements of your table into windows, including:

- [Tumble Windows](#tumble)
- [Hop Windows](#hop)
- [Cumulate Windows](#cumulate)
- Session Windows (will be supported soon)

Note that each element can logically belong to more than one window, depending on the windowing table-valued function you use. For example, HOP windowing creates overlapping windows wherein a single element can be assigned to multiple windows.

Windowing TVFs are Flink defined Polymorphic Table Functions (abbreviated PTF). PTF is the part of the SQL 2016 standard which is a special table-function, but can have table as parameter. PTF is a powerful feature to change the shape of a table. Because PTFs are semantically used like tables, their invocation occurs in a `FROM` clause of a `SELECT` statement.

Windowing TVFs is a replacement of legacy [Grouped Window Functions]({{< ref "docs/dev/table/sql/queries/window-agg" >}}#group-window-aggregation-deprecated). Windowing TVFs is more SQL standard compliant and more powerful to support complex window-based computations, e.g. Window TopN, Window Join. However, [Grouped Window Functions]({{< ref "docs/dev/table/sql/queries/window-agg" >}}#group-window-aggregation) can only support Window Aggregation.

See more how to apply further computations based on windowing TVF:
- [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}})
- [Window TopN]({{< ref "docs/dev/table/sql/queries/window-topn">}})
- Window Join (will be supported soon)

## Window Functions

Apache Flink provides 3 built-in windowing TVFs: TUMBLE, `HOP` and `CUMULATE`. The return value of windowing TVF is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The "window_time" field is a [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) of the window after windowing TVF which can be used in subsequent time-based operations, e.g. another windowing TVF, or <a href="{{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins">interval joins</a>, <a href="{{< ref "docs/dev/table/sql/queries/over-agg" >}}">over aggregations</a>. The value of `window_time` always equal to `window_end - 1ms`.

### TUMBLE

The `TUMBLE` function assigns each element to a window of a specified window size. Tumbling windows have a fixed size and do not overlap. For example, if you specify a tumbling window with a size of 5 minutes, the current window will be evaluated and a new window will be started every five minutes as illustrated by the following figure.

{{< img src="/fig/tumbling-windows.svg" alt="Tumbling Windows" width="70%">}}

The `TUMBLE` function assigns a window for each row of a relation based on a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column. The return value of `TUMBLE` is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after window TVF.

`TUMBLE` function takes three required parameters:

```sql
TUMBLE(TABLE data, DESCRIPTOR(timecol), size)
```

- `data`: is a table parameter that can be any relation with a time attribute column.
- `timecol`: is a column descriptor indicating which [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column of data should be mapped to tumbling windows.
- `size`: is a duration specifying the width of the tumbling windows.

Here is an example invocation on the `Bid` table (we simplify `bidtime` only to show the time part):

```sql
> SELECT * FROM Bid;
--+---------+-------+------+
--| bidtime | price | item |
--+---------+-------+------+
--| 08:07   | $2    | A    |
--| 08:11   | $3    | B    |
--| 08:05   | $4    | C    |
--| 08:09   | $5    | D    |
--| 08:13   | $1    | E    |
--| 08:17   | $6    | F    |
--+---------+-------+------+

> SELECT * FROM TABLE(
   TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES));
-- or with the named params
-- note: the DATA param must be the first
> SELECT * FROM TABLE(
   TUMBLE(
     DATA => TABLE Bid,
     TIMECOL => DESCRIPTOR(bidtime),
     SIZE => INTERVAL '10' MINUTES));
--+---------+-------+------+--------------+------------+--------------+
--| bidtime | price | item | window_start | window_end | window_time  |
--+---------+-------+------+--------------+------------+--------------+
--| 08:07   | $2    | A    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:20      | 08:19:59.999 |
--| 08:05   | $4    | C    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:09   | $5    | D    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:13   | $1    | E    | 08:10        | 08:20      | 08:19:59.999 |
--| 08:17   | $6    | F    | 08:10        | 08:20      | 08:19:59.999 |
--+---------+-------+------+--------------+------------+--------------+

-- apply aggregation on the tumbling windowed table
> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
--+--------------+------------+-------+
--| window_start | window_end | price |
--+--------------+------------+-------+
--| 08:00        | 08:10      | $11   |
--| 08:10        | 08:20      | $10   |
--+--------------+------------+-------+
```


### HOP

The `HOP` function assigns elements to windows of fixed length. Similar to a `TUMBLE` windowing function, the size of the windows is configured by the window size parameter. An additional window slide parameter controls how frequently a hopping window is started. Hence, hopping windows can be overlapping if the slide is smaller than the window size. In this case elements are assigned to multiple windows. Hopping windows is also known as "sliding windows".

For example, you could have windows of size 10 minutes that slides by 5 minutes. With this you get every 5 minutes a window that contains the events that arrived during the last 10 minutes as depicted by the following figure.

{{< img src="/fig/sliding-windows.svg" alt="Hopping windows" width="70%">}}

The `HOP` function assigns windows that cover rows within the interval of size and shifting every slide based on a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column. The return value of `HOP` is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after windowing TVF.

`HOP` takes three required parameters.

```sql
HOP(TABLE data, DESCRIPTOR(timecol), slide, size [, offset ])
```

- `data`: is a table parameter that can be any relation with an time attribute column.
- `timecol`: is a column descriptor indicating which [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column of data should be mapped to hopping windows.
- `slide`: is a duration specifying the duration between the start of sequential hopping windows
- `size`: is a duration specifying the width of the hopping windows.

Here is an example invocation on the `Bid` table:

```sql
> SELECT * FROM TABLE(
    HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES));
-- or with the named params
-- note: the DATA param must be the first
> SELECT * FROM TABLE(
    HOP(
      DATA => TABLE Bid,
      TIMECOL => DESCRIPTOR(bidtime),
      SLIDE => INTERVAL '5' MINUTES,
      SIZE => INTERVAL '10' MINUTES));
--+---------+-------+------+--------------+------------+----------------+
--| bidtime | price | item | window_start | window_end |  window_time   |
--+---------+-------+------+--------------+------------+----------------+
--| 08:07   | $2    | A    | 08:00        | 08:10      | 08:09:59.999   |
--| 08:07   | $2    | A    | 08:05        | 08:15      | 08:14:59.999   |
--| 08:11   | $3    | B    | 08:05        | 08:15      | 08:14:59.999   |
--| 08:11   | $3    | B    | 08:10        | 08:20      | 08:19:59.999   |
--| 08:05   | $4    | C    | 08:00        | 08:10      | 08:09:59.999   |
--| 08:05   | $4    | C    | 08:05        | 08:15      | 08:14:59.999   |
--| 08:09   | $5    | D    | 08:00        | 08:10      | 08:09:59.999   |
--| 08:09   | $5    | D    | 08:05        | 08:15      | 08:14:59.999   |
--| 08:13   | $1    | E    | 08:05        | 08:15      | 08:14:59.999   |
--| 08:13   | $1    | E    | 08:10        | 08:20      | 08:19:59.999   |
--| 08:17   | $6    | F    | 08:10        | 08:20      | 08:19:59.999   |
--| 08:17   | $6    | F    | 08:15        | 08:25      | 08:24:59.999   |
--+---------+-------+------+--------------+------------+----------------+

-- apply aggregation on the hopping windowed table
> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
--+---------------+------------+-------+
--|  window_start | window_end | price |
--+---------------+------------+-------+
--| 08:00         | 08:10      | $11   |
--| 08:05         | 08:15      | $15   |
--| 08:10         | 08:20      | $10   |
--| 08:15         | 08:25      | $6    |
--+---------------+------------+-------+
```

### CUMULATE

Cumulating windows are very useful in some scenarios, such as tumbling windows with early firing in a fixed window interval. For example, a daily dashboard draws cumulative UVs from 00:00 to every minute, the UV at 10:00 represents the total number of UV from 00:00 to 10:00. This can be easily and efficiently implemented by CUMULATE windowing.

The `CUMULATE` function assigns elements to windows that cover rows within an initial interval of step size, and expanding to one more step size (keep window start fixed) every step until to the max window size.
You can think `CUMULATE` function as applying `TUMBLE` windowing with max window size first, and split each tumbling windows into several windows with same window start and window ends of step-size difference. So cumulating windows do overlap and don't have a fixed size.

For example, you could have a cumulating window for 1 hour step and 1 day max size, and you will get windows: `[00:00, 01:00)`, `[00:00, 02:00)`, `[00:00, 03:00)`, ..., `[00:00, 24:00)` for every day.

{{< img src="/fig/cumulating-windows.png" alt="Cumulating Windows" width="70%">}}

The `CUMULATE` functions assigns windows based on a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column. The return value of `CUMULATE` is a new relation that includes all columns of original relation as well as additional 3 columns named "window_start", "window_end", "window_time" to indicate the assigned window. The original time attribute "timecol" will be a regular timestamp column after window TVF.

`CUMULATE` takes three required parameters.

```sql
CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size)
```

- `data`: is a table parameter that can be any relation with an time attribute column.
- `timecol`: is a column descriptor indicating which [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) column of data should be mapped to tumbling windows.
- `step`: is a duration specifying the increased window size between the end of sequential cumulating windows.
- `size`: is a duration specifying the max width of the cumulating windows. size must be an integral multiple of step .

Here is an example invocation on the Bid table:

```sql
> SELECT * FROM TABLE(
    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES));
-- or with the named params
-- note: the DATA param must be the first
> SELECT * FROM TABLE(
    CUMULATE(
      DATA => TABLE Bid,
      TIMECOL => DESCRIPTOR(bidtime),
      STEP => INTERVAL '2' MINUTES,
      SIZE => INTERVAL '10' MINUTES));
--+---------+-------+------+--------------+------------+--------------+
--| bidtime | price | item | window_start | window_end | window_time  |
--+---------+-------+------+--------------+------------+--------------+
--| 08:07   | $2    | A    | 08:00        | 08:08      | 08:07:59.999 |
--| 08:07   | $2    | A    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:12      | 08:11:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:14      | 08:13:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:16      | 08:15:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:18      | 08:17:59.999 |
--| 08:11   | $3    | B    | 08:10        | 08:20      | 08:19:59.999 |
--| 08:05   | $4    | C    | 08:00        | 08:06      | 08:05:59.999 |
--| 08:05   | $4    | C    | 08:00        | 08:08      | 08:07:59.999 |
--| 08:05   | $4    | C    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:09   | $5    | D    | 08:00        | 08:10      | 08:09:59.999 |
--| 08:13   | $1    | E    | 08:10        | 08:14      | 08:13:59.999 |
--| 08:13   | $1    | E    | 08:10        | 08:16      | 08:15:59.999 |
--| 08:13   | $1    | E    | 08:10        | 08:18      | 08:17:59.999 |
--| 08:13   | $1    | E    | 08:10        | 08:20      | 08:19:59.999 |
--| 08:17   | $6    | F    | 08:10        | 08:18      | 08:17:59.999 |
--| 08:17   | $6    | F    | 08:10        | 08:20      | 08:19:59.999 |
--+---------+-------+------+--------------+------------+--------------+

-- apply aggregation on the cumulating windowed table
> SELECT window_start, window_end, SUM(price)
  FROM TABLE(
    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end;
--+---------------+------------+-------+
--|  window_start | window_end | price |
--+---------------+------------+-------+
--| 08:00         | 08:06      | $4    |
--| 08:00         | 08:08      | $6    |
--| 08:00         | 08:10      | $11   |
--| 08:10         | 08:12      | $3    |
--| 08:10         | 08:14      | $4    |
--| 08:10         | 08:16      | $4    |
--| 08:10         | 08:18      | $10   |
--| 08:10         | 08:20      | $10   |
--+---------------+------------+-------+
```

{{< top >}}
