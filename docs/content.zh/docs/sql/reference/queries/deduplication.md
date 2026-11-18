---
title: "去重"
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

# 去重
{{< label Batch >}} {{< label Streaming >}}

去重是去掉重复的行，只保留第一或者最后一行。有时，上游的 ETL 生成的数据不是端到端精确一次的。在发生故障恢复后，可能会导致结果下游中出现重复记录。这些重复记录会影响下游的分析工作，比如：`SUM`、`COUNT`的结果会偏大，所以需要先去重再进行下一步的分析。

Flink 使用 `ROW_NUMBER()` 去除重复数据，就像 Top-N 查询一样。其实，去重就是 Top-N 在 N 为 1 时的特例，并且去重必须要求按照[处理或者事件时间]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}})排序。

下面的例子展示了去重语句的语法：

```sql
SELECT [column_list]
FROM table_name
QUALIFY ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]] ORDER BY time_attr [asc|desc]) = 1
```

**参数说明：**

- `ROW_NUMBER()`：为每一行分配一个唯一且连续的数字，从 1 开始。
- `PARTITION BY col1[, col2...]`：指定分区键，即需要去重的键。
- `ORDER BY time_attr [asc|desc]`：指定排序列，必须是 [时间属性]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}})。目前 Flink 支持 [处理时间属性]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}#processing-time) 和 [事件时间属性]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}#event-time)。Order by ASC 保留第一行，DESC 保留最后一行。
- `WHERE rownum = 1`：Flink 需要这个条件来识别去重语句。

{{< hint info >}}
注意：上述格式必须严格遵守，否则优化器无法识别。
{{< /hint >}}

下面的例子展示了在流表上如何指定去重 SQL 查询：

```sql
CREATE TABLE Orders (
  order_id  STRING,
  `user`      STRING,
  product     STRING,
  num         BIGINT,
  proctime AS PROCTIME()
) WITH (...);

-- remove duplicate rows on order_id and keep the first occurrence row,
-- because there shouldn't be two orders with the same order_id.
SELECT order_id, user, product, num
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num
  FROM Orders)
WHERE row_num = 1
```

## DEDUPLICATE

{{< label Streaming >}}

The `DEDUPLICATE` process table function (PTF) keeps the first or the last row per key of an append-only table, as long as the state of the key is retained. Unlike the `ROW_NUMBER()` pattern above, it needs no time attribute. Rows are ordered by arrival or by `sequence_by` columns.

### Syntax

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE source_table PARTITION BY key_col[, ...],
  [keep => 'first' | 'last',]
  [sequence_by => DESCRIPTOR(col[, ...]),]
  [state_ttl => INTERVAL '1' HOUR,]
  [reset_ttl_on_duplicate => true | false,]
  [uid => 'my-id']
)
```

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `input` | Yes | The append-only input table. `PARTITION BY` is required and its columns are the key of the output. `ORDER BY` is not supported, use `sequence_by` instead. |
| `keep` | No | `'first'` (default) keeps the earliest arrival or the smallest `sequence_by` value, `'last'` keeps the latest arrival or the greatest. Must be a literal. Arrival order is not deterministic across reruns, use `sequence_by` for a repeatable result. |
| `sequence_by` | No | One or more columns that order the records of a key instead of their arrival. The first column decides, later columns break ties, and `NULL` orders below every other value. On a tie, the later arrival wins for `'last'` and the earlier one for `'first'`. Columns must be numeric, string, binary, boolean or date-time, and not part of `PARTITION BY`. |
| `state_ttl` | No | The minimum time the state of an idle key is retained, in processing time, as a non-negative `INTERVAL` literal. If omitted, `table.exec.state.ttl` applies, including later changes to it. `INTERVAL '0'` retains state indefinitely; the TTL is then left out of the state, so one cannot be added later without losing the state. When the state of a key expires, its next record is treated as the first one and emits `+I` again, so a sink without an upsert key can hold several rows for that key. |
| `reset_ttl_on_duplicate` | No | Whether a record that is dropped refreshes the TTL of its key. Defaults to `true`. Has no effect without a TTL. |
| `uid` | No | A unique identifier for the PTF invocation. Defaults to `DEDUPLICATE`. See [Multiple Invocations]({{< ref "docs/sql/reference/queries/changelog" >}}#multiple-invocations). |

System arguments such as `uid` come last.

### Output

The output columns are the `PARTITION BY` columns, in the order listed, followed by the remaining input columns. Time attributes are emitted as regular timestamps.

The result is an updating table whose key is the `PARTITION BY` columns. The first record of a key emits `+I`. A record that replaces the kept one emits `+U`, preceded by `-U` if the consumer needs retractions. A record that equals the kept one, or that does not replace it, emits nothing. With `keep => 'first'` and no `sequence_by`, only `+I` is emitted. The output is currently declared as updating in every case, so a sink that cannot consume updates is rejected at planning time. See [Updates and Changelogs]({{< ref "docs/dev/table/functions/ptfs" >}}#updates-and-changelogs) for the meaning of the change flags.

### Examples

#### Keep the first or the last record

```sql
CREATE TABLE user_events (user_id STRING, event STRING) WITH (...);
-- (u1, login), (u2, login), (u1, click)

SELECT * FROM DEDUPLICATE(
  input => TABLE user_events PARTITION BY user_id);

-- +I[u1, login]
-- +I[u2, login]

SELECT * FROM DEDUPLICATE(
  input => TABLE user_events PARTITION BY user_id,
  keep => 'last');

-- +I[u1, login]
-- +I[u2, login]
-- +U[u1, click]     (an upsert consumer; a retract consumer also gets -U[u1, login] before it)
```

#### Order by a column

```sql
CREATE TABLE customer_changes (customer_id STRING, city STRING, lsn BIGINT) WITH (...);
-- (c1, Berlin, 100), (c1, Munich, 120), (c1, Hamburg, 110)

SELECT * FROM DEDUPLICATE(
  input => TABLE customer_changes PARTITION BY customer_id,
  keep => 'last',
  sequence_by => DESCRIPTOR(lsn),
  state_ttl => INTERVAL '1' DAY);

-- +I[c1, Berlin, 100]
-- +U[c1, Munich, 120]     (The entry (c1, Hamburg, 110) is older than the kept, row -> dropped)
```

#### Composite key

```sql
CREATE TABLE sessions (user_id STRING, page STRING, device STRING) WITH (...);
-- (u1, a, web), (u1, b, mobile), (u1, c, web)

SELECT * FROM DEDUPLICATE(
  input => TABLE sessions PARTITION BY (device, user_id),
  keep => 'last');

-- columns: device, user_id, page
-- +I[web, u1, a]
-- +I[mobile, u1, b]
-- +U[web, u1, c]
```

#### Table API

```java
// Keep the first record of each payment
Table result = payments
    .partitionBy($("payment_id"))
    .deduplicate();

// Keep the record with the greatest lsn per customer
Table result = customerChanges
    .partitionBy($("customer_id"))
    .deduplicate(
        lit("last").asArgument("keep"),
        descriptor("lsn").asArgument("sequence_by"),
        lit(Duration.ofDays(1)).asArgument("state_ttl"));
```

{{< top >}}
