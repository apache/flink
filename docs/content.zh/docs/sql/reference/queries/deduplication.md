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

The `DEDUPLICATE` process table function (PTF) keeps the first or the last row per key of an append-only table, as long as the state of the key is retained. Rows are ordered by arrival or by `sequence_by` columns.

### Syntax

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE source_table PARTITION BY key_col[, ...],
  [keep => 'first' | 'last',]
  [sequence_by => DESCRIPTOR(col[, ...]),]
  [state_ttl => INTERVAL '1' HOUR,]
  [on_time => DESCRIPTOR(time_col),]
  [uid => 'my-id']
)
```

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `input` | Yes | The append-only input table. `PARTITION BY` is required and its columns are the key of the output.|
| `keep` | No | `'first'` (default) keeps the earliest arrival or the lowest `sequence_by` value, `'last'` keeps the latest arrival or the highest. Must be a literal. Arrival order is not deterministic across reruns, use `sequence_by` for a repeatable result. |
| `sequence_by` | No | One or more columns that order the records of a key instead of their arrival, ascending, with strings and binaries compared lexicographically. The first column decides, later columns break ties, and `NULL` orders below every other value. On a tie, the later arrival wins for `'last'` and the earlier one for `'first'`. Columns must be of a type that `ORDER BY` supports, and not part of `PARTITION BY`. |
| `state_ttl` | No | How long the state of an idle key is retained, as an `INTERVAL` literal. Every record of a key, including a dropped duplicate, restarts the TTL. Defaults to `table.exec.state.ttl`, and `INTERVAL '0' SECOND` disables expiry. After the state of a key expires, its next record is emitted as `+I` again. |
| `on_time` | No | A time attribute to forward as a `rowtime` column. Only with `keep => 'first'` and no `sequence_by`. See [Keep time attributes](#keep-time-attributes). |
| `uid` | No | A unique identifier for the PTF invocation. Defaults to `DEDUPLICATE`. Can be omitted if the PTF is invoked only once per job. See [Query Evolution with UIDs]({{< ref "docs/dev/table/functions/ptfs" >}}#query-evolution-with-uids). |


### Output

The output columns are the `PARTITION BY` columns, in the order listed, followed by the remaining input columns. Time attributes are emitted as regular timestamps.

The result has one row per key. When a later row replaces the kept one, the row of that key is updated, so the result must be written to a table that accepts updates, for example one with a primary key on the `PARTITION BY` columns.

With `keep => 'first'` and no `sequence_by`, the kept row of a key never changes, so the result only contains inserts and can be written to an append-only table.

{{< hint info >}}
The first record of a key is emitted as `+I`. A record that replaces the kept one is emitted as `+U`, preceded by `-U` if the consumer needs retractions. A record that equals the kept one, or does not replace it, emits nothing. See [Updates and Changelogs]({{< ref "docs/dev/table/functions/ptfs" >}}#updates-and-changelogs).
{{< /hint >}}

### Examples

#### Keep the first or the last record

Rows of `user_events` arrive in this order:

| `user_events`   |
|:----------------|
| `(u1, login)`   |
| `(u2, login)`   |
| `(u1, click)`   |

`keep` defaults to `'first'`, which keeps the first row of each user:

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE user_events PARTITION BY user_id)
```

| Result          |
|:----------------|
| `(u1, login)`   |
| `(u2, login)`   |

With `keep => 'last'`, a later row replaces the kept one:

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE user_events PARTITION BY user_id,
  keep => 'last')
```

| Result          |
|:----------------|
| `(u1, click)`   |
| `(u2, login)`   |

#### Order by a column

`sequence_by` orders the rows of a key by a column instead of by arrival, so a row that arrives late with a smaller value does not replace the kept one:

| `customer_changes`          |
|:----------------------------|
| `(c1, Berlin, 100)`         |
| `(c1, Munich, 120)`         |
| `(c1, Hamburg, 110)`        |

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE customer_changes PARTITION BY customer_id,
  keep => 'last',
  sequence_by => DESCRIPTOR(lsn),
  state_ttl => INTERVAL '1' DAY)
```

| Result                |
|:----------------------|
| `(c1, Munich, 120)`   |

`(c1, Hamburg, 110)` is dropped because its `lsn` is smaller than that of the kept row.

#### Deduplicate exact matches

At-least-once delivery, retries, and reprocessing after a failure can write the same row more than once. To drop these copies, partition by the columns that identify a row and keep the default `keep => 'first'`.

Here, a payment is delivered twice. Both copies share the `payment_id`, but each has its own `ingested_at`:

| `payments` (`payment_id`, `amount`, `ingested_at`) |
|:---------------------------------------------------|
| `(p1, 50, 10:00:01)`                               |
| `(p2, 20, 10:00:02)`                               |
| `(p1, 50, 10:00:07)`                               |

```sql
INSERT INTO unique_payments
SELECT * FROM DEDUPLICATE(
  input => TABLE payments PARTITION BY payment_id,
  state_ttl => INTERVAL '1' HOUR)
```

| Result                 |
|:-----------------------|
| `(p1, 50, 10:00:01)`   |
| `(p2, 20, 10:00:02)`   |

The second `p1` is dropped. Partition by the columns that are equal in every copy, here `payment_id`, and leave out columns that differ between copies, such as `ingested_at`.

This query:

- **Only emits inserts.** The first row of a key is emitted as soon as it arrives and never updated, so `unique_payments` can be an append-only table, and operations that require an insert-only input can consume the result.
- **Does not depend on watermarks.** No row waits for a watermark or is dropped as late.
- **Keeps one entry per key in state.** Set `state_ttl` to how long copies can keep arriving, for example the time a source needs to recover from a failure. A copy that arrives after its key expired is emitted again.

#### Keep time attributes

PTFs take an optional [`on_time`]({{< ref "docs/dev/table/functions/ptfs" >}}#time) argument to forward a time attribute: the output gets a `rowtime` column with the time of each row, which event-time operations such as window aggregations can use. For `DEDUPLICATE`, this works when the result only contains inserts, that is with `keep => 'first'` and no `sequence_by`:

| `clicks` (`event_id`, `ts`) |
|:----------------------------|
| `(e1, 10:01)`               |
| `(e2, 10:02)`               |
| `(e1, 10:03)`               |
| `(e3, 10:12)`               |

```sql
CREATE VIEW unique_clicks AS
SELECT * FROM DEDUPLICATE(
  input => TABLE clicks PARTITION BY event_id,
  on_time => DESCRIPTOR(ts));

SELECT window_start, COUNT(*) AS events
FROM TABLE(TUMBLE(TABLE unique_clicks, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end;
```

| Result          |
|:----------------|
| `(10:00, 2)`    |
| `(10:10, 1)`    |

The repeated `e1` is not counted. The first copy of a row is kept even if it arrives late, so a later window can drop it.

#### Composite key

The columns of `PARTITION BY` come first in the output, in the order listed:

| `sessions` (`user_id`, `page`, `device`) |
|:-----------------------------------------|
| `(u1, a, web)`                           |
| `(u1, b, mobile)`                        |
| `(u1, c, web)`                           |

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE sessions PARTITION BY (device, user_id),
  keep => 'last')
```

| Result (`device`, `user_id`, `page`) |
|:-------------------------------------|
| `(web, u1, c)`                       |
| `(mobile, u1, b)`                    |

#### Table API

```java
// Keep the first record of each payment
Table result = payments
    .partitionBy($("payment_id"))
    .deduplicate();

// Keep the record with the highest lsn per customer
Table result = customerChanges
    .partitionBy($("customer_id"))
    .deduplicate(
        lit("last").asArgument("keep"),
        descriptor("lsn").asArgument("sequence_by"),
        lit(Duration.ofDays(1)).asArgument("state_ttl"));
```

{{< top >}}
