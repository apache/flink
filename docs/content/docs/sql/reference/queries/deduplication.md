---
title: "Deduplication"
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

# Deduplication
{{< label Batch >}} {{< label Streaming >}}

Deduplication removes rows that duplicate over a set of columns, keeping only the first one or the last one. In some cases, the upstream ETL jobs are not end-to-end exactly-once; this may result in duplicate records in the sink in case of failover. However, the duplicate records will affect the correctness of downstream analytical jobs - e.g. `SUM`, `COUNT` - so deduplication is needed before further analysis.

Flink uses `ROW_NUMBER()` to remove duplicates, just like the way of Top-N query. In theory, deduplication is a special case of Top-N in which the N is one and order by the processing time or event time.

The following shows the syntax of the Deduplication statement:

```sql
SELECT [column_list]
FROM table_name
QUALIFY ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]] ORDER BY time_attr [asc|desc]) = 1
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns, i.e. the deduplicate key.
- `ORDER BY time_attr [asc|desc]`: Specifies the ordering column, it must be a [time attribute]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}). Currently Flink supports [processing time attribute]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}#processing-time) and [event time attribute]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}#event-time). Ordering by ASC means keeping the first row, ordering by DESC means keeping the last row.
- `WHERE rownum = 1`: The `rownum = 1` is required for Flink to recognize this query is deduplication.

{{< hint info >}}
Note: the above pattern must be followed exactly, otherwise the optimizer won’t be able to translate the query.
{{< /hint >}}

The following examples show how to specify SQL queries with Deduplication on streaming tables.

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

The `DEDUPLICATE` process table function (PTF) keeps the first or the last row per key of an append-only or updating table, as long as the state of the key is retained. Rows are ordered by arrival or by `sequence_by` columns.

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
| `input` | Yes | The input table, append-only or updating. `PARTITION BY` is required and its columns are the key of the output. For updating input, the `PARTITION BY` columns must contain an upsert key of the input.|
| `keep` | No | `'first'` (default) keeps the earliest arrival or the lowest `sequence_by` value, `'last'` keeps the latest arrival or the highest. Must be a literal. Arrival order is not deterministic across reruns, use `sequence_by` for a repeatable result. |
| `sequence_by` | No | One or more columns that order the records of a key instead of their arrival, ascending, with strings and binaries compared lexicographically. The first column decides, later columns break ties, and `NULL` orders below every other value. On a tie, the later arrival wins for `'last'` and the earlier one for `'first'`. Columns must be of a type that `ORDER BY` supports, and not part of `PARTITION BY`. |
| `state_ttl` | No | How long the state of an idle key is retained, as an `INTERVAL` literal. Every record of a key, including a dropped duplicate, restarts the TTL. Defaults to `table.exec.state.ttl`, and `INTERVAL '0' SECOND` disables expiry. After the state of a key expires, its next record is emitted as `+I` again. |
| `on_time` | No | A time attribute to forward as a `rowtime` column. Only with `keep => 'first'` and no `sequence_by`. See [Keep time attributes](#keep-time-attributes). |
| `uid` | No | A unique identifier for the PTF invocation. Defaults to `DEDUPLICATE`. Can be omitted if the PTF is invoked only once per job. See [Query Evolution with UIDs]({{< ref "docs/dev/table/functions/ptfs" >}}#query-evolution-with-uids). |


### Output

The output columns are the `PARTITION BY` columns, in the order listed, followed by the remaining input columns. Time attributes are emitted as regular timestamps.

The result has one row per key. When a later row replaces the kept one, the row of that key is updated, so the result must be written to a table that accepts updates, for example one with a primary key on the `PARTITION BY` columns.

On append-only input with `keep => 'first'` and no `sequence_by`, the kept row of a key never changes, so the result only contains inserts and can be written to an append-only table.

{{< hint info >}}
The first record of a key is emitted as `+I`. A record that replaces the kept one is emitted as `+U`, preceded by `-U` if the consumer needs retractions. A record that equals the kept one, or does not replace it, emits nothing. On updating input, `+U` is handled as a new record, `-U` is ignored, and `-D` removes the kept row, regardless of `sequence_by`, and is emitted as `-D` with that row. A retraction does not fall back to an earlier row, so the next record of the key is emitted as `+I`. See [Updates and Changelogs]({{< ref "docs/dev/table/functions/ptfs" >}}#updates-and-changelogs).
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

In external systems, at-least-once delivery, retries, and reprocessing after a failure can write the same row more than once. To drop these copies, partition by the columns that identify a row and keep the default `keep => 'first'`.

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
WITH unique_clicks AS (
  SELECT * FROM DEDUPLICATE(
    input => TABLE clicks PARTITION BY event_id,
    on_time => DESCRIPTOR(ts))
)
SELECT window_start, COUNT(*) AS events
FROM TUMBLE(TABLE unique_clicks, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES)
GROUP BY window_start, window_end;
```

| Result          |
|:----------------|
| `(10:00, 2)`    |
| `(10:10, 1)`    |

The repeated `e1` is not counted. The first copy of a row is kept even if it arrives late, so a later window can drop it.

#### Updating input

A CDC table with primary key `customer_id`:

| `customers_cdc`          |
|:-------------------------|
| `+I(c1, Berlin, 100)`    |
| `+U(c1, Munich, 120)`    |
| `+U(c1, Hamburg, 110)`   |
| `-D(c1)`                 |
| `+U(c1, Bremen, 115)`    |

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE customers_cdc PARTITION BY customer_id,
  keep => 'last',
  sequence_by => DESCRIPTOR(lsn))
```

| Changes of the result    |                          |
|:-------------------------|:-------------------------|
| `+I(c1, Berlin, 100)`    |                          |
| `+U(c1, Munich, 120)`    |                          |
| `-D(c1, Munich, 120)`    | `Hamburg` → dropped      |
| `+I(c1, Bremen, 115)`    | no state → first record  |

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
