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

The `DEDUPLICATE` process table function (PTF) keeps the first or the last row per key of an append-only table, as long as the state of the key is retained. Rows are ordered by arrival or by `sequence_by` columns.

### Syntax

```sql
SELECT * FROM DEDUPLICATE(
  input => TABLE source_table PARTITION BY key_col[, ...],
  [keep => 'first' | 'last',]
  [sequence_by => DESCRIPTOR(col[, ...]),]
  [state_ttl => INTERVAL '1' HOUR,]
  [uid => 'my-id']
)
```

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `input` | Yes | The append-only input table. `PARTITION BY` is required and its columns are the key of the output.|
| `keep` | No | `'first'` (default) keeps the earliest arrival or the lowest `sequence_by` value, `'last'` keeps the latest arrival or the highest. Must be a literal. Arrival order is not deterministic across reruns, use `sequence_by` for a repeatable result. |
| `sequence_by` | No | One or more columns that order the records of a key instead of their arrival, ascending, with strings and binaries compared lexicographically. The first column decides, later columns break ties, and `NULL` orders below every other value. On a tie, the later arrival wins for `'last'` and the earlier one for `'first'`. Columns must be numeric, string, binary, boolean or date-time, and not part of `PARTITION BY`. |
| `state_ttl` | No | How long the state of an idle key is retained, as an `INTERVAL` literal. Every record of a key, including a dropped duplicate, restarts the TTL. Defaults to `table.exec.state.ttl`, and `INTERVAL '0' SECOND` disables expiry. After the state of a key expires, its next record is emitted as `+I` again. |
| `uid` | No | A unique identifier for the PTF invocation. Defaults to `DEDUPLICATE`. Can be omitted if the PTF is invoked only once per job. See [Query Evolution with UIDs]({{< ref "docs/dev/table/functions/ptfs" >}}#query-evolution-with-uids). |


### Output

The output columns are the `PARTITION BY` columns, in the order listed, followed by the remaining input columns. Time attributes are emitted as regular timestamps.

The result has one row per key. When a later row replaces the kept one, the row of that key is updated, so the result must be written to a table that accepts updates, for example one with a primary key on the `PARTITION BY` columns. Currently this holds for every `keep` mode, including `keep => 'first'` without `sequence_by`, which never replaces a row.

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

`sequence_by` ranks the rows of a key by a column instead of by arrival, so a late, older row does not replace a newer one:

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
