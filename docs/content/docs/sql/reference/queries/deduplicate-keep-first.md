---
title: "Deduplicate Keep First"
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

# Deduplicate Keep First

{{< label Streaming >}}

Flink SQL provides the `DEDUPLICATE_KEEP_FIRST` process table function (PTF) for removing duplicate rows while keeping only the **first** record per key. Its output is always **insert-only**, which makes it a concise alternative to the `ROW_NUMBER()` [deduplication]({{< ref "docs/sql/reference/queries/deduplication" >}}) pattern with more configuration options.

| Function | Description |
|:---------|:------------|
| [DEDUPLICATE_KEEP_FIRST](#deduplicate_keep_first) | Keeps the first record per key as an append-only table (first arrival, or earliest event time with `on_time`) |

## DEDUPLICATE_KEEP_FIRST

The `DEDUPLICATE_KEEP_FIRST` PTF keeps the first record per key and drops every later record for that key. It supports two ordering modes:

* **Processing time (default):** without `on_time`, the first record *observed* for a key is emitted; later records for that key are dropped while its state exists. This requires no watermark or event-time attribute.
* **Event time:** with `on_time`, the record with the *smallest event time* per key is kept and emitted once the watermark makes the choice final. A record arriving later but carrying an earlier event time replaces the candidate before finalization; records that can no longer become the earliest are dropped as late.

The input may be an append-only or updating table; the output is append-only in every case.

### Syntax

```sql
SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE source_table [PARTITION BY (key_col [, key_col2 ...])],
  [state_ttl => <interval>,]
  [reset_ttl_on_duplicate => <boolean>,]
  [on_time => DESCRIPTOR(rowtime_column),]
  [uid => <string>]
)
```

`on_time` and `uid` are system arguments and always come last in the signature, which matters when arguments are passed by position.

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `input` | Yes | The input table (append-only or updating). Use `PARTITION BY` to deduplicate per key; all rows for a key are routed to the same parallel instance. Without `PARTITION BY`, the whole input is a single group processed at parallelism 1, so only the first row of the entire stream is emitted and every later row is dropped regardless of its content. |
| `state_ttl` | No | An `INTERVAL` giving the processing-time retention for the per-key deduplication state. After a key's state expires, a later record for that key starts a new deduplication period and may be emitted again. If omitted, falls back to `table.exec.state.ttl`; at its default of 0, state is retained indefinitely, but a retention can still be introduced later. `state_ttl => INTERVAL '0'` also retains state indefinitely, but excludes the TTL from the state layout, so a retention cannot be introduced later without losing the state. See [State TTL]({{< ref "docs/dev/table/functions/ptfs" >}}#state-ttl). |
| `reset_ttl_on_duplicate` | No | Whether a later duplicate refreshes the key's `state_ttl`. Defaults to `TRUE`, so retention tracks the most recent occurrence of a key. Only meaningful when a TTL applies, whether set through `state_ttl` or inherited from `table.exec.state.ttl`. |
| `on_time` | No | A `DESCRIPTOR` naming a single watermarked column. When provided, the row with the smallest event time per key is kept and emitted once the watermark passes that timestamp (late rows are dropped). When omitted, the function keeps the first row observed for a key (arrival order). Requires append-only input; combining `on_time` with an updating input is rejected at planning time. |
| `uid` | No | A unique identifier for the function's state in checkpoints and savepoints. Defaults to the function name, so it is required when `DEDUPLICATE_KEEP_FIRST` is called more than once in the same job. See [Query Evolution with UIDs]({{< ref "docs/dev/table/functions/ptfs" >}}#query-evolution-with-uids). |

### Output Schema

When `PARTITION BY` is used, the partition key columns are prepended to the output, followed by the remaining (non-key) input columns. In event-time mode a rowtime column is appended for (potentially following) time-based operations.

```
[partition_key_columns] + [remaining_input_columns]
```

### Examples

#### Keyed keep-first (processing time)

```sql
-- Input (append-only):
-- +I[user_name:'Vas', action:'login']
-- +I[user_name:'Vas', action:'click']

SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE user_events PARTITION BY user_name
)

-- Output (insert-only):
-- +I[user_name:'Vas', action:'login']
```

The first row for `Vas` is emitted; the later `click` on the same key is dropped.

#### Exact duplicates (all columns as key)

```sql
-- Input (append-only):
-- +I[user_name:'Vas', action:'login']
-- +I[user_name:'Vas', action:'login']
-- +I[user_name:'Vas', action:'click']

SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE user_events PARTITION BY (user_name, action)
)

-- Output (insert-only):
-- +I[user_name:'Vas', action:'login']
-- +I[user_name:'Vas', action:'click']
```

Partitioning by every column drops only exact duplicate rows while still distributing the work across parallel instances.

#### Bounded state with state_ttl

```sql
-- Input (append-only):
-- +I[user_name:'Vas', action:'login']
-- +I[user_name:'Vas', action:'click']
-- +I[user_name:'Vas', action:'view']

SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE user_events PARTITION BY user_name,
  state_ttl => INTERVAL '5' SECOND,
  reset_ttl_on_duplicate => FALSE
)

-- Output (insert-only):
-- +I[user_name:'Vas', action:'login']
```

`state_ttl` bounds how long per-key state is retained (processing time). While the state exists, duplicates are dropped and only the first row per key is emitted; after the state expires, a later record for the key starts a new deduplication period. With the default `reset_ttl_on_duplicate => TRUE`, each dropped duplicate refreshes the retention window; with `FALSE`, retention is measured from the first record for the key.

#### Earliest by event time

```sql
-- Source declares: WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
-- Input (ts = event time):
-- +I[user_name:'Vas', action:'login', ts:3.000]
-- +I[user_name:'Vas', action:'click', ts:1.000]
-- +I[user_name:'Vas', action:'view',  ts:5.000]

SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST(
  input   => TABLE user_events PARTITION BY user_name,
  on_time => DESCRIPTOR(ts)
)

-- Output (insert-only, once the watermark finalizes the earliest):
-- +I[user_name:'Vas', action:'click']
```

With `on_time`, the record with the smallest event time per key is kept (here `click` at `ts 1.000`, not `login` which arrived first) and emitted once the watermark passes that timestamp. Late events (i.e. events with timestamp smaller or equal to the received watermark) are not taken into consideration.

#### Updating input

```sql
-- Input (updating changelog):
-- -D[user_name:'Vas', action:'logout']  key not seen yet         -> ignored
-- +I[user_name:'Vas', action:'login']   first record for the key -> emitted
-- +I[user_name:'Vas', action:'login']   duplicate                -> swallowed
-- -U[user_name:'Vas', action:'login']   retraction               -> swallowed
-- +U[user_name:'Vas', action:'click']   update                   -> swallowed
-- -D[user_name:'Vas', action:'click']   delete                   -> swallowed

SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE user_events PARTITION BY user_name
)

-- Output (insert-only):
-- +I[user_name:'Vas', action:'login']
```

The input may be an [updating changelog]({{< ref "docs/dev/table/functions/ptfs" >}}#change-data-capture-basics) containing inserts (`+I`), update-befores (`-U`), update-afters (`+U`), and deletes (`-D`). `DEDUPLICATE_KEEP_FIRST` keeps the first `+I` or `+U` observed per key and swallows every later change to that key (duplicate, `-U`, `+U`, `-D`), so the output stays insert-only. A `-U` or `-D` for a key that has not been seen yet is ignored and does not mark the key as seen, so the next `+I` or `+U` for that key is still emitted. Event-time mode (`on_time`) is not supported with updating input.

#### Table API

```java
Table userEvents = ...;

// Keep the first row per key (processing time).
Table result = userEvents.partitionBy($("user_name")).process("DEDUPLICATE_KEEP_FIRST");

// With optional arguments.
Table result = userEvents
    .partitionBy($("user_name"))
    .process(
        "DEDUPLICATE_KEEP_FIRST",
        lit(Duration.ofSeconds(5)).asArgument("state_ttl"),
        lit(false).asArgument("reset_ttl_on_duplicate"));
```

{{< top >}}
