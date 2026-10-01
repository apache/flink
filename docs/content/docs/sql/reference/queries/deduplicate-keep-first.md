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

Flink SQL provides the `DEDUPLICATE_KEEP_FIRST` process table function (PTF) for removing duplicate rows while keeping only the **first** record per key. Its output is always **insert-only**, which makes it a concise, append-in / append-out alternative to the `ROW_NUMBER()` [deduplication]({{< ref "docs/sql/reference/queries/deduplication" >}}) pattern.

| Function | Description |
|:---------|:------------|
| [DEDUPLICATE_KEEP_FIRST](#deduplicate_keep_first) | Keeps the first record per key as an insert-only stream (first arrival, or earliest event time with `on_time`) |

## DEDUPLICATE_KEEP_FIRST

The `DEDUPLICATE_KEEP_FIRST` PTF keeps the first record per key and drops every later record for that key. It supports two ordering modes:

* **Processing time (default):** without `on_time`, the first record *observed* for a key is emitted; later records for that key are dropped while its state exists. This requires no watermark or event-time attribute.
* **Event time:** with `on_time`, the record with the *smallest event time* per key is kept and emitted once the watermark makes the choice final. A record arriving later but carrying an earlier event time replaces the candidate before finalization; records that can no longer become the earliest are dropped as late.

The input may be insert-only or updating; the output is insert-only in every case.

### Syntax

```sql
SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE source_table [PARTITION BY key_col [, key_col2 ...]],
  [on_time => DESCRIPTOR(rowtime_column),]
  [state_ttl => <interval>,]
  [reset_ttl_on_duplicate => <boolean>]
)
```

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `input` | Yes | The input table (insert-only or updating). Use `PARTITION BY` to deduplicate per key; all rows for a key are routed to the same parallel instance. Without `PARTITION BY`, the whole input is a single group processed at parallelism 1, so only the first row of the entire stream is emitted and every later row is dropped regardless of its content. |
| `on_time` | No | A `DESCRIPTOR` naming a single rowtime attribute. When provided, the row with the smallest event time per key is kept and emitted once the watermark passes that timestamp (late rows are dropped). When omitted, the function keeps the first row observed for a key (arrival order). Requires insert-only input; combining `on_time` with an updating input is rejected at planning time. |
| `state_ttl` | No | An `INTERVAL` giving the processing-time retention for the per-key deduplication state. Defaults to no TTL (state retained indefinitely). After a key's state expires, a later record for that key starts a new deduplication period and may be emitted again. `INTERVAL '0'` disables retention. |
| `reset_ttl_on_duplicate` | No | Whether a later duplicate refreshes the key's `state_ttl`. Defaults to `TRUE`, so retention tracks the most recent occurrence of a key. Only meaningful together with `state_ttl`. |

### Output Schema

When `PARTITION BY` is used, the partition key columns are prepended to the output, followed by the remaining (non-key) input columns. In event-time mode a rowtime column is appended.

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

#### Whole input, no PARTITION BY

```sql
-- Input (append-only):
-- +I[user_name:'Vas',   action:'login']
-- +I[user_name:'Alice', action:'click']
-- +I[user_name:'Bob',   action:'view']

SELECT * FROM DEDUPLICATE_KEEP_FIRST(
  input => TABLE user_events
)

-- Output (insert-only):
-- +I[user_name:'Vas', action:'login']
```

Without `PARTITION BY` the whole input is a single group at parallelism 1, so only the first row of the entire stream is emitted; every later row is dropped regardless of its content.

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

With `on_time`, the record with the smallest event time per key is kept (here `click` at `ts 1.000`, not `login` which arrived first) and emitted once the watermark passes that timestamp.

#### Updating input

```sql
-- Input (updating changelog):
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

The input may be an updating changelog. `DEDUPLICATE_KEEP_FIRST` keeps the first record observed per key and swallows every later change to that key (duplicate, `-U`, `+U`, `-D`), so the output stays insert-only. Event-time mode (`on_time`) is not supported with updating input.

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
