---
title: "Interpreting Flink's Changelog"
weight: 3
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

# Interpreting Flink's Changelog

A Flink table can change over time: rows get inserted, updated, or deleted. This is internally encoded internally as changelogs inside the Flink SQL engine. When you consume that table as a stream — after writing the table to another system, through `toChangelogStream`, a table connector, or your own service reading the output — each row carries a change flag telling you what kind of change it represents. This page describes how to read that stream correctly so you're able to built a table representation out of it.

## The four row kinds

Every row in a changelog stream has a `RowKind`:

| Kind | Symbol | Meaning |
|---|---|---|
| `INSERT` | `+I` | A new row was added. |
| `UPDATE_BEFORE` | `-U` | The previous value of a row that is about to be updated. |
| `UPDATE_AFTER` | `+U` | The new value of a row that was updated. |
| `DELETE` | `-D` | A row was removed. |

## Changelog Modes

Under the hood, the Flink SQL engine is always operating with changelogs in one of three modes: append, upsert and retract. This depends on the operations present in your query and each of these modes has it's own characteristic.

**Append Mode `{+I}`**
- All messages are insert-only.
- Every insertion message is an immutable fact.
- Messages can be distributed in an arbitrary fashion across partitions and processors because they are unrelated.

**Upsert Mode `{+I, +U, -D}`**
- Messages can contain updates leading to an updating table.
- Updates are related using a key (i.e. the *upsert key*).
- Every message is either an upsert or delete message for a result under the upsert key.
- Messages for the same upsert key should land at the same partition and processor.
- Deletions can contain only values for upsert key columns (i.e. *partial deletes*) or values for
  all columns (i.e. *full deletes*).
- The mode is also known as *partial image* in the literature because `-U` messages are missing.

**Retract Mode `{+I, -U, +U, -D}`**
- Messages can contain updates leading to an updating table.
- Every insertion or update event is a fact that can be "undone" (i.e. retracted).
- Updates are related by all columns. In simplified words: The entire row is kind of the key but duplicates are supported.
  For example: `+I['Bob', 42]` is related to `-D['Bob', 42]` and `+U['Alice', 13]` is related to `-U['Alice', 13]`.
- Thus, every message is either an insertion (`+`) or its retraction (`-`).
- The mode is known as *full image* in the literature.

## Reading it as a consumer

To read a changelog correctly, you don't need to figure out which mode you're in — check one piece of metadata: whether the table has a primary key.

**If you have a primary key**, materialize rows in a map keyed by it:

- `+I`: add the row for that key.
- `+U`: replace the row for that key (add it, if you haven't seen that key before).
- `-D`: remove the row for that key.
- `-U`: ignore it. The `+U` for the same key replaces it right after.

**If you don't have a primary key**, there's no key to look up — match on the full row instead:

- `+I` or `+U`: add the row.
- `-U` or `-D`: remove the row with exactly this content. Flink sends the exact previous values on `-U`, so it matches the row you already added.

## Valid Changelog Examples

### With a primary key

#### Insert

```
Flink Changelog
-- +I[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

#### Update, upsert shape

```
Flink Changelog
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

#### Update, retract shape

```
Flink Changelog
-- -U[ id: 5, name: 'old' ]
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

#### Delete

```
Flink Changelog
-- -D[ id: 5, name: 'new' ]

Table
(empty)
```

#### Update with partial delete

```
Flink Changelog
-- +I[ id: 5, name: 'a' ]
-- +U[ id: 5, name: 'b' ]
-- -D[ id: 5, name: null ]

Table
(empty)
```

The `-D` only carries `id`; `name` arrives as `null` rather than `'b'`. The row is still removed correctly because removal is driven by the key alone. The engine tries to do this optimization when possible. As a consumer you should be aware: if you have a key, you might receive deletions with a complete or partial delete.

#### Distributed across partitions

Flink partitions the changelog by the primary key, so every event for a given key always lands on the same partition, in order:

```
Flink Changelog

Partition 0
-- -U[ id: 5, name: 'old' ]
-- +U[ id: 5, name: 'new' ]

Partition 1
-- +I[ id: 9, name: 'x' ]
-- -D[ id: 9, name: 'x' ]

Table, read while Partition 1's +I has landed but not yet its -D (transient)
-- [ id: 5, name: 'new' ]
-- [ id: 9, name: 'x' ]

Table, read after both partitions have delivered (correct)
-- [ id: 5, name: 'new' ]
```

Order is guaranteed within a partition, not across partitions. `id: 5`'s update and `id: 9`'s insert-then-delete never have to be reconciled with each other — each key's full history stays on one partition, in order. But partitions are still processed independently of each other, so reading the table mid-way, between Partition 1's two events, briefly shows `id: 9` even though it's ultimately deleted. That's transient, not a bug — it resolves once Partition 1's `-D` lands.

### Without a primary key

#### Insert

```
Flink Changelog
-- +I[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

#### Update

```
Flink Changelog
-- +U[ id: 5, name: 'old' ]
-- -U[ id: 5, name: 'old' ]
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

#### Delete

```
Flink Changelog
-- +I[ id: 5, name: 'new' ]
-- -D[ id: 5, name: 'new' ]

Table
(empty)
```

#### Distributed across partitions

Without a key, Flink partitions by the entire row instead — so only an event and an exact match of it (its own retraction, its own repeat) are guaranteed to land on the same partition. Two rows that happen to share `id` but differ elsewhere can land anywhere:

```
Flink Changelog

Partition 0
-- +U[ id: 5, name: 'old' ]
-- -U[ id: 5, name: 'old' ]

Partition 1
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

Both rows share `id: 5`, but they don't share a partition — `name` differs, and `name` is part of what's hashed. That's fine here because each partition only ever needs to match a row against its own exact retraction, never against a different row that happens to share part of its content.

Partitions are processed independently, with no ordering guarantee between them. A `-U` and the `+U` that replaces it can end up on different partitions, and from either partition's point of view they're just two unrelated records. If you read your materialized set while one has arrived and the other hasn't, you'll see an incorrect intermediate state — that's normal, not a bug. Only the result after every partition has delivered its events is guaranteed:

```
Flink Changelog

Partition 0
-- +U[ count: 1 ]
-- -U[ count: 1 ]

Partition 1
-- +U[ count: 2 ]

Table, read before Partition 0's -U arrives (transient)
-- [ count: 1 ]
-- [ count: 2 ]

Table, read after both partitions have delivered (correct)
-- [ count: 2 ]
```

## Observations

### Duplicate keys happen

Flink's `PRIMARY KEY` is `NOT ENFORCED`. Nothing stops an upstream job from producing two rows for the same key. A robust consumer keeps a small list per key rather than assuming exactly one.

```
Flink Changelog
-- +I[ id: 5, name: 'a' ]
-- +I[ id: 5, name: 'a' ]

Table
-- [ id: 5, name: 'a' ]
-- [ id: 5, name: 'a' ]
```

### Ordering only holds within a key

`-U` and `+U` for the same key are only guaranteed adjacent if the stream is partitioned by that key. An upsert connector requires this by design — that's why it's safe to drop `-U` when materializing. A plain, unpartitioned changelog stream gives no such guarantee.

```
Flink Changelog
-- -U[ id: 5, name: 'old' ]
-- -U[ id: 9, name: 'old' ]
-- +U[ id: 9, name: 'new' ]
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
-- [ id: 9, name: 'new' ]
```

Two different keys' events interleaved, but each key still ends up correct.

### `+I` for a new key is best effort

Both shapes use `+I` for the first row of a new key and `-D` for deletes. But detecting "this is the first row for this key" can be too expensive for Flink to calculate. Sometimes you'll see a `+U` as the first event instead.

```
Flink Changelog
-- +U[ id: 5, name: 'new' ]

Table
-- [ id: 5, name: 'new' ]
```

`id: 5` was never seen before, yet the first event for it is `+U`, not `+I`. Treat it the same as an insert.

### A delete may not carry the full row

Some upsert connectors negotiate `ChangelogMode.upsert(keyOnlyDeletes = true)`, meaning `-D` rows only carry the key columns — the rest of the row may be absent or null. Don't rely on non-key values in a delete.

```
Flink Changelog
-- +I[ id: 5, name: 'new' ]
-- -D[ id: 5, name: null ]

Table
(empty)
```

The row is still removed correctly by `id`. `name` being `null` on the delete doesn't mean the row's last known name was `null` — it's simply absent.

### Ranked results can look inconsistent mid-update — that's expected, not a bug

A `ROW_NUMBER()`, `RANK()`, or `ORDER BY ... LIMIT` result can reassign the same rank to a different row as part of a single underlying change — one row's rank changes because another row's did, under a different key. Because they're different keys, the two rows' events can land on different partitions, with no ordering between them. If `id: 6`'s `+U` arrives before `id: 5`'s own resolving `+U` does, you'll briefly see two rows claiming the same rank. That's transient, not a bug — it resolves once `id: 5`'s own `+U` lands.

```
Flink Changelog

Partition 0
-- +U[ id: 6, rank: 1, name: 'B' ]

Partition 1
-- +U[ id: 5, rank: 1, name: 'A' ]
-- -U[ id: 5, rank: 1, name: 'A' ]
-- +U[ id: 5, rank: 2, name: 'A' ]

Table, read after Partition 0's +U but before Partition 1's last +U (transient)
-- [ id: 5, rank: 1, name: 'A' ]
-- [ id: 6, rank: 1, name: 'B' ]

Table, read after every event has landed (correct)
-- [ id: 5, rank: 2, name: 'A' ]
-- [ id: 6, rank: 1, name: 'B' ]
```
