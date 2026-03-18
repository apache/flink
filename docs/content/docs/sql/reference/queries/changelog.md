---
title: "Changelog Conversion"
weight: 8
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

# Changelog Conversion

{{< label Streaming >}}

Flink SQL provides built-in process table functions (PTFs) for working with changelog streams.

| Function | Description |
|:---------|:------------|
| [TO_CHANGELOG](#to_changelog) | Converts a dynamic table into an append-only table with explicit operation codes |

<!-- Placeholder for future FROM_CHANGELOG function -->

## TO_CHANGELOG

The `TO_CHANGELOG` PTF converts a dynamic table (i.e. an updating table) into an append-only table with an explicit operation code column. Each input row - regardless of its original `RowKind` (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) - is emitted as an INSERT-only row with a string column indicating the original operation.

This is useful when you need to materialize changelog events into a downstream system that only supports appends (e.g., a message queue, log store, or append-only file sink). It is also useful to filter out certain types of updates, for example DELETEs.

### Syntax

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE source_table PARTITION BY key_col,
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP['INSERT', 'I', 'DELETE', 'D', ...]]
)
```

### Parameters

| Parameter    | Required | Description |
|:-------------|:---------|:------------|
| `input`      | Yes      | The input table. Must include `PARTITION BY` for parallel execution. Accepts insert-only, retract, and upsert tables. |
| `op`         | No       | A `DESCRIPTOR` with a single column name for the operation code column. Defaults to `op`. |
| `op_mapping` | No       | A `MAP<STRING, STRING>` mapping `RowKind` names to custom output codes. When provided, only mapped RowKinds are forwarded - unmapped events are dropped. |

#### Default op_mapping

When `op_mapping` is omitted, all four RowKinds are mapped to their standard names:

| RowKind         | Output value      |
|:----------------|:------------------|
| INSERT          | `'INSERT'`        |
| UPDATE_BEFORE   | `'UPDATE_BEFORE'` |
| UPDATE_AFTER    | `'UPDATE_AFTER'`  |
| DELETE          | `'DELETE'`        |

### Output Schema

The output columns are ordered as:

```
[partition_key_columns, op_column, remaining_columns]
```

All output rows have `INSERT` - the table is always append-only.

### Examples

#### Basic usage

```sql
-- Input: retract table from an aggregation
-- +I[id:1, name:'Alice', cnt:1]
-- +U[id:1, name:'Alice', cnt:2]
-- -D[id:2, name:'Bob',   cnt:1]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id
)

-- Output (append-only):
-- +I[id:1, op:'INSERT',       name:'Alice', cnt:1]
-- +I[id:1, op:'UPDATE_AFTER', name:'Alice', cnt:2]
-- +I[id:2, op:'DELETE',       name:'Bob',   cnt:1]
```

#### Custom operation column name

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id,
  op => DESCRIPTOR(operation)
)
-- The op column is now named 'operation' instead of 'op'
```

#### Custom operation codes with filtering

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id,
  op => DESCRIPTOR(op_code),
  op_mapping => MAP['INSERT', 'I', 'UPDATE_AFTER', 'U']
)
-- Only INSERT and UPDATE_AFTER events are forwarded
-- DELETE events are dropped (not in the mapping)
-- op_code values are 'I' and 'U' instead of full names
```

#### Table API

```java
// Default: adds 'op' column and supports all changelog modes
Table result = myTable.partitionBy($("id")).toChangelog();

// With custom parameters
Table result = myTable.partitionBy($("id")).toChangelog(
    descriptor("op_code").asArgument("op"),
    map("INSERT", "I", "UPDATE_AFTER", "U").asArgument("op_mapping")
);
```

{{< top >}}
