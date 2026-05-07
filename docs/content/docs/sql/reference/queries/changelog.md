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
| [FROM_CHANGELOG](#from_changelog) | Converts an append-only table with operation codes into a (potentially updating) dynamic table |
| [TO_CHANGELOG](#to_changelog) | Converts a dynamic table into an append-only table with explicit operation codes |

## FROM_CHANGELOG

The `FROM_CHANGELOG` PTF converts an append-only table with an explicit operation code column into a (potentially updating) dynamic table. Each input row is expected to have a string column that indicates the change operation. The operation column is interpreted by the engine and removed from the output.

This is useful when consuming Change Data Capture (CDC) streams from systems like Debezium where events arrive as flat append-only records with an explicit operation field. It's also useful to be used in combination with the TO_CHANGELOG function, when converting the append-only table back into an updating table after doing some specific transformation to the events.

Note: This version requires that your CDC data encodes updates using a full image (i.e. providing separate events for before and after the update). Please double-check whether your source provides both UPDATE_BEFORE and UPDATE_AFTER events. FROM_CHANGELOG is a very powerful function but might produce incorrect results in subsequent operations and tables, if not configured correctly.

### Syntax

```sql
SELECT * FROM FROM_CHANGELOG(
  input => TABLE source_table,
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP[
      'c, r', 'INSERT',
      'ub', 'UPDATE_BEFORE',
      'ua', 'UPDATE_AFTER',
      'd', 'DELETE'
  ],]
  [error_handling => 'FAIL' | 'SKIP']
)
```

### Parameters

| Parameter    | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|:-------------|:---------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`      | Yes      | The input table. Must be append-only.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `op`         | No       | A `DESCRIPTOR` with a single column name for the operation code column. Defaults to `op`. The column must exist in the input table and be of type STRING.                                                                                                                                                                                                                                                                                                                                       |
| `op_mapping` | No       | A `MAP<STRING, STRING>` mapping user-defined codes to Flink change operation names. Keys are user-defined codes (e.g., `'c'`, `'u'`, `'d'`), values are Flink change operation names (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`). Keys can contain comma-separated codes to map multiple codes to the same operation (e.g., `'c, r'`). Each change operation may appear at most once across all entries. |
| `error_handling` | No | Controls behavior when an input row's operation code is `NULL` or not present in the `op_mapping`. Valid values: `FAIL` (default) — throw a `TableRuntimeException`, `SKIP` — silently drop the row. |

#### Default op_mapping

When `op_mapping` is omitted, the following standard names are used. They allow a reverse conversion from TO_CHANGELOG by default.

| Input code         | Change operation  |
|:-------------------|:------------------|
| `'INSERT'`         | INSERT            |
| `'UPDATE_BEFORE'`  | UPDATE_BEFORE     |
| `'UPDATE_AFTER'`   | UPDATE_AFTER      |
| `'DELETE'`         | DELETE            |

By default, any input row whose op code is `NULL` or not present in the active mapping (default or user-defined) fails the job at runtime with a `TableRuntimeException`. Set `error_handling => 'SKIP'` to silently drop those rows instead.

### Output Schema

The output contains all input columns except the operation code (e.g., op) column, which is interpreted by Flink's SQL engine and removed. Each output row carries the appropriate change operation (INSERT, UPDATE_BEFORE, UPDATE_AFTER, or DELETE).

```
[all_input_columns_without_op]
```

### Examples

#### Basic usage with standard op names

```sql
-- Input (append-only):
-- +I[id:1, op:'INSERT',        name:'Alice']
-- +I[id:2, op:'INSERT',        name:'Bob']
-- +I[id:1, op:'UPDATE_BEFORE', name:'Alice']
-- +I[id:1, op:'UPDATE_AFTER',  name:'Alice2']
-- +I[id:2, op:'DELETE',        name:'Bob']

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream
)

-- Output (updating table):
-- +I[id:1, name:'Alice']
-- +I[id:2, name:'Bob']
-- -U[id:1, name:'Alice']
-- +U[id:1, name:'Alice2']
-- -D[id:2, name:'Bob']

-- Table state after all events:
-- | id | name   |
-- |----|--------|
-- | 1  | Alice2 |
```

#### Custom operation column name

```sql
-- Source schema: id INT, operation STRING, name STRING
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream,
  op => DESCRIPTOR(operation)
)
-- The operation column named 'operation' is used instead of 'op'
```

#### Invalid operation code handling

Two `error_handling` modes are supported. The job can either fail upon an invalid or unknown op code, or skip the row and continue processing.

```sql
-- Fail on unknown op codes (default behavior)
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream
)

-- Silently skip rows with NULL or unknown op codes
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream,
  error_handling => 'SKIP'
)
```

#### Table API

```java
Table cdcStream = ...;

// Default: reads 'op' column with standard change operation names
Table result = cdcStream.fromChangelog();

// With custom op column name
Table result = cdcStream.fromChangelog(
    descriptor("operation").asArgument("op")
);

// With custom op_mapping
Table result = cdcStream.fromChangelog(
    descriptor("op").asArgument("op"),
    map("c, r", "INSERT",
        "ub", "UPDATE_BEFORE",
        "ua", "UPDATE_AFTER",
        "d", "DELETE").asArgument("op_mapping")
);
```

## TO_CHANGELOG

The `TO_CHANGELOG` PTF converts a dynamic table (i.e. an updating table) into an append-only table with an explicit operation code column. Each input row - regardless of its original change operation (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) - is emitted as an INSERT-only row with a string column indicating the original operation.

This is useful when you need to materialize changelog events into a downstream system that only supports appends (e.g., a message queue, log store, or append-only file sink). It is also useful to filter out certain types of updates, for example DELETEs.

### Syntax

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE source_table [PARTITION BY key_col],
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP['INSERT', 'I', 'DELETE', 'D', ...]]
)
```

### Parameters

| Parameter    | Required | Description                                                                                                                                                                                                                                                                                                                                              |
|:-------------|:---------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`      | Yes      | The input table. With `PARTITION BY`, rows with the same key are co-located and run in the same operator instance. Without `PARTITION BY`, each row is processed independently. Accepts insert-only, retract, and upsert tables. For upsert tables, the provided `PARTITION BY` key should match or be a subset of the upsert key of the subquery.                                      |
| `op`         | No       | A `DESCRIPTOR` with a single column name for the operation code column. Defaults to `op`.                                                                                                                                                                                                                                                                |
| `op_mapping` | No       | A `MAP<STRING, STRING>` mapping change operation names to custom output codes. Keys can contain comma-separated names to map multiple operations to the same code (e.g., `'INSERT, UPDATE_AFTER'`). When provided, only mapped operations are forwarded - unmapped events are dropped. Each change operation may appear at most once across all entries. |

#### Default op_mapping

When `op_mapping` is omitted, all four change operations are mapped to their standard names:

| Change Operation | Output value      |
|:----------------|:------------------|
| INSERT          | `'INSERT'`        |
| UPDATE_BEFORE   | `'UPDATE_BEFORE'` |
| UPDATE_AFTER    | `'UPDATE_AFTER'`  |
| DELETE          | `'DELETE'`        |

### Output Schema

The output columns are ordered as:

```
[op_column, all_input_columns]
```

All output rows have `INSERT` - the table is always append-only.

### Examples

#### Basic usage

```sql
-- Input: retract table from an aggregation
-- +I[name:'Alice', cnt:1]
-- +U[name:'Alice', cnt:2]
-- -D[name:'Bob',   cnt:1]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation
)

-- Output (append-only):
-- +I[op:'INSERT',       name:'Alice', cnt:1]
-- +I[op:'UPDATE_AFTER', name:'Alice', cnt:2]
-- +I[op:'DELETE',       name:'Bob',   cnt:1]
```

#### Custom operation column name

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(operation)
)
-- The op column is now named 'operation' instead of 'op'
```

#### Custom operation codes with filtering

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(op_code),
  op_mapping => MAP['INSERT', 'I', 'UPDATE_AFTER', 'U']
)
-- Only INSERT and UPDATE_AFTER events are forwarded
-- DELETE events are dropped (not in the mapping)
-- op_code values are 'I' and 'U' instead of full names
```

#### Deletion flag pattern

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(deleted),
  op_mapping => MAP['INSERT, UPDATE_AFTER', 'false', 'DELETE', 'true']
)
-- INSERT and UPDATE_AFTER produce deleted='false'
-- DELETE produces deleted='true'
-- UPDATE_BEFORE is dropped (not in the mapping)
```

#### Partitioning by a key

```sql
-- Input table 'my_aggregation' with columns (name, id, cnt)
-- Default output schema:           [op, name, id, cnt]
-- Output schema with PARTITION BY: [id, op, name, cnt]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id
)
```
When `PARTITION BY` is provided, **the output schema changes**. The partition key columns are moved to the front by the engine, and the function emits the remaining input columns. The order becomes:

```
[partition_keys, op_column, non_partition_input_columns]
```

Prefer row semantics, when possible. `PARTITION BY` is only necessary when downstream operators are keyed on that column and you want to co-locate rows for the same key in the same parallel operator instance.

#### Table API

```java
// Default: adds 'op' column and supports all changelog modes
Table result = myTable.toChangelog();

// With custom parameters
Table result = myTable.toChangelog(
    descriptor("op_code").asArgument("op"),
    map("INSERT", "I", "UPDATE_AFTER", "U").asArgument("op_mapping")
);

// Deletion flag pattern: comma-separated keys map multiple change operations to the same code
Table result = myTable.toChangelog(
    descriptor("deleted").asArgument("op"),
    map("INSERT, UPDATE_AFTER", "false", "DELETE", "true").asArgument("op_mapping")
);
```

{{< top >}}
