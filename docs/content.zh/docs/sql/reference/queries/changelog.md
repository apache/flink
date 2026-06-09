---
title: "Changelog 转换"
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

# Changelog 转换

{{< label Streaming >}}

Flink SQL 提供了用于处理 changelog 流的内置过程表函数（PTF）。

| 函数 | 描述 |
|:---------|:------------|
| [FROM_CHANGELOG](#from_changelog) | 将带有操作码的仅追加表转换为（可能包含更新的）动态表 |
| [TO_CHANGELOG](#to_changelog) | 将动态表转换为带有显式操作码的仅追加表 |

## FROM_CHANGELOG

`FROM_CHANGELOG` PTF 将带有显式操作码列的仅追加表转换为（可能包含更新的）动态表。每行输入数据预期包含一个字符串列，用于表示变更操作类型。操作码列由引擎解释后将从输出中移除。

当从 Debezium 等系统消费变更数据捕获（CDC）流时，此函数非常有用，因为这些系统以带有显式操作字段的平坦仅追加记录形式发送事件。在对事件进行特定转换后，将仅追加表重新转换回更新表时，该函数也可与 TO_CHANGELOG 函数结合使用。

注意：此版本要求 CDC 数据使用完整镜像方式编码更新（即对更新前后分别提供独立的事件）。请仔细确认您的数据源是否同时提供 UPDATE_BEFORE 和 UPDATE_AFTER 事件。FROM_CHANGELOG 是一个非常强大的函数，但如果配置不正确，可能会在后续操作和表中产生错误结果。

### 语法

```sql
SELECT * FROM FROM_CHANGELOG(
  input => TABLE source_table [PARTITION BY key_col],
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

### 参数

| 参数           | 是否必填 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|:--------------|:--------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`       | 是      | 输入表。必须为仅追加表。使用 `PARTITION BY` 可确保同一键的行被一起处理。                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `op`          | 否      | 包含单个列名的 `DESCRIPTOR`，用于指定操作码列。默认为 `op`。该列必须存在于输入表中，且类型为 STRING。                                                                                                                                                                                                                                                                                                                                                                                           |
| `op_mapping`  | 否      | 一个 `MAP<STRING, STRING>`，将用户自定义码映射到 Flink 变更操作名称。键为用户自定义码（如 `'c'`、`'u'`、`'d'`），值为 Flink 变更操作名称（`INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE`）。键可包含逗号分隔的多个码，以将多个码映射到同一操作（如 `'c, r'`）。每个变更操作在所有条目中最多出现一次。 |
| `error_handling` | 否   | 控制当输入行的操作码为 `NULL` 或不在 `op_mapping` 中时的行为。有效值：`FAIL`（默认）—— 抛出 `TableRuntimeException`；`SKIP` —— 静默丢弃该行。                                                                                                                                                                                                                                                                                                                                                  |

#### 默认 op_mapping

当省略 `op_mapping` 时，使用以下标准名称。默认情况下，它们支持从 TO_CHANGELOG 进行反向转换。

| 输入码              | 变更操作          |
|:-------------------|:------------------|
| `'INSERT'`         | INSERT            |
| `'UPDATE_BEFORE'`  | UPDATE_BEFORE     |
| `'UPDATE_AFTER'`   | UPDATE_AFTER      |
| `'DELETE'`         | DELETE            |

默认情况下，若输入行的操作码为 `NULL` 或不在当前映射（默认或用户自定义）中，作业将在运行时抛出 `TableRuntimeException` 并失败。可将 `error_handling` 设置为 `'SKIP'` 以静默丢弃这些行。

### 输出 Schema

输出包含除操作码列（如 op 列）之外的所有输入列，该列由 Flink SQL 引擎解释后移除。每行输出携带相应的变更操作（INSERT、UPDATE_BEFORE、UPDATE_AFTER 或 DELETE）。

```
[all_input_columns_without_op]
```

### 示例

#### 使用标准操作名称的基本用法

```sql
-- 输入（仅追加）：
-- +I[id:1, op:'INSERT',        name:'Alice']
-- +I[id:2, op:'INSERT',        name:'Bob']
-- +I[id:1, op:'UPDATE_BEFORE', name:'Alice']
-- +I[id:1, op:'UPDATE_AFTER',  name:'Alice2']
-- +I[id:2, op:'DELETE',        name:'Bob']

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream
)

-- 输出（更新表）：
-- +I[id:1, name:'Alice']
-- +I[id:2, name:'Bob']
-- -U[id:1, name:'Alice']
-- +U[id:1, name:'Alice2']
-- -D[id:2, name:'Bob']

-- 所有事件处理后的表状态：
-- | id | name   |
-- |----|--------|
-- | 1  | Alice2 |
```

#### 自定义操作码列名

```sql
-- 数据源 schema：id INT, operation STRING, name STRING
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream,
  op => DESCRIPTOR(operation)
)
-- 使用名为 'operation' 的列代替 'op' 列
```

#### 按键分区

```sql
-- 输入表 'cdc_stream' 包含列（name, id, op, doc）
-- 默认输出 schema：          [name, id, doc]
-- 使用 PARTITION BY 的输出 schema：[id, name, doc]

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream PARTITION BY id
)
```

当使用 `PARTITION BY` 时，**输出 schema 会发生变化**。分区键列由引擎移至最前，函数输出其余输入列（不含操作码列）。输出 schema 变为：

```
[partition_keys, non_partition_input_columns_excluding_op]
```

在可能的情况下，优先使用行语义。仅当下游算子按该列进行键控，且您希望将同一键的行分配到同一并行算子实例时，才需要使用 `PARTITION BY`。

如果您生成的是 upsert 表——即从 CDC 输入流中仅发出 `UPDATE_AFTER` 而不发出 `UPDATE_BEFORE`——则此处选择的分区键将被引擎同时视为主键和 upsert 键。请确保 `PARTITION BY` 键与主键完全匹配。

#### 无效操作码处理

支持两种 `error_handling` 模式：遇到无效或未知操作码时，作业可以失败，也可以跳过该行并继续处理。

```sql
-- 遇到未知操作码时失败（默认行为）
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream
)

-- 静默跳过操作码为 NULL 或未知的行
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream,
  error_handling => 'SKIP'
)
```

#### Table API

```java
Table cdcStream = ...;

// 默认：使用标准变更操作名称读取 'op' 列
Table result = cdcStream.fromChangelog();

// 自定义操作码列名
Table result = cdcStream.fromChangelog(
    descriptor("operation").asArgument("op")
);

// 自定义 op_mapping
Table result = cdcStream.fromChangelog(
    descriptor("op").asArgument("op"),
    map("c, r", "INSERT",
        "ub", "UPDATE_BEFORE",
        "ua", "UPDATE_AFTER",
        "d", "DELETE").asArgument("op_mapping")
);

// 集合语义：将相同键的行分配到同一并行算子实例。
// 等价于 SQL 中的 PARTITION BY。分区键将前置于输出列。
Table result = cdcStream.partitionBy($("id")).fromChangelog();
```

## TO_CHANGELOG

`TO_CHANGELOG` PTF 将动态表（即更新表）转换为带有显式操作码列的仅追加表。每行输入数据——无论其原始变更操作类型（INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE）——都以仅 INSERT 行的形式发出，并附带一个表示原始操作的字符串列。

当您需要将 changelog 事件物化到仅支持追加的下游系统（如消息队列、日志存储或仅追加文件 sink）时，此函数非常有用。它也适用于过滤特定类型的更新，例如 DELETE 操作。

### 语法

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE source_table [PARTITION BY key_col],
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP['INSERT', 'I', 'DELETE', 'D', ...]]
)
```

### 参数

| 参数           | 是否必填 | 描述                                                                                                                                                                                                                                                                                                                                             |
|:--------------|:--------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`       | 是      | 输入表。使用 `PARTITION BY` 时，相同键的行将被共置并在同一算子实例中运行。不使用 `PARTITION BY` 时，每行独立处理。接受仅插入、retract 和 upsert 表。对于 upsert 表，提供的 `PARTITION BY` 键应与子查询的 upsert 键匹配或为其子集。                                                                                                                     |
| `op`          | 否      | 包含单个列名的 `DESCRIPTOR`，用于指定操作码列。默认为 `op`。                                                                                                                                                                                                                                                                                      |
| `op_mapping`  | 否      | 一个 `MAP<STRING, STRING>`，将变更操作名称映射到自定义输出码。键可包含逗号分隔的多个名称，以将多个操作映射到同一输出码（如 `'INSERT, UPDATE_AFTER'`）。提供该参数后，只有被映射的操作会被转发，未映射的事件将被丢弃。每个变更操作在所有条目中最多出现一次。                                                                                              |

#### 默认 op_mapping

当省略 `op_mapping` 时，四种变更操作均映射为其标准名称：

| 变更操作         | 输出值            |
|:----------------|:------------------|
| INSERT          | `'INSERT'`        |
| UPDATE_BEFORE   | `'UPDATE_BEFORE'` |
| UPDATE_AFTER    | `'UPDATE_AFTER'`  |
| DELETE          | `'DELETE'`        |

### 输出 Schema

输出 schema 为：

```
[op_column, all_input_columns]
```

所有输出行的变更操作均为 `INSERT`——该表始终为仅追加表。

### 示例

#### 基本用法

```sql
-- 输入：来自聚合操作的 retract 表
-- +I[name:'Alice', cnt:1]
-- +U[name:'Alice', cnt:2]
-- -D[name:'Bob',   cnt:1]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation
)

-- 输出（仅追加）：
-- +I[op:'INSERT',       name:'Alice', cnt:1]
-- +I[op:'UPDATE_AFTER', name:'Alice', cnt:2]
-- +I[op:'DELETE',       name:'Bob',   cnt:1]
```

#### 自定义操作码列名

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(operation)
)
-- 操作码列现在命名为 'operation' 而非 'op'
```

#### 带过滤的自定义操作码

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(op_code),
  op_mapping => MAP['INSERT', 'I', 'UPDATE_AFTER', 'U']
)
-- 仅转发 INSERT 和 UPDATE_AFTER 事件
-- DELETE 事件被丢弃（不在映射中）
-- op_code 的值为 'I' 和 'U' 而非完整名称
```

#### 删除标志模式

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(deleted),
  op_mapping => MAP['INSERT, UPDATE_AFTER', 'false', 'DELETE', 'true']
)
-- INSERT 和 UPDATE_AFTER 产生 deleted='false'
-- DELETE 产生 deleted='true'
-- UPDATE_BEFORE 被丢弃（不在映射中）
```

#### 按键分区

```sql
-- 输入表 'my_aggregation' 包含列（name, id, cnt）
-- 默认输出 schema：          [op, name, id, cnt]
-- 使用 PARTITION BY 的输出 schema：[id, op, name, cnt]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id
)
```

当使用 `PARTITION BY` 时，**输出 schema 会发生变化**。分区键列由引擎移至最前，函数输出其余输入列。输出 schema 变为：

```
[partition_keys, op_column, non_partition_input_columns]
```

在可能的情况下，优先使用行语义。仅当下游算子按该列进行键控，且您希望将同一键的行分配到同一并行算子实例时，才需要使用 `PARTITION BY`。

#### Table API

```java
// 默认：添加 'op' 列并支持所有 changelog 模式
Table result = myTable.toChangelog();

// 使用自定义参数
Table result = myTable.toChangelog(
    descriptor("op_code").asArgument("op"),
    map("INSERT", "I", "UPDATE_AFTER", "U").asArgument("op_mapping")
);

// 删除标志模式：逗号分隔的键将多个变更操作映射到同一输出码
Table result = myTable.toChangelog(
    descriptor("deleted").asArgument("op"),
    map("INSERT, UPDATE_AFTER", "false", "DELETE", "true").asArgument("op_mapping")
);

// 集合语义：将相同键的行分配到同一并行算子实例。
// 等价于 SQL 中的 PARTITION BY。分区键将前置于输出列。
Table result = myTable.partitionBy($("id")).toChangelog();
```

{{< top >}}
