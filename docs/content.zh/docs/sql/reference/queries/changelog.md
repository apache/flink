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

Flink SQL 提供了用于处理 changelog 流的内置过程表函数（process table function，PTF）。

| 函数 | 描述 |
|:---------|:------------|
| [FROM_CHANGELOG](#from_changelog) | 将带有操作码的仅追加表转换为（可能是更新的）动态表 |
| [TO_CHANGELOG](#to_changelog) | 将动态表转换为带有显式操作码的仅追加表 |

## FROM_CHANGELOG

`FROM_CHANGELOG` PTF 将带有显式操作码（operation code）列的仅追加（append-only）表转换为（可能是更新的）动态表（dynamic table）。每条输入行都应包含一个表示变更操作（change operation）的字符串列。操作码列由引擎解释，并从输出中移除。

当消费来自 Debezium 等系统的变更数据捕获（Change Data Capture，CDC）流时，此函数非常有用。这类系统中的事件以扁平的仅追加记录形式到达，并带有显式的操作字段。在对事件做了特定转换之后，也可以将它与 TO_CHANGELOG 函数结合使用，把仅追加表重新转换为更新表。

注意：当前版本要求 CDC 数据使用完整镜像（full image）来编码更新，即更新前和更新后分别作为独立的事件提供。请仔细确认你的数据源是否同时提供 UPDATE_BEFORE 和 UPDATE_AFTER 事件。FROM_CHANGELOG 是一个非常强大的函数，但如果配置不正确，可能会在后续的操作和表中产生错误结果。

<a name="syntax"></a>

### 语法

```sql
SELECT * FROM FROM_CHANGELOG(
  input => TABLE source_table [PARTITION BY key_col [ORDER BY time_col]],
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP[
      'c, r', 'INSERT',
      'ub', 'UPDATE_BEFORE',
      'ua', 'UPDATE_AFTER',
      'd', 'DELETE'
  ],]
  [error_handling => 'FAIL' | 'SKIP',]
  [uid => 'my-id']
)
```

<a name="parameters"></a>

### 参数

| 参数              | 必填 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|:-----------------|:-----|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`          | 是   | 输入表，必须是仅追加表。使用 `PARTITION BY` 可以确保同一个键的行被一起处理。添加 `ORDER BY`（需要 `PARTITION BY`）可以按键对乱序事件重新排序；第一个排序列必须是定义了 watermark 的时间属性，并且为 `ASC` 顺序。 |
| `op`             | 否   | 包含单个列名的 `DESCRIPTOR`，用于指定操作码列，默认为 `op`。该列必须存在于输入表中，且类型为 STRING。 |
| `op_mapping`     | 否   | 一个 `MAP<STRING, STRING>`，将用户自定义的操作码映射为 Flink 变更操作名称。键为用户自定义的操作码（例如 `'c'`、`'u'`、`'d'`），值为 Flink 变更操作名称（`INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE`）。键可以包含以逗号分隔的多个操作码，从而将它们映射到同一个操作（例如 `'c, r'`）。每个变更操作在所有条目中最多只能出现一次。 |
| `error_handling` | 否   | 控制当输入行的操作码为 `NULL` 或不在 `op_mapping` 中时的行为。可选值：`FAIL`（默认）——抛出 `TableRuntimeException`；`SKIP`——静默丢弃该行。 |
| `uid`            | 否   | PTF 调用的唯一标识符。使用 `PARTITION BY` 时，默认为 `FROM_CHANGELOG`。如果该 PTF 在每个作业中只调用一次，则可以省略。更多详情请参见[多次调用](#multiple-invocations)。 |

<a name="default-op_mapping"></a>

#### 默认 op_mapping

省略 `op_mapping` 时，使用以下标准名称。默认情况下，它们支持对 TO_CHANGELOG 的输出进行反向转换。

| 输入操作码          | 变更操作          |
|:-------------------|:------------------|
| `'INSERT'`         | INSERT            |
| `'UPDATE_BEFORE'`  | UPDATE_BEFORE     |
| `'UPDATE_AFTER'`   | UPDATE_AFTER      |
| `'DELETE'`         | DELETE            |

默认情况下，如果输入行的操作码为 `NULL`，或不在当前生效的映射（默认映射或用户自定义映射）中，作业会在运行时因 `TableRuntimeException` 而失败。设置 `error_handling => 'SKIP'` 可以改为静默丢弃这些行。

<a name="output-schema"></a>

### 输出 Schema

输出包含除操作码列（例如 op 列）之外的所有输入列，操作码列由 Flink SQL 引擎解释后移除。每条输出行都带有相应的变更操作（INSERT、UPDATE_BEFORE、UPDATE_AFTER 或 DELETE）。

```
[all_input_columns_without_op]
```

<a name="examples"></a>

### 示例

<a name="basic-usage-with-standard-op-names"></a>

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

-- 处理完所有事件后的表状态：
-- | id | name   |
-- |----|--------|
-- | 1  | Alice2 |
```

<a name="custom-operation-column-name"></a>

#### 自定义操作码列名

```sql
-- 源表 schema：id INT, operation STRING, name STRING
SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream,
  op => DESCRIPTOR(operation)
)
-- 使用名为 'operation' 的列代替 'op' 作为操作码列
```

<a name="partitioning-by-a-key"></a>

#### 按键分区

```sql
-- 输入表 'cdc_stream' 包含列 (name, id, op, doc)
-- 默认输出 schema：                 [name, id, doc]
-- 使用 PARTITION BY 时的输出 schema：[id, name, doc]

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream PARTITION BY id
)
```

提供 `PARTITION BY` 时，**输出 schema 会发生变化**。引擎会将分区键列移到最前面，函数则输出其余的输入列（不包括 op 列）。输出 schema 变为：

```
[partition_keys, non_partition_input_columns_excluding_op]
```

尽可能优先使用行语义（row semantics）。只有当下游算子（operator）以该列为键，并且你希望将同一个键的行放在同一个并行算子实例中处理时，才需要使用 `PARTITION BY`。

如果你生成的是 upsert 表，即 CDC 输入流只发出 `UPDATE_AFTER` 而不发出 `UPDATE_BEFORE`，那么引擎会把这里选择的分区键同时当作主键（primary key）和 upsert 键。请确保 `PARTITION BY` 键与你的主键完全一致。

<a name="upsert-table"></a>

#### Upsert 表

生成 upsert 表需要同时满足以下两个条件：

* **按键分区**：使用 `PARTITION BY <key>`，其中分区键对应数据集的唯一键或主键。
* **op_mapping 配置**：`op_mapping` 必须包含 `UPDATE_AFTER`，并且**不能**包含 `UPDATE_BEFORE`。

一个产生 `UPDATE_AFTER` 但不产生 `UPDATE_BEFORE` 的 `op_mapping` 描述的是 upsert changelog，因此需要一个键。引擎假定 `PARTITION BY` 子句中提供的键就是唯一的 upsert 键，输出的 changelog 会成为以这些分区列为键的 upsert 表。每条输入行经过处理后会产生 `INSERT`、`UPDATE_AFTER` 或 `DELETE` 事件，并将分区键作为显式的 upsert 键。因此，如果输入的 changelog 包含唯一键（例如主键），则**必须**在 `PARTITION BY` 子句中使用它们。

```sql
-- Upsert 输入：只有 INSERT / UPDATE_AFTER / DELETE
-- +I[id:1, op:'INSERT',       name:'Alice']
-- +I[id:2, op:'INSERT',       name:'Bob']
-- +I[id:1, op:'UPDATE_AFTER', name:'Alice2']
-- +I[id:2, op:'DELETE',       name:'Bob']

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream PARTITION BY id,
  op_mapping => MAP[
    'INSERT',       'INSERT',
    'UPDATE_AFTER', 'UPDATE_AFTER',
    'DELETE',       'DELETE']
)

-- 输出（upsert changelog，upsert 键 = id）：
-- +I[id:1, name:'Alice']
-- +I[id:2, name:'Bob']
-- +U[id:1, name:'Alice2']
-- -D[id:2, name:'Bob']
```

`FROM_CHANGELOG` PTF 假定事件是按顺序到达的。如果数据源本身不能保证同一个 `PARTITION BY` 键的事件有序，请使用 [`ORDER BY`]({{< ref "docs/dev/table/functions/ptfs" >}}#ordering) 对它们重新排序。

默认情况下，如果没有使用 `PARTITION BY`，或者当前生效的 `op_mapping` 包含 `UPDATE_BEFORE`，输出仍然是 retract changelog。

<a name="ordering-cdc-events-with-order-by"></a>

#### 使用 ORDER BY 对 CDC 事件排序

CDC 流中的事件可能乱序到达。例如，当事件分布在上游的多个 broker 上时，某个键的 `UPDATE_AFTER` 可能会先于与之对应的 `UPDATE_BEFORE` 到达。如果数据源本身不能保证顺序，直接应用这样的 changelog 会产生错误的状态。

`FROM_CHANGELOG` 支持 [ORDER BY 子句]({{< ref "docs/dev/table/functions/ptfs" >}}#ordering)，在处理事件之前先对每个分区内的事件进行排序。框架会按分区缓存事件，并在 watermark 推进后按排序后的顺序将它们发送给函数。迟到事件（在 watermark 之后到达的事件）会被丢弃。

要求：

* 输入表必须在 `ORDER BY` 所使用的时间属性上声明 `WATERMARK`。
* 第一个 `ORDER BY` 列必须是该时间属性，并且为 `ASC` 顺序。
* `ORDER BY` 需要 `PARTITION BY`（集合语义，set semantics），不能与行语义一起使用。

```sql
-- 源表声明了带 watermark 的事件时间
CREATE TABLE cdc_stream (
  id INT,
  op STRING,
  name STRING,
  event_time TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
) WITH (...);

SELECT * FROM FROM_CHANGELOG(
  input => TABLE cdc_stream
    PARTITION BY id
    ORDER BY event_time
)
```

**缓存与 watermark 如何相互作用**

Watermark 是独立的事件，与数据记录一起在流中流动。数据记录的到达本身不会推进 watermark；只有收到 watermark 事件时，算子当前的 watermark 才会推进。

假设 watermark 策略会把 watermark 推进到比已观察到的最大 `event_time` 晚 `5` 分钟的位置，并且 PTF 当前的 watermark 为 `10:00`：

| 事件                                            | 当前 watermark | 结果                                               |
|------------------------------------------------|---------------|----------------------------------------------------|
| `+I[id: 6, op: 'INSERT', event_time: '10:05']` | `10:00`       | 被缓存。等 watermark 超过 `10:05` 后再发出。           |
| `+I[id: 5, op: 'INSERT', event_time: '09:57']` | `10:00`       | 被丢弃。时间戳小于当前 watermark。                     |
| `+I[id: 7, op: 'INSERT', event_time: '10:11']` | `10:00`       | 被缓存。                                            |
| Watermark 事件推进到 `10:06`                     | `10:06`       | 发出记录 `id=6`。                                    |

一旦 watermark 越过了迟到记录，这些记录就再也无法恢复。请根据上游预期的乱序程度来选择 watermark 允许的延迟时间。

FROM_CHANGELOG 不会为被丢弃的事件生成替代事件，下游会受到什么影响取决于输出的 changelog 模式：

* **Retract 输出**：retract 语义依赖成对的 `UPDATE_BEFORE`/`UPDATE_AFTER`。如果 `ORDER BY` 丢弃了一个迟到的 `UPDATE_BEFORE`，而与之对应的 `UPDATE_AFTER` 按时到达，下游就只会看到单独的 `UPDATE_AFTER`。依赖 retract 语义的算子（retract 风格的聚合、retract sink）会错过这次撤回，并可能产生错误结果。
* **Upsert 输出**（`PARTITION BY` + 不包含 `UPDATE_BEFORE` 的 `op_mapping`）：由于 upsert 是幂等的，被丢弃的 `INSERT` 或 `UPDATE_AFTER` 可以由该键的下一个 `UPDATE_AFTER` 恢复。被丢弃的 `DELETE` 则无法恢复——该行会一直保留在下游状态中，直到该键的后续事件将其覆盖。

<a name="invalid-operation-code-handling"></a>

#### 无效操作码处理

支持两种 `error_handling` 模式。遇到无效或未知的操作码时，作业可以直接失败，也可以跳过该行并继续处理。

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

// 默认：读取 'op' 列，使用标准的变更操作名称
Table result = cdcStream.fromChangelog();

// 使用自定义的操作码列名
Table result = cdcStream.fromChangelog(
    descriptor("operation").asArgument("op")
);

// 使用自定义的 op_mapping
Table result = cdcStream.fromChangelog(
    descriptor("op").asArgument("op"),
    map("c, r", "INSERT",
        "ub", "UPDATE_BEFORE",
        "ua", "UPDATE_AFTER",
        "d", "DELETE").asArgument("op_mapping")
);

// 集合语义：将同一个键的行放在同一个并行算子实例中处理。
// 等价于 SQL 中的 PARTITION BY。分区键会被放到输出列的最前面。
Table result = cdcStream.partitionBy($("id")).fromChangelog();

// 在应用 changelog 之前，按事件时间对每个键的乱序 CDC 事件重新排序。
// 输入必须在 orderBy() 所使用的时间属性上声明 WATERMARK。
Table result = cdcStream
    .partitionBy($("id"))
    .orderBy($("event_time").asc())
    .fromChangelog();
```

## TO_CHANGELOG

`TO_CHANGELOG` PTF 将动态表（即更新表）转换为带有显式操作码列的仅追加表。每条输入行无论原始的变更操作是什么（INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE），都会作为仅 INSERT 的行发出，并附带一个表示原始操作的字符串列。

当你需要把 changelog 事件写入只支持追加的下游系统（例如消息队列、日志存储或仅追加的文件 sink）时，此函数非常有用。它也可以用来过滤掉某些类型的更新，例如 DELETE。

<a name="syntax-1"></a>

### 语法

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE source_table [PARTITION BY key_col],
  [op => DESCRIPTOR(op_column_name),]
  [op_mapping => MAP['INSERT', 'I', 'DELETE', 'D', ...],]
  [produces_full_deletes => BOOLEAN,]
  [uid => 'my-id']
)
```

<a name="parameters-1"></a>

### 参数

| 参数                     | 必填 | 描述                                                                                                                                                                                                                                                                                                                                              |
|:------------------------|:-----|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input`                 | 是   | 输入表。使用 `PARTITION BY` 时，同一个键的行会被放在同一个算子实例中处理。不使用 `PARTITION BY` 时，每行独立处理。支持仅插入表、retract 表和 upsert 表。对于 upsert 表，提供的 `PARTITION BY` 键应与子查询的 upsert 键一致，或是它的子集。 |
| `op`                    | 否   | 包含单个列名的 `DESCRIPTOR`，用于指定操作码列，默认为 `op`。 |
| `op_mapping`            | 否   | 一个 `MAP<STRING, STRING>`，将变更操作名称映射为自定义的输出码。键可以包含以逗号分隔的多个名称，从而将多个操作映射到同一个输出码（例如 `'INSERT, UPDATE_AFTER'`）。提供该参数时，只转发已映射的操作，未映射的事件会被丢弃。每个变更操作在所有条目中最多只能出现一次。 |
| `produces_full_deletes` | 否   | 一个 `BOOLEAN` 字面量，用于控制 DELETE 行的发出方式。为 `true`（默认）时，DELETE 行包含所有列，即完整镜像。为 `false` 时，只保留用于标识行的键列，其余列置为 null。更多详情请参见[完整删除与部分删除](#full-vs-partial-deletes)。 |
| `uid`                   | 否   | PTF 调用的唯一标识符。使用 `PARTITION BY` 时，默认为 `TO_CHANGELOG`。如果该 PTF 在每个作业中只调用一次，则可以省略。更多详情请参见[多次调用](#multiple-invocations)。 |

<a name="default-op_mapping-1"></a>

#### 默认 op_mapping

省略 `op_mapping` 时，四种变更操作都会映射为各自的标准名称：

| 变更操作          | 输出值             |
|:-----------------|:------------------|
| INSERT           | `'INSERT'`        |
| UPDATE_BEFORE    | `'UPDATE_BEFORE'` |
| UPDATE_AFTER     | `'UPDATE_AFTER'`  |
| DELETE           | `'DELETE'`        |

<a name="output-schema-1"></a>

### 输出 Schema

输出 schema 为：

```
[op_column, all_input_columns]
```

所有输出行都是 `INSERT`——输出表始终是仅追加的。

<a name="examples-1"></a>

### 示例

<a name="basic-usage"></a>

#### 基本用法

```sql
-- 输入：来自聚合的 retract 表
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

<a name="custom-operation-column-name-1"></a>

#### 自定义操作码列名

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(operation)
)
-- 操作码列现在名为 'operation'，而不是 'op'
```

<a name="custom-operation-codes-with-filtering"></a>

#### 自定义操作码并过滤

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation,
  op => DESCRIPTOR(op_code),
  op_mapping => MAP['INSERT', 'I', 'UPDATE_AFTER', 'U']
)
-- 只转发 INSERT 和 UPDATE_AFTER 事件
-- DELETE 事件被丢弃（不在映射中）
-- op_code 的值为 'I' 和 'U'，而不是完整名称
```

<a name="upsert-stream"></a>

#### Upsert 流

```sql
SELECT * FROM TO_CHANGELOG(
  input => TABLE upsert_source PARTITION BY id,
  op_mapping => MAP['INSERT, UPDATE_AFTER', 'u', 'DELETE', 'd']
)
-- INSERT 和 UPDATE_AFTER 产生 op='u'（upsert）
-- DELETE 产生 op='d'
-- UPDATE_BEFORE 被省略
```

<a name="deletion-flag-pattern"></a>

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

<a name="upsert-key"></a>

#### Upsert 键

**Upsert 键**（upsert key）是在 changelog 中能够在一行的整个生命周期内唯一标识该行的一列或多列。下游算子和 sink 通过它来判断新的 INSERT、UPDATE_AFTER 或 DELETE 对应的是之前的哪一行。

Planner 从输入表中推导 upsert 键：

* 直接读取源表时，源表上声明的 `PRIMARY KEY`。
* 上游 `GROUP BY <key>` 或 `PARTITION BY <key>` 的分组列。
* 由保留键的算子传递下来的键（例如 lookup join，以及保留了键列的 calc 投影）。

<a name="full-vs-partial-deletes"></a>

#### 完整删除与部分删除

`produces_full_deletes` 参数控制 DELETE 行的发出方式，以及 planner 对输入的要求。下面列出了使用 `PARTITION BY`（集合语义）和不使用 `PARTITION BY`（行语义）时的各种组合。为 `false` 时，函数依赖输入表的 [upsert 键](#upsert-key) 来决定保留哪些列。

<a name="produces_full_deletes--true-default"></a>

##### `produces_full_deletes => true`（默认）

Planner 要求输入的 DELETE 行包含完整数据。对于只发出键的 DELETE 的 upsert 数据源，会在上游插入 `ChangelogNormalize` 算子，从状态中计算出完整的前镜像（pre-image）。对于已经发出完整前镜像的数据源（例如 retract），该参数不起作用。随后函数在 DELETE 时会原样传递输入行。

**行语义**（不使用 `PARTITION BY`）：

```sql
-- Upsert 数据源：-D[id:5, name: null]（只有键）。
-- ChangelogNormalize 从状态中计算出完整的前镜像。
-- 输出：+I[op:'DELETE', id:5, name:'Alice']
SELECT * FROM TO_CHANGELOG(input => TABLE upsert_source)

-- Retract 数据源：-D[id:5, name:'Alice']（完整的前镜像）。
-- 不插入 ChangelogNormalize；输入行被原样传递。
-- 输出：+I[op:'DELETE', id:5, name:'Alice']
SELECT * FROM TO_CHANGELOG(input => TABLE retract_source)
```

**集合语义**（使用 `PARTITION BY`）：

```sql
-- Upsert 数据源：-D[id:5, name: null]（只有键）。
-- ChangelogNormalize 从状态中计算出完整的前镜像。
-- 输出：+I[id:5, op:'DELETE', name:'Alice']
SELECT * FROM TO_CHANGELOG(input => TABLE upsert_source PARTITION BY id)

-- Retract 数据源：-D[id:5, name:'Alice']（完整的前镜像）。
-- 不插入 ChangelogNormalize；输入行被原样传递。
-- 输出：+I[id:5, op:'DELETE', name:'Alice']
SELECT * FROM TO_CHANGELOG(input => TABLE retract_source PARTITION BY id)
```

##### `produces_full_deletes => false`

Planner 会跳过 `ChangelogNormalize`，函数发出部分 DELETE 行。对于下游不需要完整前镜像的 upsert 数据源（例如 Kafka compacted topic），这样可以避免使用有状态的规范化算子。这要求输入表存在 [upsert 键](#upsert-key)（行语义）或使用 `PARTITION BY`（集合语义）；否则调用会因校验错误而被拒绝。

{{< hint warning >}}
**当 `produces_full_deletes => false` 时，输出列的可空性会发生变化。** 不属于 upsert 键（或 `PARTITION BY` 键）的列在运行时会在 DELETE 行中被置为 null，因此类型系统会在输出 schema 中将它们放宽为可空类型。所以，输入中声明为 `NOT NULL` 的列，在 `TO_CHANGELOG(..., produces_full_deletes => false)` 调用的输出中会变为可空。Upsert 键（或分区键）列会保留其声明的可空性。如果你需要输出保留输入的 `NOT NULL` 类型，请使用默认的 `produces_full_deletes => true`。
{{< /hint >}}

**行语义**（不使用 `PARTITION BY`）：函数在 DELETE 行中保留由 planner 推导出的 upsert 键列，其余列置为 null。直接从数据源读取时，upsert 键通常是声明的 `PRIMARY KEY`，或者是 `GROUP BY <key>` 中提供的键。

```sql
-- 带有 PRIMARY KEY (id) 的 upsert 数据源：-D[id:5]（只有键）。
-- 输出：+I[op:'DELETE', id:5, name:null]
SELECT * FROM TO_CHANGELOG(input => TABLE upsert_source, produces_full_deletes => false)

-- 带有 PRIMARY KEY (id) 的 retract 数据源：-D[id:5, name:'Alice']（完整的前镜像）。
-- 输出：+I[op:'DELETE', id:5, name:null]
SELECT * FROM TO_CHANGELOG(input => TABLE retract_source, produces_full_deletes => false)
```

**集合语义**（使用 `PARTITION BY`）：函数在 DELETE 行中保留分区键，并将所有非分区键列置为 null。用作分区键列的键应该是作为记录标识符的唯一键。这与 Kafka compacted topic 等 upsert sink 所期望的数据形式一致。

```sql
-- Upsert 数据源：-D[id:5]（只有键）。
-- 输出：+I[id:5, op:'DELETE', name:null]
SELECT * FROM TO_CHANGELOG(
  input => TABLE upsert_source PARTITION BY id,
  produces_full_deletes => false
)

-- Retract 数据源：-D[id:5, name:'Alice']（完整的前镜像）。
-- 输出：+I[id:5, op:'DELETE', name:null]
SELECT * FROM TO_CHANGELOG(
  input => TABLE retract_source PARTITION BY id,
  produces_full_deletes => false
)
```

<a name="partitioning-by-a-key-1"></a>

#### 按键分区

```sql
-- 输入表 'my_aggregation' 包含列 (name, id, cnt)
-- 默认输出 schema：                 [op, name, id, cnt]
-- 使用 PARTITION BY 时的输出 schema：[id, op, name, cnt]

SELECT * FROM TO_CHANGELOG(
  input => TABLE my_aggregation PARTITION BY id
)
```
提供 `PARTITION BY` 时，**输出 schema 会发生变化**。引擎会将分区键列移到最前面，函数则输出其余的输入列。输出 schema 变为：

```
[partition_keys, op_column, non_partition_input_columns]
```

尽可能优先使用行语义。只有当下游算子以该列为键，并且你希望将同一个键的行放在同一个并行算子实例中处理时，才需要使用 `PARTITION BY`。

#### Table API

```java
// 默认：添加 'op' 列，并支持所有 changelog 模式
Table result = myTable.toChangelog();

// 使用自定义参数
Table result = myTable.toChangelog(
    descriptor("op_code").asArgument("op"),
    map("INSERT", "I", "UPDATE_AFTER", "U").asArgument("op_mapping")
);

// 删除标志模式：以逗号分隔的键可以将多个变更操作映射到同一个输出码
Table result = myTable.toChangelog(
    descriptor("deleted").asArgument("op"),
    map("INSERT, UPDATE_AFTER", "false", "DELETE", "true").asArgument("op_mapping")
);

// 不使用完整删除语义。为 `true`（默认）时，DELETE 行包含完整的前镜像。
// 为 `false` 时，只保留用于标识行的键列，其余列置为 null。
// 更多详情请参见[完整删除与部分删除](#full-vs-partial-deletes)。
Table result = myTable.toChangelog(
    lit(false).asArgument("produces_full_deletes")
);

// 集合语义：将同一个键的行放在同一个并行算子实例中处理。
// 等价于 SQL 中的 PARTITION BY。分区键会被放到输出列的最前面。
Table result = myTable.partitionBy($("id")).toChangelog();
```

<a name="multiple-invocations"></a>

## 多次调用

使用 `PARTITION BY` 时，函数名会被用作默认的唯一标识符（UID）。如果同一个 PTF 被调用多次，例如在 `STATEMENT SET` 中，那么每次调用都需要一个在整个 Flink 作业中唯一的 `uid`：

```sql
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO orders_sink
    SELECT * FROM FROM_CHANGELOG(input => TABLE orders_cdc PARTITION BY id, uid => 'orders-changelog');

  INSERT INTO customers_sink
    SELECT * FROM FROM_CHANGELOG(input => TABLE customers_cdc PARTITION BY id, uid => 'customers-changelog');
END;
```

更多详情请参见[基于 UID 的查询演进]({{< ref "docs/dev/table/functions/ptfs" >}}#query-evolution-with-uids)。

{{< top >}}
