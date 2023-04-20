---
title: "Checkpoints 与 Savepoints"
weight: 10
type: docs
aliases:
  - /ops/state/checkpoints_vs_savepoints.html
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

<a name="checkpoints-vs-savepoints"></a>

# Checkpoints 与 Savepoints

<a name="overview"></a>

## 概述

从概念上讲，Flink 的 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 与 [checkpoints]({{< ref "docs/ops/state/checkpoints" >}}) 的不同之处类似于传统数据库系统中的备份与恢复日志之间的差异。

Checkpoints 的主要目的是为意外失败的作业提供恢复机制。
[Checkpoint 的生命周期]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) 由 Flink 管理，
即 Flink 创建，管理和删除 checkpoint - 无需用户交互。
由于 checkpoint 被经常触发，且被用于作业恢复，所以 Checkpoint 的实现有两个设计目标：i）轻量级创建和 ii）尽可能快地恢复。 可能会利用某些特定的属性来达到这个目标，例如， 作业的代码在执行尝试时不会改变。

{{< hint info >}}
- 在用户终止作业后，会自动删除 Checkpoint（除非明确配置为保留的 Checkpoint）。
- Checkpoint 以状态后端特定的（原生的）数据格式存储（有些状态后端可能是增量的）。
{{< /hint >}}

尽管 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 在内部使用与 checkpoints 相同的机制创建，但它们在概念上有所不同，并且生成和恢复的成本可能会更高一些。Savepoints的设计更侧重于可移植性和操作灵活性，尤其是在 job 变更方面。Savepoint 的用例是针对计划中的、手动的运维。例如，可能是更新你的 Flink 版本，更改你的作业图等等。

{{< hint info >}}
- Savepoint 仅由用户创建、拥有和删除。这意味着 Flink 在作业终止后和恢复后都不会删除 savepoint。
- Savepoint 以状态后端独立的（标准的）数据格式存储（注意：从 Flink 1.15 开始，savepoint 也可以以后端特定的[原生]({{< ref "docs/ops/state/savepoints" >}}#savepoint-format)格式存储，这种格式创建和恢复速度更快，但有一些限制）。
{{< /hint >}}

<a name="capabilities-and-limitations"></a>

### 功能和限制

下表概述了各种类型的 savepoint 和 checkpoint 的功能和限制。
- ✓ - Flink 完全支持这种类型的快照
- x - Flink 不支持这种类型的快照
- ! - 虽然这些操作目前有效，但 Flink 并未正式保证对它们的支持，因此它们存在一定程度的风险

| 操作                              | 标准 Savepoint    | 原生 Savepoint | 对齐 Checkpoint | 非对齐 Checkpoint |
|:----------------------------------|:--------------------|:-----------------|:-------------------|:---------------------|
| 更换状态后端                      | ✓                   | x                | x                  | x                    |
| State Processor API (写)          | ✓                   | x                | x                  | x                    |
| State Processor API (读)          | ✓                   | !                | !                  | x                    |
| 自包含和可移动                    | ✓                   | ✓                | x                  | x                    |
| Schema 变更                       | ✓                   | !                | !                  | !                    |
| 任意 job 升级                     | ✓                   | ✓                | ✓                  | x                    |
| 非任意 job 升级                   | ✓                   | ✓                | ✓                  | ✓                    |
| Flink 小版本升级                  | ✓                   | ✓                | ✓                  | x                    |
| Flink bug/patch 版本升级          | ✓                   | ✓                | ✓                  | ✓                    |
| 扩缩容                            | ✓                   | ✓                | ✓                  | ✓                    |

- [更换状态后端]({{< ref "docs/ops/state/state_backends" >}})  - 配置与创建快照时使用的不同的状态后端。
- [State Processor API (写)]({{< ref "docs/libs/state_processor_api" >}}#writing-new-savepoints) - 通过 State Processor API 创建这种类型的新快照的能力。
- [State Processor API (读)]({{< ref "docs/libs/state_processor_api" >}}#reading-state) - 通过 State Processor API 从该类型的现有快照中读取状态的能力。
- 自包含和可移动 - 快照目录包含从该快照恢复所需的所有内容，并且不依赖于其他快照，这意味着如果需要的话，它可以轻松移动到另一个地方。
- [Schema 变更]({{< ref "docs/dev/datastream/fault-tolerance/serialization/schema_evolution" >}}) - 如果使用支持 Schema 变更的序列化器（例如 POJO 和 Avro 类型），则可以更改*状态*数据类型。
- 任意 job 升级 - 即使现有算子的 [partitioning 类型]({{< ref "docs/dev/datastream/operators/overview" >}}#physical-partitioning)（rescale, rebalance, map, 等）或运行中数据类型已经更改，也可以从该快照恢复。
- 非任意 job 升级 - 如果作业图拓扑和运行中数据类型保持不变，则可以使用变更后的 operator 恢复快照。
- Flink 小版本升级 - 从更旧的 Flink 小版本创建的快照恢复（1.x → 1.y）。
- Flink bug/patch 版本升级 - 从更旧的 Flink 补丁版本创建的快照恢复（1.14.x → 1.14.y）。
- 扩缩容 - 使用与快照制作时不同的并发度从该快照恢复。


{{< top >}}
