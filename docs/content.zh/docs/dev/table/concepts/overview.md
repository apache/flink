---
title: "流式概念"
weight: 1
type: docs
aliases:
  - /zh/dev/table/streaming/
is_beta: false
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

# 流式概念

Flink 的 [Table API]({{< ref "docs/dev/table/tableApi" >}}) 和 [SQL]({{< ref "docs/dev/table/sql/overview" >}}) 是流批统一的 API。
这意味着 Table API & SQL 在无论有限的批式输入还是无限的流式输入下，都具有相同的语义。
因为传统的关系代数以及 SQL 最开始都是为了批式处理而设计的，
关系型查询在流式场景下不如在批式场景下容易懂。

下面这些页面包含了概念、实际的限制，以及流式数据处理中的一些特定的配置。

<a name="state-management"></a>

状态管理
----------------
流模式下运行的表程序利用了 Flink 作为有状态流处理器的所有能力。

事实上，一个表程序（Table program）可以配置一个 [state backend]({{< ref "docs/ops/state/state_backends" >}})
和多个不同的 [checkpoint 选项]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})
以处理对不同状态大小和容错需求。这可以对正在运行的 Table API & SQL 管道（pipeline）生成 savepoint，并在这之后用其恢复应用程序的状态。

<a name="state-usage"></a>

### 状态使用

由于 Table API & SQL 程序是声明式的，管道内的状态会在哪以及如何被使用并不明确。 Planner 会确认是否需要状态来得到正确的计算结果，
管道会被现有优化规则集优化成尽可能少地使用状态。

{{< hint info >}}
从概念上讲， 源表从来不会在状态中被完全保存。 实现者处理的是逻辑表（即[动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}})）。
它们的状态取决于用到的操作。
{{< /hint >}}

形如 `SELECT ... FROM ... WHERE` 这种只包含字段映射或过滤器的查询的查询语句通常是无状态的管道。 然而诸如 join、
聚合或去重操作需要在 Flink 抽象的容错存储内保存中间结果。

{{< hint info >}}
请参考独立的算子文档来获取更多关于状态需求量和限制潜在增长状态大小的信息。
{{< /hint >}}

例如对两个表进行 join 操作的普通 SQL 需要算子保存两个表的全部输入。基于正确的 SQL 语义，运行时假设两表会在任意时间点进行匹配。
Flink 提供了 [优化窗口和时段 Join 聚合]({{< ref "docs/dev/table/sql/queries/joins" >}}) 
以利用 [watermarks]({{< ref "docs/dev/table/concepts/time_attributes" >}}) 概念来让保持较小的状态规模。

另一个计算每个会话的点击次数的查询语句的例子如下

```sql
SELECT sessionId, COUNT(*) FROM clicks GROUP BY sessionId;
```

`sessionId` 是用于分组的键，连续查询（Continuous Query）维护了每个观察到的 `sessionId` 次数。 `sessionId` 属性随着时间逐步演变，
且 `sessionId` 的值只活跃到会话结束（即在有限的时间周期内）。然而连续查询无法得知sessionId的这个性质，
并且预期每个 `sessionId` 值会在任何时间点上出现。这维护了每个可见的 `sessionId` 值。因此总状态量会随着 `sessionId` 的发现不断地增长。

<a name="idle-state-retention-time"></a>

#### 空闲状态维持时间

*空间状态位置时间*参数 [`table.exec.state.ttl`]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) 
定义了状态的键在被更新后要保持多长时间才被移除。在之前的查询例子中，`sessionId` 的数目会在配置的时间内未更新时立刻被移除。

通过移除状态的键，连续查询会完全忘记它曾经见过这个键。如果一个状态带有曾被移除状态的键被处理了，这条记录将被认为是
对应键的第一条记录。上述例子中意味着 `sessionId` 会再次从 `0` 开始计数。

<a name="stateful-upgrades-and-evolution"></a>

### 状态化更新与演化

表程序在流模式下执行将被视为*标准查询*，这意味着它们被定义一次后将被一直视为静态的端到端 (end-to-end) 管道

对于这种状态化的管道，对查询和Flink的Planner的改动都有可能导致完全不同的执行计划。这让表程序的状态化的升级和演化在目前而言
仍具有挑战，社区正致力于改进这一缺点。

例如为了添加过滤谓词，优化器可能决定重排 join 或改变内部算子的 schema。 这会阻碍从 savepoint 的恢复，因为其被改变的拓扑和
算子状态的列布局差异。

查询实现者需要确保改变在优化计划前后是兼容的，在 SQL 中使用 `EXPLAIN` 或在 Table API 中使用 `table.explain()` 
可[获取详情]({{< ref "docs/dev/table/common" >}}#explaining-a-table)。

由于新的优化器规则正不断地被添加，算子变得更加高效和专用，升级到更新的Flink版本可能造成不兼容的计划。

{{< hint warning >}}
当前框架无法保证状态可以从 savepoint 映射到新的算子拓扑上。

换言之： Savepoint 只在查询语句和版本保持恒定的情况下被支持。
{{< /hint >}}

由于社区拒绝在版本补丁（如 `1.13.1` 至 `1.13.2`）上对优化计划和算子拓扑进行修改的贡献，对一个 Table API & SQL 管道
升级到新的 bug fix 发行版应当是安全的。然而主次（major-minor）版本的更新（如 `1.12` 至 `1.13`）不被支持。

由于这两个缺点（即修改查询语句和修改Flink版本），我们推荐实现调查升级后的表程序是否可以在切换到实时数据前，被历史数据"暖机"
（即被初始化）。Flink社区正致力于 [混合源]({{< ref "docs/connectors/datastream/hybridsource" >}}) 来让切换变得尽可能方便。


<a name="where-to-go-next"></a>

接下来？
-----------------

* [动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}): 描述了动态表的概念。
* [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}): 解释了时间属性以及它是如何在 Table API & SQL 中使用的。
* [流上的 Join]({{< ref "docs/dev/table/sql/queries/joins" >}}): 支持的几种流上的 Join。
* [时态（temporal）表]({{< ref "docs/dev/table/concepts/versioned_tables" >}}): 描述了时态表的概念。
* [查询配置]({{< ref "docs/dev/table/config" >}}): Table API & SQL 特定的配置。

{{< top >}}
