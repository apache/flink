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

State Management
----------------

Table programs that run in streaming mode leverage all capabilities of Flink as a stateful stream
processor.

In particular, a table program can be configured with a [state backend]({{< ref "docs/ops/state/state_backends" >}})
and various [checkpointing options]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})
for handling different requirements regarding state size and fault tolerance. It is possible to take
a savepoint of a running Table API & SQL pipeline and to restore the application's state at a later
point in time.

### State Usage

Due to the declarative nature of Table API & SQL programs, it is not always obvious where and how much
state is used within a pipeline. The planner decides whether state is necessary to compute a correct
result. A pipeline is optimized to claim as little state as possible given the current set of optimizer
rules.

{{< hint info >}}
Conceptually, source tables are never kept entirely in state. An implementer deals with logical tables
(i.e. [dynamic tables]({{< ref "docs/dev/table/concepts/dynamic_tables" >}})). Their state requirements
depend on the used operations.
{{< /hint >}}

Queries such as `SELECT ... FROM ... WHERE` which only consist of field projections or filters are usually
stateless pipelines. However, operations such as joins, aggregations, or deduplications require keeping
intermediate results in a fault-tolerant storage for which Flink's state abstractions are used.

{{< hint info >}}
Please refer to the individual operator documentation for more details about how much state is required
and how to limit a potentially ever-growing state size.
{{< /hint >}}

For example, a regular SQL join of two tables requires the operator to keep both input tables in state
entirely. For correct SQL semantics, the runtime needs to assume that a matching could occur at any
point in time from both sides. Flink provides [optimized window and interval joins]({{< ref "docs/dev/table/sql/queries/joins" >}})
that aim to keep the state size small by exploiting the concept of [watermarks]({{< ref "docs/dev/table/concepts/time_attributes" >}}).

Another example is the following query that computes the number of clicks per session.

```sql
SELECT sessionId, COUNT(*) FROM clicks GROUP BY sessionId;
```

The `sessionId` attribute is used as a grouping key and the continuous query maintains a count
for each `sessionId` it observes. The `sessionId` attribute is evolving over time and `sessionId`
values are only active until the session ends, i.e., for a limited period of time. However, the
continuous query cannot know about this property of `sessionId` and expects that every `sessionId`
value can occur at any point of time. It maintains a count for each observed `sessionId` value.
Consequently, the total state size of the query is continuously growing as more and more `sessionId`
values are observed.

#### Idle State Retention Time

The *Idle State Retention Time* parameter [`table.exec.state.ttl`]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)
defines for how long the state of a key is retained without being updated before it is removed.
For the previous example query, the count of a`sessionId` would be removed as soon as it has not
been updated for the configured period of time.

By removing the state of a key, the continuous query completely forgets that it has seen this key
before. If a record with a key, whose state has been removed before, is processed, the record will
be treated as if it was the first record with the respective key. For the example above this means
that the count of a `sessionId` would start again at `0`.

### Stateful Upgrades and Evolution

Table programs that are executed in streaming mode are intended as *standing queries* which means they
are defined once and are continuously evaluated as static end-to-end pipelines.

In case of stateful pipelines, any change to both the query or Flink's planner might lead to a completely
different execution plan. This makes stateful upgrades and the evolution of table programs challenging
at the moment. The community is working on improving those shortcomings.

For example, by adding a filter predicate, the optimizer might decide to reorder joins or change the
schema of an intermediate operator. This prevents restoring from a savepoint due to either changed
topology or different column layout within the state of an operator.

The query implementer must ensure that the optimized plans before and after the change are compatible.
Use the `EXPLAIN` command in SQL or `table.explain()` in Table API to [get insights]({{< ref "docs/dev/table/common" >}}#explaining-a-table).

Since new optimizer rules are continuously added, and operators become more efficient and specialized,
also the upgrade to a newer Flink version could lead to incompatible plans.

{{< hint warning >}}
Currently, the framework cannot guarantee that state can be mapped from a savepoint to a new table
operator topology.

In other words: Savepoints are only supported if both the query and the Flink version remain constant.
{{< /hint >}}

Since the community rejects contributions that modify the optimized plan and the operator topology
in a patch version (e.g. from `1.13.1` to `1.13.2`), it should be safe to upgrade a Table API & SQL
pipeline to a newer bug fix release. However, major-minor upgrades from (e.g. from `1.12` to `1.13`)
are not supported.

For both shortcomings (i.e. modified query and modified Flink version), we recommend to investigate
whether the state of an updated table program can be "warmed up" (i.e. initialized) with historical
data again before switching to real-time data. The Flink community is working on a [hybrid source]({{< ref "docs/connectors/datastream/hybridsource" >}})
to make this switching as convenient as possible.

接下来？
-----------------

* [动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}): 描述了动态表的概念。
* [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}): 解释了时间属性以及它是如何在 Table API & SQL 中使用的。
* [流上的 Join]({{< ref "docs/dev/table/sql/queries/joins" >}}): 支持的几种流上的 Join。
* [时态（temporal）表]({{< ref "docs/dev/table/concepts/versioned_tables" >}}): 描述了时态表的概念。
* [查询配置]({{< ref "docs/dev/table/config" >}}): Table API & SQL 特定的配置。

{{< top >}}
