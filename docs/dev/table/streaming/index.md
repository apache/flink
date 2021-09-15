---
title: "Streaming Concepts"
nav-id: streaming_tableapi
nav-parent_id: tableapi
nav-pos: 10
is_beta: false
nav-show_overview: true
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

Flink's [Table API]({% link dev/table/tableApi.md %}) and [SQL support]({% link dev/table/sql/index.md %}) are unified APIs for batch and stream processing.
This means that Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input.

The following pages explain concepts, practical limitations, and stream-specific configuration parameters of Flink's relational APIs on streaming data.

State Management
----------------

Table programs that run in streaming mode leverage all capabilities of Flink as a stateful stream
processor.

In particular, a table program can be configured with a [state backend]({% link ops/state/state_backends.md %})
and various [checkpointing options]({% link dev/stream/state/checkpointing.md %})
for handling different requirements regarding state size and fault tolerance. It is possible to take
a savepoint of a running Table API & SQL pipeline and to restore the application's state at a later
point in time.

### State Usage

Due to the declarative nature of Table API & SQL programs, it is not always obvious where and how much
state is used within a pipeline. The planner decides whether state is necessary to compute a correct
result. A pipeline is optimized to claim as little state as possible given the current set of optimizer
rules.

{% info %}
Conceptually, source tables are never kept entirely in state. An implementer deals with logical tables
(i.e. [dynamic tables]({% link dev/table/streaming/dynamic_tables.md %})). Their state requirements
depend on the used operations.

Queries such as `SELECT ... FROM ... WHERE` which only consist of field projections or filters are usually
stateless pipelines. However, operations such as joins, aggregations, or deduplications require keeping
intermediate results in a fault-tolerant storage for which Flink's state abstractions are used.

{% info %}
Please refer to the individual operator documentation for more details about how much state is required
and how to limit a potentially ever-growing state size.

For example, a regular SQL join of two tables requires the operator to keep both input tables in state
entirely. For correct SQL semantics, the runtime needs to assume that a matching could occur at any
point in time from both sides. Flink provides [optimized window and interval joins]({% link dev/table/streaming/joins.md %})
that aim to keep the state size small by exploiting the concept of [watermarks]({% link dev/table/streaming/time_attributes.md %}).

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
Use the `EXPLAIN` command in SQL or `table.explain()` in Table API to [get insights]({% link dev/table/common.md %}#explaining-a-table).

Since new optimizer rules are continuously added, and operators become more efficient and specialized,
also the upgrade to a newer Flink version could lead to incompatible plans.

{% warn %}
Currently, the framework cannot guarantee that state can be mapped from a savepoint to a new table
operator topology. In other words: Savepoints are only supported if both the query and the Flink version remain constant.

Since the community rejects contributions that modify the optimized plan and the operator topology
in a patch version (e.g. from `1.13.1` to `1.13.2`), it should be safe to upgrade a Table API & SQL
pipeline to a newer bug fix release. However, major-minor upgrades from (e.g. from `1.12` to `1.13`)
are not supported.

For both shortcomings (i.e. modified query and modified Flink version), we recommend to investigate
whether the state of an updated table program can be "warmed up" (i.e. initialized) with historical
data again before switching to real-time data. The Flink community is working on a hybrid source
to make this switching as convenient as possible.


Where to go next?
-----------------

* [Dynamic Tables]({% link dev/table/streaming/dynamic_tables.md %}): Describes the concept of dynamic tables.
* [Time attributes]({% link dev/table/streaming/time_attributes.md %}): Explains time attributes and how time attributes are handled in Table API & SQL.
* [Versioned Tables]({% link dev/table/streaming/versioned_tables.md %}): Describes the Temporal Table concept.
* [Joins in Continuous Queries]({% link dev/table/streaming/joins.md %}): Different supported types of Joins in Continuous Queries.
* [Query configuration]({% link dev/table/streaming/query_configuration.md %}): Lists Table API & SQL specific configuration options.

{% top %}
