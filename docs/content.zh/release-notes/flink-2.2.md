---
title: "Release Notes - Flink 2.2"
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

# Release notes - Flink 2.2

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.1 and Flink 2.2. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.2.

### Table SQL / API

#### Support VECTOR_SEARCH in Flink SQL

##### [FLINK-38422](https://issues.apache.org/jira/browse/FLINK-38422)

Apache Flink has supported leveraging LLM capabilities through the `ML_PREDICT` function in Flink SQL
since version 2.1, enabling users to perform semantic analysis in a simple and efficient way. This
integration has been technically validated in scenarios such as log classification and real-time
question-answering systems. However, the current architecture allows Flink to only use embedding
models to convert unstructured data (e.g., text, images) into high-dimensional vector features,
which are then persisted to downstream storage systems. It lacks real-time online querying and
similarity analysis capabilities for vector spaces. The VECTOR_SEARCH function is provided in Flink
2.2 to enable users to perform streaming vector similarity searches and real-time context retrieval
directly within Flink.

See more details about the capabilities and usages of
Flink's [Vector Search](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/vector-search/).

#### Realtime AI Function

##### [FLINK-38104](https://issues.apache.org/jira/browse/FLINK-38104)

Apache Flink has supported leveraging LLM capabilities through the `ML_PREDICT` function in Flink SQL
since version 2.1. In Flink 2.2, the Table API also supports model inference operations that allow
you to integrate machine learning models directly into your data processing pipelines.

See more details about the capabilities and usages of
Flink's [Model Inference](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/tableapi/#model-inference).

#### Materialized Table

##### [FLINK-38532](https://issues.apache.org/jira/browse/FLINK-38532), [FLINK-38311](https://issues.apache.org/jira/browse/FLINK-38311)

By specifying data freshness and query when creating [Materialized Table](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/materialized-table/overview/),
the engine automatically derives the schema for the materialized table and creates corresponding
data refresh pipeline to achieve the specified freshness.

From Flink 2.2, the FRESHNESS clause is not a mandatory part of the CREATE MATERIALIZED TABLE and
CREATE OR ALTER MATERIALIZED TABLE DDL statements. Flink 2.2 introduces a new MaterializedTableEnricher
interface. This provides a formal extension point for customizable default logic, allowing advanced
users and vendors to implement "smart" default behaviors (e.g., inferring freshness from upstream tables).

Besides this, users can use `DISTRIBUTED BY` or`DISTRIBUTED INTO` to support bucketing concept
for Materialized tables. Users can use `SHOW MATERIALIZED TABLES` to show all Materialized tables.

#### SinkUpsertMaterializer V2

##### [FLINK-38459](https://issues.apache.org/jira/browse/FLINK-38459)

SinkUpsertMaterializer is an operator in Flink that reconciles out of order changelog events before
sending them to an upsert sink. Performance of this operator degrades exponentially in some cases.
Flink 2.2 introduces a new implementation that is optimized for such cases.

#### Delta Join

##### [FLINK-38495](https://issues.apache.org/jira/browse/FLINK-38495), [FLINK-38511](https://issues.apache.org/jira/browse/FLINK-38511), [FLINK-38556](https://issues.apache.org/jira/browse/FLINK-38556)

In 2.1, Apache Flink has introduced a new delta join operator to mitigate the challenges caused by
big state in regular joins. It replaces the large state maintained by regular joins with a
bidirectional lookup-based join that directly reuses data from the source tables.

Flink 2.2 enhances support for converting more SQL patterns into delta joins. Delta joins now
support consuming CDC sources without DELETE operations, and allow projection and filter operations
after the source. Additionally, delta joins include support for caching, which helps reduce requests
to external storage.

See more details about the capabilities and usages of Flink's
[Delta Joins](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/tuning/#delta-joins).

#### SQL Types

##### [FLINK-20539](https://issues.apache.org/jira/browse/FLINK-20539), [FLINK-38181](https://issues.apache.org/jira/browse/FLINK-38181)

Before Flink 2.2, row types defined in SQL e.g. `SELECT CAST(f AS ROW&lt;i NOT NULL&gt;)` did ignore
the `NOT NULL` constraint. This was more aligned with the SQL standard but caused many type
inconsistencies and cryptic error message when working on nested data. For example, it prevented
using rows in computed columns or join keys. The new behavior takes the nullability into consideration.
The config option `table.legacy-nested-row-nullability` allows to restore the old behavior if required,
but it is recommended to update existing queries that ignored constraints before.

Casting to TIME type now considers the correct precision (0-3). Casting incorrect strings to time
(e.g. where the hour component is higher than 24) leads to a runtime exception now. Casting between
BINARY and VARBINARY should now correctly consider the target length.

#### Use UniqueKeys instead of Upsertkeys for state management

##### [FLINK-38209](https://issues.apache.org/jira/browse/FLINK-38209)

This is considerable optimization and an breaking change for the StreamingMultiJoinOperator.
As noted in the release notes, the operator was launched in an experimental state for Flink 2.1
since we're working on relevant optimizations that could be breaking changes.

### Runtime

#### Balanced Tasks Scheduling

##### [FLINK-31757](https://issues.apache.org/jira/browse/FLINK-31757)

Introducing a balanced tasks scheduling strategy to achieve task load balancing for TMs and reducing
job bottlenecks.

See more details about the capabilities and usages of
Flink's [Balanced Tasks Scheduling](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/tasks-scheduling/balanced_tasks_scheduling/).

#### Enhanced Job History Retention Policies for HistoryServer

##### [FLINK-38229](https://issues.apache.org/jira/browse/FLINK-38229)

Before Flink 2.2, HistoryServer supports only a quantity-based job archive retention policy and
is insufficient for scenarios, requiring time-based retention or combined rules. Users can use
the new configuration `historyserver.archive.retained-ttl` combining with `historyserver.archive.retained-jobs`
to fulfill more scenario requirements.

#### Metrics

##### [FLINK-38158](https://issues.apache.org/jira/browse/FLINK-38158), [FLINK-38353](https://issues.apache.org/jira/browse/FLINK-38353)

Since 2.2.0 users can now assign custom metric variables for each operator/transformation used in the
Job. Those variables are later converted to tags/labels by the metric reporters, allowing users to
tab/label specific operator's metrics. For example, you can use this to name and differentiate sources.

Users can now control the level of details of checkpoint spans via [traces.checkpoint.span-detail-level](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/#traces-checkpoint-span-detail-level).
Highest levels report tree of spans for each task and subtask. Reported custom spans can now contain
children spans. See more details in [Traces](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/traces/).

#### Introduce Event Reporting

##### [FLINK-37426](https://issues.apache.org/jira/browse/FLINK-37426)

Since 2.1.0 users are able to report custom events using the EventReporters. Since 2.2.0 Flink reports
some built-in/system events.

### Connectors

#### Introduce RateLimiter for Source

##### [FLINK-38497](https://issues.apache.org/jira/browse/FLINK-38497)

Flink jobs frequently exchange data with external systems, which consumes their network bandwidth
and CPU. When these resources are scarce, pulling data too aggressively can disrupt other workloads.
In Flink 2.2, we introduce a RateLimiter interface to provide request rate limiting for Scan Sources
and connector developers can integrate with rate limiting frameworks to implement their own read
restriction strategies. This feature is currently only available in the DataStream API.

#### Balanced splits assignment

##### [FLINK-38564](https://issues.apache.org/jira/browse/FLINK-38564)

SplitEnumerator is responsible for assigning splits, but it lacks visibility into the actual runtime
status or distribution of these splits. This makes it impossible for SplitEnumerator to guarantee
that the sharding is evenly distributed, and data skew is very likely to occur. From Flink 2.2,
SplitEnumerator has the information of the splits distribution and provides the ability to evenly
assign splits at runtime.

### Python

#### Support async function in Python DataStream API

##### [FLINK-38190](https://issues.apache.org/jira/browse/FLINK-38190)

In Flink 2.2, we have added support of async function in Python DataStream API. This enables Python
users to efficiently query external services in their Flink jobs, e.g. large-sized LLM which is
typically deployed in a standalone GPU cluster, etc.

Furthermore, we have provided comprehensive support to ensure the stability of external service
access. On one hand, we support limiting the number of concurrent requests sent to the external
service to avoid overwhelming it. On the other hand, we have also added retry support to tolerate
temporary unavailability which maybe caused by network jitter or other transient issues.

### Dependency upgrades

#### Upgrade commons-lang3 to version 3.18.0

##### [FLINK-38193](https://issues.apache.org/jira/browse/FLINK-38193)

Upgrade org.apache.commons:commons-lang3 from 3.12.0 to 3.18.0 to mitigate CVE-2025-48924.

#### Upgrade protobuf-java from 3.x to 4.32.1 with compatibility patch for parquet-protobuf

##### [FLINK-38547](https://issues.apache.org/jira/browse/FLINK-38547)

Flink now uses protobuf-java 4.32.1 (corresponding to Protocol Buffers version 32), upgrading from
protobuf-java 3.21.7 (Protocol Buffers version 21). This major upgrade enables:

- **Protobuf Editions Support**: Full support for the new `edition = "2023"` and `edition = "2024"`
  syntax introduced in Protocol Buffers v27+. Editions provide a unified approach that combines
  proto2 and proto3 functionality with fine-grained feature control.
- **Improved Proto3 Field Presence**: Better handling of optional fields in proto3 without the
  limitations of older protobuf versions, eliminating the need to set `protobuf.read-default-values`
  to `true` for field presence checking.
- **Enhanced Performance**: Leverages performance improvements and bug fixes from 11 Protocol
  Buffers releases (versions 22-32).
- **Modern Protobuf Features**: Access to newer protobuf capabilities including Edition 2024
  features and improved runtime behavior.

Users with existing proto2 and proto3 `.proto` files will continue to work without changes.
