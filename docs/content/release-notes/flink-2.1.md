---
title: "Release Notes - Flink 2.1"
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

# Release notes - Flink 2.1

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.0 and Flink 2.1. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.1.

### Table SQL / API

#### Model DDLs using Table API

##### [FLINK-37548](https://issues.apache.org/jira/browse/FLINK-37548)

Since Flink 2.0, we have introduced dedicated syntax for AI models, enabling users to define models
as easily as creating catalog objects and invoke them like standard functions or table functions in
SQL statements. In Flink 2.1, we also added Model DDLs Table API support, allowing users to define
and manage AI models programmatically through the Table API in both Java and Python.

#### Realtime AI Function

##### [FLINK-34992](https://issues.apache.org/jira/browse/FLINK-34992), [FLINK-37777](https://issues.apache.org/jira/browse/FLINK-37777)

Based on the AI model DDL, we also expanded the `ML_PREDICT` table-valued function (TVF) to perform
realtime model inference in SQL queries, applying machine learning models to data streams
seamlessly. The implementation supports both Flink builtin model providers (OpenAI) and interfaces
for users to define custom model providers, accelerating Flink's evolution from a real-time data
processing engine to a unified realtime AI platform. Looking ahead, we plan to introduce more AI
functions to unlock end-to-end experience for real-time data processing, model training, and
inference.

See more details about the capabilities and usages of
Flink's [Model Inference](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/table/sql/queries/model-inference/).

#### Variant Type

##### [FLINK-37922](https://issues.apache.org/jira/browse/FLINK-37922)

Variant is a new data type for semi-structured data(e.g. JSON), it supports storing any
semi-structured data, including ARRAY, MAP(with STRING keys), and scalar types—while preserving
field type information in a JSON-like structure. Unlike ROW and STRUCTURED types, VARIANT provides
superior flexibility for handling deeply nested and evolving schemas.

Users can use `PARSE_JSON` or`TRY_PARSE_JSON` to convert JSON-formatted VARCHAR data to VARIANT. In
addition, table formats like Apache Paimon now support the VARIANT type, this enable
users to efficiently process semi-structured data in lakehouse using Flink SQL.

#### Structured Type Enhancements

##### [FLINK-37861](https://issues.apache.org/jira/browse/FLINK-37861)

Enabling declare user-defined objects via STRUCTURED TYPE directly in `CREATE TABLE` DDL
statements, resolving critical type equivalence issues and significantly improving API usability.

#### Delta Join

##### [FLINK-37836](https://issues.apache.org/jira/browse/FLINK-37836)

Introduced a new DeltaJoin operator in stream processing jobs, along with optimizations for simple
streaming join pipeline. Compared to traditional streaming join, delta join requires significantly
less state, effectively mitigating issues related to large state, including resource bottlenecks,
slow checkpointing, and lengthy job recovery times. This feature is enabled by default. More details
can be found
at [Delta Join](https://cwiki.apache.org/confluence/display/FLINK/FLIP-486%3A+Introduce+A+New+DeltaJoin)

#### Multiple Regular Joins

##### [FLINK-37859](https://issues.apache.org/jira/browse/FLINK-37859)

Streaming Flink jobs with multiple cascaded streaming joins often experience operational
instability and performance degradation due to large state sizes. This release introduces a
multi-join operator (`StreamingMultiJoinOperator`) that drastically reduces state size
by eliminating intermediate results. The operator achieves this by processing joins across all input
streams simultaneously within a single operator instance, storing only raw input records instead of
propagated join output.

This "zero intermediate state" approach primarily targets state reduction, offering substantial
benefits in resource consumption and operational stability. This feature is now available for
pipelines with multiple INNER/LEFT joins that share at least one common join key, enable with
`SET 'table.optimizer.multi-join.enabled' = 'true'`.

#### Async Lookup Join Enhancements

##### [FLINK-37874](https://issues.apache.org/jira/browse/FLINK-37874)

Support handling records in order based on upsert key (the unique key in the input stream deduced by
planner) while allowing parallel processing of different keys to achieve better throughput when
processing changelog data stream.

#### Sink Reuse

##### [FLINK-37227](https://issues.apache.org/jira/browse/FLINK-37227)

Within a single Flink job, when writing multiple `INSERT INTO` statements updating identical
columns (different columns will be supported in next release) of a target table, the planner will
optimize the execution plan and merge the sink nodes to achieve reuse. This will be a great
usability improvement for users using partial-update features with data lake storages like Apache
Paimon.

#### Support Smile Format for Compiled Plan Serialization

##### [FLINK-37341](https://issues.apache.org/jira/browse/FLINK-37341)

This release adds smile binary format support for compiled plans, providing a memory-efficient
alternative to JSON for serialization/deserialization. By default JSON is used, in order to use
smile format need to call `CompiledPlan#asSmileBytes` and `PlanReference#fromSmileBytes` method.

### Runtime

#### Add Pluggable Batching for Async Sink

##### [FLINK-37298](https://issues.apache.org/jira/browse/FLINK-37298)

Introducing a pluggable batching mechanism for async sink that allows users to define custom
batching write strategies tailored to specific requirements.

#### Split-level Watermark Metrics

##### [FLINK-37410](https://issues.apache.org/jira/browse/FLINK-37410)

We adds some split level watermark metrics, covering watermark progress and per-split state gauges
to enhance the watermark observability:

- `currentWatermark`: the last watermark this split has received.
- `activeTimeMsPerSecond`: the time this split is active per second.
- `pausedTimeMsPerSecond`: the time this split is paused due to watermark alignment per second.
- `idleTimeMsPerSecond`: the time this split is marked idle by idleness detection per second.
- `accumulatedActiveTimeMs`: accumulated time this split was active since registered.
- `accumulatedPausedTimeMs`: accumulated time this split was paused since registered.
- `accumulatedIdleTimeMs`: accumulated time this split was idle since registered.

### Connectors

#### Introduce SQL Connector for Keyed State

##### [FLINK-36929](https://issues.apache.org/jira/browse/FLINK-36929)

In this release, we introduce a new connector for keyed state. This connector allows
users to query keyed state directly from checkpoint or savepoint using Flink SQL, making it easier
to inspect, debug, and validate the state of Flink jobs without custom tooling. This feature is
especially useful for analyzing long-running jobs and validating state migrations.

### Python

#### Adds support of Python 3.12 and removes support of Python 3.8

##### [FLINK-37823](https://issues.apache.org/jira/browse/FLINK-37823), [FLINK-37776](https://issues.apache.org/jira/browse/FLINK-37776)

PyFlink 2.1 will support Python 3.12 and remove the support for Python 3.8.

### Dependency upgrades

#### Upgrade flink-shaded version to 20.0

##### [FLINK-37376](https://issues.apache.org/jira/browse/FLINK-37376)

Bump flink-shaded version to 20.0 to support Smile format.

#### Upgrade Parquet version to 1.15.3

##### [FLINK-37760](https://issues.apache.org/jira/browse/FLINK-37760)

Bump parquet version to 1.15.3 to resolve parquet-avro module
vulnerability found in [CVE-2025-30065](https://nvd.nist.gov/vuln/detail/CVE-2025-30065).
