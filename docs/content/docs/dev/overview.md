---
title: "Overview"
weight: 1
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

# Flink APIs

Apache Flink provides two main APIs for building streaming and batch applications: the **Table API** and the **DataStream API**. Both APIs can be used with Java, Scala, and Python.

## Choosing the Right Approach

Flink offers a spectrum from fully declarative to fully imperative programming:

| Approach | Description | When to Use |
|----------|-------------|-------------|
| **Declarative** (SQL / Table API) | Built-in relational operators | Standard ETL, analytics, joins, aggregations |
| **+ Custom Functions** (UDFs) | Custom functions within declarative pipelines | When built-in functions don't cover your needs |
| **+ State & Timers** (PTFs) | Low-level control for specific operations | Event-driven patterns, state/timers within a Table pipeline |
| **Imperative** (DataStream API) | Full control over the entire application | Complete control over state, windows, and processing logic |

### Start Declarative

For most use cases, start with SQL or the Table API:
- Flink optimizes your queries automatically
- Built-in operators cover common patterns (joins, aggregations, windows)
- Pattern matching is available via `MATCH_RECOGNIZE`

### Extend When Needed

When built-in capabilities aren't enough:
- **User-Defined Functions (UDFs)**: Add custom scalar, table, or aggregate functions
- **ProcessTableFunction**: Access state and timers for specific parts of your pipeline while staying in the Table API

### Go Imperative for Full Control

The DataStream API gives you fine-grained control over Flink's execution model, but requires understanding state management, operator chaining, and serialization. It's best suited for experienced users who have specific needs that the Table API cannot address.

Use the DataStream API when:
- You want to build the entire application imperatively
- Your use case doesn't fit the relational/table abstraction
- You need control over aspects that Table API doesn't expose

## Comparing the APIs

| | Table API | DataStream API |
|---|-----------|----------------|
| **Paradigm** | Declarative (what to compute) | Imperative (how to compute) |
| **Abstraction** | Relational operations on tables | Stream transformations |
| **Optimization** | Automatic query optimization | Manual optimization |
| **State management** | Automatic (with ProcessTableFunction for custom state) | Manual (fine-grained control) |
| **SQL integration** | Full SQL support | Limited |

## Mixing APIs

The Table API and DataStream API can be used together. You can:
- Convert a DataStream to a Table for relational operations
- Convert a Table back to a DataStream for low-level processing
- Use SQL within a DataStream application

See [DataStream API Integration]({{< ref "docs/dev/table/data_stream_api" >}}) for details.

## Where to Go Next

* [Table API]({{< ref "docs/dev/table/overview" >}}): Declarative API for relational operations.
* [DataStream API]({{< ref "docs/dev/datastream/overview" >}}): Imperative API for stream processing.
* [Configuration]({{< ref "docs/dev/configuration/overview" >}}): Project setup and dependencies.

### Getting Started Tutorials

* [Flink SQL Tutorial]({{< ref "docs/getting-started/quickstart-sql" >}}): Get started with SQL (no programming required).
* [Table API Tutorial]({{< ref "docs/getting-started/table_api" >}}): Build a streaming pipeline with the Table API.
* [DataStream API Tutorial]({{< ref "docs/getting-started/datastream" >}}): Build an event-driven application with the DataStream API.

{{< top >}}
