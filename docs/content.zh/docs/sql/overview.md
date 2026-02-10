---
title: "Overview"
weight: 1
type: docs
aliases:
  - /zh/dev/table/sql/overview.html
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

# Flink SQL

Flink SQL enables you to develop streaming and batch applications using standard SQL. Flink's SQL support is based on [Apache Calcite](https://calcite.apache.org) which implements the SQL standard. Queries have the same semantics and produce the same results regardless of whether the input is continuous (streaming) or bounded (batch).

Flink SQL integrates seamlessly with the [Table API]({{< ref "docs/dev/table/overview" >}}) and Flink's DataStream API. You can easily switch between all APIs and libraries which build upon them. For instance, you can detect patterns from a table using the [`MATCH_RECOGNIZE` clause]({{< ref "docs/sql/reference/queries/match_recognize" >}}) and later use the DataStream API to build alerting based on the matched patterns.

## Ways to Use Flink SQL

Flink SQL can be used through several interfaces depending on your use case:

| Interface | Description | Use Case |
|-----------|-------------|----------|
| [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}) | Interactive command-line interface | Ad-hoc queries, development, debugging |
| [SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}}) | REST and HiveServer2 endpoints | Remote SQL submission, integration with BI tools |
| [JDBC Driver]({{< ref "docs/sql/interfaces/jdbc-driver" >}}) | Standard JDBC connectivity | Application integration, BI tool connectivity |
| [Table API]({{< ref "docs/dev/table/overview" >}}) | Programmatic SQL execution | Embedded SQL in Java/Scala/Python applications |

## Key Concepts

Flink SQL is built on the concept of **dynamic tables**, which represent both bounded (batch) and unbounded (streaming) data. SQL queries on dynamic tables produce continuously updating results as new data arrives.

For a deeper understanding of how Flink SQL processes streaming data, see:
- [Dynamic Tables]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}) - How streaming data is represented as tables
- [Time Attributes]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}) - Event time and processing time in SQL
- [Streaming Concepts]({{< ref "docs/concepts/sql-table-concepts/overview" >}}) - Handling updating results and state

## Where to Go Next

* [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}): Interactive CLI for running SQL queries without programming.
* [SQL Reference]({{< ref "docs/sql/reference" >}}): Complete SQL syntax reference (DDL, DML, queries).
* [Built-in Functions]({{< ref "docs/sql/functions/built-in-functions" >}}): Available SQL functions for data transformation.
* [SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}}): REST and HiveServer2 service for remote SQL access.
* [JDBC Driver]({{< ref "docs/sql/interfaces/jdbc-driver" >}}): Connect to Flink SQL from any JDBC-compatible tool.
* [Catalogs]({{< ref "docs/sql/catalogs" >}}): Manage metadata for databases, tables, and functions.
* [Hive Compatibility]({{< ref "docs/sql/hive-compatibility" >}}): Integration with Apache Hive metastore and syntax.
* [Materialized Tables]({{< ref "docs/sql/materialized-table" >}}): Incrementally maintained query results.
* [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}): Available connectors and formats for reading and writing data.
* [Data Types]({{< ref "docs/sql/reference/data-types" >}}): Pre-defined data types and their properties.
* [Time Zone]({{< ref "docs/sql/timezone" >}}): How time zones affect timestamp processing.
* [Table API]({{< ref "docs/dev/table/overview" >}}): Programmatic API for relational operations.
* [OLAP Quickstart]({{< ref "docs/dev/table/olap_quickstart" >}}): Set up a Flink OLAP service for high-throughput SQL queries.

{{< top >}}
