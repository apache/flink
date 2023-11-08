---
title: Overview
weight: 1
type: docs
aliases:
  - /dev/table/
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

# Table API & SQL

Apache Flink features two relational APIs - the Table API and SQL - for unified stream and batch
processing. The Table API is a language-integrated query API for Java, Scala, and Python that
allows the composition of queries from relational operators such as selection, filter, and join in
a very intuitive way. Flink's SQL support is based on [Apache Calcite](https://calcite.apache.org)
which implements the SQL standard. Queries specified in either interface have the same semantics
and specify the same result regardless of whether the input is continuous (streaming) or bounded (batch).

The Table API and SQL interfaces integrate seamlessly with each other and Flink's DataStream API. 
You can easily switch between all APIs and libraries which build upon them.
For instance, you can detect patterns from a table using [`MATCH_RECOGNIZE` clause]({{< ref "docs/dev/table/sql/queries/match_recognize" >}})
and later use the DataStream API to build alerting based on the matched patterns.

## Table Program Dependencies

You will need to add the Table API as a dependency to a project in order to use Table API & SQL for 
defining data pipelines.

For more information on how to configure these dependencies for Java and Scala, please refer to the 
[project configuration]({{< ref "docs/dev/configuration/overview" >}}) section.

If you are using Python, please refer to the documentation on the [Python API]({{< ref "docs/dev/python/overview" >}})

Where to go next?
-----------------

* [Concepts & Common API]({{< ref "docs/dev/table/common" >}}): Shared concepts and APIs of the Table API and SQL.
* [Data Types]({{< ref "docs/dev/table/types" >}}): Lists pre-defined data types and their properties.
* [Streaming Concepts]({{< ref "docs/dev/table/concepts/overview" >}}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}): Available connectors and formats for reading and writing data to external systems.
* [Table API]({{< ref "docs/dev/table/tableApi" >}}): Supported operations and API for the Table API.
* [SQL]({{< ref "docs/dev/table/sql/overview" >}}): Supported operations and syntax for SQL.
* [Built-in Functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}): Supported functions in Table API and SQL.
* [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}): Play around with Flink SQL and submit a table program to a cluster without programming knowledge.
* [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}): A service that enables the multiple clients to execute SQL from the remote in concurrency.
* [OLAP Quickstart]({{< ref "docs/dev/table/olap_quickstart" >}}): A quickstart to show how to set up a Flink OLAP service.
* [SQL JDBC Driver]({{< ref "docs/dev/table/jdbcDriver" >}}): A JDBC Driver that submits SQL statements to sql-gateway.

{{< top >}}
