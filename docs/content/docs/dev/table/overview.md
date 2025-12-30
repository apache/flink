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

# Table API

The Table API is a language-integrated query API for Java, Scala, and Python that allows the composition of queries from relational operators such as selection, filter, and join in a very intuitive way. It shares the same underlying engine as [Flink SQL]({{< ref "docs/sql/overview" >}}), meaning queries specified in either interface have the same semantics and specify the same result regardless of whether the input is continuous (streaming) or bounded (batch).

The Table API and SQL integrate seamlessly with each other and Flink's DataStream API. You can easily switch between all APIs and libraries which build upon them. For instance, you can detect patterns from a table using the [`MATCH_RECOGNIZE` clause]({{< ref "docs/sql/reference/queries/match_recognize" >}}) and later use the DataStream API to build alerting based on the matched patterns.

## When to Use the Table API

Use the Table API when you want to:
- Write relational queries in Java, Scala, or Python code
- Compose queries programmatically with type-safe operators
- Combine SQL statements with programmatic table operations
- Integrate with the DataStream API for mixed use cases

For pure SQL usage without programming, see [Flink SQL]({{< ref "docs/sql/overview" >}}).

## Table Program Dependencies

You will need to add the Table API as a dependency to a project in order to use the Table API for defining data pipelines.

For more information on how to configure these dependencies for Java and Scala, please refer to the [project configuration]({{< ref "docs/dev/configuration/overview" >}}) section.

If you are using Python, please refer to the [PyFlink documentation]({{< ref "docs/dev/python" >}}).

## Where to Go Next

* [Concepts & Common API]({{< ref "docs/dev/table/common" >}}): Shared concepts and APIs of the Table API and SQL.
* [Data Types]({{< ref "docs/sql/data-types" >}}): Lists pre-defined data types and their properties.
* [Streaming Concepts]({{< ref "docs/concepts/sql-table-concepts/overview" >}}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}): Available connectors and formats for reading and writing data to external systems.
* [Table API Operations]({{< ref "docs/dev/table/tableApi" >}}): Supported operations and API for the Table API.
* [SQL Reference]({{< ref "docs/sql/reference/overview" >}}): Supported operations and syntax for SQL.
* [Built-in Functions]({{< ref "docs/sql/built-in-functions" >}}): Supported functions in Table API and SQL.
* [PyFlink]({{< ref "docs/dev/python" >}}): Python-specific Flink documentation.

{{< top >}}
