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

Depending on the target programming language, you need to add the Java or Scala API to a project
in order to use the Table API & SQL for defining pipelines.

{{< tabs "94f8aceb-507f-4c8f-977e-df00fe903203" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Python" >}}
{{< stable >}}
```bash
$ python -m pip install apache-flink {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}
{{< /tab >}}
{{< /tabs >}}

Additionally, if you want to run the Table API & SQL programs locally within your IDE, you must add the
following set of modules.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

### Extension Dependencies

If you want to implement a [custom format or connector]({{< ref "docs/dev/table/sourcesSinks" >}}) 
for (de)serializing rows or a set of [user-defined functions]({{< ref "docs/dev/table/functions/udfs" >}}),
the following dependency is sufficient and can be used for JAR files for the SQL Client:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

{{< top >}}

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

{{< top >}}
