---
title: "Table API & SQL"
nav-id: tableapi
nav-parent_id: dev
is_beta: false
nav-show_overview: true
nav-pos: 35
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

Apache Flink features two relational APIs - the Table API and SQL - for unified stream and batch processing. The Table API is a language-integrated query API for Scala and Java that allows the composition of queries from relational operators such as selection, filter, and join in a very intuitive way. Flink's SQL support is based on [Apache Calcite](https://calcite.apache.org) which implements the SQL standard. Queries specified in either interface have the same semantics and specify the same result regardless whether the input is a batch input (DataSet) or a stream input (DataStream).

The Table API and the SQL interfaces are tightly integrated with each other as well as Flink's DataStream and DataSet APIs. You can easily switch between all APIs and libraries which build upon the APIs. For instance, you can extract patterns from a DataStream using the [CEP library]({{ site.baseurl }}/dev/libs/cep.html) and later use the Table API to analyze the patterns, or you might scan, filter, and aggregate a batch table using a SQL query before running a [Gelly graph algorithm]({{ site.baseurl }}/dev/libs/gelly) on the preprocessed data.

**Please note that the Table API and SQL are not yet feature complete and are being actively developed. Not all operations are supported by every combination of \[Table API, SQL\] and \[stream, batch\] input.**

Dependency Structure
--------------------

Starting from Flink 1.9, Flink provides two different planner implementations for evaluating Table & SQL API programs: the Blink planner and the old planner that was available before Flink 1.9. Planners are responsible for
translating relational operators into an executable, optimized Flink job. Both of the planners come with different optimization rules and runtime classes.
They may also differ in the set of supported features.

<span class="label label-danger">Attention</span> For production use cases, we recommend the old planner that was present before Flink 1.9 for now.

All Table API and SQL components are bundled in the `flink-table` or `flink-table-blink` Maven artifacts.

The following dependencies are relevant for most projects:

* `flink-table-common`: A common module for extending the table ecosystem by custom functions, formats, etc.
* `flink-table-api-java`: The Table & SQL API for pure table programs using the Java programming language (in early development stage, not recommended!).
* `flink-table-api-scala`: The Table & SQL API for pure table programs using the Scala programming language (in early development stage, not recommended!).
* `flink-table-api-java-bridge`: The Table & SQL API with DataStream/DataSet API support using the Java programming language.
* `flink-table-api-scala-bridge`: The Table & SQL API with DataStream/DataSet API support using the Scala programming language.
* `flink-table-planner`: The table program planner and runtime. This was the only planner of Flink before the 1.9 release. It is still the recommended one.
* `flink-table-planner-blink`: The new Blink planner.
* `flink-table-runtime-blink`: The new Blink runtime.
* `flink-table-uber`: Packages the API modules above plus the old planner into a distribution for most Table & SQL API use cases. The uber JAR file `flink-table-*.jar` is located in the `/lib` directory of a Flink release by default.
* `flink-table-uber-blink`: Packages the API modules above plus the Blink specific modules into a distribution for most Table & SQL API use cases. The uber JAR file `flink-table-blink-*.jar` is located in the `/lib` directory of a Flink release by default.

See the [common API](common.html) page for more information about how to switch between the old and new Blink planner in table programs.

### Table Program Dependencies

Depending on the target programming language, you need to add the Java or Scala API to a project in order to use the Table API & SQL for defining pipelines:

{% highlight xml %}
<!-- Either... -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<!-- or... -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

Additionally, if you want to run the Table API & SQL programs locally within your IDE, you must add one of the
following set of modules, depending which planner you want to use:

{% highlight xml %}
<!-- Either... (for the old planner that was available before Flink 1.9) -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<!-- or.. (for the new Blink planner) -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

Internally, parts of the table ecosystem are implemented in Scala. Therefore, please make sure to add the following dependency for both batch and streaming applications:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

### Extension Dependencies

If you want to implement a [custom format]({{ site.baseurl }}/dev/table/sourceSinks.html#define-a-tablefactory) for interacting with Kafka or a set of [user-defined functions]({{ site.baseurl }}/dev/table/functions.html), the following dependency is sufficient and can be used for JAR files for the SQL Client:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

Currently, the module includes extension points for:
- `SerializationSchemaFactory`
- `DeserializationSchemaFactory`
- `ScalarFunction`
- `TableFunction`
- `AggregateFunction`

{% top %}

Where to go next?
-----------------

* [Concepts & Common API]({{ site.baseurl }}/dev/table/common.html): Shared concepts and APIs of the Table API and SQL.
* [Data Types]({{ site.baseurl }}/dev/table/types.html): Lists pre-defined data types and their properties.
* [Streaming Concepts]({{ site.baseurl }}/dev/table/streaming): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({{ site.baseurl }}/dev/table/functions.html): Available connectors and formats for reading and writing data to external systems.
* [Table API]({{ site.baseurl }}/dev/table/tableApi.html): Supported operations and API for the Table API.
* [SQL]({{ site.baseurl }}/dev/table/sql.html): Supported operations and syntax for SQL.
* [Built-in Functions]({{ site.baseurl }}/dev/table/functions.html): Supported functions in Table API and SQL.
* [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html): Play around with Flink SQL and submit a table program to a cluster without programming knowledge.

{% top %}
