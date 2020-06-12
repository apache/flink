---
title: "Table API & SQL"
nav-id: tableapi
nav-parent_id: dev
is_beta: false
nav-show_overview: true
nav-pos: 20
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

Apache Flink features two relational APIs - the Table API and SQL - for unified stream and batch processing. The Table API is a language-integrated query API for Java, Scala, and Python that allows the composition of queries from relational operators such as selection, filter, and join in a very intuitive way. Flink's SQL support is based on [Apache Calcite](https://calcite.apache.org) which implements the SQL standard. Queries specified in either interface have the same semantics and specify the same result regardless whether the input is a batch input (DataSet) or a stream input (DataStream).

The Table API and the SQL interfaces are tightly integrated with each other as well as Flink's DataStream and DataSet APIs. You can easily switch between all APIs and libraries which build upon the APIs. For instance, you can extract patterns from a DataStream using the [CEP library]({% link dev/libs/cep.md %}) and later use the Table API to analyze the patterns, or you might scan, filter, and aggregate a batch table using a SQL query before running a [Gelly graph algorithm]({% link dev/libs/gelly/index.md %}) on the preprocessed data.

**Please note that the Table API and SQL are not yet feature complete and are being actively developed. Not all operations are supported by every combination of \[Table API, SQL\] and \[stream, batch\] input.**

Table Planners
--------------

Table planners are responsible for translating relational operators into an exxecutable, optimized Flink job.
Flink supports two different planner implementations; the modern Blink planner and the legacy planner.
For production use cases, we recommend the blink planner that has become the default planner since 1.11.
See the [common API](common.html) page for more information on how to switch between the two planners.

### Table Program Dependencies

Depending on the target programming language, you need to add the Java or Scala API to a project in order to use the Table API & SQL for defining pipelines.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="python">
{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}
</div>
</div>

Additionally, if you want to run the Table API & SQL programs locally within your IDE, you must add the
following set of modules, depending which planner you want to use.

<div class="codetabs" markdown="1">
<div data-lang="Blink Planner" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="Legacy Planner" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
</div>

### Extension Dependencies

If you want to implement a [custom format]({% link dev/table/sourceSinks.md %}#define-a-tablefactory) for (de)serlializing rows or a set of [user-defined functions]({% link dev/table/functions/systemFunctions.md %}), the following dependency is sufficient and can be used for JAR files for the SQL Client:

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

* [Concepts & Common API]({% link dev/table/common.md %}): Shared concepts and APIs of the Table API and SQL.
* [Data Types]({% link dev/table/types.md %}): Lists pre-defined data types and their properties.
* [Streaming Concepts]({% link dev/table/streaming/index.md %}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({% link dev/table/connect.md %}): Available connectors and formats for reading and writing data to external systems.
* [Table API]({% link dev/table/tableApi.md %}): Supported operations and API for the Table API.
* [SQL]({% link dev/table/sql/index.md %}): Supported operations and syntax for SQL.
* [Built-in Functions]({% link dev/table/functions/systemFunctions.md %}): Supported functions in Table API and SQL.
* [SQL Client]({% link dev/table/sqlClient.md %}): Play around with Flink SQL and submit a table program to a cluster without programming knowledge.

{% top %}
