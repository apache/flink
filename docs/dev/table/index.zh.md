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

All Table API and SQL components are bundled in the `flink-table` Maven artifact.

The following dependencies are relevant for most projects:

* `flink-table-common`: A common module for extending the table ecosystem by custom functions, formats, etc.
* `flink-table-api-java`: The Table & SQL API for pure table programs using the Java programming language (in early development stage, not recommended!).
* `flink-table-api-scala`: The Table & SQL API for pure table programs using the Scala programming language (in early development stage, not recommended!).
* `flink-table-api-java-bridge`: The Table & SQL API with DataStream/DataSet API support using the Java programming language.
* `flink-table-api-scala-bridge`: The Table & SQL API with DataStream/DataSet API support using the Scala programming language.
* `flink-table-planner`: The table program planner and runtime.
* `flink-table-uber`: Packages the modules above into a distribution for most Table & SQL API use cases. The uber JAR file `flink-table*.jar` is located in the `/opt` directory of a Flink release and can be moved to `/lib` if desired.

### Table Program Dependencies

The following dependencies must be added to a project in order to use the Table API & SQL for defining pipelines:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Additionally, depending on the target programming language, you need to add the Java or Scala API.

{% highlight xml %}
<!-- Either... -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
<!-- or... -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Internally, parts of the table ecosystem are implemented in Scala. Therefore, please make sure to add the following dependency for both batch and streaming applications:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

### Extension Dependencies

If you want to implement a [custom format](({{ site.baseurl }}/dev/table/sourceSinks.html#define-a-tablefactory)) for interacting with Kafka or a set of [user-defined functions]({{ site.baseurl }}/dev/table/functions.html), the following dependency is sufficient and can be used for JAR files for the SQL Client:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{site.version}}</version>
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
* [Streaming Concepts]({{ site.baseurl }}/dev/table/streaming): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Connect to External Systems]({{ site.baseurl }}/dev/table/functions.html): Available connectors and formats for reading and writing data to external systems.
* [Table API]({{ site.baseurl }}/dev/table/tableApi.html): Supported operations and API for the Table API.
* [SQL]({{ site.baseurl }}/dev/table/sql.html): Supported operations and syntax for SQL.
* [Built-in Functions]({{ site.baseurl }}/dev/table/functions.html): Supported functions in Table API and SQL.
* [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html): Play around with Flink SQL and submit a table program to a cluster without programming knowledge.

{% top %}