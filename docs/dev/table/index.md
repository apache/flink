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

Setup
-----

The Table API and SQL are bundled in the `flink-table` Maven artifact. 
The following dependency must be added to your project in order to use the Table API and SQL:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

In addition, you need to add a dependency for either Flink's Scala batch or streaming API. For a batch query you need to add:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

For a streaming query you need to add:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

**Note:** Due to an issue in Apache Calcite, which prevents the user classloaders from being garbage-collected, we do *not* recommend building a fat-jar that includes the `flink-table` dependency. Instead, we recommend configuring Flink to include the `flink-table` dependency in the system classloader. This can be done by copying the `flink-table.jar` file from the `./opt` folder to the `./lib` folder. See [these instructions]({{ site.baseurl }}/dev/linking.html) for further details.

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
