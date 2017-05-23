---
title: "Table API & SQL"
nav-id: tableapi
nav-parent_id: dev
is_beta: true
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

**Table API and SQL are experimental features**

The Table API is a SQL-like expression language for relational stream and batch processing that can be easily embedded in Flink's DataSet and DataStream APIs (Java and Scala).
The Table API and SQL interface operate on a relational `Table` abstraction, which can be created from external data sources, or existing DataSets and DataStreams. With the Table API, you can apply relational operators such as selection, aggregation, and joins on `Table`s.

`Table`s can also be queried with regular SQL, as long as they are registered (see [Registering Tables](#registering-tables)). The Table API and SQL offer equivalent functionality and can be mixed in the same program. When a `Table` is converted back into a `DataSet` or `DataStream`, the logical plan, which was defined by relational operators and SQL queries, is optimized using [Apache Calcite](https://calcite.apache.org/) and transformed into a `DataSet` or `DataStream` program.

**TODO: Check, update, and add**

* What are the Table API / SQL
  * Relational APIs
  * Unified APIs for batch and streaming
    * Semantics are the same
    * But not all operations can be efficiently mapped to streams
  * Table API: language-integrated queries (LINQ) in Scala and Java
  * SQL: Standard SQL

**Please notice: Not all operations are supported by all four combinations of Stream/Batch and TableAPI/SQL.**

* This will be replaced by the TOC
{:toc}

Setup
-----

The Table API and SQL are part of the *flink-table* Maven project.
The following dependency must be added to your project in order to use the Table API and SQL:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

*Note: The Table API is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/dev/linking.html).*

**TODO: Rework and add:**
* Project dependencies (flink-table + flink-scala or flink-streaming-scala)
* Copy `./opt/flink-table.jar` to `./lib`

{% top %}

Where to go next?
-----------------

* [Concepts & Common API]({{ site.baseurl }}/dev/table/common.html): Share concepts and API of the Table API and SQL.
* [Table API]({{ site.baseurl }}/dev/table/tableapi.html): Supported Operations and API for the Table API
* [SQL]({{ site.baseurl }}/dev/table/sql.html): Supported Operations and Syntax for SQL
* [Table Sources & Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html): Ingestion and emission of tables.
* [User-Defined Functions]({{ site.baseurl }}/dev/table/udfs.html): Defintion and usage of user-defined functions.

{% top %}