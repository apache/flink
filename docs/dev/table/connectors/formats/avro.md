---
title: "Avro Format"
nav-title: Avro
nav-parent_id: sql-formats
nav-pos: 3
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

The [Apache Avro](https://avro.apache.org/) format allows to read and write Avro data based on an Avro schema. Currently, the Avro schema is derived from table schema.

Dependencies
------------

In order to setup the Avro format, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

<div class="codetabs" markdown="1">
<div data-lang="SQL Client JAR" markdown="1">
You can download flink-sql-avro from [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro/{{site.version}}/flink-sql-avro-{{site.version}}.jar)
</div>
<div data-lang="Maven dependency" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

How to create a table with Avro format
----------------

Here is an example to create a table using Kafka connector and Avro format.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'avro'
)
{% endhighlight %}
</div>
</div>

Format Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what format to use, here should be <code>'avro'</code>.</td>
    </tr>
    <tr>
      <td><h5>avro.codec</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>For <a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a> only, the compression codec for avro. No compression as default. The valid enumerations are: deflate, snappy, bzip2, xz.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the Avro schema is always derived from table schema. Explicitly defining an Avro schema is not supported yet.
So the following table lists the type mapping from Flink type to Avro type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Avro type</th>
        <th class="text-left">Avro logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>string</td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>fixed</code></td>
      <td><code>decimal</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>long</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int</code></td>
      <td><code>date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>int</code></td>
      <td><code>time-millis</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>long</code></td>
      <td><code>timestamp-millis</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MAP</code><br>
      (key must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MULTISET</code><br>
      (element must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>record</code></td>
      <td></td>
    </tr>
    </tbody>
</table>

In addition to the types listed above, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.
