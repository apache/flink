---
title: "Apache Cassandra Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 1
sub-nav-title: Cassandra
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

This connector provides a sink that writes data into a [Cassandra](https://cassandra.apache.org/) database.

The Flink Cassandra sink integrates with Flink's checkpointing mechanism to provide
exactly-once processing semantics. To achieve that, Flink buffers incoming records
and commits them only when a checkpoint completes.

To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-cassandra{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Cassandra
Follow the instructions from the [Cassandra Getting Started page](http://wiki.apache.org/cassandra/GettingStarted).

#### Cassandra Sink

Flink's Cassandra sink is called `CassandraExactlyOnceSink`.

The constructor accepts the following arguments:

1. The Host address
2. query to create a new table to write into (optional)
3. query to insert data the a table
4. checkpoint committer

A checkpoint committer stores additional information about completed checkpoints
in some resource. You can use a `CassandraCommitter` to store these in a separate
table in cassandra. Note that this table will NOT be cleaned up by Flink.

The CassandraCommitter constructor accepts the following arguments:
1. Host address
2. Keyspace
3. Table name

The CassandraExactlyOnceSink is implemented as a custom operator
instead of a sink, and as such has to be used in a transform() call.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.transform(
  "Cassandra Sink",
  null,
  new CassandraExactlyOnceSink<Tuple2<String, Integer>>(
    "127.0.0.1",
    "CREATE TABLE example.values (id text PRIMARY KEY, counter int);",
    "INSERT INTO example.values (id, counter) VALUES (?, ?);",
    new CassandraCommitter("127.0.0.1", "example", "checkpoints")));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.transform(
  "Cassandra Sink",
  null,
  new CassandraExactlyOnceSink[(String, Integer)](
    "127.0.0.1",
    "CREATE TABLE example.values (id text PRIMARY KEY, counter int);",
    "INSERT INTO example.values (id, counter) VALUES (?, ?);",
    new CassandraCommitter("127.0.0.1", "example", "checkpoints")));
{% endhighlight %}
</div>
</div>
