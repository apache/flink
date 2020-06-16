---
title: "Apache Kafka SQL Connector"
nav-title: Kafka
nav-parent_id: sql-connectors
nav-pos: 2
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

<span class="label label-primary">Scan Source: Unbounded</span>
<span class="label label-primary">Sink: Streaming Append Mode</span>

* This will be replaced by the TOC
{:toc}

The Elasticsearch connector allows for reading data from and writing data into Kafka topics.

Dependencies
------------

Apache Flink ships with multiple Kafka connectors: universal, 0.10, and 0.11.
This universal Kafka connector attempts to track the latest version of the Kafka client.
The version of the client it uses may change between Flink releases.
Modern Kafka clients are backwards compatible with broker versions 0.10.0 or later.
For most users the universal Kafka connector is the most appropriate.
However, for Kafka versions 0.11.x and 0.10.x, we recommend using the dedicated ``0.11`` and ``0.10`` connectors, respectively.
For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

| Kafka Version       | Maven dependency                                          | SQL Client JAR         |
| :------------------ | :-------------------------------------------------------- | :----------------------|
| universal           | `flink-connector-kafka{{site.scala_version_suffix}}`      | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/kafka.html) {% endif %} |
| 0.11.x              | `flink-connector-kafka-0.11{{site.scala_version_suffix}}`  | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-0.11{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/kafka.html) {% endif %} |
| 0.10.x              | `flink-connector-kafka-0.10{{site.scala_version_suffix}}`  | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-0.10{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for [stable releases]({{ site.stable_baseurl }}/zh/dev/table/connectors/kafka.html) {% endif %} |

The Kafka connectors are not currently part of the binary distribution.
See how to link with them for cluster execution [here]({% link dev/project-configuration.zh.md %}).

How to create a Kafka table
----------------

The example below shows how to create a Kafka table:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE kafkaTable (
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
 'format' = 'csv',
 'scan.startup.mode' = 'earliest-offset'
)
{% endhighlight %}
</div>
</div>

Connector Options
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
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, for Kafka the options are: <code>'kafka'</code>, <code>'kafka-0.11'</code>, <code>'kafka-0.10'</code>.</td>
    </tr>
    <tr>
      <td><h5>topic</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Topic name from which the table is read.</td>
    </tr>
    <tr>
      <td><h5>properties.bootstrap.servers</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Comma separated list of Kafka brokers.</td>
    </tr>
    <tr>
      <td><h5>properties.group.id</h5></td>
      <td>required by source</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The id of the consumer group for Kafka source, optional for Kafka sink.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize Kafka messages.
      The supported formats are <code>'csv'</code>, <code>'json'</code>, <code>'avro'</code>, <code>'debezium-json'</code> and <code>'canal-json'</code>.
      Please refer to <a href="{% link dev/table/connectors/formats/index.zh.md %}">Formats</a> page for more details and more format options.
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">group-offsets</td>
      <td>String</td>
      <td>Startup mode for Kafka consumer, valid values are <code>'earliest-offset'</code>, <code>'latest-offset'</code>, <code>'group-offsets'</code>, <code>'timestamp'</code> and <code>'specific-offsets'</code>.
       See the following <a href="#start-reading-position">Start Reading Position</a> for more details.</td>
    </tr>
    <tr>
      <td><h5>scan.startup.specific-offsets</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify offsets for each partition in case of <code>'specific-offsets'</code> startup mode, e.g. <code>'partition:0,offset:42;partition:1,offset:300'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.timestamp-millis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Start from the specified epoch timestamp (milliseconds) used in case of <code>'timestamp'</code> startup mode.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Output partitioning from Flink's partitions into Kafka's partitions. Valid values are
      <ul>
        <li><code>fixed</code>: each Flink partition ends up in at most one Kafka partition.</li>
        <li><code>round-robin</code>: a Flink partition is distributed to Kafka partitions round-robin.</li>
        <li>Custom <code>FlinkKafkaPartitioner</code> subclass: e.g. <code>'org.mycompany.MyPartitioner'</code>.</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

Features
----------------

### Start Reading Position

The config option `scan.startup.mode` specifies the startup mode for Kafka consumer. The valid enumerations are:
<ul>
<li><span markdown="span">`group-offsets`</span>: start from committed offsets in ZK / Kafka brokers of a specific consumer group.</li>
<li><span markdown="span">`earliest-offset`</span>: start from the earliest offset possible.</li>
<li><span markdown="span">`latest-offset`</span>: start from the latest offset.</li>
<li><span markdown="span">`timestamp`</span>: start from user-supplied timestamp for each partition.</li>
<li><span markdown="span">`specific-offsets`</span>: start from user-supplied specific offsets for each partition.</li>
</ul>

The default option value is `group-offsets` which indicates to consume from last committed offsets in ZK / Kafka brokers.

If `timestamp` is specified, another config option `scan.startup.timestamp-millis` is required to specify a specific startup timestamp in milliseconds since January 1, 1970 00:00:00.000 GMT.

If `specific-offsets` is specified, another config option `scan.startup.specific-offsets` is required to specify specific startup offsets for each partition,
e.g. an option value `partition:0,offset:42;partition:1,offset:300` indicates offset `42` for partition `0` and offset `300` for partition `1`.

### Changelog Source

Flink natively supports Kafka as a changelog source. If messages in Kafka topic is change event captured from other databases using CDC tools, then you can use a CDC format to interpret messages as INSERT/UPDATE/DELETE messages into Flink SQL system.
Flink provides two CDC formats [debezium-json]({% link dev/table/connectors/formats/debezium.zh.md %}) and [canal-json]({% link dev/table/connectors/formats/canal.zh.md %}) to interpret change events captured by [Debezium](https://debezium.io/) and [Canal](https://github.com/alibaba/canal/wiki).
The changelog source is a very useful feature in many cases, such as synchronizing incremental data from databases to other systems, auditing logs, materialized views on databases, temporal join changing history of a database table and so on.
See more about how to use the CDC formats in [debezium-json]({% link dev/table/connectors/formats/debezium.zh.md %}) and [canal-json]({% link dev/table/connectors/formats/canal.zh.md %}).

### Sink Partitioning

The config option `sink.partitioner` specifies output partitioning from Flink's partitions into Kafka's partitions.
By default, a Kafka sink writes to at most as many partitions as its own parallelism (each parallel instance of the sink writes to exactly one partition).
In order to distribute the writes to more partitions or control the routing of rows into partitions, a custom sink partitioner can be provided. The `round-robin` partitioner is useful to avoid an unbalanced partitioning.
However, it will cause a lot of network connections between all the Flink instances and all the Kafka brokers.

### Consistency guarantees

By default, a Kafka sink ingests data with at-least-once guarantees into a Kafka topic if the query is executed with [checkpointing enabled]({% link dev/stream/state/checkpointing.zh.md %}#enabling-and-configuring-checkpointing).

Data Type Mapping
----------------

Kafka stores message keys and values as bytes, so Kafka doesn't have schema or data types. The Kafka messages are deserialized and serialized by formats, e.g. csv, json, avro.
Thus, the data type mapping is determined by specific formats. Please refer to [Formats]({% link dev/table/connectors/formats/index.zh.md %}) pages for more details.

{% top %}
