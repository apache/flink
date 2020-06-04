---
title: "Apache Kafka SQL Connector"
nav-title: Kafka
nav-parent_id: sql-connectors
nav-pos: 1
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

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from and writing data to Kafka topics.

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
| universal           | `flink-connector-kafka{{site.scala_version_suffix}}`      | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 0.11.x              | `flink-connector-kafka-011{{site.scala_version_suffix}}`  | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-011{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 0.10.x              | `flink-connector-kafka-010{{site.scala_version_suffix}}`  | {% if site.is_stable %} [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-010{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |

Flink's streaming connectors are not currently part of the binary distribution.
See how to link with them for cluster execution [here]({{ site.baseurl}}/dev/projectsetup/dependencies.html).

How to create a Kafka table
----------------
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
 'properties.bootstrap.server' = 'localhost:9092',
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
      <td>Specify what connector to use, for Kafka the options are: 'kafka', 'kafka-0.11', 'kafka-0.10'.</td>
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
      <td>Kafka server connection string.</td>
    </tr>
    <tr>
      <td><h5>properties.group.id</h5></td>
      <td>required by source</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Consumer group in Kafka consumer, no need for Kafka producer</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Kafka connector requires to specify a format,
      the supported formats are 'csv', 'json' and 'avro'.
      Please refer to [Table Formats]({{ site.baseurl }}/dev/table/connect.html#table-formats) section for more details.
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">group-offsets</td>
      <td>String</td>
      <td>Startup mode for Kafka consumer, valid enumerations are <code>'earliest-offset'</code>, <code>'latest-offset'</code>, <code>'group-offsets'</code>, <code>'timestamp'</code> or <code>'specific-offsets'</code>.</td>
    </tr>
    <tr>
      <td><h5>scan.startup.specific-offsets</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specifies offsets for each partition in case of 'specific-offsets' startup mode, e.g. `partition:0,offset:42;partition:1,offset:300`.
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.timestamp-millis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Timestamp used in case of 'timestamp' startup mode, the 'timestamp' represents the milliseconds that have passed since January 1, 1970 00:00:00.000 GMT, e.g. '1591776274000' for '2020-06-10 16:04:34 +08:00'.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Output partitioning from Flink's partitions into Kafka's partitions. Valid enumerations are
      <ul>
        <li><span markdown="span">`fixed`</span>: each Flink partition ends up in at most one Kafka partition.</li>
        <li><span markdown="span">`round-robin`</span>: a Flink partition is distributed to Kafka partitions round-robin.</li>
        <li><span markdown="span">`custom class name`</span>: use a custom FlinkKafkaPartitioner subclass.</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

Features
----------------

### Specify the Start Reading Position
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

### Sink Partitioning
The config option `sink.partitioner` specifies output partitioning from Flink's partitions into Kafka's partitions. The valid enumerations are:
<ul>
<li><span markdown="span">`fixed`</span>: each Flink partition ends up in at most one Kafka partition.</li>
<li><span markdown="span">`round-robin`</span>: a Flink partition is distributed to Kafka partitions round-robin.</li>
<li><span markdown="span">`custom class name`</span>: use a custom FlinkKafkaPartitioner subclass.</li>
</ul>

<span class="label label-danger">Note</span> If the option value it neither `fixed` nor `round-robin`, then Flink would try to parse as
the `custom class name`, if that is not a full class name that implements `FlinkKafkaPartitioner`, an exception would be thrown.

If config option `sink.partitioner` is not specified, a partition will be assigned in a round-robin fashion.

### Consistency guarantees
The Kafka SQL sink only supports at-least-once writes now, for exactly-once writes, use the `DataStream` connector, see
<a href="{{ site.baseurl }}/dev/connectors/kafka.html#kafka-producers-and-fault-tolerance">Kafka Producers And Fault Tolerance</a> for more details.

Data Type Mapping
----------------
Kafka connector requires to specify a format, thus the supported data types are decided by the specific formats it specifies.
Please refer to <a href="{{ site.baseurl }}/dev/table/connectors/formats/index.html">Table Formats</a> section for more details.

{% top %}
