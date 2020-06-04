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

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from and writing data to Kafka topics with exactly-once guaruntees.

<span class="label label-primary">Scan Source: Unbounded</span>
<span class="label label-primary">Sink: Streaming Append Mode</span>

* This will be replaced by the TOC
{:toc}

Dependencies
------------

Apache Flink ships with multiple Kafka connectors: universal, 0.10, and 0.11.
This universal Kafka connector attempts to track the latest version of the Kafka client.
The version of the client it uses may change between Flink releases.
Modern Kafka clients are backwards compatible with broker versions 0.10.0 or later.
For most users the universal Kafka connector is the most appropriate.
However, for Kafka versions 0.11.x and 0.10.x, we recommend using the dedicated ``0.11`` and ``0.10`` connectors, respectively.
For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

{% if site.is_stable %}

| Kafka Version       | Maven dependency                                          | SQL Client JAR         |
| :------------------ | :-------------------------------------------------------- | :----------------------|
| universal           | `flink-connector-kafka{{site.scala_version_suffix}}`      | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) |
| 0.11.x              | `flink-connector-kafka-011{{site.scala_version_suffix}}`  | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-011{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) |
| 0.10.x              | `flink-connector-kafka-010{{site.scala_version_suffix}}`  | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka-010{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) |

<span class="label label-danger">Attention</span> The ``0.10`` sink does not support exactly-once writes to Kafka.
{% else %}

The dependency table is only available for stable releases.

{% endif %}

Flink's streaming connectors are not currently part of the binary distribution.
See how to link with them for cluster execution [here]({{ site.baseurl}}/dev/projectsetup/dependencies.html).

How to create a Kafka table
----------------
<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE kafkaTable (
 ...
) WITH (
 'connector' = 'kafka',
 'topic' = 'topic_name',
 'properties.bootstrap.server' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'csv',
 'scan.startup.mode' = 'group-offsets'
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
      <td>Required topic name from which the table is read.</td>
    </tr>
    <tr>
      <td><h5>properties.bootstrap.servers</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Required Kafka server connection string.</td>
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
      <td><h5>properties.group.id</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Required consumer group in Kafka consumer, no need for Kafka producer</td>
    </tr>
    <tr>
      <td><h5>scan.startup.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">group-offsets</td>
      <td>String</td>
      <td>Optional startup mode for Kafka consumer, valid enumerations are 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' or 'specific-offsets'.</td>
    </tr>
    <tr>
      <td><h5>scan.startup.specific-offsets</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Optional offsets used in case of 'specific-offsets' startup mode.
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.timestamp-millis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional timestamp used in case of 'timestamp' startup mode.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Optional output partitioning from Flink's partitions into Kafka's partitions. Valid enumerations are
      'fixed': (each Flink partition ends up in at most one Kafka partition),
      'round-robin': (a Flink partition is distributed to Kafka partitions round-robin)
      'custom class name': (use a custom FlinkKafkaPartitioner subclass).
      </td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------
Kafka connector requires to specify a format, thus the supported data types are decided by the specific formats it specifies.
Please refer to [Table Formats]({{ site.baseurl }}/dev/table/connect.html#table-formats) section for more details.

{% top %}
