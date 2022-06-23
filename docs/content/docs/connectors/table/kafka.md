
---
title: Kafka
weight: 3
type: docs
aliases:
  - /dev/table/connectors/kafka.html
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

# Apache Kafka SQL Connector

{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Streaming Append Mode" >}}

The Kafka connector allows for reading data from and writing data into Kafka topics.

Dependencies
------------

{{< sql_download_table "kafka" >}}

The Kafka connector is not currently part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

How to create a Kafka table
----------------

The example below shows how to create a Kafka table:

```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
)
```

Available Metadata
------------------

The following connector metadata can be accessed as metadata columns in a table definition.

The `R/W` column defines whether a metadata field is readable (`R`) and/or writable (`W`).
Read-only columns must be declared `VIRTUAL` to exclude them during an `INSERT INTO` operation.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 30%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
      <th class="text-center" style="width: 5%">R/W</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>topic</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Topic name of the Kafka record.</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>partition</code></td>
      <td><code>INT NOT NULL</code></td>
      <td>Partition ID of the Kafka record.</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>headers</code></td>
      <td><code>MAP<STRING, BYTES> NOT NULL</code></td>
      <td>Headers of the Kafka record as a map of raw bytes.</td>
      <td><code>R/W</code></td>
    </tr>
    <tr>
      <td><code>leader-epoch</code></td>
      <td><code>INT NULL</code></td>
      <td>Leader epoch of the Kafka record if available.</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>offset</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td>Offset of the Kafka record in the partition.</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>Timestamp of the Kafka record.</td>
      <td><code>R/W</code></td>
    </tr>
    <tr>
      <td><code>timestamp-type</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Timestamp type of the Kafka record. Either "NoTimestampType",
          "CreateTime" (also set when writing metadata), or "LogAppendTime".</td>
      <td><code>R</code></td>
    </tr>
    </tbody>
</table>

The extended `CREATE TABLE` example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE KafkaTable (
  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
  `partition` BIGINT METADATA VIRTUAL,
  `offset` BIGINT METADATA VIRTUAL,
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
```

**Format Metadata**

The connector is able to expose metadata of the value format for reading. Format metadata keys
are prefixed with `'value.'`.

The following example shows how to access both Kafka and Debezium metadata fields:

```sql
CREATE TABLE KafkaTable (
  `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
  `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  -- from Kafka connector
  `offset` BIGINT METADATA VIRTUAL,  -- from Kafka connector
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);
```

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
      <td>Specify what connector to use, for Kafka use <code>'kafka'</code>.</td>
    </tr>
    <tr>
      <td><h5>topic</h5></td>
      <td>required for sink</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like <code>'topic-1;topic-2'</code>. Note, only one of "topic-pattern" and "topic" can be specified for sources. When the table is used as sink, the topic name is the topic to write data to. Note topic list is not supported for sinks.</td>
    </tr>
    <tr>
      <td><h5>topic-pattern</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running. Note, only one of "topic-pattern" and "topic" can be specified for sources.</td>
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
      <td>optional for source, not applicable for sink</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The id of the consumer group for Kafka source. If group ID is not specified, an automatically generated id "KafkaSource-{tableIdentifier}" will be used.</td>
    </tr>
    <tr>
      <td><h5>properties.*</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
         This can set and pass arbitrary Kafka configurations. Suffix names must match the configuration key defined in <a href="https://kafka.apache.org/documentation/#configuration">Kafka Configuration documentation</a>. Flink will remove the "properties." key prefix and pass the transformed key and values to the underlying KafkaClient. For example, you can disable automatic topic creation via <code>'properties.allow.auto.create.topics' = 'false'</code>. But there are some configurations that do not support to set, because Flink will override them, e.g. <code>'key.deserializer'</code> and <code>'value.deserializer'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the value part of Kafka messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page for
      more details and more format options.
      Note: Either this option or the <code>'value.format'</code> option are required.
      </td>
    </tr>
    <tr>
      <td><h5>key.format</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the key part of Kafka messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page
      for more details and more format options. Note: If a key format is defined, the <code>'key.fields'</code>
      option is required as well. Otherwise the Kafka records will have an empty key.
      </td>
    </tr>
    <tr>
      <td><h5>key.fields</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">[]</td>
      <td>List&lt;String&gt;</td>
      <td>Defines an explicit list of physical columns from the table schema that configure the data
        type for the key format. By default, this list is empty and thus a key is undefined.
        The list should look like <code>'field1;field2'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>key.fields-prefix</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Defines a custom prefix for all fields of the key format to avoid name clashes with fields
        of the value format. By default, the prefix is empty. If a custom prefix is defined, both the
        table schema and <code>'key.fields'</code> will work with prefixed names. When constructing the
        data type of the key format, the prefix will be removed and the non-prefixed names will be used
        within the key format. Please note that this option requires that <code>'value.fields-include'</code>
        must be set to <code>'EXCEPT_KEY'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>value.format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the value part of Kafka messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page
      for more details and more format options.
      Note: Either this option or the <code>'format'</code> option are required.
      </td>
    </tr>
    <tr>
      <td><h5>value.fields-include</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">ALL</td>
      <td><p>Enum</p>Possible values: [ALL, EXCEPT_KEY]</td>
      <td>Defines a strategy how to deal with key columns in the data type of the value format. By
        default, <code>'ALL'</code> physical columns of the table schema will be included in the value
        format which means that key columns appear in the data type for both the key and value format.
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
      <td><h5>scan.topic-partition-discovery.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>Interval for consumer to discover dynamically created Kafka topics and partitions periodically.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">'default'</td>
      <td>String</td>
      <td>Output partitioning from Flink's partitions into Kafka's partitions. Valid values are
      <ul>
        <li><code>default</code>: use the kafka default partitioner to partition records.</li>
        <li><code>fixed</code>: each Flink partition ends up in at most one Kafka partition.</li>
        <li><code>round-robin</code>: a Flink partition is distributed to Kafka partitions sticky round-robin. It only works when record's keys are not specified.</li>
        <li>Custom <code>FlinkKafkaPartitioner</code> subclass: e.g. <code>'org.mycompany.MyPartitioner'</code>.</li>
      </ul>
      See the following <a href="#sink-partitioning">Sink Partitioning</a> for more details.
      </td>
    </tr>
    <tr>
      <td><h5>sink.semantic</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">at-least-once</td>
      <td>String</td>
      <td>Deprecated: Please use <code>sink.delivery-guarantee</code>.</td>
    </tr>
    <tr>
      <td><h5>sink.delivery-guarantee</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">at-least-once</td>
      <td>String</td>
      <td>Defines the delivery semantic for the Kafka sink. Valid enumerationns are <code>'at-least-once'</code>, <code>'exactly-once'</code> and <code>'none'</code>. See <a href='#consistency-guarantees'>Consistency guarantees</a> for more details. </td>
    </tr>
    <tr>
      <td><h5>sink.transactional-id-prefix</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>If the delivery guarantee is configured as <code>'exactly-once'</code> this value must be set and is used a prefix for the identifier of all opened Kafka transactions.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the Kafka sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>

Features
----------------

### Key and Value Formats

Both the key and value part of a Kafka record can be serialized to and deserialized from raw bytes using
one of the given [formats]({{< ref "docs/connectors/table/formats/overview" >}}).

**Value Format**

Since a key is optional in Kafka records, the following statement reads and writes records with a configured
value format but without a key format. The `'format'` option is a synonym for `'value.format'`. All format
options are prefixed with the format identifier.

```sql
CREATE TABLE KafkaTable (,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
```

The value format will be configured with the following data type:

```text
ROW<`user_id` BIGINT, `item_id` BIGINT, `behavior` STRING>
```

**Key and Value Format**

The following example shows how to specify and configure key and value formats. The format options are
prefixed with either the `'key'` or `'value'` plus format identifier.

```sql
CREATE TABLE KafkaTable (
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'key.fields' = 'user_id;item_id',

  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'ALL'
)
```

The key format includes the fields listed in `'key.fields'` (using `';'` as the delimiter) in the same
order. Thus, it will be configured with the following data type:

```text
ROW<`user_id` BIGINT, `item_id` BIGINT>
```

Since the value format is configured with `'value.fields-include' = 'ALL'`, key fields will also end up in
the value format's data type:

```text
ROW<`user_id` BIGINT, `item_id` BIGINT, `behavior` STRING>
```

**Overlapping Format Fields**

The connector cannot split the table's columns into key and value fields based on schema information
if both key and value formats contain fields of the same name. The `'key.fields-prefix'` option allows
to give key columns a unique name in the table schema while keeping the original names when configuring
the key format.

The following example shows a key and value format that both contain a `version` field:

```sql
CREATE TABLE KafkaTable (
  `k_version` INT,
  `k_user_id` BIGINT,
  `k_item_id` BIGINT,
  `version` INT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'key.format' = 'json',
  'key.fields-prefix' = 'k_',
  'key.fields' = 'k_version;k_user_id;k_item_id',

  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY'
)
```

The value format must be configured in `'EXCEPT_KEY'` mode. The formats will be configured with
the following data types:

```text
key format:
ROW<`version` INT, `user_id` BIGINT, `item_id` BIGINT>

value format:
ROW<`version` INT, `behavior` STRING>
```

### Topic and Partition Discovery

The config option `topic` and `topic-pattern` specifies the topics or topic pattern to consume for source. The config option `topic` can accept topic list using semicolon separator like 'topic-1;topic-2'.
The config option `topic-pattern`  will use regular expression to discover the matched topic. For example, if the `topic-pattern` is `test-topic-[0-9]`, then all topics with names that match the specified regular expression (starting with `test-topic-` and ending with a single digit)) will be subscribed by the consumer when the job starts running.

To allow the consumer to discover dynamically created topics after the job started running, set a non-negative value for `scan.topic-partition-discovery.interval`. This allows the consumer to discover partitions of new topics with names that also match the specified pattern.

Please refer to [Kafka DataStream Connector documentation]({{< ref "docs/connectors/datastream/kafka" >}}#kafka-consumers-topic-and-partition-discovery) for more about topic and partition discovery.

Note that topic list and topic pattern only work in sources. In sinks, Flink currently only supports a single topic.

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

### CDC Changelog Source

Flink natively supports Kafka as a CDC changelog source. If messages in a Kafka topic are change event captured from other databases using a CDC tool, you can use the corresponding Flink CDC format to interpret the messages as INSERT/UPDATE/DELETE statements into a Flink SQL table.

The changelog source is a very useful feature in many cases, such as synchronizing incremental data from databases to other systems, auditing logs, materialized views on databases, temporal join changing history of a database table and so on.

Flink provides several CDC formats: 

* [debezium]({{< ref "docs/connectors/table/formats/debezium.md" >}}) 
* [canal]({{< ref "docs/connectors/table/formats/canal.md" >}})
* [maxwell]({{< ref "docs/connectors/table/formats/maxwell.md" >}}) 

### Sink Partitioning

The config option `sink.partitioner` specifies output partitioning from Flink's partitions into Kafka's partitions.
By default, Flink uses the [Kafka default partitioner](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java) to partition records. It uses the [sticky partition strategy](https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/) for records with null keys and uses a murmur2 hash to compute the partition for a record with the key defined.

In order to control the routing of rows into partitions, a custom sink partitioner can be provided. The 'fixed' partitioner will write the records in the same Flink partition into the same Kafka partition, which could reduce the cost of the network connections.

### Consistency guarantees

By default, a Kafka sink ingests data with at-least-once guarantees into a Kafka topic if the query is executed with [checkpointing enabled]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing).

With Flink's checkpointing enabled, the `kafka` connector can provide exactly-once delivery guarantees.

Besides enabling Flink's checkpointing, you can also choose three different modes of operating chosen by passing appropriate `sink.delivery-guarantee` option:

 * `none`: Flink will not guarantee anything. Produced records can be lost or they can be duplicated.
 * `at-least-once` (default setting): This guarantees that no records will be lost (although they can be duplicated).
 * `exactly-once`: Kafka transactions will be used to provide exactly-once semantic. Whenever you write
 to Kafka using transactions, do not forget about setting desired `isolation.level` (`read_committed`
 or `read_uncommitted` - the latter one is the default value) for any application consuming records
 from Kafka.

Please refer to [Kafka documentation]({{< ref "docs/connectors/datastream/kafka" >}}#kafka-producers-and-fault-tolerance) for more caveats about delivery guarantees.

### Source Per-Partition Watermarks

Flink supports to emit per-partition watermarks for Kafka. Watermarks are generated inside the Kafka
consumer. The per-partition watermarks are merged in the same way as watermarks are merged during streaming
shuffles. The output watermark of the source is determined by the minimum watermark among the partitions
it reads. If some partitions in the topics are idle, the watermark generator will not advance. You can
alleviate this problem by setting the [`'table.exec.source.idle-timeout'`]({{< ref "docs/dev/table/config" >}}#table-exec-source-idle-timeout)
option in the table configuration.

Please refer to [Kafka watermark strategies]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#watermark-strategies-and-the-kafka-connector)
for more details.

### Security
In order to enable security configurations including encryption and authentication, you just need to setup security
configurations with "properties." prefix in table options. The code snippet below shows configuring Kafka table to
use PLAIN as SASL mechanism and provide JAAS configuration:
```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  ...
  'properties.security.protocol' = 'SASL_PLAINTEXT',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";'
)
```
For a more complex example, use SASL_SSL as the security protocol and use SCRAM-SHA-256 as SASL mechanism:
```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  ...
  'properties.security.protocol' = 'SASL_SSL',
  /* SSL configurations */
  /* Configure the path of truststore (CA) provided by the server */
  'properties.ssl.truststore.location' = '/path/to/kafka.client.truststore.jks',
  'properties.ssl.truststore.password' = 'test1234',
  /* Configure the path of keystore (private key) if client authentication is required */
  'properties.ssl.keystore.location' = '/path/to/kafka.client.keystore.jks',
  'properties.ssl.keystore.password' = 'test1234',
  /* SASL configurations */
  /* Set SASL mechanism as SCRAM-SHA-256 */
  'properties.sasl.mechanism' = 'SCRAM-SHA-256',
  /* Set JAAS configurations */
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";'
)
```

Please note that the class path of the login module in `sasl.jaas.config` might be different if you relocate Kafka
client dependencies, so you may need to rewrite it with the actual class path of the module in the JAR.
For example if you are using SQL client JAR, which has relocate Kafka client dependencies to `org.apache.flink.kafka.shaded.org.apache.kafka`,
the path of plain login module should be `org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule` instead.

For detailed explanations of security configurations, please refer to
<a href="https://kafka.apache.org/documentation/#security">the "Security" section in Apache Kafka documentation</a>.

Data Type Mapping
----------------

Kafka stores message keys and values as bytes, so Kafka doesn't have schema or data types. The Kafka messages are deserialized and serialized by formats, e.g. csv, json, avro.
Thus, the data type mapping is determined by specific formats. Please refer to [Formats]({{< ref "docs/connectors/table/formats/overview" >}}) pages for more details.

{{< top >}}
