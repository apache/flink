---
title: "Apache Kafka Connector"
nav-title: Kafka
nav-parent_id: connectors
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

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from and writing data to Kafka topics with exactly-once guarantees.

* This will be replaced by the TOC
{:toc}

## Dependency

Apache Flink ships with a universal Kafka connector which attempts to track the latest version of the Kafka client.
The version of the client it uses may change between Flink releases.
Modern Kafka clients are backwards compatible with broker versions 0.10.0 or later.
For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

<div class="codetabs" markdown="1">
<div data-lang="universal" markdown="1">
{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-kafka{{ site.scala_version_suffix }}</artifactId>
	<version>{{ site.version }}</version>
</dependency>
{% endhighlight %} 
</div>
</div>

Flink's streaming connectors are not currently part of the binary distribution.
See how to link with them for cluster execution [here]({% link dev/project-configuration.md %}).

## Kafka Consumer

Flink's Kafka consumer - `FlinkKafkaConsumer` provides access to read from one or more Kafka topics.

The constructor accepts the following arguments:

1. The topic name / list of topic names
2. A DeserializationSchema / KafkaDeserializationSchema for deserializing the data from Kafka
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "group.id" the id of the consumer group

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
	.addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
val stream = env
    .addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties))
{% endhighlight %}
</div>
</div>

### The `DeserializationSchema`

The Flink Kafka Consumer needs to know how to turn the binary data in Kafka into Java/Scala objects.
The `KafkaDeserializationSchema` allows users to specify such a schema. The `T deserialize(ConsumerRecord<byte[], byte[]> record)` method gets called for each Kafka message, passing the value from Kafka.

For convenience, Flink provides the following schemas out of the box:

1. `TypeInformationSerializationSchema` (and `TypeInformationKeyValueSerializationSchema`) which creates
    a schema based on a Flink's `TypeInformation`. This is useful if the data is both written and read by Flink.
    This schema is a performant Flink-specific alternative to other generic serialization approaches.

2. `JsonDeserializationSchema` (and `JSONKeyValueDeserializationSchema`) which turns the serialized JSON
    into an ObjectNode object, from which fields can be accessed using `objectNode.get("field").as(Int/String/...)()`.
    The KeyValue objectNode contains a "key" and "value" field which contain all fields, as well as
    an optional "metadata" field that exposes the offset/partition/topic for this message.
    
3. `AvroDeserializationSchema` which reads data serialized with Avro format using a statically provided schema. It can
    infer the schema from Avro generated classes (`AvroDeserializationSchema.forSpecific(...)`) or it can work with `GenericRecords`
    with a manually provided schema (with `AvroDeserializationSchema.forGeneric(...)`). This deserialization schema expects that
    the serialized records DO NOT contain embedded schema.

    - There is also a version of this schema available that can lookup the writer's schema (schema which was used to write the record) in
      [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html). Using these deserialization schema
      record will be read with the schema that was retrieved from Schema Registry and transformed to a statically provided( either through 
      `ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` or `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)`).

    <br>To use this deserialization schema one has to add the following additional dependency:
    
<div class="codetabs" markdown="1">
<div data-lang="AvroDeserializationSchema" markdown="1">
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-avro</artifactId>
    <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="ConfluentRegistryAvroDeserializationSchema" markdown="1">
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-avro-confluent-registry</artifactId>
    <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

When encountering a corrupted message that cannot be deserialized for any reason the deserialization schema should return null which will result in the record being skipped.
Due to the consumer's fault tolerance (see below sections for more details), failing the job on the corrupted message will let the consumer attempt to deserialize the message again.
Therefore, if deserialization still fails, the consumer will fall into a non-stop restart and fail loop on that corrupted message.

### Kafka Consumers Start Position Configuration

The Flink Kafka Consumer allows configuring how the start positions for Kafka partitions are determined.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(...);
myConsumer.setStartFromEarliest();     // start from the earliest record possible
myConsumer.setStartFromLatest();       // start from the latest record
myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
myConsumer.setStartFromGroupOffsets(); // the default behaviour

DataStream<String> stream = env.addSource(myConsumer);
...
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val myConsumer = new FlinkKafkaConsumer[String](...)
myConsumer.setStartFromEarliest()      // start from the earliest record possible
myConsumer.setStartFromLatest()        // start from the latest record
myConsumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
myConsumer.setStartFromGroupOffsets()  // the default behaviour

val stream = env.addSource(myConsumer)
...
{% endhighlight %}
</div>
</div>

All versions of the Flink Kafka Consumer have the above explicit configuration methods for start position.

 * `setStartFromGroupOffsets` (default behaviour): Start reading partitions from
 the consumer group's (`group.id` setting in the consumer properties) committed
 offsets in Kafka brokers. If offsets could not be
 found for a partition, the `auto.offset.reset` setting in the properties will be used.
 * `setStartFromEarliest()` / `setStartFromLatest()`: Start from the earliest / latest
 record. Under these modes, committed offsets in Kafka will be ignored and
 not used as starting positions. If offsets become out of range for a partition,
 the `auto.offset.reset` setting in the properties will be used.
 * `setStartFromTimestamp(long)`: Start from the specified timestamp. For each partition, the record
 whose timestamp is larger than or equal to the specified timestamp will be used as the start position.
 If a partition's latest record is earlier than the timestamp, the partition will simply be read
 from the latest record. Under this mode, committed offsets in Kafka will be ignored and not used as
 starting positions.
 
You can also specify the exact offsets the consumer should start from for each partition:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);

myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)

myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
{% endhighlight %}
</div>
</div>

The above example configures the consumer to start from the specified offsets for
partitions 0, 1, and 2 of topic `myTopic`. The offset values should be the
next record that the consumer should read for each partition. Note that
if the consumer needs to read a partition which does not have a specified
offset within the provided offsets map, it will fallback to the default
group offsets behaviour (i.e. `setStartFromGroupOffsets()`) for that
particular partition.

Note that these start position configuration methods do not affect the start position when the job is
automatically restored from a failure or manually restored using a savepoint.
On restore, the start position of each Kafka partition is determined by the
offsets stored in the savepoint or checkpoint
(please see the next section for information about checkpointing to enable
fault tolerance for the consumer).

### Kafka Consumers and Fault Tolerance

With Flink's checkpointing enabled, the Flink Kafka Consumer will consume records from a topic and periodically checkpoint all
its Kafka offsets, together with the state of other operations. In case of a job failure, Flink will restore
the streaming program to the state of the latest checkpoint and re-consume the records from Kafka, starting from the offsets that were
stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.
To use fault tolerant Kafka Consumers, checkpointing of the topology needs to be enabled in the [job]({% link deployment/config.md %}#execution-checkpointing-interval).

If checkpointing is disabled, the Kafka consumer will periodically commit the offsets to Zookeeper.

### Kafka Consumers Topic and Partition Discovery

#### Partition discovery

The Flink Kafka Consumer supports discovering dynamically created Kafka partitions, and consumes them with
exactly-once guarantees. All partitions discovered after the initial retrieval of partition metadata (i.e., when the
job starts running) will be consumed from the earliest possible offset.

By default, partition discovery is disabled. To enable it, set a non-negative value
for `flink.partition-discovery.interval-millis` in the provided properties config,
representing the discovery interval in milliseconds. 

#### Topic discovery

The Kafka Consumer is also capable of discovering topics by matching topic names using regular expressions.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
    java.util.regex.Pattern.compile("test-topic-[0-9]"),
    new SimpleStringSchema(),
    properties);

DataStream<String> stream = env.addSource(myConsumer);
...
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer = new FlinkKafkaConsumer[String](
  java.util.regex.Pattern.compile("test-topic-[0-9]"),
  new SimpleStringSchema,
  properties)

val stream = env.addSource(myConsumer)
...
{% endhighlight %}
</div>
</div>

In the above example, all topics with names that match the specified regular expression
(starting with `test-topic-` and ending with a single digit) will be subscribed by the consumer
when the job starts running.

To allow the consumer to discover dynamically created topics after the job started running,
set a non-negative value for `flink.partition-discovery.interval-millis`. This allows
the consumer to discover partitions of new topics with names that also match the specified
pattern.

### Kafka Consumers Offset Committing Behaviour Configuration

The Flink Kafka Consumer allows configuring the behaviour of how offsets
are committed back to Kafka brokers. Note that the
Flink Kafka Consumer does not rely on the committed offsets for fault
tolerance guarantees. The committed offsets are only a means to expose
the consumer's progress for monitoring purposes.

The way to configure offset commit behaviour is different, depending on
whether checkpointing is enabled for the job.

 - *Checkpointing disabled:* if checkpointing is disabled, the Flink Kafka
 Consumer relies on the automatic periodic offset committing capability
 of the internally used Kafka clients. Therefore, to disable or enable offset
 committing, simply set the `enable.auto.commit` / `auto.commit.interval.ms` keys to appropriate values
 in the provided `Properties` configuration.
 
 - *Checkpointing enabled:* if checkpointing is enabled, the Flink Kafka
 Consumer will commit the offsets stored in the checkpointed states when
 the checkpoints are completed. This ensures that the committed offsets
 in Kafka brokers is consistent with the offsets in the checkpointed states.
 Users can choose to disable or enable offset committing by calling the
 `setCommitOffsetsOnCheckpoints(boolean)` method on the consumer (by default,
 the behaviour is `true`).
 Note that in this scenario, the automatic periodic offset committing
 settings in `Properties` is completely ignored.

### Kafka Consumers and Timestamp Extraction/Watermark Emission

In many scenarios, the timestamp of a record is embedded in the record itself, or the metadata of the `ConsumerRecord`.
In addition, users may want to emit watermarks either periodically, or irregularly, e.g. based on
special records in the Kafka stream that contain the current event-time watermark. For these cases, the Flink Kafka
Consumer allows the specification of a [watermark strategy]({% link dev/event_time.md %}).

You can specify your custom strategy as described
[here]({% link dev/event_timestamps_watermarks.md %}), or use one from the
[predefined ones]({% link dev/event_timestamp_extractors.md %}). 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer<String> myConsumer =
    new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy.
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)));

DataStream<String> stream = env.addSource(myConsumer);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer =
    new FlinkKafkaConsumer("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy.
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)))

val stream = env.addSource(myConsumer)
{% endhighlight %}
</div>
</div>


**Note**: If a watermark assigner depends on records read from Kafka to advance its watermarks
(which is commonly the case), all topics and partitions need to have a continuous stream of records.
Otherwise, the watermarks of the whole application cannot advance and all time-based operations,
such as time windows or functions with timers, cannot make progress. A single idle Kafka partition causes this behavior.
Consider setting appropriate [idelness timeouts]({% link dev/event_timestamps_watermarks.md %}#dealing-with-idle-sources) to mitigate this issue.
 
## Kafka Producer

Flink’s Kafka Producer - `FlinkKafkaProducer` allows writing a stream of records to one or more Kafka topics.

The constructor accepts the following arguments:

1. A default output topic where events should be written
2. A SerializationSchema / KafkaSerializationSchema for serializing data into Kafka
3. Properties for the Kafka client. The following properties are required:
    * "bootstrap.servers" (comma separated list of Kafka brokers)
4. A fault-tolerance semantic

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = ...

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");

FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
        "my-topic",                  // target topic
        new SimpleStringSchema(),    // serialization schema
        properties,                  // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

stream.addSink(myProducer);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[String] = ...

val properties = new Properties
properties.setProperty("bootstrap.servers", "localhost:9092")

val myProducer = new FlinkKafkaProducer[String](
        "my-topic",                  // target topic
        new SimpleStringSchema(),    // serialization schema
        properties,                  // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // fault-tolerance

stream.addSink(myProducer)
{% endhighlight %}
</div>
</div>

## The `SerializationSchema`

The Flink Kafka Producer needs to know how to turn Java/Scala objects into binary data.
The `KafkaSerializationSchema` allows users to specify such a schema.
The `ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp)` method gets called for each record, generating a `ProducerRecord` that is written to Kafka.

The gives users fine-grained control over how data is written out to Kafka. 
Through the producer record you can:
* Set header values
* Define keys for each record
* Specify custom partitioning of data

### Kafka Producers and Fault Tolerance

With Flink's checkpointing enabled, the `FlinkKafkaProducer` can provide
exactly-once delivery guarantees.

Besides enabling Flink's checkpointing, you can also choose three different modes of operating
chosen by passing appropriate `semantic` parameter to the `FlinkKafkaProducer`:

 * `Semantic.NONE`: Flink will not guarantee anything. Produced records can be lost or they can
 be duplicated.
 * `Semantic.AT_LEAST_ONCE` (default setting): This guarantees that no records will be lost (although they can be duplicated).
 * `Semantic.EXACTLY_ONCE`: Kafka transactions will be used to provide exactly-once semantic. Whenever you write
 to Kafka using transactions, do not forget about setting desired `isolation.level` (`read_committed`
 or `read_uncommitted` - the latter one is the default value) for any application consuming records
 from Kafka.

##### Caveats

`Semantic.EXACTLY_ONCE` mode relies on the ability to commit transactions
that were started before taking a checkpoint, after recovering from the said checkpoint. If the time
between Flink application crash and completed restart is larger than Kafka's transaction timeout
there will be data loss (Kafka will automatically abort transactions that exceeded timeout time).
Having this in mind, please configure your transaction timeout appropriately to your expected down
times.

Kafka brokers by default have `transaction.max.timeout.ms` set to 15 minutes. This property will
not allow to set transaction timeouts for the producers larger than it's value.
`FlinkKafkaProducer` by default sets the `transaction.timeout.ms` property in producer config to
1 hour, thus `transaction.max.timeout.ms` should be increased before using the
`Semantic.EXACTLY_ONCE` mode.

In `read_committed` mode of `KafkaConsumer`, any transactions that were not finished
(neither aborted nor completed) will block all reads from the given Kafka topic past any
un-finished transaction. In other words after following sequence of events:

1. User started `transaction1` and written some records using it
2. User started `transaction2` and written some further records using it
3. User committed `transaction2`

Even if records from `transaction2` are already committed, they will not be visible to
the consumers until `transaction1` is committed or aborted. This has two implications:

 * First of all, during normal working of Flink applications, user can expect a delay in visibility
 of the records produced into Kafka topics, equal to average time between completed checkpoints.
 * Secondly in case of Flink application failure, topics into which this application was writing,
 will be blocked for the readers until the application restarts or the configured transaction 
 timeout time will pass. This remark only applies for the cases when there are multiple
 agents/applications writing to the same Kafka topic.

**Note**:  `Semantic.EXACTLY_ONCE` mode uses a fixed size pool of KafkaProducers
per each `FlinkKafkaProducer` instance. One of each of those producers is used per one
checkpoint. If the number of concurrent checkpoints exceeds the pool size, `FlinkKafkaProducer`
will throw an exception and will fail the whole application. Please configure max pool size and max
number of concurrent checkpoints accordingly.

**Note**: `Semantic.EXACTLY_ONCE` takes all possible measures to not leave any lingering transactions
that would block the consumers from reading from Kafka topic more then it is necessary. However in the
event of failure of Flink application before first checkpoint, after restarting such application there
is no information in the system about previous pool sizes. Thus it is unsafe to scale down Flink
application before first checkpoint completes, by factor larger than `FlinkKafkaProducer.SAFE_SCALE_DOWN_FACTOR`.

## Kafka Connector Metrics

Flink's Kafka connectors provide some metrics through Flink's [metrics system]({% link ops/metrics.md %}) to analyze
the behavior of the connector.
The producers export Kafka's internal metrics through Flink's metric system for all supported versions.
The Kafka documentation lists all exported metrics in its [documentation](http://kafka.apache.org/documentation/#selector_monitoring).

In addition to these metrics, all consumers expose the `current-offsets` and `committed-offsets` for each topic partition.
The `current-offsets` refers to the current offset in the partition. This refers to the offset of the last element that
we retrieved and emitted successfully. The `committed-offsets` is the last committed offset.

The Kafka Consumers in Flink commit the offsets back to the Kafka brokers.
If checkpointing is disabled, offsets are committed periodically.
With checkpointing, the commit happens once all operators in the streaming topology have confirmed that they've created a checkpoint of their state. 
This provides users with at-least-once semantics for the offsets committed to Zookeeper or the broker. For offsets checkpointed to Flink, the system 
provides exactly once guarantees.

The offsets committed to ZK or the broker can also be used to track the read progress of the Kafka consumer. The difference between
the committed offset and the most recent offset in each partition is called the *consumer lag*. If the Flink topology is consuming
the data slower from the topic than new data is added, the lag will increase and the consumer will fall behind.
For large production deployments we recommend monitoring that metric to avoid increasing latency.

## Enabling Kerberos Authentication

Flink provides first-class support through the Kafka connector to authenticate to a Kafka installation
configured for Kerberos. Simply configure Flink in `flink-conf.yaml` to enable Kerberos authentication for Kafka like so:

1. Configure Kerberos credentials by setting the following -
 - `security.kerberos.login.use-ticket-cache`: By default, this is `true` and Flink will attempt to use Kerberos credentials in ticket caches managed by `kinit`.
 Note that when using the Kafka connector in Flink jobs deployed on YARN, Kerberos authorization using ticket caches will not work.
 This is also the case when deploying using Mesos, as authorization using ticket cache is not supported for Mesos deployments.
 - `security.kerberos.login.keytab` and `security.kerberos.login.principal`: To use Kerberos keytabs instead, set values for both of these properties.
 
2. Append `KafkaClient` to `security.kerberos.login.contexts`: This tells Flink to provide the configured Kerberos credentials to the Kafka login context to be used for Kafka authentication.

Once Kerberos-based Flink security is enabled, you can authenticate to Kafka with either the Flink Kafka Consumer or Producer
by simply including the following two settings in the provided properties configuration that is passed to the internal Kafka client:

- Set `security.protocol` to `SASL_PLAINTEXT` (default `NONE`): The protocol used to communicate to Kafka brokers.
When using standalone Flink deployment, you can also use `SASL_SSL`; please see how to configure the Kafka client for SSL [here](https://kafka.apache.org/documentation/#security_configclients). 
- Set `sasl.kerberos.service.name` to `kafka` (default `kafka`): The value for this should match the `sasl.kerberos.service.name` used for Kafka broker configurations.
A mismatch in service name between client and server configuration will cause the authentication to fail.

For more information on Flink configuration for Kerberos security, please see [here]({% link deployment/config.md %}).
You can also find [here]({% link deployment/security/security-kerberos.md %}) further details on how Flink internally setups Kerberos-based security.

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions
guide]({% link ops/upgrading.md %}). For Kafka, you additionally need
to follow these steps:

* Do not upgrade Flink and the Kafka Connector version at the same time.
* Make sure you have a `group.id` configured for your Consumer.
* Set `setCommitOffsetsOnCheckpoints(true)` on the consumer so that read
  offsets are committed to Kafka. It's important to do this before stopping and
  taking the savepoint. You might have to do a stop/restart cycle on the old
  connector version to enable this setting.
* Set `setStartFromGroupOffsets(true)` on the consumer so that we get read
  offsets from Kafka. This will only take effect when there is no read offset
  in Flink state, which is why the next step is very important.
* Change the assigned `uid` of your source/sink. This makes sure the new
  source/sink doesn't read state from the old source/sink operators.
* Start the new job with `--allow-non-restored-state` because we still have the
  state of the previous connector version in the savepoint.

## Troubleshooting

<div class="alert alert-warning">
If you have a problem with Kafka when using Flink, keep in mind that Flink only wraps
<a href="https://kafka.apache.org/documentation/#consumerapi">KafkaConsumer</a> or
<a href="https://kafka.apache.org/documentation/#producerapi">KafkaProducer</a>
and your problem might be independent of Flink and sometimes can be solved by upgrading Kafka brokers,
reconfiguring Kafka brokers or reconfiguring <tt>KafkaConsumer</tt> or <tt>KafkaProducer</tt> in Flink.
Some examples of common problems are listed below.
</div>

### Data loss

Depending on your Kafka configuration, even after Kafka acknowledges
writes you can still experience data loss. In particular keep in mind about the following properties
in Kafka config:

- `acks`
- `log.flush.interval.messages`
- `log.flush.interval.ms`
- `log.flush.*`

Default values for the above options can easily lead to data loss.
Please refer to the Kafka documentation for more explanation.

### UnknownTopicOrPartitionException

One possible cause of this error is when a new leader election is taking place,
for example after or during restarting a Kafka broker.
This is a retriable exception, so Flink job should be able to restart and resume normal operation.
It also can be circumvented by changing `retries` property in the producer settings.
However this might cause reordering of messages,
which in turn if undesired can be circumvented by setting `max.in.flight.requests.per.connection` to 1.

{% top %}
