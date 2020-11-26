---
title: "Amazon AWS Kinesis Streams Connector"
nav-title: Kinesis
nav-parent_id: connectors
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

* This will be replaced by the TOC
{:toc}

The Kinesis connector provides access to [Amazon AWS Kinesis Streams](http://aws.amazon.com/kinesis/streams/).

To use the connector, add the following Maven dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kinesis{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

<span class="label label-danger">Attention</span> Prior to Flink version 1.10.0 the `flink-connector-kinesis{{ site.scala_version_suffix }}` has a dependency on code licensed under the [Amazon Software License](https://aws.amazon.com/asl/).
Linking to the prior versions of flink-connector-kinesis will include this code into your application.

Due to the licensing issue, the `flink-connector-kinesis{{ site.scala_version_suffix }}` artifact is not deployed to Maven central for the prior versions. Please see the version specific documentation for further information.

## Using the Amazon Kinesis Streams Service
Follow the instructions from the [Amazon Kinesis Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html)
to setup Kinesis streams.

## Configuring Access to Kinesis with IAM
Make sure to create the appropriate IAM policy to allow reading / writing to / from the Kinesis streams. See examples [here](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html).

Depending on your deployment you would choose a different Credentials Provider to allow access to Kinesis.
By default, the `AUTO` Credentials Provider is used.
If the access key ID and secret key are set in the configuration, the `BASIC` provider is used.

A specific Credentials Provider can **optionally** be set by using the `AWSConfigConstants.AWS_CREDENTIALS_PROVIDER` setting.

Supported Credential Providers are:
* `AUTO` - Using the default AWS Credentials Provider chain that searches for credentials in the following order: `ENV_VARS`, `SYS_PROPS`, `WEB_IDENTITY_TOKEN`, `PROFILE` and EC2/ECS credentials provider.
* `BASIC` - Using access key ID and secret key supplied as configuration.
* `ENV_VAR` - Using `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` environment variables.
* `SYS_PROP` - Using Java system properties aws.accessKeyId and aws.secretKey.
* `PROFILE` - Use AWS credentials profile file to create the AWS credentials.
* `ASSUME_ROLE` - Create AWS credentials by assuming a role. The credentials for assuming the role must be supplied.
* `WEB_IDENTITY_TOKEN` - Create AWS credentials by assuming a role using Web Identity Token.

## Kinesis Consumer

The `FlinkKinesisConsumer` is an exactly-once parallel streaming data source that subscribes to multiple AWS Kinesis
streams within the same AWS service region, and can transparently handle resharding of streams while the job is running. Each subtask of the consumer is
responsible for fetching data records from multiple Kinesis shards. The number of shards fetched by each subtask will
change as shards are closed and created by Kinesis.

Before consuming data from Kinesis streams, make sure that all streams are created with the status "ACTIVE" in the AWS dashboard.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties consumerConfig = new Properties();
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val consumerConfig = new Properties()
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

val env = StreamExecutionEnvironment.getExecutionEnvironment

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
{% endhighlight %}
</div>
</div>

The above is a simple example of using the consumer. Configuration for the consumer is supplied with a `java.util.Properties`
instance, the configuration keys for which can be found in `AWSConfigConstants` (AWS-specific parameters) and
`ConsumerConfigConstants` (Kinesis consumer parameters). The example
demonstrates consuming a single Kinesis stream in the AWS region "us-east-1". The AWS credentials are supplied using the basic method in which
the AWS access key ID and secret access key are directly supplied in the configuration. Also, data is being consumed
from the newest position in the Kinesis stream (the other option will be setting `ConsumerConfigConstants.STREAM_INITIAL_POSITION`
to `TRIM_HORIZON`, which lets the consumer start reading the Kinesis stream from the earliest record possible).

Other optional configuration keys for the consumer can be found in `ConsumerConfigConstants`.

Note that the configured parallelism of the Flink Kinesis Consumer source
can be completely independent of the total number of shards in the Kinesis streams.
When the number of shards is larger than the parallelism of the consumer,
then each consumer subtask can subscribe to multiple shards; otherwise
if the number of shards is smaller than the parallelism of the consumer,
then some consumer subtasks will simply be idle and wait until it gets assigned
new shards (i.e., when the streams are resharded to increase the
number of shards for higher provisioned Kinesis service throughput).

Also note that the assignment of shards to subtasks may not be optimal when
shard IDs are not consecutive (as result of dynamic re-sharding in Kinesis).
For cases where skew in the assignment leads to significant imbalanced consumption,
a custom implementation of `KinesisShardAssigner` can be set on the consumer.

### Configuring Starting Position

The Flink Kinesis Consumer currently provides the following options to configure where to start reading Kinesis streams, simply by setting `ConsumerConfigConstants.STREAM_INITIAL_POSITION` to
one of the following values in the provided configuration properties (the naming of the options identically follows [the namings used by the AWS Kinesis Streams service](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax)):

- `LATEST`: read all shards of all streams starting from the latest record.
- `TRIM_HORIZON`: read all shards of all streams starting from the earliest record possible (data may be trimmed by Kinesis depending on the retention settings).
- `AT_TIMESTAMP`: read all shards of all streams starting from a specified timestamp. The timestamp must also be specified in the configuration
properties by providing a value for `ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP`, in one of the following date pattern :
    - a non-negative double value representing the number of seconds that has elapsed since the Unix epoch (for example, `1459799926.480`).
    - a user defined pattern, which is a valid pattern for `SimpleDateFormat` provided by `ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT`.
    If `ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT` is not defined then the default pattern will be `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`
    (for example, timestamp value is `2016-04-04` and pattern is `yyyy-MM-dd` given by user or timestamp value is `2016-04-04T19:58:46.480-00:00` without given a pattern).

### Fault Tolerance for Exactly-Once User-Defined State Update Semantics

With Flink's checkpointing enabled, the Flink Kinesis Consumer will consume records from shards in Kinesis streams and
periodically checkpoint each shard's progress. In case of a job failure, Flink will restore the streaming program to the
state of the latest complete checkpoint and re-consume the records from Kinesis shards, starting from the progress that
was stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.

To use fault tolerant Kinesis Consumers, checkpointing of the topology needs to be enabled at the execution environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
{% endhighlight %}
</div>
</div>

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
Therefore, if the topology fails due to loss of a TaskManager, there must still be enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.

### Event Time for Consumed Records

If streaming topologies choose to use the [event time notion]({% link dev/event_time.zh.md %}) for record
timestamps, an *approximate arrival timestamp* will be used by default. This timestamp is attached to records by Kinesis once they
were successfully received and stored by streams. Note that this timestamp is typically referred to as a Kinesis server-side
timestamp, and there are no guarantees about the accuracy or order correctness (i.e., the timestamps may not always be
ascending).

Users can choose to override this default with a custom timestamp, as described [here]({% link dev/event_timestamps_watermarks.zh.md %}),
or use one from the [predefined ones]({% link dev/event_timestamp_extractors.zh.md %}). After doing so,
it can be passed to the consumer in the following way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>(
    "kinesis_stream_name",
    new SimpleStringSchema(),
    kinesisConsumerConfig);
consumer.setPeriodicWatermarkAssigner(new CustomAssignerWithPeriodicWatermarks());
DataStream<String> stream = env
	.addSource(consumer)
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val consumer = new FlinkKinesisConsumer[String](
    "kinesis_stream_name",
    new SimpleStringSchema(),
    kinesisConsumerConfig);
consumer.setPeriodicWatermarkAssigner(new CustomAssignerWithPeriodicWatermarks());
val stream = env
	.addSource(consumer)
	.print();
{% endhighlight %}
</div>
</div>

Internally, an instance of the assigner is executed per shard / consumer thread (see threading model below).
When an assigner is specified, for each record read from Kinesis, the extractTimestamp(T element, long previousElementTimestamp)
is called to assign a timestamp to the record and getCurrentWatermark() to determine the new watermark for the shard.
The watermark of the consumer subtask is then determined as the minimum watermark of all its shards and emitted periodically.
The per shard watermark is essential to deal with varying consumption speed between shards, that otherwise could lead
to issues with downstream logic that relies on the watermark, such as incorrect late data dropping.

By default, the watermark is going to stall if shards do not deliver new records.
The property `ConsumerConfigConstants.SHARD_IDLE_INTERVAL_MILLIS` can be used to avoid this potential issue through a
timeout that will allow the watermark to progress despite of idle shards.

### Event Time Alignment for Shard Consumers

The Flink Kinesis Consumer optionally supports synchronization between parallel consumer subtasks (and their threads)
to avoid the event time skew related problems described in [Event time synchronization across sources](https://issues.apache.org/jira/browse/FLINK-10886).

To enable synchronization, set the watermark tracker on the consumer:

<div data-lang="java" markdown="1">
{% highlight java %}
JobManagerWatermarkTracker watermarkTracker =
    new JobManagerWatermarkTracker("myKinesisSource");
consumer.setWatermarkTracker(watermarkTracker);
{% endhighlight %}
</div>

The `JobManagerWatermarkTracker` will use a global aggregate to synchronize the per subtask watermarks. Each subtask
uses a per shard queue to control the rate at which records are emitted downstream based on how far ahead of the global
watermark the next record in the queue is.

The "emit ahead" limit is configured via `ConsumerConfigConstants.WATERMARK_LOOKAHEAD_MILLIS`. Smaller values reduce
the skew but also the throughput. Larger values will allow the subtask to proceed further before waiting for the global
watermark to advance.

Another variable in the throughput equation is how frequently the watermark is propagated by the tracker.
The interval can be configured via `ConsumerConfigConstants.WATERMARK_SYNC_MILLIS`.
Smaller values reduce emitter waits and come at the cost of increased communication with the job manager.

Since records accumulate in the queues when skew occurs, increased memory consumption needs to be expected.
How much depends on the average record size. With larger sizes, it may be necessary to adjust the emitter queue capacity via
`ConsumerConfigConstants.WATERMARK_SYNC_QUEUE_CAPACITY`.

### Threading Model

The Flink Kinesis Consumer uses multiple threads for shard discovery and data consumption.

For shard discovery, each parallel consumer subtask will have a single thread that constantly queries Kinesis for shard
information even if the subtask initially did not have shards to read from when the consumer was started. In other words, if
the consumer is run with a parallelism of 10, there will be a total of 10 threads constantly querying Kinesis regardless
of the total amount of shards in the subscribed streams.

For data consumption, a single thread will be created to consume each discovered shard. Threads will terminate when the
shard it is responsible of consuming is closed as a result of stream resharding. In other words, there will always be
one thread per open shard.

### Internally Used Kinesis APIs

The Flink Kinesis Consumer uses the [AWS Java SDK](http://aws.amazon.com/sdk-for-java/) internally to call Kinesis APIs
for shard discovery and data consumption. Due to Amazon's [service limits for Kinesis Streams](http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
on the APIs, the consumer will be competing with other non-Flink consuming applications that the user may be running.
Below is a list of APIs called by the consumer with description of how the consumer uses the API, as well as information
on how to deal with any errors or warnings that the Flink Kinesis Consumer may have due to these service limits.

- *[ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html)*: this is constantly called
by a single thread in each parallel consumer subtask to discover any new shards as a result of stream resharding. By default,
the consumer performs the shard discovery at an interval of 10 seconds, and will retry indefinitely until it gets a result
from Kinesis. If this interferes with other non-Flink consuming applications, users can slow down the consumer of
calling this API by setting a value for `ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS` in the supplied
configuration properties. This sets the discovery interval to a different value. Note that this setting directly impacts
the maximum delay of discovering a new shard and starting to consume it, as shards will not be discovered during the interval.

- *[GetShardIterator](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)*: this is called
only once when per shard consuming threads are started, and will retry if Kinesis complains that the transaction limit for the
API has exceeded, up to a default of 3 attempts. Note that since the rate limit for this API is per shard (not per stream),
the consumer itself should not exceed the limit. Usually, if this happens, users can either try to slow down any other
non-Flink consuming applications of calling this API, or modify the retry behaviour of this API call in the consumer by
setting keys prefixed by `ConsumerConfigConstants.SHARD_GETITERATOR_*` in the supplied configuration properties.

- *[GetRecords](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html)*: this is constantly called
by per shard consuming threads to fetch records from Kinesis. When a shard has multiple concurrent consumers (when there
are any other non-Flink consuming applications running), the per shard rate limit may be exceeded. By default, on each call
of this API, the consumer will retry if Kinesis complains that the data size / transaction limit for the API has exceeded,
up to a default of 3 attempts. Users can either try to slow down other non-Flink consuming applications, or adjust the throughput
of the consumer by setting the `ConsumerConfigConstants.SHARD_GETRECORDS_MAX` and
`ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS` keys in the supplied configuration properties. Setting the former
adjusts the maximum number of records each consuming thread tries to fetch from shards on each call (default is 10,000), while
the latter modifies the sleep interval between each fetch (default is 200). The retry behaviour of the
consumer when calling this API can also be modified by using the other keys prefixed by `ConsumerConfigConstants.SHARD_GETRECORDS_*`.

## Kinesis Producer

The `FlinkKinesisProducer` uses [Kinesis Producer Library (KPL)](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html) to put data from a Flink stream into a Kinesis stream.

Note that the producer is not participating in Flink's checkpointing and doesn't provide exactly-once processing guarantees. Also, the Kinesis producer does not guarantee that records are written in order to the shards (See [here](https://github.com/awslabs/amazon-kinesis-producer/issues/23) and [here](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#API_PutRecord_RequestSyntax) for more details).

In case of a failure or a resharding, data will be written again to Kinesis, leading to duplicates. This behavior is usually called "at-least-once" semantics.

To put data into a Kinesis stream, make sure the stream is marked as "ACTIVE" in the AWS dashboard.

For the monitoring to work, the user accessing the stream needs access to the CloudWatch service.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties producerConfig = new Properties();
// Required configs
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
// Optional configs
producerConfig.put("AggregationMaxCount", "4294967295");
producerConfig.put("CollectionMaxCount", "1000");
producerConfig.put("RecordTtl", "30000");
producerConfig.put("RequestTimeout", "6000");
producerConfig.put("ThreadPoolSize", "15");

// Disable Aggregation if it's not supported by a consumer
// producerConfig.put("AggregationEnabled", "false");
// Switch KinesisProducer's threading model
// producerConfig.put("ThreadingModel", "PER_REQUEST");

FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
kinesis.setFailOnError(true);
kinesis.setDefaultStream("kinesis_stream_name");
kinesis.setDefaultPartition("0");

DataStream<String> simpleStringStream = ...;
simpleStringStream.addSink(kinesis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val producerConfig = new Properties()
// Required configs
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
// Optional KPL configs
producerConfig.put("AggregationMaxCount", "4294967295")
producerConfig.put("CollectionMaxCount", "1000")
producerConfig.put("RecordTtl", "30000")
producerConfig.put("RequestTimeout", "6000")
producerConfig.put("ThreadPoolSize", "15")

// Disable Aggregation if it's not supported by a consumer
// producerConfig.put("AggregationEnabled", "false")
// Switch KinesisProducer's threading model
// producerConfig.put("ThreadingModel", "PER_REQUEST")

val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, producerConfig)
kinesis.setFailOnError(true)
kinesis.setDefaultStream("kinesis_stream_name")
kinesis.setDefaultPartition("0")

val simpleStringStream = ...
simpleStringStream.addSink(kinesis)
{% endhighlight %}
</div>
</div>

The above is a simple example of using the producer. To initialize `FlinkKinesisProducer`, users are required to pass in `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` via a `java.util.Properties` instance. Users can also pass in KPL's configurations as optional parameters to customize the KPL underlying `FlinkKinesisProducer`. The full list of KPL configs and explanations can be found [here](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties). The example demonstrates producing a single Kinesis stream in the AWS region "us-east-1".

If users don't specify any KPL configs and values, `FlinkKinesisProducer` will use default config values of KPL, except `RateLimit`. `RateLimit` limits the maximum allowed put rate for a shard, as a percentage of the backend limits. KPL's default value is 150 but it makes KPL throw `RateLimitExceededException` too frequently and breaks Flink sink as a result. Thus `FlinkKinesisProducer` overrides KPL's default value to 100.

Instead of a `SerializationSchema`, it also supports a `KinesisSerializationSchema`. The `KinesisSerializationSchema` allows to send the data to multiple streams. This is
done using the `KinesisSerializationSchema.getTargetStream(T element)` method. Returning `null` there will instruct the producer to write the element to the default stream.
Otherwise, the returned stream name is used.

### Threading Model

Since Flink 1.4.0, `FlinkKinesisProducer` switches its default underlying KPL from a one-thread-per-request mode to a thread-pool mode. KPL in thread-pool mode uses a queue and thread pool to execute requests to Kinesis. This limits the number of threads that KPL's native process may create, and therefore greatly lowers CPU utilization and improves efficiency. **Thus, We highly recommend Flink users use thread-pool model.** The default thread pool size is `10`. Users can set the pool size in `java.util.Properties` instance with key `ThreadPoolSize`, as shown in the above example.

Users can still switch back to one-thread-per-request mode by setting a key-value pair of `ThreadingModel` and `PER_REQUEST` in `java.util.Properties`, as shown in the code commented out in above example.

### Backpressure

By default, `FlinkKinesisProducer` does not backpressure. Instead, records that
cannot be sent because of the rate restriction of 1 MB per second per shard are
buffered in an unbounded queue and dropped when their `RecordTtl` expires.

To avoid data loss, you can enable backpressuring by restricting the size of the
internal queue:

```
// 200 Bytes per record, 1 shard
kinesis.setQueueLimit(500);
```

The value for `queueLimit` depends on the expected record size. To choose a good
value, consider that Kinesis is rate-limited to 1MB per second per shard. If
less than one second's worth of records is buffered, then the queue may not be
able to operate at full capacity. With the default `RecordMaxBufferedTime` of
100ms, a queue size of 100kB per shard should be sufficient. The `queueLimit`
can then be computed via

```
queue limit = (number of shards * queue size per shard) / record size
```

E.g. for 200Bytes per record and 8 shards, a queue limit of 4000 is a good
starting point. If the queue size limits throughput (below 1MB per second per
shard), try increasing the queue limit slightly.


## Using Custom Kinesis Endpoints

It is sometimes desirable to have Flink operate as a consumer or producer against a Kinesis VPC endpoint or a non-AWS
Kinesis endpoint such as [Kinesalite](https://github.com/mhart/kinesalite); this is especially useful when performing
functional testing of a Flink application. The AWS endpoint that would normally be inferred by the AWS region set in the
Flink configuration must be overridden via a configuration property.

To override the AWS endpoint, set the `AWSConfigConstants.AWS_ENDPOINT` and `AWSConfigConstants.AWS_REGION` properties. The region will be used to sign the endpoint URL.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties producerConfig = new Properties();
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val producerConfig = new Properties()
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567")
{% endhighlight %}
</div>
</div>

{% top %}
