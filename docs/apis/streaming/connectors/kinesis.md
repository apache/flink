---
title: "Amazon AWS Kinesis Streams Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 5
sub-nav-title: Amazon Kinesis Streams
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

The Kinesis connector provides access to [Amazon AWS Kinesis Streams](http://aws.amazon.com/kinesis/streams/). 

To use the connector, add the following Maven dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kinesis{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

**The `flink-connector-kinesis{{ site.scala_version_suffix }}` has a dependency on code licensed under the [Amazon Software License](https://aws.amazon.com/asl/) (ASL).
Linking to the flink-connector-kinesis will include ASL licensed code into your application.**

The `flink-connector-kinesis{{ site.scala_version_suffix }}` artifact is not deployed to Maven central as part of
Flink releases because of the licensing issue. Therefore, you need to build the connector yourself from the source.

Download the Flink source or check it out from the git repository. Then, use the following Maven command to build the module:
{% highlight bash %}
mvn clean install -Pinclude-kinesis -DskipTests
{% endhighlight %}


The streaming connectors are not part of the binary distribution. See how to link with them for cluster 
execution [here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

### Using the Amazon Kinesis Streams Service
Follow the instructions from the [Amazon Kinesis Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html)
to setup Kinesis streams. Make sure to create the appropriate IAM policy and user to read / write to the Kinesis streams.

### Kinesis Consumer

The `FlinkKinesisConsumer` is an exactly-once parallel streaming data source that subscribes to multiple AWS Kinesis
streams within the same AWS service region, and can handle resharding of streams. Each subtask of the consumer is
responsible for fetching data records from multiple Kinesis shards. The number of shards fetched by each subtask will
change as shards are closed and created by Kinesis.

Before consuming data from Kinesis streams, make sure that all streams are created with the status "ACTIVE" in the AWS dashboard.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties consumerConfig = new Properties();
consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
consumerConfig.put(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val consumerConfig = new Properties();
consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
consumerConfig.put(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

val env = StreamExecutionEnvironment.getEnvironment

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
{% endhighlight %}
</div>
</div>

The above is a simple example of using the consumer. Configuration for the consumer is supplied with a `java.util.Properties`
instance, the configuration keys for which can be found in `ConsumerConfigConstants`. The example
demonstrates consuming a single Kinesis stream in the AWS region "us-east-1". The AWS credentials are supplied using the basic method in which
the AWS access key ID and secret access key are directly supplied in the configuration (other options are setting
`ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER` to `ENV_VAR`, `SYS_PROP`, and `PROFILE`). Also, data is being consumed
from the newest position in the Kinesis stream (the other option will be setting `ConsumerConfigConstants.STREAM_INITIAL_POSITION`
to `TRIM_HORIZON`, which lets the consumer start reading the Kinesis stream from the earliest record possible).

Other optional configuration keys for the consumer can be found in `ConsumerConfigConstants`.

#### Fault Tolerance for Exactly-Once User-Defined State Update Semantics

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

#### Event Time for Consumed Records

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

If streaming topologies choose to use the [event time notion]({{site.baseurl}}/apis/streaming/event_time.html) for record
timestamps, an *approximate arrival timestamp* will be used by default. This timestamp is attached to records by Kinesis once they
were successfully received and stored by streams. Note that this timestamp is typically referred to as a Kinesis server-side
timestamp, and there are no guarantees about the accuracy or order correctness (i.e., the timestamps may not always be
ascending).

Users can choose to override this default with a custom timestamp, as described [here]({{ site.baseurl }}/apis/streaming/event_timestamps_watermarks.html),
or use one from the [predefined ones]({{ site.baseurl }}/apis/streaming/event_timestamp_extractors.html). After doing so,
it can be passed to the consumer in the following way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), kinesisConsumerConfig));
kinesis = kinesis.assignTimestampsAndWatermarks(new CustomTimestampAssigner());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, kinesisConsumerConfig))
kinesis = kinesis.assignTimestampsAndWatermarks(new CustomTimestampAssigner)
{% endhighlight %}
</div>
</div>

#### Threading Model

The Flink Kinesis Consumer uses multiple threads for shard discovery and data consumption.

For shard discovery, each parallel consumer subtask will have a single thread that constantly queries Kinesis for shard
information even if the subtask initially did not have shards to read from when the consumer was started. In other words, if
the consumer is run with a parallelism of 10, there will be a total of 10 threads constantly querying Kinesis regardless
of the total amount of shards in the subscribed streams.

For data consumption, a single thread will be created to consume each discovered shard. Threads will terminate when the
shard it is responsible of consuming is closed as a result of stream resharding. In other words, there will always be
one thread per open shard.

#### Internally Used Kinesis APIs

The Flink Kinesis Consumer uses the [AWS Java SDK](http://aws.amazon.com/sdk-for-java/) internally to call Kinesis APIs
for shard discovery and data consumption. Due to Amazon's [service limits for Kinesis Streams](http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
on the APIs, the consumer will be competing with other non-Flink consuming applications that the user may be running.
Below is a list of APIs called by the consumer with description of how the consumer uses the API, as well as information
on how to deal with any errors or warnings that the Flink Kinesis Consumer may have due to these service limits.

- *[DescribeStream](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html)*: this is constantly called
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
adjusts the maximum number of records each consuming thread tries to fetch from shards on each call (default is 100), while
the latter modifies the sleep interval between each fetch (there will be no sleep by default). The retry behaviour of the
consumer when calling this API can also be modified by using the other keys prefixed by `ConsumerConfigConstants.SHARD_GETRECORDS_*`.

### Kinesis Producer

The `FlinkKinesisProducer` is used for putting data from a Flink stream into a Kinesis stream. Note that the producer is not participating in
Flink's checkpointing and doesn't provide exactly-once processing guarantees. 
Also, the Kinesis producer does not guarantee that records are written in order to the shards (See [here](https://github.com/awslabs/amazon-kinesis-producer/issues/23) and [here](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#API_PutRecord_RequestSyntax) for more details). 

In case of a failure or a resharding, data will be written again to Kinesis, leading to duplicates. This behavior is usually called "at-least-once" semantics.

To put data into a Kinesis stream, make sure the stream is marked as "ACTIVE" in the AWS dashboard.

For the monitoring to work, the user accessing the stream needs access to the Cloud watch service.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties producerConfig = new Properties();
producerConfig.put(ProducerConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

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
val producerConfig = new Properties();
producerConfig.put(ProducerConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, producerConfig);
kinesis.setFailOnError(true);
kinesis.setDefaultStream("kinesis_stream_name");
kinesis.setDefaultPartition("0");

val simpleStringStream = ...;
simpleStringStream.addSink(kinesis);
{% endhighlight %}
</div>
</div>

The above is a simple example of using the producer. Configuration for the producer with the mandatory configuration values is supplied with a `java.util.Properties`
instance as described above for the consumer. The example demonstrates producing a single Kinesis stream in the AWS region "us-east-1".

Instead of a `SerializationSchema`, it also supports a `KinesisSerializationSchema`. The `KinesisSerializationSchema` allows to send the data to multiple streams. This is 
done using the `KinesisSerializationSchema.getTargetStream(T element)` method. Returning `null` there will instruct the producer to write the element to the default stream.
Otherwise, the returned stream name is used.

Other optional configuration keys for the producer can be found in `ProducerConfigConstants`.
		
		
### Using Non-AWS Kinesis Endpoints for Testing

It is sometimes desirable to have Flink operate as a consumer or producer against a non-AWS Kinesis endpoint such as
[Kinesalite](https://github.com/mhart/kinesalite); this is especially useful when performing functional testing of a Flink
application. The AWS endpoint that would normally be inferred by the AWS region set in the Flink configuration must be overridden via a configuration property.

To override the AWS endpoint, taking the producer for example, set the `ProducerConfigConstants.AWS_ENDPOINT` property in the
Flink configuration, in addition to the `ProducerConfigConstants.AWS_REGION` required by Flink. Although the region is
required, it will not be used to determine the AWS endpoint URL.

The following example shows how one might supply the `ProducerConfigConstants.AWS_ENDPOINT` configuration property:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties producerConfig = new Properties();
producerConfig.put(ProducerConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(ProducerConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val producerConfig = new Properties();
producerConfig.put(ProducerConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(ProducerConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
{% endhighlight %}
</div>
</div>
