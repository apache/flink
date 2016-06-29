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



Note that the streaming connectors are not part of the binary distribution. 
See how to link with them for cluster execution [here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

### Using the Amazon Kinesis Streams Service
Follow the instructions from the [Amazon Kinesis Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html)
to setup Kinesis streams. Make sure to create the appropriate IAM policy and user to read / write to the Kinesis streams.

### Kinesis Consumer

The `FlinkKinesisConsumer` can be used to pull data from multiple Kinesis streams within the same AWS region in parallel.
It participates in Flink's distributed snapshot checkpointing and provides exactly-once user-defined state update guarantees. Note
that the current version can not handle resharding of Kinesis streams. When Kinesis streams are resharded, the consumer
will fail and the Flink streaming job must be resubmitted.

Before consuming data from Kinesis streams, make sure that all streams are created with the status "ACTIVE" in the AWS dashboard.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties kinesisConsumerConfig = new Properties();
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID,
    "aws_access_key_id_here");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY,
    "aws_secret_key_here");
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "LATEST");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), kinesisConsumerConfig))
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val kinesisConsumerConfig = new Properties();
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID,
    "aws_access_key_id_here");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY,
    "aws_secret_key_here");
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "LATEST");

val env = StreamExecutionEnvironment.getEnvironment

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, kinesisConsumerConfig))
{% endhighlight %}
</div>
</div>

The above is a simple example of using the consumer. Configuration for the consumer is supplied with a `java.util.Properties`
instance, the setting keys for which are enumerated in `KinesisConfigConstants`. The example
demonstrates consuming a single Kinesis stream in the AWS region "us-east-1". The AWS credentials are supplied using the basic method in which
the AWS access key ID and secret key are directly supplied in the configuration (other options are setting
`KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE` to `ENV_VAR`, `SYS_PROP`, and `PROFILE`). Also, data is being consumed
from the newest position in the Kinesis stream (the other option will be setting `KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE`
to `TRIM_HORIZON`, which lets the consumer start reading the Kinesis stream from the earliest record possible).

Other optional configuration keys can be found in `KinesisConfigConstants`.

### Kinesis Producer

The `FlinkKinesisProducer` is used for putting data from a Flink stream into a Kinesis stream. Note that the producer is not participating in
Flink's checkpointing and doesn't provide exactly-once processing guarantees. In case of a failure, data will be written again
to Kinesis, leading to duplicates. This behavior is usually called "at-least-once" semantics.

To put data into a Kinesis stream, make sure the stream is marked as "ACTIVE" in the AWS dashboard.

For the monitoring to work, the user accessing the stream needs access to the Cloud watch service.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties kinesisProducerConfig = new Properties();
kinesisProducerConfig.put(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
kinesisProducerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID,
    "aws_access_key_id_here");
kinesisProducerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY,
    "aws_secret_key_here");

FlinkKinesisProducer<String> kinesis =
    new FlinkKinesisProducer<>(new SimpleStringSchema(), kinesisProducerConfig);
kinesis.setFailOnError(true);
kinesis.setDefaultStream("kinesis_stream_name");
kinesis.setDefaultPartition("0");

DataStream<String> simpleStringStream = ...;
simpleStringStream.addSink(kinesis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val kinesisProducerConfig = new Properties();
kinesisProducerConfig.put(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
kinesisProducerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID,
    "aws_access_key_id_here");
kinesisProducerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY,
    "aws_secret_key_here");

val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, kinesisProducerConfig);
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

Other optional configuration keys can be found in `KinesisConfigConstants`.
		
		
