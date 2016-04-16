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

The Kinesis connector allows to produce data into an [Amazon AWS Kinesis Stream](http://aws.amazon.com/kinesis/streams/). 

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

Note that the streaming connectors are not part of the binary distribution. 
See linking with them for cluster execution [here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Usage of Consumer

The `FlinkKinesisConsumer` can be used to pull data from multiple Kinesis streams within the same AWS region in parallel.
It participates with Flink's distributed snapshot checkpointing and provides exactly-once processing guarantees. Note
that the current version can not handle resharding of Kinesis streams. When Kinesis streams are resharded, the consumer
will fail and the Flink streaming job must be resubmitted.

Before consuming data from Kinesis streams, make sure that all streams are created with the status "ACTIVE" in the AWS dashboard.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties kinesisConsumerConfig = new Properties();
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID,
    "aws_access_key_id_here");
kinesisConsumerConfig.put(
    KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY,
    "aws_secret_key_here");
kinesisConsumerConfig.put(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "LATEST");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getEnvironment();

DataStream<String> kinesisRecords = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), kinesisConsumerConfig))
{% endhighlight %}
</div>
</div>

The above is a simple example of using the consumer. Configuration for the consumer is supplied with a `java.util.Properties`
instance, with which the configuration setting keys used can be found in `KinesisConfigConstants`. The example
demonstrates consuming a single Kinesis stream in the AWS region "us-east-1". The AWS credentials is supplied using the basic method in which
the AWS access key ID and secret key are directly supplied in the configuration (other options are setting
`KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE` to `ENV_VAR`, `SYS_PROP`, and `PROFILE`). Also, data is being consumed
from the newest position in the Kinesis stream (the other option will be setting `KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE`
to `TRIM_HORIZON`, which lets the consumer start reading the Kinesis stream from the earliest record possible).

#### Usage of Producer

The `FlinkKinesisProducer` is used for sending data from a Flink stream into a Kinesis stream. Note that the producer is not participating in 
Flink's checkpointing and doesn't provide exactly-once processing guarantees. In case of a failure, data will be written again
to Kinesis, leading to duplicates. This behavior is usually called "at-least-once" semantics.

To produce data into a Kinesis stream, make sure that you have a stream created with the status "ACTIVE" in the AWS dashboard.

For the monitoring to work, the user accessing the stream needs access to the Cloud watch service.

The Kinesis producer expects at least the following arguments: `FlinkKinesisProducer(String region, String accessKey, String secretKey, final SerializationSchema<OUT> schema)`.


Instead of a `SerializationSchema`, it also supports a `KinesisSerializationSchema`. The `KinesisSerializationSchema` allows to send the data to multiple streams. This is 
done using the `KinesisSerializationSchema.getTargetStream(T element)` method. Returning `null` there will instruct the producer to write the element to the default stream.
Otherwise, the returned stream name is used.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>("<aws region>",
        "<accessKey>", "<secretKey>", new SimpleStringSchema());

kinesis.setFailOnError(true);
kinesis.setDefaultStream("test-flink");
kinesis.setDefaultPartition("0");

DataStream<String> simpleStringStream = ...;
simpleStringStream.addSink(kinesis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val kinesis = new FlinkKinesisProducer[String]("<aws region>",
        "<accessKey>", "<secretKey>", new SimpleStringSchema());

kinesis.setFailOnError(true);
kinesis.setDefaultStream("test-flink");
kinesis.setDefaultPartition("0");

val simpleStringStream: DataStream[String] = ...;
simpleStringStream.addSink(kinesis);
{% endhighlight %}
</div>
</div>


		
		
