---
title: "RabbitMQ Connector"
nav-title: RabbitMQ
nav-parent_id: connectors
nav-pos: 6
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

## License of the RabbitMQ Connector

Flink's RabbitMQ connector defines a Maven dependency on the
"RabbitMQ AMQP Java Client", is triple-licensed under the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

Flink itself neither reuses source code from the "RabbitMQ AMQP Java Client"
nor packages binaries from the "RabbitMQ AMQP Java Client".

Users that create and publish derivative work based on Flink's
RabbitMQ connector (thereby re-distributing the "RabbitMQ AMQP Java Client")
must be aware that this may be subject to conditions declared in the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

## RabbitMQ Connector

This connector provides access to data streams from [RabbitMQ](http://www.rabbitmq.com/). To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-rabbitmq{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here]({{site.baseurl}}/dev/project-configuration.html).

### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts, and the application connecting to RabbitMQ can be launched.

### RabbitMQ Source

This connector provides a `RMQSource` class to consume messages from a RabbitMQ
queue. This source provides three different levels of guarantees, depending
on how it is configured with Flink:

1. **Exactly-once**: In order to achieve exactly-once guarantees with the
RabbitMQ source, the following is required -
 - *Enable checkpointing*: With checkpointing enabled, messages are only
 acknowledged (hence, removed from the RabbitMQ queue) when checkpoints
 are completed.
 - *Use correlation ids*: Correlation ids are a RabbitMQ application feature.
 You have to set it in the message properties when injecting messages into RabbitMQ.
 The correlation id is used by the source to deduplicate any messages that
 have been reprocessed when restoring from a checkpoint.
 - *Non-parallel source*: The source must be non-parallel (parallelism set
 to 1) in order to achieve exactly-once. This limitation is mainly due to
 RabbitMQ's approach to dispatching messages from a single queue to multiple
 consumers.


2. **At-least-once**: When checkpointing is enabled, but correlation ids
are not used or the source is parallel, the source only provides at-least-once
guarantees.

3. **No guarantee**: If checkpointing isn't enabled, the source does not
have any strong delivery guarantees. Under this setting, instead of
collaborating with Flink's checkpointing, messages will be automatically
acknowledged once the source receives and processes them.

Below is a code example for setting up an exactly-once RabbitMQ source.
Inline comments explain which parts of the configuration can be ignored
for more relaxed guarantees.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// checkpointing is required for exactly-once or at-least-once guarantees
env.enableCheckpointing(...);

final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
    .setHost("localhost")
    .setPort(5000)
    ...
    .build();
    
final DataStream<String> stream = env
    .addSource(new RMQSource<String>(
        connectionConfig,            // config for the RabbitMQ connection
        "queueName",                 // name of the RabbitMQ queue to consume
        true,                        // use correlation ids; can be false if only at-least-once is required
        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
    .setParallelism(1);              // non-parallel source is only required for exactly-once
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
// checkpointing is required for exactly-once or at-least-once guarantees
env.enableCheckpointing(...)

val connectionConfig = new RMQConnectionConfig.Builder()
    .setHost("localhost")
    .setPort(5000)
    ...
    .build
    
val stream = env
    .addSource(new RMQSource[String](
        connectionConfig,            // config for the RabbitMQ connection
        "queueName",                 // name of the RabbitMQ queue to consume
        true,                        // use correlation ids; can be false if only at-least-once is required
        new SimpleStringSchema))     // deserialization schema to turn messages into Java objects
    .setParallelism(1)               // non-parallel source is only required for exactly-once
{% endhighlight %}
</div>
</div>

#### Quality of Service (QoS) / Consumer Prefetch

The RabbitMQ Source provides a simple way to set the `basicQos` on the source's channel through the `RMQConnectionConfig`.
Since there is one connection/ channel per-parallel source, this prefetch count will effectively be multiplied by the
source's parallelism for how many total unacknowledged messages can be sent to the job at one time.
If more complex configuration is required, `RMQSource#setupChannel(Connection)` can be overridden and manually configured.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
    .setPrefetchCount(30_000)
    ...
    .build();

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val connectionConfig = new RMQConnectionConfig.Builder()
    .setPrefetchCount(30000)
    ...
    .build
{% endhighlight %}
</div>
</div>

The prefetch count is unset by default, meaning the RabbitMQ server will send unlimited messages. In production, it
is best to set this value. For high volume queues and checkpointing enabled, some tuning may be required to reduce
wasted cycles, as messages are only acknowledged on checkpoints if enabled.

More about QoS and prefetch can be found [here](https://www.rabbitmq.com/confirms.html#channel-qos-prefetch)
and more about the options available in AMQP 0-9-1 [here](https://www.rabbitmq.com/consumer-prefetch.html).

### RabbitMQ Sink
This connector provides a `RMQSink` class for sending messages to a RabbitMQ
queue. Below is a code example for setting up a RabbitMQ sink.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final DataStream<String> stream = ...

final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
    .setHost("localhost")
    .setPort(5000)
    ...
    .build();
    
stream.addSink(new RMQSink<String>(
    connectionConfig,            // config for the RabbitMQ connection
    "queueName",                 // name of the RabbitMQ queue to send messages to
    new SimpleStringSchema()));  // serialization schema to turn Java objects to messages
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[String] = ...

val connectionConfig = new RMQConnectionConfig.Builder()
    .setHost("localhost")
    .setPort(5000)
    ...
    .build
    
stream.addSink(new RMQSink[String](
    connectionConfig,         // config for the RabbitMQ connection
    "queueName",              // name of the RabbitMQ queue to send messages to
    new SimpleStringSchema))  // serialization schema to turn Java objects to messages
{% endhighlight %}
</div>
</div>

More about RabbitMQ can be found [here](http://www.rabbitmq.com/).

{% top %}
