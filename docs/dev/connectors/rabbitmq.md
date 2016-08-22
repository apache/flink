---
title: "RabbitMQ Connector"
nav-title: RabbitMQ
nav-parent_id: connectors
nav-pos: 7
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

This connector provides access to data streams from [RabbitMQ](http://www.rabbitmq.com/). To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-rabbitmq{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here]({{site.baseurl}}/dev/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts, and the application connecting to RabbitMQ can be launched.

#### RabbitMQ Source

A class which provides an interface for receiving data from RabbitMQ.

The followings have to be provided for the `RMQSource(…)` constructor in order:

- RMQConnectionConfig.
- queueName: The RabbitMQ queue name.
- usesCorrelationId: `true` when correlation ids should be used, `false` otherwise (default is `false`).
- deserializationSchema: Deserialization schema to turn messages into Java objects.

This source can be operated in three different modes:

1. Exactly-once (when checkpointed) with RabbitMQ transactions and messages with
    unique correlation IDs.
2. At-least-once (when checkpointed) with RabbitMQ transactions but no deduplication mechanism
    (correlation id is not set).
3. No strong delivery guarantees (without checkpointing) with RabbitMQ auto-commit mode.

Correlation ids are a RabbitMQ application feature. You have to set it in the message properties
when injecting messages into RabbitMQ. If you set `usesCorrelationId` to true and do not supply
unique correlation ids, the source will throw an exception (if the correlation id is null) or ignore
messages with non-unique correlation ids. If you set `usesCorrelationId` to false, then you don't
have to supply correlation ids.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
.setHost("localhost").setPort(5000).setUserName(..)
.setPassword(..).setVirtualHost("/").build();
DataStream<String> streamWithoutCorrelationIds = env
	.addSource(new RMQSource<String>(connectionConfig, "hello", new SimpleStringSchema()))
	.print

DataStream<String> streamWithCorrelationIds = env
	.addSource(new RMQSource<String>(connectionConfig, "hello", true, new SimpleStringSchema()))
	.print
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val connectionConfig = new RMQConnectionConfig.Builder()
.setHost("localhost").setPort(5000).setUserName(..)
.setPassword(..).setVirtualHost("/").build()
streamWithoutCorrelationIds = env
    .addSource(new RMQSource[String](connectionConfig, "hello", new SimpleStringSchema))
    .print

streamWithCorrelationIds = env
    .addSource(new RMQSource[String](connectionConfig, "hello", true, new SimpleStringSchema))
    .print
{% endhighlight %}
</div>
</div>

#### RabbitMQ Sink
A class providing an interface for sending data to RabbitMQ.

The followings have to be provided for the `RMQSink(…)` constructor in order:

1. RMQConnectionConfig
2. The queue name
3. Serialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
.setHost("localhost").setPort(5000).setUserName(..)
.setPassword(..).setVirtualHost("/").build();
stream.addSink(new RMQSink<String>(connectionConfig, "hello", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val connectionConfig = new RMQConnectionConfig.Builder()
.setHost("localhost").setPort(5000).setUserName(..)
.setPassword(..).setVirtualHost("/").build()
stream.addSink(new RMQSink[String](connectionConfig, "hello", new SimpleStringSchema))
{% endhighlight %}
</div>
</div>

More about RabbitMQ can be found [here](http://www.rabbitmq.com/).
