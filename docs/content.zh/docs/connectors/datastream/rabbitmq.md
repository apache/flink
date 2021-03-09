---
title: RabbitMQ
weight: 7
type: docs
aliases:
  - /zh/dev/connectors/rabbitmq.html
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

# RabbitMQ 连接器

## RabbitMQ 连接器的许可证

Flink 的 RabbitMQ 连接器依赖了 "RabbitMQ AMQP Java Client"，它基于三种协议下发行：Mozilla Public License 1.1 ("MPL")、GNU General Public License version 2 ("GPL") 和 Apache License version 2 ("ASL")。

Flink 自身既没有复用 "RabbitMQ AMQP Java Client" 的代码，也没有将 "RabbitMQ AMQP Java Client" 打二进制包。

如果用户发布的内容是基于 Flink 的 RabbitMQ 连接器的（进而重新发布了 "RabbitMQ AMQP Java Client" ），那么一定要注意这可能会受到 Mozilla Public License 1.1 ("MPL")、GNU General Public License version 2 ("GPL")、Apache License version 2 ("ASL") 协议的限制.

## RabbitMQ 连接器

这个连接器可以访问 [RabbitMQ](http://www.rabbitmq.com/) 的数据流。使用这个连接器，需要在工程里添加下面的依赖：

{{< artifact flink-connector-rabbitmq withScalaVersion >}}

注意连接器现在没有包含在二进制发行版中。集群执行的相关信息请参考 [这里]({{< ref "docs/dev/datastream/project-configuration" >}}).

### 安装 RabbitMQ
安装 RabbitMQ 请参考 [RabbitMQ 下载页面](http://www.rabbitmq.com/download.html)。安装完成之后，服务会自动拉起，应用程序就可以尝试连接到 RabbitMQ 了。

### RabbitMQ Source

`RMQSource` 负责从 RabbitMQ 中消费数据，可以配置三种不同级别的保证：

1. **精确一次**: 保证精确一次需要以下条件 -
 - *开启 checkpointing*: 开启 checkpointing 之后，消息在 checkpoints 
 完成之后才会被确认（然后从 RabbitMQ 队列中删除）.
 - *使用关联标识（Correlation ids）*: 关联标识是 RabbitMQ 的一个特性，消息写入 RabbitMQ 时在消息属性中设置。
 从 checkpoint 恢复时有些消息可能会被重复处理，source 可以利用关联标识对消息进行去重。
 - *非并发 source*: 为了保证精确一次的数据投递，source 必须是非并发的（并行度设置为1）。
  这主要是由于 RabbitMQ 分发数据时是从单队列向多个消费者投递消息的。

2. **至少一次**:  在 checkpointing 开启的条件下，如果没有使用关联标识或者 source 是并发的，
那么 source 就只能提供至少一次的保证。

3. **无任何保证**: 如果没有开启 checkpointing，source 就不能提供任何的数据投递保证。
使用这种设置时，source 一旦接收到并处理消息，消息就会被自动确认。

下面是一个保证 exactly-once 的 RabbitMQ source 示例。 注释部分展示了更加宽松的保证应该如何配置。

{{< tabs "62892e62-eb37-4fed-aa0f-64690135b4d6" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

#### 服务质量 (QoS) / 消费者预取(Consumer Prefetch)

RabbitMQ Source 通过 `RMQConnectionConfig` 类提供了一种简单的方式，来设置 source channel 上的 `basicQos`（见下方示例）。要注意的是这里的 prefetch count 是对单个 channel 设置的，并且由于每个并发的 source 都持有一个 connection/channel，因此这个值实际上会乘以 source 的并行度，来表示同一时间可以向这个 job 总共发送多少条未确认的消息。如果需要更复杂的配置，可以通过重写 `RMQSource#setupChannel(Connection)` 方法来实现手动配置。

{{< tabs "a899561b-bd98-4b65-a3a3-b81ee1f1f677" >}}
{{< tab "Java" >}}
```java
final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
    .setPrefetchCount(30_000)
    ...
    .build();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val connectionConfig = new RMQConnectionConfig.Builder()
    .setPrefetchCount(30000)
    ...
    .build
```
{{< /tab >}}
{{< /tabs >}}

RabbitMQ Source 默认情况下是不设置 prefetch count 的，这意味着 RabbitMQ 服务器将会无限制地向 source 发送消息。因此在生产环境中，最好要设置它。当消费海量数据的队列并且启用 checkpointing 时，消息只有在做完 checkpoint 后才会被确认，因此也许需要对 prefetch count 做一些调整来减少不必要的循环。

更多关于 QoS 以及 prefetch 相关的内容可以参考 [这里](https://www.rabbitmq.com/confirms.html#channel-qos-prefetch).
更多关于在 AMQP 0-9-1 中可选的选项可以参考 [这里](https://www.rabbitmq.com/consumer-prefetch.html).

### RabbitMQ Sink
该连接器提供了一个 `RMQSink` 类，用来向 RabbitMQ 队列发送数据。下面是设置 RabbitMQ sink 的代码示例：

{{< tabs "b8966b60-db25-4853-8382-751bbe1a89c7" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

更多关于 RabbitMQ 的信息请参考 [这里](http://www.rabbitmq.com/).

{{< top >}}
