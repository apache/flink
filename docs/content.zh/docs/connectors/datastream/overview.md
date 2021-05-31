---
title: 概览
weight: 1
type: docs
aliases:
  - /zh/dev/connectors/
  - /zh/apis/connectors.html
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

# DataStream Connectors

## 预定义的 Source 和 Sink

一些比较基本的 Source 和 Sink 已经内置在 Flink 里。
[预定义 data sources]({{< ref "docs/dev/datastream/overview" >}}#data-sources) 支持从文件、目录、socket，以及 collections 和 iterators 中读取数据。
[预定义 data sinks]({{< ref "docs/dev/datastream/overview" >}}#data-sinks) 支持把数据写入文件、标准输出（stdout）、标准错误输出（stderr）和 socket。

## 附带的连接器

连接器可以和多种多样的第三方系统进行交互。目前支持以下系统:

 * [Apache Kafka]({{< ref "docs/connectors/datastream/kafka" >}}) (source/sink)
 * [Apache Cassandra]({{< ref "docs/connectors/datastream/cassandra" >}}) (sink)
 * [Amazon Kinesis Streams]({{< ref "docs/connectors/datastream/kinesis" >}}) (source/sink)
 * [Elasticsearch]({{< ref "docs/connectors/datastream/elasticsearch" >}}) (sink)
 * [FileSystem（包括 Hadoop ） - 仅支持流]({{< ref "docs/connectors/datastream/streamfile_sink" >}}) (sink)
 * [FileSystem（包括 Hadoop ） - 流批统一]({{< ref "docs/connectors/datastream/file_sink" >}}) (sink)
 * [RabbitMQ]({{< ref "docs/connectors/datastream/rabbitmq" >}}) (source/sink)
 * [Apache NiFi]({{< ref "docs/connectors/datastream/nifi" >}}) (source/sink)
 * [Twitter Streaming API]({{< ref "docs/connectors/datastream/twitter" >}}) (source)
 * [Google PubSub]({{< ref "docs/connectors/datastream/pubsub" >}}) (source/sink)
 * [JDBC]({{< ref "docs/connectors/datastream/jdbc" >}}) (sink)

请记住，在使用一种连接器时，通常需要额外的第三方组件，比如：数据存储服务器或者消息队列。
要注意这些列举的连接器是 Flink 工程的一部分，包含在发布的源码中，但是不包含在二进制发行版中。
更多说明可以参考对应的子部分。

## Apache Bahir 中的连接器

Flink 还有些一些额外的连接器通过 [Apache Bahir](https://bahir.apache.org/) 发布, 包括:

 * [Apache ActiveMQ](https://bahir.apache.org/docs/flink/current/flink-streaming-activemq/) (source/sink)
 * [Apache Flume](https://bahir.apache.org/docs/flink/current/flink-streaming-flume/) (sink)
 * [Redis](https://bahir.apache.org/docs/flink/current/flink-streaming-redis/) (sink)
 * [Akka](https://bahir.apache.org/docs/flink/current/flink-streaming-akka/) (sink)
 * [Netty](https://bahir.apache.org/docs/flink/current/flink-streaming-netty/) (source)

## 连接Fink的其他方法

### 异步 I/O

使用connector并不是唯一可以使数据进入或者流出Flink的方式。
一种常见的模式是从外部数据库或者 Web 服务查询数据得到初始数据流，然后通过 `Map` 或者 `FlatMap` 对初始数据流进行丰富和增强。
Flink 提供了[异步 I/O]({{< ref "docs/dev/datastream/operators/asyncio" >}}) API 来让这个过程更加简单、高效和稳定。

### 可查询状态

当 Flink 应用程序需要向外部存储推送大量数据时会导致 I/O 瓶颈问题出现。在这种场景下，如果对数据的读操作远少于写操作，那么让外部应用从 Flink 拉取所需的数据会是一种更好的方式。
[可查询状态]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) 接口可以实现这个功能，该接口允许被 Flink 托管的状态可以被按需查询。

{{< top >}}
