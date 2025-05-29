---
title: Overview
weight: 1
type: docs
aliases:
  - /dev/connectors/
  - /api/connectors.html
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

## Predefined Sources and Sinks

A few basic data sources and sinks are built into Flink and are always available.
The [predefined data sources]({{< ref "docs/dev/datastream/overview" >}}#data-sources) include reading from files, directories, and sockets, and
ingesting data from collections and iterators.
The [predefined data sinks]({{< ref "docs/dev/datastream/overview" >}}#data-sinks) support writing to files, to stdout and stderr, and to sockets.

## Flink Project Connectors

Connectors provide code for interfacing with various third-party systems. 
Currently these systems are supported as part of the Apache Flink project:

 * [Apache Kafka]({{< ref "docs/connectors/datastream/kafka" >}}) (source/sink)
 * [Apache Cassandra]({{< ref "docs/connectors/datastream/cassandra" >}}) (source/sink)
 * [Amazon DynamoDB]({{< ref "docs/connectors/datastream/dynamodb" >}}) (sink)
 * [Amazon Kinesis Data Streams]({{< ref "docs/connectors/datastream/kinesis" >}}) (source/sink)
 * [Amazon Kinesis Data Firehose]({{< ref "docs/connectors/datastream/firehose" >}}) (sink)
 * [DataGen]({{< ref "docs/connectors/datastream/datagen" >}}) (source)
 * [Elasticsearch]({{< ref "docs/connectors/datastream/elasticsearch" >}}) (sink)
 * [Opensearch]({{< ref "docs/connectors/datastream/opensearch" >}}) (sink)
 * [FileSystem]({{< ref "docs/connectors/datastream/filesystem" >}}) (source/sink)
 * [RabbitMQ]({{< ref "docs/connectors/datastream/rabbitmq" >}}) (source/sink)
 * [Google PubSub]({{< ref "docs/connectors/datastream/pubsub" >}}) (source/sink)
 * [Hybrid Source]({{< ref "docs/connectors/datastream/hybridsource" >}}) (source)
 * [Apache Pulsar]({{< ref "docs/connectors/datastream/pulsar" >}}) (source)
 * [JDBC]({{< ref "docs/connectors/datastream/jdbc" >}}) (sink)
 * [MongoDB]({{< ref "docs/connectors/datastream/mongodb" >}}) (source/sink)
 * [Prometheus]({{< ref "docs/connectors/datastream/prometheus" >}}) (sink)

Keep in mind that to use one of these connectors in an application, additional third party
components are usually required, e.g. servers for the data stores or message queues.
Note also that while the streaming connectors listed in this section are part of the
Flink project and are included in source releases, they are not included in the binary distributions. 
Further instructions can be found in the corresponding subsections.

## Connectors in Apache Bahir

Additional streaming connectors for Flink are being released through [Apache Bahir](https://bahir.apache.org/), including:

 * [Apache ActiveMQ](https://bahir.apache.org/docs/flink/current/flink-streaming-activemq/) (source/sink)
 * [Apache Flume](https://bahir.apache.org/docs/flink/current/flink-streaming-flume/) (sink)
 * [Redis](https://bahir.apache.org/docs/flink/current/flink-streaming-redis/) (sink)
 * [Akka](https://bahir.apache.org/docs/flink/current/flink-streaming-akka/) (sink)
 * [Netty](https://bahir.apache.org/docs/flink/current/flink-streaming-netty/) (source)

## Other Ways to Connect to Flink

### Data Enrichment via Async I/O

Using a connector isn't the only way to get data in and out of Flink.
One common pattern is to query an external database or web service in a `Map` or `FlatMap`
in order to enrich the primary datastream.
Flink offers an API for [Asynchronous I/O]({{< ref "docs/dev/datastream/operators/asyncio" >}})
to make it easier to do this kind of enrichment efficiently and robustly.

{{< top >}}

