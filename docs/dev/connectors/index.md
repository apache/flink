---
title: "DataStream Connectors"
nav-id: connectors
nav-title: DataStream Connectors
nav-parent_id: connectors-root
nav-pos: 1
nav-show_overview: true
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

* toc
{:toc}

## Predefined Sources and Sinks

A few basic data sources and sinks are built into Flink and are always available.
The [predefined data sources]({{ site.baseurl }}/dev/datastream_api.html#data-sources) include reading from files, directories, and sockets, and
ingesting data from collections and iterators.
The [predefined data sinks]({{ site.baseurl }}/dev/datastream_api.html#data-sinks) support writing to files, to stdout and stderr, and to sockets.

## Bundled Connectors

Connectors provide code for interfacing with various third-party systems. Currently these systems are supported:

 * [Apache Kafka](kafka.html) (source/sink)
 * [Apache Cassandra](cassandra.html) (sink)
 * [Amazon Kinesis Streams](kinesis.html) (source/sink)
 * [Elasticsearch](elasticsearch.html) (sink)
 * [Hadoop FileSystem](filesystem_sink.html) (sink)
 * [RabbitMQ](rabbitmq.html) (source/sink)
 * [Apache NiFi](nifi.html) (source/sink)
 * [Twitter Streaming API](twitter.html) (source)
 * [Google PubSub](pubsub.html) (source/sink)
 * [JDBC](jdbc.html) (sink)

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
Flink offers an API for [Asynchronous I/O]({{ site.baseurl }}/dev/stream/operators/asyncio.html)
to make it easier to do this kind of enrichment efficiently and robustly.

### Queryable State

When a Flink application pushes a lot of data to an external data store, this
can become an I/O bottleneck.
If the data involved has many fewer reads than writes, a better approach can be
for an external application to pull from Flink the data it needs.
The [Queryable State]({{ site.baseurl }}/dev/stream/state/queryable_state.html) interface
enables this by allowing the state being managed by Flink to be queried on demand.

{% top %}
