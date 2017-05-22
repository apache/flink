---
title: "Streaming Connectors"
nav-id: connectors
nav-title: Connectors
nav-parent_id: streaming
nav-pos: 30
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
The [predefined data sources]({{ site.baseurll }}/dev/datastream_api.html#data-sources) include reading from files, directories, and sockets, and
ingesting data from collections and iterators.
The [predefined data sinks]({{ site.baseurl }}/dev/datastream_api.html#data-sinks) support writing to files, to stdout and stderr, and to sockets.

## Bundled Connectors

Connectors provide code for interfacing with various third-party systems. Currently these systems are supported:

 * [Apache Kafka](kafka.html) (sink/source)
 * [Apache Cassandra](cassandra.html) (sink)
 * [Amazon Kinesis Streams](kinesis.html) (sink/source)
 * [Elasticsearch](elasticsearch.html) (sink)
 * [Hadoop FileSystem](filesystem_sink.html) (sink)
 * [RabbitMQ](rabbitmq.html) (sink/source)
 * [Apache NiFi](nifi.html) (sink/source)
 * [Twitter Streaming API](twitter.html) (source)

Keep in mind that to use one of these connectors in an application, additional third party
components are usually required, e.g. servers for the data stores or message queues.
Note also that while the streaming connectors listed in this section are part of the
Flink project and are included in source releases, they are not included in the binary distributions. 
Further instructions can be found in the corresponding subsections.

## Related Topics

### Data Enrichment via Async I/O

Streaming applications sometimes need to pull in data from external services and databases
in order to enrich their event streams.
Flink offers an API for [Asynchronous I/O]({{ site.baseurl }}/dev/stream/asyncio.html)
to make it easier to do this efficiently and robustly.

### Side Outputs

You can always connect an input stream to as many sinks as you like, but sometimes it is
useful to emit additional result streams "on the side," as it were.
[Side Outputs]({{ site.baseurl }}/dev/stream/side_output.html) allow you to flexibily
split and filter your datastream in a typesafe way.

### Queryable State

Rather than always pushing data to external data stores, it is also possible for external applications to query Flink,
and read from the partitioned state it manages on demand.
In some cases this [Queryable State]({{ site.baseurl }}/dev/stream/queryable_state.html) interface can
eliminate what would otherwise be a bottleneck.
