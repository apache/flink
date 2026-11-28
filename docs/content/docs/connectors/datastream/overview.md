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

 * [Cassandra]({{< ref "docs/connectors/datastream/cassandra" >}}) (source/sink)
 * [DataGen]({{< ref "docs/connectors/datastream/datagen" >}}) (source)
 * [Dynamic Kafka]({{< ref "docs/connectors/datastream/dynamic-kafka" >}}) (source)
 * [DynamoDB]({{< ref "docs/connectors/datastream/dynamodb" >}}) (sink)
 * [Elasticsearch]({{< ref "docs/connectors/datastream/elasticsearch" >}}) (sink)
 * [FileSystem]({{< ref "docs/connectors/datastream/filesystem" >}}) (source/sink)
 * [Firehose]({{< ref "docs/connectors/datastream/firehose" >}}) (sink)
 * [Google Cloud PubSub]({{< ref "docs/connectors/datastream/pubsub" >}}) (source/sink)
 * [HTTP]({{< ref "docs/connectors/datastream/http" >}}) (sink)
 * [Hybrid Source]({{< ref "docs/connectors/datastream/hybridsource" >}}) (source)
 * [JDBC]({{< ref "docs/connectors/datastream/jdbc" >}}) (source/sink)
 * [Kafka]({{< ref "docs/connectors/datastream/kafka" >}}) (source/sink)
 * [Kinesis]({{< ref "docs/connectors/datastream/kinesis" >}}) (source/sink)
 * [MongoDB]({{< ref "docs/connectors/datastream/mongodb" >}}) (source/sink)
 * [Opensearch]({{< ref "docs/connectors/datastream/opensearch" >}}) (sink)
 * [Prometheus]({{< ref "docs/connectors/datastream/prometheus" >}}) (sink)
 * [Pulsar]({{< ref "docs/connectors/datastream/pulsar" >}}) (source/sink)
 * [RabbitMQ]({{< ref "docs/connectors/datastream/rabbitmq" >}}) (source/sink)
 * [SQS]({{< ref "docs/connectors/datastream/sqs" >}}) (sink)

Keep in mind that to use one of these connectors in an application, additional third party
components are usually required, e.g. servers for the data stores or message queues.
Note also that while the streaming connectors listed in this section are part of the
Flink project and are included in source releases, they are not included in the binary distributions. 
Further instructions can be found in the corresponding subsections.

{{< hint info >}}
Because the flink-connector-base dependency has been bundled in flink-dist,
these externalized connectors start to stop bundling the flink-connector-base dependency. See more
in <a href="https://issues.apache.org/jira/browse/FLINK-30400">FLINK-30400</a>.
If you need to run examples locally, make sure that the flink-connector-base dependency is
provided and can be found in your own classpath.
{{< /hint >}}

## Flink CDC

[Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) is an Apache Flink sub-project with its own releases. Its source connectors read
changes directly from databases such as MySQL, PostgreSQL, Oracle, SQL Server, MongoDB and Db2. See
the [Flink CDC sources](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/flink-sources/overview/) for the supported databases and Flink versions.

## Fault Tolerance Guarantees

Flink recovers from failures by restarting from the last [checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}). Checkpointing is
disabled by default. For exactly-once results, the source must be able to rewind to the position
stored in the checkpoint, and the sink must take part in checkpointing, either by committing its
writes transactionally or by writing idempotently. See [Stateful Stream Processing]({{< ref "docs/concepts/stateful-stream-processing" >}}#checkpointing) for how checkpoints work.

Sinks built on the [Sink API]({{< ref "docs/internals/sinks" >}}) that support more than one
guarantee take a {{< javadoc file="org/apache/flink/connector/base/DeliveryGuarantee.html" name="DeliveryGuarantee" >}}
(`NONE`, `AT_LEAST_ONCE` or `EXACTLY_ONCE`) in their builder, for example the
[Kafka sink]({{< ref "docs/connectors/datastream/kafka" >}}). Each connector's page documents the
guarantees it provides.

## Integrations Maintained by Other Projects

Some projects maintain their own Flink integration, such as Apache Iceberg and Apache Paimon. See
[Integrations Maintained by Other Projects]({{< ref "docs/connectors/other-projects/overview" >}}).

## Other Ways to Connect to Flink

### Data Enrichment via Async I/O

Using a connector isn't the only way to get data in and out of Flink.
One common pattern is to query an external database or web service in a `Map` or `FlatMap`
in order to enrich the primary datastream.
Flink offers an API for [Asynchronous I/O]({{< ref "docs/dev/datastream/operators/asyncio" >}})
to make it easier to do this kind of enrichment efficiently and robustly.

### 3rd Party Connnectors
* [NATS Connector](https://github.com/synadia-io/flink-connector-nats) (Core and JetStream sinks/sources)

{{< top >}}

