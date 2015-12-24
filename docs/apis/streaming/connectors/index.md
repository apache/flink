---
title: "Streaming Connectors"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-id: connectors
sub-nav-pos: 2
sub-nav-title: Connectors
---

Connectors provide code for interfacing with various third-party systems.

Currently these systems are supported:

 * [Apache Kafka](https://kafka.apache.org/) (sink/source)
 * [Elasticsearch](https://elastic.co/) (sink)
 * [Hadoop FileSystem](http://hadoop.apache.org) (sink)
 * [RabbitMQ](http://www.rabbitmq.com/) (sink/source)
 * [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis) (source)

To run an application using one of these connectors, additional third party
components are usually required to be installed and launched, e.g. the servers
for the message queues. Further instructions for these can be found in the
corresponding subsections.
