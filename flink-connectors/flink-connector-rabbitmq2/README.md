# License of the Rabbit MQ Connector

Flink's RabbitMQ connector defines a Maven dependency on the
"RabbitMQ AMQP Java Client", is triple-licensed under the Mozilla Public License 1.1 ("MPL"),
the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

Flink itself neither reuses source code from the "RabbitMQ AMQP Java Client"
nor packages binaries from the "RabbitMQ AMQP Java Client".

Users that create and publish derivative work based on Flink's
RabbitMQ connector (thereby re-distributing the "RabbitMQ AMQP Java Client")
must be aware that this may be subject to conditions declared in the
Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL")
and the Apache License version 2 ("ASL").

This connector allows consuming messages from and publishing to RabbitMQ. It supports the
Source API specified in [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
and the Sink API specified in [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API).

For more information about RabbitMQ visit https://www.rabbitmq.com/.

In order to view how to use the connector inspect
[Source](src/main/java/org/apache/flink/connector/rabbitmq2/source/README.md) and 
[Sink](src/main/java/org/apache/flink/connector/rabbitmq2/sink/README.md).
