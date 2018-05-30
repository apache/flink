# License of the Rabbit MQ Connector

Flink's RabbitMQ connector defines a Maven dependency on the
"RabbitMQ AMQP Java Client", is triple-licensed under the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

Flink itself neither reuses source code from the "RabbitMQ AMQP Java Client"
nor packages binaries from the "RabbitMQ AMQP Java Client".

Users that create and publish derivative work based on Flink's
RabbitMQ connector (thereby re-distributing the "RabbitMQ AMQP Java Client")
must be aware that this may be subject to conditions declared in the Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2 ("GPL") and the Apache License version 2 ("ASL").

# This version provides a mechanism to handle AMQ Messaging features

One of its Constructor uses an implemented interface object with five methods and an optionnal returned message handler. See RMQSinkFeatureTest class to get a sample of the methods to implement. The returned message handler is an implementation of the standard com.rabbitmq.client.ReturnListener interface. As this mechasnism uses RoutingKeys, queueName is null then the queue can not be declared to RabbitMQ during start. 
