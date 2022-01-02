# License of the RabbitMQ Connector

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

This connector allows consuming messages from and publishing to RabbitMQ. It implements the
Source API specified in [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
and the Sink API specified in [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API).

For more information about RabbitMQ visit https://www.rabbitmq.com/.

# RabbitMQ Source

Flink's RabbitMQ connector provides a streaming-only source which enables you to receive messages
from a RabbitMQ queue in three different consistency modes: at-most-once, at-least-once,
and exactly-once.

## Consistency Modes

With **at-most-once**, the source will receive each message and automatically acknowledges it to
RabbitMQ. The message content is then polled by the output. If the system crashes in the meantime,
the messages that the source buffers are lost.

By contrast, the messages in the **at-least-once** mode are not automatically acknowledged but
instead the delivery tag is stored in order to acknowledge it later to RabbitMQ. Messages are polled
by the output and when the notification for a completed checkpoint is received the messages that were
polled are acknowledged to RabbitMQ. Therefore, the mode requires _checkpointing enabled_. This way,
it is assured that the messages are correctly processed by the system. If the system crashes in the
meantime, the unacknowledged messages will be resend by RabbitMQ to assure at-least-once behavior.

The **exactly-once-mode** mode uses _correlation ids_ to deduplicate messages. Correlation ids are
properties of the messages and need to be set by the message publisher (who publishes the messages
to RabbitMQ) in order for the mode to function. The user has the obligation to ensure that the set
correlation id for a message is unique, otherwise no exactly-once can be guaranteed here since
RabbitMQ itself has no support for automatic exactly-once ids or the required behavior. In addition,
it requires _checkpointing enabled_and only \_parallelism 1_ is allowed. Similar to at-least-once,
the messages are received from RabbitMQ,buffered, and passed to the output when polled. A set of
seen correlation ids is maintained to apply the deduplication. During a checkpoint, the seen
correlation ids are stored so that in case of failure they can be recovered and used for
deduplication. When the notification for a completed checkpoint is received, all polled messages are
acknowledged as one transaction to ensure the reception by RabbitMQ. Afterwards, the set of
correlation ids is updated as RabbitMQ will not send the acknowledged messages again. This behavior
assures exactly-once processing but also has a performance drawback. Committing many messages will
take time and will thus increase the overall time it takes to do a checkpoint. This can result in
checkpoint delays and in peaks where checkpoint have either many or just a few messages.

## How to use it

```java
public class Main {
    public static void main(String[]args) {

        RabbitMQSource<T> source =
                        RabbitMQSource.<T>builder()
                                .setConnectionConfig(RMQ_CONNECTION_CONFIG)
                                .setQueueName(RABBITMQ_QUEUE_NAME)
                                .setDeserializationSchema(DESERIALIZATION_SCHEMA)
                                .setConsistencyMode(CONSISTENCY_MODE)
                                .build();

        // ******************* An example usage looks like this *******************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RMQConnectionConfig rmqConnectionConfig =
                        new RMQConnectionConfig.Builder()
                                .setHost("localhost")
                                .setVirtualHost("/")
                                .setUserName("guest")
                                .setPassword("guest")
                                .setPort(5672)
                                .build();

        RabbitMQSource<String> rmqSource =
                        RabbitMQSource.<String>builder()
                                .setConnectionConfig(rmqConnectionConfig)
                                .setQueueName("consume-queue")
                                .setDeserializationSchema(new SimpleStringSchema())
                                .setConsistencyMode(ConsistencyMode.AT_MOST_ONCE)
                                .build();

        DataStream<String> stream = env.fromSource(rmqSource, WatermarkStrategy.noWatermarks(), "RMQSource");
    }
}
```

# RabbitMQ Sink

Flink's RabbitMQ connector provides a sink which enables you to publish your stream directly
to a RabbitMQ exchange in three different consistency modes: at-most-once, at-least-once,
and exactly-once. Furthermore, user defined publish options can be used to customize each message
options in regard to exchange and publish settings in the RabbitMQ context.

## Consistency Mode

With **at-most-once**, the sink will simply take each message and publish the serialization of it
(with publish options if given) to RabbitMQ. At this point the sink gives up the ownership of the message.

For **at-least-once** the same process as for at-most-once is executed except that the ownership of
the message does not end immediately with publishing it. The sink will keep the individual publishing
id for each message as well as the message itself and buffer it as long as it takes to receive the
message received acknowledgment from RabbitMQ. Since we are in the need of buffering messages and waiting
for their asynchronous acknowledgment, this requires _checkpointing enabled_. On each checkpoint,
all to this point buffered (and thus send) but unacknowledged messages will be stored. Simultaneously,
on each checkpoint a resend will be triggered to send all unacknowledged messages once again since
we have to assume that something went wrong for it during the publishing process. Since it can take a
moment until messages get acknowledged from RabbitMQ this can and probably will result in a message
duplication and therefore this logic becomes at-least-once.

By contrast, the **exactly-once-mode** mode will not send messages on receive. All incoming messages
will be buffered until a checkpoint is triggered. On each checkpoint all messages will be
published/committed as one transaction to ensure the reception acknowledge by RabbitMQ.
If successful, all messages which were committed will be given up, otherwise they will be stored
and tried to commit again in the next transaction during the next checkpoint.
This consistency mode ensures that each message will be stored in RabbitMQ exactly once but also has
a performance drawback. Committing many messages will take time and will thus increase the overall
time it takes to do a checkpoint. This can result in checkpoint delays and in peaks where
checkpoint have either many or just a few messages. This also correlates to the latency of each message.

## How to use it

```java
RabbitMQSink<T> sink =
                RabbitMQSink.<T>builder()
                        .setConnectionConfig(<RMQConnectionConfig>)
                        .setQueueName(<RabbitMQ Queue Name>)
                        .setSerializationSchema(<Serialization Schema>)
                        .setConsistencyMode(<ConsistencyMode>)
                        .build();

// ******************* An example usage looks like this *******************

RMQConnectionConfig rmqConnectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setVirtualHost("/")
                        .setUserName("guest")
                        .setPassword("guest")
                        .setPort(5672)
                        .build();

RabbitMQSink<String> rmqSink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(rmqConnectionConfig)
                        .setQueueName("publish-queue")
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(ConsistencyMode.AT_MOST_ONCE)
                        .build();

(DataStream<String>).sinkTo(rmqSink)
```
