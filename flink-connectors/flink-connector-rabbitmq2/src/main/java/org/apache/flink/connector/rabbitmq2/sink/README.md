# RabbitMQ Sink

Flink's RabbitMQ connector provides a sink which enables you to publish your stream directly
to a RabbitMQ exchange in three different consistency modes: at-most-once, at-least-once,
and exactly-once. Furthermore, user defined publish options can be used to customize each message
options in regard to exchange and publish settings in the RabbitMQ context.

## Consistency Behaviour
With __at-most-once__, the sink will simply take each message and publish the serialization of it
(with publish options if given) to RabbitMQ. At this point the sink gives up the ownership of the message.

For __at-least-once__ the same process as for at-most-once is executed except that the ownership of
the message does not end immediately with publishing it. The sink will keep the individual publishing
id for each message as well as the message itself and buffer it as long as it takes to receive the
message received acknowledgment from RabbitMQ. Since we are in the need of buffering messages and waiting
for their asynchronous acknowledgment, this requires _checkpointing enabled_. On each checkpoint,
all to this point buffered (and thus send) but unacknowledged messages will be stored. Simultaneously,
on each checkpoint a resend will be triggered to send all unacknowledged messages once again since
we have to assume that something went wrong for it during the publishing process. Since it can take a
moment until messages get acknowledged from RabbitMQ this can and probably will result in a message
duplication and therefore this logic becomes at-least-once.

By contrast, the __exactly-once-mode__ mode will not send messages on receive. All incoming messages
will be buffered until a checkpoint is triggered. On each checkpoint all messages will be
published/committed as one transaction to ensure the reception acknowledge by RabbitMQ.
If successful, all messages which were committed will be given up, otherwise they will be stored
and tried to commit again in the next transaction during the next checkpoint.
This behaviour ensures that each message will be stored in RabbitMQ exactly once but also has
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
