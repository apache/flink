# RabbitMQ Source

Flink's RabbitMQ connector provides a streaming-only source which enables you to receive messages
from a RabbitMQqueue in three different consistency modes: at-most-once, at-least-once, 
and exactly-once.

## Consistency Behaviour
With __at-most-once__, the source will receive each message and automatically acknowledges it to
RabbitMQ. The message content is then polled by the output. If the system crashes in the meantime,
the messages that the source buffers are lost.

By contrast, the messages in the __at-least-once__ mode are not automatically acknowledged but
instead the delivery tag is stored in order to acknowledge it later to RabbitMQ. Messages are polled
by the output and when the notification for a completed checkpoint is received the messages that were
polled are acknowledged to RabbitMQ. Therefore, the mode requires _checkpointing enabled_. This way,
it is assured that the messages are correctly processed by the system. If the system crashes in the
meantime, the unacknowledged messages will be resend by RabbitMQ to assure at-least-once behavior.

The __exactly-once-mode__ mode uses _correlation ids_ to deduplicate messages. Correlation ids are
properties of the messages and need to be set by the message publisher (who publishes the messages 
to RabbitMQ) in order for the mode to function. The user has the obligation to ensure that the set 
correlation id for a message is unique, otherwise no exactly-once can be guaranteed here since 
RabbitMQ itself has no support for automatic exactly-once ids or the required behavior. In addition,
it requires _checkpointing enabled_and only _parallelism 1_ is allowed. Similar to at-least-once, 
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
