package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.gcp.pubsub.BlockingGrpcPubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** @param <T> the type of the record. */
public class PubSubSplitReader<T> implements SplitReader<Tuple2<T, Long>, PubSubSplit> {
    private final PubSubSubscriber subscriber;
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final SimpleCollector<T> collector;

    public PubSubSplitReader(
            PubSubDeserializationSchema deserializationSchema,
            String projectName,
            String subscriptionName,
            String hostAndPort)
            throws IOException {
        ManagedChannel managedChannel =
                NettyChannelBuilder.forTarget(hostAndPort)
                        .usePlaintext() // This is 'Ok' because this is ONLY used for testing.
                        .build();

        //		TODO: doing this without giving credentials will not work universally, there should be a
        // factory for creating pubsub subscribers like with the old source...
        SubscriberGrpc.SubscriberBlockingStub stub = SubscriberGrpc.newBlockingStub(managedChannel);

        String projectSubscriptionName =
                ProjectSubscriptionName.format(projectName, subscriptionName);

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        //			TODO: add arg for this
                        .setMaxMessages(3)
                        .setSubscription(projectSubscriptionName)
                        .build();

        //		TODO: add args for these two; retries is 10 in existing PubSubSource, timeout the same
        int retries = 2;
        Duration timeout = Duration.ofSeconds(1);

        this.subscriber =
                new BlockingGrpcPubSubSubscriber(
                        projectSubscriptionName,
                        managedChannel,
                        stub,
                        pullRequest,
                        retries,
                        timeout);

        this.deserializationSchema = deserializationSchema;
        this.collector = new SimpleCollector<T>();
    }

    @Override
    public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
        RecordsBySplits.Builder<Tuple2<T, Long>> messagesToReturn = new RecordsBySplits.Builder<>();
        List<ReceivedMessage> receivedMessages = subscriber.pull();
        //		TODO: no checkpointing yet...
        List<String> messageIdsToAck = new ArrayList<>();
        for (ReceivedMessage receivedMessage : receivedMessages) {
            try {
                deserializationSchema.deserialize(receivedMessage.getMessage(), collector);
                //				TODO: is there any case, where there can be more than one message in
                // collector?
                collector
                        .getMessages()
                        .forEach(
                                r -> {
                                    messagesToReturn.add(
                                            "0",
                                            new Tuple2<>(
                                                    r,
                                                    receivedMessage
                                                            .getMessage()
                                                            .getPublishTime()
                                                            .getSeconds()));
                                    System.out.println(
                                            r.toString() + " " + receivedMessage.getAckId());
                                });

                System.out.println(receivedMessage.getAckId());
                messageIdsToAck.add(receivedMessage.getAckId());
            } catch (Exception e) {
                throw new IOException("Failed to deserialize received message due to", e);
            } finally {
                collector.reset();
            }
        }
        subscriber.acknowledge(messageIdsToAck);
        return messagesToReturn.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PubSubSplit> splitsChanges) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        subscriber.close();
    }

    //	TODO: share this collector between KafkaSource and PubSubSource if it remains the same?
    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> messages = new ArrayList<>();

        @Override
        public void collect(T record) {
            messages.add(record);
        }

        @Override
        public void close() {}

        private List<T> getMessages() {
            return messages;
        }

        private void reset() {
            messages.clear();
        }
    }
}
