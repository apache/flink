package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** @param <T> the type of the record. */
public class PubSubSplitReader<T> implements SplitReader<Tuple2<T, Long>, PubSubSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitReader.class);
    private final PubSubSubscriber subscriber;
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final PubSubCollector collector;
    private final List<String> messageIdsToAcknowledge = new ArrayList<>();
    private boolean isEndOfStreamSignalled = false;

    public PubSubSplitReader(
            PubSubDeserializationSchema deserializationSchema, PubSubSubscriber subscriber) {

        this.subscriber = subscriber;
        this.deserializationSchema = deserializationSchema;
        this.collector = new PubSubCollector();
    }

    @Override
    public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
        RecordsBySplits.Builder<Tuple2<T, Long>> recordsBySplits = new RecordsBySplits.Builder<>();

        for (ReceivedMessage receivedMessage : subscriber.pull()) {
            try {
                deserializationSchema.deserialize(receivedMessage.getMessage(), collector);
                collector
                        .getMessages()
                        .forEach(
                                message ->
                                        recordsBySplits.add(
                                                PubSubSplit.SPLIT_ID,
                                                new Tuple2<>(
                                                        message,
                                                        receivedMessage
                                                                .getMessage()
                                                                .getPublishTime()
                                                                .getSeconds())));
            } catch (Exception e) {
                throw new IOException("Failed to deserialize received message due to", e);
            } finally {
                collector.reset();
            }

            //            LOG.info(
            //                    "About to add message ID {} to messages to be acknowledged",
            //                    receivedMessage.getAckId());
            messageIdsToAcknowledge.add(receivedMessage.getAckId());
        }

        if (collector.isEndOfStreamSignalled()) {
            recordsBySplits.addFinishedSplit(PubSubSplit.SPLIT_ID);
        }

        return recordsBySplits.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PubSubSplit> splitsChanges) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        subscriber.close();
    }

    private class PubSubCollector implements Collector<T> {
        private final List<T> messages = new ArrayList<>();

        private boolean endOfStreamSignalled = false;

        @Override
        public void collect(T message) {
            if (endOfStreamSignalled || deserializationSchema.isEndOfStream(message)) {
                this.endOfStreamSignalled = true;
                return;
            }

            messages.add(message);
        }

        @Override
        public void close() {}

        private List<T> getMessages() {
            return messages;
        }

        private void reset() {
            messages.clear();
        }

        public boolean isEndOfStreamSignalled() {
            return endOfStreamSignalled;
        }
    }

    //    ------------------------------------------------------

    public void notifyCheckpointComplete() {
        LOG.info("Acknowledging messages with IDs {}", messageIdsToAcknowledge);
        if (!messageIdsToAcknowledge.isEmpty()) {
            subscriber.acknowledge(messageIdsToAcknowledge);
            messageIdsToAcknowledge.clear();
        }
    }
}
