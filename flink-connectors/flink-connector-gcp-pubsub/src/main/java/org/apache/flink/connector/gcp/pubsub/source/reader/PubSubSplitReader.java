/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.connector.gcp.pubsub.source.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.gcp.pubsub.source.split.PubSubSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Collector;

import com.google.auth.Credentials;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A {@link SplitReader} to read from a given {@link PubSubSubscriber}.
 *
 * @param <T> the type of the record.
 */
public class PubSubSplitReader<T> implements SplitReader<Tuple2<T, Long>, PubSubSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitReader.class);
    private static final long UPCOMING_CHECKPOINT = 0;
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final PubSubSubscriberFactory pubSubSubscriberFactory;
    private final Credentials credentials;
    private PubSubSubscriber subscriber;
    private final PubSubCollector collector;
    // Store the IDs of GCP Pub/Sub messages that yet have to be acknowledged so that they are not
    // resent. Must be synchronized because it's accessed both by the fetcher and the reader thread.
    private final SortedMap<Long, List<String>> messageIdsToAcknowledge =
            Collections.synchronizedSortedMap(new TreeMap<>());

    /**
     * @param deserializationSchema a deserialization schema to apply to incoming message payloads.
     * @param pubSubSubscriberFactory a factory from which a new subscriber can be created from
     * @param credentials the credentials to use for creating a new subscriber
     */
    public PubSubSplitReader(
            PubSubDeserializationSchema deserializationSchema,
            PubSubSubscriberFactory pubSubSubscriberFactory,
            Credentials credentials) {

        this.deserializationSchema = deserializationSchema;
        this.pubSubSubscriberFactory = pubSubSubscriberFactory;
        this.credentials = credentials;
        this.collector = new PubSubCollector();

        this.messageIdsToAcknowledge.put(UPCOMING_CHECKPOINT, new ArrayList<>());
    }

    @Override
    public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
        RecordsBySplits.Builder<Tuple2<T, Long>> recordsBySplits = new RecordsBySplits.Builder<>();
        if (subscriber == null) {
            subscriber = pubSubSubscriberFactory.getSubscriber(credentials);
        }

        for (ReceivedMessage receivedMessage : subscriber.pull()) {
            try {
                // Deserialize messages into a collector so that logic in the user-provided
                // deserialization schema decides how to map GCP Pub/Sub messages to records in
                // Flink. This allows e.g. batching together multiple Flink records in a single GCP
                // Pub/Sub message.
                deserializationSchema.deserialize(receivedMessage.getMessage(), collector);
                collector
                        .getMessages()
                        .forEach(
                                message ->
                                        recordsBySplits.add(
                                                PubSubSplit.SPLIT_ID,
                                                new Tuple2<>(
                                                        message,
                                                        // A timestamp provided by GCP Pub/Sub
                                                        // indicating when the message was initially
                                                        // published
                                                        receivedMessage
                                                                .getMessage()
                                                                .getPublishTime()
                                                                .getSeconds())));
            } catch (Exception e) {
                throw new IOException("Failed to deserialize received message due to", e);
            } finally {
                collector.reset();
            }

            messageIdsToAcknowledge.get(UPCOMING_CHECKPOINT).add(receivedMessage.getAckId());
        }

        return recordsBySplits.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PubSubSplit> splitsChanges) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (subscriber != null) {
            subscriber.close();
        }
    }

    private class PubSubCollector implements Collector<T> {
        private final List<T> messages = new ArrayList<>();

        @Override
        public void collect(T message) {
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
    }

    //    ------------------------------------------------------

    void prepareForAcknowledgement(long checkpointId) {
        synchronized (messageIdsToAcknowledge) {
            messageIdsToAcknowledge.put(
                    checkpointId, messageIdsToAcknowledge.remove(UPCOMING_CHECKPOINT));
            messageIdsToAcknowledge.put(UPCOMING_CHECKPOINT, new ArrayList<>());
        }
    }

    /**
     * Acknowledge the reception of messages towards GCP Pub/Sub since the last checkpoint. As long
     * as a received message has not been acknowledged, GCP Pub/Sub will attempt to deliver it
     * again.
     *
     * <p>Calling this message is enqueued by the {@link PubSubSourceFetcherManager} on checkpoint.
     */
    void acknowledgeMessages(long checkpointId) {
        synchronized (messageIdsToAcknowledge) {
            List<String> messageIdsForCheckpoint = messageIdsToAcknowledge.get(checkpointId);
            if (!messageIdsForCheckpoint.isEmpty() && subscriber != null) {
                LOG.info(
                        "Acknowledging messages for checkpoint {} with IDs {}",
                        checkpointId,
                        messageIdsForCheckpoint);
                subscriber.acknowledge(messageIdsForCheckpoint);
            }
            messageIdsToAcknowledge.remove(checkpointId);
        }
    }
}
