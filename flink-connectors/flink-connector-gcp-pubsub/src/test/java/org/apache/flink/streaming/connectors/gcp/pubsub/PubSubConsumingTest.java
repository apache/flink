/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;

import com.google.auth.Credentials;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for consuming records with {@link PubSubSource}. */
public class PubSubConsumingTest {

    @Test
    public void testProcessMessage() throws Exception {
        TestPubSubSubscriber testPubSubSubscriber =
                new TestPubSubSubscriber(
                        receivedMessage("1", pubSubMessage("A")),
                        receivedMessage("2", pubSubMessage("B")));
        // Pubsub source without endpoint
        PubSubSource<String> pubSubSource =
                PubSubSource.newBuilder()
                        .withDeserializationSchema(new SimpleStringSchema())
                        .withProjectName("fakeProject")
                        .withSubscriptionName("fakeSubscription")
                        .withPubSubSubscriberFactory(credentials -> testPubSubSubscriber)
                        .withCredentials(mock(Credentials.class))
                        .build();

        Object lock = new Object();
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();
        Thread thread = createSourceThread(pubSubSource, lock, results);
        try {
            thread.start();
            awaitRecordCount(results, 2);

            assertThat(new ArrayList<>(results), equalTo(Arrays.asList("A", "B")));
            pubSubSource.snapshotState(0, 0);
            pubSubSource.notifyCheckpointComplete(0);
            assertThat(testPubSubSubscriber.getAcknowledgedIds(), equalTo(Arrays.asList("1", "2")));
        } finally {
            pubSubSource.cancel();
            thread.join();
        }
    }

    @Test
    public void testStoppingConnectorWhenDeserializationSchemaIndicatesEndOfStream()
            throws Exception {
        TestPubSubSubscriber testPubSubSubscriber =
                new TestPubSubSubscriber(
                        receivedMessage("1", pubSubMessage("A")),
                        receivedMessage("2", pubSubMessage("B")),
                        receivedMessage("3", pubSubMessage("C")),
                        receivedMessage("4", pubSubMessage("D")));

        // Pubsub source with endpoint
        PubSubSource<String> pubSubSource =
                PubSubSource.newBuilder()
                        .withDeserializationSchema(
                                new SimpleStringSchema() {
                                    @Override
                                    public boolean isEndOfStream(String nextElement) {
                                        return nextElement.equals("C");
                                    }
                                })
                        .withProjectName("fakeProject")
                        .withSubscriptionName("fakeSubscription")
                        .withPubSubSubscriberFactory(credentials -> testPubSubSubscriber)
                        .withCredentials(mock(Credentials.class))
                        .withEndpoint("us-central1-pubsub.googleapis.com:443")
                        .build();

        Object lock = new Object();
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();
        Thread thread = createSourceThread(pubSubSource, lock, results);
        try {
            thread.start();
            awaitRecordCount(results, 2);

            // we do not emit the end of stream record
            assertThat(new ArrayList<>(results), equalTo(Arrays.asList("A", "B")));
            pubSubSource.snapshotState(0, 0);
            pubSubSource.notifyCheckpointComplete(0);
            // we acknowledge also the end of the stream record
            assertThat(
                    testPubSubSubscriber.getAcknowledgedIds(),
                    equalTo(Arrays.asList("1", "2", "3")));
        } finally {
            pubSubSource.cancel();
            thread.join();
        }
    }

    @Test
    public void testProducingMultipleResults() throws Exception {
        TestPubSubSubscriber testPubSubSubscriber =
                new TestPubSubSubscriber(
                        receivedMessage("1", pubSubMessage("A")),
                        receivedMessage("2", pubSubMessage("B,C,D")),
                        receivedMessage("3", pubSubMessage("E")));
        PubSubSource<String> pubSubSource =
                PubSubSource.newBuilder()
                        .withDeserializationSchema(
                                new SimpleStringSchema() {
                                    @Override
                                    public void deserialize(byte[] message, Collector<String> out) {
                                        String[] records = super.deserialize(message).split(",");
                                        for (String record : records) {
                                            out.collect(record);
                                        }
                                    }

                                    @Override
                                    public boolean isEndOfStream(String nextElement) {
                                        return nextElement.equals("C");
                                    }
                                })
                        .withProjectName("fakeProject")
                        .withSubscriptionName("fakeSubscription")
                        .withPubSubSubscriberFactory(credentials -> testPubSubSubscriber)
                        .withCredentials(mock(Credentials.class))
                        .build();

        Object lock = new Object();
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();
        Thread thread = createSourceThread(pubSubSource, lock, results);
        try {
            thread.start();
            awaitRecordCount(results, 2);

            // we emit only the records prior to the end of the stream
            assertThat(new ArrayList<>(results), equalTo(Arrays.asList("A", "B")));
            pubSubSource.snapshotState(0, 0);
            pubSubSource.notifyCheckpointComplete(0);
            // we acknowledge also the end of the stream record
            assertThat(testPubSubSubscriber.getAcknowledgedIds(), equalTo(Arrays.asList("1", "2")));
        } finally {
            pubSubSource.cancel();
            thread.join();
        }
    }

    private Thread createSourceThread(
            PubSubSource<String> pubSubSource, Object lock, ConcurrentLinkedQueue<String> results) {
        return new Thread(
                () -> {
                    try {
                        pubSubSource.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
                        pubSubSource.open(new Configuration());
                        pubSubSource.run(new CollectingSourceContext<>(lock, results));
                    } catch (InterruptedException e) {
                        // expected on cancel
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static <T> void awaitRecordCount(ConcurrentLinkedQueue<T> queue, int count)
            throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        while (deadline.hasTimeLeft() && queue.size() < count) {
            Thread.sleep(10);
        }
    }

    private ReceivedMessage receivedMessage(String ackId, PubsubMessage pubsubMessage) {
        return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(pubsubMessage).build();
    }

    private PubsubMessage pubSubMessage(String message) {
        return PubsubMessage.newBuilder()
                .setMessageId("some id")
                .setData(ByteString.copyFrom(message.getBytes()))
                .build();
    }

    private static class TestPubSubSubscriber implements PubSubSubscriber {

        private final Queue<ReceivedMessage> queue;
        private final List<String> acknowledgedIds = new ArrayList<>();

        private TestPubSubSubscriber(ReceivedMessage... messages) {
            this.queue = new ArrayBlockingQueue<>(messages.length, true, Arrays.asList(messages));
        }

        @Override
        public List<ReceivedMessage> pull() {
            ReceivedMessage message = queue.poll();
            if (message != null) {
                return Collections.singletonList(message);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public void close() {}

        public List<String> getAcknowledgedIds() {
            return acknowledgedIds;
        }

        @Override
        public void acknowledge(List<String> ids) {
            acknowledgedIds.addAll(ids);
        }
    }
}
