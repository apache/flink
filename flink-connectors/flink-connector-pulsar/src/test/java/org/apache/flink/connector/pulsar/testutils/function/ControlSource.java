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

package org.apache.flink.connector.pulsar.testutils.function;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/**
 * This source is used for testing in Pulsar sink. We would generate a fix number of records by the
 * topic name and message index.
 */
public class ControlSource extends AbstractRichFunction
        implements SourceFunction<String>, CheckpointListener, CheckpointedFunction {
    private static final long serialVersionUID = -3124248855144675017L;

    private static final Logger LOG = LoggerFactory.getLogger(StopSignal.class);

    private final SharedReference<MessageGenerator> sharedGenerator;
    private final SharedReference<StopSignal> sharedSignal;
    private Object lock;

    public ControlSource(
            SharedObjectsExtension sharedObjects,
            PulsarRuntimeOperator operator,
            String topic,
            DeliveryGuarantee guarantee,
            int messageCounts,
            Duration timeout) {
        MessageGenerator generator = new MessageGenerator(topic, guarantee, messageCounts);
        StopSignal signal = new StopSignal(operator, topic, messageCounts, timeout);

        this.sharedGenerator = sharedObjects.add(generator);
        this.sharedSignal = sharedObjects.add(signal);
    }

    @Override
    public void run(SourceContext<String> ctx) {
        MessageGenerator generator = sharedGenerator.get();
        StopSignal signal = sharedSignal.get();
        this.lock = ctx.getCheckpointLock();

        while (!signal.canStop()) {
            synchronized (lock) {
                if (generator.hasNext()) {
                    String message = generator.next();
                    ctx.collect(message);
                }
            }
        }
    }

    public List<String> getExpectedRecords() {
        MessageGenerator generator = sharedGenerator.get();
        return generator.getExpectedRecords();
    }

    public List<String> getConsumedRecords() {
        StopSignal signal = sharedSignal.get();
        return signal.getConsumedRecords();
    }

    @Override
    public void cancel() {
        LOG.warn("Triggering cancel action. Set the stop timeout to zero.");
        StopSignal signal = sharedSignal.get();
        signal.deadline.set(System.currentTimeMillis());
    }

    @Override
    public void close() throws Exception {
        StopSignal signal = sharedSignal.get();
        signal.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        // Nothing to do.
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // Nothing to do.
    }

    private static class MessageGenerator implements Iterator<String> {

        private final String topic;
        private final DeliveryGuarantee guarantee;
        private final int messageCounts;
        private final List<String> expectedRecords;

        public MessageGenerator(String topic, DeliveryGuarantee guarantee, int messageCounts) {
            this.topic = topic;
            this.guarantee = guarantee;
            this.messageCounts = messageCounts;
            this.expectedRecords = new ArrayList<>(messageCounts);
        }

        @Override
        public boolean hasNext() {
            return messageCounts > expectedRecords.size();
        }

        @Override
        public String next() {
            String content =
                    guarantee.name()
                            + "-"
                            + topic
                            + "-"
                            + expectedRecords.size()
                            + "-"
                            + randomAlphanumeric(10);
            expectedRecords.add(content);
            return content;
        }

        public List<String> getExpectedRecords() {
            return expectedRecords;
        }
    }

    /**
     * This is used in {@link ControlSource}, we can stop the source by this method. Make sure you
     * wrap this instance into a {@link SharedReference}.
     */
    private static class StopSignal implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(StopSignal.class);

        private final String topic;
        private final int desiredCounts;
        // This is a thread-safe list.
        private final List<String> consumedRecords;
        private final AtomicLong deadline;
        private final ExecutorService executor;

        public StopSignal(
                PulsarRuntimeOperator operator, String topic, int messageCounts, Duration timeout) {
            this.topic = topic;
            this.desiredCounts = messageCounts;
            this.consumedRecords = Collections.synchronizedList(new ArrayList<>(messageCounts));
            this.deadline = new AtomicLong(timeout.toMillis() + System.currentTimeMillis());
            this.executor = Executors.newSingleThreadExecutor();

            // Start consuming.
            executor.execute(
                    () -> {
                        while (consumedRecords.size() < desiredCounts) {
                            // This method would block until we consumed a message.
                            int counts = desiredCounts - consumedRecords.size();
                            List<Message<String>> messages =
                                    operator.receiveMessages(this.topic, Schema.STRING, counts);
                            for (Message<String> message : messages) {
                                consumedRecords.add(message.getValue());
                            }
                        }
                    });
        }

        public boolean canStop() {
            if (deadline.get() < System.currentTimeMillis()) {
                String errorMsg =
                        String.format(
                                "Timeout for waiting the records from Pulsar. We have consumed %d messages, expect %d messages.",
                                consumedRecords.size(), desiredCounts);
                LOG.warn(errorMsg);
                return true;
            }

            return consumedRecords.size() >= desiredCounts;
        }

        public List<String> getConsumedRecords() {
            return consumedRecords;
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }
}
