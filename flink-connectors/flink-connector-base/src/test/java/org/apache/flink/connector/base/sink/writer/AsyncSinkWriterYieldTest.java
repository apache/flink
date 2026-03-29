/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for conditional yielding behavior in AsyncSinkWriter flush method. Verifies that
 * yieldIfThereExistsInFlightRequests() only yields when there are actual in-flight requests.
 */
class AsyncSinkWriterYieldTest {

    private TestSinkInitContext sinkInitContext;
    private final List<Integer> results = new ArrayList<>();
    private AtomicInteger yieldCallCount;

    @BeforeEach
    void setup() {
        yieldCallCount = new AtomicInteger(0);
        sinkInitContext = new TestSinkInitContext();
        results.clear();
    }

    @Test
    void testFlushWithEmptyBufferDoesNotYield() throws Exception {
        TrackingMailboxExecutor trackingMailbox = new TrackingMailboxExecutor(yieldCallCount);
        TestSinkInitContext contextWithTracking =
                new TestSinkInitContextWithCustomMailbox(trackingMailbox);

        TestAsyncSinkWriter sink =
                new TestAsyncSinkWriter(contextWithTracking, 10, 100, 1000, results);

        int yieldCountBefore = yieldCallCount.get();
        sink.flush(false);
        int yieldCountAfter = yieldCallCount.get();

        assertThat(yieldCountAfter).isEqualTo(yieldCountBefore);
        assertThat(yieldCountAfter).isEqualTo(0);
    }

    @Test
    void testFlushWithBufferedElementsButNoInFlightRequestsDoesNotYield() throws Exception {
        TrackingMailboxExecutor trackingMailbox = new TrackingMailboxExecutor(yieldCallCount);
        TestSinkInitContext contextWithTracking =
                new TestSinkInitContextWithCustomMailbox(trackingMailbox);

        TestAsyncSinkWriter sink =
                new TestAsyncSinkWriter(contextWithTracking, 10, 100, 1000, results);

        sink.write("1");
        sink.write("2");

        int yieldCountBefore = yieldCallCount.get();
        sink.flush(false);
        int yieldCountAfter = yieldCallCount.get();

        assertThat(yieldCountAfter).isEqualTo(yieldCountBefore);
        assertThat(yieldCountAfter).isEqualTo(0);
    }

    @Test
    void testFlushWithTrueFlushesAllElementsWithoutYielding() throws Exception {
        TrackingMailboxExecutor trackingMailbox = new TrackingMailboxExecutor(yieldCallCount);
        TestSinkInitContext contextWithTracking =
                new TestSinkInitContextWithCustomMailbox(trackingMailbox);

        TestAsyncSinkWriter sink =
                new TestAsyncSinkWriter(contextWithTracking, 10, 100, 1000, results);

        sink.write("1");
        sink.write("2");
        sink.write("3");

        sink.flush(true);

        assertThat(results).containsExactly(1, 2, 3);
        assertThat(yieldCallCount.get()).isEqualTo(0);
    }

    private static class TestAsyncSinkWriter extends AsyncSinkWriter<String, Integer> {
        private final List<Integer> results;

        TestAsyncSinkWriter(
                TestSinkInitContext context,
                int maxBatchSize,
                int maxBufferedRequests,
                long maxBatchSizeInBytes,
                List<Integer> results) {
            super(
                    (elem, ctx) -> Integer.parseInt(elem),
                    context,
                    AsyncSinkWriterConfiguration.builder()
                            .setMaxBatchSize(maxBatchSize)
                            .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                            .setMaxInFlightRequests(1)
                            .setMaxBufferedRequests(maxBufferedRequests)
                            .setMaxTimeInBufferMS(1000)
                            .setMaxRecordSizeInBytes(100)
                            .setRateLimitingStrategy(
                                    CongestionControlRateLimitingStrategy.builder()
                                            .setInitialMaxInFlightMessages(maxBatchSize)
                                            .setMaxInFlightRequests(1)
                                            .setScalingStrategy(
                                                    AIMDScalingStrategy.builder(maxBatchSize)
                                                            .build())
                                            .build())
                            .build(),
                    Collections.emptyList());
            this.results = results;
        }

        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, ResultHandler<Integer> resultHandler) {
            results.addAll(requestEntries);
            resultHandler.complete();
        }

        @Override
        protected long getSizeInBytes(Integer requestEntry) {
            return 4;
        }

        public void write(String val) throws IOException, InterruptedException {
            write(val, null);
        }
    }

    private static class TrackingMailboxExecutor implements MailboxExecutor {
        private final AtomicInteger yieldCount;

        TrackingMailboxExecutor(AtomicInteger yieldCount) {
            this.yieldCount = yieldCount;
        }

        @Override
        public void execute(
                MailOptions mailOptions,
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void yield() throws InterruptedException {
            yieldCount.incrementAndGet();
        }

        @Override
        public boolean tryYield() {
            return false;
        }

        @Override
        public boolean shouldInterrupt() {
            return false;
        }
    }

    private static class TestSinkInitContextWithCustomMailbox extends TestSinkInitContext {
        private final MailboxExecutor customMailbox;

        TestSinkInitContextWithCustomMailbox(MailboxExecutor mailbox) {
            this.customMailbox = mailbox;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return customMailbox;
        }
    }
}
