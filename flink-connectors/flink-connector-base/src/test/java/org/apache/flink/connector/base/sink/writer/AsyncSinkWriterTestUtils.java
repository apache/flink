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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/** Utils class for {@link AsyncSinkWriter} related test. */
public class AsyncSinkWriterTestUtils {

    public static <T extends Serializable> BufferedRequestState<T> getTestState(
            ElementConverter<String, T> elementConverter,
            Function<T, Integer> requestSizeExtractor) {
        return new BufferedRequestState<>(
                IntStream.range(0, 100)
                        .mapToObj(i -> String.format("value:%d", i))
                        .map(element -> elementConverter.apply(element, null))
                        .map(
                                request ->
                                        new RequestEntryWrapper<>(
                                                request, requestSizeExtractor.apply(request)))
                        .collect(Collectors.toList()));
    }

    public static <T extends Serializable> void assertThatBufferStatesAreEqual(
            BufferedRequestState<T> actual, BufferedRequestState<T> expected) {
        // Equal states must have equal sizes
        assertEquals(actual.getStateSize(), expected.getStateSize());

        // Equal states must have the same number of requests.
        int actualLength = actual.getBufferedRequestEntries().size();
        assertEquals(actualLength, expected.getBufferedRequestEntries().size());

        List<RequestEntryWrapper<T>> actualRequests = actual.getBufferedRequestEntries();
        List<RequestEntryWrapper<T>> expectedRequests = expected.getBufferedRequestEntries();

        // Equal states must have same requests in the same order.
        for (int i = 0; i < actualLength; i++) {
            assertEquals(
                    actualRequests.get(i).getRequestEntry(),
                    expectedRequests.get(i).getRequestEntry());
            assertEquals(actualRequests.get(i).getSize(), expectedRequests.get(i).getSize());
        }
    }

    public static BufferedRequestState<Integer> getWriterState(
            AsyncSinkWriter<String, Integer> sinkWriter) {
        List<BufferedRequestState<Integer>> states = sinkWriter.snapshotState(1);
        assertEquals(states.size(), 1);
        return states.get(0);
    }

    /** Writer Implementation that writes to {@code destination} list. */
    public static class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        private final Set<Integer> failedFirstAttempts = new HashSet<>();
        private final boolean simulateFailures;
        private final int delay;
        protected List<Integer> destination;

        private AsyncSinkWriterImpl(
                List<Integer> destination,
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                long maxBatchSizeInBytes,
                long maxTimeInBufferMS,
                long maxRecordSizeInBytes,
                boolean simulateFailures,
                int delay,
                RateLimitingStrategy rateLimitingStrategy) {
            this(
                    destination,
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    simulateFailures,
                    delay,
                    rateLimitingStrategy,
                    Collections.emptyList());
        }

        private AsyncSinkWriterImpl(
                List<Integer> destination,
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                long maxBatchSizeInBytes,
                long maxTimeInBufferMS,
                long maxRecordSizeInBytes,
                boolean simulateFailures,
                int delay,
                RateLimitingStrategy rateLimitingStrategy,
                List<BufferedRequestState<Integer>> bufferedState) {

            super(
                    (elem, ctx) -> Integer.parseInt(elem),
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    rateLimitingStrategy,
                    bufferedState);
            this.simulateFailures = simulateFailures;
            this.delay = delay;
            this.destination = destination;
        }

        public void write(String val) throws IOException, InterruptedException {
            write(val, null);
        }

        /**
         * Fails if any value is between 101 and 200. If {@code simulateFailures} is set, it will
         * fail on the first attempt but succeeds upon retry on all others for entries strictly
         * greater than 200.
         *
         * <p>A limitation of this basic implementation is that each element written must be unique.
         *
         * @param requestEntries a set of request entries that should be persisted to {@code res}
         * @param requestResult a Consumer that needs to accept a collection of failure elements
         *     once all request entries have been persisted
         */
        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, Consumer<List<Integer>> requestResult) {
            maybeDelay();

            if (requestEntries.stream().anyMatch(val -> val > 100 && val <= 200)) {
                throw new RuntimeException(
                        "Deliberate runtime exception occurred in SinkWriterImplementation.");
            }
            if (simulateFailures) {
                List<Integer> successfulRetries =
                        failedFirstAttempts.stream()
                                .filter(requestEntries::contains)
                                .collect(Collectors.toList());
                failedFirstAttempts.removeIf(successfulRetries::contains);

                List<Integer> firstTimeFailed =
                        requestEntries.stream()
                                .filter(x -> !successfulRetries.contains(x))
                                .filter(val -> val > 200)
                                .collect(Collectors.toList());
                failedFirstAttempts.addAll(firstTimeFailed);

                requestEntries.removeAll(firstTimeFailed);
                destination.addAll(requestEntries);
                requestResult.accept(firstTimeFailed);
            } else {
                destination.addAll(requestEntries);
                requestResult.accept(new ArrayList<>());
            }
        }

        private void maybeDelay() {
            if (delay <= 0) {
                return;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                fail("Thread sleeping for delay in submitRequestEntries was interrupted.");
            }
        }

        /**
         * @return If we're simulating failures and the requestEntry value is greater than 200, then
         *     the entry is size 100 bytes, otherwise each entry is 4 bytes.
         */
        @Override
        protected long getSizeInBytes(Integer requestEntry) {
            return requestEntry > 200 && simulateFailures ? 100 : 4;
        }

        public BufferedRequestState<Integer> wrapRequests(Integer... requests) {
            return wrapRequests(Arrays.asList(requests));
        }

        public BufferedRequestState<Integer> wrapRequests(List<Integer> requests) {
            List<RequestEntryWrapper<Integer>> wrapperList = new ArrayList<>();
            for (Integer request : requests) {
                wrapperList.add(new RequestEntryWrapper<>(request, getSizeInBytes(request)));
            }

            return new BufferedRequestState<>(wrapperList);
        }

        public int getBufferSize() {
            return snapshotState(1).get(0).getBufferedRequestEntries().size();
        }
    }

    /** A builder for {@link AsyncSinkWriterImpl}. */
    public static class AsyncSinkWriterImplBuilder {

        protected boolean simulateFailures = false;
        protected List<Integer> destination;
        protected int delay = 0;
        protected Sink.InitContext context;
        protected int maxBatchSize = 10;
        protected int maxInFlightRequests = 1;
        protected int maxBufferedRequests = 100;
        protected long maxBatchSizeInBytes = 110;
        protected long maxTimeInBufferMS = 1_000;
        protected long maxRecordSizeInBytes = maxBatchSizeInBytes;
        protected RateLimitingStrategy rateLimitingStrategy;

        public AsyncSinkWriterImplBuilder(List<Integer> destination) {
            this.destination = destination;
        }

        public AsyncSinkWriterImplBuilder context(Sink.InitContext context) {
            this.context = context;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxBufferedRequests(int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxBatchSizeInBytes(long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public AsyncSinkWriterImplBuilder maxRecordSizeInBytes(long maxRecordSizeInBytes) {
            this.maxRecordSizeInBytes = maxRecordSizeInBytes;
            return this;
        }

        public AsyncSinkWriterImplBuilder delay(int delay) {
            this.delay = delay;
            return this;
        }

        public AsyncSinkWriterImplBuilder rateLimitingStrategy(
                RateLimitingStrategy rateLimitingStrategy) {
            this.rateLimitingStrategy = rateLimitingStrategy;
            return this;
        }

        public AsyncSinkWriterImplBuilder simulateFailures(boolean simulateFailures) {
            this.simulateFailures = simulateFailures;
            return this;
        }

        public AsyncSinkWriterImpl build() {
            verifyRateLimitingStrategy();
            return new AsyncSinkWriterImpl(
                    destination,
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    simulateFailures,
                    delay,
                    rateLimitingStrategy);
        }

        public AsyncSinkWriterImpl buildWithState(
                List<BufferedRequestState<Integer>> bufferedState) {
            verifyRateLimitingStrategy();
            return new AsyncSinkWriterImpl(
                    destination,
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    simulateFailures,
                    delay,
                    rateLimitingStrategy,
                    bufferedState);
        }

        private void verifyRateLimitingStrategy() {
            rateLimitingStrategy =
                    Optional.ofNullable(rateLimitingStrategy)
                            .orElse(
                                    new FixedRateLimitingStrategy(
                                            maxInFlightRequests * maxBatchSize));
        }
    }

    /**
     * This SinkWriter releases the lock on existing threads blocked by {@code delayedStartLatch}
     * and blocks itself until {@code blockedThreadLatch} is unblocked.
     */
    public static class AsyncSinkReleaseAndBlockWriterImpl extends AsyncSinkWriterImpl {

        private CountDownLatch blockedThreadLatch;
        private CountDownLatch delayedStartLatch;
        private final boolean blockForLimitedTime;
        private final int maxBatchSize;

        public AsyncSinkReleaseAndBlockWriterImpl(
                List<Integer> destination,
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                long maxBatchSizeInBytes,
                long maxTimeInBufferMS,
                long maxRecordSizeInBytes,
                CountDownLatch blockedThreadLatch,
                CountDownLatch delayedStartLatch,
                boolean blockForLimitedTime,
                RateLimitingStrategy rateLimitingStrategy) {
            super(
                    destination,
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    false,
                    0,
                    rateLimitingStrategy);
            this.blockedThreadLatch = blockedThreadLatch;
            this.delayedStartLatch = delayedStartLatch;
            this.blockForLimitedTime = blockForLimitedTime;
            this.maxBatchSize = maxBatchSize;
        }

        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, Consumer<List<Integer>> requestResult) {
            if (requestEntries.size() == maxBatchSize) {
                try {
                    delayedStartLatch.countDown();
                    if (blockForLimitedTime) {
                        assertFalse(
                                blockedThreadLatch.await(500, TimeUnit.MILLISECONDS),
                                "The countdown latch was released before the full amount"
                                        + "of time was reached.");
                    } else {
                        blockedThreadLatch.await();
                    }
                } catch (InterruptedException e) {
                    fail("The unit test latch must not have been interrupted by another thread.");
                }
            }

            destination.addAll(requestEntries);
            requestResult.accept(new ArrayList<>());
        }
    }

    /** builder for {@code AsyncSinkReleaseAndBlockWriterImpl}. */
    public static class AsyncSinkReleaseAndBlockWriterImplBuilder {

        private CountDownLatch blockedLatch;
        private CountDownLatch delayedLatch;
        private boolean blockForLimitedTime;

        protected int delay = 0;
        protected Sink.InitContext context;
        protected int maxBatchSize = 10;
        protected int maxInFlightRequests = 1;
        protected int maxBufferedRequests = 100;
        protected long maxBatchSizeInBytes = 110;
        protected long maxTimeInBufferMS = 1_000;
        protected long maxRecordSizeInBytes = maxBatchSizeInBytes;
        protected RateLimitingStrategy rateLimitingStrategy;

        public AsyncSinkReleaseAndBlockWriterImplBuilder() {}

        public AsyncSinkReleaseAndBlockWriterImplBuilder blockedThreadLatch(
                CountDownLatch blockedLatch) {
            this.blockedLatch = blockedLatch;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder delayedThreadLatch(
                CountDownLatch delayedLatch) {
            this.delayedLatch = delayedLatch;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder blockForLimitedTime(
                boolean blockForLimitedTime) {
            this.blockForLimitedTime = blockForLimitedTime;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder context(Sink.InitContext context) {
            this.context = context;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder maxInFlightRequests(
                int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder maxBufferedRequests(
                int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder maxBatchSizeInBytes(
                long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder maxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImplBuilder rateLimitingStrategy(
                RateLimitingStrategy rateLimitingStrategy) {
            this.rateLimitingStrategy = rateLimitingStrategy;
            return this;
        }

        public AsyncSinkReleaseAndBlockWriterImpl build() {
            return new AsyncSinkReleaseAndBlockWriterImpl(
                    new ArrayList<>(),
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    blockedLatch,
                    delayedLatch,
                    blockForLimitedTime,
                    rateLimitingStrategy);
        }
    }
}
