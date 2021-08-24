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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit Tests the functionality of AsyncSinkWriter without any assumptions of what a concrete
 * implementation might do.
 */
public class AsyncSinkWriterTest {

    private final List<Integer> res = new ArrayList<>();
    private final SinkInitContext sinkInitContext = new SinkInitContext();

    @Before
    public void before() {
        res.clear();
    }

    @Test
    public void testNumberOfRecordsIsAMultipleOfBatchSizeResultsInThatNumberOfRecordsBeingWritten()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 80; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(80, res.size());
    }

    @Test
    public void testThatUnwrittenRecordsInBufferArePersistedWhenSnapshotIsTaken()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(20, res.size());
        assertEquals(Arrays.asList(20, 21, 22), new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void testPreparingCommitAtSnapshotTimeEnsuresBufferedRecordsArePersistedToDestination()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        sink.prepareCommit(true);
        assertEquals(23, res.size());
    }

    @Test
    public void testThatSnapshotsAreTakenOfBufferCorrectlyBeforeAndAfterAutomaticFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);

        sink.write("25");
        sink.write("55");
        assertEquals(Arrays.asList(25, 55), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(0, res.size());

        sink.write("75");
        assertEquals(Arrays.asList(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(3, res.size());
    }

    @Test
    public void testThatSnapshotsAreTakenOfBufferCorrectlyBeforeAndAfterManualFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        assertEquals(Arrays.asList(95, 955), new ArrayList<>(sink.snapshotState().get(0)));
        sink.prepareCommit(true);
        assertEquals(Arrays.asList(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(5, res.size());
    }

    @Test
    public void testRuntimeErrorsInSubmitRequestEntriesEndUpAsIOExceptionsWithNumOfFailedRequests()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("35");
        Exception e = assertThrows(RuntimeException.class, () -> sink.write("135"));
        assertEquals(
                "Deliberate runtime exception occurred in SinkWriterImplementation.",
                e.getMessage());
        assertEquals(3, res.size());
    }

    @Test
    public void testRetryableErrorsDoNotViolateAtLeastOnceSemanticsDueToRequeueOfFailures()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "25", Arrays.asList(), Arrays.asList(25));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "55", Arrays.asList(), Arrays.asList(25, 55));

        // 25, 55 persisted; 965 failed and inflight
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "965", Arrays.asList(25, 55), Arrays.asList());

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "75", Arrays.asList(25, 55), Arrays.asList(75));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "95", Arrays.asList(25, 55), Arrays.asList(75, 95));

        /*
         * Writing 955 to the sink increases the buffer to size 3 containing [75, 95, 955]. This
         * triggers the outstanding in flight request with the failed 965 to be run, and 965 is
         * placed at the front of the queue. The first {@code maxBatchSize = 3} elements are
         * persisted, with 965 succeeding this (second) time. 955 remains in the buffer.
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "955", Arrays.asList(25, 55, 965, 75, 95), Arrays.asList(955));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "550", Arrays.asList(25, 55, 965, 75, 95), Arrays.asList(955, 550));

        /*
         * [955, 550, 45] are attempted to be persisted
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "45", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList());

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "35", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList(35));

        /* [35, 535] should be in the bufferedRequestEntries
         * [955, 550] should be in the inFlightRequest, ready to be added
         * [25, 55, 965, 75, 95, 45] should be downstream already
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "535", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList(35, 535));

        // Checkpoint occurs
        sink.prepareCommit(true);

        // Everything is saved
        assertEquals(Arrays.asList(25, 55, 965, 75, 95, 45, 550, 955, 35, 535), res);
        assertEquals(0, sink.snapshotState().get(0).size());
    }

    @Test
    public void testFailedEntriesAreRetriedInTheNextPossiblePersistRequestAndNoLater()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);
        sink.write("25");
        sink.write("55");
        sink.write("965");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        assertTrue(res.contains(965));
        sink.write("550");
        sink.write("645");
        sink.write("545");
        sink.write("535");
        sink.write("515");
        assertTrue(res.contains(955));
        sink.write("505");
        assertTrue(res.contains(550));
        assertTrue(res.contains(645));
        sink.prepareCommit(true);
        assertTrue(res.contains(545));
        assertTrue(res.contains(535));
        assertTrue(res.contains(515));
    }

    @Test
    public void testThatMaxBufferSizeOfSinkShouldBeStrictlyGreaterThanMaxSizeOfEachBatch() {
        Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 10, false));
        assertEquals(
                e.getMessage(),
                "The maximum number of requests that may be buffered should be "
                        + "strictly greater than the maximum number of requests per batch.");
    }

    private void writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
            AsyncSinkWriterImpl sink, String x, List<Integer> y, List<Integer> z)
            throws IOException, InterruptedException {
        sink.write(x);
        assertEquals(y, res);
        assertEquals(z, new ArrayList<>(sink.snapshotState().get(0)));
    }

    private class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        private final Set<Integer> failedFirstAttempts = new HashSet<>();
        private final boolean simulateFailures;

        public AsyncSinkWriterImpl(
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                boolean simulateFailures) {
            super(
                    (elem, ctx) -> Integer.parseInt(elem),
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests);
            this.simulateFailures = simulateFailures;
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
                List<Integer> requestEntries, Consumer<Collection<Integer>> requestResult) {
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
                res.addAll(requestEntries);
                requestResult.accept(firstTimeFailed);
            } else {
                res.addAll(requestEntries);
                requestResult.accept(new ArrayList<>());
            }
        }
    }

    private static class SinkInitContext implements Sink.InitContext {

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            StreamTaskActionExecutor streamTaskActionExecutor =
                    new StreamTaskActionExecutor() {
                        @Override
                        public void run(RunnableWithException e) throws Exception {
                            e.run();
                        }

                        @Override
                        public <E extends Throwable> void runThrowing(
                                ThrowingRunnable<E> throwingRunnable) throws E {
                            throwingRunnable.run();
                        }

                        @Override
                        public <R> R call(Callable<R> callable) throws Exception {
                            return callable.call();
                        }
                    };
            return new MailboxExecutorImpl(
                    new TaskMailboxImpl(Thread.currentThread()),
                    Integer.MAX_VALUE,
                    streamTaskActionExecutor);
        }

        @Override
        public Sink.ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return null;
        }
    }
}
