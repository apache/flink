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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
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
    public void numOfRecordsIsAMultipleOfBatchSizeResultsInThatNumberOfRecordsBeingWritten()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 80; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(80, res.size());
    }

    @Test
    public void unwrittenRecordsInBufferArePersistedWhenSnapshotIsTaken()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(20, res.size());
        assertEquals(List.of(20, 21, 22), new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void preparingCommitAtSnapshotTimeEnsuresTheBufferedRecordsArePersistedToDestination()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        sink.prepareCommit(true);
        assertEquals(23, res.size());
    }

    @Test
    public void snapshotsAreTakenOfBufferCorrectlyBeforeAndAfterAutomaticFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);

        sink.write("25");
        sink.write("55");
        assertEquals(List.of(25, 55), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(0, res.size());

        sink.write("75");
        assertEquals(List.of(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(3, res.size());
    }

    @Test
    public void snapshotsAreTakenOfBufferCorrectlyBeforeAndAfterManualFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        assertEquals(List.of(95, 955), new ArrayList<>(sink.snapshotState().get(0)));
        sink.prepareCommit(true);
        assertEquals(List.of(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(5, res.size());
    }

    @Test
    public void runtimeErrorsInSubmitRequestEntriesEndUpAsIOExceptionsWithNumberOfFailedRequests()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("125");
        Exception e = assertThrows(IOException.class, () -> sink.write("135"));
        assertEquals(
                "Failed to submit up to [3] request entries, POSSIBLE DATA LOSS. A "
                        + "runtime exception occured during the submission of the request entries",
                e.getMessage());
        assertEquals(
                "Deliberate runtime exception occurred in SinkWriterImplementation.",
                e.getCause().getMessage());

        sink.prepareCommit(true);
        assertEquals(3, res.size());
    }

    @Test
    public void nonRuntimeErrorsDoNotResultInViolationOfAtLeastOnceSemanticsDueToRequeueOfFailures()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);
        sink.write("25");
        sink.write("55");
        sink.write("965");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        sink.write("550");
        sink.write("45");
        sink.write("35");
        sink.write("535");

        // [535] should be in the bufferedRequestEntries
        // [550] should be in the inFlightRequest, ready to be added
        // [25, 55, 75, 95, 965, 45, 35, 955] should be downstream already
        assertEquals(List.of(535), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(List.of(25, 55, 75, 95, 965, 45, 35, 955), res);

        // Checkpoint occurs
        sink.prepareCommit(true);
        // Everything is saved
        assertEquals(List.of(25, 55, 75, 95, 965, 45, 35, 955, 550, 535), res);
        assertEquals(0, sink.snapshotState().get(0).size());
    }

    @Test
    public void failedEntriesAreRetriedInTheNextPossiblePersistRequestAndNoLater()
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
        assertTrue(res.contains(955));
        sink.write("535");
        sink.write("515");
        sink.write("505");
        assertTrue(res.contains(550));
        assertTrue(res.contains(645));
        assertTrue(res.contains(545));
        sink.prepareCommit(true);
        assertTrue(res.contains(545));
        assertTrue(res.contains(535));
        assertTrue(res.contains(515));
    }

    @Test
    public void maxBufferSizeOfSinkShouldBeStrictlyGreaterThanMaxSizeOfEachBatch() {
        Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 10, false));
        assertEquals(
                e.getMessage(),
                "The maximum number of requests that may be buffered "
                        + "should be strictly greater than the maximum number of requests per batch.");
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
         * Fails if any value is between 101 and 200, and if {@code simulateFailures} is set, fails
         * on the first attempt but succeeds upon retry on all others.
         *
         * @param requestEntries a set of request entries that should be persisted to {@code res}
         * @param requestResult a ResultFuture that needs to be completed once all request entries
         *     have been persisted. Any failures should be elements of the list being completed
         */
        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, ResultFuture<Integer> requestResult) {
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

                List<Integer> requestEntriesWithoutSuccessfulRetries =
                        requestEntries.stream()
                                .filter(x -> !successfulRetries.contains(x))
                                .collect(Collectors.toList());

                List<Integer> firstTimeFailed =
                        requestEntriesWithoutSuccessfulRetries.stream()
                                .filter(val -> val > 200)
                                .collect(Collectors.toList());
                requestEntriesWithoutSuccessfulRetries.removeAll(firstTimeFailed);
                failedFirstAttempts.addAll(firstTimeFailed);

                res.addAll(requestEntriesWithoutSuccessfulRetries);
                res.addAll(successfulRetries);
                requestResult.complete(firstTimeFailed);
            } else {
                res.addAll(requestEntries);
                requestResult.complete(new ArrayList<>());
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
