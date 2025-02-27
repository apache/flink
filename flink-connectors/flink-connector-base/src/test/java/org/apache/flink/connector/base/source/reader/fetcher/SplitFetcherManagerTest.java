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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.mocks.TestingRecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.test.util.TestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Unit tests for the {@link SplitFetcherManager}. */
class SplitFetcherManagerTest {

    @Test
    void testExceptionPropagationFirstFetch() throws Exception {
        testExceptionPropagation();
    }

    @Test
    void testExceptionPropagationSuccessiveFetch() throws Exception {
        testExceptionPropagation(
                new TestingRecordsWithSplitIds<>("testSplit", 1, 2, 3, 4),
                new TestingRecordsWithSplitIds<>("testSplit", 5, 6, 7, 8));
    }

    @Test
    void testCloseFetcherWithException() throws Exception {
        TestingSplitReader<Object, TestingSourceSplit> reader = new TestingSplitReader<>();
        reader.setCloseWithException();
        SplitFetcherManager<Object, TestingSourceSplit> fetcherManager =
                createFetcher("test-split", reader, new Configuration());
        fetcherManager.close(30000L);
        assertThatThrownBy(fetcherManager::checkErrors)
                .hasRootCauseMessage("Artificial exception on closing the split reader.");
    }

    @Test
    @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
    void testCloseCleansUpPreviouslyClosedFetcher() throws Exception {
        final String splitId = "testSplit";
        // Set the queue capacity to 1 to make sure in this case the
        // fetcher shutdown won't block on putting the batches into the queue.
        Configuration config = new Configuration();
        config.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
        final AwaitingReader<Integer, TestingSourceSplit> reader =
                new AwaitingReader<>(
                        new IOException("Should not happen"),
                        new RecordsBySplits<>(
                                Collections.emptyMap(), Collections.singleton(splitId)));
        final SplitFetcherManager<Integer, TestingSourceSplit> fetcherManager =
                createFetcher(splitId, reader, config);
        // Ensure the fetcher has emitted an element into the queue.
        fetcherManager.getQueue().getAvailabilityFuture().get();
        waitUntil(
                () -> {
                    fetcherManager.maybeShutdownFinishedFetchers();
                    return fetcherManager.fetchers.isEmpty();
                },
                "The idle fetcher should have been removed.");
        // Now close the fetcher manager. The fetcher manager closing should not block.
        fetcherManager.close(Long.MAX_VALUE);
    }

    /**
     * This test is somewhat testing the implementation instead of contract. This is because the
     * test is trying to make sure the element queue draining thread is not tight looping.
     */
    @Test
    public void testCloseBlockingWaitingForFetcherShutdown() throws Exception {
        final String splitId = "testSplit";
        // create a reader which blocks on close().
        final AwaitingReader<Integer, TestingSourceSplit> reader = new AwaitingReader<>();
        final SplitFetcherManager<Integer, TestingSourceSplit> fetcherManager =
                createFetcher(splitId, reader, new Configuration());
        // Now close the fetcher manager. The fetcher should still be running when the fetcher
        // manager returns.
        Thread closingThread =
                new Thread(
                        () -> {
                            try {
                                fetcherManager.close(Long.MAX_VALUE);
                            } catch (Exception e) {
                                fail("failed.");
                                throw new RuntimeException(e);
                            }
                        },
                        "closingThread");
        closingThread.start();

        waitUntil(
                () -> findThread(SplitFetcherManager.THREAD_NAME_PREFIX).size() == 2,
                Duration.ofSeconds(30),
                "The element queue draining thread should have started.");
        for (Thread t : findThread(SplitFetcherManager.THREAD_NAME_PREFIX)) {
            waitUntil(
                    () ->
                            t.getState().equals(Thread.State.WAITING)
                                    || t.getState().equals(Thread.State.TIMED_WAITING),
                    Duration.ofSeconds(30),
                    "All the executor threads should be in waiting status.");
        }

        assertThat(fetcherManager.getQueue().getAvailabilityFuture().getNumberOfDependents())
                .as("The future should have just one dependent stage")
                .isLessThanOrEqualTo(1);
        assertThat(fetcherManager.fetchers.size()).isEqualTo(1);
        reader.triggerThrowException();
        reader.triggerClose();
        waitUntil(fetcherManager.fetchers::isEmpty, "The fetcher should be closed now.");
        closingThread.join();
    }

    @Test
    void testIdleShutdownSplitFetcherWaitsUntilRecordProcessed() throws Exception {
        final String splitId = "testSplit";
        final AwaitingReader<Integer, TestingSourceSplit> reader =
                new AwaitingReader<>(
                        new IOException("Should not happen"),
                        new RecordsBySplits<>(
                                Collections.emptyMap(), Collections.singleton(splitId)));
        final SplitFetcherManager<Integer, TestingSourceSplit> fetcherManager =
                createFetcher(splitId, reader, new Configuration());
        try {
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Integer>> queue =
                    fetcherManager.getQueue();
            // Wait util the data batch is emitted.
            queue.getAvailabilityFuture().get();
            waitUntil(
                    () -> {
                        fetcherManager.maybeShutdownFinishedFetchers();
                        return fetcherManager.getNumAliveFetchers() == 0;
                    },
                    Duration.ofSeconds(1),
                    "The fetcher should have already been removed from the alive fetchers.");

            // There should be two fetches available, one for data (although empty), one for the
            // shutdown synchronization (also empty).
            waitUntil(
                    () -> queue.size() == 2,
                    Duration.ofSeconds(1),
                    "The element queue should have 2 batches when the fetcher is closed.");

            // Finish the first batch (data batch).
            queue.poll().recycle();
            assertThat(reader.isClosed).as("The reader should have not been closed.").isFalse();
            // Finish the second batch (synchronization batch).
            queue.poll().recycle();
            waitUntil(
                    () -> reader.isClosed,
                    Duration.ofSeconds(1),
                    "The reader should hava been closed.");
        } finally {
            fetcherManager.close(30_000);
        }
    }

    // the final modifier is important so that '@SafeVarargs' is accepted on Java 8
    @SuppressWarnings("FinalPrivateMethod")
    @SafeVarargs
    private final void testExceptionPropagation(
            final RecordsWithSplitIds<Integer>... fetchesBeforeError) throws Exception {
        final IOException testingException = new IOException("test");

        final AwaitingReader<Integer, TestingSourceSplit> reader =
                new AwaitingReader<>(testingException, fetchesBeforeError);
        final Configuration configuration = new Configuration();
        configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 10);
        final SplitFetcherManager<Integer, TestingSourceSplit> fetcher =
                createFetcher("testSplit", reader, configuration);

        reader.awaitAllRecordsReturned();
        drainQueue(fetcher.getQueue());

        assertThat(fetcher.getQueue().getAvailabilityFuture().isDone()).isFalse();
        reader.triggerThrowException();

        // await the error propagation
        fetcher.getQueue().getAvailabilityFuture().get();

        try {
            fetcher.checkErrors();
            fail("expected exception");
        } catch (Exception e) {
            assertThat(e.getCause().getCause()).isSameAs(testingException);
        } finally {
            fetcher.close(20_000L);
        }
    }

    // ------------------------------------------------------------------------
    //  test helpers
    // ------------------------------------------------------------------------

    private static <E> SplitFetcherManager<E, TestingSourceSplit> createFetcher(
            final String splitId,
            final SplitReader<E, TestingSourceSplit> reader,
            final Configuration configuration) {

        final SingleThreadFetcherManager<E, TestingSourceSplit> fetcher =
                new SingleThreadFetcherManager<>(() -> reader, configuration);
        fetcher.addSplits(Collections.singletonList(new TestingSourceSplit(splitId)));
        return fetcher;
    }

    private static void drainQueue(FutureCompletingBlockingQueue<?> queue) {
        //noinspection StatementWithEmptyBody
        while (queue.poll() != null) {}
    }

    private static List<Thread> findThread(String keyword) {
        List<Thread> threads = new ArrayList<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains(keyword)) {
                threads.add(t);
            }
        }
        return threads;
    }

    // ------------------------------------------------------------------------
    //  test mocks
    // ------------------------------------------------------------------------

    private static final class AwaitingReader<E, SplitT extends SourceSplit>
            implements SplitReader<E, SplitT> {

        private final Queue<RecordsWithSplitIds<E>> fetches;
        private final IOException testError;

        private final OneShotLatch inBlocking = new OneShotLatch();
        private final OneShotLatch throwError = new OneShotLatch();
        private final OneShotLatch closeBlocker = new OneShotLatch();
        private volatile boolean isClosed = false;

        @SafeVarargs
        AwaitingReader(IOException testError, RecordsWithSplitIds<E>... fetches) {
            this.testError = testError;
            this.fetches = new ArrayDeque<>(Arrays.asList(fetches));
            this.closeBlocker.trigger();
        }

        AwaitingReader() {
            this.testError = new IOException("DummyException");
            this.fetches = new ArrayDeque<>(Collections.emptyList());
        }

        @Override
        public RecordsWithSplitIds<E> fetch() throws IOException {
            if (!fetches.isEmpty()) {
                return fetches.poll();
            } else {
                inBlocking.trigger();
                try {
                    throwError.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted");
                }
                throw testError;
            }
        }

        @Override
        public void handleSplitsChanges(SplitsChange<SplitT> splitsChanges) {}

        @Override
        public void wakeUp() {}

        @Override
        public void close() throws Exception {
            closeBlocker.await();
            isClosed = true;
        }

        public void awaitAllRecordsReturned() throws InterruptedException {
            inBlocking.await();
        }

        public void triggerThrowException() {
            throwError.trigger();
        }

        public void triggerClose() {
            closeBlocker.trigger();
        }
    }
}
