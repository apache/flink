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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.TestingRecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.testutils.OneShotLatch;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/** Unit tests for the {@link SplitFetcherManager}. */
public class SplitFetcherManagerTest {

    @Test
    public void testExceptionPropagationFirstFetch() throws Exception {
        testExceptionPropagation();
    }

    @Test
    public void testExceptionPropagationSuccessiveFetch() throws Exception {
        testExceptionPropagation(
                new TestingRecordsWithSplitIds<>("testSplit", 1, 2, 3, 4),
                new TestingRecordsWithSplitIds<>("testSplit", 5, 6, 7, 8));
    }

    @Test
    public void testCloseFetcherWithException() throws Exception {
        TestingSplitReader<Object, TestingSourceSplit> reader = new TestingSplitReader<>();
        reader.setCloseWithException();
        SplitFetcherManager<Object, TestingSourceSplit> fetcherManager =
                createFetcher("test-split", new FutureCompletingBlockingQueue<>(), reader);
        fetcherManager.close(1000L);
        try {
            fetcherManager.checkErrors();
        } catch (Exception e) {
            assertEquals(
                    "Artificial exception on closing the split reader.",
                    ExceptionUtils.getRootCause(e).getMessage());
        }
    }

    // the final modifier is important so that '@SafeVarargs' is accepted on Java 8
    @SuppressWarnings("FinalPrivateMethod")
    @SafeVarargs
    private final void testExceptionPropagation(
            final RecordsWithSplitIds<Integer>... fetchesBeforeError) throws Exception {
        final IOException testingException = new IOException("test");

        final FutureCompletingBlockingQueue<RecordsWithSplitIds<Integer>> queue =
                new FutureCompletingBlockingQueue<>(10);
        final AwaitingReader<Integer, TestingSourceSplit> reader =
                new AwaitingReader<>(testingException, fetchesBeforeError);
        final SplitFetcherManager<Integer, TestingSourceSplit> fetcher =
                createFetcher("testSplit", queue, reader);

        reader.awaitAllRecordsReturned();
        drainQueue(queue);

        assertFalse(queue.getAvailabilityFuture().isDone());
        reader.triggerThrowException();

        // await the error propagation
        queue.getAvailabilityFuture().get();

        try {
            fetcher.checkErrors();
            fail("expected exception");
        } catch (Exception e) {
            assertSame(testingException, e.getCause().getCause());
        } finally {
            fetcher.close(20_000L);
        }
    }

    // ------------------------------------------------------------------------
    //  test helpers
    // ------------------------------------------------------------------------

    private static <E> SplitFetcherManager<E, TestingSourceSplit> createFetcher(
            final String splitId,
            final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> queue,
            final SplitReader<E, TestingSourceSplit> reader) {

        final SingleThreadFetcherManager<E, TestingSourceSplit> fetcher =
                new SingleThreadFetcherManager<>(queue, () -> reader);
        fetcher.addSplits(Collections.singletonList(new TestingSourceSplit(splitId)));
        return fetcher;
    }

    private static void drainQueue(FutureCompletingBlockingQueue<?> queue) {
        //noinspection StatementWithEmptyBody
        while (queue.poll() != null) {}
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

        @SafeVarargs
        AwaitingReader(IOException testError, RecordsWithSplitIds<E>... fetches) {
            this.testError = testError;
            this.fetches = new ArrayDeque<>(Arrays.asList(fetches));
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
        public void close() throws Exception {}

        public void awaitAllRecordsReturned() throws InterruptedException {
            inBlocking.await();
        }

        public void triggerThrowException() {
            throwError.trigger();
        }
    }
}
