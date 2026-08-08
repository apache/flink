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

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResultStore}. */
class ResultStoreTest {

    @Test
    @Timeout(10)
    void testInterruptHandling() throws Exception {
        // Create a blocking iterator that will wait until signaled
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch interruptedLatch = new CountDownLatch(1);
        AtomicBoolean wasInterrupted = new AtomicBoolean(false);

        CloseableIterator<RowData> blockingIterator =
                new CloseableIterator<RowData>() {
                    private boolean hasReturned = false;

                    @Override
                    public boolean hasNext() {
                        if (!hasReturned) {
                            return true;
                        }
                        // Block until interrupted
                        try {
                            blockLatch.await();
                        } catch (InterruptedException e) {
                            wasInterrupted.set(true);
                            Thread.currentThread().interrupt();
                        }
                        return false;
                    }

                    @Override
                    public RowData next() {
                        if (!hasReturned) {
                            hasReturned = true;
                            return GenericRowData.of(StringData.fromString("test"));
                        }
                        throw new NoSuchElementException();
                    }

                    @Override
                    public void close() throws Exception {
                        blockLatch.countDown();
                    }
                };

        // Create ResultStore with small buffer to trigger wait
        ResultStore resultStore = new ResultStore(blockingIterator, 1);

        // Give the retrieval thread time to start
        Thread.sleep(100);

        // Close the result store, which should interrupt the retrieval thread
        resultStore.close();

        // Wait a bit for interrupt to propagate
        interruptedLatch.await(5, TimeUnit.SECONDS);

        // Verify the thread is no longer running
        assertThat(resultStore.isRetrieving()).isFalse();
    }

    @Test
    @Timeout(10)
    void testProcessRecordWithFullBuffer() throws Exception {
        // Create an iterator with multiple rows
        List<RowData> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(GenericRowData.of(StringData.fromString("row" + i)));
        }

        CloseableIterator<RowData> iterator = CloseableIterator.adapterForIterator(rows.iterator());

        // Create ResultStore with buffer size 5
        ResultStore resultStore = new ResultStore(iterator, 5);

        // Wait for some records to be buffered
        Thread.sleep(200);

        // Verify records can be retrieved
        int totalRetrieved = 0;
        while (resultStore.isRetrieving() || resultStore.getBufferedRecordSize() > 0) {
            resultStore.retrieveRecords().ifPresent(records -> {});
            totalRetrieved++;
            Thread.sleep(50);

            // Prevent infinite loop
            if (totalRetrieved > 20) {
                break;
            }
        }

        resultStore.close();
    }

    @Test
    @Timeout(10)
    void testInterruptDuringBufferWait() throws Exception {
        // Create a slow producer that generates rows continuously
        CloseableIterator<RowData> slowIterator =
                new CloseableIterator<RowData>() {
                    private int count = 0;

                    @Override
                    public boolean hasNext() {
                        return count < 100;
                    }

                    @Override
                    public RowData next() {
                        count++;
                        return GenericRowData.of(StringData.fromString("row" + count));
                    }

                    @Override
                    public void close() throws Exception {}
                };

        // Create ResultStore with very small buffer (1) to force waiting
        ResultStore resultStore = new ResultStore(slowIterator, 1);

        // Let the buffer fill up
        Thread.sleep(100);

        // Now close, which should interrupt the waiting thread
        resultStore.close();

        // Verify cleanup completed quickly (thread responded to interrupt)
        Thread.sleep(200);
        assertThat(resultStore.isRetrieving()).isFalse();
    }

    @Test
    @Timeout(10)
    void testGracefulShutdown() throws Exception {
        // Create simple iterator
        List<RowData> rows = new ArrayList<>();
        rows.add(GenericRowData.of(StringData.fromString("test1")));
        rows.add(GenericRowData.of(StringData.fromString("test2")));

        CloseableIterator<RowData> iterator = CloseableIterator.adapterForIterator(rows.iterator());

        ResultStore resultStore = new ResultStore(iterator, 10);

        // Wait for processing to complete
        Thread.sleep(100);

        // Close should complete quickly
        long startTime = System.currentTimeMillis();
        resultStore.close();
        long duration = System.currentTimeMillis() - startTime;

        // Should complete in well under 1 second
        assertThat(duration).isLessThan(1000);
        assertThat(resultStore.isRetrieving()).isFalse();
    }
}
