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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** A result store which stores and buffers results. */
public class ResultStore {

    private static final Logger LOG = LoggerFactory.getLogger(ResultStore.class);

    public static final ResultStore DUMMY_RESULT_STORE =
            new ResultStore(CloseableIterator.adapterForIterator(Collections.emptyIterator()), 0);

    static {
        DUMMY_RESULT_STORE.close();
    }

    private final CloseableIterator<RowData> result;
    private final List<RowData> recordsBuffer = new ArrayList<>();
    private final int maxBufferSize;

    private final Object resultLock = new Object();
    private final AtomicReference<SqlExecutionException> executionException =
            new AtomicReference<>();
    private final ResultRetrievalThread retrievalThread = new ResultRetrievalThread();

    public ResultStore(CloseableIterator<RowData> result, int maxBufferSize) {
        this.result = result;
        this.maxBufferSize = maxBufferSize;
        this.retrievalThread.start();
    }

    public void close() {
        retrievalThread.isRunning = false;
        retrievalThread.interrupt();

        try {
            result.close();
        } catch (Exception e) {
            LOG.error("Failed to close the ResultStore. Ignore the error.", e);
        }
    }

    public Optional<List<RowData>> retrieveRecords() {
        synchronized (resultLock) {
            // retrieval thread is alive return a record if available
            // but the program must not have failed
            if (isRetrieving() && executionException.get() == null) {
                if (recordsBuffer.isEmpty()) {
                    return Optional.of(Collections.emptyList());
                } else {
                    final List<RowData> change = new ArrayList<>(recordsBuffer);
                    recordsBuffer.clear();
                    resultLock.notifyAll();
                    return Optional.of(change);
                }
            }
            // retrieval thread is dead but there is still a record to be delivered
            else if (!isRetrieving() && !recordsBuffer.isEmpty()) {
                final List<RowData> change = new ArrayList<>(recordsBuffer);
                recordsBuffer.clear();
                return Optional.of(change);
            }
            // no results can be returned anymore
            else {
                return handleMissingResult();
            }
        }
    }

    public int getBufferedRecordSize() {
        synchronized (resultLock) {
            return recordsBuffer.size();
        }
    }

    public void waitUntilHasData() {
        synchronized (resultLock) {
            while (isRetrieving() && recordsBuffer.isEmpty()) {
                try {
                    resultLock.wait();
                } catch (InterruptedException e) {
                    throw new SqlExecutionException("Failed to wait the result is ready.", e);
                }
            }
        }
    }

    public boolean isRetrieving() {
        return retrievalThread.isRunning;
    }

    private Optional<List<RowData>> handleMissingResult() {
        if (executionException.get() != null) {
            throw executionException.get();
        }

        // we assume that a bounded job finished
        return Optional.empty();
    }

    private void processRecord(RowData row) {
        synchronized (resultLock) {
            // wait if the buffer is full
            if (recordsBuffer.size() >= maxBufferSize) {
                try {
                    resultLock.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            recordsBuffer.add(row);
            // Notify the consumer to consume
            resultLock.notifyAll();
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Thread to retrieve data from the {@link CloseableIterator} and process. */
    private class ResultRetrievalThread extends Thread {
        public volatile boolean isRunning = true;

        @Override
        public void run() {
            try {
                while (isRunning && result.hasNext()) {
                    processRecord(result.next());
                }
            } catch (RuntimeException e) {
                executionException.compareAndSet(
                        null, new SqlExecutionException("Error while retrieving result.", e));
            }

            // no result anymore
            // either the job is done or an error occurred
            isRunning = false;

            synchronized (resultLock) {
                resultLock.notify();
            }
        }
    }
}
