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

package org.apache.flink.table.client.gateway.result;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import java.util.concurrent.atomic.AtomicReference;

/** A result that works through {@link TableResult#collect()}. */
public abstract class CollectResultBase implements DynamicResult {
    private final CloseableIterator<RowData> resultIterator;

    protected final Object resultLock;
    protected AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();
    protected final ResultRetrievalThread retrievalThread;

    public CollectResultBase(StatementResult tableResult) {
        this.resultIterator = tableResult;
        this.resultLock = new Object();
        this.retrievalThread = new ResultRetrievalThread();
    }

    @Override
    public void close() {
        retrievalThread.isRunning = false;
        retrievalThread.interrupt();
        try {
            // cancel the job if it is not terminated
            resultIterator.close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected <T> TypedResult<T> handleMissingResult() {
        if (executionException.get() != null) {
            throw executionException.get();
        }

        // we assume that a bounded job finished
        return TypedResult.endOfStream();
    }

    protected abstract void processRecord(RowData row);

    protected boolean isRetrieving() {
        return retrievalThread.isRunning;
    }

    // --------------------------------------------------------------------------------------------

    /** Thread to retrieve data from the {@link CloseableIterator} and process. */
    protected class ResultRetrievalThread extends Thread {
        public volatile boolean isRunning = true;

        @Override
        public void run() {
            try {
                while (isRunning && resultIterator.hasNext()) {
                    processRecord(resultIterator.next());
                }
            } catch (RuntimeException e) {
                executionException.compareAndSet(
                        null, new SqlExecutionException("Error while retrieving result.", e));
            }

            // no result anymore
            // either the job is done or an error occurred
            isRunning = false;
        }
    }
}
