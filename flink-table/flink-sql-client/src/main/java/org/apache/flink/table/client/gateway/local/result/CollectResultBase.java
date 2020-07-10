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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.util.concurrent.atomic.AtomicReference;

/** A result that works through {@link TableResult#collect()}. */
public abstract class CollectResultBase implements DynamicResult {
    private final CloseableIterator<Row> result;
    private final ResultRetrievalThread retrievalThread;

    protected final Object resultLock;
    protected AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();

    public CollectResultBase(TableResult tableResult) {
        result = tableResult.collect();
        resultLock = new Object();
        retrievalThread = new ResultRetrievalThread();
        // start listener thread
        retrievalThread.start();
    }

    @Override
    public void close() throws Exception {
        retrievalThread.isRunning = false;
        retrievalThread.interrupt();
        result.close();
    }

    protected <T> TypedResult<T> handleMissingResult() {
        if (executionException.get() != null) {
            throw executionException.get();
        }

        // we assume that a bounded job finished
        return TypedResult.endOfStream();
    }

    // TODO change Tuple2<Boolean, Row> to Row
    protected abstract void processRecord(Tuple2<Boolean, Row> change);

    protected boolean isRetrieving() {
        return retrievalThread.isRunning;
    }

    // --------------------------------------------------------------------------------------------

    private class ResultRetrievalThread extends Thread {
        public volatile boolean isRunning = true;

        @Override
        public void run() {
            try {
                while (isRunning && result.hasNext()) {
                    Row change = result.next();
                    boolean isInsert =
                            change.getKind() == RowKind.INSERT
                                    || change.getKind() == RowKind.UPDATE_AFTER;
                    // Always set the RowKind to INSERT, so that we can compare rows correctly
                    // (RowKind will be ignored),
                    // just use the Boolean of Tuple2<Boolean, Row> to figure out whether it is
                    // insert or delete.
                    change.setKind(RowKind.INSERT);
                    processRecord(new Tuple2<>(isInsert, change));
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
