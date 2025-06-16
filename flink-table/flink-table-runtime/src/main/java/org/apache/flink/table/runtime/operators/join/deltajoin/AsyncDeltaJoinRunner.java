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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** The async join runner to look up the dimension table in delta join. */
public class AsyncDeltaJoinRunner extends RichAsyncFunction<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDeltaJoinRunner.class);

    private static final long serialVersionUID = 1L;
    private static final String METRIC_DELTA_JOIN_LEFT_CALL_ASYNC_FETCH_COST_TIME =
            "deltaJoinLeftCallAsyncFetchCostTime";
    private static final String METRIC_DELTA_JOIN_RIGHT_CALL_ASYNC_FETCH_COST_TIME =
            "deltaJoinRightCallAsyncFetchCostTime";
    private final GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher;
    private final DataStructureConverter<RowData, Object> fetcherConverter;
    private final GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture;
    private final int asyncBufferCapacity;

    private transient AsyncFunction<RowData, Object> fetcher;

    protected final RowDataSerializer lookupSideRowSerializer;

    private final boolean treatRightAsLookupTable;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;

    /**
     * A Collection contains all ResultFutures in the runner which is used to invoke {@code close()}
     * on every ResultFuture. {@link #resultFutureBuffer} may not contain all the ResultFutures
     * because ResultFutures will be polled from the buffer when processing.
     */
    private transient List<JoinedRowResultFuture> allResultFutures;

    // metrics
    private transient long callAsyncFetchCostTime = 0L;

    public AsyncDeltaJoinRunner(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            RowDataSerializer lookupSideRowSerializer,
            int asyncBufferCapacity,
            boolean treatRightAsLookupTable) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
        this.generatedResultFuture = generatedResultFuture;
        this.lookupSideRowSerializer = lookupSideRowSerializer;
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.treatRightAsLookupTable = treatRightAsLookupTable;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);

        // try to compile the generated ResultFuture, fail fast if the code is corrupt.
        generatedResultFuture.compile(getRuntimeContext().getUserCodeClassLoader());

        fetcherConverter.open(getRuntimeContext().getUserCodeClassLoader());

        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        this.allResultFutures = new ArrayList<>();
        LOG.info(
                "Begin to initialize reusable result futures with size {}",
                asyncBufferCapacity + 1);
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            JoinedRowResultFuture rf =
                    new JoinedRowResultFuture(
                            resultFutureBuffer,
                            createFetcherResultFuture(openContext),
                            fetcherConverter,
                            treatRightAsLookupTable);
            // add will throw exception immediately if the queue is full which should never happen
            resultFutureBuffer.add(rf);
            allResultFutures.add(rf);
        }
        LOG.info("Finish initializing reusable result futures");

        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        treatRightAsLookupTable
                                ? METRIC_DELTA_JOIN_LEFT_CALL_ASYNC_FETCH_COST_TIME
                                : METRIC_DELTA_JOIN_RIGHT_CALL_ASYNC_FETCH_COST_TIME,
                        () -> callAsyncFetchCostTime);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        JoinedRowResultFuture outResultFuture = resultFutureBuffer.take();

        // the input row is copied when object reuse in StreamDeltaJoinOperator
        outResultFuture.reset(input, resultFuture);

        long startTime = System.currentTimeMillis();
        // fetcher has copied the input field when object reuse is enabled
        fetcher.asyncInvoke(input, outResultFuture);
        callAsyncFetchCostTime = System.currentTimeMillis() - startTime;
    }

    public TableFunctionResultFuture<RowData> createFetcherResultFuture(OpenContext openContext)
            throws Exception {
        TableFunctionResultFuture<RowData> resultFuture =
                generatedResultFuture.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(resultFuture, getRuntimeContext());
        FunctionUtils.openFunction(resultFuture, openContext);
        return resultFuture;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (allResultFutures != null) {
            for (JoinedRowResultFuture rf : allResultFutures) {
                rf.close();
            }
        }
    }

    @VisibleForTesting
    public AsyncFunction<RowData, Object> getFetcher() {
        return fetcher;
    }

    /**
     * The {@link JoinedRowResultFuture} is used to combine left {@link RowData} and right {@link
     * RowData} into {@link JoinedRowData}.
     *
     * <p>There are 3 phases in this collector similar with {@see
     * AsyncLookupJoinRunner#JoinedRowResultFuture}. Furthermore, this {@link JoinedRowResultFuture}
     * also handles logic about bidirectional lookup join processing.
     */
    @VisibleForTesting
    public static final class JoinedRowResultFuture implements ResultFuture<Object> {
        private final BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
        private final TableFunctionResultFuture<RowData> joinConditionResultFuture;
        private final DataStructureConverter<RowData, Object> resultConverter;

        private final DelegateResultFuture delegate;
        private final boolean treatRightAsLookupTable;

        private RowData streamRow;
        private ResultFuture<RowData> realOutput;

        private JoinedRowResultFuture(
                BlockingQueue<JoinedRowResultFuture> resultFutureBuffer,
                TableFunctionResultFuture<RowData> joinConditionResultFuture,
                DataStructureConverter<RowData, Object> resultConverter,
                boolean treatRightAsLookupTable) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.joinConditionResultFuture = joinConditionResultFuture;
            this.resultConverter = resultConverter;
            this.delegate = new DelegateResultFuture();
            this.treatRightAsLookupTable = treatRightAsLookupTable;
        }

        public void reset(RowData row, ResultFuture<RowData> realOutput) {
            this.realOutput = realOutput;
            this.streamRow = row;
            joinConditionResultFuture.setInput(row);
            joinConditionResultFuture.setResultFuture(delegate);
            delegate.reset();
        }

        @Override
        public void complete(Collection<Object> result) {
            Collection<RowData> rowDataCollection = convertToInternalData(result);
            // call condition collector first,
            // the filtered result will be routed to the delegateCollector
            try {
                joinConditionResultFuture.complete(rowDataCollection);
            } catch (Throwable t) {
                // we should catch the exception here to let the framework know
                completeExceptionally(t);
                return;
            }

            Collection<RowData> lookupRows = delegate.collection;
            if (lookupRows == null || lookupRows.isEmpty()) {
                realOutput.complete(Collections.emptyList());
            } else {
                List<RowData> outRows = new ArrayList<>();
                for (RowData lookupRow : lookupRows) {
                    RowData outRow;
                    if (treatRightAsLookupTable) {
                        outRow = new JoinedRowData(streamRow.getRowKind(), streamRow, lookupRow);
                    } else {
                        outRow = new JoinedRowData(streamRow.getRowKind(), lookupRow, streamRow);
                    }
                    outRows.add(outRow);
                }
                realOutput.complete(outRows);
            }
            try {
                // put this collector to the queue to avoid this collector is used
                // again before outRows in the collector is not consumed.
                resultFutureBuffer.put(this);
            } catch (InterruptedException e) {
                completeExceptionally(e);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            realOutput.completeExceptionally(error);
        }

        /**
         * Unsupported, because the containing classes are AsyncFunctions which don't have access to
         * the mailbox to invoke from the caller thread.
         */
        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            throw new UnsupportedOperationException();
        }

        public void close() throws Exception {
            joinConditionResultFuture.close();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Collection<RowData> convertToInternalData(Collection<Object> data) {
            if (resultConverter.isIdentityConversion()) {
                return (Collection) data;
            } else {
                Collection<RowData> result = new ArrayList<>(data.size());
                for (Object element : data) {
                    result.add(resultConverter.toInternal(element));
                }
                return result;
            }
        }

        private final class DelegateResultFuture implements ResultFuture<RowData> {

            private Collection<RowData> collection;

            public void reset() {
                this.collection = null;
            }

            @Override
            public void complete(Collection<RowData> result) {
                this.collection = result;
            }

            @Override
            public void completeExceptionally(Throwable error) {
                JoinedRowResultFuture.this.completeExceptionally(error);
            }

            /**
             * Unsupported, because the containing classes are AsyncFunctions which don't have
             * access to the mailbox to invoke from the caller thread.
             */
            @Override
            public void complete(CollectionSupplier<RowData> supplier) {
                throw new UnsupportedOperationException();
            }
        }
    }
}
