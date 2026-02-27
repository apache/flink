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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.apache.flink.shaded.curator5.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The async batch join runner to lookup the dimension table.
 *
 * <p>This runner implements async batch lookup join functionality that batches multiple lookup
 * requests together to reduce network overhead and improve throughput. It is particularly
 * beneficial for high-throughput scenarios with frequent dimension table lookups.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Batches multiple lookup requests to reduce network round-trips
 *   <li>Asynchronous processing to maintain low latency
 *   <li>Configurable batch size and flush interval
 *   <li>Object pooling to reduce garbage collection overhead
 * </ul>
 */
public class AsyncBatchLookupJoinRunner extends RichAsyncFunction<RowData, RowData> {
    private static final long serialVersionUID = -6664660022391632480L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBatchLookupJoinRunner.class);

    private final GeneratedFunction<AsyncFunction<List<RowData>, Object>> generatedFetcher;
    private final DataStructureConverter<RowData, Object> fetcherConverter;
    private final GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>>
            generatedResultFuture;
    private final boolean isLeftOuterJoin;
    private final int asyncBufferCapacity;

    private final int batchSize;
    private final long flushIntervalMillis;

    private final List<RowData> rowDatas = new ArrayList<>();
    private final List<ResultFuture<RowData>> resultFutures = new ArrayList<>();

    private transient ScheduledExecutorService executorService;

    private transient AsyncFunction<List<RowData>, Object> fetcher;

    protected final RowDataSerializer rightRowSerializer;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<BatchJoinedRowResultFuture> resultFutureBuffer;

    /**
     * A Collection contains all ResultFutures in the runner which is used to invoke {@code close()}
     * on every ResultFuture. {@link #resultFutureBuffer} may not contain all the ResultFutures
     * because ResultFutures will be polled from the buffer when processing.
     */
    private transient List<BatchJoinedRowResultFuture> allResultFutures;

    /**
     * Constructor for AsyncBatchLookupJoinRunner.
     *
     * @param generatedFetcher Generated function for async lookup operations
     * @param fetcherConverter Converter for data structure conversion
     * @param generatedResultFuture Generated result future for handling results
     * @param rightRowSerializer Serializer for right side rows
     * @param isLeftOuterJoin Whether this is a left outer join
     * @param asyncBufferCapacity Capacity of the async buffer
     * @param batchSize Size of each batch for lookup requests
     * @param flushIntervalMillis Flush interval in milliseconds
     */
    public AsyncBatchLookupJoinRunner(
            GeneratedFunction<AsyncFunction<List<RowData>, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>> generatedResultFuture,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity,
            int batchSize,
            long flushIntervalMillis) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
        this.generatedResultFuture = generatedResultFuture;
        this.rightRowSerializer = rightRowSerializer;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.batchSize = batchSize;
        this.flushIntervalMillis = flushIntervalMillis;
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
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            BatchJoinedRowResultFuture rf =
                    new BatchJoinedRowResultFuture(
                            resultFutureBuffer,
                            createFetcherResultFuture(openContext),
                            fetcherConverter,
                            isLeftOuterJoin,
                            rightRowSerializer.getArity());
            // add will throw exception immediately if the queue is full which should never happen
            resultFutureBuffer.add(rf);
            allResultFutures.add(rf);
        }
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("batch-async-table-function-flush-thread")
                        .setDaemon(true)
                        .build();

        executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

        executorService.scheduleWithFixedDelay(
                this::flush, 1000, flushIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Asynchronously processes a single input record by adding it to the current batch.
     *
     * <p>When the batch size is reached, the batch is automatically flushed. Otherwise, the record
     * waits for more records or the flush timer to trigger.
     *
     * @param input The input row data
     * @param resultFuture The result future to complete when lookup is done
     */
    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        synchronized (this.rowDatas) {
            rowDatas.add(input);
            resultFutures.add(resultFuture);

            LOG.debug("current row size: {}", this.rowDatas.size());
            if (this.rowDatas.size() >= batchSize) {
                LOG.debug("Exceeding the batch size, flush now");
                flush();
            }
        }
    }

    /**
     * Flushes the current batch of lookup requests.
     *
     * <p>This method is called either when the batch size is reached or when the flush timer
     * expires. It processes all accumulated requests in a single batch operation to the dimension
     * table.
     */
    private void flush() {
        synchronized (this.rowDatas) {
            if (this.rowDatas.size() > 0) {
                List<RowData> tempRowDatas = new ArrayList<>(rowDatas);
                List<ResultFuture<RowData>> tempResultFutures = new ArrayList<>(resultFutures);
                try {
                    BatchJoinedRowResultFuture outResultFuture = resultFutureBuffer.take();
                    // the input row is copied when object reuse in AsyncWaitOperator
                    outResultFuture.reset(tempRowDatas, tempResultFutures);
                    // fetcher has copied the input field when object reuse is enabled
                    fetcher.asyncInvoke(tempRowDatas, outResultFuture);
                } catch (Exception e) {
                    throw new RuntimeException("Invoking method 'async invoke' failed", e);
                }

                this.rowDatas.clear();
                this.resultFutures.clear();
            }
        }
    }

    public TableFunctionResultFuture<List<RowData>> createFetcherResultFuture(
            OpenContext openContext) throws Exception {
        TableFunctionResultFuture<List<RowData>> resultFuture =
                generatedResultFuture.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(resultFuture, getRuntimeContext());
        FunctionUtils.openFunction(resultFuture, openContext);
        return resultFuture;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (executorService != null) {
            executorService.shutdown();
        }
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (allResultFutures != null) {
            for (BatchJoinedRowResultFuture rf : allResultFutures) {
                rf.close();
            }
        }
    }

    @VisibleForTesting
    public List<BatchJoinedRowResultFuture> getAllResultFutures() {
        return allResultFutures;
    }

    /**
     * The {@link BatchJoinedRowResultFuture} is used to combine left {@link RowData} and right
     * {@link RowData} into {@link JoinedRowData}.
     *
     * <p>There are 3 phases in this collector.
     *
     * <ol>
     *   <li>accept lookup function return result and convert it into RowData, call it right result
     *   <li>project & filter the right result if there is a calc on the temporal table, see {@link
     *       AsyncLookupJoinWithCalcRunner#createFetcherResultFuture(Configuration)}
     *   <li>filter the result if a join condition exist, see {@link
     *       AsyncLookupJoinRunner#createFetcherResultFuture(Configuration)}
     *   <li>combine left input and the right result into a JoinedRowData, call it join result
     * </ol>
     *
     * <p>TODO: code generate a whole JoinedRowResultFuture in the future
     */
    private static final class BatchJoinedRowResultFuture implements ResultFuture<Object> {

        private final BlockingQueue<BatchJoinedRowResultFuture> resultFutureBuffer;
        private final TableFunctionResultFuture<List<RowData>> joinConditionResultFuture;
        private final DataStructureConverter<RowData, Object> resultConverter;
        private final boolean isLeftOuterJoin;

        private final BatchJoinedRowResultFuture.DelegateResultFuture delegate;
        private final GenericRowData nullRow;

        private List<RowData> leftRows;
        private List<ResultFuture<RowData>> realOutputs;

        private Integer currentIndex = 0;

        private BatchJoinedRowResultFuture(
                BlockingQueue<BatchJoinedRowResultFuture> resultFutureBuffer,
                TableFunctionResultFuture<List<RowData>> joinConditionResultFuture,
                DataStructureConverter<RowData, Object> resultConverter,
                boolean isLeftOuterJoin,
                int rightArity) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.joinConditionResultFuture = joinConditionResultFuture;
            this.resultConverter = resultConverter;
            this.isLeftOuterJoin = isLeftOuterJoin;
            this.delegate = new BatchJoinedRowResultFuture.DelegateResultFuture();
            this.nullRow = new GenericRowData(rightArity);
        }

        public void reset(List<RowData> rowDatas, List<ResultFuture<RowData>> resultFutures) {
            currentIndex = 0;
            this.leftRows = rowDatas;
            this.realOutputs = resultFutures;
            joinConditionResultFuture.setInput(rowDatas);
            joinConditionResultFuture.setResultFuture(delegate);
            delegate.reset();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void complete(Collection<Object> result) {
            try {
                Collection<List<RowData>> rowDataCollection = new ArrayList<>(result.size());
                for (Object element : result) {
                    if (element == null) {
                        rowDataCollection.add(Collections.emptyList());
                    } else {
                        rowDataCollection.add(
                                Collections.singletonList(resultConverter.toInternal(element)));
                    }
                }

                // call condition collector first,
                // the filtered result will be routed to the delegateCollector
                try {
                    joinConditionResultFuture.complete(rowDataCollection);
                } catch (Throwable t) {
                    // we should catch the exception here to let the framework know
                    completeExceptionally(t);
                    return;
                }

                Collection<List<RowData>> rightRowList = delegate.collection;

                for (List<RowData> rightRows : rightRowList) {
                    if (rightRows.isEmpty()) {
                        if (isLeftOuterJoin) {
                            RowData leftRow = leftRows.get(currentIndex);
                            RowData outRow =
                                    new JoinedRowData(leftRow.getRowKind(), leftRow, nullRow);
                            realOutputs
                                    .get(currentIndex)
                                    .complete(Collections.singletonList(outRow));
                        } else {
                            realOutputs.get(currentIndex).complete(Collections.emptyList());
                        }
                    } else {
                        RowData outRow =
                                new JoinedRowData(
                                        leftRows.get(currentIndex).getRowKind(),
                                        leftRows.get(currentIndex),
                                        rightRows.get(0));
                        realOutputs.get(currentIndex).complete(Collections.singletonList(outRow));
                    }
                    currentIndex++;
                }

                try {
                    // When resultFuture completes,
                    // there is no need to retain the batch data to avoid memory occupation.
                    reset(null, null);
                    // put this collector to the queue to avoid this collector is used
                    // again before outRows in the collector is not consumed.
                    resultFutureBuffer.put(this);
                } catch (InterruptedException e) {
                    completeExceptionally(e);
                }
            } catch (Throwable t) {
                LOG.error("Error while processing async lookup join result.", t);
            }
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            try {
                Collection<Object> result = supplier.get();
                complete(result);
            } catch (Throwable t) {
                completeExceptionally(t);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (realOutputs != null && currentIndex < realOutputs.size()) {
                realOutputs.get(currentIndex).completeExceptionally(error);
            }
        }

        public void close() throws Exception {
            joinConditionResultFuture.close();
        }

        private final class DelegateResultFuture implements ResultFuture<List<RowData>> {

            private Collection<List<RowData>> collection;

            public void reset() {
                this.collection = null;
            }

            @Override
            public void complete(Collection<List<RowData>> result) {
                this.collection = result;
            }

            @Override
            public void completeExceptionally(Throwable error) {
                BatchJoinedRowResultFuture.this.completeExceptionally(error);
            }

            @Override
            public void complete(CollectionSupplier<List<RowData>> supplier) {
                try {
                    Collection<List<RowData>> result = supplier.get();
                    complete(result);
                } catch (Throwable t) {
                    completeExceptionally(t);
                }
            }
        }
    }
}
