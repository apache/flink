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
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.CalcCollectionCollector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
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
    private final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>>
            lookupSideGeneratedCalc;
    private final GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture;
    private final int asyncBufferCapacity;

    private transient AsyncFunction<RowData, Object> fetcher;

    protected final RowDataSerializer lookupSideRowSerializer;

    private final boolean treatRightAsLookupTable;

    private final boolean enableCache;

    /** Selector to get join key from left input. */
    private final RowDataKeySelector leftJoinKeySelector;

    /** Selector to get upsert key from left input. */
    private final RowDataKeySelector leftUpsertKeySelector;

    /** Selector to get join key from right input. */
    private final RowDataKeySelector rightJoinKeySelector;

    /** Selector to get upsert key from right input. */
    private final RowDataKeySelector rightUpsertKeySelector;

    private transient DeltaJoinCache cache;

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
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            RowDataSerializer lookupSideRowSerializer,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector leftUpsertKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            RowDataKeySelector rightUpsertKeySelector,
            int asyncBufferCapacity,
            boolean treatRightAsLookupTable,
            boolean enableCache) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
        this.lookupSideGeneratedCalc = lookupSideGeneratedCalc;
        this.generatedResultFuture = generatedResultFuture;
        this.lookupSideRowSerializer = lookupSideRowSerializer;
        this.leftJoinKeySelector = leftJoinKeySelector;
        this.leftUpsertKeySelector = leftUpsertKeySelector;
        this.rightJoinKeySelector = rightJoinKeySelector;
        this.rightUpsertKeySelector = rightUpsertKeySelector;
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.treatRightAsLookupTable = treatRightAsLookupTable;
        this.enableCache = enableCache;
    }

    public void setCache(DeltaJoinCache cache) {
        this.cache = cache;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);

        // try to compile the generated Calc and ResultFuture, fail fast if the code is corrupt.
        if (lookupSideGeneratedCalc != null) {
            lookupSideGeneratedCalc.compile(getRuntimeContext().getUserCodeClassLoader());
        }
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
                            createCalcFunction(openContext),
                            createFetcherResultFuture(openContext),
                            fetcherConverter,
                            treatRightAsLookupTable,
                            leftUpsertKeySelector,
                            rightUpsertKeySelector,
                            lookupSideRowSerializer,
                            enableCache,
                            cache);
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

        RowData streamJoinKey = null;
        if (enableCache) {
            if (treatRightAsLookupTable) {
                streamJoinKey = leftJoinKeySelector.getKey(input);
                cache.requestRightCache();
            } else {
                streamJoinKey = rightJoinKeySelector.getKey(input);
                cache.requestLeftCache();
            }
        }

        // the input row is copied when object reuse in StreamDeltaJoinOperator
        outResultFuture.reset(streamJoinKey, input, resultFuture);

        if (enableCache) {
            Optional<Collection<Object>> dataFromCache = tryGetDataFromCache(streamJoinKey);
            if (dataFromCache.isPresent()) {
                outResultFuture.complete(dataFromCache.get(), true);
                return;
            }
        }

        long startTime = System.currentTimeMillis();
        // fetcher has copied the input field when object reuse is enabled
        fetcher.asyncInvoke(input, outResultFuture);
        callAsyncFetchCostTime = System.currentTimeMillis() - startTime;
    }

    @Nullable
    private FlatMapFunction<RowData, RowData> createCalcFunction(OpenContext openContext)
            throws Exception {
        FlatMapFunction<RowData, RowData> calc = null;
        if (lookupSideGeneratedCalc != null) {
            calc =
                    lookupSideGeneratedCalc.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(calc, getRuntimeContext());
            FunctionUtils.openFunction(calc, openContext);
        }
        return calc;
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

    @VisibleForTesting
    public DeltaJoinCache getCache() {
        return cache;
    }

    private Optional<Collection<Object>> tryGetDataFromCache(RowData joinKey) {
        Preconditions.checkState(enableCache);

        if (treatRightAsLookupTable) {
            LinkedHashMap<RowData, Object> rightRows = cache.getData(joinKey, true);
            if (rightRows != null) {
                cache.hitRightCache();
                return Optional.of(rightRows.values());
            }
        } else {
            LinkedHashMap<RowData, Object> leftRows = cache.getData(joinKey, false);
            if (leftRows != null) {
                cache.hitLeftCache();
                return Optional.of(leftRows.values());
            }
        }
        return Optional.empty();
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
        private final @Nullable FlatMapFunction<RowData, RowData> calcFunction;
        private final CalcCollectionCollector calcCollector;
        private final TableFunctionResultFuture<RowData> joinConditionResultFuture;
        private final DataStructureConverter<RowData, Object> resultConverter;

        private final boolean enableCache;
        private final DeltaJoinCache cache;

        private final DelegateResultFuture delegate;
        private final boolean treatRightAsLookupTable;

        private final RowDataKeySelector leftUpsertKeySelector;
        private final RowDataKeySelector rightUpsertKeySelector;

        private @Nullable RowData streamJoinKey;
        private RowData streamRow;
        private ResultFuture<RowData> realOutput;

        private JoinedRowResultFuture(
                BlockingQueue<JoinedRowResultFuture> resultFutureBuffer,
                @Nullable FlatMapFunction<RowData, RowData> calcFunction,
                TableFunctionResultFuture<RowData> joinConditionResultFuture,
                DataStructureConverter<RowData, Object> resultConverter,
                boolean treatRightAsLookupTable,
                RowDataKeySelector leftUpsertKeySelector,
                RowDataKeySelector rightUpsertKeySelector,
                RowDataSerializer lookupSideRowSerializer,
                boolean enableCache,
                DeltaJoinCache cache) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.calcFunction = calcFunction;
            this.calcCollector = new CalcCollectionCollector(lookupSideRowSerializer);
            this.joinConditionResultFuture = joinConditionResultFuture;
            this.resultConverter = resultConverter;
            this.enableCache = enableCache;
            this.cache = cache;
            this.delegate = new DelegateResultFuture();
            this.treatRightAsLookupTable = treatRightAsLookupTable;
            this.leftUpsertKeySelector = leftUpsertKeySelector;
            this.rightUpsertKeySelector = rightUpsertKeySelector;
        }

        public void reset(
                @Nullable RowData joinKey, RowData row, ResultFuture<RowData> realOutput) {
            Preconditions.checkState(
                    (enableCache && joinKey != null) || (!enableCache && joinKey == null));
            this.realOutput = realOutput;
            this.streamJoinKey = joinKey;
            this.streamRow = row;

            joinConditionResultFuture.setInput(row);
            joinConditionResultFuture.setResultFuture(delegate);
            delegate.reset();
            calcCollector.reset();
        }

        @Override
        public void complete(Collection<Object> result) {
            complete(result, false);
        }

        public void complete(Collection<Object> result, boolean fromCache) {
            if (result == null) {
                result = Collections.emptyList();
            }

            Collection<RowData> rowDataCollection = convertToInternalData(result);

            Collection<RowData> lookupRowsAfterCalc = rowDataCollection;
            if (!fromCache && calcFunction != null && rowDataCollection != null) {
                for (RowData row : rowDataCollection) {
                    try {
                        calcFunction.flatMap(row, calcCollector);
                    } catch (Exception e) {
                        completeExceptionally(e);
                    }
                }
                lookupRowsAfterCalc = calcCollector.getCollection();
            }

            // now we have received the rows from the lookup table, try to set them to the cache
            try {
                updateCacheIfNecessary(lookupRowsAfterCalc);
            } catch (Throwable t) {
                LOG.info("Failed to update the cache", t);
                completeExceptionally(t);
                return;
            }

            // call join condition collector,
            // the filtered result will be routed to the delegateCollector
            try {
                joinConditionResultFuture.complete(lookupRowsAfterCalc);
            } catch (Throwable t) {
                // we should catch the exception here to let the framework know
                completeExceptionally(t);
                return;
            }

            Collection<RowData> lookupRowsAfterJoin = delegate.collection;
            if (lookupRowsAfterJoin == null || lookupRowsAfterJoin.isEmpty()) {
                realOutput.complete(Collections.emptyList());
            } else {
                List<RowData> outRows = new ArrayList<>();
                for (RowData lookupRow : lookupRowsAfterJoin) {
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

        private void updateCacheIfNecessary(Collection<RowData> lookupRows) throws Exception {
            if (!enableCache) {
                return;
            }

            // 1. build the cache in lookup side if not exists
            // 2. update the cache in stream side if exists
            if (treatRightAsLookupTable) {
                if (cache.getData(streamJoinKey, true) == null) {
                    cache.buildCache(streamJoinKey, buildMapWithUkAsKeys(lookupRows, true), true);
                }

                LinkedHashMap<RowData, Object> leftCacheData = cache.getData(streamJoinKey, false);
                if (leftCacheData != null) {
                    RowData uk = leftUpsertKeySelector.getKey(streamRow);
                    cache.upsertCache(streamJoinKey, uk, streamRow, false);
                }
            } else {
                if (cache.getData(streamJoinKey, false) == null) {
                    cache.buildCache(streamJoinKey, buildMapWithUkAsKeys(lookupRows, false), false);
                }

                LinkedHashMap<RowData, Object> rightCacheData = cache.getData(streamJoinKey, true);
                if (rightCacheData != null) {
                    RowData uk = rightUpsertKeySelector.getKey(streamRow);
                    cache.upsertCache(streamJoinKey, uk, streamRow, true);
                }
            }
        }

        private LinkedHashMap<RowData, Object> buildMapWithUkAsKeys(
                Collection<RowData> lookupRows, boolean treatRightAsLookupTable) throws Exception {
            LinkedHashMap<RowData, Object> map = new LinkedHashMap<>();
            for (Object lookupRow : lookupRows) {
                RowData rowData = convertToInternalData(lookupRow);
                RowData uk;
                if (treatRightAsLookupTable) {
                    uk = rightUpsertKeySelector.getKey(rowData);
                    map.put(uk, lookupRow);
                } else {
                    uk = leftUpsertKeySelector.getKey(rowData);
                    map.put(uk, lookupRow);
                }
            }
            return map;
        }

        private RowData convertToInternalData(Object data) {
            if (resultConverter.isIdentityConversion()) {
                return (RowData) data;
            } else {
                return resultConverter.toInternal(data);
            }
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
