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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** The async join runner to look up one or multi dimension tables in delta join. */
public class AsyncDeltaJoinRunner extends RichAsyncFunction<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDeltaJoinRunner.class);

    private static final long serialVersionUID = 1L;
    private static final String METRIC_DELTA_JOIN_LEFT_CALL_ASYNC_FETCH_COST_TIME =
            "deltaJoinLeftCallAsyncFetchCostTime";
    private static final String METRIC_DELTA_JOIN_RIGHT_CALL_ASYNC_FETCH_COST_TIME =
            "deltaJoinRightCallAsyncFetchCostTime";

    /** Selector to get join key from left input. */
    private final RowDataKeySelector leftJoinKeySelector;

    /** Selector to get upsert key from left input. */
    private final RowDataKeySelector leftUpsertKeySelector;

    /** Selector to get join key from right input. */
    private final RowDataKeySelector rightJoinKeySelector;

    /** Selector to get upsert key from right input. */
    private final RowDataKeySelector rightUpsertKeySelector;

    private final int[] eachBinaryInputFieldSize;
    private final @Nullable GeneratedFilterCondition generatedRemainingJoinCondition;
    private final DeltaJoinHandlerChain handlerChainTemplate;
    private final DeltaJoinRuntimeTree joinTreeTemplate;
    private final Set<Set<Integer>> allDrivenInputsWhenLookup;

    private final boolean lookupRight;
    private final int asyncBufferCapacity;
    private final boolean enableCache;

    private transient DeltaJoinCache cache;

    /**
     * Buffers {@link DeltaJoinProcessor} to avoid newInstance cost when processing elements every
     * time. We use {@link BlockingQueue} to make sure the head {@link DeltaJoinProcessor}s are
     * available.
     */
    private transient BlockingQueue<DeltaJoinProcessor> processorBuffer;

    /**
     * A Collection contains all DeltaJoinProcessors in the runner which is used to invoke {@code
     * close()} on every {@link DeltaJoinProcessor}. {@link #processorBuffer} may not contain all
     * the DeltaJoinProcessors because DeltaJoinProcessors will be polled from the buffer when
     * processing.
     */
    private transient List<DeltaJoinProcessor> allProcessors;

    // metrics
    private transient long callAsyncFetchCostTime = 0L;

    public AsyncDeltaJoinRunner(
            int[] eachBinaryInputFieldSize,
            @Nullable GeneratedFilterCondition generatedRemainingJoinCondition,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector leftUpsertKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            RowDataKeySelector rightUpsertKeySelector,
            DeltaJoinHandlerChain handlerChainTemplate,
            DeltaJoinRuntimeTree joinTreeTemplate,
            Set<Set<Integer>> allDrivenInputsWhenLookup,
            boolean lookupRight,
            int asyncBufferCapacity,
            boolean enableCache) {
        this.eachBinaryInputFieldSize = eachBinaryInputFieldSize;
        this.generatedRemainingJoinCondition = generatedRemainingJoinCondition;

        this.leftJoinKeySelector = leftJoinKeySelector;
        this.leftUpsertKeySelector = leftUpsertKeySelector;
        this.rightJoinKeySelector = rightJoinKeySelector;
        this.rightUpsertKeySelector = rightUpsertKeySelector;

        this.handlerChainTemplate = handlerChainTemplate;
        this.joinTreeTemplate = joinTreeTemplate;
        this.allDrivenInputsWhenLookup = allDrivenInputsWhenLookup;

        this.asyncBufferCapacity = asyncBufferCapacity;
        this.lookupRight = lookupRight;
        this.enableCache = enableCache;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        DeltaJoinOpenContext deltaJoinOpenContext = (DeltaJoinOpenContext) openContext;
        this.cache = deltaJoinOpenContext.getCache();

        RuntimeContext runtimeContext = getRuntimeContext();
        ClassLoader cl = runtimeContext.getUserCodeClassLoader();

        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        this.processorBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        this.allProcessors = new ArrayList<>();
        LOG.info("Begin to initialize reusable processors with size {}", asyncBufferCapacity + 1);
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            DeltaJoinRuntimeTree joinRuntimeTree = joinTreeTemplate.copy();
            MultiInputRowDataBuffer multiInputRowDataBuffer =
                    new MultiInputRowDataBuffer(
                            eachBinaryInputFieldSize, joinRuntimeTree, allDrivenInputsWhenLookup);
            DeltaJoinHandlerChain chain = handlerChainTemplate.copy();

            FilterCondition remainingJoinCondition =
                    Optional.ofNullable(this.generatedRemainingJoinCondition)
                            .map(c -> c.newInstance(cl))
                            .orElse(null);

            DeltaJoinProcessor processor =
                    new DeltaJoinProcessor(
                            processorBuffer,
                            multiInputRowDataBuffer,
                            chain,
                            remainingJoinCondition,
                            enableCache,
                            cache,
                            lookupRight,
                            leftUpsertKeySelector.copy(),
                            rightUpsertKeySelector.copy(),
                            runtimeContext);

            processor.open(deltaJoinOpenContext);

            // add will throw exception immediately if the queue is full which should never happen
            processorBuffer.add(processor);
            allProcessors.add(processor);
        }
        LOG.info("Finish initializing reusable processors");

        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        lookupRight
                                ? METRIC_DELTA_JOIN_LEFT_CALL_ASYNC_FETCH_COST_TIME
                                : METRIC_DELTA_JOIN_RIGHT_CALL_ASYNC_FETCH_COST_TIME,
                        () -> callAsyncFetchCostTime);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        DeltaJoinProcessor deltaJoinProcessor = processorBuffer.take();

        RowData streamJoinKey = null;
        if (enableCache) {
            if (lookupRight) {
                streamJoinKey = leftJoinKeySelector.getKey(input);
                cache.requestRightCache();
            } else {
                streamJoinKey = rightJoinKeySelector.getKey(input);
                cache.requestLeftCache();
            }
        }

        // the input row is copied when object reuse in StreamDeltaJoinOperator
        deltaJoinProcessor.reset(streamJoinKey, input, resultFuture);

        if (enableCache) {
            Optional<Collection<RowData>> dataFromCache = tryGetDataFromCache(streamJoinKey);
            if (dataFromCache.isPresent()) {
                deltaJoinProcessor.complete(dataFromCache.get());
                return;
            }
        }

        long startTime = System.currentTimeMillis();
        // fetcher has copied the input field when object reuse is enabled
        deltaJoinProcessor.asyncHandle(input);
        callAsyncFetchCostTime = System.currentTimeMillis() - startTime;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (allProcessors != null) {
            for (DeltaJoinProcessor processor : allProcessors) {
                processor.close();
            }
        }
    }

    @VisibleForTesting
    public DeltaJoinCache getCache() {
        return cache;
    }

    @VisibleForTesting
    public List<DeltaJoinProcessor> getAllProcessors() {
        return allProcessors;
    }

    private Optional<Collection<RowData>> tryGetDataFromCache(RowData joinKey) {
        Preconditions.checkState(enableCache);

        if (lookupRight) {
            LinkedHashMap<RowData, RowData> rightRows = cache.getData(joinKey, true);
            if (rightRows != null) {
                cache.hitRightCache();
                return Optional.of(rightRows.values());
            }
        } else {
            LinkedHashMap<RowData, RowData> leftRows = cache.getData(joinKey, false);
            if (leftRows != null) {
                cache.hitLeftCache();
                return Optional.of(leftRows.values());
            }
        }
        return Optional.empty();
    }

    /**
     * The processor to handle the inputs of the delta join runner.
     *
     * <p>It mainly does the following things:
     *
     * <ol>
     *   <li>Trigger to lookup by invoking {@link DeltaJoinHandlerChain#asyncHandle} and get all
     *       lookup results back.
     *   <li>Build and update the cache if necessary.
     *   <li>Do the final filter for each lookup result.
     * </ol>
     */
    @VisibleForTesting
    public static final class DeltaJoinProcessor implements ResultFuture<RowData> {

        private static final Logger LOG = LoggerFactory.getLogger(DeltaJoinProcessor.class);

        private final BlockingQueue<DeltaJoinProcessor> processorBuffer;

        private final DeltaJoinHandlerChain handlerChain;
        private final MultiInputRowDataBuffer multiInputRowDataBuffer;
        private final @Nullable FilterCondition remainingJoinCondition;

        private final boolean enableCache;
        private final DeltaJoinCache cache;
        private final boolean lookupRight;
        private final RuntimeContext runtimeContext;

        private final RowDataKeySelector leftUpsertKeySelector;
        private final RowDataKeySelector rightUpsertKeySelector;

        private @Nullable RowData streamJoinKey;
        private RowData streamRow;
        private ResultFuture<RowData> realOutput;

        private DeltaJoinProcessor(
                BlockingQueue<DeltaJoinProcessor> processorBuffer,
                MultiInputRowDataBuffer multiInputRowDataBuffer,
                DeltaJoinHandlerChain handlerChain,
                @Nullable FilterCondition remainingJoinCondition,
                boolean enableCache,
                DeltaJoinCache cache,
                boolean lookupRight,
                RowDataKeySelector leftUpsertKeySelector,
                RowDataKeySelector rightUpsertKeySelector,
                RuntimeContext runtimeContext) {
            this.processorBuffer = processorBuffer;
            this.multiInputRowDataBuffer = multiInputRowDataBuffer;
            this.handlerChain = handlerChain;
            this.remainingJoinCondition = remainingJoinCondition;
            this.enableCache = enableCache;
            this.cache = cache;
            this.lookupRight = lookupRight;
            this.leftUpsertKeySelector = leftUpsertKeySelector;
            this.rightUpsertKeySelector = rightUpsertKeySelector;
            this.runtimeContext = runtimeContext;
        }

        public void open(DeltaJoinOpenContext openContext) throws Exception {
            multiInputRowDataBuffer.open(openContext, runtimeContext);

            HandlerChainContextImpl context =
                    HandlerChainContextImpl.create(
                            multiInputRowDataBuffer,
                            this,
                            runtimeContext,
                            openContext.getMailboxExecutor(),
                            openContext.getLookupFunctions(),
                            lookupRight);
            handlerChain.open(openContext, context);

            if (remainingJoinCondition != null) {
                FunctionUtils.setFunctionRuntimeContext(remainingJoinCondition, runtimeContext);
                FunctionUtils.openFunction(remainingJoinCondition, openContext);
            }
        }

        public void close() throws Exception {
            multiInputRowDataBuffer.close();
            handlerChain.close();

            if (remainingJoinCondition != null) {
                remainingJoinCondition.close();
            }
        }

        public void reset(
                @Nullable RowData joinKey, RowData row, ResultFuture<RowData> realOutput) {
            Preconditions.checkState(
                    (enableCache && joinKey != null) || (!enableCache && joinKey == null));
            this.realOutput = realOutput;
            this.streamJoinKey = joinKey;
            this.streamRow = row;

            this.multiInputRowDataBuffer.reset();
            this.handlerChain.reset();
        }

        public void asyncHandle(RowData input) throws Exception {
            handlerChain.asyncHandle(input);
        }

        @Override
        public void complete(Collection<RowData> lookupRows) {
            if (lookupRows == null) {
                lookupRows = Collections.emptyList();
            }

            // now we have received the rows from the lookup tables,
            // try to set them to the cache
            try {
                updateCacheIfNecessary(lookupRows);
            } catch (Throwable t) {
                LOG.info("Failed to update the cache", t);
                completeExceptionally(t);
                return;
            }

            List<RowData> outRows;
            if (lookupRows.isEmpty()) {
                outRows = Collections.emptyList();
            } else {
                outRows = new ArrayList<>();
                for (RowData lookupRow : lookupRows) {
                    JoinedRowData outRow =
                            lookupRight
                                    ? new JoinedRowData(
                                            streamRow.getRowKind(), streamRow, lookupRow)
                                    : new JoinedRowData(
                                            streamRow.getRowKind(), lookupRow, streamRow);
                    if (remainingJoinCondition != null
                            && !remainingJoinCondition.apply(
                                    FilterCondition.Context.INVALID_CONTEXT, outRow)) {
                        continue;
                    }
                    outRows.add(outRow);
                }
            }

            realOutput.complete(outRows);
            try {
                // put this collector to the queue to avoid this collector is used
                // again before outRows in the collector is not consumed.
                processorBuffer.put(this);
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
        public void complete(CollectionSupplier<RowData> supplier) {
            throw new UnsupportedOperationException();
        }

        @VisibleForTesting
        public DeltaJoinHandlerChain getDeltaJoinHandlerChain() {
            return handlerChain;
        }

        @VisibleForTesting
        public MultiInputRowDataBuffer getMultiInputRowDataBuffer() {
            return multiInputRowDataBuffer;
        }

        private void updateCacheIfNecessary(Collection<RowData> lookupRows) throws Exception {
            if (!enableCache) {
                return;
            }

            // 1. build the cache in lookup side if not exists
            // 2. update the cache in stream side if exists
            if (lookupRight) {
                if (cache.getData(streamJoinKey, true) == null) {
                    cache.buildCache(streamJoinKey, buildMapWithUkAsKeys(lookupRows, true), true);
                }

                LinkedHashMap<RowData, RowData> leftCacheData = cache.getData(streamJoinKey, false);
                if (leftCacheData != null) {
                    RowData uk = leftUpsertKeySelector.getKey(streamRow);
                    cache.upsertCache(streamJoinKey, uk, streamRow, false);
                }
            } else {
                if (cache.getData(streamJoinKey, false) == null) {
                    cache.buildCache(streamJoinKey, buildMapWithUkAsKeys(lookupRows, false), false);
                }

                LinkedHashMap<RowData, RowData> rightCacheData = cache.getData(streamJoinKey, true);
                if (rightCacheData != null) {
                    RowData uk = rightUpsertKeySelector.getKey(streamRow);
                    cache.upsertCache(streamJoinKey, uk, streamRow, true);
                }
            }
        }

        private LinkedHashMap<RowData, RowData> buildMapWithUkAsKeys(
                Collection<RowData> lookupRows, boolean treatRightAsLookupTable) throws Exception {
            LinkedHashMap<RowData, RowData> map = new LinkedHashMap<>();
            for (RowData lookupRow : lookupRows) {
                RowData uk;
                if (treatRightAsLookupTable) {
                    uk = rightUpsertKeySelector.getKey(lookupRow);
                    map.put(uk, lookupRow);
                } else {
                    uk = leftUpsertKeySelector.getKey(lookupRow);
                    map.put(uk, lookupRow);
                }
            }
            return map;
        }
    }

    /**
     * A {@link DeltaJoinHandlerBase.DeltaJoinHandlerContext} that exposes some context for the
     * lookup chain.
     */
    private static class HandlerChainContextImpl
            implements DeltaJoinHandlerBase.DeltaJoinHandlerContext {

        private final MultiInputRowDataBuffer sharedMultiInputRowDataBuffer;
        private final ResultFuture<RowData> realOutput;
        private final RuntimeContext runtimeContext;
        private final MailboxExecutor mailboxExecutor;
        private final Map<Integer, AsyncFunction<RowData, Object>> lookupFunctions;
        private final boolean inLeft2RightLookupChain;

        private HandlerChainContextImpl(
                MultiInputRowDataBuffer sharedMultiInputRowDataBuffer,
                ResultFuture<RowData> realOutput,
                RuntimeContext runtimeContext,
                MailboxExecutor mailboxExecutor,
                Map<Integer, AsyncFunction<RowData, Object>> lookupFunctions,
                boolean inLeft2RightLookupChain) {
            this.sharedMultiInputRowDataBuffer = sharedMultiInputRowDataBuffer;
            this.realOutput = realOutput;
            this.runtimeContext = runtimeContext;
            this.mailboxExecutor = mailboxExecutor;
            this.lookupFunctions = lookupFunctions;
            this.inLeft2RightLookupChain = inLeft2RightLookupChain;
        }

        public static HandlerChainContextImpl create(
                MultiInputRowDataBuffer sharedMultiInputRowDataBuffer,
                ResultFuture<RowData> realOutput,
                RuntimeContext runtimeContext,
                MailboxExecutor mailboxExecutor,
                Map<Integer, AsyncFunction<RowData, Object>> lookupFunctions,
                boolean inLeft2RightLookupChain) {
            return new HandlerChainContextImpl(
                    sharedMultiInputRowDataBuffer,
                    realOutput,
                    runtimeContext,
                    mailboxExecutor,
                    lookupFunctions,
                    inLeft2RightLookupChain);
        }

        @Override
        public MultiInputRowDataBuffer getSharedMultiInputRowDataBuffer() {
            return sharedMultiInputRowDataBuffer;
        }

        @Override
        public ResultFuture<RowData> getRealOutputResultFuture() {
            return realOutput;
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return mailboxExecutor;
        }

        @Override
        public AsyncFunction<RowData, Object> getLookupFunction(int lookupOrdinal) {
            Preconditions.checkArgument(lookupFunctions.containsKey(lookupOrdinal));
            return lookupFunctions.get(lookupOrdinal);
        }

        @Override
        public boolean inLeft2RightLookupChain() {
            return inLeft2RightLookupChain;
        }
    }
}
