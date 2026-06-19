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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** A base class used to test {@link StreamingDeltaJoinOperator}. */
public class StreamingDeltaJoinOperatorTestBase {

    protected static final int AEC_CAPACITY = 100;
    protected static final int CACHE_SIZE = 10;

    protected KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createDeltaJoinOperatorTestHarness(
                    int[] eachBinaryInputFieldSize,
                    DeltaJoinHandlerChain left2RightHandlerChain,
                    DeltaJoinHandlerChain right2LeftHandlerChain,
                    @Nullable GeneratedFilterCondition remainingJoinCondition,
                    DeltaJoinRuntimeTree joinRuntimeTree,
                    Set<Set<Integer>> left2RightDrivenSideInfo,
                    Set<Set<Integer>> right2LeftDrivenSideInfo,
                    RowDataKeySelector leftJoinKeySelector,
                    RowDataKeySelector leftUpsertKeySelector,
                    RowDataKeySelector rightJoinKeySelector,
                    RowDataKeySelector rightUpsertKeySelector,
                    Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> lookupFunctions,
                    InternalTypeInfo<RowData> leftTypeInfo,
                    InternalTypeInfo<RowData> rightTypeInfo,
                    boolean enableCache)
                    throws Exception {
        TaskMailbox mailbox = new TaskMailboxImpl();
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(controller -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);

        AsyncDeltaJoinRunner left2RightRunner =
                new AsyncDeltaJoinRunner(
                        eachBinaryInputFieldSize,
                        remainingJoinCondition,
                        leftJoinKeySelector,
                        leftUpsertKeySelector,
                        rightJoinKeySelector,
                        rightUpsertKeySelector,
                        left2RightHandlerChain,
                        joinRuntimeTree,
                        left2RightDrivenSideInfo,
                        true,
                        AEC_CAPACITY,
                        enableCache);

        AsyncDeltaJoinRunner right2LeftRunner =
                new AsyncDeltaJoinRunner(
                        eachBinaryInputFieldSize,
                        remainingJoinCondition,
                        leftJoinKeySelector,
                        leftUpsertKeySelector,
                        rightJoinKeySelector,
                        rightUpsertKeySelector,
                        right2LeftHandlerChain,
                        joinRuntimeTree,
                        right2LeftDrivenSideInfo,
                        false,
                        AEC_CAPACITY,
                        enableCache);

        InternalTypeInfo<RowData> joinKeyTypeInfo = leftJoinKeySelector.getProducedType();

        StreamingDeltaJoinOperator operator =
                new StreamingDeltaJoinOperator(
                        left2RightRunner,
                        right2LeftRunner,
                        lookupFunctions,
                        leftJoinKeySelector,
                        rightJoinKeySelector,
                        -1L,
                        AEC_CAPACITY,
                        new TestProcessingTimeService(),
                        new MailboxExecutorImpl(
                                mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, mailboxProcessor),
                        CACHE_SIZE,
                        CACHE_SIZE,
                        (RowType) leftTypeInfo.toLogicalType(),
                        (RowType) rightTypeInfo.toLogicalType());

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                leftJoinKeySelector,
                rightJoinKeySelector,
                joinKeyTypeInfo,
                1,
                1,
                0,
                leftTypeInfo.toSerializer(),
                rightTypeInfo.toSerializer());
    }

    protected RowDataHarnessAssertor createAssertor(RowType outputRowType) {
        final int[] outputFieldIndices =
                IntStream.range(0, outputRowType.getFieldCount()).toArray();
        return new RowDataHarnessAssertor(
                outputRowType.getChildren().toArray(new LogicalType[0]),
                // sort the result by the output upsert key
                (o1, o2) -> {
                    for (int keyIndex : outputFieldIndices) {
                        LogicalType type = outputRowType.getChildren().get(keyIndex);
                        RowData.FieldGetter getter = RowData.createFieldGetter(type, keyIndex);

                        int compareResult =
                                Objects.requireNonNull(getter.getFieldOrNull(o1))
                                        .toString()
                                        .compareTo(
                                                Objects.requireNonNull(getter.getFieldOrNull(o2))
                                                        .toString());

                        if (compareResult != 0) {
                            return compareResult;
                        }
                    }
                    return o1.toString().compareTo(o2.toString());
                });
    }

    protected void verifyCacheData(
            DeltaJoinCache actualCache,
            Map<RowData, Map<RowData, RowData>> expectedCacheData,
            long expectedCacheRequestCount,
            long expectedCacheHitCount,
            RowType joinKeyRowType,
            RowType upsertKeyRowType,
            RowType valueRowType,
            boolean testLeftCache) {
        String errorPrefix = testLeftCache ? "left cache " : "right cache ";

        @SuppressWarnings("unchecked")
        Map<RowData, Map<RowData, RowData>> actualCacheData =
                ((Map<RowData, Map<RowData, RowData>>)
                        (Map<RowData, ?>)
                                (testLeftCache
                                        ? actualCache.getLeftCache().asMap()
                                        : actualCache.getRightCache().asMap()));
        assertThat(
                        convertCacheMapToGenericRowDataMap(
                                actualCacheData, joinKeyRowType, upsertKeyRowType, valueRowType))
                .as(errorPrefix + "data mismatch")
                .isEqualTo(
                        convertCacheMapToGenericRowDataMap(
                                expectedCacheData, joinKeyRowType, upsertKeyRowType, valueRowType));

        long actualCacheSize =
                testLeftCache
                        ? actualCache.getLeftCache().size()
                        : actualCache.getRightCache().size();
        assertThat(actualCacheSize)
                .as(errorPrefix + "size mismatch")
                .isEqualTo(expectedCacheData.size());

        long actualTotalSize =
                testLeftCache
                        ? actualCache.getLeftTotalSize().get()
                        : actualCache.getRightTotalSize().get();
        assertThat(actualTotalSize)
                .as(errorPrefix + "total size mismatch")
                .isEqualTo(expectedCacheData.values().stream().mapToInt(Map::size).sum());

        long actualRequestCount =
                testLeftCache
                        ? actualCache.getLeftRequestCount().get()
                        : actualCache.getRightRequestCount().get();
        assertThat(actualRequestCount)
                .as(errorPrefix + "request count mismatch")
                .isEqualTo(expectedCacheRequestCount);

        long actualHitCount =
                testLeftCache
                        ? actualCache.getLeftHitCount().get()
                        : actualCache.getRightHitCount().get();
        assertThat(actualHitCount)
                .as(errorPrefix + "hit count mismatch")
                .isEqualTo(expectedCacheHitCount);
    }

    private GenericRowData convert2GenericRowData(RowData row, RowType rowType) {
        if (row instanceof GenericRowData) {
            return (GenericRowData) row;
        }

        List<RowData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < rowType.getChildren().size(); i++) {
            LogicalType type = rowType.getChildren().get(i);
            fieldGetters.add(RowData.createFieldGetter(type, i));
        }
        GenericRowData newRow = new GenericRowData(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            newRow.setField(i, fieldGetters.get(i).getFieldOrNull(row));
        }
        return newRow;
    }

    private Map<GenericRowData, Map<GenericRowData, RowData>> convertCacheMapToGenericRowDataMap(
            Map<RowData, Map<RowData, RowData>> originalMap,
            RowType joinKeyRowType,
            RowType uniqueKeyRowType,
            RowType valueRowType) {
        Map<GenericRowData, Map<GenericRowData, RowData>> convertedMap = new HashMap<>();

        for (Map.Entry<RowData, Map<RowData, RowData>> entry : originalMap.entrySet()) {
            GenericRowData genericKey = convert2GenericRowData(entry.getKey(), joinKeyRowType);
            Map<GenericRowData, RowData> innerMap = new HashMap<>();

            for (Map.Entry<RowData, RowData> innerEntry : entry.getValue().entrySet()) {
                GenericRowData genericInnerKey =
                        convert2GenericRowData(innerEntry.getKey(), uniqueKeyRowType);
                GenericRowData genericInnerValue =
                        convert2GenericRowData(innerEntry.getValue(), valueRowType);
                innerMap.put(genericInnerKey, genericInnerValue);
            }

            convertedMap.put(genericKey, innerMap);
        }

        return convertedMap;
    }

    protected TableAsyncExecutionController<RowData, RowData, RowData> unwrapAEC(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    testHarness) {
        return unwrapOperator(testHarness).getAsyncExecutionController();
    }

    protected DeltaJoinCache unwrapCache(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    testHarness) {
        DeltaJoinCache cacheInLeftRunner =
                unwrapOperator(testHarness).getLeftTriggeredUserFunction().getCache();
        DeltaJoinCache cacheInRightRunner =
                unwrapOperator(testHarness).getRightTriggeredUserFunction().getCache();

        // the object ref must be the same
        assertThat(cacheInLeftRunner == cacheInRightRunner).isTrue();
        return cacheInLeftRunner;
    }

    protected StreamingDeltaJoinOperator unwrapOperator(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    testHarness) {
        return (StreamingDeltaJoinOperator) testHarness.getOperator();
    }

    protected static GeneratedFilterCondition getFilterCondition(
            RowType rowType,
            int[] allInvolvingFields,
            Function<GenericRowData, Boolean> condition) {
        return new GeneratedFilterCondition("", "", new Object[0]) {
            @Override
            public FilterCondition newInstance(ClassLoader classLoader) {
                return new MockFilterCondition() {
                    @Override
                    public boolean apply(RowData in) {
                        GenericRowData rowData = new GenericRowData(allInvolvingFields.length);
                        for (int i = 0; i < allInvolvingFields.length; i++) {
                            int leftField = allInvolvingFields[i];
                            RowData.FieldGetter fieldGetter =
                                    RowData.createFieldGetter(
                                            rowType.getChildren().get(leftField), leftField);
                            rowData.setField(i, fieldGetter.getFieldOrNull(in));
                        }
                        return condition.apply(rowData);
                    }
                };
            }

            @Override
            public Class<FilterCondition> getClass(ClassLoader classLoader) {
                return FilterCondition.class;
            }
        };
    }

    protected static RowType combineRowTypes(RowType... rowTypes) {
        return new RowType(
                Arrays.stream(rowTypes)
                        .flatMap(i -> i.getFields().stream())
                        .collect(Collectors.toList()));
    }

    protected static RowDataKeySelector getKeySelector(int[] keys, RowType rowType) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                keys, rowType.getChildren().toArray(new LogicalType[0]));
    }

    protected static void validateCalcFunctionAndCollectorWhenLookup(
            @Nullable ResultFuture<Object> resultFuture, boolean shouldOmit) {
        assertThat(resultFuture).isNotNull();
        assertThat(resultFuture)
                .isInstanceOf(LookupHandlerBase.Object2RowDataConverterResultFuture.class);
        LookupHandlerBase.Object2RowDataConverterResultFuture object2RowDataConverterResultFuture =
                (LookupHandlerBase.Object2RowDataConverterResultFuture) resultFuture;
        if (shouldOmit) {
            assertThat(object2RowDataConverterResultFuture.getCalcFunction()).isNull();
            assertThat(object2RowDataConverterResultFuture.getCalcCollector()).isNull();
        } else {
            assertThat(object2RowDataConverterResultFuture.getCalcFunction()).isNotNull();
            assertThat(object2RowDataConverterResultFuture.getCalcCollector()).isNotNull();
        }
    }

    protected GeneratedFunction<FlatMapFunction<RowData, RowData>> createFlatMap(
            Function<RowData, Optional<RowData>> flatMapFunc) {
        return new GeneratedFunction("", "", new Object[0]) {

            @Override
            public org.apache.flink.api.common.functions.Function newInstance(
                    ClassLoader classLoader) {
                return (FlatMapFunction<RowData, RowData>)
                        (value, out) -> {
                            Optional<RowData> result = flatMapFunc.apply(value);
                            result.ifPresent(out::collect);
                        };
            }

            @Override
            public Class<? extends org.apache.flink.api.common.functions.Function> compile(
                    ClassLoader classLoader) {
                return FlatMapFunction.class;
            }
        };
    }

    protected GeneratedFunction<FlatMapFunction<RowData, RowData>> createDoubleGreaterThanFilter(
            double bottom, int fieldIndex) {
        return createFlatMap(
                rowData ->
                        rowData.getDouble(fieldIndex) >= bottom
                                ? Optional.of(rowData)
                                : Optional.empty());
    }

    protected GeneratedFunction<AsyncFunction<RowData, Object>> createFetcherFunction(
            Map<Integer, LinkedHashMap<RowData, RowData>> tableCurrentDataMap,
            RowDataKeySelector streamSideLookupKeySelector,
            RowDataKeySelector lookupSideLookupKeySelector,
            int lookupTableIdx,
            @Nullable Throwable expectedThrownException) {

        return new GeneratedFunction("", "", new Object[0]) {
            @Override
            public MyAsyncFunction newInstance(ClassLoader classLoader) {
                return new MyAsyncFunction(
                        tableCurrentDataMap,
                        streamSideLookupKeySelector,
                        lookupSideLookupKeySelector,
                        lookupTableIdx,
                        expectedThrownException);
            }

            @Override
            public Class<MyAsyncFunction> getClass(ClassLoader classLoader) {
                return MyAsyncFunction.class;
            }
        };
    }

    /** Provide some callback methods for test. */
    protected static class MyAsyncExecutionControllerDelegate
            extends TableAsyncExecutionController<RowData, RowData, RowData> {

        public MyAsyncExecutionControllerDelegate(
                TableAsyncExecutionController<RowData, RowData, RowData> innerAec,
                boolean insertTableDataAfterEmit,
                BiConsumer<Integer, RowData> insertDataConsumer) {
            super(
                    innerAec.getAsyncInvoke(),
                    innerAec.getEmitWatermark(),
                    entry -> {
                        if (insertTableDataAfterEmit) {
                            StreamingDeltaJoinOperator.InputIndexAwareStreamRecordQueueEntry
                                    inputIndexAwareEntry =
                                            ((StreamingDeltaJoinOperator
                                                            .InputIndexAwareStreamRecordQueueEntry)
                                                    entry);
                            int inputIndex = inputIndexAwareEntry.getInputIndex();
                            insertDataConsumer.accept(
                                    inputIndex,
                                    (RowData) inputIndexAwareEntry.getInputElement().getValue());
                        }

                        innerAec.getEmitResult().accept(entry);
                    },
                    innerAec.getInferDrivenInputIndex(),
                    innerAec.getInferBlockingKey());
        }
    }

    protected abstract static class MockFilterCondition implements FilterCondition {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(OpenContext openContext) throws Exception {}

        protected abstract boolean apply(RowData in);

        @Override
        public boolean apply(Context ctx, RowData in) {
            return apply(in);
        }

        @Override
        public void close() throws Exception {}

        @Override
        public RuntimeContext getRuntimeContext() {
            return null;
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return null;
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {}
    }

    /** An async function used for test. */
    protected static class MyAsyncFunction extends RichAsyncFunction<RowData, Object> {

        private static final long serialVersionUID = 1L;

        private static final long TERMINATION_TIMEOUT = 5000L;
        private static final int THREAD_POOL_SIZE = 10;

        private static ExecutorService executorService;

        private static @Nullable CountDownLatch lock;

        private static final Map<Integer, AtomicInteger> lookupInvokeCount =
                new ConcurrentHashMap<>();

        public static Map<Integer, AtomicInteger> getLookupInvokeCount() {
            return lookupInvokeCount;
        }

        private final Map<Integer, LinkedHashMap<RowData, RowData>> tableCurrentDataMap;
        private final RowDataKeySelector streamSideLookupKeySelector;
        private final RowDataKeySelector lookupSideLookupKeySelector;
        private final int lookupTableIdx;
        private final @Nullable Throwable expectedThrownException;

        private @Nullable ResultFuture<Object> lastResultFuture;

        public MyAsyncFunction(
                Map<Integer, LinkedHashMap<RowData, RowData>> tableCurrentDataMap,
                RowDataKeySelector streamSideLookupKeySelector,
                RowDataKeySelector lookupSideLookupKeySelector,
                int lookupTableIdx,
                @Nullable Throwable expectedThrownException) {
            this.tableCurrentDataMap = tableCurrentDataMap;
            this.streamSideLookupKeySelector = streamSideLookupKeySelector;
            this.lookupSideLookupKeySelector = lookupSideLookupKeySelector;
            this.lookupTableIdx = lookupTableIdx;
            this.expectedThrownException = expectedThrownException;
        }

        public static void block() throws Exception {
            lock = new CountDownLatch(1);
        }

        public static void release() {
            Objects.requireNonNull(lock).countDown();
        }

        @Override
        public void asyncInvoke(final RowData input, final ResultFuture<Object> resultFuture) {
            this.lastResultFuture = resultFuture;
            executorService.submit(
                    () -> {
                        try {
                            if (expectedThrownException != null) {
                                throw expectedThrownException;
                            }

                            if (lock != null) {
                                lock.await();
                            }

                            LinkedHashMap<RowData, RowData> lookupTableData;
                            final RowDataKeySelector copiedStreamSideLookupKeySelector =
                                    streamSideLookupKeySelector.copy();
                            final RowDataKeySelector copiedLookupSideLookupKeySelector =
                                    lookupSideLookupKeySelector.copy();

                            synchronized (tableCurrentDataMap) {
                                lookupTableData =
                                        new LinkedHashMap<>(
                                                tableCurrentDataMap.getOrDefault(
                                                        lookupTableIdx, new LinkedHashMap<>()));
                            }

                            lookupInvokeCount.compute(
                                    lookupTableIdx,
                                    (k, v) -> {
                                        if (v == null) {
                                            v = new AtomicInteger(0);
                                        }
                                        v.incrementAndGet();
                                        return v;
                                    });

                            List<Object> results = new ArrayList<>();
                            for (RowData row : lookupTableData.values()) {
                                if (copiedStreamSideLookupKeySelector
                                        .getKey(input)
                                        .equals(copiedLookupSideLookupKeySelector.getKey(row))) {
                                    results.add(row);
                                }
                            }

                            resultFuture.complete(results);
                        } catch (Throwable e) {
                            resultFuture.completeExceptionally(
                                    new RuntimeException("Failed to look up table", e));
                        }
                    });
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            synchronized (MyAsyncFunction.class) {
                if (executorService == null) {
                    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                }
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            freeExecutor();
        }

        @Nullable
        public ResultFuture<Object> getLastResultFuture() {
            return lastResultFuture;
        }

        private void freeExecutor() {
            synchronized (MyAsyncFunction.class) {
                if (executorService == null) {
                    return;
                }

                executorService.shutdown();

                try {
                    if (!executorService.awaitTermination(
                            TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException interrupted) {
                    executorService.shutdownNow();

                    Thread.currentThread().interrupt();
                }
                executorService = null;
            }
        }
    }

    /** Base test specification shared by binary and cascaded delta join. */
    protected abstract static class AbstractTestSpec {

        abstract RowType getLeftInputRowType();

        final InternalTypeInfo<RowData> getLeftInputTypeInfo() {
            return InternalTypeInfo.of(getLeftInputRowType());
        }

        abstract RowType getRightInputRowType();

        final InternalTypeInfo<RowData> getRightInputTypeInfo() {
            return InternalTypeInfo.of(getRightInputRowType());
        }

        final RowType getOutputRowType() {
            return RowType.of(
                    Stream.concat(
                                    getLeftInputRowType().getChildren().stream(),
                                    getRightInputRowType().getChildren().stream())
                            .toArray(LogicalType[]::new),
                    Stream.concat(
                                    getLeftInputRowType().getFieldNames().stream(),
                                    getRightInputRowType().getFieldNames().stream())
                            .toArray(String[]::new));
        }

        abstract int[] getLeftJoinKeyIndices();

        final RowDataKeySelector getLeftJoinKeySelector() {
            return HandwrittenSelectorUtil.getRowDataSelector(
                    getLeftJoinKeyIndices(),
                    getLeftInputRowType().getChildren().toArray(new LogicalType[0]));
        }

        abstract int[] getRightJoinKeyIndices();

        final RowDataKeySelector getRightJoinKeySelector() {
            return HandwrittenSelectorUtil.getRowDataSelector(
                    getRightJoinKeyIndices(),
                    getRightInputRowType().getChildren().toArray(new LogicalType[0]));
        }

        abstract Optional<int[]> getLeftUpsertKey();

        final RowDataKeySelector getLeftUpsertKeySelector() {
            return getUpsertKeySelector(getLeftInputRowType(), getLeftUpsertKey().orElse(null));
        }

        abstract Optional<int[]> getRightUpsertKey();

        final RowDataKeySelector getRightUpsertKeySelector() {
            return getUpsertKeySelector(getRightInputRowType(), getRightUpsertKey().orElse(null));
        }

        private RowDataKeySelector getUpsertKeySelector(
                RowType rowType, @Nullable int[] upsertKey) {
            if (upsertKey == null) {
                upsertKey = IntStream.range(0, rowType.getFieldCount()).toArray();
            }
            return HandwrittenSelectorUtil.getRowDataSelector(
                    upsertKey, rowType.getChildren().toArray(new LogicalType[0]));
        }
    }

    protected static class MockGeneratedFlatMapFunction
            extends GeneratedFunction<FlatMapFunction<RowData, RowData>> {

        private static final long serialVersionUID = 1L;

        private final @Nullable Function<RowData, Boolean> condition;

        public MockGeneratedFlatMapFunction(@Nullable Function<RowData, Boolean> condition) {
            super("", "", new Object[0]);
            this.condition = condition;
        }

        @Override
        public FlatMapFunction<RowData, RowData> newInstance(ClassLoader classLoader) {
            return (value, out) -> {
                if (condition == null || condition.apply(value)) {
                    out.collect(value);
                }
            };
        }

        @Override
        public Class<FlatMapFunction<RowData, RowData>> compile(ClassLoader classLoader) {
            // just avoid exceptions
            return null;
        }
    }
}
