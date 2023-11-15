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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.RESOURCE_COUNTER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Runtime function implementations for {@link TestValuesTableFactory}. */
final class TestValuesRuntimeFunctions {

    static final Object LOCK = TestValuesTableFactory.class;

    // [table_name, [task_id, List[value]]]
    private static final Map<String, Map<Integer, List<Row>>> globalRawResult = new HashMap<>();
    // [table_name, [task_id, Map[key, value]]]
    private static final Map<String, Map<Integer, Map<Row, Row>>> globalUpsertResult =
            new HashMap<>();
    // [table_name, [task_id, List[value]]]
    private static final Map<String, Map<Integer, List<Row>>> globalRetractResult = new HashMap<>();
    // [table_name, [watermark]]
    private static final Map<String, List<Watermark>> watermarkHistory = new HashMap<>();

    // [table_name, [List[observer]]
    private static final Map<String, List<BiConsumer<Integer, List<Row>>>>
            localRawResultsObservers = new HashMap<>();

    static List<String> getRawResultsAsStrings(String tableName) {
        return getRawResults(tableName).stream()
                .map(TestValuesRuntimeFunctions::rowToString)
                .collect(Collectors.toList());
    }

    static List<Row> getRawResults(String tableName) {
        synchronized (LOCK) {
            if (globalRawResult.containsKey(tableName)) {
                return globalRawResult.get(tableName).values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }

    /** Returns raw results if there was only one table with results, throws otherwise. */
    static List<String> getOnlyRawResultsAsStrings() {
        return getOnlyRawResults().stream()
                .map(TestValuesRuntimeFunctions::rowToString)
                .collect(Collectors.toList());
    }

    static List<Row> getOnlyRawResults() {
        synchronized (LOCK) {
            if (globalRawResult.size() != 1) {
                throw new IllegalStateException(
                        "Expected results for only one table to be present, but found "
                                + globalRawResult.size());
            }

            return globalRawResult.values().iterator().next().values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
    }

    static List<Watermark> getWatermarks(String tableName) {
        synchronized (LOCK) {
            if (watermarkHistory.containsKey(tableName)) {
                return new ArrayList<>(watermarkHistory.get(tableName));
            } else {
                return Collections.emptyList();
            }
        }
    }

    static List<String> getResultsAsStrings(String tableName) {
        return getResults(tableName).stream().map(Row::toString).collect(Collectors.toList());
    }

    static List<Row> getResults(String tableName) {
        synchronized (LOCK) {
            if (globalUpsertResult.containsKey(tableName)) {
                return globalUpsertResult.get(tableName).values().stream()
                        .flatMap(map -> map.values().stream())
                        .collect(Collectors.toList());
            } else if (globalRetractResult.containsKey(tableName)) {
                return globalRetractResult.get(tableName).values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
            } else if (globalRawResult.containsKey(tableName)) {
                return getRawResults(tableName);
            }
        }
        return Collections.emptyList();
    }

    static void registerLocalRawResultsObserver(
            String tableName, BiConsumer<Integer, List<Row>> observer) {
        localRawResultsObservers.computeIfAbsent(tableName, n -> new ArrayList<>()).add(observer);
    }

    static void clearResults() {
        synchronized (LOCK) {
            globalRawResult.clear();
            globalUpsertResult.clear();
            globalRetractResult.clear();
            watermarkHistory.clear();
        }
    }

    private static String rowToString(Row row) {
        if (RowUtils.USE_LEGACY_TO_STRING) {
            return String.format("%s(%s)", row.getKind().shortString(), row);
        } else {
            return row.toString();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Source Function implementations
    // ------------------------------------------------------------------------------------------

    public static class FromElementSourceFunctionWithWatermark implements SourceFunction<RowData> {

        /** The (de)serializer to be used for the data elements. */
        private final TypeSerializer<RowData> serializer;

        /** The actual data elements, in serialized form. */
        private final byte[] elementsSerialized;

        /** The number of serialized elements. */
        private final int numElements;

        /** The number of elements emitted already. */
        private volatile int numElementsEmitted;

        /** WatermarkStrategy to generate watermark generator. */
        private final WatermarkStrategy<RowData> watermarkStrategy;

        private volatile boolean isRunning = true;

        private final String tableName;

        public FromElementSourceFunctionWithWatermark(
                String tableName,
                TypeSerializer<RowData> serializer,
                Iterable<RowData> elements,
                WatermarkStrategy<RowData> watermarkStrategy)
                throws IOException {
            this.tableName = tableName;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

            int count = 0;
            try {
                for (RowData element : elements) {
                    serializer.serialize(element, wrapper);
                    count++;
                }
            } catch (Exception e) {
                throw new IOException(
                        "Serializing the source elements failed: " + e.getMessage(), e);
            }

            this.numElements = count;
            this.elementsSerialized = baos.toByteArray();
            this.watermarkStrategy = watermarkStrategy;
            this.serializer = serializer;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
            final DataInputView input = new DataInputViewStreamWrapper(bais);
            WatermarkGenerator<RowData> generator =
                    watermarkStrategy.createWatermarkGenerator(() -> null);
            WatermarkOutput output = new TestValuesWatermarkOutput(ctx);
            final Object lock = ctx.getCheckpointLock();

            while (isRunning && numElementsEmitted < numElements) {
                RowData next;
                try {
                    next = serializer.deserialize(input);
                    generator.onEvent(next, Long.MIN_VALUE, output);
                    generator.onPeriodicEmit(output);
                } catch (Exception e) {
                    throw new IOException(
                            "Failed to deserialize an element from the source. "
                                    + "If you are using user-defined serialization (Value and Writable types), check the "
                                    + "serialization functions.\nSerializer is "
                                    + serializer,
                            e);
                }

                synchronized (lock) {
                    ctx.collect(next);
                    numElementsEmitted++;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        private class TestValuesWatermarkOutput implements WatermarkOutput {
            SourceContext<RowData> ctx;

            public TestValuesWatermarkOutput(SourceContext<RowData> ctx) {
                this.ctx = ctx;
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                ctx.emitWatermark(
                        new org.apache.flink.streaming.api.watermark.Watermark(
                                watermark.getTimestamp()));
                synchronized (LOCK) {
                    watermarkHistory
                            .computeIfAbsent(tableName, k -> new LinkedList<>())
                            .add(watermark);
                }
            }

            @Override
            public void markIdle() {}

            @Override
            public void markActive() {}
        }
    }

    // ------------------------------------------------------------------------------------------
    // Sink Function implementations
    // ------------------------------------------------------------------------------------------

    /**
     * The sink implementation is end-to-end exactly once, so that it can be used to check the state
     * restoring in streaming sql.
     */
    private abstract static class AbstractExactlyOnceSink extends RichSinkFunction<RowData>
            implements CheckpointedFunction {
        private static final long serialVersionUID = 1L;

        protected final String tableName;
        protected final DataType consumedDataType;
        protected final DataStructureConverter converter;
        protected transient ListState<Row> rawResultState;
        protected transient List<Row> localRawResult;

        protected AbstractExactlyOnceSink(
                String tableName, DataType consumedDataType, DataStructureConverter converter) {
            this.tableName = tableName;
            this.consumedDataType = consumedDataType;
            this.converter = converter;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.rawResultState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "sink-results",
                                            ExternalSerializer.of(consumedDataType)));
            this.localRawResult = new ArrayList<>();
            if (context.isRestored()) {
                for (Row value : rawResultState.get()) {
                    localRawResult.add(value);
                }
            }
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            synchronized (LOCK) {
                globalRawResult
                        .computeIfAbsent(tableName, k -> new HashMap<>())
                        .put(taskId, localRawResult);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            synchronized (LOCK) {
                rawResultState.update(localRawResult);
            }
        }

        protected void addLocalRawResult(Row row) {
            localRawResult.add(row);
            Optional.ofNullable(localRawResultsObservers.get(tableName))
                    .orElse(Collections.emptyList())
                    .forEach(
                            c ->
                                    c.accept(
                                            getRuntimeContext().getIndexOfThisSubtask(),
                                            localRawResult));
        }
    }

    static class AppendingSinkFunction extends AbstractExactlyOnceSink {

        private static final long serialVersionUID = 1L;
        private final int rowtimeIndex;

        protected AppendingSinkFunction(
                String tableName,
                DataType consumedDataType,
                DataStructureConverter converter,
                int rowtimeIndex) {
            super(tableName, consumedDataType, converter);
            this.rowtimeIndex = rowtimeIndex;
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            if (value.getRowKind() == RowKind.INSERT) {
                if (rowtimeIndex >= 0) {
                    // currently, rowtime attribute always uses 3 precision
                    TimestampData rowtime = value.getTimestamp(rowtimeIndex, 3);
                    long mark = context.currentWatermark();
                    if (mark > rowtime.getMillisecond()) {
                        // discard the late data
                        return;
                    }
                }
                synchronized (LOCK) {
                    addLocalRawResult((Row) converter.toExternal(value));
                }
            } else {
                throw new RuntimeException(
                        "AppendingSinkFunction received " + value.getRowKind() + " messages.");
            }
        }
    }

    /**
     * NOTE: This class should use a global map to store upsert values. Just like other external
     * databases.
     */
    static class KeyedUpsertingSinkFunction extends AbstractExactlyOnceSink {
        private static final long serialVersionUID = 1L;
        private final int[] keyIndices;
        private final int[] targetColumnIndices;
        private final int expectedSize;
        private final int totalColumns;

        // [key, value] map result
        private transient Map<Row, Row> localUpsertResult;
        private transient int receivedNum;

        protected KeyedUpsertingSinkFunction(
                String tableName,
                DataType consumedDataType,
                DataStructureConverter converter,
                int[] keyIndices,
                int[] targetColumnIndices,
                int expectedSize,
                int totalColumns) {
            super(tableName, consumedDataType, converter);
            this.keyIndices = keyIndices;
            this.targetColumnIndices = targetColumnIndices;
            this.expectedSize = expectedSize;
            this.totalColumns = totalColumns;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            super.initializeState(context);

            synchronized (LOCK) {
                // always store in a single map, global upsert
                this.localUpsertResult =
                        globalUpsertResult
                                .computeIfAbsent(tableName, k -> new HashMap<>())
                                .computeIfAbsent(0, k -> new HashMap<>());
            }
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            RowKind kind = value.getRowKind();

            Row row = (Row) converter.toExternal(value);
            assertThat(row).isNotNull();

            synchronized (LOCK) {
                addLocalRawResult(row);

                Row key = Row.project(row, keyIndices);
                key.setKind(RowKind.INSERT);

                final Row upsertRow = Row.copy(row);
                upsertRow.setKind(RowKind.INSERT);

                if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                    if (targetColumnIndices.length > 0) {
                        // perform partial insert
                        localUpsertResult.compute(
                                key,
                                (entryKey, currentValue) ->
                                        updateRowValue(
                                                currentValue, upsertRow, targetColumnIndices));
                    } else {
                        localUpsertResult.put(key, upsertRow);
                    }
                } else {
                    Row oldValue = localUpsertResult.remove(key);
                    if (oldValue == null) {
                        throw new RuntimeException(
                                "Tried to delete a value that wasn't inserted first. "
                                        + "This is probably an incorrectly implemented test.");
                    }
                }
                receivedNum++;
                if (expectedSize != -1 && receivedNum == expectedSize) {
                    // some sources are infinite (e.g. kafka),
                    // we throw a SuccessException to indicate job is finished.
                    throw new SuccessException();
                }
            }
        }

        private Row updateRowValue(Row old, Row newRow, int[] targetColumnIndices) {
            if (old == null) {
                // no old value, just return current
                return newRow;
            } else {
                assert old.getArity() == totalColumns;
                // exist old value, simply simulate an update
                for (int idx : targetColumnIndices) {
                    old.setField(idx, newRow.getField(idx));
                }
                return old;
            }
        }
    }

    static class RetractingSinkFunction extends AbstractExactlyOnceSink {
        private static final long serialVersionUID = 1L;

        protected transient ListState<Row> retractResultState;
        protected transient List<Row> localRetractResult;

        protected RetractingSinkFunction(
                String tableName, DataType consumedDataType, DataStructureConverter converter) {
            super(tableName, consumedDataType, converter);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            super.initializeState(context);
            this.retractResultState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "sink-retract-results",
                                            ExternalSerializer.of(consumedDataType)));
            this.localRetractResult = new ArrayList<>();

            if (context.isRestored()) {
                for (Row value : retractResultState.get()) {
                    localRetractResult.add(value);
                }
            }

            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            synchronized (LOCK) {
                globalRetractResult
                        .computeIfAbsent(tableName, k -> new HashMap<>())
                        .put(taskId, localRetractResult);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            super.snapshotState(context);
            synchronized (LOCK) {
                retractResultState.update(localRetractResult);
            }
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void invoke(RowData value, Context context) throws Exception {
            RowKind kind = value.getRowKind();
            Row row = (Row) converter.toExternal(value);
            assertThat(row).isNotNull();
            synchronized (LOCK) {
                addLocalRawResult(row);
                final Row retractRow = Row.copy(row);
                retractRow.setKind(RowKind.INSERT);
                if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                    localRetractResult.add(retractRow);
                } else {
                    boolean contains = localRetractResult.remove(retractRow);
                    if (!contains) {
                        throw new RuntimeException(
                                "Tried to retract a value that wasn't inserted first. "
                                        + "This is probably an incorrectly implemented test.");
                    }
                }
            }
        }
    }

    static class AppendingOutputFormat extends RichOutputFormat<RowData> {

        private static final long serialVersionUID = 1L;
        private final String tableName;
        private final DataStructureConverter converter;

        protected transient List<Row> localRawResult;

        protected AppendingOutputFormat(String tableName, DataStructureConverter converter) {
            this.tableName = tableName;
            this.converter = converter;
        }

        @Override
        public void configure(Configuration parameters) {
            // nothing to do
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            this.localRawResult = new ArrayList<>();
            synchronized (LOCK) {
                globalRawResult
                        .computeIfAbsent(tableName, k -> new HashMap<>())
                        .put(taskNumber, localRawResult);
            }
        }

        @Override
        public void writeRecord(RowData value) throws IOException {
            if (value.getRowKind() == RowKind.INSERT) {
                Row row = (Row) converter.toExternal(value);
                assertThat(row).isNotNull();
                synchronized (LOCK) {
                    localRawResult.add(row);
                    Optional.ofNullable(localRawResultsObservers.get(tableName))
                            .orElse(Collections.emptyList())
                            .forEach(
                                    c ->
                                            c.accept(
                                                    getRuntimeContext().getIndexOfThisSubtask(),
                                                    localRawResult));
                }
            } else {
                throw new RuntimeException(
                        "AppendingOutputFormat received " + value.getRowKind() + " messages.");
            }
        }

        @Override
        public void close() throws IOException {
            // nothing to do
        }
    }

    // ------------------------------------------------------------------------------------------
    // Lookup Function implementations
    // ------------------------------------------------------------------------------------------

    /**
     * A lookup function which find matched rows with the given fields. NOTE: We have to declare it
     * as public because it will be used in code generation.
     */
    public static class TestValuesLookupFunction extends LookupFunction {

        private static final long serialVersionUID = 1L;
        private final List<Row> data;
        private final int[] lookupIndices;

        private final RowType producedRowType;

        private final LookupTableSource.DataStructureConverter converter;
        private final GeneratedProjection generatedProjection;
        private final boolean projectable;
        private transient Map<RowData, List<RowData>> indexedData;
        private transient boolean isOpenCalled = false;
        private transient Projection<RowData, GenericRowData> projection;
        private transient TypeSerializer<RowData> rowSerializer;

        protected TestValuesLookupFunction(
                List<Row> data,
                int[] lookupIndices,
                RowType producedRowType,
                LookupTableSource.DataStructureConverter converter,
                Optional<GeneratedProjection> generatedProjection) {
            this.data = data;
            this.lookupIndices = lookupIndices;
            this.producedRowType = producedRowType;
            this.converter = converter;
            this.projectable = generatedProjection.isPresent();
            this.generatedProjection = generatedProjection.orElse(null);
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            RESOURCE_COUNTER.incrementAndGet();
            isOpenCalled = true;
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (projectable) {
                projection = generatedProjection.newInstance(classLoader);
            }
            converter.open(RuntimeConverter.Context.create(classLoader));
            rowSerializer = InternalSerializers.create(producedRowType);
            indexDataByKey();
        }

        @Override
        public Collection<RowData> lookup(RowData keyRow) throws IOException {
            checkArgument(isOpenCalled, "open() is not called.");
            for (int i = 0; i < keyRow.getArity(); i++) {
                checkNotNull(
                        ((GenericRowData) keyRow).getField(i),
                        String.format(
                                "Lookup key %s contains null value, which should not happen.",
                                keyRow));
            }
            return indexedData.get(keyRow);
        }

        @Override
        public void close() throws Exception {
            RESOURCE_COUNTER.decrementAndGet();
        }

        private void indexDataByKey() {
            indexedData = new HashMap<>();
            data.forEach(
                    record -> {
                        GenericRowData rowData = (GenericRowData) converter.toInternal(record);
                        if (projectable) {
                            rowData = projection.apply(rowData);
                        }
                        checkNotNull(
                                rowData, "Cannot convert record to internal GenericRowData type");
                        RowData key =
                                GenericRowData.of(
                                        Arrays.stream(lookupIndices)
                                                .mapToObj(rowData::getField)
                                                .toArray());
                        RowData copiedRow = rowSerializer.copy(rowData);
                        List<RowData> list = indexedData.get(key);
                        if (list != null) {
                            list.add(copiedRow);
                        } else {
                            list = new ArrayList<>();
                            list.add(copiedRow);
                            indexedData.put(key, list);
                        }
                    });
        }
    }

    /**
     * An async lookup function which find matched rows with the given fields. NOTE: We have to
     * declare it as public because it will be used in code generation.
     */
    public static class AsyncTestValueLookupFunction extends AsyncLookupFunction {

        private static final long serialVersionUID = 1L;
        private final List<Row> data;
        private final int[] lookupIndices;
        private final RowType producedRowType;
        private final LookupTableSource.DataStructureConverter converter;

        private final GeneratedProjection generatedProjection;
        private final boolean projectable;
        private final Random random;
        private transient boolean isOpenCalled = false;
        private transient ExecutorService executor;
        private transient Map<RowData, List<RowData>> indexedData;
        private transient Projection<RowData, GenericRowData> projection;
        private transient TypeSerializer<RowData> rowSerializer;

        protected AsyncTestValueLookupFunction(
                List<Row> data,
                int[] lookupIndices,
                RowType producedRowType,
                LookupTableSource.DataStructureConverter converter,
                Optional<GeneratedProjection> generatedProjection) {
            this.data = data;
            this.lookupIndices = lookupIndices;
            this.producedRowType = producedRowType;
            this.converter = converter;
            this.projectable = generatedProjection.isPresent();
            this.generatedProjection = generatedProjection.orElse(null);
            this.random = new Random();
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            RESOURCE_COUNTER.incrementAndGet();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (projectable) {
                projection = generatedProjection.newInstance(classLoader);
            }
            converter.open(RuntimeConverter.Context.create(classLoader));
            rowSerializer = InternalSerializers.create(producedRowType);
            isOpenCalled = true;
            // generate unordered result for async lookup
            executor = Executors.newFixedThreadPool(2);
            indexDataByKey();
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            checkArgument(isOpenCalled, "open() is not called.");
            for (int i = 0; i < keyRow.getArity(); i++) {
                checkNotNull(
                        ((GenericRowData) keyRow).getField(i),
                        String.format(
                                "Lookup key %s contains null value, which should not happen.",
                                keyRow));
            }
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(random.nextInt(5));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return indexedData.get(keyRow);
                    },
                    executor);
        }

        @Override
        public void close() throws Exception {
            RESOURCE_COUNTER.decrementAndGet();
            if (executor != null && !executor.isShutdown()) {
                executor.shutdown();
            }
        }

        private void indexDataByKey() {
            indexedData = new HashMap<>();
            data.forEach(
                    record -> {
                        GenericRowData rowData = (GenericRowData) converter.toInternal(record);
                        if (projectable) {
                            rowData = projection.apply(rowData);
                        }
                        checkNotNull(
                                rowData, "Cannot convert record to internal GenericRowData type");
                        RowData key =
                                GenericRowData.of(
                                        Arrays.stream(lookupIndices)
                                                .mapToObj(rowData::getField)
                                                .toArray());
                        RowData copiedRow = rowSerializer.copy(rowData);
                        List<RowData> list = indexedData.get(key);
                        if (list != null) {
                            list.add(copiedRow);
                        } else {
                            list = new ArrayList<>();
                            list.add(copiedRow);
                            indexedData.put(key, list);
                        }
                    });
        }
    }

    /**
     * The {@link TestNoLookupUntilNthAccessLookupFunction} extends {@link
     * TestValuesLookupFunction}, it will not do real lookup for a key (return null value
     * immediately) until which lookup times beyond predefined threshold 'lookupThreshold'.
     */
    public static class TestNoLookupUntilNthAccessLookupFunction extends TestValuesLookupFunction {

        private static final long serialVersionUID = 1L;

        /** The threshold that a real lookup can happen, otherwise no lookup at all. */
        private final int lookupThreshold;

        private transient Map<RowData, Integer> accessCounter;

        protected TestNoLookupUntilNthAccessLookupFunction(
                List<Row> data,
                int[] lookupIndices,
                RowType producedRowType,
                LookupTableSource.DataStructureConverter converter,
                Optional<GeneratedProjection> generatedProjection,
                int lookupThreshold) {
            super(data, lookupIndices, producedRowType, converter, generatedProjection);
            this.lookupThreshold = lookupThreshold;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            accessCounter = new HashMap<>();
        }

        protected int counter(RowData key) {
            int currentCnt = accessCounter.computeIfAbsent(key, cnt -> 0) + 1;
            accessCounter.put(key, currentCnt);
            return currentCnt;
        }

        @Override
        public Collection<RowData> lookup(RowData keyRow) throws IOException {
            int currentCnt = counter(keyRow);
            if (currentCnt <= lookupThreshold) {
                return null;
            }
            return super.lookup(keyRow);
        }
    }

    /**
     * The {@link TestNoLookupUntilNthAccessAsyncLookupFunction} extends {@link
     * AsyncTestValueLookupFunction}, it will not do real lookup for a key (return empty result
     * immediately) until which lookup times beyond predefined threshold 'lookupThreshold'.
     */
    public static class TestNoLookupUntilNthAccessAsyncLookupFunction
            extends AsyncTestValueLookupFunction {
        private static final long serialVersionUID = 1L;
        private static Collection<RowData> emptyResult = Collections.emptyList();

        /** The threshold that a real lookup can happen, otherwise no lookup at all. */
        private final int lookupThreshold;

        private transient Map<RowData, Integer> accessCounter;

        public TestNoLookupUntilNthAccessAsyncLookupFunction(
                List<Row> data,
                int[] lookupIndices,
                RowType producedRowType,
                LookupTableSource.DataStructureConverter converter,
                Optional<GeneratedProjection> generatedProjection,
                int lookupThreshold) {
            super(data, lookupIndices, producedRowType, converter, generatedProjection);
            this.lookupThreshold = lookupThreshold;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            accessCounter = new HashMap<>();
        }

        protected int counter(RowData key) {
            int currentCnt = accessCounter.computeIfAbsent(key, cnt -> 0) + 1;
            accessCounter.put(key, currentCnt);
            return currentCnt;
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            int currentCnt = counter(keyRow);
            if (currentCnt <= lookupThreshold) {
                return CompletableFuture.supplyAsync(() -> emptyResult);
            }
            return super.asyncLookup(keyRow);
        }
    }
}
