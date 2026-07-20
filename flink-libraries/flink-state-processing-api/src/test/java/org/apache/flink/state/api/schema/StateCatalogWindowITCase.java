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

package org.apache.flink.state.api.schema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.StateTableUtils;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.state.catalog.StateCatalog;
import org.apache.flink.state.table.SavepointConnectorOptions.StateType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests verifying that the savepoint/checkpoint table connector exposes window-operator
 * state via the {@code _windowed}/{@code _windowed_flat} tables, across window types (time-based,
 * global, non-keyed {@code windowAll()}) and window-function shapes ({@code reduce()} => VALUE,
 * {@code process()} => LIST), while a window operator's plain per-key state (e.g. {@code
 * merging-window-set}) stays excluded from those tables and remains available via the ordinary
 * {@code _keyed} table.
 *
 * <p>Subclassed per state backend (see {@code HashMapStateCatalogWindowITCase} and {@code
 * EmbeddedRocksDBStateCatalogWindowITCase}) so that window-scoped state is verified against both
 * the heap and RocksDB keyed-state-handle formats.
 */
public abstract class StateCatalogWindowITCase extends SavepointTestBase {

    protected abstract Configuration getConfiguration();

    private static final String SESSION_REDUCE_UID = "session-window-reduce-operator";
    private static final String TUMBLING_APPLY_UID = "tumbling-window-apply-operator";
    private static final String TUMBLING_APPLY_ALL_UID = "tumbling-window-apply-all-operator";
    private static final String TUMBLING_APPLY_POJO_UID = "tumbling-window-apply-pojo-operator";
    private static final String GLOBAL_REDUCE_UID = "global-window-reduce-operator";
    private static final String EXPLICIT_GLOBAL_STATE_UID = "explicit-global-state-operator";
    private static final String WINDOW_WITH_GLOBAL_STATE_UID = "window-with-global-state-operator";

    /** A plain POJO (public fields, public no-arg constructor) used as a window element type. */
    public static class Event {
        public String name;
        public long value;

        public Event() {}

        public Event(String name, long value) {
            this.name = name;
            this.value = value;
        }
    }

    @Test
    public void testSessionWindowReduceExposesValueShapedWindowState() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Tuple2<String, Long>[] data =
                new Tuple2[] {Tuple2.of("key", 1L), Tuple2.of("key", 2L), Tuple2.of("key", 3L)};

        env.addSource(createSource(data))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.f1))
                .keyBy(t -> t.f0)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .reduce(
                        (ReduceFunction<Tuple2<String, Long>>)
                                (v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1))
                .uid(SESSION_REDUCE_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(SESSION_REDUCE_UID);

        // "merging-window-set" is Flink-internal session-window bookkeeping, excluded by name;
        // "window-contents" is window-scoped and can't be exposed via the plain per-key pathway.
        List<String> stateNames = StateTableUtils.getKeyedStates(metadata, opId);
        assertFalse(
                stateNames.contains("merging-window-set"),
                "Expected 'merging-window-set' to be excluded as Flink-internal bookkeeping, got: "
                        + stateNames);
        assertFalse(
                stateNames.contains("window-contents"),
                "Expected 'window-contents' (window-namespaced state) to be excluded, got: "
                        + stateNames);

        KeyedStateSchemaInfo plainSchema = StateTableUtils.getKeyedStateSchema(metadata, opId);
        assertFalse(
                plainSchema.stateSchemas.containsKey("window-contents"),
                "Expected 'window-contents' to be excluded from the plain keyed schema");

        KeyedStateSchemaInfo windowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, opId);
        assertTrue(
                windowSchema.stateSchemas.containsKey("window-contents"),
                "Expected 'window-contents' to be present in the window-keyed schema");
        KeyedStateSchemaInfo.StateEntryInfo entryInfo =
                windowSchema.stateSchemas.get("window-contents");
        assertEquals(StateType.VALUE, entryInfo.stateType);
        assertTimeWindowRowType(entryInfo.windowLogicalType);

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String windowTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + SESSION_REDUCE_UID
                            + StateCatalog.WINDOW_TABLE_SUFFIX;
            List<Row> rows =
                    StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + windowTable + "`");
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("key", row.getField("state_key"));
            Row reduced = row.getFieldAs("window-contents");
            assertEquals("key", reduced.getField("f0"));
            assertEquals(6L, reduced.getField("f1"));

            Row window = row.getFieldAs("state_window");
            LocalDateTime start = window.getFieldAs("window_start");
            LocalDateTime end = window.getFieldAs("window_end");
            assertEquals(1L, toEpochMilli(start));
            assertEquals(1L + Duration.ofSeconds(5).toMillis(), toEpochMilli(end));
        } finally {
            catalog.close();
        }
    }

    // Also covers windowAll(): Flink implements it as a keyed window operator internally
    // partitioned by a constant (byte) 0 key, so it's exercised alongside the keyed pipeline here
    // rather than via a separate test.
    @Test
    public void testTumblingWindowProcessExposesListShapedWindowState() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Tuple2<String, Long>[] data = new Tuple2[] {Tuple2.of("key", 10L), Tuple2.of("key", 20L)};

        env.addSource(createSource(data))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.f1))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(
                        new ProcessWindowFunction<
                                Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<Tuple2<String, Long>> out) {
                                elements.forEach(out::collect);
                            }
                        })
                .uid(TUMBLING_APPLY_UID)
                .sinkTo(new DiscardingSink<>());

        env.addSource(createSource(data))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.f1))
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(
                        new ProcessAllWindowFunction<
                                Tuple2<String, Long>, Tuple2<String, Long>, TimeWindow>() {
                            @Override
                            public void process(
                                    Context context,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<Tuple2<String, Long>> out) {
                                elements.forEach(out::collect);
                            }
                        })
                .uid(TUMBLING_APPLY_ALL_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(TUMBLING_APPLY_UID);
        OperatorIdentifier allOpId = OperatorIdentifier.forUid(TUMBLING_APPLY_ALL_UID);

        KeyedStateSchemaInfo windowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, opId);
        assertTrue(windowSchema.stateSchemas.containsKey("window-contents"));
        KeyedStateSchemaInfo.StateEntryInfo entryInfo =
                windowSchema.stateSchemas.get("window-contents");
        assertEquals(StateType.LIST, entryInfo.stateType);
        assertTimeWindowRowType(entryInfo.windowLogicalType);

        KeyedStateSchemaInfo allWindowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, allOpId);
        assertTrue(allWindowSchema.stateSchemas.containsKey("window-contents"));
        KeyedStateSchemaInfo.StateEntryInfo allEntryInfo =
                allWindowSchema.stateSchemas.get("window-contents");
        assertEquals(StateType.LIST, allEntryInfo.stateType);
        assertTimeWindowRowType(allEntryInfo.windowLogicalType);

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String flattenedWindowTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + TUMBLING_APPLY_UID
                            + "_window-contents"
                            + StateCatalog.FLAT_WINDOW_TABLE_SUFFIX;
            List<Row> rows =
                    StateCatalogTestUtils.collect(
                            tableEnv,
                            "SELECT * FROM `" + flattenedWindowTable + "` ORDER BY list_index");
            assertEquals(2, rows.size());
            assertEquals(0L, rows.get(0).getField("list_index"));
            assertEquals(1L, rows.get(1).getField("list_index"));
            Row first = rows.get(0).getFieldAs("list_value");
            Row second = rows.get(1).getFieldAs("list_value");
            assertEquals("key", first.getField("f0"));
            assertEquals(10L, first.getField("f1"));
            assertEquals("key", second.getField("f0"));
            assertEquals(20L, second.getField("f1"));

            // windowAll()'s implicit state key is the constant (byte) 0 that Flink's
            // NullByteKeySelector assigns to every record.
            String flattenedAllWindowTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + TUMBLING_APPLY_ALL_UID
                            + "_window-contents"
                            + StateCatalog.FLAT_WINDOW_TABLE_SUFFIX;
            List<Row> allRows =
                    StateCatalogTestUtils.collect(
                            tableEnv,
                            "SELECT * FROM `" + flattenedAllWindowTable + "` ORDER BY list_index");
            assertEquals(2, allRows.size());
            assertEquals((byte) 0, allRows.get(0).getField("state_key"));
            assertEquals((byte) 0, allRows.get(1).getField("state_key"));
            assertEquals(0L, allRows.get(0).getField("list_index"));
            assertEquals(1L, allRows.get(1).getField("list_index"));
            Row allFirst = allRows.get(0).getFieldAs("list_value");
            Row allSecond = allRows.get(1).getFieldAs("list_value");
            assertEquals("key", allFirst.getField("f0"));
            assertEquals(10L, allFirst.getField("f1"));
            assertEquals("key", allSecond.getField("f0"));
            assertEquals(20L, allSecond.getField("f1"));
        } finally {
            catalog.close();
        }
    }

    // Verifies the LIST-shaped window state falls back to PojoToRowDataDeserializer rather than
    // throwing ClassNotFoundException when the element's class is missing from the classpath.
    // Simulated by swapping the thread's context classloader (consulted by
    // SavepointLoader/SavepointReader to resolve serializer snapshot classes) for one that
    // specifically hides the Event class, as flink-core's PojoSerializerSnapshotLenientReadTest
    // does.
    @Test
    public void testTumblingWindowProcessWithMissingPojoClassFallsBackToRowData() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Event[] data = new Event[] {new Event("shared", 10L), new Event("shared", 20L)};

        env.addSource(createSource(data))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.value))
                .keyBy(e -> e.name)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(
                        new ProcessWindowFunction<Event, Event, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<Event> elements,
                                    Collector<Event> out) {
                                elements.forEach(out::collect);
                            }
                        })
                .uid(TUMBLING_APPLY_POJO_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader hidingEventClass =
                new ClassLoader(original) {
                    @Override
                    public Class<?> loadClass(String name) throws ClassNotFoundException {
                        if (name.equals(Event.class.getName())) {
                            throw new ClassNotFoundException(name);
                        }
                        return super.loadClass(name);
                    }
                };
        Thread.currentThread().setContextClassLoader(hidingEventClass);
        try {
            TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
            StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
            try {
                String dbName = catalog.listDatabases().get(0);
                tableEnv.useCatalog("state");
                tableEnv.useDatabase(dbName);

                String flattenedWindowTable =
                        StateCatalog.OPERATOR_UID_PREFIX
                                + TUMBLING_APPLY_POJO_UID
                                + "_window-contents"
                                + StateCatalog.FLAT_WINDOW_TABLE_SUFFIX;
                List<Row> rows =
                        StateCatalogTestUtils.collect(
                                tableEnv,
                                "SELECT * FROM `" + flattenedWindowTable + "` ORDER BY list_index");
                assertEquals(2, rows.size());
                Row first = rows.get(0).getFieldAs("list_value");
                Row second = rows.get(1).getFieldAs("list_value");
                assertEquals("shared", first.getField("name"));
                assertEquals(10L, first.getField("value"));
                assertEquals("shared", second.getField("name"));
                assertEquals(20L, second.getField("value"));
            } finally {
                catalog.close();
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void testGlobalWindowReduceExposesZeroFieldWindowColumn() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        Tuple2<String, Long>[] data = new Tuple2[] {Tuple2.of("key", 5L), Tuple2.of("key", 7L)};

        env.addSource(createSource(data))
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(2))
                .reduce(
                        (ReduceFunction<Tuple2<String, Long>>)
                                (v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1))
                .uid(GLOBAL_REDUCE_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(GLOBAL_REDUCE_UID);

        KeyedStateSchemaInfo windowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, opId);
        assertTrue(windowSchema.stateSchemas.containsKey("window-contents"));
        KeyedStateSchemaInfo.StateEntryInfo entryInfo =
                windowSchema.stateSchemas.get("window-contents");
        assertEquals(StateType.VALUE, entryInfo.stateType);
        assertTrue(
                entryInfo.windowLogicalType instanceof RowType,
                "Expected the GlobalWindow namespace to be a zero-field ROW");
        assertTrue(((RowType) entryInfo.windowLogicalType).getFields().isEmpty());

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String windowTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + GLOBAL_REDUCE_UID
                            + StateCatalog.WINDOW_TABLE_SUFFIX;
            List<Row> rows =
                    StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + windowTable + "`");
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("key", row.getField("state_key"));
            Row reduced = row.getFieldAs("window-contents");
            assertEquals("key", reduced.getField("f0"));
            assertEquals(12L, reduced.getField("f1"));

            Row window = row.getFieldAs("state_window");
            assertNotNull(window);
            assertEquals(0, window.getArity());
        } finally {
            catalog.close();
        }
    }

    // Also exercises incremental checkpointing: for RocksDB, this is the mode that produces
    // IncrementalRemoteKeyedStateHandle instances backed by shared state, which
    // SavepointLoader/TableMappingSupport must be able to read through the connector.
    @Test
    public void testExplicitGlobalStateStillExposedViaPlainKeyedTable() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        Tuple2<String, Long>[] data = new Tuple2[] {Tuple2.of("key", 1L), Tuple2.of("key", 4L)};

        env.addSource(createSource(data))
                .keyBy(t -> t.f0)
                .process(
                        new KeyedProcessFunction<String, Tuple2<String, Long>, Void>() {
                            private transient ValueState<Long> sum;

                            @Override
                            public void open(OpenContext openContext) {
                                sum =
                                        getRuntimeContext()
                                                .getState(
                                                        new ValueStateDescriptor<>(
                                                                "explicit-sum",
                                                                BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void processElement(
                                    Tuple2<String, Long> value, Context ctx, Collector<Void> out)
                                    throws Exception {
                                Long current = sum.value();
                                sum.update((current == null ? 0L : current) + value.f1);
                            }
                        })
                .uid(EXPLICIT_GLOBAL_STATE_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(EXPLICIT_GLOBAL_STATE_UID);

        KeyedStateSchemaInfo plainSchema = StateTableUtils.getKeyedStateSchema(metadata, opId);
        assertTrue(plainSchema.stateSchemas.containsKey("explicit-sum"));
        assertEquals(StateType.VALUE, plainSchema.stateSchemas.get("explicit-sum").stateType);

        KeyedStateSchemaInfo windowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, opId);
        assertTrue(
                windowSchema.stateSchemas.isEmpty(),
                "A non-windowed operator must not have any window-scoped state");

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String keyedTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + EXPLICIT_GLOBAL_STATE_UID
                            + StateCatalog.OPERATOR_TABLE_SUFFIX;
            List<Row> rows =
                    StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + keyedTable + "`");
            assertEquals(1, rows.size());
            assertEquals("key", rows.get(0).getField("state_key"));
            assertEquals(5L, rows.get(0).getField("explicit-sum"));
        } finally {
            catalog.close();
        }
    }

    @Test
    public void testWindowOperatorExposesGlobalStateAlongsideWindowState() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(getConfiguration());
        env.setParallelism(1);

        // Two tumbling windows for the same key: [0s, 5s) gets 2 elements, [5s, 10s) gets 3.
        Tuple2<String, Long>[] data =
                new Tuple2[] {
                    Tuple2.of("key", 1000L),
                    Tuple2.of("key", 2000L),
                    Tuple2.of("key", 6000L),
                    Tuple2.of("key", 7000L),
                    Tuple2.of("key", 8000L)
                };

        env.addSource(createSource(data))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.f1))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                // Fire deterministically on every element instead of relying on watermark
                // progress: the test source never completes (see WaitingSource), so the
                // watermark never advances past the last element and an EventTimeTrigger would
                // never fire, meaning process() (and therefore globalState()) would never run.
                .trigger(CountTrigger.of(1))
                // Without this, once the watermark advances past the first window's end (as soon
                // as the second window receives data), WindowOperator purges the first window's
                // state entirely, leaving nothing to observe at savepoint time.
                .allowedLateness(Duration.ofDays(1))
                .process(
                        new ProcessWindowFunction<
                                Tuple2<String, Long>, Void, String, TimeWindow>() {
                            private final ValueStateDescriptor<Long> globalCountDescriptor =
                                    new ValueStateDescriptor<>(
                                            "global-count", BasicTypeInfo.LONG_TYPE_INFO);

                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<Tuple2<String, Long>> elements,
                                    Collector<Void> out)
                                    throws Exception {
                                // Global (void-namespace) state: a running count shared across
                                // all windows for this key, unlike the per-window
                                // "window-contents" list state that WindowOperator maintains
                                // internally to buffer each window's elements. CountTrigger.of(1)
                                // guarantees exactly one process() call per incoming element.
                                ValueState<Long> globalCount =
                                        context.globalState().getState(globalCountDescriptor);
                                Long previous = globalCount.value();
                                globalCount.update((previous == null ? 0L : previous) + 1);
                            }
                        })
                .uid(WINDOW_WITH_GLOBAL_STATE_UID)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepointPath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(WINDOW_WITH_GLOBAL_STATE_UID);

        // "global-count" is void-namespace (per-key, not per-window) state: it belongs to the
        // plain keyed schema/table, not the windowed one, even though this is a window operator.
        KeyedStateSchemaInfo plainSchema = StateTableUtils.getKeyedStateSchema(metadata, opId);
        assertTrue(
                plainSchema.stateSchemas.containsKey("global-count"),
                "Expected 'global-count' (global per-key state) in the plain keyed schema, got: "
                        + plainSchema.stateSchemas.keySet());
        assertEquals(StateType.VALUE, plainSchema.stateSchemas.get("global-count").stateType);
        assertFalse(
                plainSchema.stateSchemas.containsKey("window-contents"),
                "Expected 'window-contents' to be excluded from the plain keyed schema");

        KeyedStateSchemaInfo windowSchema =
                StateTableUtils.getWindowKeyedStateSchema(metadata, opId);
        assertTrue(
                windowSchema.stateSchemas.containsKey("window-contents"),
                "Expected 'window-contents' (per-window state) in the window-keyed schema, got: "
                        + windowSchema.stateSchemas.keySet());
        assertFalse(
                windowSchema.stateSchemas.containsKey("global-count"),
                "Expected 'global-count' to be excluded from the window-keyed schema");

        TableEnvironment tableEnv = StateCatalogTestUtils.newTableEnv();
        StateCatalog catalog = StateCatalogTestUtils.registerCatalog(tableEnv, savepointPath);
        try {
            String dbName = catalog.listDatabases().get(0);
            tableEnv.useCatalog("state");
            tableEnv.useDatabase(dbName);

            String keyedTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + WINDOW_WITH_GLOBAL_STATE_UID
                            + StateCatalog.OPERATOR_TABLE_SUFFIX;
            List<Row> keyedRows =
                    StateCatalogTestUtils.collect(tableEnv, "SELECT * FROM `" + keyedTable + "`");
            assertEquals(1, keyedRows.size());
            assertEquals("key", keyedRows.get(0).getField("state_key"));
            assertEquals(5L, keyedRows.get(0).getField("global-count"));

            String flattenedWindowTable =
                    StateCatalog.OPERATOR_UID_PREFIX
                            + WINDOW_WITH_GLOBAL_STATE_UID
                            + "_window-contents"
                            + StateCatalog.FLAT_WINDOW_TABLE_SUFFIX;
            Table flattenedWindowQuery =
                    tableEnv.sqlQuery("SELECT * FROM `" + flattenedWindowTable + "`");
            assertFalse(
                    flattenedWindowQuery
                            .getResolvedSchema()
                            .getColumnNames()
                            .contains("global-count"),
                    "The windowed table must not expose the global 'global-count' state");

            List<Row> windowRows =
                    StateCatalogTestUtils.collect(
                            tableEnv, "SELECT * FROM `" + flattenedWindowTable + "`");
            List<Long> timestamps = new ArrayList<>();
            for (Row row : windowRows) {
                Row element = row.getFieldAs("list_value");
                timestamps.add(element.<Long>getFieldAs("f1"));
            }
            Collections.sort(timestamps);
            assertEquals(List.of(1000L, 2000L, 6000L, 7000L, 8000L), timestamps);
        } finally {
            catalog.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void assertTimeWindowRowType(LogicalType windowLogicalType) {
        assertTrue(windowLogicalType instanceof RowType);
        List<String> fieldNames = ((RowType) windowLogicalType).getFieldNames();
        assertEquals(List.of("window_start", "window_end"), fieldNames);
    }

    private static long toEpochMilli(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
