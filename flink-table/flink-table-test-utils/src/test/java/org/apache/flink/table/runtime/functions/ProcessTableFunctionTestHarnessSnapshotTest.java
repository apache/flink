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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness.TableArgument;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness.TestSnapshot;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ProcessTableFunctionTestHarness#snapshot()} and its restoration. */
class ProcessTableFunctionTestHarnessSnapshotTest {

    /** Emits the input row if its value reaches the given threshold. */
    @DataTypeHint("ROW<value INT>")
    public static class ThresholdFilterPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer threshold) {
            int value = input.getFieldAs("value");
            if (value >= threshold) {
                collect(input);
            }
        }
    }

    /** Counts rows per partition in POJO state. */
    @DataTypeHint("ROW<count BIGINT>")
    public static class CountingPTF extends ProcessTableFunction<Row> {
        /** Counter held as structured-type state. */
        public static class CounterState {
            public long counter = 0L;
        }

        public void eval(
                @StateHint CounterState state,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            state.counter++;
            collect(Row.of(state.counter));
        }
    }

    /** Accumulates all seen values per partition in {@link ListView} state. */
    @DataTypeHint("ROW<values ARRAY<INT>>")
    public static class ListStatePTF extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(type = @DataTypeHint("ARRAY<INT>")) ListView<Integer> listState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            listState.add(input.<Integer>getFieldAs("value"));

            final List<Integer> values = new ArrayList<>();
            for (Integer value : listState.get()) {
                values.add(value);
            }
            collect(Row.of((Object) values.toArray(new Integer[0])));
        }
    }

    /** Counts occurrences of every key per partition in {@link MapView} state. */
    @DataTypeHint("ROW<key STRING, count INT>")
    public static class MapStatePTF extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint MapView<String, Integer> mapState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            final String key = input.getFieldAs("key");
            final Integer count = mapState.get(key);
            mapState.put(key, count == null ? 1 : count + 1);
            collect(Row.of(key, mapState.get(key)));
        }
    }

    /** Counts rows per partition in {@link Row} state. */
    @DataTypeHint("ROW<count BIGINT>")
    public static class RowStatePTF extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(type = @DataTypeHint("ROW<count BIGINT>")) Row memory,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            long count = 1L;
            if (memory.getField("count") != null) {
                count += memory.<Long>getFieldAs("count");
            }
            memory.setField("count", count);
            collect(Row.of(count));
        }
    }

    /** Accumulates a running total per partition in {@link ValueView} state. */
    @DataTypeHint("ROW<total INT>")
    public static class ValueViewStatePTF extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint ValueView<Integer> total,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            Integer current = total.getValue();
            if (current == null) {
                current = 0;
            }
            current += input.<Integer>getFieldAs("value");
            total.setValue(current);
            collect(Row.of(current));
        }
    }

    /** Registers a named timer five seconds after the row's event time. */
    @DataTypeHint("ROW<message STRING>")
    public static class NamedTimerPTF extends ProcessTableFunction<Row> {
        public void eval(
                Context ctx,
                @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                        Row input) {
            final TimeContext<LocalDateTime> timeCtx = ctx.timeContext(LocalDateTime.class);
            timeCtx.registerOnTime("myTimer", timeCtx.time().plus(Duration.ofSeconds(5)));
        }

        public void onTimer(OnTimerContext ctx) {
            collect(Row.of("fired-" + ctx.currentTimer()));
        }
    }

    /** Registers a timer one millisecond after the current watermark. */
    @DataTypeHint("ROW<message STRING>")
    public static class WatermarkTimerPTF extends ProcessTableFunction<Row> {
        public void eval(Context ctx, @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            final Long watermark = timeCtx.currentWatermark();
            timeCtx.registerOnTime("t", watermark == null ? 1L : watermark + 1);
            collect(Row.of("registered"));
        }

        public void onTimer(OnTimerContext ctx) {
            collect(Row.of("fired-" + ctx.currentTimer()));
        }
    }

    /** Counts its invocations on the function instance instead of in state. */
    @DataTypeHint("ROW<invocation INT>")
    public static class InstanceCountingPTF extends ProcessTableFunction<Row> {
        private int invocations = 0;

        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            invocations++;
            collect(Row.of(invocations));
        }
    }

    // -------------------------------------------------------------------------
    // Output
    // -------------------------------------------------------------------------

    @Test
    void testSnapshotAndRestoreOutput() throws Exception {
        final TestSnapshot<Row> snapshot;

        try (ProcessTableFunctionTestHarness<Row> harness = filterHarness()) {
            harness.processElement(Row.of(100));
            harness.processElement(Row.of(30));
            assertThat(harness.getOutput()).containsExactly(Row.of(100));

            snapshot = harness.snapshot();

            harness.processElement(Row.of(75));
            assertThat(harness.getOutput()).containsExactly(Row.of(100), Row.of(75));
        }

        try (ProcessTableFunctionTestHarness<Row> restored =
                ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
            assertThat(restored.getOutput()).containsExactly(Row.of(100));
            assertThat(restored.getFunctionOutput()).containsExactly(Row.of(100));

            restored.processElement(Row.of(75));
            assertThat(restored.getOutput()).containsExactly(Row.of(100), Row.of(75));
        }
    }

    @Test
    void testSnapshotIsNotAffectedByLaterOutput() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness = filterHarness()) {
            harness.processElement(Row.of(100));
            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.processElement(Row.of(75));
            harness.clearOutput();

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                assertThat(restored.getOutput()).containsExactly(Row.of(100));
            }
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    @Test
    void testSnapshotAndRestoreValueState() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness = partitionedHarness(CountingPTF.class)) {
            harness.processElement(Row.of("Alice", 10));
            harness.processElement(Row.of("Alice", 15));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.processElement(Row.of("Alice", 20));
            final CountingPTF.CounterState live = harness.getStateForKey("state", Row.of("Alice"));
            assertThat(live.counter).isEqualTo(3L);

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                final CountingPTF.CounterState state =
                        restored.getStateForKey("state", Row.of("Alice"));
                assertThat(state.counter).isEqualTo(2L);

                restored.clearOutput();
                restored.processElement(Row.of("Alice", 20));
                assertThat(restored.getOutput()).containsExactly(Row.of("Alice", 3L));
            }
        }
    }

    @Test
    void testSnapshotIsNotAffectedByLaterStateChanges() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness = partitionedHarness(CountingPTF.class)) {
            harness.processElement(Row.of("Alice", 10));
            harness.processElement(Row.of("Alice", 15));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.clearStateForKey("state", Row.of("Alice"));
            harness.clearAllStatesForKey(Row.of("Alice"));

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                final CountingPTF.CounterState state =
                        restored.getStateForKey("state", Row.of("Alice"));
                assertThat(state.counter).isEqualTo(2L);
            }
        }
    }

    @Test
    void testSnapshotAndRestoreListViewState() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                partitionedHarness(ListStatePTF.class)) {
            harness.processElement(Row.of("Alice", 1));
            harness.processElement(Row.of("Alice", 2));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.processElement(Row.of("Alice", 3));

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                final ListView<Integer> listState =
                        restored.getStateForKey("listState", Row.of("Alice"));
                assertThat(listState.get()).containsExactly(1, 2);

                restored.clearOutput();
                restored.processElement(Row.of("Alice", 9));
                assertThat(restored.getOutput())
                        .containsExactly(Row.of("Alice", new Integer[] {1, 2, 9}));
            }
        }
    }

    @Test
    void testSnapshotAndRestoreMapViewState() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MapStatePTF.class)
                        .withTableArgument(
                                TableArgument.forName("input")
                                        .type(DataTypes.of("ROW<name STRING, key STRING>"))
                                        .partitionBy("name")
                                        .build())
                        .build()) {

            harness.processElement(Row.of("Alice", "foo"));
            harness.processElement(Row.of("Alice", "foo"));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.processElement(Row.of("Alice", "foo"));

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                final MapView<String, Integer> mapState =
                        restored.getStateForKey("mapState", Row.of("Alice"));
                assertThat(mapState.get("foo")).isEqualTo(2);

                restored.clearOutput();
                restored.processElement(Row.of("Alice", "foo"));
                assertThat(restored.getOutput()).containsExactly(Row.of("Alice", "foo", 3));
            }
        }
    }

    @Test
    void testSnapshotAndRestoreRowState() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness = partitionedHarness(RowStatePTF.class)) {
            harness.processElement(Row.of("Alice", 10));
            harness.processElement(Row.of("Alice", 20));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            harness.processElement(Row.of("Alice", 30));

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                final Row state = restored.getStateForKey("memory", Row.of("Alice"));
                assertThat((Long) state.getFieldAs("count")).isEqualTo(2L);

                restored.clearOutput();
                restored.processElement(Row.of("Alice", 30));
                assertThat(restored.getOutput()).containsExactly(Row.of("Alice", 3L));
            }
        }
    }

    @Test
    void testSnapshotAndRestoreValueViewState() throws Exception {
        final TestSnapshot<Row> snapshot;

        try (ProcessTableFunctionTestHarness<Row> harness =
                partitionedHarness(ValueViewStatePTF.class)) {
            harness.processElement(Row.of("Alice", 10));
            harness.processElement(Row.of("Alice", 20));

            snapshot = harness.snapshot();

            harness.processElement(Row.of("Alice", 30));
            final ValueView<Integer> live = harness.getStateForKey("total", Row.of("Alice"));
            assertThat(live.getValue()).isEqualTo(60);
        }

        try (ProcessTableFunctionTestHarness<Row> restored =
                ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
            final ValueView<Integer> state = restored.getStateForKey("total", Row.of("Alice"));
            assertThat(state.getValue()).isEqualTo(30);

            restored.processElement(Row.of("Alice", 5));
            assertThat(restored.getOutput().get(2)).isEqualTo(Row.of("Alice", 35));
        }
    }

    // -------------------------------------------------------------------------
    // Timers & Watermarks
    // -------------------------------------------------------------------------

    @Test
    void testSnapshotAndRestoreTimers() throws Exception {
        final TestSnapshot<Row> snapshot;

        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(NamedTimerPTF.class)
                        .withTableArgument(
                                TableArgument.forName("input")
                                        .type(DataTypes.of("ROW<name STRING, ts TIMESTAMP(3)>"))
                                        .partitionBy("name")
                                        .build())
                        .withOnTimeColumn("ts")
                        .build()) {

            harness.processElement(Row.of("Alice", LocalDateTime.of(2025, 1, 1, 0, 0, 1)));
            harness.processElement(Row.of("Bob", LocalDateTime.of(2025, 1, 1, 0, 0, 10)));
            harness.setWatermark(LocalDateTime.of(2025, 1, 1, 0, 0, 6));

            assertThat(harness.getFiredTimers()).hasSize(1);
            assertThat(harness.getPendingTimers()).hasSize(1);

            snapshot = harness.snapshot();

            // firing the remaining timer afterwards must not show up in the snapshot
            harness.setWatermark(LocalDateTime.of(2025, 1, 1, 0, 0, 15));
            assertThat(harness.getFiredTimers()).hasSize(2);
        }

        try (ProcessTableFunctionTestHarness<Row> restored =
                ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {

            assertThat(restored.getFiredTimers()).hasSize(1);
            assertThat(restored.getFiredTimers().get(0).getKey()).isEqualTo(Row.of("Alice"));
            assertThat(restored.getFiredTimers().get(0).hasFired()).isTrue();

            assertThat(restored.getPendingTimers()).hasSize(1);
            assertThat(restored.getPendingTimers().get(0).getKey()).isEqualTo(Row.of("Bob"));
            assertThat(restored.getPendingTimers().get(0).hasFired()).isFalse();

            assertThat(restored.getOutput())
                    .containsExactly(
                            Row.of(
                                    "Alice",
                                    "fired-myTimer",
                                    LocalDateTime.of(2025, 1, 1, 0, 0, 6)));

            restored.setWatermark(LocalDateTime.of(2025, 1, 1, 0, 0, 15));
            assertThat(restored.getOutput())
                    .containsExactly(
                            Row.of("Alice", "fired-myTimer", LocalDateTime.of(2025, 1, 1, 0, 0, 6)),
                            Row.of("Bob", "fired-myTimer", LocalDateTime.of(2025, 1, 1, 0, 0, 15)));
        }
    }

    @Test
    void testSnapshotAndRestoreWatermark() throws Exception {
        final TestSnapshot<Row> snapshot;

        try (ProcessTableFunctionTestHarness<Row> harness =
                partitionedHarness(WatermarkTimerPTF.class)) {
            harness.setWatermark(Instant.ofEpochMilli(1000));
            snapshot = harness.snapshot();
        }

        try (ProcessTableFunctionTestHarness<Row> restored =
                ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {

            // the PTF registers its timer at currentWatermark() + 1
            restored.processElement(Row.of("Alice", 42));
            assertThat(restored.getPendingTimers()).hasSize(1);
            assertThat(restored.getPendingTimers().get(0).getTimestamp()).isEqualTo(1001L);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> restored.setWatermark(Instant.ofEpochMilli(500)));
        }
    }

    // -------------------------------------------------------------------------
    // Restoration
    // -------------------------------------------------------------------------

    @Test
    void testSnapshotCanBeRestoredMoreThanOnce() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness = partitionedHarness(CountingPTF.class)) {
            harness.processElement(Row.of("Alice", 10));
            final TestSnapshot<Row> snapshot = harness.snapshot();

            try (ProcessTableFunctionTestHarness<Row> first =
                            ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot);
                    ProcessTableFunctionTestHarness<Row> second =
                            ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {

                first.processElement(Row.of("Alice", 20));
                first.processElement(Row.of("Alice", 30));

                final CountingPTF.CounterState firstState =
                        first.getStateForKey("state", Row.of("Alice"));
                final CountingPTF.CounterState secondState =
                        second.getStateForKey("state", Row.of("Alice"));

                assertThat(firstState.counter).isEqualTo(3L);
                assertThat(secondState.counter).isEqualTo(1L);
                assertThat(second.getOutput()).containsExactly(Row.of("Alice", 1L));
            }
        }
    }

    @Test
    void testRestoreDropsStateThatWasClearedBeforeTheSnapshot() throws Exception {
        final CountingPTF.CounterState initial = new CountingPTF.CounterState();
        initial.counter = 5L;

        final TestSnapshot<Row> snapshot;
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(CountingPTF.class)
                        .withTableArgument(
                                TableArgument.forName("input")
                                        .type(DataTypes.of("ROW<name STRING, value INT>"))
                                        .partitionBy("name")
                                        .build())
                        .withInitialStateForKey("state", Row.of("Bob"), initial)
                        .build()) {

            harness.clearAllStatesForKey(Row.of("Bob"));
            snapshot = harness.snapshot();
        }

        try (ProcessTableFunctionTestHarness<Row> restored =
                ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
            // the initial state configured on the builder must not come back on a restore
            assertThat(restored.getKeysForState("state")).isEmpty();
        }
    }

    @Test
    void testRestoreUsesAFreshFunctionInstance() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(InstanceCountingPTF.class)
                        .withTableArgument(
                                TableArgument.forName("input")
                                        .type(DataTypes.of("ROW<value INT>"))
                                        .build())
                        .build()) {

            harness.processElement(Row.of(1));
            harness.processElement(Row.of(2));
            assertThat(harness.getOutput()).containsExactly(Row.of(1), Row.of(2));

            final TestSnapshot<Row> snapshot = harness.snapshot();

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                restored.clearOutput();
                restored.processElement(Row.of(0));

                // the counter lives on the function instance, not in state, so it starts over
                assertThat(restored.getOutput()).containsExactly(Row.of(1));
            }
        }
    }

    @Test
    void testSnapshotIsNotAffectedByLaterBuilderConfiguration() throws Exception {
        final ProcessTableFunctionTestHarness.Builder<Row> builder =
                ProcessTableFunctionTestHarness.ofClass(ThresholdFilterPTF.class)
                        .withTableArgument(
                                TableArgument.forName("input")
                                        .type(DataTypes.of("ROW<value INT>"))
                                        .build())
                        .withScalarArgument("threshold", 50);

        try (ProcessTableFunctionTestHarness<Row> harness = builder.build()) {
            harness.processElement(Row.of(100));
            final TestSnapshot<Row> snapshot = harness.snapshot();

            builder.withOnTimeColumn("does-not-exist");

            try (ProcessTableFunctionTestHarness<Row> restored =
                    ProcessTableFunctionTestHarness.restoreFromSnapshot(snapshot)) {
                assertThat(restored.getOutput()).containsExactly(Row.of(100));
            }
        }
    }

    private static ProcessTableFunctionTestHarness<Row> filterHarness() throws Exception {
        return ProcessTableFunctionTestHarness.ofClass(ThresholdFilterPTF.class)
                .withTableArgument(
                        TableArgument.forName("input").type(DataTypes.of("ROW<value INT>")).build())
                .withScalarArgument("threshold", 50)
                .build();
    }

    private static ProcessTableFunctionTestHarness<Row> partitionedHarness(
            Class<? extends ProcessTableFunction<Row>> functionClass) throws Exception {
        return ProcessTableFunctionTestHarness.ofClass(functionClass)
                .withTableArgument(
                        TableArgument.forName("input")
                                .type(DataTypes.of("ROW<name STRING, value INT>"))
                                .partitionBy("name")
                                .build())
                .build();
    }
}
