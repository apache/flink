/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils;
import org.apache.flink.test.checkpointing.utils.SnapshotMigrationTestBase;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Migration ITCases for a stateful job. The tests are parameterized to cover migrating for multiple
 * previous Flink versions, as well as for different state backends.
 */
@RunWith(Parameterized.class)
public class StatefulJobSnapshotMigrationITCase extends SnapshotMigrationTestBase
        implements MigrationTest {

    private static final int NUM_SOURCE_ELEMENTS = 4;

    @Parameterized.Parameters(name = "Test snapshot: {0}")
    public static Collection<SnapshotSpec> createSpecsForTestRuns() {
        return internalParameters(null);
    }

    public static Collection<SnapshotSpec> createSpecsForTestDataGeneration(
            FlinkVersion targetVersion) {
        return internalParameters(targetVersion);
    }

    private static Collection<SnapshotSpec> internalParameters(
            @Nullable FlinkVersion targetGeneratingVersion) {
        BiFunction<FlinkVersion, FlinkVersion, Collection<FlinkVersion>> getFlinkVersions =
                (minInclVersion, maxInclVersion) -> {
                    if (targetGeneratingVersion != null) {
                        return FlinkVersion.rangeOf(minInclVersion, maxInclVersion).stream()
                                .filter(v -> v.equals(targetGeneratingVersion))
                                .collect(Collectors.toList());
                    } else {
                        return FlinkVersion.rangeOf(minInclVersion, maxInclVersion);
                    }
                };

        Collection<SnapshotSpec> parameters = new LinkedList<>();
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.MEMORY_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        getFlinkVersions.apply(FlinkVersion.v1_8, FlinkVersion.v1_14)));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_15,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_CANONICAL,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_8,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_NATIVE,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_15,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.SAVEPOINT_NATIVE,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_15,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
                        SnapshotType.CHECKPOINT,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_15,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        parameters.addAll(
                SnapshotSpec.withVersions(
                        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
                        SnapshotType.CHECKPOINT,
                        getFlinkVersions.apply(
                                FlinkVersion.v1_15,
                                MigrationTest.getMostRecentlyPublishedVersion())));
        return parameters;
    }

    private final SnapshotSpec snapshotSpec;

    public StatefulJobSnapshotMigrationITCase(SnapshotSpec snapshotSpec) throws Exception {
        this.snapshotSpec = snapshotSpec;
    }

    @ParameterizedSnapshotsGenerator("createSpecsForTestDataGeneration")
    public void generateSnapshots(SnapshotSpec snapshotSpec) throws Exception {
        testOrCreateSavepoint(ExecutionMode.CREATE_SNAPSHOT, snapshotSpec);
    }

    @Test
    public void testSavepoint() throws Exception {
        testOrCreateSavepoint(ExecutionMode.VERIFY_SNAPSHOT, snapshotSpec);
    }

    private void testOrCreateSavepoint(ExecutionMode executionMode, SnapshotSpec snapshotSpec)
            throws Exception {

        final int parallelism = 4;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        switch (snapshotSpec.getStateBackendType()) {
            case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
                env.setStateBackend(new EmbeddedRocksDBStateBackend());

                if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
                    // disable changelog backend for now to ensure determinism in test data
                    // generation (see FLINK-31766)
                    env.enableChangelogStateBackend(false);
                }
                break;
            case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
                env.setStateBackend(new MemoryStateBackend());
                break;
            case StateBackendLoader.HASHMAP_STATE_BACKEND_NAME:
                env.setStateBackend(new HashMapStateBackend());
                break;
            default:
                throw new UnsupportedOperationException();
        }

        env.enableCheckpointing(500);
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);

        SourceFunction<Tuple2<Long, Long>> nonParallelSource;
        SourceFunction<Tuple2<Long, Long>> parallelSource;
        RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> flatMap;
        OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> timelyOperator;

        if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
            nonParallelSource =
                    new MigrationTestUtils.CheckpointingNonParallelSourceWithListState(
                            NUM_SOURCE_ELEMENTS);
            parallelSource =
                    new MigrationTestUtils.CheckpointingParallelSourceWithUnionListState(
                            NUM_SOURCE_ELEMENTS);
            flatMap = new CheckpointingKeyedStateFlatMap();
            timelyOperator = new CheckpointingTimelyStatefulOperator();
        } else if (executionMode == ExecutionMode.VERIFY_SNAPSHOT) {
            nonParallelSource =
                    new MigrationTestUtils.CheckingNonParallelSourceWithListState(
                            NUM_SOURCE_ELEMENTS);
            parallelSource =
                    new MigrationTestUtils.CheckingParallelSourceWithUnionListState(
                            NUM_SOURCE_ELEMENTS);
            flatMap = new CheckingKeyedStateFlatMap();
            timelyOperator = new CheckingTimelyStatefulOperator();
        } else {
            throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
        }

        env.addSource(nonParallelSource)
                .uid("CheckpointingSource1")
                .keyBy(0)
                .flatMap(flatMap)
                .startNewChain()
                .uid("CheckpointingKeyedStateFlatMap1")
                .keyBy(0)
                .transform(
                        "timely_stateful_operator",
                        new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
                        timelyOperator)
                .uid("CheckpointingTimelyStatefulOperator1")
                .addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

        env.addSource(parallelSource)
                .uid("CheckpointingSource2")
                .keyBy(0)
                .flatMap(flatMap)
                .startNewChain()
                .uid("CheckpointingKeyedStateFlatMap2")
                .keyBy(0)
                .transform(
                        "timely_stateful_operator",
                        new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
                        timelyOperator)
                .uid("CheckpointingTimelyStatefulOperator2")
                .addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

        final String snapshotPath = getSnapshotPath(snapshotSpec);

        if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
            executeAndSnapshot(
                    env,
                    "src/test/resources/" + snapshotPath,
                    snapshotSpec.getSnapshotType(),
                    new Tuple2<>(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2));
        } else {
            restoreAndExecute(
                    env,
                    getResourceFilename(snapshotPath),
                    new Tuple2<>(
                            MigrationTestUtils.CheckingNonParallelSourceWithListState
                                    .SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR,
                            1),
                    new Tuple2<>(
                            MigrationTestUtils.CheckingParallelSourceWithUnionListState
                                    .SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR,
                            parallelism),
                    new Tuple2<>(
                            CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2),
                    new Tuple2<>(
                            CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2),
                    new Tuple2<>(
                            CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2),
                    new Tuple2<>(
                            CheckingTimelyStatefulOperator
                                    .SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2),
                    new Tuple2<>(
                            MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
                            NUM_SOURCE_ELEMENTS * 2));
        }
    }

    private static String getSnapshotPath(SnapshotSpec snapshotSpec) {
        return "new-stateful-udf-migration-itcase-" + snapshotSpec;
    }

    private static class CheckpointingKeyedStateFlatMap
            extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out)
                throws Exception {
            out.collect(value);

            getRuntimeContext().getState(stateDescriptor).update(value.f1);
        }
    }

    private static class CheckingKeyedStateFlatMap
            extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private static final long serialVersionUID = 1L;

        static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR =
                CheckingKeyedStateFlatMap.class + "_RESTORE_CHECK";

        private final ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            getRuntimeContext()
                    .addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out)
                throws Exception {
            out.collect(value);

            ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
            if (state == null) {
                throw new RuntimeException("Missing key value state for " + value);
            }

            assertEquals(value.f1, state.value());
            getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
        }
    }

    private static class CheckpointingTimelyStatefulOperator
            extends AbstractStreamOperator<Tuple2<Long, Long>>
            implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>,
                    Triggerable<Long, Long> {
        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

        private transient InternalTimerService<Long> timerService;

        @Override
        public void open() throws Exception {
            super.open();

            timerService = getInternalTimerService("timer", LongSerializer.INSTANCE, this);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
            ValueState<Long> state =
                    getKeyedStateBackend()
                            .getPartitionedState(
                                    element.getValue().f0,
                                    LongSerializer.INSTANCE,
                                    stateDescriptor);

            state.update(element.getValue().f1);

            timerService.registerEventTimeTimer(
                    element.getValue().f0, timerService.currentWatermark() + 10);
            timerService.registerProcessingTimeTimer(
                    element.getValue().f0, timerService.currentProcessingTime() + 30_000);

            output.collect(element);
        }

        @Override
        public void onEventTime(InternalTimer<Long, Long> timer) {}

        @Override
        public void onProcessingTime(InternalTimer<Long, Long> timer) {}

        @Override
        public void processWatermark(Watermark mark) {
            output.emitWatermark(mark);
        }
    }

    private static class CheckingTimelyStatefulOperator
            extends AbstractStreamOperator<Tuple2<Long, Long>>
            implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>,
                    Triggerable<Long, Long> {
        private static final long serialVersionUID = 1L;

        static final String SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR =
                CheckingTimelyStatefulOperator.class + "_PROCESS_CHECKS";
        static final String SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR =
                CheckingTimelyStatefulOperator.class + "_ET_CHECKS";
        static final String SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR =
                CheckingTimelyStatefulOperator.class + "_PT_CHECKS";

        private final ValueStateDescriptor<Long> stateDescriptor =
                new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

        @Override
        public void open() throws Exception {
            super.open();

            // have to re-register to ensure that our onEventTime() is called
            getInternalTimerService("timer", LongSerializer.INSTANCE, this);

            getRuntimeContext()
                    .addAccumulator(SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, new IntCounter());
            getRuntimeContext()
                    .addAccumulator(SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, new IntCounter());
            getRuntimeContext()
                    .addAccumulator(SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, new IntCounter());
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
            ValueState<Long> state =
                    getKeyedStateBackend()
                            .getPartitionedState(
                                    element.getValue().f0,
                                    LongSerializer.INSTANCE,
                                    stateDescriptor);

            assertEquals(state.value(), element.getValue().f1);
            getRuntimeContext().getAccumulator(SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR).add(1);

            output.collect(element);
        }

        @Override
        public void onEventTime(InternalTimer<Long, Long> timer) throws Exception {
            ValueState<Long> state =
                    getKeyedStateBackend()
                            .getPartitionedState(
                                    timer.getNamespace(), LongSerializer.INSTANCE, stateDescriptor);

            assertEquals(state.value(), timer.getNamespace());
            getRuntimeContext().getAccumulator(SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR).add(1);
        }

        @Override
        public void onProcessingTime(InternalTimer<Long, Long> timer) throws Exception {
            ValueState<Long> state =
                    getKeyedStateBackend()
                            .getPartitionedState(
                                    timer.getNamespace(), LongSerializer.INSTANCE, stateDescriptor);

            assertEquals(state.value(), timer.getNamespace());
            getRuntimeContext().getAccumulator(SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR).add(1);
        }
    }
}
