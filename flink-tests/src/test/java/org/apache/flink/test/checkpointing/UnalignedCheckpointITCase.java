/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.test.checkpointing.UnalignedCheckpointTestBase.ChannelType.LOCAL;
import static org.apache.flink.test.checkpointing.UnalignedCheckpointTestBase.ChannelType.MIXED;
import static org.apache.flink.test.checkpointing.UnalignedCheckpointTestBase.ChannelType.REMOTE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test for performing the unaligned checkpoint.
 *
 * <p>This tests checks for completeness and orderness of results after recovery. In particular, the
 * following topology is used:
 *
 * <ol>
 *   <li>A source that generates unique, monotonically increasing, equidistant longs. Given
 *       parallelism=4, the first subtask generates [0, 4, ...], the second subtask [1, 5, ...].
 *   <li>A shuffle that shifts all outputs of partition i to the input of partition i+1 mod n, such
 *       that the order of records are retained.
 *   <li>A failing map that fails during map, snapshotState, initializeState, and close on certain
 *       checkpoints / run attempts (more see below).
 *   <li>A shuffle that fairly distributes the records deterministically, such that duplicates can
 *       be detected.
 *   <li>A verifying sink that counts all anomalies and exposes counters for verification in the
 *       calling test case.
 * </ol>
 *
 * <p>The tests are executed in a certain degree of parallelism until a given number of checkpoints
 * have been successfully taken. Tests time out to guard against infinite failure loops or if no
 * successful checkpoint has been taken for other reasons.
 *
 * <p>Failures are triggered on certain checkpoints to spread them evenly and run attempts to avoid
 * infinite failure loops. For the tests, the failures are induced in the 1. subtask after m&lt;n
 * successful checkpoints with n the number of successful checkpoints needed to pass the test:
 *
 * <ol>
 *   <li>After {@code m=1/4*n}, map fails.
 *   <li>After {@code m=1/2*n}, snapshotState fails.
 *   <li>After {@code m=3/4*n}, map fails and the corresponding recovery fails once.
 *   <li>At the end, close fails once.
 * </ol>
 *
 * <p>The following verifications are performed.
 *
 * <ul>
 *   <li>The number of outputs should be the number of inputs (no lost records).
 *   <li>No record of source subtask {@code i} can overtake a record of the same subtask
 *       (orderness).
 *   <li>No record should arrive at the sink twice (e.g., being emitted or recovered multiple
 *       times), which tests exactly once.
 *   <li>The number of successful checkpoints is indeed {@code >=n}.
 * </ul>
 */
@RunWith(Parameterized.class)
@Category(FailsWithAdaptiveScheduler.class) // FLINK-21689
public class UnalignedCheckpointITCase extends UnalignedCheckpointTestBase {
    enum Topology implements DagCreator {
        PIPELINE {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {
                final int parallelism = env.getParallelism();
                final SingleOutputStreamOperator<Long> stream =
                        env.fromSource(
                                        new LongSource(
                                                minCheckpoints,
                                                parallelism,
                                                expectedRestarts,
                                                env.getCheckpointInterval()),
                                        noWatermarks(),
                                        "source")
                                .slotSharingGroup(slotSharing ? "default" : "source")
                                .disableChaining()
                                .map(i -> checkHeader(i))
                                .name("forward")
                                .uid("forward")
                                .slotSharingGroup(slotSharing ? "default" : "forward")
                                .keyBy(i -> withoutHeader(i) % parallelism * parallelism)
                                .process(new KeyedIdentityFunction())
                                .name("keyed")
                                .uid("keyed");
                addFailingPipeline(minCheckpoints, slotSharing, stream);
            }
        },

        MULTI_INPUT {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {
                final int parallelism = env.getParallelism();
                DataStream<Long> combinedSource = null;
                for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
                    final SingleOutputStreamOperator<Long> source =
                            env.fromSource(
                                            new LongSource(
                                                    minCheckpoints,
                                                    parallelism,
                                                    expectedRestarts,
                                                    env.getCheckpointInterval()),
                                            noWatermarks(),
                                            "source" + inputIndex)
                                    .slotSharingGroup(
                                            slotSharing ? "default" : ("source" + inputIndex))
                                    .disableChaining();
                    combinedSource =
                            combinedSource == null
                                    ? source
                                    : combinedSource
                                            .connect(source)
                                            .flatMap(new MinEmittingFunction())
                                            .name("min" + inputIndex)
                                            .uid("min" + inputIndex)
                                            .slotSharingGroup(
                                                    slotSharing ? "default" : ("min" + inputIndex));
                }

                addFailingPipeline(minCheckpoints, slotSharing, combinedSource);
            }
        },

        UNION {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {
                final int parallelism = env.getParallelism();
                DataStream<Tuple2<Integer, Long>> combinedSource = null;
                for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
                    int finalInputIndex = inputIndex;
                    final SingleOutputStreamOperator<Tuple2<Integer, Long>> source =
                            env.fromSource(
                                            new LongSource(
                                                    minCheckpoints,
                                                    parallelism,
                                                    expectedRestarts,
                                                    env.getCheckpointInterval()),
                                            noWatermarks(),
                                            "source" + inputIndex)
                                    .slotSharingGroup(
                                            slotSharing ? "default" : ("source" + inputIndex))
                                    .map(i -> new Tuple2<>(finalInputIndex, checkHeader(i)))
                                    .returns(
                                            TypeInformation.of(
                                                    new TypeHint<Tuple2<Integer, Long>>() {}))
                                    .slotSharingGroup(
                                            slotSharing ? "default" : ("source" + inputIndex))
                                    .disableChaining();
                    combinedSource = combinedSource == null ? source : combinedSource.union(source);
                }

                final SingleOutputStreamOperator<Long> deduplicated =
                        combinedSource
                                .flatMap(new SourceAwareMinEmittingFunction(NUM_SOURCES))
                                .name("min")
                                .uid("min")
                                .slotSharingGroup(slotSharing ? "default" : "min");
                addFailingPipeline(minCheckpoints, slotSharing, deduplicated);
            }
        };

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    @Parameterized.Parameters(name = "{0} with {2} channels, p = {1}, timeout = {3}")
    public static Object[][] parameters() {
        Object[] defaults = {Topology.PIPELINE, 1, MIXED, 0};

        Object[][] runs = {
            new Object[] {Topology.PIPELINE, 1, LOCAL},
            new Object[] {Topology.PIPELINE, 1, REMOTE},
            new Object[] {Topology.PIPELINE, 5, LOCAL},
            new Object[] {Topology.PIPELINE, 5, REMOTE},
            new Object[] {Topology.PIPELINE, 20},
            new Object[] {Topology.PIPELINE, 20, MIXED, 1},
            new Object[] {Topology.PIPELINE, 20, MIXED, 5},
            new Object[] {Topology.MULTI_INPUT, 5},
            new Object[] {Topology.MULTI_INPUT, 10},
            new Object[] {Topology.UNION, 5},
            new Object[] {Topology.UNION, 10},
        };
        return Stream.of(runs)
                .map(params -> addDefaults(params, defaults))
                .toArray(Object[][]::new);
    }

    private static Object[] addDefaults(Object[] params, Object[] defaults) {
        return ArrayUtils.addAll(
                params, ArrayUtils.subarray(defaults, params.length, defaults.length));
    }

    private final UnalignedSettings settings;

    public UnalignedCheckpointITCase(
            Topology topology, int parallelism, ChannelType channelType, int timeout) {
        settings =
                new UnalignedSettings(topology)
                        .setParallelism(parallelism)
                        .setChannelTypes(channelType)
                        .setExpectedFailures(5)
                        .setFailuresAfterSourceFinishes(1)
                        // prevent test from timing out in case when a failover happens concurrently
                        // with triggering a checkpoint (some execution status can change right
                        // after triggering)
                        .setCheckpointTimeout(Duration.ofSeconds(30))
                        .setTolerableCheckpointFailures(3)
                        .setAlignmentTimeout(timeout);
    }

    @Test
    public void execute() throws Exception {
        execute(settings);
    }

    protected void checkCounters(JobExecutionResult result) {
        collector.checkThat(
                "NUM_OUT_OF_ORDER",
                result.<Long>getAccumulatorResult(NUM_OUT_OF_ORDER),
                equalTo(0L));
        collector.checkThat(
                "NUM_DUPLICATES", result.<Long>getAccumulatorResult(NUM_DUPLICATES), equalTo(0L));
        collector.checkThat("NUM_LOST", result.<Long>getAccumulatorResult(NUM_LOST), equalTo(0L));
        collector.checkThat(
                "NUM_FAILURES",
                result.<Integer>getAccumulatorResult(NUM_FAILURES),
                equalTo(settings.expectedFailures));
    }

    private static DataStreamSink<Long> addFailingPipeline(
            long minCheckpoints, boolean slotSharing, DataStream<Long> combinedSource) {
        return combinedSource
                // shifts records from one partition to another evenly to retain order
                .partitionCustom(new ShiftingPartitioner(), l -> l)
                .map(
                        new FailingMapper(
                                state ->
                                        state.completedCheckpoints >= minCheckpoints / 4
                                                        && state.runNumber == 0
                                                || state.completedCheckpoints
                                                                >= minCheckpoints * 3 / 4
                                                        && state.runNumber == 2,
                                state ->
                                        state.completedCheckpoints >= minCheckpoints / 2
                                                && state.runNumber == 1,
                                state -> state.runNumber == 3,
                                state -> state.runNumber == 4))
                .name("failing-map")
                .uid("failing-map")
                .slotSharingGroup(slotSharing ? "default" : "failing-map")
                .partitionCustom(new ChunkDistributingPartitioner(), l -> l)
                .addSink(
                        new StrictOrderVerifyingSink(
                                minCheckpoints,
                                combinedSource.getExecutionEnvironment().getCheckpointInterval()))
                .name("sink")
                .uid("sink")
                .slotSharingGroup(slotSharing ? "default" : "sink");
    }

    /**
     * A sink that checks if the members arrive in the expected order without any missing values.
     */
    protected static class StrictOrderVerifyingSink
            extends VerifyingSinkBase<StrictOrderVerifyingSink.State> {
        private boolean firstOutOfOrder = true;
        private boolean firstDuplicate = true;
        private boolean firstLostValue = true;

        protected StrictOrderVerifyingSink(long minCheckpoints, long checkpointingInterval) {
            super(minCheckpoints, checkpointingInterval);
        }

        @Override
        protected State createState() {
            return new State(getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            super.initializeState(context);
        }

        @Override
        public void invoke(Long value, Context context) throws Exception {
            value = withoutHeader(value);
            int parallelism = state.lastRecordInPartitions.length;
            int partition = (int) (value % parallelism);
            long lastRecord = state.lastRecordInPartitions[partition];
            if (value < lastRecord) {
                state.numOutOfOrderness++;
                if (firstOutOfOrder) {
                    LOG.info(
                            "Out of order records current={} and last={} @ {} subtask ({} attempt)",
                            value,
                            lastRecord,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getAttemptNumber());
                    firstOutOfOrder = false;
                }
            } else if (value == lastRecord) {
                state.numDuplicates++;
                if (firstDuplicate) {
                    LOG.info(
                            "Duplicate record {} @ {} subtask ({} attempt)",
                            value,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getAttemptNumber());
                    firstDuplicate = false;
                }
            } else if (lastRecord != -1) {
                long expectedValue = lastRecord + parallelism * parallelism;
                if (value != expectedValue) {
                    state.numLostValues++;
                    if (firstLostValue) {
                        LOG.info(
                                "Lost records current={}, expected={}, and last={} @ {} subtask ({} attempt)",
                                value,
                                expectedValue,
                                lastRecord,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getAttemptNumber());
                        firstLostValue = false;
                    }
                }
            }
            state.lastRecordInPartitions[partition] = value;
            state.numOutput++;

            induceBackpressure();
        }

        static class State extends VerifyingSinkStateBase {
            private final long[] lastRecordInPartitions;

            private State(int numberOfParallelSubtasks) {
                lastRecordInPartitions = new long[numberOfParallelSubtasks];
                Arrays.fill(lastRecordInPartitions, -1);
            }
        }
    }

    private static class KeyedIdentityFunction extends KeyedProcessFunction<Long, Long, Long> {
        ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "keyedState", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<Long> out) {
            checkHeader(value);
            out.collect(value);
        }
    }

    private static class SourceAwareMinEmittingFunction
            extends RichFlatMapFunction<Tuple2<Integer, Long>, Long>
            implements CheckpointedFunction {
        private final int numSources;
        private State state;

        private ListState<State> stateList;

        public SourceAwareMinEmittingFunction(int numSources) {
            this.numSources = numSources;
        }

        @Override
        public void flatMap(Tuple2<Integer, Long> sourceValue, Collector<Long> out)
                throws Exception {
            int source = sourceValue.f0;
            long value = withoutHeader(sourceValue.f1);
            int partition = (int) (value % getRuntimeContext().getNumberOfParallelSubtasks());
            state.lastValues[source][partition] = value;
            for (int index = 0; index < numSources; index++) {
                if (state.lastValues[index][partition] < value) {
                    return;
                }
            }
            out.collect(sourceValue.f1);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateList.clear();
            stateList.add(state);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateList =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", State.class));
            state =
                    getOnlyElement(
                            stateList.get(),
                            new State(
                                    numSources, getRuntimeContext().getNumberOfParallelSubtasks()));
        }

        private static class State {
            private final long[][] lastValues;

            public State(int numSources, int numberOfParallelSubtasks) {
                this.lastValues = new long[numSources][numberOfParallelSubtasks];
                for (long[] lastValue : lastValues) {
                    Arrays.fill(lastValue, Long.MIN_VALUE);
                }
            }
        }
    }
}
