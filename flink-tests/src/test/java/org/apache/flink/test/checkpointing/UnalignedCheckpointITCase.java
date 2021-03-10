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
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.BitSet;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
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

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][] {
            new Object[] {
                "non-parallel pipeline with local channels", createPipelineSettings(1, 1, true)
            },
            new Object[] {
                "non-parallel pipeline with remote channels", createPipelineSettings(1, 1, false)
            },
            new Object[] {
                "parallel pipeline with local channels, p = 5", createPipelineSettings(5, 5, true)
            },
            new Object[] {
                "parallel pipeline with remote channels, p = 5", createPipelineSettings(5, 1, false)
            },
            new Object[] {
                "parallel pipeline with mixed channels, p = 5", createPipelineSettings(5, 3, true)
            },
            new Object[] {
                "parallel pipeline with mixed channels, p = 20",
                createPipelineSettings(20, 10, true)
            },
            new Object[] {
                "parallel pipeline with mixed channels, p = 20, timeout=1",
                createPipelineSettings(20, 10, true, 1)
            },
            new Object[] {"Parallel cogroup, p = 5", createCogroupSettings(5)},
            new Object[] {"Parallel cogroup, p = 10", createCogroupSettings(10)},
            new Object[] {"Parallel union, p = 5", createUnionSettings(5)},
            new Object[] {"Parallel union, p = 10", createUnionSettings(10)},
        };
    }

    private static UnalignedSettings createPipelineSettings(
            int parallelism, int slotsPerTaskManager, boolean slotSharing) {
        return createPipelineSettings(parallelism, slotsPerTaskManager, slotSharing, 0);
    }

    private static UnalignedSettings createPipelineSettings(
            int parallelism, int slotsPerTaskManager, boolean slotSharing, int timeout) {
        int numShuffles = 4;
        return new UnalignedSettings(UnalignedCheckpointITCase::createPipeline)
                .setParallelism(parallelism)
                .setSlotSharing(slotSharing)
                .setNumSlots(slotSharing ? parallelism : parallelism * numShuffles)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .setExpectedFailures(5)
                .setFailuresAfterSourceFinishes(1)
                .setAlignmentTimeout(timeout);
    }

    private static UnalignedSettings createCogroupSettings(int parallelism) {
        int numShuffles = 10;
        return new UnalignedSettings(UnalignedCheckpointITCase::createMultipleInputTopology)
                .setParallelism(parallelism)
                .setSlotSharing(true)
                .setNumSlots(parallelism * numShuffles)
                .setSlotsPerTaskManager(parallelism)
                .setExpectedFailures(5)
                .setFailuresAfterSourceFinishes(1);
    }

    private static UnalignedSettings createUnionSettings(int parallelism) {
        int numShuffles = 6;
        return new UnalignedSettings(UnalignedCheckpointITCase::createUnionTopology)
                .setParallelism(parallelism)
                .setSlotSharing(true)
                .setNumSlots(parallelism * numShuffles)
                .setSlotsPerTaskManager(parallelism)
                .setExpectedFailures(5)
                .setFailuresAfterSourceFinishes(1);
    }

    private final UnalignedSettings settings;

    public UnalignedCheckpointITCase(String desc, UnalignedSettings settings) {
        this.settings = settings;
    }

    @Test(timeout = 60_000)
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

    private static void createPipeline(
            StreamExecutionEnvironment env,
            int minCheckpoints,
            boolean slotSharing,
            int expectedRestarts) {
        final int parallelism = env.getParallelism();
        final SingleOutputStreamOperator<Long> stream =
                env.fromSource(
                                new LongSource(minCheckpoints, parallelism, expectedRestarts),
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

    private static void createMultipleInputTopology(
            StreamExecutionEnvironment env,
            int minCheckpoints,
            boolean slotSharing,
            int expectedRestarts) {
        final int parallelism = env.getParallelism();
        DataStream<Long> combinedSource = null;
        for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
            final SingleOutputStreamOperator<Long> source =
                    env.fromSource(
                                    new LongSource(minCheckpoints, parallelism, expectedRestarts),
                                    noWatermarks(),
                                    "source" + inputIndex)
                            .slotSharingGroup(slotSharing ? "default" : ("source" + inputIndex))
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

    private static void createUnionTopology(
            StreamExecutionEnvironment env,
            int minCheckpoints,
            boolean slotSharing,
            int expectedRestarts) {
        final int parallelism = env.getParallelism();
        DataStream<Long> combinedSource = null;
        for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
            final SingleOutputStreamOperator<Long> source =
                    env.fromSource(
                                    new LongSource(minCheckpoints, parallelism, expectedRestarts),
                                    noWatermarks(),
                                    "source" + inputIndex)
                            .slotSharingGroup(slotSharing ? "default" : ("source" + inputIndex))
                            .disableChaining();
            combinedSource = combinedSource == null ? source : combinedSource.union(source);
        }

        final SingleOutputStreamOperator<Long> deduplicated =
                combinedSource
                        .partitionCustom(
                                (key, numPartitions) -> (int) (withoutHeader(key) % numPartitions),
                                l -> l)
                        .flatMap(new CountingMapFunction(NUM_SOURCES));
        addFailingPipeline(minCheckpoints, slotSharing, deduplicated);
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
                .slotSharingGroup(slotSharing ? "default" : "map")
                .partitionCustom(new ChunkDistributingPartitioner(), l -> l)
                .addSink(new StrictOrderVerifyingSink(minCheckpoints))
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

        protected StrictOrderVerifyingSink(long minCheckpoints) {
            super(minCheckpoints);
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

            if (backpressure) {
                // induce backpressure until enough checkpoints have been written
                Thread.sleep(1);
            }
            // after all checkpoints have been completed, the remaining data should be flushed out
            // fairly quickly
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

    private static class CountingMapFunction extends RichFlatMapFunction<Long, Long>
            implements CheckpointedFunction {
        private BitSet seenRecords;

        private final int withdrawnCount;

        private ListState<BitSet> stateList;

        public CountingMapFunction(int numSources) {
            withdrawnCount = numSources - 1;
        }

        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            long baseValue = withoutHeader(value);
            final int offset = StrictMath.toIntExact(baseValue * withdrawnCount);
            for (int index = 0; index < withdrawnCount; index++) {
                if (!seenRecords.get(index + offset)) {
                    seenRecords.set(index + offset);
                    return;
                }
            }
            out.collect(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateList.clear();
            stateList.add(seenRecords);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateList =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "state", new GenericTypeInfo<>(BitSet.class)));
            seenRecords = getOnlyElement(stateList.get(), new BitSet());
        }
    }
}
