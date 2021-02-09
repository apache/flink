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
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.BitSet;
import java.util.Collections;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;

/** Integration test for performing rescale of unaligned checkpoint. */
@RunWith(Parameterized.class)
public class UnalignedCheckpointRescaleITCase extends UnalignedCheckpointTestBase {
    public static final int NUM_GROUPS = 100;
    private final Topology topology;
    private final int oldParallelism;
    private final int newParallelism;

    enum Topology {
        PIPELINE {
            @Override
            UnalignedSettings createSettings(int parallelism) {
                int numShuffles = 8;
                final int numSlots = parallelism * numShuffles + 3;
                // aim for 3 TMs
                int slotsPerTaskManager = (numSlots + 2) / 3;
                return new UnalignedSettings(this::createPipeline)
                        .setParallelism(parallelism)
                        .setSlotSharing(false)
                        .setNumSlots(numSlots)
                        .setSlotsPerTaskManager(slotsPerTaskManager)
                        .setExpectedFailures(1);
            }

            private void createPipeline(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {
                final int parallelism = env.getParallelism();
                final DataStream<Long> source =
                        createSourcePipeline(
                                env,
                                minCheckpoints,
                                slotSharing,
                                expectedRestarts,
                                parallelism,
                                0,
                                val -> true);
                addFailingSink(source, minCheckpoints, slotSharing);
            }
        },

        MULTI_INPUT {
            @Override
            UnalignedSettings createSettings(int parallelism) {
                int numShuffles = NUM_SOURCES * 8 + 1;
                int numSlots = parallelism * numShuffles + 3 * NUM_SOURCES;
                // aim for 3 TMs
                int slotsPerTaskManager = (numSlots + 2) / 3;
                return new UnalignedSettings(this::createPipeline)
                        .setParallelism(parallelism)
                        .setSlotSharing(false)
                        .setNumSlots(numSlots)
                        .setSlotsPerTaskManager(slotsPerTaskManager)
                        .setExpectedFailures(1);
            }

            private void createPipeline(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {

                final int parallelism = env.getParallelism();
                DataStream<Long> combinedSource = null;
                for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
                    int finalInputIndex = inputIndex;
                    final DataStream<Long> source =
                            createSourcePipeline(
                                    env,
                                    minCheckpoints,
                                    slotSharing,
                                    expectedRestarts,
                                    parallelism,
                                    inputIndex,
                                    val -> withoutHeader(val) % NUM_SOURCES == finalInputIndex);
                    combinedSource =
                            combinedSource == null
                                    ? source
                                    : combinedSource
                                            .connect(source)
                                            .map(new UnionLikeCoGroup())
                                            .name("min" + inputIndex)
                                            .uid("min" + inputIndex)
                                            .setParallelism(parallelism + inputIndex)
                                            .slotSharingGroup(
                                                    slotSharing ? "default" : ("min" + inputIndex));
                }

                addFailingSink(combinedSource, minCheckpoints, slotSharing);
            }
        },

        UNION {
            @Override
            UnalignedSettings createSettings(int parallelism) {
                int numShuffles = NUM_SOURCES * 8 + 1;
                int numSlots = parallelism * numShuffles + 3 * NUM_SOURCES;
                // aim for 3 TMs
                int slotsPerTaskManager = (numSlots + 2) / 3;
                return new UnalignedSettings(this::createPipeline)
                        .setParallelism(parallelism)
                        .setSlotSharing(false)
                        .setNumSlots(numSlots)
                        .setSlotsPerTaskManager(slotsPerTaskManager)
                        .setExpectedFailures(1);
            }

            private void createPipeline(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {

                final int parallelism = env.getParallelism();
                DataStream<Long> combinedSource = null;
                for (int inputIndex = 0; inputIndex < NUM_SOURCES; inputIndex++) {
                    int finalInputIndex = inputIndex;
                    final DataStream<Long> source =
                            createSourcePipeline(
                                    env,
                                    minCheckpoints,
                                    slotSharing,
                                    expectedRestarts,
                                    parallelism,
                                    inputIndex,
                                    val -> withoutHeader(val) % NUM_SOURCES == finalInputIndex);
                    combinedSource = combinedSource == null ? source : combinedSource.union(source);
                }

                addFailingSink(combinedSource, minCheckpoints, slotSharing);
            }
        };

        void addFailingSink(
                DataStream<Long> combinedSource, long minCheckpoints, boolean slotSharing) {
            combinedSource
                    .shuffle()
                    .map(
                            new FailingMapper(
                                    state -> false,
                                    state ->
                                            state.completedCheckpoints >= minCheckpoints / 2
                                                    && state.runNumber == 0,
                                    state -> false,
                                    state -> false))
                    .name("failing-map")
                    .uid("failing-map")
                    .slotSharingGroup(slotSharing ? "default" : "failing-map")
                    .shuffle()
                    .addSink(new VerifyingSink(minCheckpoints))
                    .setParallelism(1)
                    .name("sink")
                    .uid("sink")
                    .slotSharingGroup(slotSharing ? "default" : "sink");
        }

        DataStream<Long> createSourcePipeline(
                StreamExecutionEnvironment env,
                int minCheckpoints,
                boolean slotSharing,
                int expectedRestarts,
                int parallelism,
                int inputIndex,
                FilterFunction<Long> sourceFilter) {
            return env.fromSource(
                            new LongSource(minCheckpoints, parallelism, expectedRestarts),
                            noWatermarks(),
                            "source" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("source" + inputIndex))
                    .filter(sourceFilter)
                    .name("input-filter" + inputIndex)
                    .uid("input-filter" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("source" + inputIndex))
                    .map(new InputCountFunction())
                    .name("input-counter" + inputIndex)
                    .uid("input-counter" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("source" + inputIndex))
                    .global()
                    .map(i -> checkHeader(i))
                    .name("global" + inputIndex)
                    .uid("global" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("global" + inputIndex))
                    .rebalance()
                    .map(i -> checkHeader(i))
                    .setParallelism(parallelism + 1)
                    .name("rebalance" + inputIndex)
                    .uid("rebalance" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("rebalance" + inputIndex))
                    .shuffle()
                    .map(i -> checkHeader(i))
                    .name("upscale" + inputIndex)
                    .uid("upscale" + inputIndex)
                    .setParallelism(2 * parallelism)
                    .slotSharingGroup(slotSharing ? "default" : ("upscale" + inputIndex))
                    .shuffle()
                    .map(i -> checkHeader(i))
                    .name("downscale" + inputIndex)
                    .uid("downscale" + inputIndex)
                    .setParallelism(parallelism + 1)
                    .slotSharingGroup(slotSharing ? "default" : ("downscale" + inputIndex))
                    .keyBy(i -> withoutHeader(i) % NUM_GROUPS)
                    .map(new StatefulKeyedMap())
                    .name("keyby" + inputIndex)
                    .uid("keyby" + inputIndex)
                    .slotSharingGroup(slotSharing ? "default" : ("keyby" + inputIndex));
        }

        abstract UnalignedSettings createSettings(int parallelism);

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    @Parameterized.Parameters(name = "{0} {1} from {2} to {3}")
    public static Object[][] getScaleFactors() {
        return new Object[][] {
            new Object[] {"upscale", Topology.PIPELINE, 1, 2},
            new Object[] {"upscale", Topology.PIPELINE, 2, 3},
            new Object[] {"upscale", Topology.PIPELINE, 3, 7},
            new Object[] {"upscale", Topology.PIPELINE, 4, 8},
            new Object[] {"upscale", Topology.PIPELINE, 20, 21},
            new Object[] {"downscale", Topology.PIPELINE, 2, 1},
            new Object[] {"downscale", Topology.PIPELINE, 3, 2},
            new Object[] {"downscale", Topology.PIPELINE, 7, 3},
            new Object[] {"downscale", Topology.PIPELINE, 8, 4},
            new Object[] {"downscale", Topology.PIPELINE, 21, 20},
            new Object[] {"no scale", Topology.PIPELINE, 1, 1},
            new Object[] {"no scale", Topology.PIPELINE, 3, 3},
            new Object[] {"no scale", Topology.PIPELINE, 7, 7},
            new Object[] {"no scale", Topology.PIPELINE, 20, 20},
            new Object[] {"upscale", Topology.UNION, 1, 2},
            new Object[] {"upscale", Topology.UNION, 2, 3},
            new Object[] {"upscale", Topology.UNION, 3, 7},
            new Object[] {"downscale", Topology.UNION, 2, 1},
            new Object[] {"downscale", Topology.UNION, 3, 2},
            new Object[] {"downscale", Topology.UNION, 7, 3},
            new Object[] {"no scale", Topology.UNION, 1, 1},
            new Object[] {"no scale", Topology.UNION, 7, 7},
            new Object[] {"upscale", Topology.MULTI_INPUT, 1, 2},
            new Object[] {"upscale", Topology.MULTI_INPUT, 2, 3},
            new Object[] {"upscale", Topology.MULTI_INPUT, 3, 7},
            new Object[] {"downscale", Topology.MULTI_INPUT, 2, 1},
            new Object[] {"downscale", Topology.MULTI_INPUT, 3, 2},
            new Object[] {"downscale", Topology.MULTI_INPUT, 7, 3},
            new Object[] {"no scale", Topology.MULTI_INPUT, 1, 1},
            new Object[] {"no scale", Topology.MULTI_INPUT, 7, 7},
        };
    }

    public UnalignedCheckpointRescaleITCase(
            String desc, Topology topology, int oldParallelism, int newParallelism) {
        this.topology = topology;
        this.oldParallelism = oldParallelism;
        this.newParallelism = newParallelism;
    }

    @Test
    public void shouldRescaleUnalignedCheckpoint() throws Exception {
        final UnalignedSettings prescaleSettings = topology.createSettings(oldParallelism);
        prescaleSettings.setGenerateCheckpoint(true);
        final File checkpointDir = super.execute(prescaleSettings);

        // resume
        final UnalignedSettings postscaleSettings = topology.createSettings(newParallelism);
        postscaleSettings.setRestoreCheckpoint(checkpointDir);
        super.execute(postscaleSettings);
    }

    protected void checkCounters(JobExecutionResult result) {
        collector.checkThat(
                "NUM_OUTPUTS = NUM_INPUTS",
                result.<Long>getAccumulatorResult(NUM_OUTPUTS),
                equalTo(result.getAccumulatorResult(NUM_INPUTS)));
        collector.checkThat(
                "NUM_DUPLICATES", result.<Long>getAccumulatorResult(NUM_DUPLICATES), equalTo(0L));
    }

    /**
     * A sink that checks if the members arrive in the expected order without any missing values.
     */
    protected static class VerifyingSink extends VerifyingSinkBase<VerifyingSink.State> {
        private boolean firstDuplicate = true;

        protected VerifyingSink(long minCheckpoints) {
            super(minCheckpoints);
        }

        @Override
        protected State createState() {
            return new State();
        }

        @Override
        public void invoke(Long value, Context context) throws Exception {
            final int intValue = (int) withoutHeader(value);
            if (state.encounteredNumbers.get(intValue)) {
                state.numDuplicates++;
                if (firstDuplicate) {
                    LOG.info(
                            "Duplicate record {} @ {} subtask ({} attempt)",
                            intValue,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getAttemptNumber());
                    firstDuplicate = false;
                }
            }
            state.encounteredNumbers.set(intValue);
            state.numOutput++;

            if (backpressure) {
                // induce heavy backpressure until enough checkpoints have been written
                Thread.sleep(1);
            }
            // after all checkpoints have been completed, the remaining data should be flushed out
            // fairly quickly
        }

        @Override
        public void close() throws Exception {
            state.numLostValues =
                    state.encounteredNumbers.length() - state.encounteredNumbers.cardinality();
            super.close();
        }

        static class State extends VerifyingSinkStateBase {
            private final BitSet encounteredNumbers = new BitSet();
        }
    }

    private static class StatefulKeyedMap extends RichMapFunction<Long, Long> {
        private static final ValueStateDescriptor<Long> DESC =
                new ValueStateDescriptor<>("group", LongSerializer.INSTANCE);
        ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(DESC);
        }

        @Override
        public Long map(Long value) throws Exception {
            final Long lastGroup = state.value();
            final long rawValue = withoutHeader(value);
            final long group = rawValue % NUM_GROUPS;
            if (lastGroup != null) {
                checkState(group == lastGroup, "Mismatched key group");
            } else {
                state.update(group);
            }
            return value;
        }
    }

    private static class InputCountFunction extends RichMapFunction<Long, Long>
            implements CheckpointedFunction {
        private static final long serialVersionUID = -1098571965968341646L;
        private final LongCounter numInputCounter = new LongCounter();
        private ListState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(NUM_INPUTS, numInputCounter);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> descriptor =
                    new ListStateDescriptor<>("num-inputs", Types.LONG);
            state = context.getOperatorStateStore().getListState(descriptor);
            for (Long numInputs : state.get()) {
                numInputCounter.add(numInputs);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(Collections.singletonList(numInputCounter.getLocalValue()));
        }

        @Override
        public Long map(Long value) throws Exception {
            numInputCounter.add(1L);
            return checkHeader(value);
        }
    }

    private static class UnionLikeCoGroup implements CoMapFunction<Long, Long, Long> {
        @Override
        public Long map1(Long value) throws Exception {
            return checkHeader(value);
        }

        @Override
        public Long map2(Long value) throws Exception {
            return checkHeader(value);
        }
    }
}
