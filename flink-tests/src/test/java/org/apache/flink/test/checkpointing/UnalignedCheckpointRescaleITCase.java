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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
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
    private final int buffersPerChannel;

    enum Topology implements DagCreator {
        PIPELINE {
            @Override
            public void create(
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
            public void create(
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
                                            .slotSharingGroup(
                                                    slotSharing ? "default" : ("min" + inputIndex));
                }

                addFailingSink(combinedSource, minCheckpoints, slotSharing);
            }
        },

        KEYED_DIFFERENT_PARALLELISM {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {

                final int parallelism = env.getParallelism();
                checkState(parallelism >= 4);
                final DataStream<Long> source1 =
                        createSourcePipeline(
                                env,
                                minCheckpoints,
                                slotSharing,
                                expectedRestarts,
                                parallelism / 2,
                                0,
                                val -> withoutHeader(val) % 2 == 0);
                final DataStream<Long> source2 =
                        createSourcePipeline(
                                env,
                                minCheckpoints,
                                slotSharing,
                                expectedRestarts,
                                parallelism / 3,
                                1,
                                val -> withoutHeader(val) % 2 == 1);

                KeySelector<Long, Long> keySelector = i -> withoutHeader(i) % NUM_GROUPS;
                SingleOutputStreamOperator<Long> connected =
                        source1.connect(source2)
                                .keyBy(keySelector, keySelector)
                                .process(new TestKeyedCoProcessFunction())
                                .setParallelism(parallelism);

                addFailingSink(connected, minCheckpoints, slotSharing);
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
        },

        BROADCAST {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {

                final int parallelism = env.getParallelism();
                final DataStream<Long> broadcastSide =
                        env.fromSource(
                                new LongSource(
                                        minCheckpoints,
                                        parallelism,
                                        expectedRestarts,
                                        env.getCheckpointInterval()),
                                noWatermarks(),
                                "source");
                final DataStream<Long> source =
                        createSourcePipeline(
                                        env,
                                        minCheckpoints,
                                        slotSharing,
                                        expectedRestarts,
                                        parallelism,
                                        0,
                                        val -> true)
                                .map(i -> checkHeader(i))
                                .name("map")
                                .uid("map")
                                .slotSharingGroup(slotSharing ? "default" : "failing-map");

                final MapStateDescriptor<Long, Long> descriptor =
                        new MapStateDescriptor<>(
                                "broadcast",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.LONG_TYPE_INFO);
                final BroadcastStream<Long> broadcast = broadcastSide.broadcast(descriptor);
                final SingleOutputStreamOperator<Long> joined =
                        source.connect(broadcast)
                                .process(new TestBroadcastProcessFunction())
                                .setParallelism(2 * parallelism);

                addFailingSink(joined, minCheckpoints, slotSharing);
            }
        },

        KEYED_BROADCAST {
            @Override
            public void create(
                    StreamExecutionEnvironment env,
                    int minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {

                final int parallelism = env.getParallelism();
                final DataStream<Long> broadcastSide1 =
                        env.fromSource(
                                        new LongSource(
                                                minCheckpoints,
                                                1,
                                                expectedRestarts,
                                                env.getCheckpointInterval()),
                                        noWatermarks(),
                                        "source-1")
                                .setParallelism(1);
                final DataStream<Long> broadcastSide2 =
                        env.fromSource(
                                        new LongSource(
                                                minCheckpoints,
                                                1,
                                                expectedRestarts,
                                                env.getCheckpointInterval()),
                                        noWatermarks(),
                                        "source-2")
                                .setParallelism(1);
                final DataStream<Long> broadcastSide3 =
                        env.fromSource(
                                        new LongSource(
                                                minCheckpoints,
                                                1,
                                                expectedRestarts,
                                                env.getCheckpointInterval()),
                                        noWatermarks(),
                                        "source-3")
                                .setParallelism(1);
                final DataStream<Long> source =
                        createSourcePipeline(
                                        env,
                                        minCheckpoints,
                                        slotSharing,
                                        expectedRestarts,
                                        parallelism,
                                        0,
                                        val -> true)
                                .map(i -> checkHeader(i))
                                .name("map")
                                .uid("map")
                                .slotSharingGroup(slotSharing ? "default" : "failing-map");

                final MapStateDescriptor<Long, Long> descriptor =
                        new MapStateDescriptor<>(
                                "broadcast",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.LONG_TYPE_INFO);
                DataStream<Long> broadcastSide =
                        broadcastSide1.union(broadcastSide2).union(broadcastSide3);
                final BroadcastStream<Long> broadcast = broadcastSide.broadcast(descriptor);
                final SingleOutputStreamOperator<Long> joined =
                        source.keyBy(i -> withoutHeader(i) % NUM_GROUPS)
                                .connect(broadcast)
                                .process(new TestKeyedBroadcastProcessFunction())
                                .setParallelism(parallelism + 2);

                addFailingSink(joined, minCheckpoints, slotSharing);
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
                    .addSink(
                            new VerifyingSink(
                                    minCheckpoints,
                                    combinedSource
                                            .getExecutionEnvironment()
                                            .getCheckpointInterval()))
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
                            new LongSource(
                                    minCheckpoints,
                                    parallelism,
                                    expectedRestarts,
                                    env.getCheckpointInterval()),
                            noWatermarks(),
                            "source" + inputIndex)
                    .uid("source" + inputIndex)
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
                    .slotSharingGroup(slotSharing ? "default" : ("keyby" + inputIndex))
                    .rescale()
                    .map(i -> checkHeader(i))
                    .name("rescale" + inputIndex)
                    .uid("rescale" + inputIndex)
                    .setParallelism(Math.max(parallelism + 1, parallelism * 3 / 2))
                    .slotSharingGroup(slotSharing ? "default" : ("rescale" + inputIndex));
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }

        private static class TestBroadcastProcessFunction
                extends BroadcastProcessFunction<Long, Long, Long> {
            private static final long serialVersionUID = 7852973507735751404L;

            TestBroadcastProcessFunction() {}

            @Override
            public void processElement(Long value, ReadOnlyContext ctx, Collector<Long> out) {
                out.collect(checkHeader(value));
            }

            @Override
            public void processBroadcastElement(Long value, Context ctx, Collector<Long> out) {}
        }

        private static class TestKeyedCoProcessFunction
                extends KeyedCoProcessFunction<Long, Long, Long, Long> {
            private static final long serialVersionUID = 1L;

            TestKeyedCoProcessFunction() {}

            @Override
            public void processElement1(Long value, Context ctx, Collector<Long> out)
                    throws Exception {
                out.collect(checkHeader(value));
            }

            @Override
            public void processElement2(Long value, Context ctx, Collector<Long> out)
                    throws Exception {
                out.collect(checkHeader(value));
            }
        }

        private static class TestKeyedBroadcastProcessFunction
                extends KeyedBroadcastProcessFunction<Long, Long, Long, Long> {
            private static final long serialVersionUID = 7852973507735751404L;

            TestKeyedBroadcastProcessFunction() {}

            @Override
            public void processElement(Long value, ReadOnlyContext ctx, Collector<Long> out) {
                out.collect(checkHeader(value));
            }

            @Override
            public void processBroadcastElement(Long value, Context ctx, Collector<Long> out) {}
        }
    }

    @Parameterized.Parameters(name = "{0} {1} from {2} to {3}, buffersPerChannel = {4}")
    public static Object[][] getScaleFactors() {
        Object[][] parameters =
                new Object[][] {
                    new Object[] {"downscale", Topology.KEYED_DIFFERENT_PARALLELISM, 12, 7},
                    new Object[] {"upscale", Topology.KEYED_DIFFERENT_PARALLELISM, 7, 12},
                    new Object[] {"downscale", Topology.KEYED_BROADCAST, 7, 2},
                    new Object[] {"upscale", Topology.KEYED_BROADCAST, 2, 7},
                    new Object[] {"downscale", Topology.BROADCAST, 5, 2},
                    new Object[] {"upscale", Topology.BROADCAST, 2, 5},
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
        return Arrays.stream(parameters)
                .map(
                        params ->
                                new Object[][] {
                                    ArrayUtils.add(params, 0),
                                    ArrayUtils.add(params, BUFFER_PER_CHANNEL)
                                })
                .flatMap(Arrays::stream)
                .toArray(Object[][]::new);
    }

    public UnalignedCheckpointRescaleITCase(
            String desc,
            Topology topology,
            int oldParallelism,
            int newParallelism,
            int buffersPerChannel) {
        this.topology = topology;
        this.oldParallelism = oldParallelism;
        this.newParallelism = newParallelism;
        this.buffersPerChannel = buffersPerChannel;
    }

    @Test
    public void shouldRescaleUnalignedCheckpoint() throws Exception {
        final UnalignedSettings prescaleSettings =
                new UnalignedSettings(topology)
                        .setParallelism(oldParallelism)
                        .setExpectedFailures(1)
                        .setBuffersPerChannel(buffersPerChannel);
        prescaleSettings.setGenerateCheckpoint(true);
        final File checkpointDir = super.execute(prescaleSettings);

        // resume
        final UnalignedSettings postscaleSettings =
                new UnalignedSettings(topology)
                        .setParallelism(newParallelism)
                        .setExpectedFailures(1)
                        .setBuffersPerChannel(buffersPerChannel);
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

        protected VerifyingSink(long minCheckpoints, long checkpointingInterval) {
            super(minCheckpoints, checkpointingInterval);
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

            induceBackpressure();
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
