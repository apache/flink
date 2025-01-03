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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link AdaptiveBatchScheduler}. */
class AdaptiveBatchSchedulerITCase {

    private static final int DEFAULT_MAX_PARALLELISM = 4;
    private static final int SOURCE_PARALLELISM_1 = 2;
    private static final int SOURCE_PARALLELISM_2 = 8;
    private static final int NUMBERS_TO_PRODUCE = 10000;

    private static ConcurrentLinkedQueue<Map<Long, Long>> numberCountResults;

    private Map<Long, Long> expectedResult;

    @BeforeEach
    void setUp() {
        expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 2L));

        numberCountResults = new ConcurrentLinkedQueue<>();
    }

    @Test
    void testScheduling() throws Exception {
        testSchedulingBase(false);
    }

    @Test
    void testSchedulingWithDynamicSourceParallelismInference() throws Exception {
        testSchedulingBase(true);
    }

    @Test
    void testParallelismOfForwardGroupLargerThanGlobalMaxParallelism() throws Exception {
        final Configuration configuration = createConfiguration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(8);

        final DataStream<Long> source =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source")
                        .slotSharingGroup("group1");

        source.forward().map(new NumberCounter()).name("map").slotSharingGroup("group2");
        env.execute();
    }

    @Test
    void testDifferentConsumerParallelism() throws Exception {
        final Configuration configuration = createConfiguration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(8);

        final DataStream<Long> source2 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source2")
                        .slotSharingGroup("group2");

        final DataStream<Long> source1 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(8)
                        .name("source1")
                        .slotSharingGroup("group1");

        source1.forward()
                .union(source2)
                .map(new NumberCounter())
                .name("map1")
                .slotSharingGroup("group3");

        source2.map(new NumberCounter()).name("map2").slotSharingGroup("group4");

        env.execute();
    }

    @Test
    void testAdaptiveOptimizeStreamGraph() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                StreamGraphOptimizationStrategy.STREAM_GRAPH_OPTIMIZATION_STRATEGY,
                List.of(TestingStreamGraphOptimizerStrategy.class.getName()));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.disableOperatorChaining();
        env.setParallelism(8);

        SingleOutputStreamOperator<Long> source1 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_1)
                        .name("source1");
        SingleOutputStreamOperator<Long> source2 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_2)
                        .name("source2");

        source1.keyBy(i -> i % SOURCE_PARALLELISM_1)
                .map(i -> i)
                .name("map1")
                .rebalance()
                .union(source2)
                .rebalance()
                .map(new NumberCounter())
                .name("map2")
                .setParallelism(1);

        StreamGraph streamGraph = env.getStreamGraph();
        StreamNode sourceNode1 =
                streamGraph.getStreamNodes().stream()
                        .filter(node -> node.getOperatorName().contains("source1"))
                        .findFirst()
                        .get();
        StreamNode mapNode1 =
                streamGraph.getStreamNodes().stream()
                        .filter(node -> node.getOperatorName().contains("map1"))
                        .findFirst()
                        .get();

        TestingStreamGraphOptimizerStrategy.convertToRescaleEdgeIds.add(
                sourceNode1.getOutEdges().get(0).getEdgeId());
        TestingStreamGraphOptimizerStrategy.convertToBroadcastEdgeIds.add(
                mapNode1.getOutEdges().get(0).getEdgeId());

        env.execute(streamGraph);

        Map<Long, Long> numberCountResultMap =
                numberCountResults.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, Map.Entry::getValue, Long::sum));

        // One part comes from source1, while the other parts come from the broadcast results of
        // source2.
        Map<Long, Long> expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 2L));
        assertThat(numberCountResultMap).isEqualTo(expectedResult);
    }

    private void testSchedulingBase(Boolean useSourceParallelismInference) throws Exception {
        executeJob(useSourceParallelismInference);

        Map<Long, Long> numberCountResultMap =
                numberCountResults.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (v1, v2) -> v1 + v2));

        for (int i = 0; i < NUMBERS_TO_PRODUCE; i++) {
            if (numberCountResultMap.get(i) != expectedResult.get(i)) {
                System.out.println(i + ": " + numberCountResultMap.get(i));
            }
        }
        assertThat(numberCountResultMap).isEqualTo(expectedResult);
    }

    private void executeJob(Boolean useSourceParallelismInference) throws Exception {
        final Configuration configuration = createConfiguration();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<SlotSharingGroup> slotSharingGroups = new ArrayList<>();

        for (int i = 0; i < 3; ++i) {
            SlotSharingGroup group =
                    SlotSharingGroup.newBuilder("group" + i)
                            .setCpuCores(1.0)
                            .setTaskHeapMemory(MemorySize.parse("100m"))
                            .build();
            slotSharingGroups.add(group);
        }

        DataStream<Long> source1;
        DataStream<Long> source2;

        if (useSourceParallelismInference) {
            source1 =
                    env.fromSource(
                                    new TestingParallelismInferenceNumberSequenceSource(
                                            0, NUMBERS_TO_PRODUCE - 1, SOURCE_PARALLELISM_1),
                                    WatermarkStrategy.noWatermarks(),
                                    "source1")
                            .slotSharingGroup(slotSharingGroups.get(0));
            source2 =
                    env.fromSource(
                                    new TestingParallelismInferenceNumberSequenceSource(
                                            0, NUMBERS_TO_PRODUCE - 1, SOURCE_PARALLELISM_2),
                                    WatermarkStrategy.noWatermarks(),
                                    "source2")
                            .slotSharingGroup(slotSharingGroups.get(1));
        } else {
            source1 =
                    env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                            .setParallelism(SOURCE_PARALLELISM_1)
                            .name("source1")
                            .slotSharingGroup(slotSharingGroups.get(0));
            source2 =
                    env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                            .setParallelism(SOURCE_PARALLELISM_2)
                            .name("source2")
                            .slotSharingGroup(slotSharingGroups.get(1));
        }

        source1.union(source2)
                .rescale()
                .map(new NumberCounter())
                .name("map")
                .slotSharingGroup(slotSharingGroups.get(2));

        env.execute();
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(JobManagerOptions.SLOT_REQUEST_TIMEOUT, Duration.ofMillis(5000L));
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM,
                DEFAULT_MAX_PARALLELISM);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("150kb"));
        configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        return configuration;
    }

    private static class NumberCounter extends RichMapFunction<Long, Long> {

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public Long map(Long value) throws Exception {
            numberCountResult.put(value, numberCountResult.getOrDefault(value, 0L) + 1L);

            return value;
        }

        @Override
        public void close() throws Exception {
            numberCountResults.add(numberCountResult);
        }
    }

    private static class TestingParallelismInferenceNumberSequenceSource
            extends NumberSequenceSource implements DynamicParallelismInference {
        private static final long serialVersionUID = 1L;
        private final int expectedParallelism;

        public TestingParallelismInferenceNumberSequenceSource(
                long from, long to, int expectedParallelism) {
            super(from, to);
            this.expectedParallelism = expectedParallelism;
        }

        @Override
        public int inferParallelism(Context context) {
            return expectedParallelism;
        }
    }

    public static final class TestingStreamGraphOptimizerStrategy
            implements StreamGraphOptimizationStrategy {

        private static final Set<String> convertToBroadcastEdgeIds = new HashSet<>();
        private static final Set<String> convertToRescaleEdgeIds = new HashSet<>();

        @Override
        public boolean onOperatorsFinished(
                OperatorsFinished operatorsFinished, StreamGraphContext context) throws Exception {
            List<Integer> finishedStreamNodeIds = operatorsFinished.getFinishedStreamNodeIds();
            List<StreamEdgeUpdateRequestInfo> requestInfos = new ArrayList<>();
            for (Integer finishedStreamNodeId : finishedStreamNodeIds) {
                for (ImmutableStreamEdge outEdge :
                        context.getStreamGraph()
                                .getStreamNode(finishedStreamNodeId)
                                .getOutEdges()) {
                    StreamEdgeUpdateRequestInfo requestInfo =
                            new StreamEdgeUpdateRequestInfo(
                                    outEdge.getEdgeId(),
                                    outEdge.getSourceId(),
                                    outEdge.getTargetId());
                    if (convertToBroadcastEdgeIds.contains(outEdge.getEdgeId())) {
                        requestInfo.withOutputPartitioner(new BroadcastPartitioner<>());
                        requestInfos.add(requestInfo);
                    } else if (convertToRescaleEdgeIds.contains(outEdge.getEdgeId())) {
                        requestInfo.withOutputPartitioner(new RescalePartitioner<>());
                        requestInfos.add(requestInfo);
                    }
                }
            }
            return context.modifyStreamEdge(requestInfos);
        }
    }
}
