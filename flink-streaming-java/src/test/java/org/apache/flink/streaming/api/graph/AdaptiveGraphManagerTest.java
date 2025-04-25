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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AdaptiveGraphManager}. */
public class AdaptiveGraphManagerTest extends JobGraphGeneratorTestBase {
    @Override
    JobGraph createJobGraph(StreamGraph streamGraph) {
        return generateJobGraphInLazilyMode(streamGraph);
    }

    @Override
    void verifyManagedMemoryFractionForUnknownResourceSpec(
            JobVertex vertex1,
            JobVertex vertex2,
            JobVertex vertex3,
            Configuration taskManagerConfig) {
        final StreamConfig sourceConfig = new StreamConfig(vertex1.getConfiguration());
        verifyFractions(sourceConfig, 0.6 / 2, 0.0, 0.0, taskManagerConfig);

        final StreamConfig map1Config =
                Iterables.getOnlyElement(
                        sourceConfig
                                .getTransitiveChainedTaskConfigs(
                                        JobGraphGeneratorTestBase.class.getClassLoader())
                                .values());
        verifyFractions(map1Config, 0.6 / 2, 0.4, 0.0, taskManagerConfig);

        // In the progressive batch scheduling scenario, when vertex2 is being created, vertex1 has
        // already finished execution. In this case, the python fraction for vertex2 should be 1.0.
        final StreamConfig map2Config = new StreamConfig(vertex2.getConfiguration());
        verifyFractions(map2Config, 0.0, 1.0, 0.0, taskManagerConfig);

        final StreamConfig map3Config = new StreamConfig(vertex3.getConfiguration());
        verifyFractions(map3Config, 1.0, 0.0, 0.0, taskManagerConfig);
    }

    @Test
    void testCreateJobVertexLazily() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> input =
                env.fromData("a", "b", "c", "d", "e", "f")
                        .map(
                                new MapFunction<String, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(String value) {
                                        return new Tuple2<>(value, value);
                                    }
                                });

        DataStream<Tuple2<String, String>> result =
                input.keyBy(x -> x.f0)
                        .map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(
                                            Tuple2<String, String> value) {
                                        return value;
                                    }
                                });

        result.sinkTo(new DiscardingSink<>());
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setDynamic(true);

        AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(), streamGraph, Runnable::run);
        JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size()).isEqualTo(1);
        while (!jobVertices.isEmpty()) {
            List<JobVertex> newJobVertices = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                newJobVertices.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = newJobVertices;
        }
        jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(jobVertices.size()).isEqualTo(2);
        assertThat(adaptiveGraphManager.getPendingOperatorsCount()).isEqualTo(0);
        assertThat(adaptiveGraphManager.getStreamNodeIdsByJobVertexId(jobVertices.get(0).getID()))
                .isEqualTo(List.of(streamNodes.get(1).getId(), streamNodes.get(0).getId()));
        assertThat(adaptiveGraphManager.getStreamNodeIdsByJobVertexId(jobVertices.get(1).getID()))
                .isEqualTo(List.of(streamNodes.get(3).getId(), streamNodes.get(2).getId()));
        assertThat(
                        adaptiveGraphManager.getProducerStreamNodeId(
                                jobVertices.get(0).getProducedDataSets().get(0).getId()))
                .isEqualTo(streamNodes.get(1).getId());
        assertThat(
                        adaptiveGraphManager.getOutputStreamEdges(
                                jobVertices.get(0).getProducedDataSets().get(0).getId()))
                .isEqualTo(streamNodes.get(1).getOutEdges());

        Set<Integer> forwardGroup =
                adaptiveGraphManager
                        .getStreamNodeForwardGroupByVertexId(jobVertices.get(0).getID())
                        .getVertexIds();
        assertThat(forwardGroup.size()).isEqualTo(2);
        assertThat(forwardGroup.contains(streamNodes.get(0).getId())).isTrue();
        assertThat(forwardGroup.contains(streamNodes.get(1).getId())).isTrue();
    }

    @Test
    void testTheCorrectnessOfJobGraph() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> input =
                env.fromData("a", "b", "c", "d", "e", "f")
                        .map(
                                new MapFunction<String, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(String value) {
                                        return new Tuple2<>(value, value);
                                    }
                                });

        DataStream<Tuple2<String, String>> result =
                input.keyBy(x -> x.f0)
                        .map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(
                                            Tuple2<String, String> value) {
                                        return value;
                                    }
                                });

        result.sinkTo(new DiscardingSink<>());
        StreamGraph streamGraph1 = env.getStreamGraph(false);
        JobGraph jobGraph1 = generateJobGraphInLazilyMode(streamGraph1);

        // we could not reuse the streamGraph1 because the streamGraph1 could have been modified in
        // the adaptive graph manager.
        StreamGraph streamGraph2 = env.getStreamGraph(false);
        JobGraph jobGraph2 = StreamingJobGraphGenerator.createJobGraph(streamGraph2);
        assertThat(isJobGraphEquivalent(jobGraph1, jobGraph2)).isEqualTo(true);
    }

    @Test
    void testSourceChain() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(100);
        env.setParallelism(100);
        MultipleInputTransformation<Long> transform =
                new MultipleInputTransformation<>(
                        "mit", new UnusedOperatorFactory(), Types.LONG, -1);

        Transformation<Long> input1 =
                env.fromSource(
                                new NumberSequenceSource(1, 2),
                                WatermarkStrategy.noWatermarks(),
                                "input1")
                        .setParallelism(100)
                        .getTransformation();
        Transformation<Long> input2 =
                env.fromSource(
                                new NumberSequenceSource(1, 2),
                                WatermarkStrategy.noWatermarks(),
                                "input2")
                        .setParallelism(1)
                        .getTransformation();
        Transformation<Long> input3 =
                env.fromSource(
                                new NumberSequenceSource(1, 2),
                                WatermarkStrategy.noWatermarks(),
                                "input3")
                        .setParallelism(1)
                        .getTransformation();
        transform.addInput(input1);
        transform.addInput(input2);
        transform.addInput(input3);
        transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        DataStream<Long> dataStream = new DataStream<>(env, transform);
        // do not chain with sink operator.
        dataStream.rebalance().sinkTo(new DiscardingSink<>()).name("sink");
        env.addOperator(transform);
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setDynamic(true);
        JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources().size()).isEqualTo(4);
    }

    @Test
    void testForwardForConsecutiveHashPartitionerChain() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        final DataStream<Integer> source = env.fromData(1, 2, 3);
        final DataStream<Integer> forward =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new ForwardForConsecutiveHashPartitioner<>(
                                        new RebalancePartitioner<>()),
                                StreamExchangeMode.BATCH));
        forward.print();

        StreamGraph streamGraph1 = env.getStreamGraph(false);
        streamGraph1.setDynamic(true);
        JobGraph jobGraph1 = generateJobGraphInLazilyMode(streamGraph1);

        StreamGraph streamGraph2 = env.getStreamGraph(false);
        streamGraph2.setDynamic(true);
        JobGraph jobGraph2 = StreamingJobGraphGenerator.createJobGraph(streamGraph2);
        assertThat(isJobGraphEquivalent(jobGraph1, jobGraph2)).isTrue();
    }

    private static JobGraph generateJobGraphInLazilyMode(StreamGraph streamGraph) {
        AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(), streamGraph, Runnable::run);
        JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        while (!jobVertices.isEmpty()) {
            List<JobVertex> newJobVertices = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                newJobVertices.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = newJobVertices;
        }
        return jobGraph;
    }

    private static boolean isJobGraphEquivalent(JobGraph jobGraph1, JobGraph jobGraph2) {
        assertThat(jobGraph1.getJobConfiguration()).isEqualTo(jobGraph2.getJobConfiguration());
        assertThat(jobGraph1.getJobType()).isEqualTo(jobGraph2.getJobType());
        assertThat(jobGraph1.isDynamic()).isEqualTo(jobGraph2.isDynamic());
        assertThat(jobGraph1.isApproximateLocalRecoveryEnabled())
                .isEqualTo(jobGraph2.isApproximateLocalRecoveryEnabled());
        assertThat(jobGraph1.getSerializedExecutionConfig())
                .isEqualTo(jobGraph2.getSerializedExecutionConfig());
        assertThat(jobGraph1.getCheckpointingSettings().toString())
                .isEqualTo(jobGraph2.getCheckpointingSettings().toString());
        assertThat(jobGraph1.getSavepointRestoreSettings())
                .isEqualTo(jobGraph2.getSavepointRestoreSettings());
        assertThat(jobGraph1.getUserJars()).isEqualTo(jobGraph2.getUserJars());
        assertThat(jobGraph1.getUserArtifacts()).isEqualTo(jobGraph2.getUserArtifacts());
        assertThat(jobGraph1.getUserJarBlobKeys()).isEqualTo(jobGraph2.getUserJarBlobKeys());
        assertThat(jobGraph1.getClasspaths()).isEqualTo(jobGraph2.getClasspaths());
        assertThat(jobGraph1.getJobStatusHooks()).isEqualTo(jobGraph2.getJobStatusHooks());

        List<JobVertex> vertices1 = jobGraph1.getVerticesSortedTopologicallyFromSources();
        List<JobVertex> vertices2 = jobGraph2.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices1.size()).isEqualTo(vertices2.size());
        for (int i = 1; i < vertices1.size(); i++) {
            JobVertex vertex1 = vertices1.get(i);
            JobVertex vertex2 = vertices2.get(i);
            assertThat(vertex1.getID()).isEqualTo(vertex2.getID());
            assertThat(vertex1.getInputs().size()).isEqualTo(vertex2.getInputs().size());
            assertThat(vertex1.getParallelism()).isEqualTo(vertex2.getParallelism());
            assertThat(vertex1.getMaxParallelism()).isEqualTo(vertex2.getMaxParallelism());
            assertThat(vertex1.getMinResources()).isEqualTo(vertex2.getMinResources());
            assertThat(vertex1.getPreferredResources()).isEqualTo(vertex2.getPreferredResources());
            assertThat(vertex1.getInvokableClassName()).isEqualTo(vertex2.getInvokableClassName());
            assertThat(vertex1.getName()).isEqualTo(vertex2.getName());
            assertThat(vertex1.getOperatorName()).isEqualTo(vertex2.getOperatorName());
            assertThat(vertex1.isSupportsConcurrentExecutionAttempts())
                    .isEqualTo(vertex2.isSupportsConcurrentExecutionAttempts());
            assertThat(vertex1.isAnyOutputBlocking()).isEqualTo(vertex2.isAnyOutputBlocking());
            assertThat(vertex1.isParallelismConfigured())
                    .isEqualTo(vertex2.isParallelismConfigured());
        }
        return true;
    }
}
