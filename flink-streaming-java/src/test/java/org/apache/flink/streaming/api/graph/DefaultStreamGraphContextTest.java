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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit tests for {@link DefaultStreamGraphContext}. */
class DefaultStreamGraphContextTest {
    @Test
    void testModifyStreamEdge() {
        StreamGraph streamGraph = createStreamGraphForModifyStreamEdgeTest();
        Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache = new HashMap<>();
        Map<Integer, Integer> frozenNodeToStartNodeMap = new HashMap<>();
        Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches =
                new HashMap<>();
        StreamGraphContext streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        forwardGroupsByEndpointNodeIdCache,
                        frozenNodeToStartNodeMap,
                        opIntermediateOutputsCaches,
                        new HashMap<>(),
                        new HashSet<>(),
                        Thread.currentThread().getContextClassLoader());

        StreamNode sourceNode =
                streamGraph.getStreamNode(streamGraph.getSourceIDs().iterator().next());
        StreamNode targetNode =
                streamGraph.getStreamNode(sourceNode.getOutEdges().get(0).getTargetId());
        targetNode.setParallelism(1);
        StreamEdge targetEdge = sourceNode.getOutEdges().get(0);

        StreamNodeForwardGroup forwardGroup1 =
                new StreamNodeForwardGroup(Collections.singleton(sourceNode));
        StreamNodeForwardGroup forwardGroup2 =
                new StreamNodeForwardGroup(Collections.singleton(targetNode));
        forwardGroupsByEndpointNodeIdCache.put(sourceNode.getId(), forwardGroup1);
        forwardGroupsByEndpointNodeIdCache.put(targetNode.getId(), forwardGroup2);

        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                targetEdge.getEdgeId(),
                                targetEdge.getSourceId(),
                                targetEdge.getTargetId())
                        .withOutputPartitioner(new ForwardForUnspecifiedPartitioner<>());

        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof ForwardPartitioner).isTrue();

        // We cannot modify when partitioner is forward partitioner.
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);

        // We cannot modify when target node job vertex is created.
        frozenNodeToStartNodeMap.put(targetEdge.getTargetId(), targetEdge.getTargetId());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);

        NonChainedOutput nonChainedOutput =
                new NonChainedOutput(
                        targetEdge.supportsUnalignedCheckpoints(),
                        targetEdge.getSourceId(),
                        targetNode.getParallelism(),
                        targetNode.getMaxParallelism(),
                        targetEdge.getBufferTimeout(),
                        false,
                        new IntermediateDataSetID(),
                        targetEdge.getOutputTag(),
                        targetEdge.getPartitioner(),
                        ResultPartitionType.BLOCKING);
        opIntermediateOutputsCaches.put(
                targetEdge.getSourceId(),
                Map.of(
                        targetEdge,
                        nonChainedOutput,
                        targetNode.getOutEdges().get(0),
                        nonChainedOutput));

        // We cannot modify when target edge is consumed by multi edges.
        frozenNodeToStartNodeMap.put(targetEdge.getTargetId(), targetEdge.getTargetId());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);
    }

    @Test
    void testModifyToForwardPartitionerButResultIsRescale() {
        StreamGraph streamGraph = createStreamGraphForModifyStreamEdgeTest();

        Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache = new HashMap<>();
        Map<Integer, Integer> frozenNodeToStartNodeMap = new HashMap<>();
        Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches =
                new HashMap<>();

        StreamGraphContext streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        forwardGroupsByEndpointNodeIdCache,
                        frozenNodeToStartNodeMap,
                        opIntermediateOutputsCaches,
                        new HashMap<>(),
                        new HashSet<>(),
                        Thread.currentThread().getContextClassLoader());

        StreamNode sourceNode =
                streamGraph.getStreamNode(streamGraph.getSourceIDs().iterator().next());
        StreamNode targetNode =
                streamGraph.getStreamNode(sourceNode.getOutEdges().get(0).getTargetId());
        StreamEdge targetEdge = sourceNode.getOutEdges().get(0);

        StreamNodeForwardGroup forwardGroup1 =
                new StreamNodeForwardGroup(Collections.singleton(sourceNode));
        StreamNodeForwardGroup forwardGroup2 =
                new StreamNodeForwardGroup(Collections.singleton(targetNode));
        forwardGroupsByEndpointNodeIdCache.put(sourceNode.getId(), forwardGroup1);
        forwardGroupsByEndpointNodeIdCache.put(targetNode.getId(), forwardGroup2);

        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                targetEdge.getEdgeId(),
                                targetEdge.getSourceId(),
                                targetEdge.getTargetId())
                        .withOutputPartitioner(new ForwardForUnspecifiedPartitioner<>());

        // Modify rescale partitioner to forward partitioner.

        // 1. If the source and target are non-chainable.
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();

        // 2. If the forward group cannot be merged.
        targetNode.setParallelism(1);
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();

        // 3. If the upstream job vertex is created.
        frozenNodeToStartNodeMap.put(
                streamGraph.getSourceIDs().iterator().next(),
                streamGraph.getSourceIDs().iterator().next());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isTrue();
        assertThat(targetEdge.getPartitioner() instanceof RescalePartitioner).isTrue();
    }

    @Test
    void testModifyIntraInputKeyCorrelation() {
        StreamGraph streamGraph = createStreamGraphWithCorrelatedInputs();
        StreamGraphContext streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashSet<>(),
                        Thread.currentThread().getContextClassLoader());
        StreamNode sourceNode =
                streamGraph.getStreamNode(streamGraph.getSourceIDs().iterator().next());
        StreamEdge targetEdge = sourceNode.getOutEdges().get(0);
        assertThat(targetEdge.areInterInputsKeysCorrelated()).isTrue();
        assertThat(targetEdge.isIntraInputKeyCorrelated()).isTrue();
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(
                                        new StreamEdgeUpdateRequestInfo(
                                                        targetEdge.getEdgeId(),
                                                        targetEdge.getSourceId(),
                                                        targetEdge.getTargetId())
                                                .withIntraInputKeyCorrelated(false))))
                .isTrue();
        assertThat(targetEdge.isIntraInputKeyCorrelated()).isFalse();

        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(
                                        new StreamEdgeUpdateRequestInfo(
                                                        targetEdge.getEdgeId(),
                                                        targetEdge.getSourceId(),
                                                        targetEdge.getTargetId())
                                                .withIntraInputKeyCorrelated(true))))
                .isTrue();
        assertThat(targetEdge.isIntraInputKeyCorrelated()).isTrue();
    }

    private StreamGraph createStreamGraphForModifyStreamEdgeTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements(1) -> Map(2) -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3).setParallelism(1);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(2);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        partitionAfterMapDataStream.print();

        return env.getStreamGraph();
    }

    private StreamGraph createStreamGraphWithCorrelatedInputs() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // A--
        //    -(join)->  C
        // B--
        DataStream<Tuple2<Integer, String>> streamA =
                env.fromData(new Tuple2<>(1, "a1"), new Tuple2<>(2, "a2"), new Tuple2<>(3, "a3"))
                        .keyBy(value -> value.f0);
        DataStream<Tuple2<Integer, String>> streamB =
                env.fromData(new Tuple2<>(1, "b1"), new Tuple2<>(2, "b2"), new Tuple2<>(3, "b3"))
                        .keyBy(value -> value.f0);
        DataStream<String> joinedStream =
                streamA.join(streamB)
                        .where(v -> v.f0)
                        .equalTo(v -> v.f0)
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
                        .apply(
                                (first, second) -> first.f1 + second.f1,
                                BasicTypeInfo.STRING_TYPE_INFO);
        joinedStream.print();
        return env.getStreamGraph();
    }
}
