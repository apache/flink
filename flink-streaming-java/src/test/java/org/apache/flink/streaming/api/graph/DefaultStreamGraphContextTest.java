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

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit tests for {@link DefaultStreamGraphContextTest}. */
class DefaultStreamGraphContextTest {
    @Test
    void testModifyStreamEdge() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements -> Map1 -> Map2 -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> mapDataStream2 =
                new DataStream<>(
                                env,
                                new PartitionTransformation<>(
                                        mapDataStream.getTransformation(),
                                        new ForwardForConsecutiveHashPartitioner<>(
                                                new KeyGroupStreamPartitioner<>(record -> 0L, 100)),
                                        StreamExchangeMode.PIPELINED))
                        .map(value -> value)
                        .setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream2.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));

        partitionAfterMapDataStream.print().setParallelism(1);

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setDynamic(false);

        Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache = new HashMap<>();
        Map<Integer, Integer> frozenNodeToStartNodeMap = new HashMap<>();
        Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches =
                new HashMap<>();

        StreamGraphContext streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        forwardGroupsByEndpointNodeIdCache,
                        frozenNodeToStartNodeMap,
                        opIntermediateOutputsCaches);

        // Modify forward partitioner to rescale partitioner.
        ImmutableStreamGraph immutableStreamGraph = streamGraphContext.getStreamGraph();
        ImmutableStreamNode targetNode =
                immutableStreamGraph.getStreamNode(
                        immutableStreamGraph
                                .getStreamNode(streamGraph.getSourceIDs().iterator().next())
                                .getOutEdges()
                                .get(0)
                                .getTargetId());
        ImmutableStreamEdge targetEdge = targetNode.getOutEdges().get(0);

        StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                new StreamEdgeUpdateRequestInfo(
                                targetEdge.getEdgeId(),
                                targetEdge.getSourceId(),
                                targetEdge.getTargetId())
                        .outputPartitioner(new RescalePartitioner<>());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(true);

        List<JobVertex> jobVerticesAfterModified =
                StreamingJobGraphGenerator.createJobGraph(streamGraph)
                        .getVerticesSortedTopologicallyFromSources();

        assertThat(jobVerticesAfterModified.size()).isEqualTo(4);

        // We cannot modify nodes in the frozen list
        frozenNodeToStartNodeMap.put(targetEdge.getTargetId(), targetEdge.getTargetId());
        assertThat(
                        streamGraphContext.modifyStreamEdge(
                                Collections.singletonList(streamEdgeUpdateRequestInfo)))
                .isEqualTo(false);
    }
}
