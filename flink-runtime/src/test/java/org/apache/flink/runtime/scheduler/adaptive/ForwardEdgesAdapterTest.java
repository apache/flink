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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.MutableVertexParallelismStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.apache.flink.runtime.scheduler.adaptive.ForwardEdgesAdapter.copyJobGraphWithAdaptedForwardEdges;
import static org.apache.flink.runtime.scheduler.adaptive.ForwardEdgesAdapter.isSameParallelism;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link ForwardEdgesAdapter}. */
class ForwardEdgesAdapterTest {

    private JobGraph initialJobGraph;
    private JobGraphJobInformation jobInformation;

    @BeforeEach
    void setUp() {
        this.initialJobGraph = generateJobGraph();
        this.jobInformation =
                new JobGraphJobInformation(initialJobGraph, new DefaultVertexParallelismStore());
    }

    @RepeatedTest(100000)
    void testAdaptiveChangeForwardJobEdges() throws Exception {

        // Test initial info.
        MutableVertexParallelismStore vertexParallelismStore =
                getParallelismStore(initialJobGraph, 4, 4, 4, 4);
        JobGraph adjustedJobGraph =
                copyJobGraphWithAdaptedForwardEdges(jobInformation, vertexParallelismStore);
        assertInitialForwardableInputs(adjustedJobGraph, vertexParallelismStore);

        // Test forward -> rebalance.
        vertexParallelismStore = getParallelismStore(adjustedJobGraph, 4, 4, 2, 4);
        adjustedJobGraph =
                copyJobGraphWithAdaptedForwardEdges(jobInformation, vertexParallelismStore);
        assertInitialForwardableInputs(adjustedJobGraph, vertexParallelismStore);
    }

    private MutableVertexParallelismStore getParallelismStore(
            JobGraph graph, int... parallelismsOfVerticesSortedTopologicallyFromSources) {
        MutableVertexParallelismStore vertexParallelismStore = new DefaultVertexParallelismStore();
        List<JobVertex> verticesSortedTopologicallyFromSources =
                graph.getVerticesSortedTopologicallyFromSources();
        for (int i = 0; i < verticesSortedTopologicallyFromSources.size(); i++) {
            vertexParallelismStore.setParallelismInfo(
                    verticesSortedTopologicallyFromSources.get(i).getID(),
                    new DefaultVertexParallelismInfo(
                            parallelismsOfVerticesSortedTopologicallyFromSources[i],
                            Integer.MAX_VALUE,
                            integer -> Optional.empty()));
        }
        return vertexParallelismStore;
    }

    private JobGraph generateJobGraph() {
        JobVertex source = new JobVertex("source");
        JobVertex middle = new JobVertex("middle");
        JobVertex firstSink = new JobVertex("firstSink");
        JobVertex secondSink = new JobVertex("secondSink");
        for (JobVertex vertex : new JobVertex[] {source, middle, firstSink, secondSink}) {
            vertex.setParallelism(2);
        }
        middle.connectNewDataSetAsInput(source, POINTWISE, ResultPartitionType.PIPELINED)
                .setForward(true);
        firstSink
                .connectNewDataSetAsInput(middle, POINTWISE, ResultPartitionType.PIPELINED)
                .setForward(true);
        secondSink
                .connectNewDataSetAsInput(middle, POINTWISE, ResultPartitionType.PIPELINED)
                .setForward(true);
        return JobGraphTestUtils.streamingJobGraph(source, middle, firstSink, secondSink);
    }

    private void assertInitialForwardableInputs(
            JobGraph adjustedGraph, MutableVertexParallelismStore vertexParallelismStore) {
        List<JobVertex> adjustedSortedVertices =
                adjustedGraph.getVerticesSortedTopologicallyFromSources();
        List<JobVertex> originalSortedVertices =
                initialJobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(originalSortedVertices).hasSameSizeAs(adjustedSortedVertices);
        for (int i = 0; i < originalSortedVertices.size(); i++) {
            JobVertex originalVertex = originalSortedVertices.get(i);
            JobVertex adjustedVertex = adjustedSortedVertices.get(i);
            List<JobEdge> inputEdgesOfOriginalVertex = originalVertex.getInputs();
            List<JobEdge> inputEdgesOfAdjustedVertex = adjustedVertex.getInputs();
            assertThat(inputEdgesOfOriginalVertex).hasSameSizeAs(inputEdgesOfAdjustedVertex);
            for (int j = 0; j < inputEdgesOfOriginalVertex.size(); j++) {
                JobEdge adjustedInput = inputEdgesOfAdjustedVertex.get(j);
                JobEdge originalInput = inputEdgesOfOriginalVertex.get(j);
                if (originalInput.isForward()) {
                    final boolean forwardable =
                            isSameParallelism(adjustedInput, vertexParallelismStore);
                    assertThat(adjustedInput.isForward()).isEqualTo(forwardable);
                    assertThat(adjustedInput.getDistributionPattern())
                            .isEqualTo(forwardable ? POINTWISE : ALL_TO_ALL);
                    assertThat(adjustedInput.getUpstreamSubtaskStateMapper())
                            .isEqualTo(
                                    forwardable
                                            ? SubtaskStateMapper.UNSUPPORTED
                                            : SubtaskStateMapper.ARBITRARY);
                    assertThat(adjustedInput.getDownstreamSubtaskStateMapper())
                            .isEqualTo(
                                    forwardable
                                            ? SubtaskStateMapper.UNSUPPORTED
                                            : SubtaskStateMapper.ROUND_ROBIN);
                    assertThat(adjustedInput.getShipStrategyName())
                            .isEqualTo(forwardable ? "FORWARD" : "REBALANCE");

                    IntermediateDataSet adjustedInputSource = adjustedInput.getSource();
                    assertThat(adjustedInputSource.getDistributionPattern())
                            .isEqualTo(forwardable ? POINTWISE : ALL_TO_ALL);
                }
            }
        }
    }
}
