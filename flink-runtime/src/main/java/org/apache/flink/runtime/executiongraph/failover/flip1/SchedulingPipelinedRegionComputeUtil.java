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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.buildRawRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.mergeRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.uniqueRegions;
import static org.apache.flink.util.Preconditions.checkState;

/** Utils for computing {@link SchedulingPipelinedRegion}s. */
public final class SchedulingPipelinedRegionComputeUtil {

    public static Set<Set<SchedulingExecutionVertex>> computePipelinedRegions(
            final Iterable<? extends SchedulingExecutionVertex> topologicallySortedVertices,
            final Function<ExecutionVertexID, ? extends SchedulingExecutionVertex>
                    executionVertexRetriever,
            final Function<IntermediateResultPartitionID, ? extends SchedulingResultPartition>
                    resultPartitionRetriever) {

        final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion =
                buildRawRegions(
                        topologicallySortedVertices,
                        vertex ->
                                getNonReconnectableConsumedResults(
                                        vertex, resultPartitionRetriever));

        return mergeRegionsOnCycles(vertexToRegion, executionVertexRetriever);
    }

    /**
     * Merge the regions base on <a
     * href="https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm">
     * Tarjan's strongly connected components algorithm</a>. For more details please see <a
     * href="https://issues.apache.org/jira/browse/FLINK-17330">FLINK-17330</a>.
     */
    private static Set<Set<SchedulingExecutionVertex>> mergeRegionsOnCycles(
            final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion,
            final Function<ExecutionVertexID, ? extends SchedulingExecutionVertex>
                    executionVertexRetriever) {

        final List<Set<SchedulingExecutionVertex>> regionList =
                new ArrayList<>(uniqueRegions(vertexToRegion));
        final List<List<Integer>> outEdges =
                buildOutEdgesDesc(vertexToRegion, regionList, executionVertexRetriever);
        final Set<Set<Integer>> sccs =
                StronglyConnectedComponentsComputeUtils.computeStronglyConnectedComponents(
                        outEdges.size(), outEdges);

        final Set<Set<SchedulingExecutionVertex>> mergedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (Set<Integer> scc : sccs) {
            checkState(scc.size() > 0);

            Set<SchedulingExecutionVertex> mergedRegion = new HashSet<>();
            for (int regionIndex : scc) {
                mergedRegion =
                        mergeRegions(mergedRegion, regionList.get(regionIndex), vertexToRegion);
            }
            mergedRegions.add(mergedRegion);
        }

        return mergedRegions;
    }

    private static List<List<Integer>> buildOutEdgesDesc(
            final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion,
            final List<Set<SchedulingExecutionVertex>> regionList,
            final Function<ExecutionVertexID, ? extends SchedulingExecutionVertex>
                    executionVertexRetriever) {

        final Map<Set<SchedulingExecutionVertex>, Integer> regionIndices = new IdentityHashMap<>();
        for (int i = 0; i < regionList.size(); i++) {
            regionIndices.put(regionList.get(i), i);
        }

        final List<List<Integer>> outEdges = new ArrayList<>(regionList.size());
        for (Set<SchedulingExecutionVertex> currentRegion : regionList) {
            final List<Integer> currentRegionOutEdges = new ArrayList<>();
            for (SchedulingExecutionVertex vertex : currentRegion) {
                for (SchedulingResultPartition producedResult : vertex.getProducedResults()) {
                    if (!producedResult.getResultType().isReconnectable()) {
                        continue;
                    }
                    for (ConsumerVertexGroup consumerVertexGroup :
                            producedResult.getConsumerVertexGroups()) {
                        for (ExecutionVertexID consumerVertexId : consumerVertexGroup) {
                            SchedulingExecutionVertex consumerVertex =
                                    executionVertexRetriever.apply(consumerVertexId);
                            // Skip the ConsumerVertexGroup if its vertices are outside current
                            // regions and cannot be merged
                            if (!vertexToRegion.containsKey(consumerVertex)) {
                                break;
                            }
                            if (!currentRegion.contains(consumerVertex)) {
                                currentRegionOutEdges.add(
                                        regionIndices.get(vertexToRegion.get(consumerVertex)));
                            }
                        }
                    }
                }
            }
            outEdges.add(currentRegionOutEdges);
        }

        return outEdges;
    }

    private static Iterable<SchedulingResultPartition> getNonReconnectableConsumedResults(
            SchedulingExecutionVertex vertex,
            Function<IntermediateResultPartitionID, ? extends SchedulingResultPartition>
                    resultPartitionRetriever) {
        List<SchedulingResultPartition> nonReconnectableConsumedResults = new ArrayList<>();
        for (ConsumedPartitionGroup consumedPartitionGroup : vertex.getConsumedPartitionGroups()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingResultPartition consumedResult =
                        resultPartitionRetriever.apply(partitionId);
                if (consumedResult.getResultType().isReconnectable()) {
                    // The result types of partitions in one ConsumedPartitionGroup are all the same
                    break;
                }
                nonReconnectableConsumedResults.add(consumedResult);
            }
        }
        return nonReconnectableConsumedResults;
    }

    private SchedulingPipelinedRegionComputeUtil() {}
}
