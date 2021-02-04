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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.failover.flip1.StronglyConnectedComponentsComputeUtils;
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

import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.mergeRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.uniqueRegions;
import static org.apache.flink.util.Preconditions.checkState;

/** Utility for computing {@link SchedulingPipelinedRegion}. */
public final class DefaultSchedulingPipelinedRegionComputeUtil {

    public static Set<Set<SchedulingExecutionVertex>> computePipelinedRegions(
            final Iterable<DefaultExecutionVertex> topologicallySortedVertexes) {
        final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion =
                buildRawRegions(topologicallySortedVertexes);
        return mergeRegionsOnCycles(vertexToRegion);
    }

    private static Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> buildRawRegions(
            final Iterable<? extends DefaultExecutionVertex> topologicallySortedVertexes) {

        final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion =
                new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (DefaultExecutionVertex vertex : topologicallySortedVertexes) {
            Set<SchedulingExecutionVertex> currentRegion = new HashSet<>();
            currentRegion.add(vertex);
            vertexToRegion.put(vertex, currentRegion);

            for (ConsumedPartitionGroup consumedResultIds : vertex.getGroupedConsumedResults()) {
                // Similar to the BLOCKING ResultPartitionType, each vertex connected through
                // PIPELINED_APPROXIMATE
                // is also considered as a single region. This attribute is called "reconnectable".
                // reconnectable will be removed after FLINK-19895, see also {@link
                // ResultPartitionType#isReconnectable}
                for (IntermediateResultPartitionID consumerId :
                        consumedResultIds.getResultPartitions()) {
                    SchedulingResultPartition consumedResult =
                            vertex.getResultPartition(consumerId);
                    if (!consumedResult.getResultType().isReconnectable()) {
                        final SchedulingExecutionVertex producerVertex =
                                consumedResult.getProducer();
                        final Set<SchedulingExecutionVertex> producerRegion =
                                vertexToRegion.get(producerVertex);

                        if (producerRegion == null) {
                            throw new IllegalStateException(
                                    "Producer task "
                                            + producerVertex.getId()
                                            + " failover region is null while calculating failover region for the consumer task "
                                            + vertex.getId()
                                            + ". This should be a failover region building bug.");
                        }

                        // check if it is the same as the producer region, if so skip the merge
                        // this check can significantly reduce compute complexity in All-to-All
                        // PIPELINED edge case
                        if (currentRegion != producerRegion) {
                            currentRegion =
                                    mergeRegions(currentRegion, producerRegion, vertexToRegion);
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        return vertexToRegion;
    }

    public static Set<Set<SchedulingExecutionVertex>> mergeRegionsOnCycles(
            final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion) {

        final List<Set<SchedulingExecutionVertex>> regionList =
                new ArrayList<>(uniqueRegions(vertexToRegion));
        final List<List<Integer>> outEdges = buildOutEdgesDesc(vertexToRegion, regionList);
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
            final List<Set<SchedulingExecutionVertex>> regionList) {

        final Map<Set<SchedulingExecutionVertex>, Integer> regionIndices = new IdentityHashMap<>();
        for (int i = 0; i < regionList.size(); i++) {
            regionIndices.put(regionList.get(i), i);
        }

        final List<List<Integer>> outEdges = new ArrayList<>(regionList.size());
        for (Set<SchedulingExecutionVertex> schedulingExecutionVertices : regionList) {
            final List<Integer> currentRegionOutEdges = new ArrayList<>();
            for (SchedulingExecutionVertex vertex : schedulingExecutionVertices) {
                for (SchedulingResultPartition producedResult : vertex.getProducedResults()) {
                    if (producedResult.getResultType().isPipelined()) {
                        continue;
                    }
                    for (ConsumerVertexGroup consumerGroup : producedResult.getGroupedConsumers()) {
                        for (ExecutionVertexID consumerVertexId : consumerGroup.getVertices()) {
                            SchedulingExecutionVertex consumerVertex =
                                    producedResult.getVertex(consumerVertexId);
                            if (!schedulingExecutionVertices.contains(consumerVertex)) {
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
}
