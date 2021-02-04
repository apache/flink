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

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyUtils.initPartitionGroupConsumerRegions;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Releases blocking intermediate result partitions that are incident to a {@link
 * SchedulingPipelinedRegion}, as soon as the region's execution vertices are finished.
 */
public class RegionPartitionReleaseStrategy implements PartitionReleaseStrategy {

    private final SchedulingTopology schedulingTopology;

    private final Map<ExecutionVertexID, PipelinedRegionExecutionView> regionExecutionViewByVertex =
            new HashMap<>();

    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new HashMap<>();

    /** PartitionID -> PartitionID group. */
    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>>
            partitionGroupsById = new HashMap<>();

    /** Region -> Region set. Only contains regions with consumed results. */
    private final Map<SchedulingPipelinedRegion, List<Set<SchedulingPipelinedRegion>>>
            consumerRegionSetMapping = new HashMap<>();

    /** The progress of Region set. Only contains regions with consumed results. */
    private final Map<Set<SchedulingPipelinedRegion>, Set<SchedulingPipelinedRegion>>
            consumerRegionFinishProgress = new HashMap<>();

    /** The progress of PartitionID group. */
    private final Map<ConsumedPartitionGroup, Boolean> partitionGroupFinishProgress =
            new HashMap<>();

    public RegionPartitionReleaseStrategy(final SchedulingTopology schedulingTopology) {
        this.schedulingTopology = checkNotNull(schedulingTopology);
        init();
    }

    private void init() {
        initRegionExecutionViewByVertex();

        initPartitionGroupConsumerRegions(
                schedulingTopology.getAllPipelinedRegions(), partitionGroupConsumerRegions);

        for (ConsumedPartitionGroup group : partitionGroupConsumerRegions.keySet()) {
            for (IntermediateResultPartitionID partitionId : group.getResultPartitions()) {
                partitionGroupsById
                        .computeIfAbsent(partitionId, id -> new ArrayList<>())
                        .add(group);
            }
        }

        for (Set<SchedulingPipelinedRegion> schedulingPipelinedRegionSet :
                partitionGroupConsumerRegions.values()) {
            for (SchedulingPipelinedRegion schedulingRegion : schedulingPipelinedRegionSet) {
                consumerRegionSetMapping
                        .computeIfAbsent(schedulingRegion, region -> new ArrayList<>())
                        .add(schedulingPipelinedRegionSet);
            }
            consumerRegionFinishProgress.computeIfAbsent(
                    schedulingPipelinedRegionSet, set -> new HashSet<>());
        }

        for (ConsumedPartitionGroup partitionIdGroup : partitionGroupConsumerRegions.keySet()) {
            partitionGroupFinishProgress.putIfAbsent(partitionIdGroup, Boolean.FALSE);
        }
    }

    private void initRegionExecutionViewByVertex() {
        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            final PipelinedRegionExecutionView regionExecutionView =
                    new PipelinedRegionExecutionView(pipelinedRegion);
            for (SchedulingExecutionVertex executionVertexId : pipelinedRegion.getVertices()) {
                regionExecutionViewByVertex.put(executionVertexId.getId(), regionExecutionView);
            }
        }
    }

    @Override
    public List<IntermediateResultPartitionID> vertexFinished(
            final ExecutionVertexID finishedVertex) {
        final PipelinedRegionExecutionView regionExecutionView =
                getPipelinedRegionExecutionViewForVertex(finishedVertex);
        regionExecutionView.vertexFinished(finishedVertex);

        if (regionExecutionView.isFinished()) {
            final SchedulingPipelinedRegion pipelinedRegion =
                    schedulingTopology.getPipelinedRegionOfVertex(finishedVertex);

            for (Set<SchedulingPipelinedRegion> regionSet :
                    consumerRegionSetMapping.getOrDefault(
                            pipelinedRegion, Collections.emptyList())) {
                consumerRegionFinishProgress
                        .computeIfAbsent(regionSet, set -> new HashSet<>())
                        .add(pipelinedRegion);
            }

            return filterReleasablePartitions(pipelinedRegion.getGroupedConsumedResults());
        }
        return Collections.emptyList();
    }

    @Override
    public void vertexUnfinished(final ExecutionVertexID executionVertexId) {
        final PipelinedRegionExecutionView regionExecutionView =
                getPipelinedRegionExecutionViewForVertex(executionVertexId);

        regionExecutionView.vertexUnfinished(executionVertexId);
        final SchedulingPipelinedRegion pipelinedRegion =
                schedulingTopology.getPipelinedRegionOfVertex(executionVertexId);

        for (Set<SchedulingPipelinedRegion> regionSet :
                consumerRegionSetMapping.getOrDefault(pipelinedRegion, Collections.emptyList())) {
            consumerRegionFinishProgress
                    .computeIfAbsent(regionSet, set -> new HashSet<>())
                    .remove(pipelinedRegion);
        }
    }

    private PipelinedRegionExecutionView getPipelinedRegionExecutionViewForVertex(
            final ExecutionVertexID executionVertexId) {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                regionExecutionViewByVertex.get(executionVertexId);
        checkState(
                pipelinedRegionExecutionView != null,
                "PipelinedRegionExecutionView not found for execution vertex %s",
                executionVertexId);
        return pipelinedRegionExecutionView;
    }

    private List<IntermediateResultPartitionID> filterReleasablePartitions(
            final List<ConsumedPartitionGroup> schedulingResultPartitions) {
        final List<IntermediateResultPartitionID> filteredPartitions = new ArrayList<>();
        for (ConsumedPartitionGroup group : schedulingResultPartitions) {
            final Set<SchedulingPipelinedRegion> consumerRegionSet =
                    partitionGroupConsumerRegions.get(group);
            if (consumerRegionFinishProgress.get(consumerRegionSet).size()
                    == consumerRegionSet.size()) {
                partitionGroupFinishProgress.replace(group, Boolean.TRUE);
                for (IntermediateResultPartitionID partitionId : group.getResultPartitions()) {
                    if (partitionGroupsById.get(partitionId).stream()
                            .map(partitionGroupFinishProgress::get)
                            .allMatch(progress -> progress == Boolean.TRUE)) {
                        filteredPartitions.add(partitionId);
                    }
                }
            }
        }
        return filteredPartitions;
    }

    /** Factory for {@link PartitionReleaseStrategy}. */
    public static class Factory implements PartitionReleaseStrategy.Factory {

        @Override
        public PartitionReleaseStrategy createInstance(
                final SchedulingTopology schedulingStrategy) {
            return new RegionPartitionReleaseStrategy(schedulingStrategy);
        }
    }
}
