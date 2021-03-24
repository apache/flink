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
import java.util.List;
import java.util.Map;

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

    private final Map<ConsumedPartitionGroup, ConsumerRegionGroupExecutionView>
            partitionGroupConsumerRegions = new HashMap<>();

    private final ConsumerRegionGroupExecutionViewMaintainer
            consumerRegionGroupExecutionViewMaintainer;

    public RegionPartitionReleaseStrategy(final SchedulingTopology schedulingTopology) {
        this.schedulingTopology = checkNotNull(schedulingTopology);

        initRegionExecutionViewByVertex();

        initPartitionGroupConsumerRegions();

        this.consumerRegionGroupExecutionViewMaintainer =
                new ConsumerRegionGroupExecutionViewMaintainer(
                        partitionGroupConsumerRegions.values());
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

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllBlockingConsumedPartitionGroups()) {
                partitionGroupConsumerRegions
                        .computeIfAbsent(
                                consumedPartitionGroup, g -> new ConsumerRegionGroupExecutionView())
                        .add(region);
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
            consumerRegionGroupExecutionViewMaintainer.regionFinished(pipelinedRegion);

            return filterReleasablePartitions(
                    pipelinedRegion.getAllBlockingConsumedPartitionGroups());
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
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(pipelinedRegion);
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
            final Iterable<ConsumedPartitionGroup> consumedPartitionGroups) {

        final List<IntermediateResultPartitionID> releasablePartitions = new ArrayList<>();

        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            final ConsumerRegionGroupExecutionView consumerRegionGroup =
                    partitionGroupConsumerRegions.get(consumedPartitionGroup);
            if (consumerRegionGroup.isFinished()) {
                for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                    // At present, there's only one ConsumerVertexGroup for each
                    // ConsumedPartitionGroup, so if a ConsumedPartitionGroup is fully consumed, all
                    // it's partitions are releasable.
                    releasablePartitions.add(partitionId);
                }
            }
        }

        return releasablePartitions;
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
