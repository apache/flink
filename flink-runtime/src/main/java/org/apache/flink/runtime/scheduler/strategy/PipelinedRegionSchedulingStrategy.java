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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyUtils.initPartitionGroupConsumerRegions;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final DeploymentOption deploymentOption = new DeploymentOption(false);

    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new HashMap<>();

    private final Map<IntermediateDataSetID, Set<ConsumedPartitionGroup>>
            correlatedResultPartitionGroups = new HashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted =
            new IdentityHashMap<>();

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    private void init() {

        initPartitionGroupConsumerRegions(
                schedulingTopology.getAllPipelinedRegions(), partitionGroupConsumerRegions);

        for (ConsumedPartitionGroup group : partitionGroupConsumerRegions.keySet()) {
            for (IntermediateResultPartitionID partitionId : group.getResultPartitions()) {
                correlatedResultPartitionGroups
                        .computeIfAbsent(
                                partitionId.getIntermediateDataSetID(), id -> new HashSet<>())
                        .add(group);
            }
        }

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region =
                    schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }

    @Override
    public void startScheduling() {
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        .filter(region -> !region.getGroupedConsumedResults().iterator().hasNext())
                        .collect(Collectors.toSet());
        maybeScheduleRegions(sourceRegions);
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            final Set<ConsumedPartitionGroup> finishedPartitions =
                    IterableUtils.toStream(
                                    schedulingTopology
                                            .getVertex(executionVertexId)
                                            .getProducedResults())
                            .filter(
                                    partition ->
                                            partition.getState() == ResultPartitionState.CONSUMABLE)
                            .flatMap(
                                    partition ->
                                            correlatedResultPartitionGroups
                                                    .getOrDefault(
                                                            partition.getResultId(),
                                                            Collections.emptySet())
                                                    .stream())
                            .collect(Collectors.toSet());

            final Set<SchedulingPipelinedRegion> consumerRegions =
                    finishedPartitions.stream()
                            .flatMap(
                                    partitionGroup ->
                                            partitionGroupConsumerRegions.get(partitionGroup)
                                                    .stream())
                            .collect(Collectors.toSet());
            maybeScheduleRegions(consumerRegions);
        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        final List<SchedulingPipelinedRegion> regionsSorted =
                SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regions);

        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        for (SchedulingPipelinedRegion region : regionsSorted) {
            maybeScheduleRegion(region, consumableStatusCache);
        }
    }

    private void maybeScheduleRegion(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        if (!areRegionInputsAllConsumable(region, consumableStatusCache)) {
            return;
        }

        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");

        final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions =
                SchedulingStrategyUtils.createExecutionVertexDeploymentOptions(
                        regionVerticesSorted.get(region), id -> deploymentOption);
        schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup partitionIdGroup : region.getGroupedConsumedResults()) {
            if (!consumableStatusCache.computeIfAbsent(
                    partitionIdGroup,
                    group ->
                            areResultPartitionGroupConsumable(group, region::getResultPartition))) {
                return false;
            }
        }
        return true;
    }

    private boolean areResultPartitionGroupConsumable(
            final ConsumedPartitionGroup partitionIdGroup,
            final Function<IntermediateResultPartitionID, SchedulingResultPartition>
                    resultPartitionsById) {
        // For grouped pipelined result partitions, they may not be consumable at the same time
        for (IntermediateResultPartitionID partitionId : partitionIdGroup.getResultPartitions()) {
            if (resultPartitionsById.apply(partitionId).getState()
                    != ResultPartitionState.CONSUMABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
