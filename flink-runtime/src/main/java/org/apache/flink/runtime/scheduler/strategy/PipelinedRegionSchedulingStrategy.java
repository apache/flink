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
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);

	/** Result partitions are correlated if they have the same result id. */
	private final Map<IntermediateDataSetID, Set<SchedulingResultPartition>> correlatedResultPartitions = new HashMap<>();

	private final Map<IntermediateResultPartitionID, Set<SchedulingPipelinedRegion>> partitionConsumerRegions = new HashMap<>();

	private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted = new IdentityHashMap<>();

	public PipelinedRegionSchedulingStrategy(
			final SchedulerOperations schedulerOperations,
			final SchedulingTopology schedulingTopology) {

		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);

		init();
	}

	private void init() {
		for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
			for (SchedulingResultPartition partition : region.getConsumedResults()) {
				checkState(partition.getResultType().isBlocking());

				partitionConsumerRegions.computeIfAbsent(partition.getId(), pid -> new HashSet<>()).add(region);
				correlatedResultPartitions.computeIfAbsent(partition.getResultId(), rid -> new HashSet<>()).add(partition);
			}
		}

		for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
			final SchedulingPipelinedRegion region = schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
			regionVerticesSorted.computeIfAbsent(region, r -> new ArrayList<>()).add(vertex.getId());
		}
	}

	@Override
	public void startScheduling() {
		final Set<SchedulingPipelinedRegion> sourceRegions = IterableUtils
			.toStream(schedulingTopology.getAllPipelinedRegions())
			.filter(region -> !region.getConsumedResults().iterator().hasNext())
			.collect(Collectors.toSet());
		maybeScheduleRegions(sourceRegions);
	}

	@Override
	public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
		final Set<SchedulingPipelinedRegion> regionsToRestart = verticesToRestart.stream()
			.map(schedulingTopology::getPipelinedRegionOfVertex)
			.collect(Collectors.toSet());
		maybeScheduleRegions(regionsToRestart);
	}

	@Override
	public void onExecutionStateChange(final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
		if (executionState == ExecutionState.FINISHED) {
			final Set<SchedulingResultPartition> finishedPartitions = IterableUtils
				.toStream(schedulingTopology.getVertex(executionVertexId).getProducedResults())
				.filter(partition -> partitionConsumerRegions.containsKey(partition.getId()))
				.filter(partition -> partition.getState() == ResultPartitionState.CONSUMABLE)
				.flatMap(partition -> correlatedResultPartitions.get(partition.getResultId()).stream())
				.collect(Collectors.toSet());

			final Set<SchedulingPipelinedRegion> consumerRegions = finishedPartitions.stream()
				.flatMap(partition -> partitionConsumerRegions.get(partition.getId()).stream())
				.collect(Collectors.toSet());
			maybeScheduleRegions(consumerRegions);
		}
	}

	@Override
	public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {
	}

	private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
		final List<SchedulingPipelinedRegion> regionsSorted =
			SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(schedulingTopology, regions);
		for (SchedulingPipelinedRegion region : regionsSorted) {
			maybeScheduleRegion(region);
		}
	}

	private void maybeScheduleRegion(final SchedulingPipelinedRegion region) {
		if (!areRegionInputsAllConsumable(region)) {
			return;
		}

		checkState(areRegionVerticesAllInCreatedState(region), "BUG: trying to schedule a region which is not in CREATED state");

		final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptions(
				regionVerticesSorted.get(region),
				id -> deploymentOption);
		schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
	}

	private boolean areRegionInputsAllConsumable(final SchedulingPipelinedRegion region) {
		for (SchedulingResultPartition partition : region.getConsumedResults()) {
			if (partition.getState() != ResultPartitionState.CONSUMABLE) {
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

	/**
	 * The factory for creating {@link PipelinedRegionSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {
		@Override
		public SchedulingStrategy createInstance(
				final SchedulerOperations schedulerOperations,
				final SchedulingTopology schedulingTopology) {
			return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
