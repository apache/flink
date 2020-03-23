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

import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import java.util.Collection;
import java.util.Collections;
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
 * Releases blocking intermediate result partitions that are incident to a {@link PipelinedRegion},
 * as soon as the region's execution vertices are finished.
 */
public class RegionPartitionReleaseStrategy implements PartitionReleaseStrategy {

	private final SchedulingTopology<?, ?> schedulingTopology;

	private final Map<PipelinedRegion, PipelinedRegionConsumedBlockingPartitions> consumedBlockingPartitionsByRegion = new IdentityHashMap<>();

	private final Map<ExecutionVertexID, PipelinedRegionExecutionView> regionExecutionViewByVertex = new HashMap<>();

	public RegionPartitionReleaseStrategy(
			final SchedulingTopology<?, ?> schedulingTopology,
			final Set<PipelinedRegion> pipelinedRegions) {

		this.schedulingTopology = checkNotNull(schedulingTopology);

		checkNotNull(pipelinedRegions);
		initConsumedBlockingPartitionsByRegion(pipelinedRegions);
		initRegionExecutionViewByVertex(pipelinedRegions);
	}

	private void initConsumedBlockingPartitionsByRegion(final Set<PipelinedRegion> pipelinedRegions) {
		for (PipelinedRegion pipelinedRegion : pipelinedRegions) {
			final PipelinedRegionConsumedBlockingPartitions consumedPartitions = computeConsumedPartitionsOfVertexRegion(pipelinedRegion);
			consumedBlockingPartitionsByRegion.put(pipelinedRegion, consumedPartitions);
		}
	}

	private void initRegionExecutionViewByVertex(final Set<PipelinedRegion> pipelinedRegions) {
		for (PipelinedRegion pipelinedRegion : pipelinedRegions) {
			final PipelinedRegionExecutionView regionExecutionView = new PipelinedRegionExecutionView(pipelinedRegion);
			for (ExecutionVertexID executionVertexId : pipelinedRegion) {
				regionExecutionViewByVertex.put(executionVertexId, regionExecutionView);
			}
		}
	}

	private PipelinedRegionConsumedBlockingPartitions computeConsumedPartitionsOfVertexRegion(final PipelinedRegion pipelinedRegion) {
		final Set<IntermediateResultPartitionID> resultPartitionsOutsideOfRegion = findResultPartitionsOutsideOfRegion(pipelinedRegion);
		return new PipelinedRegionConsumedBlockingPartitions(pipelinedRegion, resultPartitionsOutsideOfRegion);
	}

	private Set<IntermediateResultPartitionID> findResultPartitionsOutsideOfRegion(final PipelinedRegion pipelinedRegion) {
		final Set<SchedulingResultPartition<?, ?>> allConsumedPartitionsInRegion = pipelinedRegion
			.getExecutionVertexIds()
			.stream()
			.map(schedulingTopology::getVertexOrThrow)
			.flatMap(vertex -> IterableUtils.toStream(vertex.getConsumedResults()))
			.collect(Collectors.toSet());

		return filterResultPartitionsOutsideOfRegion(allConsumedPartitionsInRegion, pipelinedRegion);
	}

	private static Set<IntermediateResultPartitionID> filterResultPartitionsOutsideOfRegion(
			final Collection<SchedulingResultPartition<?, ?>> resultPartitions,
			final PipelinedRegion pipelinedRegion) {

		final Set<IntermediateResultPartitionID> result = new HashSet<>();
		for (final SchedulingResultPartition<?, ?> maybeOutsidePartition : resultPartitions) {
			final SchedulingExecutionVertex<?, ?> producer = maybeOutsidePartition.getProducer();
			if (!pipelinedRegion.contains(producer.getId())) {
				result.add(maybeOutsidePartition.getId());
			}
		}
		return result;
	}

	@Override
	public List<IntermediateResultPartitionID> vertexFinished(final ExecutionVertexID finishedVertex) {
		final PipelinedRegionExecutionView regionExecutionView = getPipelinedRegionExecutionViewForVertex(finishedVertex);
		regionExecutionView.vertexFinished(finishedVertex);

		if (regionExecutionView.isFinished()) {
			final PipelinedRegion pipelinedRegion = getPipelinedRegionForVertex(finishedVertex);
			final PipelinedRegionConsumedBlockingPartitions consumedPartitionsOfVertexRegion = getConsumedBlockingPartitionsForRegion(pipelinedRegion);
			return filterReleasablePartitions(consumedPartitionsOfVertexRegion);
		}
		return Collections.emptyList();
	}

	@Override
	public void vertexUnfinished(final ExecutionVertexID executionVertexId) {
		final PipelinedRegionExecutionView regionExecutionView = getPipelinedRegionExecutionViewForVertex(executionVertexId);
		regionExecutionView.vertexUnfinished(executionVertexId);
	}

	private PipelinedRegionExecutionView getPipelinedRegionExecutionViewForVertex(final ExecutionVertexID executionVertexId) {
		final PipelinedRegionExecutionView pipelinedRegionExecutionView = regionExecutionViewByVertex.get(executionVertexId);
		checkState(pipelinedRegionExecutionView != null,
			"PipelinedRegionExecutionView not found for execution vertex %s", executionVertexId);
		return pipelinedRegionExecutionView;
	}

	private PipelinedRegion getPipelinedRegionForVertex(final ExecutionVertexID executionVertexId) {
		final PipelinedRegionExecutionView pipelinedRegionExecutionView = getPipelinedRegionExecutionViewForVertex(executionVertexId);
		return pipelinedRegionExecutionView.getPipelinedRegion();
	}

	private PipelinedRegionConsumedBlockingPartitions getConsumedBlockingPartitionsForRegion(final PipelinedRegion pipelinedRegion) {
		final PipelinedRegionConsumedBlockingPartitions pipelinedRegionConsumedBlockingPartitions = consumedBlockingPartitionsByRegion.get(pipelinedRegion);
		checkState(pipelinedRegionConsumedBlockingPartitions != null,
			"Consumed partitions not found for pipelined region %s", pipelinedRegion);
		checkState(pipelinedRegionConsumedBlockingPartitions.getPipelinedRegion() == pipelinedRegion);
		return pipelinedRegionConsumedBlockingPartitions;
	}

	private List<IntermediateResultPartitionID> filterReleasablePartitions(final PipelinedRegionConsumedBlockingPartitions consumedPartitionsOfVertexRegion) {
		return consumedPartitionsOfVertexRegion
			.getConsumedBlockingPartitions()
			.stream()
			.filter(this::areConsumerRegionsFinished)
			.collect(Collectors.toList());
	}

	private boolean areConsumerRegionsFinished(final IntermediateResultPartitionID resultPartitionId) {
		final SchedulingResultPartition<?, ?> resultPartition = schedulingTopology.getResultPartitionOrThrow(resultPartitionId);
		return IterableUtils.toStream(resultPartition.getConsumers())
			.map(SchedulingExecutionVertex::getId)
			.allMatch(this::isRegionOfVertexFinished);
	}

	private boolean isRegionOfVertexFinished(final ExecutionVertexID executionVertexId) {
		final PipelinedRegionExecutionView regionExecutionView = getPipelinedRegionExecutionViewForVertex(executionVertexId);
		return regionExecutionView.isFinished();
	}

	/**
	 * Factory for {@link PartitionReleaseStrategy}.
	 */
	public static class Factory implements PartitionReleaseStrategy.Factory {

		@Override
		public PartitionReleaseStrategy createInstance(final SchedulingTopology<?, ?> schedulingStrategy) {

			final Set<? extends Set<? extends SchedulingExecutionVertex<?, ?>>> distinctRegions =
				PipelinedRegionComputeUtil.computePipelinedRegions(schedulingStrategy);

			return new RegionPartitionReleaseStrategy(
				schedulingStrategy,
				PipelinedRegionComputeUtil.toPipelinedRegionsSet(distinctRegions));
		}
	}
}
