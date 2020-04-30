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
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Releases blocking intermediate result partitions that are incident to a {@link SchedulingPipelinedRegion},
 * as soon as the region's execution vertices are finished.
 */
public class RegionPartitionReleaseStrategy implements PartitionReleaseStrategy {

	private final SchedulingTopology schedulingTopology;

	private final Map<ExecutionVertexID, PipelinedRegionExecutionView> regionExecutionViewByVertex = new HashMap<>();

	public RegionPartitionReleaseStrategy(final SchedulingTopology schedulingTopology) {
		this.schedulingTopology = checkNotNull(schedulingTopology);

		initRegionExecutionViewByVertex();
	}

	private void initRegionExecutionViewByVertex() {
		for (SchedulingPipelinedRegion pipelinedRegion : schedulingTopology.getAllPipelinedRegions()) {
			final PipelinedRegionExecutionView regionExecutionView = new PipelinedRegionExecutionView(pipelinedRegion);
			for (SchedulingExecutionVertex executionVertexId : pipelinedRegion.getVertices()) {
				regionExecutionViewByVertex.put(executionVertexId.getId(), regionExecutionView);
			}
		}
	}

	@Override
	public List<IntermediateResultPartitionID> vertexFinished(final ExecutionVertexID finishedVertex) {
		final PipelinedRegionExecutionView regionExecutionView = getPipelinedRegionExecutionViewForVertex(finishedVertex);
		regionExecutionView.vertexFinished(finishedVertex);

		if (regionExecutionView.isFinished()) {
			final SchedulingPipelinedRegion pipelinedRegion = schedulingTopology.getPipelinedRegionOfVertex(finishedVertex);
			return filterReleasablePartitions(pipelinedRegion.getConsumedResults());
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

	private List<IntermediateResultPartitionID> filterReleasablePartitions(final Iterable<? extends SchedulingResultPartition> schedulingResultPartitions) {
		return IterableUtils.toStream(schedulingResultPartitions)
			.map(SchedulingResultPartition::getId)
			.filter(this::areConsumerRegionsFinished)
			.collect(Collectors.toList());
	}

	private boolean areConsumerRegionsFinished(final IntermediateResultPartitionID resultPartitionId) {
		final SchedulingResultPartition resultPartition = schedulingTopology.getResultPartition(resultPartitionId);
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
		public PartitionReleaseStrategy createInstance(final SchedulingTopology schedulingStrategy) {
			return new RegionPartitionReleaseStrategy(schedulingStrategy);
		}
	}
}
