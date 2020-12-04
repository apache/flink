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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}.
 */
public class DefaultExecutionTopology implements SchedulingTopology {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionTopology.class);

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;

	private final List<DefaultExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

	@Nullable
	private Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex;

	@Nullable
	private List<DefaultSchedulingPipelinedRegion> pipelinedRegions;

	private DefaultExecutionTopology(ExecutionGraph graph) {
		checkNotNull(graph, "execution graph can not be null");

		this.executionVerticesById = new HashMap<>();
		this.executionVerticesList = new ArrayList<>(graph.getTotalNumberOfVertices());
		Map<IntermediateResultPartitionID, DefaultResultPartition> tmpResultPartitionsById = new HashMap<>();
		Map<ExecutionVertex, DefaultExecutionVertex> executionVertexMap = new HashMap<>();

		for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
			List<DefaultResultPartition> producedPartitions = generateProducedSchedulingResultPartition(vertex.getProducedPartitions());

			producedPartitions.forEach(partition -> tmpResultPartitionsById.put(partition.getId(), partition));

			DefaultExecutionVertex schedulingVertex = generateSchedulingExecutionVertex(vertex, producedPartitions);
			this.executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
			this.executionVerticesList.add(schedulingVertex);
			executionVertexMap.put(vertex, schedulingVertex);
		}
		this.resultPartitionsById = tmpResultPartitionsById;

		connectVerticesToConsumedPartitions(executionVertexMap, tmpResultPartitionsById);
	}

	private void setPipelinedRegions(List<DefaultSchedulingPipelinedRegion> pipelinedRegions) {
		this.pipelinedRegions = checkNotNull(pipelinedRegions);

		this.pipelinedRegionsByVertex = new HashMap<>();
		for (DefaultSchedulingPipelinedRegion pipelinedRegion : pipelinedRegions) {
			for (SchedulingExecutionVertex executionVertex : pipelinedRegion.getVertices()) {
				pipelinedRegionsByVertex.put(executionVertex.getId(), pipelinedRegion);
			}
		}
	}

	@Override
	public Iterable<DefaultExecutionVertex> getVertices() {
		return executionVerticesList;
	}

	@Override
	public DefaultExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
		final DefaultExecutionVertex executionVertex = executionVerticesById.get(executionVertexId);
		if (executionVertex == null) {
			throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
		}
		return executionVertex;
	}

	@Override
	public DefaultResultPartition getResultPartition(final IntermediateResultPartitionID intermediateResultPartitionId) {
		final DefaultResultPartition resultPartition = resultPartitionsById.get(intermediateResultPartitionId);
		if (resultPartition == null) {
			throw new IllegalArgumentException("can not find partition: " + intermediateResultPartitionId);
		}
		return resultPartition;
	}

	@Override
	public Iterable<DefaultSchedulingPipelinedRegion> getAllPipelinedRegions() {
		checkNotNull(pipelinedRegions);

		return Collections.unmodifiableCollection(pipelinedRegions);
	}

	@Override
	public DefaultSchedulingPipelinedRegion getPipelinedRegionOfVertex(final ExecutionVertexID vertexId) {
		checkNotNull(pipelinedRegionsByVertex);

		final DefaultSchedulingPipelinedRegion pipelinedRegion = pipelinedRegionsByVertex.get(vertexId);
		if (pipelinedRegion == null) {
			throw new IllegalArgumentException("Unknown execution vertex " + vertexId);
		}
		return pipelinedRegion;
	}

	private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedIntermediatePartitions) {

		List<DefaultResultPartition> producedSchedulingPartitions = new ArrayList<>(producedIntermediatePartitions.size());

		producedIntermediatePartitions.values().forEach(
			irp -> producedSchedulingPartitions.add(
				new DefaultResultPartition(
					irp.getPartitionId(),
					irp.getIntermediateResult().getId(),
					irp.getResultType(),
					() -> irp.isConsumable() ? ResultPartitionState.CONSUMABLE : ResultPartitionState.CREATED)));

		return producedSchedulingPartitions;
	}

	private static DefaultExecutionVertex generateSchedulingExecutionVertex(
		ExecutionVertex vertex,
		List<DefaultResultPartition> producedPartitions) {

		DefaultExecutionVertex schedulingVertex = new DefaultExecutionVertex(
			vertex.getID(),
			producedPartitions,
			() -> vertex.getExecutionState(),
			vertex.getInputDependencyConstraint());

		producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

		return schedulingVertex;
	}

	private static void connectVerticesToConsumedPartitions(
		Map<ExecutionVertex, DefaultExecutionVertex> executionVertexMap,
		Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitions) {

		for (Map.Entry<ExecutionVertex, DefaultExecutionVertex> mapEntry : executionVertexMap.entrySet()) {
			final DefaultExecutionVertex schedulingVertex = mapEntry.getValue();
			final ExecutionVertex executionVertex = mapEntry.getKey();

			for (int index = 0; index < executionVertex.getNumberOfInputs(); index++) {
				for (ExecutionEdge edge : executionVertex.getInputEdges(index)) {
					DefaultResultPartition partition = resultPartitions.get(edge.getSource().getPartitionId());
					schedulingVertex.addConsumedResult(partition);
					partition.addConsumer(schedulingVertex);
				}
			}
		}
	}

	public static DefaultExecutionTopology fromExecutionGraph(ExecutionGraph executionGraph) {
		final DefaultExecutionTopology topology = new DefaultExecutionTopology(executionGraph);

		final List<DefaultSchedulingPipelinedRegion> pipelinedRegions = generatePipelinedRegions(topology);
		topology.setPipelinedRegions(pipelinedRegions);
		ensureCoLocatedVerticesInSameRegion(pipelinedRegions, executionGraph);

		return topology;
	}

	private static List<DefaultSchedulingPipelinedRegion> generatePipelinedRegions(DefaultExecutionTopology topology) {
		final long buildRegionsStartTime = System.nanoTime();

		final Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions = PipelinedRegionComputeUtil
			.computePipelinedRegions(topology);

		final List<DefaultSchedulingPipelinedRegion> pipelinedRegions = new ArrayList<>();
		for (Set<? extends SchedulingExecutionVertex> rawPipelinedRegion : rawPipelinedRegions) {
			//noinspection unchecked
			final DefaultSchedulingPipelinedRegion pipelinedRegion = new DefaultSchedulingPipelinedRegion(
				(Set<DefaultExecutionVertex>) rawPipelinedRegion);
			pipelinedRegions.add(pipelinedRegion);
		}

		final long buildRegionsDuration = (System.nanoTime() - buildRegionsStartTime) / 1_000_000;
		LOG.info("Built {} pipelined regions in {} ms", pipelinedRegions.size(), buildRegionsDuration);

		return pipelinedRegions;
	}

	/**
	 * Co-location constraints are only used for iteration head and tail.
	 * A paired head and tail needs to be in the same pipelined region so
	 * that they can be restarted together.
	 */
	private static void ensureCoLocatedVerticesInSameRegion(
			List<DefaultSchedulingPipelinedRegion> pipelinedRegions,
			ExecutionGraph executionGraph) {

		final Map<CoLocationConstraint, DefaultSchedulingPipelinedRegion> constraintToRegion = new IdentityHashMap<>();
		for (DefaultSchedulingPipelinedRegion region : pipelinedRegions) {
			for (DefaultExecutionVertex vertex : region.getVertices()) {
				final CoLocationConstraint constraint = getCoLocationConstraint(vertex.getId(), executionGraph);
				if (constraint != null) {
					final DefaultSchedulingPipelinedRegion regionOfConstraint = constraintToRegion.get(constraint);
					checkState(
						regionOfConstraint == null || regionOfConstraint == region,
						"co-located tasks must be in the same pipelined region");
					constraintToRegion.putIfAbsent(constraint, region);
				}
			}
		}
	}

	private static CoLocationConstraint getCoLocationConstraint(
			ExecutionVertexID executionVertexId,
			ExecutionGraph executionGraph) {

		return executionGraph
			.getJobVertex(executionVertexId.getJobVertexId())
			.getTaskVertices()[executionVertexId.getSubtaskIndex()]
			.getLocationConstraint();
	}
}
