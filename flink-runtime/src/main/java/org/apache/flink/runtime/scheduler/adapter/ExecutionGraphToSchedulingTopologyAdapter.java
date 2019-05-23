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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}.
 */
public class ExecutionGraphToSchedulingTopologyAdapter implements SchedulingTopology {

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;

	private final List<SchedulingExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, ? extends SchedulingResultPartition> resultPartitionsById;

	public ExecutionGraphToSchedulingTopologyAdapter(ExecutionGraph graph) {
		checkNotNull(graph, "execution graph can not be null");

		final int totalVertexCnt = graph.getTotalNumberOfVertices();
		int totalPartitionCnt = 0;
		for (ExecutionJobVertex jobVertex : graph.getAllVertices().values()) {
			totalPartitionCnt += jobVertex.getProducedDataSets().length * jobVertex.getParallelism();
		}

		this.executionVerticesById = new HashMap<>(totalVertexCnt);
		this.executionVerticesList = new ArrayList<>(totalVertexCnt);
		Map<IntermediateResultPartitionID, DefaultResultPartition> tmpResultPartitionsById = new HashMap<>(totalPartitionCnt);
		Map<ExecutionVertex, DefaultExecutionVertex> executionVertexMap = new HashMap<>(totalVertexCnt);

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

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return executionVerticesList;
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId) {
		return Optional.ofNullable(executionVerticesById.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.ofNullable(resultPartitionsById.get(intermediateResultPartitionId));
	}

	private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedIntermediatePartitions) {
		List<DefaultResultPartition> producedSchedulingPartitions = new ArrayList<>(producedIntermediatePartitions.size());

		producedIntermediatePartitions.values().forEach(
			irp -> producedSchedulingPartitions.add(
				new DefaultResultPartition(
					irp.getPartitionId(),
					irp.getIntermediateResult().getId(),
					irp.getResultType())));

		return producedSchedulingPartitions;
	}

	private static DefaultExecutionVertex generateSchedulingExecutionVertex(
		ExecutionVertex vertex,
		List<DefaultResultPartition> producedPartitions) {

		DefaultExecutionVertex schedulingVertex = new DefaultExecutionVertex(
			new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()),
			producedPartitions,
			new ExecutionStateSupplier(vertex));

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
					schedulingVertex.addConsumedPartition(partition);
					partition.addConsumer(schedulingVertex);
				}
			}
		}
	}

	private static class ExecutionStateSupplier implements Supplier<ExecutionState> {

		private final ExecutionVertex executionVertex;

		ExecutionStateSupplier(ExecutionVertex vertex) {
			executionVertex = checkNotNull(vertex);
		}

		@Override
		public ExecutionState get() {
			return executionVertex.getExecutionState();
		}
	}
}
