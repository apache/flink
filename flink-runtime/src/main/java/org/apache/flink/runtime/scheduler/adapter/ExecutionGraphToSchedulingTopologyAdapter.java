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
import java.util.Collections;
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

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertices;

	private final List<SchedulingExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitions;

	public ExecutionGraphToSchedulingTopologyAdapter(ExecutionGraph graph) {
		checkNotNull(graph, "execution graph can not be null");

		final int totalVertexCnt = graph.getTotalNumberOfVertices();
		int totalPartitionCnt = 0;
		for (ExecutionJobVertex jobVertex : graph.getAllVertices().values()) {
			totalPartitionCnt += jobVertex.getProducedDataSets().length * jobVertex.getParallelism();
		}

		this.executionVertices = new HashMap<>(totalVertexCnt);
		this.resultPartitions = new HashMap<>(totalPartitionCnt);

		List<DefaultExecutionVertex> verticesList = new ArrayList<>(totalVertexCnt);
		Map<ExecutionVertex, DefaultExecutionVertex> executionVertexMap = new HashMap<>(totalVertexCnt);
		for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
			Map<IntermediateResultPartitionID, IntermediateResultPartition> producedPartitions = vertex.getProducedPartitions();
			List<SchedulingResultPartition> schedulingPartitions = new ArrayList<>(producedPartitions.size());
			for (IntermediateResultPartition irp : producedPartitions.values()) {
				schedulingPartitions.add(new DefaultResultPartition(
					irp.getPartitionId(),
					irp.getIntermediateResult().getId(),
					irp.getResultType()));
			}
			DefaultExecutionVertex scheduleVertex = new DefaultExecutionVertex(
				new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()),
				schedulingPartitions,
				vertex.getInputDependencyConstraint(),
				new ExecutionStateSupplier(vertex));
			this.executionVertices.put(scheduleVertex.getId(), scheduleVertex);
			verticesList.add(scheduleVertex);
			executionVertexMap.put(vertex, scheduleVertex);

			for (SchedulingResultPartition srp : schedulingPartitions) {
				DefaultResultPartition drp = (DefaultResultPartition) srp;
				drp.setProducer(scheduleVertex);
				this.resultPartitions.put(drp.getId(), drp);
			}
		}
		this.executionVerticesList = Collections.unmodifiableList(verticesList);

		for (Map.Entry<ExecutionVertex, DefaultExecutionVertex> mapEntry : executionVertexMap.entrySet()) {
			final DefaultExecutionVertex scheduleVertex = mapEntry.getValue();
			final ExecutionVertex executionVertex = mapEntry.getKey();

			for (int index = 0; index < executionVertex.getNumberOfInputs(); index++) {
				for (ExecutionEdge edge : executionVertex.getInputEdges(index)) {
					DefaultResultPartition partition = this.resultPartitions.get(edge.getSource().getPartitionId());
					scheduleVertex.addConsumedPartition(partition);
					partition.addConsumer(scheduleVertex);
				}
			}
		}
	}

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return executionVerticesList;
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId) {
		return Optional.ofNullable(executionVertices.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.ofNullable(resultPartitions.get(intermediateResultPartitionId));
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
