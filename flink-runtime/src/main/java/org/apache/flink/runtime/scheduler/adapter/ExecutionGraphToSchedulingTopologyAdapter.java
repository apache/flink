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
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}.
 */
public class ExecutionGraphToSchedulingTopologyAdapter implements SchedulingTopology {

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertices;

	private final List<DefaultExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitions;

	public ExecutionGraphToSchedulingTopologyAdapter(ExecutionGraph graph) {
		Preconditions.checkNotNull(graph, "execution graph can not be null");

		this.executionVertices = new HashMap<>();
		this.resultPartitions = new HashMap<>();
		this.executionVerticesList = new ArrayList<>();

		Map<ExecutionVertex, DefaultExecutionVertex> executionVertexMap = new HashMap<>();
		for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
			DefaultExecutionVertex scheduleVertex = new DefaultExecutionVertex(vertex);
			executionVertices.put(scheduleVertex.getId(), scheduleVertex);
			executionVerticesList.add(scheduleVertex);
			executionVertexMap.put(vertex, scheduleVertex);

			for (SchedulingResultPartition srp : scheduleVertex.getProducedResultPartitions()) {
				DefaultResultPartition drp = (DefaultResultPartition) srp;
				resultPartitions.put(drp.getId(), drp);
			}
		}

		for (Map.Entry<ExecutionVertex, DefaultExecutionVertex> mapEntry : executionVertexMap.entrySet()) {
			final DefaultExecutionVertex scheduleVertex = mapEntry.getValue();
			final ExecutionVertex executionVertex = mapEntry.getKey();

			for (int index = 0; index < executionVertex.getNumberOfInputs(); index++) {
				for (ExecutionEdge edge : executionVertex.getInputEdges(index)) {
					DefaultResultPartition partition = resultPartitions.get(edge.getSource().getPartitionId());
					scheduleVertex.addConsumedPartition(partition);
					partition.addConsumer(scheduleVertex);
				}
			}
		}
	}

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return Collections.unmodifiableList(executionVerticesList);
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId) {
		return Optional.of(executionVertices.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.of(resultPartitions.get(intermediateResultPartitionId));
	}
}
