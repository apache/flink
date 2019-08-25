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

package org.apache.flink.runtime.executiongraph.failover.adapter;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link FailoverTopology} which is an adaptor of {@link ExecutionGraph}.
 */
public class DefaultFailoverTopology implements FailoverTopology {

	private final boolean containsCoLocationConstraints;

	private final List<DefaultFailoverVertex> failoverVertices;

	public DefaultFailoverTopology(ExecutionGraph executionGraph) {
		checkNotNull(executionGraph);

		this.containsCoLocationConstraints = executionGraph.getAllVertices().values().stream()
			.map(ExecutionJobVertex::getCoLocationGroup)
			.anyMatch(Objects::nonNull);

		// generate vertices
		this.failoverVertices = new ArrayList<>();
		final Map<ExecutionVertex, DefaultFailoverVertex> failoverVertexMap = new IdentityHashMap<>();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			final DefaultFailoverVertex failoverVertex = new DefaultFailoverVertex(
				new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex()),
				vertex.getTaskNameWithSubtaskIndex());
			this.failoverVertices.add(failoverVertex);
			failoverVertexMap.put(vertex, failoverVertex);
		}

		// generate edges
		connectVerticesWithEdges(failoverVertexMap);
	}

	private void connectVerticesWithEdges(Map<ExecutionVertex, DefaultFailoverVertex> failoverVertexMap) {
		for (ExecutionVertex vertex : failoverVertexMap.keySet()) {
			final DefaultFailoverVertex failoverVertex = failoverVertexMap.get(vertex);
			vertex.getProducedPartitions().values().stream()
				.map(IntermediateResultPartition::getConsumers)
				.flatMap(Collection::stream)
				.flatMap(Collection::stream)
				.forEach(e -> {
					final DefaultFailoverVertex consumerFailoverVertex = failoverVertexMap.get(e.getTarget());
					final DefaultFailoverEdge failoverEdge = new DefaultFailoverEdge(
						e.getSource().getPartitionId(),
						e.getSource().getResultType(),
						failoverVertex,
						consumerFailoverVertex);
					failoverVertex.addOutputEdge(failoverEdge);
					consumerFailoverVertex.addInputEdge(failoverEdge);
				});
		}
	}

	@Override
	public Iterable<? extends FailoverVertex> getFailoverVertices() {
		return failoverVertices;
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}
}
