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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * A FailoverTopology implementation for tests.
 */
public class TestFailoverTopology implements FailoverTopology {

	private final Collection<FailoverVertex> vertices;
	private final boolean containsCoLocationConstraints;

	public TestFailoverTopology(Collection<FailoverVertex> vertices, boolean containsCoLocationConstraints) {
		this.vertices = vertices;
		this.containsCoLocationConstraints = containsCoLocationConstraints;
	}

	@Override
	public Iterable<? extends FailoverVertex> getFailoverVertices() {
		return vertices::iterator;
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}

	/**
	 * A FailoverVertex implementation for tests.
	 */
	public static class TestFailoverVertex implements FailoverVertex {

		private final Collection<FailoverEdge> inputEdges = new ArrayList<>();
		private final Collection<FailoverEdge> outputEdges = new ArrayList<>();
		private final ExecutionVertexID id;
		private final String name;

		public TestFailoverVertex(ExecutionVertexID id, String name) {
			this.id = id;
			this.name = name;
		}

		void addInputEdge(FailoverEdge edge) {
			inputEdges.add(edge);
		}

		void addOuputEdge(FailoverEdge edge) {
			outputEdges.add(edge);
		}

		public ExecutionVertexID getExecutionVertexID() {
			return id;
		}

		@Override
		public String getExecutionVertexName() {
			return name;
		}

		@Override
		public Iterable<? extends FailoverEdge> getInputEdges() {
			return inputEdges::iterator;
		}

		@Override
		public Iterable<? extends FailoverEdge> getOutputEdges() {
			return outputEdges::iterator;
		}
	}

	/**
	 * A FailoverEdge implementation for tests.
	 */
	public static class TestFailoverEdge implements FailoverEdge {

		private final IntermediateResultPartitionID resultPartitionID;
		private final ResultPartitionType resultPartitionType;
		private final FailoverVertex sourceVertex;
		private final FailoverVertex targetVertex;

		public TestFailoverEdge(IntermediateResultPartitionID resultPartitionID, ResultPartitionType resultPartitionType, FailoverVertex sourceVertex, FailoverVertex targetVertex) {
			this.resultPartitionID = resultPartitionID;
			this.resultPartitionType = resultPartitionType;
			this.sourceVertex = sourceVertex;
			this.targetVertex = targetVertex;
		}

		@Override
		public IntermediateResultPartitionID getResultPartitionID() {
			return resultPartitionID;
		}

		@Override
		public ResultPartitionType getResultPartitionType() {
			return resultPartitionType;
		}

		@Override
		public FailoverVertex getSourceVertex() {
			return sourceVertex;
		}

		@Override
		public FailoverVertex getTargetVertex() {
			return targetVertex;
		}
	}

	/**
	 * Builder for {@link TestFailoverTopology}.
	 */
	public static class Builder {
		private boolean containsCoLocationConstraints = false;
		private Collection<FailoverVertex> vertices = new ArrayList<>();

		public TestFailoverVertex newVertex() {
			return newVertex(UUID.randomUUID().toString());
		}

		public TestFailoverVertex newVertex(String name) {
			TestFailoverVertex testFailoverVertex = new TestFailoverVertex(new ExecutionVertexID(new JobVertexID(), 0), name);
			vertices.add(testFailoverVertex);
			return testFailoverVertex;
		}

		public Builder connect(TestFailoverVertex source, TestFailoverVertex target, ResultPartitionType partitionType) {
			FailoverEdge edge = new TestFailoverEdge(new IntermediateResultPartitionID(), partitionType, source, target);
			source.addOuputEdge(edge);
			target.addInputEdge(edge);

			return this;
		}

		public Builder connect(TestFailoverVertex source, TestFailoverVertex target, ResultPartitionType partitionType, IntermediateResultPartitionID partitionID) {
			FailoverEdge edge = new TestFailoverEdge(partitionID, partitionType, source, target);
			source.addOuputEdge(edge);
			target.addInputEdge(edge);

			return this;
		}

		public Builder setContainsCoLocationConstraints(boolean containsCoLocationConstraints) {
			this.containsCoLocationConstraints = containsCoLocationConstraints;
			return this;
		}

		public FailoverTopology build() {
			return new TestFailoverTopology(vertices, containsCoLocationConstraints);
		}
	}
}
