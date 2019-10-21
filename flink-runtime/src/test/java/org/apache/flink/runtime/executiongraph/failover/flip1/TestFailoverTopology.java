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
import java.util.Collections;
import java.util.List;

/**
 * A {@link FailoverTopology} implementation for tests.
 */
public class TestFailoverTopology
	implements FailoverTopology<TestFailoverTopology.TestFailoverVertex, TestFailoverTopology.TestFailoverResultPartition> {

	private final Collection<TestFailoverVertex> vertices;
	private final boolean containsCoLocationConstraints;

	public TestFailoverTopology(Collection<TestFailoverVertex> vertices, boolean containsCoLocationConstraints) {
		this.vertices = vertices;
		this.containsCoLocationConstraints = containsCoLocationConstraints;
	}

	@Override
	public Iterable<TestFailoverVertex> getVertices() {
		return vertices;
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}

	/**
	 * A {@link FailoverVertex} implementation for tests.
	 */
	public static class TestFailoverVertex
		implements FailoverVertex<TestFailoverVertex, TestFailoverResultPartition> {

		private final Collection<TestFailoverResultPartition> consumedPartitions = new ArrayList<>();
		private final Collection<TestFailoverResultPartition> producedPartitions = new ArrayList<>();
		private final ExecutionVertexID id;

		public TestFailoverVertex(ExecutionVertexID id) {
			this.id = id;
		}

		void addConsumedPartition(TestFailoverResultPartition partition) {
			consumedPartitions.add(partition);
		}

		void addProducedPartition(TestFailoverResultPartition partition) {
			producedPartitions.add(partition);
		}

		public ExecutionVertexID getId() {
			return id;
		}

		@Override
		public Iterable<TestFailoverResultPartition> getConsumedResults() {
			return consumedPartitions;
		}

		@Override
		public Iterable<TestFailoverResultPartition> getProducedResults() {
			return producedPartitions;
		}
	}

	/**
	 * A {@link FailoverResultPartition} implementation for tests.
	 */
	public static class TestFailoverResultPartition
		implements FailoverResultPartition<TestFailoverVertex, TestFailoverResultPartition> {

		private final IntermediateResultPartitionID resultPartitionID;
		private final ResultPartitionType resultPartitionType;
		private final TestFailoverVertex producer;
		private final List<TestFailoverVertex> consumers;

		public TestFailoverResultPartition(
				IntermediateResultPartitionID resultPartitionID,
				ResultPartitionType resultPartitionType,
				TestFailoverVertex producer,
				TestFailoverVertex consumer) {

			this.resultPartitionID = resultPartitionID;
			this.resultPartitionType = resultPartitionType;
			this.producer = producer;
			this.consumers = Collections.singletonList(consumer);
		}

		@Override
		public IntermediateResultPartitionID getId() {
			return resultPartitionID;
		}

		@Override
		public ResultPartitionType getResultType() {
			return resultPartitionType;
		}

		@Override
		public TestFailoverVertex getProducer() {
			return producer;
		}

		@Override
		public Iterable<TestFailoverVertex> getConsumers() {
			return consumers;
		}
	}

	/**
	 * Builder for {@link TestFailoverTopology}.
	 */
	public static class Builder {
		private boolean containsCoLocationConstraints = false;
		private Collection<TestFailoverVertex> vertices = new ArrayList<>();

		public TestFailoverVertex newVertex() {
			TestFailoverVertex testFailoverVertex = new TestFailoverVertex(new ExecutionVertexID(new JobVertexID(), 0));
			vertices.add(testFailoverVertex);
			return testFailoverVertex;
		}

		public Builder connect(TestFailoverVertex source, TestFailoverVertex target, ResultPartitionType partitionType) {
			TestFailoverResultPartition partition = new TestFailoverResultPartition(new IntermediateResultPartitionID(), partitionType, source, target);
			source.addProducedPartition(partition);
			target.addConsumedPartition(partition);

			return this;
		}

		public Builder connect(TestFailoverVertex source, TestFailoverVertex target, ResultPartitionType partitionType, IntermediateResultPartitionID partitionID) {
			TestFailoverResultPartition partition = new TestFailoverResultPartition(partitionID, partitionType, source, target);
			source.addProducedPartition(partition);
			target.addConsumedPartition(partition);

			return this;
		}

		public Builder setContainsCoLocationConstraints(boolean containsCoLocationConstraints) {
			this.containsCoLocationConstraints = containsCoLocationConstraints;
			return this;
		}

		public TestFailoverTopology build() {
			return new TestFailoverTopology(vertices, containsCoLocationConstraints);
		}
	}
}
