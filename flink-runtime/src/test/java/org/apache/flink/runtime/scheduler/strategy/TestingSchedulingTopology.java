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

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.DONE;

/**
 * A simple scheduling topology for testing purposes.
 */
public class TestingSchedulingTopology implements SchedulingTopology {

	private final Map<ExecutionVertexID, SchedulingExecutionVertex> schedulingExecutionVertices = new HashMap<>();

	private final Map<IntermediateResultPartitionID, SchedulingResultPartition> schedulingResultPartitions = new HashMap<>();

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return Collections.unmodifiableCollection(schedulingExecutionVertices.values());
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId)  {
		return Optional.ofNullable(schedulingExecutionVertices.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(
			IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.of(schedulingResultPartitions.get(intermediateResultPartitionId));
	}

	void addSchedulingExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		schedulingExecutionVertices.put(schedulingExecutionVertex.getId(), schedulingExecutionVertex);
		addSchedulingResultPartitions(schedulingExecutionVertex.getConsumedResultPartitions());
		addSchedulingResultPartitions(schedulingExecutionVertex.getProducedResultPartitions());
	}

	private void addSchedulingResultPartitions(final Collection<SchedulingResultPartition> resultPartitions) {
		for (SchedulingResultPartition schedulingResultPartition : resultPartitions) {
			schedulingResultPartitions.put(schedulingResultPartition.getId(), schedulingResultPartition);
		}
	}

	private void addSchedulingExecutionVertices(List<TestingSchedulingExecutionVertex> vertices) {
		for (TestingSchedulingExecutionVertex vertex : vertices) {
			addSchedulingExecutionVertex(vertex);
		}
	}

	public SchedulingExecutionVerticesBuilder addExecutionVertices() {
		return new SchedulingExecutionVerticesBuilder();
	}

	public ProducerConsumerConnectionBuilder connectPointwise(
		final List<TestingSchedulingExecutionVertex> producers,
		final List<TestingSchedulingExecutionVertex> consumers) {

		return new ProducerConsumerPointwiseConnectionBuilder(producers, consumers);
	}

	public ProducerConsumerConnectionBuilder connectAllToAll(
		final List<TestingSchedulingExecutionVertex> producers,
		final List<TestingSchedulingExecutionVertex> consumers) {

		return new ProducerConsumerAllToAllConnectionBuilder(producers, consumers);
	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition}.
	 */
	public abstract class ProducerConsumerConnectionBuilder {

		protected final List<TestingSchedulingExecutionVertex> producers;

		protected final List<TestingSchedulingExecutionVertex> consumers;

		protected ResultPartitionType resultPartitionType = ResultPartitionType.BLOCKING;

		protected SchedulingResultPartition.ResultPartitionState resultPartitionState = DONE;

		protected ProducerConsumerConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			this.producers = producers;
			this.consumers = consumers;
		}

		public ProducerConsumerConnectionBuilder withResultPartitionType(final ResultPartitionType resultPartitionType) {
			this.resultPartitionType = resultPartitionType;
			return this;
		}

		public ProducerConsumerConnectionBuilder withResultPartitionState(final SchedulingResultPartition.ResultPartitionState state) {
			this.resultPartitionState = state;
			return this;
		}

		public List<TestingSchedulingResultPartition> finish() {
			final List<TestingSchedulingResultPartition> resultPartitions = connect();

			TestingSchedulingTopology.this.addSchedulingExecutionVertices(producers);
			TestingSchedulingTopology.this.addSchedulingExecutionVertices(consumers);

			return resultPartitions;
		}

		TestingSchedulingResultPartition.Builder initTestingSchedulingResultPartitionBuilder() {
			return new TestingSchedulingResultPartition.Builder()
				.withResultPartitionType(resultPartitionType);
		}

		protected abstract List<TestingSchedulingResultPartition> connect();

	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition} of {@link DistributionPattern#POINTWISE}.
	 */
	private class ProducerConsumerPointwiseConnectionBuilder extends ProducerConsumerConnectionBuilder {

		private ProducerConsumerPointwiseConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			super(producers, consumers);
			// currently we only support one to one
			Preconditions.checkState(producers.size() == consumers.size());
		}

		@Override
		protected List<TestingSchedulingResultPartition> connect() {
			final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();
			final IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();

			for (int idx = 0; idx < producers.size(); idx++) {
				final TestingSchedulingExecutionVertex producer = producers.get(idx);
				final TestingSchedulingExecutionVertex consumer = consumers.get(idx);

				final TestingSchedulingResultPartition resultPartition = initTestingSchedulingResultPartitionBuilder()
					.withIntermediateDataSetID(intermediateDataSetId)
					.withResultPartitionState(resultPartitionState)
					.build();
				resultPartition.setProducer(producer);
				producer.addProducedPartition(resultPartition);
				consumer.addConsumedPartition(resultPartition);
				resultPartition.addConsumer(consumer);
				resultPartitions.add(resultPartition);
			}

			return resultPartitions;
		}
	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition} of {@link DistributionPattern#ALL_TO_ALL}.
	 */
	private class ProducerConsumerAllToAllConnectionBuilder extends ProducerConsumerConnectionBuilder {

		private ProducerConsumerAllToAllConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			super(producers, consumers);
		}

		@Override
		protected List<TestingSchedulingResultPartition> connect() {
			final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();
			final IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();

			for (TestingSchedulingExecutionVertex producer : producers) {

				final TestingSchedulingResultPartition resultPartition = initTestingSchedulingResultPartitionBuilder()
					.withIntermediateDataSetID(intermediateDataSetId)
					.withResultPartitionState(resultPartitionState)
					.build();
				resultPartition.setProducer(producer);
				producer.addProducedPartition(resultPartition);

				for (TestingSchedulingExecutionVertex consumer : consumers) {
					consumer.addConsumedPartition(resultPartition);
					resultPartition.addConsumer(consumer);
				}
				resultPartitions.add(resultPartition);
			}

			return resultPartitions;
		}
	}

	/**
	 * Builder for {@link TestingSchedulingExecutionVertex}.
	 */
	public class SchedulingExecutionVerticesBuilder {

		private final JobVertexID jobVertexId = new JobVertexID();

		private int parallelism = 1;

		private InputDependencyConstraint inputDependencyConstraint = InputDependencyConstraint.ANY;

		public SchedulingExecutionVerticesBuilder withParallelism(final int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public SchedulingExecutionVerticesBuilder withInputDependencyConstraint(final InputDependencyConstraint inputDependencyConstraint) {
			this.inputDependencyConstraint = inputDependencyConstraint;
			return this;
		}

		public List<TestingSchedulingExecutionVertex> finish() {
			final List<TestingSchedulingExecutionVertex> vertices = new ArrayList<>();
			for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
				vertices.add(new TestingSchedulingExecutionVertex(jobVertexId, subtaskIndex, inputDependencyConstraint));
			}

			TestingSchedulingTopology.this.addSchedulingExecutionVertices(vertices);

			return vertices;
		}
	}
}
