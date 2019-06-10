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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for the implementation of shuffle service local environment.
 *
 * <p>Input/Output interface of local shuffle service environment is based on memory {@link Buffer Buffers}. A producer
 * can write shuffle data into the buffers, obtained from the created {@link ResultPartitionWriter ResultPartitionWriters}
 * and a consumer reads the buffers from the created {@link InputGate InputGates}.
 *
 * <h1>Lifecycle management.</h1>
 *
 * <p>The interface contains method's to manage the lifecycle of the local shuffle service environment:
 * <ol>
 *     <li>{@link ShuffleEnvironment#start} must be called before using the shuffle service environment.</li>
 *     <li>{@link ShuffleEnvironment#close} is called to release the shuffle service environment.</li>
 * </ol>
 *
 * <h1>Shuffle Input/Output management.</h1>
 *
 * <h2>Result partition management.</h2>
 *
 * <p>The interface implements a factory of result partition writers to produce shuffle data:
 * {@link ShuffleEnvironment#createResultPartitionWriters}. The created writers are grouped per owner.
 * The owner is responsible for the writers' lifecycle from the moment of creation.
 *
 * <p>Partitions are released in the following cases:
 * <ol>
 *     <li>{@link ResultPartitionWriter#fail(Throwable)} and {@link ResultPartitionWriter#close()} are called
 *     if the production has failed.
 *     </li>
 *     <li>{@link ResultPartitionWriter#finish()} and {@link ResultPartitionWriter#close()} are called
 *     if the production is done. The actual release can take some time depending on implementation details,
 *     e.g. if the `end of consumption' confirmation from the consumer is being awaited implicitly
 *     or the partition is later released by {@link ShuffleEnvironment#releasePartitions(Collection)}.</li>
 *     <li>{@link ShuffleEnvironment#releasePartitions(Collection)} is called outside of the producer thread,
 *     e.g. to manage the lifecycle of BLOCKING result partitions which can outlive their producers.</li>
 * </ol>
 * The partitions, which currently still occupy local resources, can be queried with
 * {@link ShuffleEnvironment#getPartitionsOccupyingLocalResources}.
 *
 * <h2>Input gate management.</h2>
 *
 * <p>The interface implements a factory for the input gates: {@link ShuffleEnvironment#createInputGates}.
 * The created gates are grouped per owner. The owner is responsible for the gates' lifecycle from the moment of creation.
 *
 * <p>When the input gates are created, it can happen that not all consumed partitions are known at that moment
 * e.g. because their producers have not been started yet. Therefore, the {@link ShuffleEnvironment} provides
 * a method {@link ShuffleEnvironment#updatePartitionInfo} to update them externally, when the producer becomes known.
 * The update mechanism has to be threadsafe because the updated gate can be read concurrently from a different thread.
 *
 * @param <P> type of provided result partition writers
 * @param <G> type of provided input gates
 */
public interface ShuffleEnvironment<P extends ResultPartitionWriter, G extends InputGate> extends AutoCloseable {

	/**
	 * Start the internal related services before using the shuffle service environment.
	 *
	 * @return a port to connect for the shuffle data exchange, -1 if only local connection is possible.
	 */
	int start() throws IOException;

	/**
	 * Factory method for the {@link ResultPartitionWriter ResultPartitionWriters} to produce result partitions.
	 *
	 * <p>The order of the {@link ResultPartitionWriter ResultPartitionWriters} in the returned collection
	 * should be the same as the iteration order of the passed {@code resultPartitionDeploymentDescriptors}.
	 *
	 * @param ownerName the owner name, used for logs
	 * @param executionAttemptID execution attempt id of the producer
	 * @param resultPartitionDeploymentDescriptors descriptors of the partition, produced by the owner
	 * @param outputGroup shuffle specific group for output metrics
	 * @param buffersGroup shuffle specific group for buffer metrics
	 * @return collection of the {@link ResultPartitionWriter ResultPartitionWriters}
	 */
	Collection<P> createResultPartitionWriters(
		String ownerName,
		ExecutionAttemptID executionAttemptID,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		MetricGroup outputGroup,
		MetricGroup buffersGroup);

	/**
	 * Release local resources occupied by the given partitions.
	 *
	 * @param partitionIds identifying the partitions to be released
	 */
	void releasePartitions(Collection<ResultPartitionID> partitionIds);

	/**
	 * Report partitions which still occupy some resources locally.
	 *
	 * @return collection of partitions which still occupy some resources locally
	 * and have not been released yet.
	 */
	Collection<ResultPartitionID> getPartitionsOccupyingLocalResources();

	/**
	 * Factory method for the {@link InputGate InputGates} to consume result partitions.
	 *
	 * <p>The order of the {@link InputGate InputGates} in the returned collection should be the same as the iteration order
	 * of the passed {@code inputGateDeploymentDescriptors}.
	 *
	 * @param ownerName the owner name, used for logs
	 * @param executionAttemptID execution attempt id of the consumer
	 * @param partitionProducerStateProvider producer state provider to query whether the producer is ready for consumption
	 * @param inputGateDeploymentDescriptors descriptors of the input gates to consume
	 * @param parentGroup parent of shuffle specific metric group
	 * @param inputGroup shuffle specific group for input metrics
	 * @param buffersGroup shuffle specific group for buffer metrics
	 * @return collection of the {@link InputGate InputGates}
	 */
	Collection<G> createInputGates(
		String ownerName,
		ExecutionAttemptID executionAttemptID,
		PartitionProducerStateProvider partitionProducerStateProvider,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		MetricGroup parentGroup,
		MetricGroup inputGroup,
		MetricGroup buffersGroup);

	/**
	 * Update a gate with the newly available partition information, previously unknown.
	 *
	 * @param consumerID execution id to distinguish gates with the same id from the different consumer executions
	 * @param partitionInfo information needed to consume the updated partition, e.g. network location
	 * @return {@code true} if the partition has been updated or {@code false} if the partition is not available anymore.
	 * @throws IOException IO problem by the update
	 * @throws InterruptedException potentially blocking operation was interrupted
	 */
	boolean updatePartitionInfo(
		ExecutionAttemptID consumerID,
		PartitionInfo partitionInfo) throws IOException, InterruptedException;
}
