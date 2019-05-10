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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Factory for {@link SingleInputGate} to use in {@link org.apache.flink.runtime.io.network.NetworkEnvironment}.
 */
public class SingleInputGateFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	private final boolean isCreditBased;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	@Nonnull
	private final ConnectionManager connectionManager;

	@Nonnull
	private final ResultPartitionManager partitionManager;

	@Nonnull
	private final TaskEventPublisher taskEventPublisher;

	@Nonnull
	private final NetworkBufferPool networkBufferPool;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	public SingleInputGateFactory(
			@Nonnull NetworkEnvironmentConfiguration networkConfig,
			@Nonnull ConnectionManager connectionManager,
			@Nonnull ResultPartitionManager partitionManager,
			@Nonnull TaskEventPublisher taskEventPublisher,
			@Nonnull NetworkBufferPool networkBufferPool) {
		this.isCreditBased = networkConfig.isCreditBased();
		this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
		this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
		this.networkBuffersPerChannel = networkConfig.networkBuffersPerChannel();
		this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
		this.connectionManager = connectionManager;
		this.partitionManager = partitionManager;
		this.taskEventPublisher = taskEventPublisher;
		this.networkBufferPool = networkBufferPool;
	}

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public SingleInputGate create(
			@Nonnull String owningTaskName,
			@Nonnull JobID jobId,
			@Nonnull InputGateDeploymentDescriptor igdd,
			@Nonnull TaskActions taskActions,
			@Nonnull InputChannelMetrics metrics,
			@Nonnull Counter numBytesInCounter) {
		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length, taskActions, numBytesInCounter, isCreditBased,
			createBufferPoolFactory(icdd.length, consumedPartitionType));

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					partitionManager,
					taskEventPublisher,
					partitionRequestInitialBackoff,
					partitionRequestMaxBackoff,
					metrics);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					connectionManager,
					partitionRequestInitialBackoff,
					partitionRequestMaxBackoff,
					metrics,
					networkBufferPool);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					partitionManager,
					taskEventPublisher,
					connectionManager,
					partitionRequestInitialBackoff,
					partitionRequestMaxBackoff,
					metrics,
					networkBufferPool);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("{}: Created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}

	private SupplierWithException<BufferPool, IOException> createBufferPoolFactory(int size, ResultPartitionType type) {
		return createBufferPoolFactory(
			networkBufferPool, isCreditBased, networkBuffersPerChannel, floatingNetworkBuffersPerGate, size, type);
	}

	@VisibleForTesting
	static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
		BufferPoolFactory bufferPoolFactory,
		boolean isCreditBased,
		int networkBuffersPerChannel,
		int floatingNetworkBuffersPerGate,
		int size,
		ResultPartitionType type) {

		if (isCreditBased) {
			int maxNumberOfMemorySegments = type.isBounded() ?
				floatingNetworkBuffersPerGate : Integer.MAX_VALUE;

			return () -> bufferPoolFactory.createBufferPool(0, maxNumberOfMemorySegments);
		} else {
			int maxNumberOfMemorySegments = type.isBounded() ?
				size * networkBuffersPerChannel +
					floatingNetworkBuffersPerGate : Integer.MAX_VALUE;

			return () -> bufferPoolFactory.createBufferPool(size, maxNumberOfMemorySegments);
		}
	}
}
