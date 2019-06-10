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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;

/**
 * Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}.
 */
public class SingleInputGateFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	@Nonnull
	private final ResourceID taskExecutorLocation;

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
			@Nonnull ResourceID taskExecutorLocation,
			@Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
			@Nonnull ConnectionManager connectionManager,
			@Nonnull ResultPartitionManager partitionManager,
			@Nonnull TaskEventPublisher taskEventPublisher,
			@Nonnull NetworkBufferPool networkBufferPool) {
		this.taskExecutorLocation = taskExecutorLocation;
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
			@Nonnull InputGateDeploymentDescriptor igdd,
			@Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
			@Nonnull InputChannelMetrics metrics) {
		SupplierWithException<BufferPool, IOException> bufferPoolFactory = createBufferPoolFactory(
			networkBufferPool,
			isCreditBased,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			igdd.getShuffleDescriptors().length,
			igdd.getConsumedPartitionType());

		SingleInputGate inputGate = new SingleInputGate(
			owningTaskName,
			igdd.getConsumedResultId(),
			igdd.getConsumedPartitionType(),
			igdd.getConsumedSubpartitionIndex(),
			igdd.getShuffleDescriptors().length,
			partitionProducerStateProvider,
			isCreditBased,
			bufferPoolFactory);

		createInputChannels(owningTaskName, igdd, inputGate, metrics);
		return inputGate;
	}

	private void createInputChannels(
			String owningTaskName,
			InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
			SingleInputGate inputGate,
			InputChannelMetrics metrics) {
		ShuffleDescriptor[] shuffleDescriptors = inputGateDeploymentDescriptor.getShuffleDescriptors();

		// Create the input channels. There is one input channel for each consumed partition.
		InputChannel[] inputChannels = new InputChannel[shuffleDescriptors.length];

		ChannelStatistics channelStatistics = new ChannelStatistics();

		for (int i = 0; i < inputChannels.length; i++) {
			inputChannels[i] = createInputChannel(
				inputGate,
				i,
				shuffleDescriptors[i],
				channelStatistics,
				metrics);
			ResultPartitionID resultPartitionID = inputChannels[i].getPartitionId();
			inputGate.setInputChannel(resultPartitionID.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("{}: Created {} input channels ({}).",
			owningTaskName,
			inputChannels.length,
			channelStatistics);
	}

	private InputChannel createInputChannel(
			SingleInputGate inputGate,
			int index,
			ShuffleDescriptor shuffleDescriptor,
			ChannelStatistics channelStatistics,
			InputChannelMetrics metrics) {
		return applyWithShuffleTypeCheck(
			NettyShuffleDescriptor.class,
			shuffleDescriptor,
			unknownShuffleDescriptor -> {
				channelStatistics.numUnknownChannels++;
				return new UnknownInputChannel(
					inputGate,
					index,
					unknownShuffleDescriptor.getResultPartitionID(),
					partitionManager,
					taskEventPublisher,
					connectionManager,
					partitionRequestInitialBackoff,
					partitionRequestMaxBackoff,
					metrics,
					networkBufferPool);
			},
			nettyShuffleDescriptor ->
				createKnownInputChannel(
					inputGate,
					index,
					nettyShuffleDescriptor,
					channelStatistics,
					metrics));
	}

	private InputChannel createKnownInputChannel(
			SingleInputGate inputGate,
			int index,
			NettyShuffleDescriptor inputChannelDescriptor,
			ChannelStatistics channelStatistics,
			InputChannelMetrics metrics) {
		ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
		if (inputChannelDescriptor.isLocalTo(taskExecutorLocation)) {
			// Consuming task is deployed to the same TaskManager as the partition => local
			channelStatistics.numLocalChannels++;
			return new LocalInputChannel(
				inputGate,
				index,
				partitionId,
				partitionManager,
				taskEventPublisher,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics);
		} else {
			// Different instances => remote
			channelStatistics.numRemoteChannels++;
			return new RemoteInputChannel(
				inputGate,
				index,
				partitionId,
				inputChannelDescriptor.getConnectionId(),
				connectionManager,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics,
				networkBufferPool);
		}
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
			int maxNumberOfMemorySegments = type.isBounded() ? floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			return () -> bufferPoolFactory.createBufferPool(0, maxNumberOfMemorySegments);
		} else {
			int maxNumberOfMemorySegments = type.isBounded() ?
				size * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			return () -> bufferPoolFactory.createBufferPool(size, maxNumberOfMemorySegments);
		}
	}

	private static class ChannelStatistics {
		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		@Override
		public String toString() {
			return String.format(
				"local: %s, remote: %s, unknown: %s",
				numLocalChannels,
				numRemoteChannels,
				numUnknownChannels);
		}
	}
}
