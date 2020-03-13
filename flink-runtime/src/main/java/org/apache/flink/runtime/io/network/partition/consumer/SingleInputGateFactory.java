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
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
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
	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

	@Nonnull
	protected final ResourceID taskExecutorResourceId;

	protected final int partitionRequestInitialBackoff;

	protected final int partitionRequestMaxBackoff;

	@Nonnull
	protected final ConnectionManager connectionManager;

	@Nonnull
	protected final ResultPartitionManager partitionManager;

	@Nonnull
	protected final TaskEventPublisher taskEventPublisher;

	@Nonnull
	protected final NetworkBufferPool networkBufferPool;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	private final boolean blockingShuffleCompressionEnabled;

	private final String compressionCodec;

	private final int networkBufferSize;

	public SingleInputGateFactory(
			@Nonnull ResourceID taskExecutorResourceId,
			@Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
			@Nonnull ConnectionManager connectionManager,
			@Nonnull ResultPartitionManager partitionManager,
			@Nonnull TaskEventPublisher taskEventPublisher,
			@Nonnull NetworkBufferPool networkBufferPool) {
		this.taskExecutorResourceId = taskExecutorResourceId;
		this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
		this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
		this.networkBuffersPerChannel = networkConfig.networkBuffersPerChannel();
		this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
		this.blockingShuffleCompressionEnabled = networkConfig.isBlockingShuffleCompressionEnabled();
		this.compressionCodec = networkConfig.getCompressionCodec();
		this.networkBufferSize = networkConfig.networkBufferSize();
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
			int gateIndex,
			@Nonnull InputGateDeploymentDescriptor igdd,
			@Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
			@Nonnull InputChannelMetrics metrics) {
		SupplierWithException<BufferPool, IOException> bufferPoolFactory = createBufferPoolFactory(
			networkBufferPool,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			igdd.getShuffleDescriptors().length,
			igdd.getConsumedPartitionType());

		BufferDecompressor bufferDecompressor = null;
		if (igdd.getConsumedPartitionType().isBlocking() && blockingShuffleCompressionEnabled) {
			bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
		}

		SingleInputGate inputGate = new SingleInputGate(
			owningTaskName,
			gateIndex,
			igdd.getConsumedResultId(),
			igdd.getConsumedPartitionType(),
			igdd.getConsumedSubpartitionIndex(),
			igdd.getShuffleDescriptors().length,
			partitionProducerStateProvider,
			bufferPoolFactory,
			bufferDecompressor);

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
			inputGate.setInputChannel(inputChannels[i]);
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

	@VisibleForTesting
	protected InputChannel createKnownInputChannel(
			SingleInputGate inputGate,
			int index,
			NettyShuffleDescriptor inputChannelDescriptor,
			ChannelStatistics channelStatistics,
			InputChannelMetrics metrics) {
		ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
		if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
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
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			int size,
			ResultPartitionType type) {
		return () -> bufferPoolFactory.createBufferPool(0, floatingNetworkBuffersPerGate);
	}

	/**
	 * Statistics of input channels.
	 */
	protected static class ChannelStatistics {
		int numLocalChannels;
		int numRemoteChannels;
		int numUnknownChannels;

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
