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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MemoryArchitecture;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Factory for {@link ResultPartition} to use in {@link NettyShuffleEnvironment}.
 */
public class ResultPartitionFactory {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionFactory.class);

	private final ResultPartitionManager partitionManager;

	private final FileChannelManager channelManager;

	private final BufferPoolFactory bufferPoolFactory;

	private final BoundedBlockingSubpartitionType blockingSubpartitionType;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	private final int networkBufferSize;

	private final boolean forcePartitionReleaseOnConsumption;

	public ResultPartitionFactory(
		ResultPartitionManager partitionManager,
		FileChannelManager channelManager,
		BufferPoolFactory bufferPoolFactory,
		BoundedBlockingSubpartitionType blockingSubpartitionType,
		int networkBuffersPerChannel,
		int floatingNetworkBuffersPerGate,
		int networkBufferSize,
		boolean forcePartitionReleaseOnConsumption) {

		this.partitionManager = partitionManager;
		this.channelManager = channelManager;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.bufferPoolFactory = bufferPoolFactory;
		this.blockingSubpartitionType = blockingSubpartitionType;
		this.networkBufferSize = networkBufferSize;
		this.forcePartitionReleaseOnConsumption = forcePartitionReleaseOnConsumption;
	}

	public ResultPartition create(
			String taskNameWithSubtaskAndId,
			ResultPartitionDeploymentDescriptor desc) {
		return create(
			taskNameWithSubtaskAndId,
			desc.getShuffleDescriptor().getResultPartitionID(),
			desc.getPartitionType(),
			desc.getNumberOfSubpartitions(),
			desc.getMaxParallelism(),
			createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType()));
	}

	@VisibleForTesting
	public ResultPartition create(
			String taskNameWithSubtaskAndId,
			ResultPartitionID id,
			ResultPartitionType type,
			int numberOfSubpartitions,
			int maxParallelism,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

		ResultPartition partition = forcePartitionReleaseOnConsumption || !type.isBlocking()
			? new ReleaseOnConsumptionResultPartition(
				taskNameWithSubtaskAndId,
				id,
				type,
				subpartitions,
				maxParallelism,
				partitionManager,
				bufferPoolFactory)
			: new ResultPartition(
				taskNameWithSubtaskAndId,
				id,
				type,
				subpartitions,
				maxParallelism,
				partitionManager,
				bufferPoolFactory);

		createSubpartitions(partition, type, blockingSubpartitionType, subpartitions);

		LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

		return partition;
	}

	private void createSubpartitions(
			ResultPartition partition,
			ResultPartitionType type,
			BoundedBlockingSubpartitionType blockingSubpartitionType,
			ResultSubpartition[] subpartitions) {
		// Create the subpartitions.
		if (type.isBlocking()) {
			initializeBoundedBlockingPartitions(
				subpartitions,
				partition,
				blockingSubpartitionType,
				networkBufferSize,
				channelManager);
		} else {
			for (int i = 0; i < subpartitions.length; i++) {
				subpartitions[i] = new PipelinedSubpartition(i, partition);
			}
		}
	}

	private static void initializeBoundedBlockingPartitions(
			ResultSubpartition[] subpartitions,
			ResultPartition parent,
			BoundedBlockingSubpartitionType blockingSubpartitionType,
			int networkBufferSize,
			FileChannelManager channelManager) {
		int i = 0;
		try {
			for (i = 0; i < subpartitions.length; i++) {
				final File spillFile = channelManager.createChannel().getPathFile();
				subpartitions[i] = blockingSubpartitionType.create(i, parent, spillFile, networkBufferSize);
			}
		}
		catch (IOException e) {
			// undo all the work so that a failed constructor does not leave any resources
			// in need of disposal
			releasePartitionsQuietly(subpartitions, i);

			// this is not good, we should not be forced to wrap this in a runtime exception.
			// the fact that the ResultPartition and Task constructor (which calls this) do not tolerate any exceptions
			// is incompatible with eager initialization of resources (RAII).
			throw new FlinkRuntimeException(e);
		}
	}

	private static void releasePartitionsQuietly(ResultSubpartition[] partitions, int until) {
		for (int i = 0; i < until; i++) {
			final ResultSubpartition subpartition = partitions[i];
			ExceptionUtils.suppressExceptions(subpartition::release);
		}
	}

	@VisibleForTesting
	FunctionWithException<BufferPoolOwner, BufferPool, IOException> createBufferPoolFactory(
			int numberOfSubpartitions,
			ResultPartitionType type) {
		return p -> {
			int maxNumberOfMemorySegments = type.isBounded() ?
				numberOfSubpartitions * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			// If the partition type is back pressure-free, we register with the buffer pool for
			// callbacks to release memory.
			return bufferPoolFactory.createBufferPool(numberOfSubpartitions,
				maxNumberOfMemorySegments,
				type.hasBackPressure() ? Optional.empty() : Optional.of(p));
		};
	}

	static BoundedBlockingSubpartitionType getBoundedBlockingType() {
		switch (MemoryArchitecture.get()) {
			case _64_BIT:
				return BoundedBlockingSubpartitionType.FILE_MMAP;
			case _32_BIT:
				return BoundedBlockingSubpartitionType.FILE;
			case UNKNOWN:
			default:
				LOG.warn("Cannot determine memory architecture. Using pure file-based shuffle.");
				return BoundedBlockingSubpartitionType.FILE;
		}
	}
}
