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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Optional;

/**
 * Factory for {@link ResultPartition} to use in {@link org.apache.flink.runtime.io.network.NetworkEnvironment}.
 */
public class ResultPartitionFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

	@Nonnull
	private final ResultPartitionManager partitionManager;

	@Nonnull
	private final IOManager ioManager;

	@Nonnull
	private final BufferPoolFactory bufferPoolFactory;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	public ResultPartitionFactory(
		@Nonnull ResultPartitionManager partitionManager,
		@Nonnull IOManager ioManager,
		@Nonnull BufferPoolFactory bufferPoolFactory,
		int networkBuffersPerChannel,
		int floatingNetworkBuffersPerGate) {

		this.partitionManager = partitionManager;
		this.ioManager = ioManager;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.bufferPoolFactory = bufferPoolFactory;
	}

	public ResultPartition create(
		@Nonnull String taskNameWithSubtaskAndId,
		@Nonnull TaskActions taskActions,
		@Nonnull JobID jobId,
		@Nonnull ExecutionAttemptID executionAttemptID,
		@Nonnull ResultPartitionDeploymentDescriptor desc,
		@Nonnull ResultPartitionConsumableNotifier partitionConsumableNotifier) {

		return create(
			taskNameWithSubtaskAndId,
			taskActions,
			jobId,
			new ResultPartitionID(desc.getPartitionId(), executionAttemptID),
			desc.getPartitionType(),
			desc.getNumberOfSubpartitions(),
			desc.getMaxParallelism(),
			partitionConsumableNotifier,
			desc.sendScheduleOrUpdateConsumersMessage(),
			createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType()));
	}

	@VisibleForTesting
	public ResultPartition create(
		@Nonnull String taskNameWithSubtaskAndId,
		@Nonnull TaskActions taskActions,
		@Nonnull JobID jobId,
		@Nonnull ResultPartitionID id,
		@Nonnull ResultPartitionType type,
		int numberOfSubpartitions,
		int maxParallelism,
		@Nonnull ResultPartitionConsumableNotifier partitionConsumableNotifier,
		boolean sendScheduleOrUpdateConsumersMessage,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

		ResultPartition partition = new ResultPartition(
			taskNameWithSubtaskAndId,
			taskActions,
			jobId,
			id,
			type,
			subpartitions,
			maxParallelism,
			partitionManager,
			partitionConsumableNotifier,
			sendScheduleOrUpdateConsumersMessage,
			bufferPoolFactory);

		createSubpartitions(partition, type, subpartitions);

		// Initially, partitions should be consumed once before release.
		partition.pin();

		LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

		return partition;
	}

	private void createSubpartitions(
		ResultPartition partition, ResultPartitionType type, ResultSubpartition[] subpartitions) {

		// Create the subpartitions.
		switch (type) {
			case BLOCKING:
				initializeBoundedBlockingPartitions(subpartitions, partition, ioManager);
				break;

			case PIPELINED:
			case PIPELINED_BOUNDED:
				for (int i = 0; i < subpartitions.length; i++) {
					subpartitions[i] = new PipelinedSubpartition(i, partition);
				}

				break;

			default:
				throw new IllegalArgumentException("Unsupported result partition type.");
		}
	}

	private static void initializeBoundedBlockingPartitions(
		ResultSubpartition[] subpartitions,
		ResultPartition parent,
		IOManager ioManager) {

		int i = 0;
		try {
			for (; i < subpartitions.length; i++) {
				subpartitions[i] = new BoundedBlockingSubpartition(
					i, parent, ioManager.createChannel().getPathFile().toPath());
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
		int numberOfSubpartitions, ResultPartitionType type) {

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
}
