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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.Optional;

/**
 * Utility class to encapsulate the logic of building a {@link ResultPartition} instance.
 */
public class ResultPartitionBuilder {
	private JobID jobId = new JobID();

	private final TaskActions taskActions = new NoOpTaskActions();

	private ResultPartitionID partitionId = new ResultPartitionID();

	private ResultPartitionType partitionType = ResultPartitionType.PIPELINED;

	private int numberOfSubpartitions = 1;

	private int numTargetKeyGroups = 1;

	private ResultPartitionManager partitionManager = new ResultPartitionManager();

	private ResultPartitionConsumableNotifier partitionConsumableNotifier = new NoOpResultPartitionConsumableNotifier();

	private IOManager ioManager = new IOManagerAsync();

	private boolean sendScheduleOrUpdateConsumersMessage = false;

	private NetworkBufferPool networkBufferPool = new NetworkBufferPool(1, 1,1);

	private int networkBuffersPerChannel = 1;

	private int floatingNetworkBuffersPerGate = 1;

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private Optional<FunctionWithException<BufferPoolOwner, BufferPool, IOException>> bufferPoolFactory = Optional.empty();

	public ResultPartitionBuilder setJobId(JobID jobId) {
		this.jobId = jobId;
		return this;
	}

	public ResultPartitionBuilder setResultPartitionId(ResultPartitionID partitionId) {
		this.partitionId = partitionId;
		return this;
	}

	public ResultPartitionBuilder setResultPartitionType(ResultPartitionType partitionType) {
		this.partitionType = partitionType;
		return this;
	}

	public ResultPartitionBuilder setNumberOfSubpartitions(int numberOfSubpartitions) {
		this.numberOfSubpartitions = numberOfSubpartitions;
		return this;
	}

	public ResultPartitionBuilder setNumTargetKeyGroups(int numTargetKeyGroups) {
		this.numTargetKeyGroups = numTargetKeyGroups;
		return this;
	}

	public ResultPartitionBuilder setResultPartitionManager(ResultPartitionManager partitionManager) {
		this.partitionManager = partitionManager;
		return this;
	}

	ResultPartitionBuilder setResultPartitionConsumableNotifier(ResultPartitionConsumableNotifier notifier) {
		this.partitionConsumableNotifier = notifier;
		return this;
	}

	public ResultPartitionBuilder setIOManager(IOManager ioManager) {
		this.ioManager = ioManager;
		return this;
	}

	public ResultPartitionBuilder setSendScheduleOrUpdateConsumersMessage(
		boolean sendScheduleOrUpdateConsumersMessage) {

		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
		return this;
	}

	public ResultPartitionBuilder setupBufferPoolFactoryFromNetworkEnvironment(NetworkEnvironment environment) {
		return setNetworkBuffersPerChannel(environment.getConfiguration().networkBuffersPerChannel())
			.setFloatingNetworkBuffersPerGate(environment.getConfiguration().floatingNetworkBuffersPerGate())
			.setNetworkBufferPool(environment.getNetworkBufferPool());
	}

	public ResultPartitionBuilder setNetworkBufferPool(NetworkBufferPool networkBufferPool) {
		this.networkBufferPool = networkBufferPool;
		return this;
	}

	private ResultPartitionBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	private ResultPartitionBuilder setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		return this;
	}

	public ResultPartitionBuilder setBufferPoolFactory(
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		this.bufferPoolFactory = Optional.of(bufferPoolFactory);
		return this;
	}

	public ResultPartition build() {
		ResultPartitionFactory resultPartitionFactory = new ResultPartitionFactory(
			partitionManager,
			ioManager,
			networkBufferPool,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate);

		FunctionWithException<BufferPoolOwner, BufferPool, IOException> factory = bufferPoolFactory.orElseGet(() ->
			resultPartitionFactory.createBufferPoolFactory(numberOfSubpartitions, partitionType));

		return resultPartitionFactory.create(
			"Result Partition task",
			taskActions,
			jobId,
			partitionId,
			partitionType,
			numberOfSubpartitions,
			numTargetKeyGroups,
			partitionConsumableNotifier,
			sendScheduleOrUpdateConsumersMessage,
			factory);
	}
}
