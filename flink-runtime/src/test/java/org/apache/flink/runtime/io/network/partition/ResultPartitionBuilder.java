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

import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.Optional;

/**
 * Utility class to encapsulate the logic of building a {@link ResultPartition} instance.
 */
public class ResultPartitionBuilder {

	private ResultPartitionID partitionId = new ResultPartitionID();

	private ResultPartitionType partitionType = ResultPartitionType.PIPELINED;

	private BoundedBlockingSubpartitionType blockingSubpartitionType = BoundedBlockingSubpartitionType.AUTO;

	private int numberOfSubpartitions = 1;

	private int numTargetKeyGroups = 1;

	private ResultPartitionManager partitionManager = new ResultPartitionManager();

	private FileChannelManager channelManager = NoOpFileChannelManager.INSTANCE;

	private NetworkBufferPool networkBufferPool = new NetworkBufferPool(1, 1, 1);

	private int networkBuffersPerChannel = 1;

	private int floatingNetworkBuffersPerGate = 1;

	private int networkBufferSize = 1;

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private Optional<FunctionWithException<BufferPoolOwner, BufferPool, IOException>> bufferPoolFactory = Optional.empty();

	private boolean releasedOnConsumption;

	private boolean blockingShuffleCompressionEnabled = false;

	private boolean pipelinedShuffleCompressionEnabled = false;

	private String compressionCodec = "LZ4";

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

	public ResultPartitionBuilder setFileChannelManager(FileChannelManager channelManager) {
		this.channelManager = channelManager;
		return this;
	}

	public ResultPartitionBuilder setupBufferPoolFactoryFromNettyShuffleEnvironment(NettyShuffleEnvironment environment) {
		return setNetworkBuffersPerChannel(environment.getConfiguration().networkBuffersPerChannel())
			.setFloatingNetworkBuffersPerGate(environment.getConfiguration().floatingNetworkBuffersPerGate())
			.setNetworkBufferSize(environment.getConfiguration().networkBufferSize())
			.setNetworkBufferPool(environment.getNetworkBufferPool());
	}

	public ResultPartitionBuilder setNetworkBufferPool(NetworkBufferPool networkBufferPool) {
		this.networkBufferPool = networkBufferPool;
		return this;
	}

	public ResultPartitionBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	public ResultPartitionBuilder setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		return this;
	}

	ResultPartitionBuilder setNetworkBufferSize(int networkBufferSize) {
		this.networkBufferSize = networkBufferSize;
		return this;
	}

	public ResultPartitionBuilder setBufferPoolFactory(
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		this.bufferPoolFactory = Optional.of(bufferPoolFactory);
		return this;
	}

	public ResultPartitionBuilder isReleasedOnConsumption(boolean releasedOnConsumption) {
		this.releasedOnConsumption = releasedOnConsumption;
		return this;
	}

	public ResultPartitionBuilder setBlockingShuffleCompressionEnabled(boolean blockingShuffleCompressionEnabled) {
		this.blockingShuffleCompressionEnabled = blockingShuffleCompressionEnabled;
		return this;
	}

	public ResultPartitionBuilder setPipelinedShuffleCompressionEnabled(boolean pipelinedShuffleCompressionEnabled) {
		this.pipelinedShuffleCompressionEnabled = pipelinedShuffleCompressionEnabled;
		return this;
	}

	public ResultPartitionBuilder setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
		return this;
	}

	ResultPartitionBuilder setBoundedBlockingSubpartitionType(
			@SuppressWarnings("SameParameterValue") BoundedBlockingSubpartitionType blockingSubpartitionType) {
		this.blockingSubpartitionType = blockingSubpartitionType;
		return this;
	}

	public ResultPartition build() {
		ResultPartitionFactory resultPartitionFactory = new ResultPartitionFactory(
			partitionManager,
			channelManager,
			networkBufferPool,
			blockingSubpartitionType,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			networkBufferSize,
			releasedOnConsumption,
			blockingShuffleCompressionEnabled,
			pipelinedShuffleCompressionEnabled,
			compressionCodec);

		FunctionWithException<BufferPoolOwner, BufferPool, IOException> factory = bufferPoolFactory.orElseGet(() ->
			resultPartitionFactory.createBufferPoolFactory(numberOfSubpartitions, partitionType));

		return resultPartitionFactory.create(
			"Result Partition task",
			partitionId,
			partitionType,
			numberOfSubpartitions,
			numTargetKeyGroups,
			factory);
	}
}
