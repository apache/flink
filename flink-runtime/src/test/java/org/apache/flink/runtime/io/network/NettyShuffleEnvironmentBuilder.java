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

package org.apache.flink.runtime.io.network;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Builder for the {@link NettyShuffleEnvironment}.
 */
public class NettyShuffleEnvironmentBuilder {

	private static final int DEFAULT_NETWORK_BUFFER_SIZE = 32 << 10;
	private static final int DEFAULT_NUM_NETWORK_BUFFERS = 1024;

	private static final String[] DEFAULT_TEMP_DIRS = {EnvironmentInformation.getTemporaryFileDirectory()};
	private static final Duration DEFAULT_REQUEST_SEGMENTS_TIMEOUT = Duration.ofMillis(30000L);

	private int bufferSize = DEFAULT_NETWORK_BUFFER_SIZE;

	private int numNetworkBuffers = DEFAULT_NUM_NETWORK_BUFFERS;

	private int partitionRequestInitialBackoff;

	private int partitionRequestMaxBackoff;

	private int networkBuffersPerChannel = 2;

	private int floatingNetworkBuffersPerGate = 8;

	private int maxBuffersPerChannel = Integer.MAX_VALUE;

	private boolean blockingShuffleCompressionEnabled = false;

	private String compressionCodec = "LZ4";

	private ResourceID taskManagerLocation = ResourceID.generate();

	private NettyConfig nettyConfig;

	private MetricGroup metricGroup = UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

	private ResultPartitionManager resultPartitionManager = new ResultPartitionManager();

	private Executor ioExecutor = Executors.directExecutor();

	public NettyShuffleEnvironmentBuilder setTaskManagerLocation(ResourceID taskManagerLocation) {
		this.taskManagerLocation = taskManagerLocation;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setNumNetworkBuffers(int numNetworkBuffers) {
		this.numNetworkBuffers = numNetworkBuffers;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff) {
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff) {
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setMaxBuffersPerChannel(int maxBuffersPerChannel) {
		this.maxBuffersPerChannel = maxBuffersPerChannel;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setBlockingShuffleCompressionEnabled(boolean blockingShuffleCompressionEnabled) {
		this.blockingShuffleCompressionEnabled = blockingShuffleCompressionEnabled;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setNettyConfig(NettyConfig nettyConfig) {
		this.nettyConfig = nettyConfig;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setMetricGroup(MetricGroup metricGroup) {
		this.metricGroup = metricGroup;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setResultPartitionManager(ResultPartitionManager resultPartitionManager) {
		this.resultPartitionManager = resultPartitionManager;
		return this;
	}

	public NettyShuffleEnvironmentBuilder setIoExecutor(Executor ioExecutor) {
		this.ioExecutor = ioExecutor;
		return this;
	}

	public NettyShuffleEnvironment build() {
		return NettyShuffleServiceFactory.createNettyShuffleEnvironment(
			new NettyShuffleEnvironmentConfiguration(
				numNetworkBuffers,
				DEFAULT_NETWORK_BUFFER_SIZE,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				networkBuffersPerChannel,
				floatingNetworkBuffersPerGate,
				DEFAULT_REQUEST_SEGMENTS_TIMEOUT,
				false,
				nettyConfig,
				DEFAULT_TEMP_DIRS,
				BoundedBlockingSubpartitionType.AUTO,
				false,
				blockingShuffleCompressionEnabled,
				compressionCodec,
				maxBuffersPerChannel),
			taskManagerLocation,
			new TaskEventDispatcher(),
			resultPartitionManager,
			metricGroup,
			ioExecutor);
	}
}
