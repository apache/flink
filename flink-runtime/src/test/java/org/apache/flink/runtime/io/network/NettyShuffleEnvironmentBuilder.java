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
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.time.Duration;

/**
 * Builder for the {@link NettyShuffleEnvironment}.
 */
public class NettyShuffleEnvironmentBuilder {

	private static final int DEFAULT_NETWORK_BUFFER_SIZE = 32 << 10;
	private static final int DEFAULT_NUM_NETWORK_BUFFERS = 1024;

	private static final String[] DEFAULT_TEMP_DIRS = {EnvironmentInformation.getTemporaryFileDirectory()};
	private static final Duration DEFAULT_REQUEST_SEGMENTS_TIMEOUT = Duration.ofMillis(30000L);

	private int numNetworkBuffers = DEFAULT_NUM_NETWORK_BUFFERS;

	private int partitionRequestInitialBackoff;

	private int partitionRequestMaxBackoff;

	private int networkBuffersPerChannel = 2;

	private int floatingNetworkBuffersPerGate = 8;

	private boolean isCreditBased = true;

	private ResourceID taskManagerLocation = ResourceID.generate();

	private NettyConfig nettyConfig;

	private MetricGroup metricGroup = UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

	public NettyShuffleEnvironmentBuilder setTaskManagerLocation(ResourceID taskManagerLocation) {
		this.taskManagerLocation = taskManagerLocation;
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

	public NettyShuffleEnvironmentBuilder setIsCreditBased(boolean isCreditBased) {
		this.isCreditBased = isCreditBased;
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
				isCreditBased,
				false,
				nettyConfig,
				DEFAULT_TEMP_DIRS,
				BoundedBlockingSubpartitionType.AUTO,
				false),
			taskManagerLocation,
			new TaskEventDispatcher(),
			metricGroup);
	}
}
