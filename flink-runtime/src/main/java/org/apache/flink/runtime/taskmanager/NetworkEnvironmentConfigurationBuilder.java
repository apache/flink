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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.io.network.netty.NettyConfig;

/**
 * Builder for the {@link NetworkEnvironmentConfiguration}.
 */
public class NetworkEnvironmentConfigurationBuilder {

	private int numNetworkBuffers = 1024;

	private int networkBufferSize = 32 * 1024;

	private int partitionRequestInitialBackoff = 0;

	private int partitionRequestMaxBackoff = 0;

	private int networkBuffersPerChannel = 2;

	private int floatingNetworkBuffersPerGate = 8;

	private boolean isCreditBased = true;

	private NettyConfig nettyConfig;

	public NetworkEnvironmentConfigurationBuilder setNumNetworkBuffers(int numNetworkBuffers) {
		this.numNetworkBuffers = numNetworkBuffers;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setNetworkBufferSize(int networkBufferSize) {
		this.networkBufferSize = networkBufferSize;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff) {
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff) {
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setIsCreditBased(boolean isCreditBased) {
		this.isCreditBased = isCreditBased;
		return this;
	}

	public NetworkEnvironmentConfigurationBuilder setNettyConfig(NettyConfig nettyConfig) {
		this.nettyConfig = nettyConfig;
		return this;
	}

	public NetworkEnvironmentConfiguration build() {
		return new NetworkEnvironmentConfiguration(
			numNetworkBuffers,
			networkBufferSize,
			partitionRequestInitialBackoff,
			partitionRequestMaxBackoff,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			isCreditBased,
			nettyConfig);
	}
}
